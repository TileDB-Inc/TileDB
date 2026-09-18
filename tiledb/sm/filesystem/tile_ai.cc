/**
 * @file tile_ai.cc
 *
 * VFS backend implementation routing S3 access through presigned URLs.
 */

#include "tiledb/sm/filesystem/tile_ai.h"

#include <curl/curl.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <optional>
#include <sstream>
#include <unordered_set>

#include "tiledb/common/logger.h"

namespace tiledb::sm {

using tiledb::common::filesystem::directory_entry;

namespace {

constexpr uint64_t kMinS3MultipartPartSize = 5ULL * 1024 * 1024;

struct ReadBuffer {
  void* dest;
  uint64_t written;
  uint64_t max_bytes;
};

static size_t curl_write_to_buffer(
    char* ptr, size_t size, size_t nmemb, void* userdata) {
  auto* rb = static_cast<ReadBuffer*>(userdata);
  size_t total = size * nmemb;
  size_t to_copy =
      std::min(total, static_cast<size_t>(rb->max_bytes - rb->written));
  if (to_copy > 0) {
    std::memcpy(static_cast<char*>(rb->dest) + rb->written, ptr, to_copy);
    rb->written += to_copy;
  }
  return total;
}

struct WriteSource {
  const char* data;
  uint64_t size;
  uint64_t sent;
};

static size_t curl_read_from_buffer(
    char* ptr, size_t size, size_t nmemb, void* userdata) {
  auto* ws = static_cast<WriteSource*>(userdata);
  size_t remaining = ws->size - ws->sent;
  size_t to_send = std::min(remaining, size * nmemb);
  if (to_send > 0) {
    std::memcpy(ptr, ws->data + ws->sent, to_send);
    ws->sent += to_send;
  }
  return to_send;
}

struct HeaderCapture {
  std::string header_name;
  std::string value;
};

static size_t curl_header_cb(
    char* buffer, size_t size, size_t nitems, void* userdata) {
  auto* cap = static_cast<HeaderCapture*>(userdata);
  size_t total = size * nitems;
  std::string line(buffer, total);
  auto colon = line.find(':');
  if (colon != std::string::npos) {
    std::string name = line.substr(0, colon);
    std::transform(name.begin(), name.end(), name.begin(), ::tolower);
    if (name == cap->header_name) {
      std::string val = line.substr(colon + 1);
      auto start = val.find_first_not_of(" \t\r\n");
      auto end = val.find_last_not_of(" \t\r\n");
      if (start != std::string::npos) {
        cap->value = val.substr(start, end - start + 1);
      }
    }
  }
  return total;
}

static size_t curl_write_string(
    char* ptr, size_t size, size_t nmemb, void* userdata) {
  auto* buf = static_cast<std::string*>(userdata);
  buf->append(ptr, size * nmemb);
  return size * nmemb;
}

}  // anonymous namespace

TileAi::TileAi() = default;
TileAi::~TileAi() = default;

void TileAi::init(const Config& config) {
  // Credentials are resolved through the standard Config chain:
  // user-set → TILEDB_VFS_TILE_* env → TILE_API_* SDK env alias →
  // profile → default. The SDK alias step lives in Config::get_from_env
  // so this code is just a config read.
  server_url_ = std::string(
      config.get<std::string_view>("vfs.tile.server_url").value_or(""));
  if (server_url_.empty()) {
    throw TileAiException(
        "vfs.tile.server_url must be set to use tile:// backend "
        "(or set the TILE_API_URL environment variable)");
  }

  api_key_ = std::string(
      config.get<std::string_view>("vfs.tile.api_key").value_or(""));
  if (api_key_.empty()) {
    throw TileAiException(
        "vfs.tile.api_key must be set to use tile:// backend "
        "(or set the TILE_API_KEY environment variable)");
  }

  // Workspace context. Optional at init time — when absent the server
  // applies its documented three-tier resolution (single-workspace
  // auto-resolve, then derive from entity ids in the request). When
  // set, the client appends `?workspaceId=<value>` to every request
  // URL and the server validates membership plus, on per-tile routes,
  // that the resource lives in the named workspace.
  workspace_ = std::string(
      config.get<std::string_view>("vfs.tile.workspace").value_or(""));

  multipart_threshold_bytes_ =
      config.get<uint64_t>("vfs.tile.multipart_threshold_bytes")
          .value_or(5ULL * 1024 * 1024);

  multipart_part_size_bytes_ =
      config.get<uint64_t>("vfs.tile.multipart_part_size_bytes")
          .value_or(5ULL * 1024 * 1024);

  if (multipart_part_size_bytes_ < kMinS3MultipartPartSize) {
    throw TileAiException(
        "vfs.tile.multipart_part_size_bytes must be at least 5242880 "
        "bytes for S3-compatible multipart uploads");
  }

  if (multipart_threshold_bytes_ < multipart_part_size_bytes_) {
    throw TileAiException(
        "vfs.tile.multipart_threshold_bytes must be greater than or "
        "equal to vfs.tile.multipart_part_size_bytes");
  }

  client_ = std::make_unique<TileAiClient>(server_url_, api_key_, workspace_);
  initialized_ = true;
}

void TileAi::ensure_initialized() const {
  if (!initialized_ || client_ == nullptr) {
    throw TileAiException(
        "TileAi backend is not initialized; set "
        "vfs.tile.server_url and vfs.tile.api_key and enable "
        "TILEDB_TILE_AI");
  }
}

TileAi::ParsedUri TileAi::parse_uri(const URI& uri) {
  std::string path = uri.to_string();
  const std::string prefix = "tile://";
  if (path.rfind(prefix, 0) != 0) {
    throw TileAiException("TileAi: URI must start with tile://");
  }
  // Two URI shapes share this parser:
  //   id-form        tile://{id}                                  — zero
  //   slashes
  //                                                                 after
  //                                                                 scheme
  //   hierarchical   tile://{teamspace_ref}/{name}[/rel]           — one or
  //   more
  //                                                                 slashes
  //
  // `teamspace_ref` is either the canonical teamspace id (`{label}-{uuid}`)
  // or the human-readable teamspace name. The server resolves either form;
  // name lookups are scoped by the caller's workspace memberships and 400
  // on ambiguity (multi-workspace API key, same teamspace name in two
  // workspaces, no `vfs.tile.workspace` pin).
  //
  // Path structure alone disambiguates id-form from hierarchical. No
  // prefix check on the id segment: tile IDs, teamspace IDs, and
  // teamspace names can all be arbitrary strings (a teamspace named
  // "TileDB" gets ID `tiledb-{uuid}`, identical in shape to a tile ID),
  // so any first-segment regex would either reject legitimate
  // hierarchical URIs or invite false positives. Existence is the
  // catalog's job — `canonicalize_resource` calls `lookup_resource` and
  // a non-existent ref surfaces a clean 404 there.
  std::string remainder = path.substr(prefix.size());
  // Strip a single trailing slash. TileDB normalises directory-style
  // URIs by appending `/` (e.g. `Array::open` probes `is_dir(uri + "/")`
  // before reading), and that trailing `/` doesn't change the resource
  // identity. Without this strip, `tile://{id}/` would parse as
  // hierarchical with an empty `name`, the catalog client would fire
  // `GET /api/v1/tiles/{id}/`, and Next.js's trailing-slash rewrite
  // would 308 — a redirect libcurl doesn't follow under `CURLOPT_POST`
  // semantics, manifesting as an opaque "GET … failed: HTTP 308" with
  // no useful signal for the caller.
  if (!remainder.empty() && remainder.back() == '/') {
    remainder.pop_back();
  }
  if (remainder.empty()) {
    // `tile://` (or `tile:///`) carries no resource handle at all — no
    // id to look up, no teamspace to scope by. Fail at the parser
    // boundary so callers don't spend a round-trip on a 404 they could
    // have caught locally.
    throw TileAiException(
        "TileAi: URI 'tile://' has no resource segment "
        "(expected tile://{id} or tile://{teamspace}/{name}[/{relative_key}])");
  }
  ParsedUri result;
  auto slash1 = remainder.find('/');
  if (slash1 == std::string::npos) {
    // Id-form: a single opaque resource handle. `array_id` and
    // `relative_key` stay empty until a caller hands the parsed URI to
    // `canonicalize_resource`, which fills them in from the lookup.
    result.id_form = true;
    result.id = remainder;
  } else {
    auto slash2 = remainder.find('/', slash1 + 1);
    if (slash2 == std::string::npos) {
      // "teamspace/name" with no relative key
      result.array_id = remainder;
    } else {
      // "teamspace/name/rel/key..."
      result.array_id = remainder.substr(0, slash2);
      result.relative_key = remainder.substr(slash2 + 1);
    }
  }
  return result;
}

TileAi::ParsedUri TileAi::canonicalize_resource(
    TileAi::ParsedUri parsed) const {
  if (client_ == nullptr) {
    throw TileAiException("TileAi: cannot canonicalize URI before init");
  }

  // Two paths into the catalog: by id (single-segment URI) or by name
  // (hierarchical URI). Both go through the same `try /tiles → /groups`
  // resolver on the server and return the same `ResourceLocator` shape,
  // so the only difference here is the cache key + which client method
  // we call. Cache is keyed on the original URI form so id-form and
  // hierarchical lookups for the same resource are stored separately
  // (they're effectively two different access keys; the cost of an
  // extra HTTP per form on first use is fine).
  const std::string cache_key = parsed.id_form ? parsed.id : parsed.array_id;

  ResourceLocator loc;
  {
    std::lock_guard<std::mutex> lock(id_cache_mutex_);
    auto it = id_cache_.find(cache_key);
    if (it != id_cache_.end()) {
      loc = it->second;
    }
  }
  if (loc.type.empty()) {
    // The two resolver methods are type-agnostic by construction (each
    // tries `/tiles` then falls through to `/groups`), so they don't
    // need an EntityType arg.
    if (parsed.id_form) {
      loc = client_->lookup_resource(parsed.id);
    } else {
      auto slash = parsed.array_id.find('/');
      if (slash == std::string::npos) {
        throw TileAiException(
            "TileAi: hierarchical URI missing teamspace/name "
            "separator: " +
            parsed.array_id);
      }
      const std::string ts = parsed.array_id.substr(0, slash);
      const std::string nm = parsed.array_id.substr(slash + 1);
      try {
        loc = client_->lookup_resource_by_name(ts, nm);
      } catch (const TileAiException& e) {
        if (e.http_status() != 404)
          throw;
        // Fall back to id-form: maybe `ts` is actually a stable resource
        // id and `nm` (plus any parsed relative_key) is the full
        // in-array path. Tile ids and teamspace ids share the same
        // `{label}-{uuid}` shape so the parser can't disambiguate by
        // inspection — we discover it by trying the catalog. This is
        // what lets `Array(ctx, "tile://{id}", READ)` work: TileDB
        // appends `/__schema/...`, `/__fragments/...` etc. to the
        // user-supplied URI when probing the array, and every one of
        // those sub-URIs lands here as hierarchical-shaped input that
        // resolves through this fallback.
        try {
          loc = client_->lookup_resource(ts);
        } catch (const TileAiException&) {
          throw e;  // original 404 — neither form resolves
        }
      }
    }
    std::lock_guard<std::mutex> lock(id_cache_mutex_);
    id_cache_[cache_key] = loc;
  }

  // Id-form fallback fix-up. Runs on every call (fresh lookup OR cache
  // hit) because the cache stores only the resolved `loc` and we need
  // to recompute `relative_key` from the parser's wrong-but-cacheable
  // split. Compare the parser's first segment against the resolved
  // teamspace id — when they diverge, the parser's "name" segment is
  // actually a relative-path prefix that must be promoted into
  // `relative_key`. (Without this on the cache-hit path, ls_with_sizes
  // re-canonicalizes and gets relative_key="" back, then iterates
  // listing paths as if listing the array root, emitting dir entries
  // pointing at `tile://{ts}/{name}/__schema` instead of file entries
  // pointing at `tile://{ts}/{name}/__schema/{schema-id}.tdb`.)
  if (!parsed.id_form) {
    auto slash = parsed.array_id.find('/');
    const std::string parsed_ts = parsed.array_id.substr(0, slash);
    if (parsed_ts != loc.teamspace_id) {
      const std::string extra = parsed.array_id.substr(slash + 1);
      parsed.relative_key = parsed.relative_key.empty() ?
                                extra :
                                (extra + "/" + parsed.relative_key);
    }
  }

  // Stamp the canonical hierarchical address (overwrites for id-form,
  // identity for hierarchical) and the resolved type. Sub-key (if any)
  // is preserved verbatim from `parse_uri`.
  parsed.array_id = loc.teamspace_id + "/" + loc.name;
  parsed.effective_entity_type = loc.type;
  return parsed;
}

std::string TileAi::cache_key(
    const std::string& array_id, const std::string& relative_key) {
  return array_id + "/" + relative_key;
}

std::string TileAi::get_read_url(
    const std::string& array_id,
    const std::string& relative_key,
    const std::string& type) const {
  ensure_initialized();
  std::string ck = cache_key(array_id, relative_key);
  auto now = std::chrono::system_clock::now();
  // Refresh cached presigned URLs this many seconds before they expire.
  auto safety_margin = std::chrono::seconds(5);

  {
    std::lock_guard<std::mutex> lock(url_cache_mutex_);
    auto it = url_cache_.find(ck);
    if (it != url_cache_.end()) {
      if (it->second.expires_at - safety_margin > now) {
        return it->second.url;
      }
      url_cache_.erase(it);
    }
  }

  // Cache miss. If the components for this array are already cached (e.g.,
  // populated by an earlier `file_size` / `ls` probe), expand the request to
  // every known key in one batch — the server's `presign_read` endpoint
  // accepts arrays of keys. This turns N+1 per-key roundtrips into one.
  // If components aren't cached, or the requested key isn't in them yet
  // (freshly written fragment), fall back to a single-key request.
  std::vector<std::string> keys_to_presign;
  auto collect = [&](const std::vector<ObjectEntry>& entries) {
    for (auto& e : entries)
      keys_to_presign.push_back(e.key);
  };
  {
    std::lock_guard<std::mutex> lock(components_cache_mutex_);
    if (type == "group") {
      auto it = group_components_cache_.find(array_id);
      if (it != group_components_cache_.end()) {
        collect(it->second.marker);
        collect(it->second.commits);
        collect(it->second.group_metadata);
        collect(it->second.other);
      }
    } else {
      auto it = components_cache_.find(array_id);
      if (it != components_cache_.end()) {
        collect(it->second.schema);
        collect(it->second.array_metadata);
        collect(it->second.commits);
        collect(it->second.other);
        for (auto& frag : it->second.fragments)
          for (auto& obj : frag.objects)
            keys_to_presign.push_back(obj.key);
      }
    }
  }

  bool requested_covered = false;
  for (auto& k : keys_to_presign) {
    if (k == relative_key) {
      requested_covered = true;
      break;
    }
  }
  if (!requested_covered) {
    keys_to_presign = {relative_key};
  }

  auto urls = client_->presign_read(type, array_id, keys_to_presign);

  if (urls.empty()) {
    throw TileAiException(
        "Server returned no presigned URL for key: " + relative_key);
  }

  std::string result;
  {
    std::lock_guard<std::mutex> lock(url_cache_mutex_);
    for (auto& u : urls) {
      url_cache_[cache_key(array_id, u.key)] = {u.url, u.expires_at};
      if (u.key == relative_key)
        result = u.url;
    }
  }

  if (result.empty()) {
    throw TileAiException(
        "Server returned no presigned URL for key: " + relative_key);
  }
  return result;
}

void TileAi::do_http_get(
    const std::string& url,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes,
    long* http_status,
    uint64_t* bytes_read) const {
  CURL* curl = curl_easy_init();
  if (!curl)
    throw TileAiException("Failed to initialize libcurl");

  ReadBuffer rb{buffer, 0, nbytes};

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_to_buffer);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &rb);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

  std::string range =
      std::to_string(offset) + "-" + std::to_string(offset + nbytes - 1);
  curl_easy_setopt(curl, CURLOPT_RANGE, range.c_str());

  CURLcode res = curl_easy_perform(curl);
  if (res != CURLE_OK) {
    std::string err = curl_easy_strerror(res);
    curl_easy_cleanup(curl);
    throw TileAiException("S3 GET failed: " + err);
  }

  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, http_status);
  curl_easy_cleanup(curl);
  *bytes_read = rb.written;
}

void TileAi::do_http_put(
    const std::string& url,
    const void* buffer,
    uint64_t nbytes,
    std::string* etag,
    long* http_status) const {
  CURL* curl = curl_easy_init();
  if (!curl)
    throw TileAiException("Failed to initialize libcurl");

  WriteSource ws{static_cast<const char*>(buffer), nbytes, 0};
  HeaderCapture hcap{"etag", ""};

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
  curl_easy_setopt(curl, CURLOPT_READFUNCTION, curl_read_from_buffer);
  curl_easy_setopt(curl, CURLOPT_READDATA, &ws);
  curl_easy_setopt(
      curl, CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(nbytes));
  curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, curl_header_cb);
  curl_easy_setopt(curl, CURLOPT_HEADERDATA, &hcap);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);

  struct curl_slist* headers = nullptr;
  headers = curl_slist_append(headers, "Expect:");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  std::string discard;
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_string);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &discard);

  CURLcode res = curl_easy_perform(curl);
  curl_slist_free_all(headers);

  if (res != CURLE_OK) {
    std::string err = curl_easy_strerror(res);
    curl_easy_cleanup(curl);
    throw TileAiException("S3 PUT failed: " + err);
  }

  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, http_status);
  curl_easy_cleanup(curl);

  if (etag)
    *etag = hcap.value;
}

uint64_t TileAi::read_impl(
    const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const {
  auto parsed = canonicalize_resource(parse_uri(uri));
  auto url = get_read_url(
      parsed.array_id, parsed.relative_key, parsed.effective_entity_type);

  long http_status = 0;
  uint64_t bytes_read = 0;
  do_http_get(url, offset, buffer, nbytes, &http_status, &bytes_read);

  if (http_status == 200 || http_status == 206) {
    return bytes_read;
  }

  if (http_status == 403) {
    LOG_WARN("Presigned URL expired for " + parsed.relative_key + ", retrying");
    {
      std::lock_guard<std::mutex> lock(url_cache_mutex_);
      url_cache_.erase(cache_key(parsed.array_id, parsed.relative_key));
    }
    url = get_read_url(
        parsed.array_id, parsed.relative_key, parsed.effective_entity_type);
    do_http_get(url, offset, buffer, nbytes, &http_status, &bytes_read);
    if (http_status == 200 || http_status == 206) {
      return bytes_read;
    }
  }

  throw TileAiException(
      "S3 GET failed with HTTP " + std::to_string(http_status) +
          " for key: " + parsed.relative_key,
      http_status);
}

std::vector<std::string> TileAi::ls_impl(const URI& uri) const {
  ensure_initialized();
  auto parsed = canonicalize_resource(parse_uri(uri));

  std::vector<std::string> paths;
  const auto& prefix = parsed.relative_key;
  auto collect = [&](const std::vector<ObjectEntry>& entries) {
    for (auto& e : entries) {
      if (prefix.empty() || e.key.substr(0, prefix.size()) == prefix)
        paths.push_back(e.key);
    }
  };

  if (parsed.effective_entity_type == "group") {
    GroupComponents components;
    {
      std::lock_guard<std::mutex> lock(components_cache_mutex_);
      auto it = group_components_cache_.find(parsed.array_id);
      if (it != group_components_cache_.end())
        components = it->second;
    }
    if (components.group_id.empty()) {
      try {
        components = client_->list_group_components(parsed.array_id);
      } catch (const TileAiException& e) {
        if (e.http_status() == 404)
          return {};
        throw;
      }
      std::lock_guard<std::mutex> lock(components_cache_mutex_);
      group_components_cache_[parsed.array_id] = components;
    }
    collect(components.marker);
    collect(components.commits);
    collect(components.group_metadata);
    collect(components.other);
  } else {
    ArrayComponents components;
    {
      std::lock_guard<std::mutex> lock(components_cache_mutex_);
      auto it = components_cache_.find(parsed.array_id);
      if (it != components_cache_.end())
        components = it->second;
    }
    if (components.array_id.empty()) {
      try {
        components = client_->list_array_components(parsed.array_id);
      } catch (const TileAiException& e) {
        if (e.http_status() == 404)
          return {};
        throw;
      }
      std::lock_guard<std::mutex> lock(components_cache_mutex_);
      components_cache_[parsed.array_id] = components;
    }
    collect(components.schema);
    collect(components.array_metadata);
    collect(components.commits);
    collect(components.other);
    for (auto& frag : components.fragments) {
      for (auto& obj : frag.objects) {
        if (prefix.empty() || obj.key.substr(0, prefix.size()) == prefix)
          paths.push_back(obj.key);
      }
    }
  }

  return paths;
}

uint64_t TileAi::file_size_impl(const URI& uri) const {
  ensure_initialized();
  auto parsed = canonicalize_resource(parse_uri(uri));

  uint64_t size = 0;
  auto search = [&](const std::vector<ObjectEntry>& entries) -> bool {
    for (auto& e : entries) {
      if (e.key == parsed.relative_key) {
        size = static_cast<uint64_t>(e.size_bytes);
        return true;
      }
    }
    return false;
  };

  if (parsed.effective_entity_type == "group") {
    GroupComponents components;
    {
      std::lock_guard<std::mutex> lock(components_cache_mutex_);
      auto it = group_components_cache_.find(parsed.array_id);
      if (it != group_components_cache_.end())
        components = it->second;
    }
    if (components.group_id.empty()) {
      try {
        components = client_->list_group_components(parsed.array_id);
      } catch (const TileAiException& e) {
        if (e.http_status() == 404) {
          throw TileAiException(
              "TileAi: file not found in components: " + parsed.relative_key,
              404);
        }
        throw;
      }
      std::lock_guard<std::mutex> lock(components_cache_mutex_);
      group_components_cache_[parsed.array_id] = components;
    }
    if (search(components.marker))
      return size;
    if (search(components.commits))
      return size;
    if (search(components.group_metadata))
      return size;
    if (search(components.other))
      return size;
    throw TileAiException(
        "TileAi: file not found in components: " + parsed.relative_key, 404);
  }

  ArrayComponents components;
  {
    std::lock_guard<std::mutex> lock(components_cache_mutex_);
    auto it = components_cache_.find(parsed.array_id);
    if (it != components_cache_.end())
      components = it->second;
  }

  if (components.array_id.empty()) {
    try {
      components = client_->list_array_components(parsed.array_id);
    } catch (const TileAiException& e) {
      if (e.http_status() == 404) {
        throw TileAiException(
            "TileAi: file not found in components: " + parsed.relative_key,
            404);
      }
      throw;
    }
    std::lock_guard<std::mutex> lock(components_cache_mutex_);
    components_cache_[parsed.array_id] = components;
  }

  if (search(components.schema))
    return size;
  if (search(components.array_metadata))
    return size;
  if (search(components.commits))
    return size;
  if (search(components.other))
    return size;
  for (auto& frag : components.fragments) {
    if (search(frag.objects))
      return size;
  }

  throw TileAiException(
      "TileAi: file not found in components: " + parsed.relative_key, 404);
}

void TileAi::write_impl(const URI& uri, const void* buffer, uint64_t nbytes) {
  ensure_initialized();
  auto parsed = canonicalize_resource(parse_uri(uri));

  std::string ck = cache_key(parsed.array_id, parsed.relative_key);
  std::lock_guard<std::mutex> lock(write_state_mutex_);

  auto it = multipart_state_.find(ck);

  if (it == multipart_state_.end()) {
    MultipartState state;
    state.array_id = parsed.array_id;
    state.relative_key = parsed.relative_key;
    state.entity_type = parsed.effective_entity_type;

    const auto* src = static_cast<const uint8_t*>(buffer);
    state.part_buffer.insert(state.part_buffer.end(), src, src + nbytes);

    if (state.part_buffer.size() >= multipart_threshold_bytes_) {
      auto create_result = client_->multipart_create(
          parsed.effective_entity_type, parsed.array_id, parsed.relative_key);

      state.upload_id = std::move(create_result.upload_id);
      state.next_part_url = std::move(create_result.parts[0]);
      state.next_part_number = 2;
      state.write_session_id = std::move(create_result.write_session_id);

      multipart_state_[ck] = std::move(state);
      auto& ms = multipart_state_[ck];
      while (ms.part_buffer.size() >= multipart_part_size_bytes_) {
        flush_part(ms);
      }
    } else {
      multipart_state_[ck] = std::move(state);
    }

    return;
  }

  auto& state = it->second;
  const auto* src = static_cast<const uint8_t*>(buffer);
  state.part_buffer.insert(state.part_buffer.end(), src, src + nbytes);

  if (state.upload_id.empty() &&
      state.part_buffer.size() >= multipart_threshold_bytes_) {
    auto create_result = client_->multipart_create(
        parsed.effective_entity_type, parsed.array_id, parsed.relative_key);
    state.upload_id = std::move(create_result.upload_id);
    state.next_part_url = std::move(create_result.parts[0]);
    state.next_part_number = 2;
    state.write_session_id = std::move(create_result.write_session_id);
  }

  if (!state.upload_id.empty()) {
    while (state.part_buffer.size() >= multipart_part_size_bytes_) {
      flush_part(state);
    }
  }
}

void TileAi::flush_part(MultipartState& state) {
  assert(!state.upload_id.empty());
  assert(!state.part_buffer.empty());

  uint64_t part_size = std::min(
      static_cast<uint64_t>(state.part_buffer.size()),
      multipart_part_size_bytes_);

  auto now = std::chrono::system_clock::now();
  // Refresh cached presigned URLs this many seconds before they expire.
  auto safety = std::chrono::seconds(5);

  std::string part_url;
  int part_number;

  if (!state.next_part_url.url.empty() &&
      state.next_part_url.expires_at - safety > now) {
    part_url = state.next_part_url.url;
    part_number = state.next_part_url.part_number;
  } else {
    int pn = state.completed_parts.empty() ?
                 1 :
                 state.completed_parts.back().part_number + 1;
    auto parts = client_->multipart_parts(
        state.entity_type,
        state.array_id,
        state.relative_key,
        state.upload_id,
        {pn});
    if (parts.empty())
      throw TileAiException("Server returned no presigned URL for part");
    part_url = parts[0].url;
    part_number = parts[0].part_number;
  }

  std::string etag;
  long http_status = 0;
  do_http_put(
      part_url, state.part_buffer.data(), part_size, &etag, &http_status);

  if (http_status == 403) {
    LOG_WARN(
        "Presigned upload URL expired for part " + std::to_string(part_number) +
        ", retrying");
    auto parts = client_->multipart_parts(
        state.entity_type,
        state.array_id,
        state.relative_key,
        state.upload_id,
        {part_number});
    if (parts.empty())
      throw TileAiException("Server returned no presigned URL on retry");
    do_http_put(
        parts[0].url, state.part_buffer.data(), part_size, &etag, &http_status);
  }

  if (http_status != 200) {
    throw TileAiException(
        "S3 UploadPart returned HTTP " + std::to_string(http_status),
        http_status);
  }

  state.completed_parts.emplace_back(part_number, std::move(etag));
  state.part_buffer.erase(
      state.part_buffer.begin(),
      state.part_buffer.begin() + static_cast<ptrdiff_t>(part_size));

  int next_pn = part_number + 1;
  state.next_part_number = next_pn + 1;
  try {
    auto next_parts = client_->multipart_parts(
        state.entity_type,
        state.array_id,
        state.relative_key,
        state.upload_id,
        {next_pn});
    if (!next_parts.empty()) {
      state.next_part_url = next_parts[0];
    } else {
      state.next_part_url = {};
    }
  } catch (const TileAiException&) {
    state.next_part_url = {};
  }
}

void TileAi::flush_impl(const URI& uri) {
  ensure_initialized();
  auto parsed = canonicalize_resource(parse_uri(uri));

  std::string ck = cache_key(parsed.array_id, parsed.relative_key);
  std::lock_guard<std::mutex> lock(write_state_mutex_);

  auto it = multipart_state_.find(ck);
  if (it == multipart_state_.end())
    return;

  auto& state = it->second;

  if (state.upload_id.empty()) {
    auto write_result = client_->presign_write(
        parsed.effective_entity_type, parsed.array_id, {parsed.relative_key});
    if (write_result.urls.empty())
      throw TileAiException("Server returned no presigned URL for simple PUT");

    std::string etag;
    long http_status = 0;
    do_http_put(
        write_result.urls[0].url,
        state.part_buffer.data(),
        state.part_buffer.size(),
        &etag,
        &http_status);

    if (http_status != 200)
      throw TileAiException(
          "S3 PUT failed with HTTP " + std::to_string(http_status),
          http_status);

    client_->commit_write_session(
        parsed.effective_entity_type,
        parsed.array_id,
        write_result.write_session_id,
        {},
        true);
    {
      std::lock_guard<std::mutex> clock(components_cache_mutex_);
      // Each id only ever lives in one cache; erasing from the wrong
      // one is a cheap no-op. Always erase from both so a write into a
      // group invalidates the group cache too — the dual caches are an
      // implementation detail of this backend, not the call-site's
      // concern.
      components_cache_.erase(parsed.array_id);
      group_components_cache_.erase(parsed.array_id);
    }

    multipart_state_.erase(it);
    return;
  }

  if (!state.part_buffer.empty()) {
    flush_part(state);
  }

  client_->multipart_complete(
      parsed.effective_entity_type,
      parsed.array_id,
      parsed.relative_key,
      state.upload_id,
      state.completed_parts);

  client_->commit_write_session(
      parsed.effective_entity_type,
      parsed.array_id,
      state.write_session_id,
      {},
      true);
  {
    std::lock_guard<std::mutex> clock(components_cache_mutex_);
    components_cache_.erase(parsed.array_id);
  }

  multipart_state_.erase(it);
}

bool TileAi::supports_uri(const URI& uri) const {
  return uri.is_tile();
}

void TileAi::create_dir(const URI& uri) const {
  // No-op for hierarchical tile:// URIs. S3 has no real directories,
  // and catalog registration of new arrays / groups happens through
  // TileAiClient (called from Array::create / Group::create directly),
  // not via the VFS. Validates that the URI is hierarchical and
  // rejects tile://{id} forms, which can only refer to existing
  // server-side ids.
  auto parsed = parse_uri(uri);
  if (parsed.id_form) {
    throw TileAiException(
        "TileAi: create_dir on tile://{id} URI is not supported "
        "(ids are server-generated; use the hierarchical form "
        "tile://{teamspace}/{name})");
  }
}

void TileAi::touch(const URI& uri) const {
  static const char empty = '\0';
  const_cast<TileAi*>(this)->write(uri, &empty, 0);
  const_cast<TileAi*>(this)->flush(uri);
}

bool TileAi::is_dir(const URI& uri) const {
  ParsedUri parsed;
  try {
    parsed = canonicalize_resource(parse_uri(uri));
  } catch (const TileAiException&) {
    // Either an unsupported scheme or a 404 from `lookup_resource` for
    // an id-form URI — neither is a "dir".
    return false;
  }
  if (parsed.relative_key.empty()) {
    if (client_ == nullptr)
      return false;
    try {
      if (parsed.effective_entity_type == "group") {
        client_->list_group_components(parsed.array_id);
      } else {
        client_->list_array_components(parsed.array_id);
      }
      return true;
    } catch (const TileAiException&) {
      return false;
    }
  }

  return !ls_with_sizes(uri).empty();
}

bool TileAi::is_file(const URI& uri) const {
  try {
    (void)file_size_impl(uri);
    return true;
  } catch (const TileAiException&) {
    return false;
  }
}

void TileAi::remove_dir(const URI&) const {
  // The current tile.ai server does not expose deletion APIs yet. TileDB
  // still calls remove_dir on some internal rollback paths during create/write
  // flows, so treat it as a best-effort no-op until server-side deletion is
  // implemented.
}

void TileAi::remove_file(const URI&) const {
  throw FilesystemException(
      "Removing files is not supported on the tile.ai filesystem backend.");
}

uint64_t TileAi::file_size(const URI& uri) const {
  return file_size_impl(uri);
}

std::vector<directory_entry> TileAi::ls_with_sizes(const URI& parent) const {
  auto paths = ls_impl(parent);
  auto parsed = canonicalize_resource(parse_uri(parent));

  std::string normalized_prefix = parsed.relative_key;
  if (!normalized_prefix.empty() && !normalized_prefix.ends_with('/')) {
    normalized_prefix.push_back('/');
  }

  auto try_file_size = [&](const URI& u) -> std::optional<uint64_t> {
    try {
      return file_size_impl(u);
    } catch (const TileAiException&) {
      return std::nullopt;
    }
  };

  std::vector<directory_entry> entries;
  std::unordered_set<std::string> seen_dirs;
  for (const auto& path : paths) {
    if (!parsed.relative_key.empty() && path == parsed.relative_key) {
      if (auto size = try_file_size(parent); size.has_value()) {
        entries.emplace_back(parent.to_string(), *size, false);
      }
      continue;
    }

    if (!normalized_prefix.empty()) {
      if (!path.starts_with(normalized_prefix)) {
        continue;
      }
    }

    const auto remainder = normalized_prefix.empty() ?
                               path :
                               path.substr(normalized_prefix.size());
    if (remainder.empty()) {
      continue;
    }

    const auto slash = remainder.find('/');
    if (slash == std::string::npos) {
      const auto full_path =
          normalized_prefix.empty() ? path : normalized_prefix + remainder;
      const auto file_uri = "tile://" + parsed.array_id + "/" + full_path;
      if (auto size = try_file_size(URI(file_uri)); size.has_value()) {
        entries.emplace_back(file_uri, *size, false);
      }
    } else {
      const auto dir_name = remainder.substr(0, slash);
      const auto dir_uri =
          "tile://" + parsed.array_id + "/" + normalized_prefix + dir_name;
      if (seen_dirs.insert(dir_uri).second) {
        entries.emplace_back(dir_uri, 0, true);
      }
    }
  }

  return entries;
}

uint64_t TileAi::read(
    const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const {
  return read_impl(uri, offset, buffer, nbytes);
}

void TileAi::write(
    const URI& uri, const void* buffer, uint64_t buffer_size, bool) {
  write_impl(uri, buffer, buffer_size);
}

void TileAi::flush(const URI& uri, bool) {
  flush_impl(uri);
}

}  // namespace tiledb::sm
