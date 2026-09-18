/**
 * @file tile_ai_client.cc
 *
 * Implementation of the tile.ai HTTP client. Peer of `RestClient`.
 * Uses libcurl for HTTP and nlohmann/json for parsing.
 */

#include "tiledb/sm/rest/tile_ai_client.h"

#include "tiledb/sm/curl/curl_init.h"
#include "tiledb/sm/filesystem/tile_ai.h"
#include "tiledb/sm/filesystem/uri.h"

#include <curl/curl.h>

#include <ctime>
#include <iomanip>
#include <optional>
#include <sstream>
#include <stdexcept>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace tiledb::sm {

static size_t curl_write_cb(
    char* ptr, size_t size, size_t nmemb, void* userdata) {
  auto* buf = static_cast<std::string*>(userdata);
  buf->append(ptr, size * nmemb);
  return size * nmemb;
}

namespace {

// RAII wrapper holding one persistent CURL handle for the calling thread.
// The handle is reused across requests via `curl_easy_reset`, which keeps
// the underlying TCP/TLS connection pool alive between calls — eliminating
// the per-request connect handshake that dominates short HTTP calls to
// tile-ai (~5-30ms per call on local-loopback).
//
// Thread-local because libcurl easy handles are not thread-safe. One handle
// per thread means no locking and full keep-alive benefit per thread.
//
// The `tiledb::sm::curl::LibCurlInitializer` member ensures `curl_global_init`
// has run before any easy handle is constructed; the LibCurlInitializer
// uses `std::call_once` internally so multiple instances are safe.
// `curl_init.cc` enables the real `curl_global_init` whenever
// `TILEDB_SERIALIZATION` or `HAVE_TILE_AI` is defined.
struct CurlHandle {
  tiledb::sm::curl::LibCurlInitializer curl_inited_;
  CURL* handle = nullptr;
  CurlHandle()
      : handle(curl_easy_init()) {
  }
  ~CurlHandle() {
    if (handle)
      curl_easy_cleanup(handle);
  }
  CurlHandle(const CurlHandle&) = delete;
  CurlHandle& operator=(const CurlHandle&) = delete;
};

static CURL* thread_local_curl() {
  thread_local CurlHandle tl;
  return tl.handle;
}

// Convert the catalog's string entity-type (`"array"` or `"group"`,
// as it travels in `parsed.effective_entity_type`, multipart
// `state.entity_type`, and server JSON `loc.type`) into the typed
// enum. Throws on anything else so a typo surfaces at the call site
// rather than as an opaque 404 from the server. Used by the
// string-taking convenience overloads below.
EntityType entity_type_from_string(std::string_view type) {
  if (type == "array")
    return EntityType::Array;
  if (type == "group")
    return EntityType::Group;
  throw TileAiException(
      "Invalid entity type: '" + std::string(type) +
      "' (expected \"array\" or \"group\")");
}

}  // namespace

TileAiClient::TileAiClient(
    const std::string& server_url,
    const std::string& api_key,
    const std::string& workspace)
    : server_url_(server_url)
    , api_key_(api_key)
    , workspace_(workspace) {
  if (!server_url_.empty() && server_url_.back() == '/') {
    server_url_.pop_back();
  }
}

std::string TileAiClient::base_path(EntityType entity_type) {
  return entity_type == EntityType::Group ? "/api/v1/groups" : "/api/v1/tiles";
}

std::string TileAiClient::with_workspace_query(const std::string& path) const {
  if (workspace_.empty())
    return path;
  CURL* curl = thread_local_curl();
  char* escaped =
      curl ?
          curl_easy_escape(
              curl, workspace_.c_str(), static_cast<int>(workspace_.size())) :
          nullptr;
  std::string value = escaped ? std::string(escaped) : workspace_;
  if (escaped)
    curl_free(escaped);
  const char sep = (path.find('?') == std::string::npos) ? '?' : '&';
  return path + sep + "workspaceId=" + value;
}

std::string TileAiClient::http_request(
    const std::string& method,
    const std::string& path,
    const std::string& request_body,
    long expected_status) {
  CURL* curl = thread_local_curl();
  if (!curl) {
    throw TileAiException("Failed to initialize libcurl");
  }
  // Reset cached options from a prior request on this thread. The underlying
  // connection pool survives the reset, so keep-alive carries across calls.
  curl_easy_reset(curl);

  std::string url = server_url_ + with_workspace_query(path);
  std::string resp_buf;

  struct curl_slist* headers = nullptr;
  std::string auth_header = "Authorization: Bearer " + api_key_;
  headers = curl_slist_append(headers, auth_header.c_str());
  headers = curl_slist_append(headers, "Content-Type: application/json");
  headers = curl_slist_append(headers, "Accept: application/json");

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp_buf);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
  curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);

  if (method == "POST") {
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_body.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)request_body.size());
  } else if (method == "PUT") {
    // CUSTOMREQUEST + POSTFIELDS carries an in-memory body for any verb.
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_body.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)request_body.size());
  }

  CURLcode res = curl_easy_perform(curl);

  if (res != CURLE_OK) {
    std::string err = curl_easy_strerror(res);
    curl_slist_free_all(headers);
    throw TileAiException(method + " " + path + " failed: " + err);
  }

  long status_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status_code);

  curl_slist_free_all(headers);

  if (status_code != expected_status) {
    throw TileAiException(
        method + " " + path + " failed: HTTP " + std::to_string(status_code),
        status_code);
  }

  return resp_buf;
}

std::chrono::system_clock::time_point TileAiClient::parse_timestamp(
    const std::string& ts) {
  std::tm tm = {};
  std::istringstream ss(ts);
  ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
  auto tp = std::chrono::system_clock::from_time_t(timegm(&tm));
  return tp;
}

std::vector<TileInfo> TileAiClient::list_resources(
    std::string_view entity_type) {
  return list_resources(entity_type_from_string(entity_type));
}

std::vector<TileInfo> TileAiClient::list_resources(EntityType entity_type) {
  auto j = json::parse(http_request("GET", base_path(entity_type)));

  // The list endpoint envelopes the array under "tiles" for /api/v1/tiles
  // and "groups" for /api/v1/groups. Pick whichever the response carries.
  std::vector<TileInfo> resources;
  const auto& items = j.contains("groups") ? j["groups"] : j["tiles"];
  for (auto& item : items) {
    resources.emplace_back(
        item.value("id", std::string{}),
        item["base"].get<std::string>(),
        item["storage_uri"].get<std::string>(),
        item["created_at"].get<std::string>(),
        item["updated_at"].get<std::string>());
  }

  return resources;
}

ResourceLocator TileAiClient::lookup_resource(const std::string& id) {
  // Try the array endpoint first. The two endpoint families are gated on
  // `template` server-side, so a group id 404s on /api/v1/tiles/{id} and
  // we fall through. Any non-404 error (auth, permission, transport)
  // propagates immediately — those aren't disambiguated by retrying the
  // other family.
  auto try_endpoint =
      [&](const std::string& path,
          const std::string& expected_type) -> std::optional<ResourceLocator> {
    try {
      auto j = json::parse(http_request("GET", path));
      ResourceLocator loc;
      loc.id = j["id"].get<std::string>();
      loc.type = j["type"].get<std::string>();
      loc.teamspace_id = j["teamspace_id"].get<std::string>();
      loc.name = j["name"].get<std::string>();
      loc.storage_uri = j.value("storage_uri", std::string{});
      if (loc.type != expected_type) {
        throw TileAiException(
            "Resource lookup returned unexpected type '" + loc.type +
            "' from " + path);
      }
      return loc;
    } catch (const TileAiException& e) {
      if (e.http_status() == 404) {
        return std::nullopt;
      }
      throw;
    }
  };

  if (auto loc = try_endpoint("/api/v1/tiles/" + id, "array")) {
    return *loc;
  }
  if (auto loc = try_endpoint("/api/v1/groups/" + id, "group")) {
    return *loc;
  }
  throw TileAiException(
      "Resource '" + id + "' not found under /tiles or /groups", 404);
}

ResourceLocator TileAiClient::lookup_resource_by_name(
    const std::string& teamspace_id, const std::string& name) {
  // Same fall-through pattern as `lookup_resource(id)`, but against the
  // by-name endpoints. Their response shape differs from the by-id
  // endpoints (`{base, storage_uri, ...}` rather than `{id, type, ...}`),
  // so we split `base` back into teamspace/name and synthesise `type`
  // from whichever endpoint answered 200.
  auto try_endpoint = [&](const std::string& path,
                          const std::string& synthesised_type)
      -> std::optional<ResourceLocator> {
    try {
      auto j = json::parse(http_request("GET", path));
      ResourceLocator loc;
      loc.id = j.value("id", std::string{});
      loc.type = synthesised_type;
      const auto base = j.value("base", std::string{});
      auto slash = base.find('/');
      if (slash != std::string::npos) {
        loc.teamspace_id = base.substr(0, slash);
        loc.name = base.substr(slash + 1);
      }
      loc.storage_uri = j.value("storage_uri", std::string{});
      return loc;
    } catch (const TileAiException& e) {
      if (e.http_status() == 404) {
        return std::nullopt;
      }
      throw;
    }
  };

  const std::string tail = "/" + teamspace_id + "/" + name;
  if (auto loc = try_endpoint("/api/v1/tiles" + tail, "array")) {
    return *loc;
  }
  if (auto loc = try_endpoint("/api/v1/groups" + tail, "group")) {
    return *loc;
  }
  throw TileAiException(
      "Resource '" + teamspace_id + "/" + name +
          "' not found under /tiles or /groups",
      404);
}

std::vector<TeamspaceInfo> TileAiClient::list_teamspaces() {
  auto j = json::parse(http_request("GET", "/api/v1/teamspaces"));

  std::vector<TeamspaceInfo> teamspaces;
  for (auto& item : j["teamspaces"]) {
    teamspaces.emplace_back(
        item["id"].get<std::string>(),
        item["name"].get<std::string>(),
        item["workspace_id"].get<std::string>());
  }

  return teamspaces;
}

TileInfo TileAiClient::create_resource(
    std::string_view entity_type,
    const std::string& base,
    const std::string& storage_uri) {
  return create_resource(
      entity_type_from_string(entity_type), base, storage_uri);
}

TileInfo TileAiClient::create_resource(
    EntityType entity_type,
    const std::string& base,
    const std::string& storage_uri) {
  json req = {{"base", base}, {"storage_uri", storage_uri}};
  auto j = json::parse(
      http_request("POST", base_path(entity_type), req.dump(), 201));
  return TileInfo{
      j.value("id", std::string{}),
      j["base"].get<std::string>(),
      j["storage_uri"].get<std::string>(),
      j["created_at"].get<std::string>(),
      j["updated_at"].get<std::string>(),
  };
}

TileInfo TileAiClient::create_resource(
    std::string_view entity_type, const std::string& base) {
  return create_resource(entity_type_from_string(entity_type), base);
}

TileInfo TileAiClient::create_resource(
    EntityType entity_type, const std::string& base) {
  json req = {{"base", base}};
  auto j = json::parse(
      http_request("POST", base_path(entity_type), req.dump(), 201));
  return TileInfo{
      j.value("id", std::string{}),
      j["base"].get<std::string>(),
      j["storage_uri"].get<std::string>(),
      j["created_at"].get<std::string>(),
      j["updated_at"].get<std::string>(),
  };
}

// Local helper — both array and group components responses use the same
// `[{key, size_bytes}, ...]` shape for their leaf entries.
static std::vector<ObjectEntry> parse_object_entries(const json& arr) {
  std::vector<ObjectEntry> entries;
  for (auto& item : arr) {
    entries.emplace_back(
        item["key"].get<std::string>(), item["size_bytes"].get<int64_t>());
  }
  return entries;
}

ArrayComponents TileAiClient::list_array_components(
    const std::string& array_id) {
  std::string path = "/api/v1/tiles/" + array_id + "/components";
  auto j = json::parse(http_request("GET", path));

  ArrayComponents components;
  components.array_id = j["array_id"].get<std::string>();
  components.s3_uri = j["storage_uri"].get<std::string>();

  components.schema = parse_object_entries(j["schema"]);
  components.array_metadata = parse_object_entries(j["array_metadata"]);
  components.commits = parse_object_entries(j["commits"]);
  components.other = parse_object_entries(j["other"]);

  for (auto& frag : j["fragments"]) {
    PresignedFragmentInfo fi;
    fi.name = frag["name"].get<std::string>();
    fi.objects = parse_object_entries(frag["objects"]);
    components.fragments.push_back(std::move(fi));
  }

  return components;
}

GroupComponents TileAiClient::list_group_components(
    const std::string& group_id) {
  std::string path = "/api/v1/groups/" + group_id + "/components";
  auto j = json::parse(http_request("GET", path));

  GroupComponents components;
  components.group_id = j["group_id"].get<std::string>();
  components.s3_uri = j["storage_uri"].get<std::string>();
  components.marker = parse_object_entries(j["marker"]);
  components.commits = parse_object_entries(j["commits"]);
  components.group_metadata = parse_object_entries(j["group_metadata"]);
  components.other = parse_object_entries(j["other"]);

  return components;
}

std::vector<MemberEntry> TileAiClient::list_members(
    const std::string& group_id) {
  std::string path = "/api/v1/groups/" + group_id + "/members";
  auto j = json::parse(http_request("GET", path));

  std::vector<MemberEntry> members;
  for (auto& m : j["members"]) {
    MemberEntry entry;
    entry.uri = m["uri"].get<std::string>();
    // `name` is nullable in the server schema; coerce null → empty string.
    if (m.contains("name") && !m["name"].is_null()) {
      entry.name = m["name"].get<std::string>();
    }
    entry.type = m.value("type", std::string{"array"});
    entry.relative = m.value("relative", false);
    members.push_back(std::move(entry));
  }
  return members;
}

void TileAiClient::put_members(
    const std::string& group_id, const std::vector<MemberEntry>& members) {
  std::string path = "/api/v1/groups/" + group_id + "/members";

  json members_json = json::array();
  for (const auto& m : members) {
    json entry = {
        {"uri", m.uri},
        {"type", m.type},
        {"relative", m.relative},
    };
    if (!m.name.empty()) {
      entry["name"] = m.name;
    }
    members_json.push_back(std::move(entry));
  }

  json req = {{"members", members_json}};
  http_request("PUT", path, req.dump());
}

std::vector<PresignedUrl> TileAiClient::presign_read(
    std::string_view entity_type,
    const std::string& resource_id,
    const std::vector<std::string>& keys) {
  return presign_read(entity_type_from_string(entity_type), resource_id, keys);
}

std::vector<PresignedUrl> TileAiClient::presign_read(
    EntityType entity_type,
    const std::string& resource_id,
    const std::vector<std::string>& keys) {
  std::string path =
      base_path(entity_type) + "/" + resource_id + "/presign/read";
  json req = {{"keys", keys}};
  auto j = json::parse(http_request("POST", path, req.dump()));

  std::vector<PresignedUrl> urls;
  for (auto& item : j["urls"]) {
    urls.emplace_back(
        item["key"].get<std::string>(),
        item["url"].get<std::string>(),
        parse_timestamp(item["expires_at"].get<std::string>()));
  }

  return urls;
}

WritePresignResult TileAiClient::presign_write(
    std::string_view entity_type,
    const std::string& resource_id,
    const std::vector<std::string>& keys) {
  return presign_write(entity_type_from_string(entity_type), resource_id, keys);
}

WritePresignResult TileAiClient::presign_write(
    EntityType entity_type,
    const std::string& resource_id,
    const std::vector<std::string>& keys) {
  std::string path =
      base_path(entity_type) + "/" + resource_id + "/presign/write";
  json req = {{"keys", keys}};
  auto j = json::parse(http_request("POST", path, req.dump()));

  WritePresignResult result;
  result.write_session_id = j["write_session_id"].get<std::string>();
  for (auto& item : j["urls"]) {
    result.urls.emplace_back(
        item["key"].get<std::string>(),
        item["url"].get<std::string>(),
        parse_timestamp(item["expires_at"].get<std::string>()));
  }

  return result;
}

MultipartCreateResult TileAiClient::multipart_create(
    std::string_view entity_type,
    const std::string& resource_id,
    const std::string& key) {
  return multipart_create(
      entity_type_from_string(entity_type), resource_id, key);
}

MultipartCreateResult TileAiClient::multipart_create(
    EntityType entity_type,
    const std::string& resource_id,
    const std::string& key) {
  std::string path =
      base_path(entity_type) + "/" + resource_id + "/presign/multipart/create";
  json req = {{"key", key}};
  auto j = json::parse(http_request("POST", path, req.dump()));

  MultipartCreateResult result;
  result.upload_id = j["upload_id"].get<std::string>();
  result.key = j["key"].get<std::string>();
  result.write_session_id = j["write_session_id"].get<std::string>();
  for (auto& p : j["parts"]) {
    result.parts.emplace_back(
        p["part_number"].get<int>(),
        p["url"].get<std::string>(),
        parse_timestamp(p["expires_at"].get<std::string>()));
  }

  return result;
}

std::vector<PresignedPartUrl> TileAiClient::multipart_parts(
    std::string_view entity_type,
    const std::string& resource_id,
    const std::string& key,
    const std::string& upload_id,
    const std::vector<int>& part_numbers) {
  return multipart_parts(
      entity_type_from_string(entity_type),
      resource_id,
      key,
      upload_id,
      part_numbers);
}

std::vector<PresignedPartUrl> TileAiClient::multipart_parts(
    EntityType entity_type,
    const std::string& resource_id,
    const std::string& key,
    const std::string& upload_id,
    const std::vector<int>& part_numbers) {
  std::string path =
      base_path(entity_type) + "/" + resource_id + "/presign/multipart/parts";
  json req = {
      {"key", key}, {"upload_id", upload_id}, {"part_numbers", part_numbers}};
  auto j = json::parse(http_request("POST", path, req.dump()));

  std::vector<PresignedPartUrl> parts;
  for (auto& p : j["parts"]) {
    parts.emplace_back(
        p["part_number"].get<int>(),
        p["url"].get<std::string>(),
        parse_timestamp(p["expires_at"].get<std::string>()));
  }

  return parts;
}

void TileAiClient::multipart_complete(
    std::string_view entity_type,
    const std::string& resource_id,
    const std::string& key,
    const std::string& upload_id,
    const std::vector<CompletedPart>& parts) {
  multipart_complete(
      entity_type_from_string(entity_type), resource_id, key, upload_id, parts);
}

void TileAiClient::multipart_complete(
    EntityType entity_type,
    const std::string& resource_id,
    const std::string& key,
    const std::string& upload_id,
    const std::vector<CompletedPart>& parts) {
  std::string path = base_path(entity_type) + "/" + resource_id +
                     "/presign/multipart/complete";

  json parts_json = json::array();
  for (auto& p : parts) {
    parts_json.push_back({{"part_number", p.part_number}, {"etag", p.etag}});
  }

  json req = {{"key", key}, {"upload_id", upload_id}, {"parts", parts_json}};
  http_request("POST", path, req.dump());
}

void TileAiClient::multipart_abort(
    std::string_view entity_type,
    const std::string& resource_id,
    const std::string& key,
    const std::string& upload_id) {
  multipart_abort(
      entity_type_from_string(entity_type), resource_id, key, upload_id);
}

void TileAiClient::multipart_abort(
    EntityType entity_type,
    const std::string& resource_id,
    const std::string& key,
    const std::string& upload_id) {
  std::string path =
      base_path(entity_type) + "/" + resource_id + "/presign/multipart/abort";
  json req = {{"key", key}, {"upload_id", upload_id}};
  http_request("POST", path, req.dump());
}

void TileAiClient::commit_write_session(
    std::string_view entity_type,
    const std::string& resource_id,
    const std::string& write_session_id,
    const std::vector<std::string>& fragments_written,
    bool metadata_updated) {
  commit_write_session(
      entity_type_from_string(entity_type),
      resource_id,
      write_session_id,
      fragments_written,
      metadata_updated);
}

void TileAiClient::commit_write_session(
    EntityType entity_type,
    const std::string& resource_id,
    const std::string& write_session_id,
    const std::vector<std::string>& fragments_written,
    bool metadata_updated) {
  std::string path = base_path(entity_type) + "/" + resource_id + "/writes/" +
                     write_session_id + "/commit";
  json req = {
      {"fragments_written", fragments_written},
      {"metadata_updated", metadata_updated},
  };
  http_request("POST", path, req.dump());
}

// ----------------------------------------------------------------------
// tile_ai:: high-level URI-taking helpers
// ----------------------------------------------------------------------

namespace tile_ai {

namespace {

std::string base_from_uri(const URI& uri) {
  auto parsed = TileAi::parse_uri(uri);
  if (parsed.id_form) {
    throw TileAiException(
        "tile_ai: operation on tile://{id} URI is not supported "
        "(ids are server-generated; use the hierarchical form "
        "tile://{teamspace}/{name})");
  }
  return parsed.array_id;
}

// Try create_resource via @p client; on conflict fall back to
// list_resources on the same endpoint family to confirm the resource
// already exists at @p base. Idempotency wrapper used by both
// create_array and create_group.
void register_resource(
    TileAiClient& client,
    EntityType entity_type,
    const std::string& base,
    const std::string& create_storage_uri) {
  try {
    if (create_storage_uri.empty()) {
      client.create_resource(entity_type, base);
    } else {
      client.create_resource(entity_type, base, create_storage_uri);
    }
  } catch (const TileAiException&) {
    auto existing = client.list_resources(entity_type);
    for (const auto& e : existing) {
      if (e.base == base) {
        return;
      }
    }
    throw;
  }
}

}  // namespace

void create_array(
    TileAiClient& client,
    const URI& uri,
    const std::string& create_storage_uri) {
  register_resource(
      client, EntityType::Array, base_from_uri(uri), create_storage_uri);
}

void create_group(
    TileAiClient& client,
    const URI& uri,
    const std::string& create_storage_uri) {
  register_resource(
      client, EntityType::Group, base_from_uri(uri), create_storage_uri);
}

void put_members(
    TileAiClient& client,
    const URI& uri,
    const std::vector<GroupMember>& members) {
  auto base = base_from_uri(uri);
  std::vector<MemberEntry> wire_members;
  wire_members.reserve(members.size());
  for (const auto& m : members) {
    MemberEntry entry;
    entry.uri = m.uri;
    if (m.name.has_value()) {
      entry.name = *m.name;
    }
    entry.type = m.type;
    entry.relative = m.relative;
    wire_members.push_back(std::move(entry));
  }
  client.put_members(base, wire_members);
}

void commit_array_writes(
    TileAiClient& client,
    const URI& uri,
    const std::vector<std::string>& session_ids,
    const std::vector<std::string>& fragments_written,
    bool metadata_updated) {
  if (session_ids.empty()) {
    return;
  }
  auto base = base_from_uri(uri);
  for (const auto& session_id : session_ids) {
    client.commit_write_session(
        EntityType::Array,
        base,
        session_id,
        fragments_written,
        metadata_updated);
  }
}

}  // namespace tile_ai

}  // namespace tiledb::sm
