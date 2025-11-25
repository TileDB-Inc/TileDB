/**
 * @file   uri.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file implements class URI.
 */

#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/misc/constants.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/path_win.h"
#endif

#include <iostream>

using namespace tiledb::common;

namespace tiledb::sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

URI::URI() = default;

URI::URI(const char* path)
    : URI(path == nullptr ? std::string_view() : std::string_view(path)) {
}

URI::URI(std::string_view path, bool get_abs) {
  if (path.empty()) {
    uri_ = "";
  } else if (URI::is_file(path)) {
    if (get_abs) {
      uri_ = VFS::abs_path(path);
    } else {
      uri_ = path;
    }
  } else if (
      URI::is_s3(path) || URI::is_azure(path) || URI::is_gcs(path) ||
      URI::is_memfs(path) || URI::is_tiledb(path)) {
    uri_ = path;
  } else {
    uri_ = "";
  }
}

URI::URI(const char* path, const MustBeValidMarker&)
    : URI(path) {
  if (uri_.empty()) {
    throw std::invalid_argument(
        "Failed to construct valid URI. Given path is invalid.");
  }
}

URI::~URI() = default;

/* ********************************* */
/*                API                */
/* ********************************* */

URI URI::add_trailing_slash() const {
  if (uri_.empty()) {
    return URI("/");
  } else if (uri_.back() != '/') {
    return URI(CreateRaw, uri_ + '/');
  } else {
    return *this;
  }
}

URI URI::remove_trailing_slash() const {
  if (!uri_.empty() && uri_.back() == '/') {
    std::string uri_str = uri_;
    uri_str.pop_back();
    return URI(CreateRaw, std::move(uri_str));
  }

  return *this;
}

bool URI::empty() const {
  return uri_.empty();
}

const char* URI::c_str() const {
  return uri_.c_str();
}

bool URI::is_invalid() const {
  return uri_.empty();
}

bool URI::is_file(std::string_view path) {
#ifdef _WIN32
  return utils::parse::starts_with(path, "file://") ||
         path.find("://") == std::string::npos;
#else
  return utils::parse::starts_with(path, "file:///") ||
         path.find("://") == std::string::npos;
#endif
}

bool URI::contains(std::string_view str) const {
  return uri_.find(str, 0) != std::string::npos;
}

bool URI::is_file() const {
#ifdef _WIN32
  return is_file(uri_);
#else
  // Observed: semantics here differ from sibling
  // is_file(std::string_view path), here is missing
  // additional check using "://".
  return utils::parse::starts_with(uri_, "file:///");
#endif
}

bool URI::is_s3(std::string_view path) {
  return utils::parse::starts_with(path, "s3://") ||
         utils::parse::starts_with(path, "http://") ||
         utils::parse::starts_with(path, "https://");
}

bool URI::is_s3() const {
  return utils::parse::starts_with(uri_, "s3://") ||
         utils::parse::starts_with(uri_, "http://") ||
         utils::parse::starts_with(uri_, "https://");
}

bool URI::is_azure(std::string_view path) {
  return utils::parse::starts_with(path, "azure://");
}

bool URI::is_azure() const {
  return utils::parse::starts_with(uri_, "azure://");
}

bool URI::is_gcs(std::string_view path) {
  return utils::parse::starts_with(path, "gcs://") ||
         utils::parse::starts_with(path, "gs://");
}

bool URI::is_gcs() const {
  return utils::parse::starts_with(uri_, "gcs://") ||
         utils::parse::starts_with(uri_, "gs://");
}

bool URI::is_memfs(std::string_view path) {
  return utils::parse::starts_with(path, "mem://");
}

bool URI::is_memfs() const {
  return utils::parse::starts_with(uri_, "mem://");
}

bool URI::is_tiledb(std::string_view path) {
  return utils::parse::starts_with(path, "tiledb://");
}

bool URI::is_tiledb() const {
  return utils::parse::starts_with(uri_, "tiledb://");
}

bool URI::is_timestamped_name() const {
  std::string part = last_path_part();
  // __1_2_<32-digit-UUID> must be at minimum 38 characters long.
  if (!part.starts_with("__") || part.size() < 38) {
    return false;
  }

  // Separator between t1_t2.
  size_t t1_separator = part.find('_', 2);
  // Separator between t2_uuid.
  size_t t2_separator = part.find('_', t1_separator + 1);
  if (t1_separator == std::string::npos || t2_separator == std::string::npos) {
    return false;
  }

  // Validate the timestamps are formatted correctly.
  std::string t1 = part.substr(2, t1_separator - 2);
  std::string t2 =
      part.substr(t1_separator + 1, t2_separator - t1_separator - 1);
  for (const auto& t : {t1, t2}) {
    if (!std::all_of(
            t.begin(), t.end(), [](const char c) { return std::isdigit(c); })) {
      return false;
    }
  }

  // Separator between uuid[_v].
  size_t uuid_separator = part.find('_', t2_separator + 1);
  std::string uuid = part.substr(t2_separator + 1, uuid_separator);
  // UUIDs generated for timestamped names are 32 characters long.
  if (uuid.size() != 32) {
    return false;
  }

  // Version is optional and may not appear in files using a timestamped name.
  if (uuid_separator != std::string::npos) {
    std::string version = part.substr(uuid_separator + 1);
    if (version.size() > std::to_string(constants::format_version).size()) {
      return false;
    }
  }

  return true;
}

std::optional<size_t> URI::get_storage_component_index(
    size_t start_index) const {
  // Find '://' between path and array name. iff it exists
  auto storage_indicator = uri_.find("://", start_index + 1);
  if (storage_indicator == std::string::npos) {
    return std::nullopt;  // No storage component found
  }

  // Find the beginning of the storage path (e.g., s3, gs, azure)
  size_t storage_path_start = storage_indicator;
  while (storage_path_start > start_index &&
         uri_[storage_path_start - 1] != '/') {
    auto c = uri_[storage_path_start - 1];
    if (!(std::isalnum(c) || c == '+' || c == '-' || c == '.')) {
      // This is not a valid storage path.
      throw std::invalid_argument(
          "Invalid URI format; invalid character in storage URI scheme.");
    }
    --storage_path_start;
  }

  if (storage_path_start == storage_indicator) {
    // No storage path found, just the separator
    throw std::invalid_argument(
        "Invalid URI format; missing storage URI scheme before '://'.");
  }

  if (!std::isalpha(uri_[storage_path_start])) {
    throw std::invalid_argument(
        "Invalid URI format; storage URI scheme must begin with a letter.");
  }

  return storage_path_start;
}

Status URI::get_rest_components(
    bool legacy, RESTURIComponents* rest_components) const {
  const std::string prefix = "tiledb://";
  const std::string ns_component =
      legacy ? "tiledb://<namespace>" : "tiledb://<workspace>/<teamspace>";
  const auto error_st = Status_RestError(
      "Invalid array URI for REST service: '" + uri_ +
      "'; expected format is '" + ns_component + "/<array-name-or-uri>'.");

  if (!is_tiledb() || uri_.empty() || uri_.find(prefix) == std::string::npos ||
      uri_.size() <= prefix.size()) {
    return LOG_STATUS(error_st);
  }

  if (legacy) {
    // Extract '<namespace>' if we are talking to legacy REST.

    // Find '/' between namespace and array uri.
    auto slash = uri_.find('/', prefix.size());
    if (slash == std::string::npos) {
      return LOG_STATUS(error_st);
    }
    auto namespace_len = slash - prefix.size();
    auto array_len = uri_.size() - (slash + 1);
    if (namespace_len == 0 || array_len == 0) {
      return LOG_STATUS(error_st);
    }
    rest_components->server_namespace =
        uri_.substr(prefix.size(), namespace_len);
    rest_components->asset_storage = uri_.substr(slash + 1, array_len);
    rest_components->server_path = rest_components->asset_storage;
  } else {
    // Extract '<workspace>/<teamspace>' if we are talking to TileDB-Server.

    // Find '/' between workspace and teamspace.
    auto ws_slash = uri_.find('/', prefix.size());
    if (ws_slash == std::string::npos) {
      return LOG_STATUS(error_st);
    }
    // Find '/' between teamspace and array path.
    auto ts_slash = uri_.find('/', ws_slash + 1);
    if (ts_slash == std::string::npos) {
      return LOG_STATUS(error_st);
    }
    auto ws_len = ws_slash - prefix.size();
    auto ts_len = ts_slash - (ws_len + 1) - prefix.size();
    auto array_len = uri_.size() - (ts_len + 1);
    if (ws_len == 0 || ts_len == 0 || array_len == 0) {
      return LOG_STATUS(error_st);
    }

    std::string ws = uri_.substr(prefix.size(), ws_len);
    std::string ts = uri_.substr(ws_slash + 1, ts_len);
    rest_components->server_namespace = ws + "/" + ts;
    // If there is a trailing slash in the URI, this returns an empty string.
    auto asset_name = remove_trailing_slash().last_path_part();

    auto storage_component_index = get_storage_component_index(ts_slash + 1);
    if (!storage_component_index.has_value()) {
      // No storage component found, just the array name.
      rest_components->asset_storage = "";
      rest_components->server_path = uri_.substr(ts_slash + 1);
    } else {
      // Storage component found, extract it.
      rest_components->asset_storage =
          uri_.substr(storage_component_index.value());
      // Construct the server path, it's what's between teamspace and the
      // storage scheme
      rest_components->server_path =
          uri_.substr(
              ts_slash + 1, storage_component_index.value() - ts_slash - 1) +
          asset_name;
    }
  }

  return Status::Ok();
}

std::optional<URI> URI::get_fragment_name() const {
  auto to_find = "/" + constants::array_fragments_dir_name + "/";
  auto pos = uri_.find(to_find);
  if (pos == std::string::npos) {
    // Unable to find '/__fragments/' anywhere.
    return std::nullopt;
  }

  if (pos + to_find.size() == uri_.size()) {
    // URI is to the '/__fragments/' directory, no name present.
    return std::nullopt;
  }

  auto slash_pos = uri_.find("/", pos + to_find.size());
  if (slash_pos == pos + to_find.size()) {
    // URI has an empty fragment name with '/__fragments//'
    return std::nullopt;
  }

  if (slash_pos != std::string::npos) {
    return URI(CreateRaw, uri_.substr(0, slash_pos));
  }

  return *this;
}

URI URI::join_path(const std::string& path) const {
  // Check for empty strings.
  if (path.empty()) {
    return *this;
  } else if (uri_.empty()) {
    return URI(path);
  }

  if (uri_.back() == '/') {
    if (path.front() == '/') {
      return URI(CreateRaw, uri_ + path.substr(1, path.size()));
    }
    return URI(CreateRaw, uri_ + path);
  } else {
    if (path.front() == '/') {
      return URI(CreateRaw, uri_ + path);
    } else {
      return URI(CreateRaw, uri_ + "/" + path);
    }
  }
}

URI URI::join_path(const URI& uri) const {
  return join_path(uri.to_string());
}

URI URI::append_string(const std::string& str) const {
  return URI(CreateRaw, uri_ + str);
}

std::string URI::last_path_part() const {
  return uri_.substr(uri_.find_last_of('/') + 1);
}

std::string URI::last_two_path_parts() const {
  return uri_.substr(uri_.rfind('/', uri_.rfind('/') - 1) + 1);
}

URI URI::parent_path() const {
  auto pos = this->remove_trailing_slash().to_string().find_last_of('/');
  return URI(CreateRaw, uri_.substr(0, pos + 1));
}

std::string URI::to_path(const std::string& uri) {
  if (is_file(uri)) {
#ifdef _WIN32
    return path_win::path_from_uri(uri);
#else
    return uri.substr(std::string("file://").size());
#endif
  }

  if (is_memfs(uri)) {
    return uri.substr(std::string("mem://").size());
  }

  if (is_s3(uri) || is_azure(uri) || is_gcs(uri) || is_tiledb(uri))
    return uri;

  // Error
  return "";
}

std::string URI::backend_name() const {
  return uri_.substr(0, uri_.find_first_of(':'));
}

std::string URI::to_path() const {
  return to_path(uri_);
}

const std::string& URI::to_string() const {
  return uri_;
}

URI::operator std::string_view() const noexcept {
  return std::string_view(uri_);
}

bool URI::operator==(const URI& uri) const {
  return uri_ == uri.uri_;
}

std::strong_ordering URI::operator<=>(const URI& uri) const {
  return uri_ <=> uri.uri_;
}

}  // namespace tiledb::sm

namespace std {
size_t hash<tiledb::sm::URI>::operator()(const tiledb::sm::URI& val) const {
  return hash<string>()(val.to_string());
}
}  // namespace std
