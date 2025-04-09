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

URI::URI() {
  uri_ = "";
}

URI::URI(char* path)
    : URI((path == nullptr) ? std::string("") : std::string(path)) {
}

URI::URI(const char* path)
    : URI((path == nullptr) ? std::string("") : std::string(path)) {
}

URI::URI(std::string_view path) {
  if (path.empty())
    uri_ = "";
  else if (URI::is_file(path))
    uri_ = VFS::abs_path(path);
  else if (
      URI::is_s3(path) || URI::is_azure(path) || URI::is_gcs(path) ||
      URI::is_memfs(path) || URI::is_tiledb(path))
    uri_ = path;
  else
    uri_ = "";
}

URI::URI(std::string_view path, const bool& get_abs) {
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
    return URI(uri_ + '/');
  } else {
    return URI(uri_);
  }
}

URI URI::remove_trailing_slash() const {
  if (!uri_.empty() && uri_.back() == '/') {
    std::string uri_str = uri_;
    uri_str.pop_back();
    return URI(uri_str);
  }

  return URI(uri_);
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

Status URI::get_rest_components(
    std::string* array_namespace, std::string* array_uri, bool legacy) const {
  const std::string prefix = "tiledb://";
  const std::string ns_component =
      legacy ? "<namespace>" : "<workspace>/<teamspace>";
  const auto error_st = Status_RestError(
      "Invalid array URI for REST service; expected format is "
      "'tiledb://" +
      ns_component +
      "/<array-name>' or "
      "'tiledb://" +
      ns_component + "/<array-uri>'.");

  if (!is_tiledb() || uri_.empty() || uri_.find(prefix) == std::string::npos ||
      uri_.size() <= prefix.size()) {
    return LOG_STATUS(error_st);
  }

  if (legacy) {
    // Extract '<namespace>' if we are using a 3.0 REST server.

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
    *array_namespace = uri_.substr(prefix.size(), namespace_len);
    *array_uri = uri_.substr(slash + 1, array_len);
  } else {
    // Extract '<workspace>/<teamspace>' if we are using a 3.0 REST server.

    // Find '/' between workspace and teamspace.
    auto ws_slash = uri_.find('/', prefix.size());
    if (ws_slash == std::string::npos) {
      return LOG_STATUS(error_st);
    }
    // Find '/' between teamspace and array uri.
    auto ts_slash = uri_.find('/', ws_slash + 1);
    if (ts_slash == std::string::npos) {
      return LOG_STATUS(error_st);
    }
    auto ws_len = ws_slash - prefix.size();
    auto ts_len = ts_slash - (ws_len + 1) - prefix.size();
    auto array_len = uri_.size() - (ts_len + 1);

    std::string ws = uri_.substr(prefix.size(), ws_len);
    std::string ts = uri_.substr(ws_slash + 1, ts_len);
    std::string arr_uri = uri_.substr(ts_slash + 1, array_len);
    *array_namespace = ws + "/" + ts;
    *array_uri = arr_uri;
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
    return URI(uri_.substr(0, slash_pos));
  }

  return URI(uri_);
}

URI URI::join_path(const std::string& path) const {
  // Check for empty strings.
  if (path.empty()) {
    return URI(uri_);
  } else if (uri_.empty()) {
    return URI(path);
  }

  if (uri_.back() == '/') {
    if (path.front() == '/') {
      return URI(uri_ + path.substr(1, path.size()));
    }
    return URI(uri_ + path);
  } else {
    if (path.front() == '/') {
      return URI(uri_ + path);
    } else {
      return URI(uri_ + "/" + path);
    }
  }
}

URI URI::join_path(const URI& uri) const {
  return join_path(uri.to_string());
}

std::string URI::last_path_part() const {
  return uri_.substr(uri_.find_last_of('/') + 1);
}

std::string URI::last_two_path_parts() const {
  return uri_.substr(uri_.rfind('/', uri_.rfind('/') - 1) + 1);
}

URI URI::parent_path() const {
  auto pos = this->remove_trailing_slash().to_string().find_last_of('/');
  return URI(uri_.substr(0, pos + 1));
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
  if (is_tiledb(uri_)) {
    return "";
  } else {
    return uri_.substr(0, uri_.find_first_of(':'));
  }
}

std::string URI::to_path() const {
  return to_path(uri_);
}

std::string URI::to_string() const {
  return uri_;
}

URI::operator std::string_view() const noexcept {
  return std::string_view(uri_);
}

bool URI::operator==(const URI& uri) const {
  return uri_ == uri.uri_;
}

bool URI::operator!=(const URI& uri) const {
  return !operator==(uri);
}

bool URI::operator<(const URI& uri) const {
  return uri_ < uri.uri_;
}

bool URI::operator>(const URI& uri) const {
  return uri_ > uri.uri_;
}

}  // namespace tiledb::sm
