/**
 * @file   uri.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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

#include "tiledb/sm/misc/uri.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#endif

#include <iostream>

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

URI::URI() {
  std::cerr << "[DEBUG] URI 1" << std::endl;
  uri_ = "";
}

URI::URI(char* path)
    : URI((path == nullptr) ? std::string("") : std::string(path)) {
  std::cerr << "[DEBUG] URI 2" << std::endl;
}

URI::URI(const char* path)
    : URI((path == nullptr) ? std::string("") : std::string(path)) {
  std::cerr << "[DEBUG] URI 3" << std::endl;
}

URI::URI(const std::string& path) {
  std::cerr << "[DEBUG] URI 4" << std::endl;
  if (path.empty()) {
    std::cerr << "[DEBUG] URI 5" << std::endl;
    uri_ = "";
  } else if (URI::is_file(path)) {
    std::cerr << "[DEBUG] URI 6" << std::endl;
    uri_ = VFS::abs_path(path);
    std::cerr << "[DEBUG] URI 7" << std::endl;
  } else if (
      URI::is_hdfs(path) || URI::is_s3(path) || URI::is_azure(path) ||
      URI::is_gcs(path) || URI::is_tiledb(path)) {
    std::cerr << "[DEBUG] URI 8" << std::endl;
    uri_ = path;
    std::cerr << "[DEBUG] URI 9" << std::endl;
  } else {
    uri_ = "";
  }

  std::cerr << "[DEBUG] URI 10" << std::endl;
}

URI::~URI() = default;

/* ********************************* */
/*                API                */
/* ********************************* */

URI URI::add_trailing_slash() const {
  std::cerr << "[DEBUG] URI add_trailing_slash" << std::endl;
  if (uri_.empty()) {
    return URI("/");
  } else if (uri_.back() != '/') {
    return URI(uri_ + '/');
  } else {
    return URI(uri_);
  }
}

URI URI::remove_trailing_slash() const {
  std::cerr << "[DEBUG] URI remove_trailing_slash" << std::endl;
  if (uri_.back() == '/') {
    std::string uri_str = uri_;
    uri_str.pop_back();
    return URI(uri_str);
  }

  return URI(uri_);
}

const char* URI::c_str() const {
  std::cerr << "[DEBUG] URI c_str, size: " << uri_.size() << std::endl;
  return uri_.c_str();
}

bool URI::is_invalid() const {
  std::cerr << "[DEBUG] URI is_invalid, size: " << uri_.size() << std::endl;
  return uri_.empty();
}

bool URI::is_file(const std::string& path) {
  std::cerr << "[DEBUG] URI is_file, size: " << path.size() << std::endl;
  return utils::parse::starts_with(path, "file:///") ||
         path.find("://") == std::string::npos;
}

bool URI::is_file() const {
  std::cerr << "[DEBUG] URI is_file 2, size: " << uri_.size() << std::endl;
  return utils::parse::starts_with(uri_, "file:///");
}

bool URI::is_hdfs(const std::string& path) {
  std::cerr << "[DEBUG] URI is_hdfs 1, size: " << path.size() << std::endl;
  return utils::parse::starts_with(path, "hdfs://");
}

bool URI::is_hdfs() const {
  std::cerr << "[DEBUG] URI is_hdfs 2, size: " << uri_.size() << std::endl;
  return utils::parse::starts_with(uri_, "hdfs://");
}

bool URI::is_s3(const std::string& path) {
  std::cerr << "[DEBUG] URI is_s3 1, size: " << path.size() << std::endl;
  return utils::parse::starts_with(path, "s3://") ||
         utils::parse::starts_with(path, "http://") ||
         utils::parse::starts_with(path, "https://");
}

bool URI::is_s3() const {
  std::cerr << "[DEBUG] URI is_s3 2, size: " << uri_.size() << std::endl;
  return utils::parse::starts_with(uri_, "s3://") ||
         utils::parse::starts_with(uri_, "http://") ||
         utils::parse::starts_with(uri_, "https://");
}

bool URI::is_azure(const std::string& path) {
  std::cerr << "[DEBUG] URI is_azure 1, size: " << path.size() << std::endl;
  return utils::parse::starts_with(path, "azure://");
}

bool URI::is_azure() const {
  std::cerr << "[DEBUG] URI is_azure 2, size: " << uri_.size() << std::endl;
  return utils::parse::starts_with(uri_, "azure://");
}

bool URI::is_gcs(const std::string& path) {
  std::cerr << "[DEBUG] URI is_gcs 1, size: " << path.size() << std::endl;
  return utils::parse::starts_with(path, "gcs://");
}

bool URI::is_gcs() const {
  std::cerr << "[DEBUG] URI is_gcs 2, size: " << uri_.size() << std::endl;
  return utils::parse::starts_with(uri_, "gcs://");
}

bool URI::is_tiledb(const std::string& path) {
  return utils::parse::starts_with(path, "tiledb://");
}

bool URI::is_tiledb() const {
  return utils::parse::starts_with(uri_, "tiledb://");
}

Status URI::get_rest_components(
    std::string* array_namespace, std::string* array_uri) const {
  const std::string prefix = "tiledb://";
  const auto error_st = Status::RestError(
      "Invalid array URI for REST service; expected format is "
      "'tiledb://<namespace>/<array-name>' or "
      "'tiledb://<namespace>/<array-uri>'.");

  if (!is_tiledb() || uri_.empty() || uri_.find(prefix) == std::string::npos ||
      uri_.size() <= prefix.size())
    return LOG_STATUS(error_st);

  // Find '/' between namespace and array uri.
  auto slash = uri_.find('/', prefix.size());
  if (slash == std::string::npos)
    return LOG_STATUS(error_st);

  auto namespace_len = slash - prefix.size();
  auto array_len = uri_.size() - (slash + 1);
  if (namespace_len == 0 || array_len == 0)
    return LOG_STATUS(error_st);

  *array_namespace = uri_.substr(prefix.size(), namespace_len);
  *array_uri = uri_.substr(slash + 1, array_len);

  return Status::Ok();
}

URI URI::join_path(const std::string& path) const {
  std::cerr << "[DEBUG] URI join_path, size: " << path.size() << std::endl;
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

std::string URI::last_path_part() const {
  std::cerr << "[DEBUG] URI last_path_part, size: " << uri_.size() << std::endl;
  return uri_.substr(uri_.find_last_of('/') + 1);
}

URI URI::parent() const {
  std::cerr << "[DEBUG] URI parent, size: " << uri_.size() << std::endl;
  if (uri_.empty())
    return URI();

  auto uri = uri_;
  if (uri.back() == '/')
    uri.pop_back();
  uint64_t pos = uri.find_last_of('/');
  if (pos == std::string::npos)
    return URI();
  return URI(uri_.substr(0, pos));
}

std::string URI::to_path(const std::string& uri) {
  std::cerr << "[DEBUG] URI to_path 1, size: " << uri.size() << std::endl;
  if (is_file(uri)) {
#ifdef _WIN32
    return Win::path_from_uri(uri);
#else
    return uri.substr(std::string("file://").size());
#endif
  }

  if (is_hdfs(uri) || is_s3(uri) || is_azure(uri) || is_gcs(uri) ||
      is_tiledb(uri))
    return uri;

  // Error
  return "";
}

std::string URI::to_path() const {
  std::cerr << "[DEBUG] URI to_path 2, size: " << uri_.size() << std::endl;
  return to_path(uri_);
}

std::string URI::to_string() const {
  return uri_;
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

}  // namespace sm
}  // namespace tiledb
