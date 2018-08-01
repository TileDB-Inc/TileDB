/**
 * @file   uri.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
  uri_ = "";
}

URI::URI(const std::string& path) {
  if (path.empty())
    uri_ = "";
  else if (URI::is_file(path))
    uri_ = VFS::abs_path(path);
  else if (URI::is_hdfs(path) || URI::is_s3(path))
    uri_ = path;
  else
    uri_ = "";

  if (uri_.length() > constants::uri_max_len) {
    LOG_ERROR("URI '" + uri_ + "' exceeds length limit.");
    uri_ = "";
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

const char* URI::c_str() const {
  return uri_.c_str();
}

bool URI::is_invalid() const {
  return uri_.empty();
}

bool URI::is_file(const std::string& path) {
  return utils::parse::starts_with(path, "file:///") ||
         path.find("://") == std::string::npos;
}

bool URI::is_file() const {
  return utils::parse::starts_with(uri_, "file:///");
}

bool URI::is_hdfs(const std::string& path) {
  return utils::parse::starts_with(path, "hdfs://");
}

bool URI::is_hdfs() const {
  return utils::parse::starts_with(uri_, "hdfs://");
}

bool URI::is_s3(const std::string& path) {
  return utils::parse::starts_with(path, "s3://") ||
         utils::parse::starts_with(path, "http://") ||
         utils::parse::starts_with(path, "https://");
}

bool URI::is_s3() const {
  return utils::parse::starts_with(uri_, "s3://") ||
         utils::parse::starts_with(uri_, "http://") ||
         utils::parse::starts_with(uri_, "https://");
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

std::string URI::last_path_part() const {
  return uri_.substr(uri_.find_last_of('/') + 1);
}

URI URI::parent() const {
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
  if (is_file(uri)) {
#ifdef _WIN32
    return Win::path_from_uri(uri);
#else
    return uri.substr(std::string("file://").size());
#endif
  }

  if (is_hdfs(uri) || is_s3(uri))
    return uri;

  // Error
  return "";
}

std::string URI::to_path() const {
  return to_path(uri_);
}

std::string URI::to_string() const {
  return uri_;
}

}  // namespace sm
}  // namespace tiledb
