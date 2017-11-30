/**
 * @file   uri.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include "uri.h"
#include "utils.h"
#include "vfs.h"

#include <iostream>

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

URI::URI() {
  uri_ = "";
}

URI::URI(const std::string& path) {
  if (URI::is_posix(path))
    uri_ = VFS::abs_path(path);
  else if (URI::is_hdfs(path) || URI::is_s3(path))
    uri_ = path;
  else
    uri_ = "";
}

URI::~URI() = default;

/* ********************************* */
/*                API                */
/* ********************************* */

const char* URI::c_str() const {
  return uri_.c_str();
}

bool URI::is_invalid() const {
  return uri_.empty();
}

bool URI::is_posix(const std::string& path) {
  return utils::starts_with(path, "file:///") ||
         path.find("://") == std::string::npos;
}

bool URI::is_posix() const {
  return utils::starts_with(uri_, "file:///");
}

bool URI::is_hdfs(const std::string& path) {
  return utils::starts_with(path, "hdfs://");
}

bool URI::is_hdfs() const {
  return utils::starts_with(uri_, "hdfs://");
}

bool URI::is_s3(const std::string& path) {
  return utils::starts_with(path, "s3://") ||
         utils::starts_with(path, "http://") ||
         utils::starts_with(path, "https://");
}

bool URI::is_s3() const {
  return utils::starts_with(uri_, "s3://") ||
         utils::starts_with(uri_, "http://") ||
         utils::starts_with(uri_, "https://");
}

URI URI::join_path(const std::string& path) const {
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
  uint64_t pos = uri_.find_last_of('/');
  if (pos == std::string::npos)
    return URI();
  return URI(uri_.substr(0, pos));
}

std::string URI::to_path() const {
  if (is_posix())
    return uri_.substr(std::string("file://").size());

  if (is_hdfs() || is_s3())
    return uri_;

  // Error
  return "";
}

std::string URI::to_string() const {
  return uri_;
}

}  // namespace tiledb
