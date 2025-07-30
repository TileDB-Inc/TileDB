/**
 * @file   local.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB, Inc.
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
 * This file includes definitions of the base Local VFS functions.
 */

#include "local.h"

#include <filesystem>
#include <sstream>

#include "tiledb/common/logger.h"

using namespace tiledb::common;

namespace tiledb::sm {
LsObjects LocalFilesystem::ls_filtered(
    const URI& parent, FileFilter file_filter, bool recursive) const {
  /*
   * The input URI was useful to the top-level VFS to identify this is a
   * regular filesystem path, but we don't need the "file://" qualifier
   * anymore and can reason with unqualified strings for the rest of the
   * function.
   */
  const auto parentstr = parent.to_path();

  LsObjects qualifyingPaths;

  // awkward way of iterating, avoids bug in OSX
  auto begin = std::filesystem::recursive_directory_iterator(parentstr);
  auto end = std::filesystem::recursive_directory_iterator();

  for (auto iter = begin; iter != end; ++iter) {
    auto& entry = *iter;
    const auto abspath = entry.path().string();
    const auto absuri = URI(abspath);
    if (entry.is_directory()) {
      if (file_filter(absuri, 0)) {
        qualifyingPaths.push_back(
            std::make_pair(tiledb::sm::URI(abspath).to_string(), 0));
        if (!recursive) {
          iter.disable_recursion_pending();
        }
      } else {
        /* do not descend into directories which don't qualify */
        iter.disable_recursion_pending();
      }
    } else {
      /*
       * A leaf of the filesystem
       * (or symbolic link - split to a separate case if we want to descend into
       * them)
       */
      if (file_filter(absuri, entry.file_size())) {
        qualifyingPaths.push_back(
            std::make_pair(absuri.to_string(), entry.file_size()));
      }
    }
  }

  return qualifyingPaths;
}

void LocalFilesystem::copy_file(const URI& old_uri, const URI& new_uri) const {
  std::filesystem::copy_file(
      old_uri.to_path(),
      new_uri.to_path(),
      std::filesystem::copy_options::overwrite_existing);
}

void LocalFilesystem::copy_dir(const URI& old_uri, const URI& new_uri) const {
  std::filesystem::copy(
      old_uri.to_path(),
      new_uri.to_path(),
      std::filesystem::copy_options::overwrite_existing |
          std::filesystem::copy_options::recursive);
}

Status LocalFilesystem::ensure_directory(const std::string& path) {
  std::filesystem::path p{path};
  if (p.has_parent_path()) {
    std::error_code ec;
    std::filesystem::create_directories(p.parent_path(), ec);
    if (ec) {
      std::stringstream ss;
      ss << "Cannot create parent directories of '" << path << "' (" << ec
         << ")";
      return LOG_STATUS(Status_IOError(ss.str()));
    }
  }
  return Status::Ok();
}
}  // namespace tiledb::sm
