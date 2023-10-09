/**
 * @file vfs_experimental.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file declares the experimental C++ API for VFS.
 */

#ifndef TILEDB_VFS_EXPERIMENTAL_H
#define TILEDB_VFS_EXPERIMENTAL_H

#include <string>
#include <vector>
#include "context.h"
#include "tiledb_experimental.h"
#include "vfs.h"

namespace tiledb {
class VFSExperimental {
 private:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /**
   * Typedef for ls inclusion predicate function used to check if a single
   * result should be included in the final results returned from ls_recursive.
   *
   * @param path The path of a visited object for the relative filesystem.
   * @return True if the result should be included, else false.
   * @sa ls_include
   */
  typedef std::function<bool(const std::string_view&)> LsInclude;

  /**
   * Default typedef for objects collected by recursive ls, storing a vector of
   * pairs for each object path and size. This can be overridden by the client
   * to store results into a custom data structure using a custom callback.
   * @sa LsCallback
   */
  typedef std::vector<std::pair<std::string, uint64_t>> LsObjects;

  /* ********************************* */
  /*       PRIVATE STATIC METHODS      */
  /* ********************************* */

  /**
   * Default callback function to be used when invoking recursive ls. The
   * callback will cast 'data' to LsRecursiveData struct, which stores a
   * vector of strings for each path collected and a uint64 vector of file
   * sizes for the objects at each path.
   *
   * @param path The path of a visited object for the relative filesystem.
   * @param path_length The length of the path string.
   * @param file_size The size of the object at the path.
   * @param data Cast to LsRecursiveData struct to store paths and offsets.
   * @return `1` if the walk should continue to the next object, `0` if the walk
   *    should stop, and `-1` on error.
   * @sa LsCallback
   */
  static int ls_recursive_gather(
      const char* path, size_t path_length, uint64_t file_size, void* data) {
    auto ls_objects = static_cast<LsObjects*>(data);
    ls_objects->emplace_back(std::string_view(path, path_length), file_size);
    return 1;
  }

 public:
  /* ********************************* */
  /*       PUBLIC STATIC METHODS       */
  /* ********************************* */

  /**
   * Recursively lists objects at the input URI, invoking the provided callback
   * on each entry gathered. The callback is passed the data pointer provided
   * on each invocation and is responsible for writing the collected results
   * into this structure.
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS instance to use.
   * @param uri The base URI to list results recursively.
   * @param cb The callback to invoke on each entry.
   * @param data The data structure to store results in.
   */
  static void ls_recursive(
      const Context& ctx,
      const VFS& vfs,
      const std::string& uri,
      const LsCallback& cb,
      void* data) {
    ctx.handle_error(tiledb_vfs_ls_recursive(
        ctx.ptr().get(), vfs.ptr().get(), uri.c_str(), cb, data));
  }

  /**
   * Recursively lists objects at the input URI, invoking the provided callback
   * on each entry gathered. The callback should return true if the entry should
   * be included in the results. By default all entries are included using the
   * ls_include predicate, which simply returns true for all results.
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS instance to use.
   * @param uri The base URI to list results recursively.
   * @param include Predicate function to check if a result should be included.
   * @return Vector of pairs for each object path and size.
   */
  static std::vector<std::pair<std::string, uint64_t>> ls_recursive(
      const Context& ctx,
      const VFS& vfs,
      const std::string& uri,
      std::optional<LsInclude> include = std::nullopt) {
    LsObjects ls_objects;
    ctx.handle_error(tiledb_vfs_ls_recursive(
        ctx.ptr().get(),
        vfs.ptr().get(),
        uri.c_str(),
        ls_recursive_gather,
        &ls_objects));
    if (include.has_value()) {
      ls_objects.erase(
          std::remove_if(
              ls_objects.begin(),
              ls_objects.end(),
              [&](const std::pair<std::string, uint64_t>& p) {
                return !include.value()(p.first);
              }),
          ls_objects.end());
    }
    return ls_objects;
  }
};
}  // namespace tiledb

#endif  // TILEDB_VFS_EXPERIMENTAL_H
