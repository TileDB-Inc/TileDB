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

#include <optional>
#include <string>
#include <vector>
#include "context.h"
#include "tiledb_experimental.h"
#include "vfs.h"

namespace tiledb {
class VFSExperimental {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /**
   * Typedef for ls callback function used to collect results from ls_recursive.
   *
   * If the callback returns True, the walk will continue. If False, the walk
   * will stop. If an error is thrown, the walk will stop and the error will be
   * propagated to the caller using std::throw_with_nested.
   *
   * @param path The path of a visited object for the relative filesystem.
   * @param object_size The size of the object at the current path.
   * @return True if the walk should continue, else false.
   */
  using LsCallback = std::function<bool(std::string_view, uint64_t)>;

  /**
   * Typedef for ls inclusion predicate function used to check if a single
   * result should be included in the final results returned from ls_recursive.
   *
   * If the predicate returns True, the result will be included. If False, the
   * result will not be included. If an error is thrown, the walk will stop and
   * the error will be propagated.
   *
   * @param path The path of a visited object for the relative filesystem.
   * @param object_size The size of the object at the current path.
   * @return True if the result should be included, else false.
   */
  using LsInclude = std::function<bool(std::string_view, uint64_t)>;

  /**
   * Default typedef for objects collected by recursive ls, storing a vector of
   * pairs for each object path and size. This can be overridden by the client
   * to store results into a custom data structure using a custom callback.
   * @sa LsCallback
   */
  using LsObjects = std::vector<std::pair<std::string, uint64_t>>;

  /* ********************************* */
  /*       PUBLIC STATIC METHODS       */
  /* ********************************* */

  /**
   * Recursively lists objects at the input URI, invoking the provided callback
   * on each entry gathered. The callback is passed the data pointer provided
   * on each invocation and is responsible for writing the collected results
   * into this structure. If the callback returns True, the walk will continue.
   * If False, the walk will stop. If an error is thrown, the walk will stop and
   * the error will be propagated to the caller using std::throw_with_nested.
   *
   * Currently only S3 is supported, and the `path` must be a valid S3 URI.
   *
   * @code{.c}
   * VFSExperimental::LsObjects ls_objects;
   * VFSExperimental::LsCallback cb = [&](const std::string_view& path,
   *                                      uint64_t size) {
   *    ls_objects.emplace_back(path, size);
   *    return true;  // Continue traversal to next entry.
   * }
   *
   * VFSExperimental::ls_recursive(ctx, vfs, "s3://bucket/foo", cb);
   * @endcode
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS instance to use.
   * @param uri The base URI to list results recursively.
   * @param cb The callback to invoke on each entry.
   */
  static void ls_recursive(
      const Context& ctx,
      const VFS& vfs,
      const std::string& uri,
      LsCallback cb) {
    tiledb::sm::CallbackWrapperCPP wrapper(cb);
    ctx.handle_error(tiledb_vfs_ls_recursive(
        ctx.ptr().get(),
        vfs.ptr().get(),
        uri.c_str(),
        ls_callback_wrapper,
        &wrapper));
  }

  /**
   * Recursively lists objects at the input URI, invoking the provided callback
   * on each entry gathered. The callback should return true if the entry should
   * be included in the results and false otherwise. If no inclusion predicate
   * is provided, all results are returned.
   *
   * Currently only S3 is supported, and the `path` must be a valid S3 URI.
   *
   * @code{.c}
   * VFSExperimental::LsInclude predicate = [](std::string_view path,
   *                                           uint64_t object_size) {
   *   return path.find(".txt") != std::string::npos;
   * }
   * // Include only files with '.txt' extension using a custom predicate.
   * auto ret = VFSExperimental::ls_recursive_filter(
   *    ctx, vfs, "s3://bucket/foo", predicate);
   *
   * // Optionally omit the predicate to include all paths collected.
   * auto all_paths = VFSExperimental::ls_recursive_filter(
   *    ctx, vfs, "s3://bucket/foo");
   * @endcode
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS instance to use.
   * @param uri The base URI to list results recursively.
   * @param include Predicate function to check if a result should be included.
   * @return Vector of pairs for each object path and size.
   */
  static LsObjects ls_recursive_filter(
      const Context& ctx,
      const VFS& vfs,
      const std::string& uri,
      std::optional<LsInclude> include = std::nullopt) {
    LsObjects ls_objects;
    if (include.has_value()) {
      auto include_cb = include.value();
      ls_recursive(ctx, vfs, uri, [&](std::string_view path, uint64_t size) {
        if (include_cb(path, size)) {
          ls_objects.emplace_back(path, size);
        }
        return true;
      });
    } else {
      ls_recursive(ctx, vfs, uri, [&](std::string_view path, uint64_t size) {
        ls_objects.emplace_back(path, size);
        return true;
      });
    }
    return ls_objects;
  }

 private:
  /* ********************************* */
  /*       PRIVATE STATIC METHODS      */
  /* ********************************* */

  /**
   * Callback function for invoking the C++ ls_recursive callback via C API.
   *
   * @param path The path of a visited object for the relative filesystem.
   * @param path_len The length of the path.
   * @param object_size The size of the object at the current path.
   * @param data Data passed to the callback used to store collected results.
   * @return 1 if the callback should continue to the next object, or 0 to stop
   *      traversal.
   * @sa tiledb_ls_callback_t
   */
  static int32_t ls_callback_wrapper(
      const char* path, size_t path_len, uint64_t object_size, void* data) {
    tiledb::sm::CallbackWrapperCPP* cb =
        static_cast<tiledb::sm::CallbackWrapperCPP*>(data);
    return (*cb)({path, path_len}, object_size);
  }
};
}  // namespace tiledb

#endif  // TILEDB_VFS_EXPERIMENTAL_H
