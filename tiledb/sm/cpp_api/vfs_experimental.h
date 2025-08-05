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
   * Typedef for ls callback function used to collect results from
   * ls_recursive_v2.
   *
   * If the callback returns True, the walk will continue. If False, the walk
   * will stop. If an error is thrown, the walk will stop and the error will be
   * propagated to the caller using std::throw_with_nested.
   *
   * @param path The path of a visited object for the relative filesystem.
   * @param object_size The size of the object at the current path.
   * @param is_dir True if the current result is a directory, else False.
   * @return True if the walk should continue, else false.
   */
  using LsCallbackV2 = std::function<bool(std::string_view, uint64_t, bool)>;

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
   * Typedef for ls inclusion predicate function used to check if a single
   * result should be included in the final results returned from
   * ls_recursive_v2.
   *
   * If the predicate returns True, the result will be included. If False, the
   * result will not be included. If an error is thrown, the walk will stop and
   * the error will be propagated.
   *
   * @param path The path of a visited object for the relative filesystem.
   * @param object_size The size of the object at the current path.
   * @param is_dir True if the object is a directory or prefix, else False.
   * @return True if the result should be included, else false.
   */
  using LsIncludeV2 = std::function<bool(std::string_view, uint64_t, bool)>;

  /**
   * Default typedef for objects collected by recursive ls, storing a vector of
   * pairs for each object path and size. This can be overridden by the client
   * to store results into a custom data structure using a custom callback.
   * @sa LsCallback
   */
  using LsObjects = std::vector<std::pair<std::string, uint64_t>>;

  /** Class to wrap C++ FilterPredicate for passing to the C API. */
  class CallbackWrapperCPP {
   public:
    /** Default constructor is deleted */
    CallbackWrapperCPP() = delete;

    /** Constructor */
    CallbackWrapperCPP(LsCallback cb)
        : cb_(std::move(cb)) {
      if (cb_ == nullptr) {
        throw std::logic_error("ls_recursive callback function cannot be null");
      }
    }

    CallbackWrapperCPP(LsCallbackV2 cb)
        : cb_v2_(std::move(cb)) {
      if (cb_v2_ == nullptr) {
        throw std::logic_error(
            "ls_recursive_v2 callback function cannot be null");
      }
    }

    /**
     * Operator to wrap the FilterPredicate used in the C++ API.
     *
     * @param path The path of the object.
     * @param size The size of the object in bytes.
     * @return True if the object should be included, False otherwise.
     */
    bool operator()(std::string_view path, uint64_t size) {
      return cb_(path, size);
    }

    /**
     * Operator to wrap the FilterPredicateV2 used in the C++ API.
     *
     * @param path The path of the object.
     * @param size The size of the object in bytes.
     * @return True if the object should be included, False otherwise.
     */
    bool operator()(std::string_view path, uint64_t size, bool is_dir) {
      return cb_v2_(path, size, is_dir);
    }

   private:
    LsCallback cb_;
    LsCallbackV2 cb_v2_;
  };

  /* ********************************* */
  /*       PUBLIC STATIC METHODS       */
  /* ********************************* */

#ifndef TILEDB_REMOVE_DEPRECATIONS
 private:
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
  TILEDB_DEPRECATED static int32_t ls_callback_wrapper(
      const char* path, size_t path_len, uint64_t object_size, void* data) {
    CallbackWrapperCPP* cb = static_cast<CallbackWrapperCPP*>(data);
    return (*cb)({path, path_len}, object_size);
  }

 public:
  /**
   * Recursively lists objects at the input URI, invoking the provided callback
   * on each entry gathered. The callback is passed the data pointer provided
   * on each invocation and is responsible for writing the collected results
   * into this structure. If the callback returns True, the walk will continue.
   * If False, the walk will stop. If an error is thrown, the walk will stop and
   * the error will be propagated to the caller using std::throw_with_nested.
   *
   * Currently LocalFS, S3, Azure, and GCS are supported. Objects and
   * directories will be collected for LocalFS. Only objects will be collected
   * for cloud storage backends such as S3, Azure, and GCS.
   *
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
  TILEDB_DEPRECATED static void ls_recursive(
      const Context& ctx,
      const VFS& vfs,
      const std::string& uri,
      LsCallback cb) {
    CallbackWrapperCPP wrapper(cb);
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
   * Currently only local filesystem, S3, Azure and GCS are supported, and the
   * `path` must be a valid URI for one of those filesystems. The results will
   * include objects and directories for LocalFS.
   *
   * Only objects will be collected for cloud storage backends when recursion is
   * enabled. If recursive if set to false, cloud storage backends will return
   * common prefix results.
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
  TILEDB_DEPRECATED static LsObjects ls_recursive_filter(
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
#endif  // TILEDB_REMOVE_DEPRECATIONS

  /**
   * Recursively lists objects at the input URI, invoking the provided callback
   * on each entry gathered. The callback is passed the data pointer provided
   * on each invocation and is responsible for writing the collected results
   * into this structure. If the callback returns True, the walk will continue.
   * If False, the walk will stop. If an error is thrown, the walk will stop and
   * the error will be propagated to the caller using std::throw_with_nested.
   *
   *
   * Currently LocalFS, S3, Azure, and GCS are supported. The results will
   * include objects and directories for all storage backends.
   *
   * The LsCallbackV2 used in this API adds an additional parameter for checking
   * if the current result is a directory. This can be used by the caller to
   * include or exclude directories as needed during traversal.
   *
   * @code{.c}
   * VFSExperimental::LsObjects ls_objects;
   * VFSExperimental::LsCallbackV2 cb = [&](const std::string_view& path,
   *                                      uint64_t size,
   *                                      bool is_dir) {
   *    ls_objects.emplace_back(path, size);
   *    return true;  // Continue traversal to next entry.
   * }
   *
   * VFSExperimental::ls_recursive_v2(ctx, vfs, "s3://bucket/foo", cb);
   * @endcode
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS instance to use.
   * @param uri The base URI to list results recursively.
   * @param cb The callback to invoke on each entry.
   */
  static void ls_recursive_v2(
      const Context& ctx,
      const VFS& vfs,
      const std::string& uri,
      LsCallbackV2 cb) {
    CallbackWrapperCPP wrapper(cb);
    ctx.handle_error(tiledb_vfs_ls_recursive_v2(
        ctx.ptr().get(),
        vfs.ptr().get(),
        uri.c_str(),
        ls_callback_wrapper_v2,
        &wrapper));
  }

  /**
   * Recursively lists objects at the input URI, invoking the provided callback
   * on each entry gathered. The callback should return true if the entry should
   * be included in the results and false otherwise. If no inclusion predicate
   * is provided, all results are returned.
   *
   * Currently only local filesystem, S3, Azure and GCS are supported, and the
   * `path` must be a valid URI for one of those filesystems. The results will
   * include objects and directories for all storage backends.
   *
   * The LsIncludeV2 inclusion predicate used in this API adds an additional
   * parameter for checking if the current result is a directory. This can be
   * used by the caller to include or exclude directories as needed.
   *
   * @code{.c}
   * VFSExperimental::LsIncludeV2 predicate = [](std::string_view path,
   *                                             uint64_t object_size,
   *                                             bool is_dir) {
   *   return path.find(".txt") != std::string::npos;
   * }
   * // Include only files with '.txt' extension using a custom predicate.
   * auto ret = VFSExperimental::ls_recursive_filter_v2(
   *    ctx, vfs, "s3://bucket/foo", predicate);
   *
   * // Optionally omit the predicate to include all paths collected.
   * auto all_paths = VFSExperimental::ls_recursive_filter_v2(
   *    ctx, vfs, "s3://bucket/foo");
   * @endcode
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS instance to use.
   * @param uri The base URI to list results recursively.
   * @param include Predicate function to check if a result should be included.
   * @return Vector of pairs for each object path and size.
   */
  static LsObjects ls_recursive_filter_v2(
      const Context& ctx,
      const VFS& vfs,
      const std::string& uri,
      std::optional<LsIncludeV2> include = std::nullopt) {
    LsObjects ls_objects;
    if (include.has_value()) {
      auto include_cb = include.value();
      ls_recursive_v2(
          ctx,
          vfs,
          uri,
          [&](std::string_view path, uint64_t size, bool is_dir) {
            if (include_cb(path, size, is_dir)) {
              ls_objects.emplace_back(path, size);
            }
            return true;
          });
    } else {
      ls_recursive_v2(
          ctx, vfs, uri, [&](std::string_view path, uint64_t size, bool) {
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
   * @param is_dir Whether or not the current object is a directory.
   * @param data Data passed to the callback used to store collected results.
   * @return 1 if the callback should continue to the next object, or 0 to stop
   *      traversal.
   * @sa tiledb_ls_callback_t
   */
  static int32_t ls_callback_wrapper_v2(
      const char* path,
      size_t path_len,
      uint64_t object_size,
      uint8_t is_dir,
      void* data) {
    CallbackWrapperCPP* cb = static_cast<CallbackWrapperCPP*>(data);
    return (*cb)({path, path_len}, object_size, is_dir);
  }
};
}  // namespace tiledb

#endif  // TILEDB_VFS_EXPERIMENTAL_H
