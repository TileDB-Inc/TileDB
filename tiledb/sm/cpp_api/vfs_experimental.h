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
  using LsFileCallback = std::function<bool(std::string_view, uint64_t)>;
  using LsDirCallback = std::function<bool(std::string_view)>;

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
  using LsIncludeFile = std::function<bool(std::string_view, uint64_t)>;
  using LsIncludeDir = std::function<bool(std::string_view)>;

  /**
   * Default typedef for objects collected by recursive ls, storing a vector of
   * pairs for each object path and size. This can be overridden by the client
   * to store results into a custom data structure using a custom callback.
   * @sa LsCallback
   */
  using LsObjects = std::vector<std::pair<std::string, uint64_t>>;

  /** Class to wrap C++ FilePredicate for passing to the C API. */
  class CallbackWrapperCPP {
   public:
    /** Default constructor is deleted */
    CallbackWrapperCPP() = delete;

    /** Constructor */
    explicit CallbackWrapperCPP(LsFileCallback file_cb)
        : file_cb_(std::move(file_cb))
        , include_dirs_(false) {
      if (file_cb_ == nullptr) {
        throw std::logic_error(
            "ls_recursive files callback function cannot be null");
      }
    }

    /** Constructor */
    CallbackWrapperCPP(LsFileCallback file_cb, LsDirCallback dir_cb)
        : file_cb_(std::move(file_cb))
        , dir_cb_(std::move(dir_cb))
        , include_dirs_(true) {
      if (file_cb_ == nullptr) {
        throw std::logic_error(
            "ls_recursive files callback function cannot be null");
      } else if (dir_cb_ == nullptr) {
        throw std::logic_error(
            "ls_recursive directory callback function cannot be null");
      }
    }

    /**
     * Operator to wrap the FileFilter used in the C++ API.
     *
     * @param path The path of the object.
     * @param size The size of the object in bytes.
     * @return True if the object should be included, False otherwise.
     */
    bool operator()(std::string_view path, uint64_t size) {
      return file_cb_(path, size);
    }

    /**
     * Operator to wrap the DirectoryFilter used in the C++ API.
     *
     * @param path The path of the object.
     * @return True if the directory should be included, False otherwise.
     */
    bool operator()(std::string_view path) {
      return dir_cb_(path);
    }

   private:
    LsFileCallback file_cb_;
    LsDirCallback dir_cb_;
    bool include_dirs_;
  };

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
      LsFileCallback file_cb,
      std::optional<LsDirCallback> dir_cb = std::nullopt) {
    if (dir_cb.has_value()) {
      CallbackWrapperCPP wrapper(std::move(file_cb), dir_cb.value());
      ctx.handle_error(tiledb_vfs_ls_recursive_v2(
          ctx.ptr().get(),
          vfs.ptr().get(),
          uri.c_str(),
          ls_callback_file_wrapper,
          ls_callback_dir_wrapper,
          &wrapper));
    } else {
      CallbackWrapperCPP wrapper(std::move(file_cb));
      ctx.handle_error(tiledb_vfs_ls_recursive(
          ctx.ptr().get(),
          vfs.ptr().get(),
          uri.c_str(),
          ls_callback_file_wrapper,
          &wrapper));
    }
  }

  /**
   * Recursively lists objects at the input URI, invoking the provided callback
   * on each entry gathered. The callback should return true if the entry should
   * be included in the results and false otherwise. If no inclusion predicate
   * is provided, all results are returned.
   *
   * Currently only local filesystem, S3, Azure and GCS are supported, and the
   * `path` must be a valid URI for one of those filesystems.
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
      std::optional<LsIncludeFile> include_file = std::nullopt,
      std::optional<LsIncludeDir> include_dir = std::nullopt) {
    LsObjects ls_objects;
    // If either of the filters do not have a value, accept all entries.
    ls_recursive(
        ctx,
        vfs,
        uri,
        [&](std::string_view path, uint64_t size) {
          if (!include_file.has_value() || include_file.value()(path, size)) {
            ls_objects.emplace_back(path, size);
          }
          return true;
        },
        [&](std::string_view path) {
          if (!include_dir.has_value() || include_dir.value()(path)) {
            ls_objects.emplace_back(path, 0);
          }
          return true;
        });
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
  static int32_t ls_callback_file_wrapper(
      const char* path, size_t path_len, uint64_t object_size, void* data) {
    CallbackWrapperCPP* cb = static_cast<CallbackWrapperCPP*>(data);
    return (*cb)({path, path_len}, object_size);
  }

  /**
   * Callback function for invoking the C++ ls_recursive callback via C API.
   *
   * @param path The path of a visited directory for the relative filesystem.
   * @param path_len The length of the path.
   * @param data Data passed to the callback used to store collected results.
   * @return 1 if the callback should continue to the next object, or 0 to stop
   *      traversal.
   * @sa tiledb_ls_callback_t
   */
  static int32_t ls_callback_dir_wrapper(
      const char* path, size_t path_len, void* data) {
    CallbackWrapperCPP* cb = static_cast<CallbackWrapperCPP*>(data);
    return (*cb)({path, path_len});
  }
};
}  // namespace tiledb

#endif  // TILEDB_VFS_EXPERIMENTAL_H
