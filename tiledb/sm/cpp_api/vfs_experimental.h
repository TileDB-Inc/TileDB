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
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /**
   * Typedef for ls callback function used to collect results from C++ API.
   *
   * @param path The path of a visited object for the relative filesystem.
   * @param object_size The size of the object in bytes.
   * @return True if the callback should continue to next object, else false.
   *    If an error is thrown, the callback will stop and the error will be
   *    propagated to the caller using std::throw_with_nested.
   */
  typedef std::function<bool(const std::string_view&, uint64_t)> LsCallback;

  /**
   * Typedef for ls inclusion predicate function used to check if a single
   * result should be included in the final results returned from ls_recursive.
   *
   * @param path The path of a visited object for the relative filesystem.
   * @return True if the result should be included, else false.
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
      LsCallback cb) {
    CallbackWrapper wrapper(cb);
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
   * be included in the results. If no inclusion predicate is provided, all
   * results are returned.
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS instance to use.
   * @param uri The base URI to list results recursively.
   * @param include Predicate function to check if a result should be included.
   * @return Vector of pairs for each object path and size.
   */
  static LsObjects ls_recursive(
      const Context& ctx,
      const VFS& vfs,
      const std::string& uri,
      std::optional<LsInclude> include = std::nullopt) {
    LsObjects ls_objects;
    if (include.has_value()) {
      auto include_cb = include.value();
      ls_recursive(
          ctx, vfs, uri, [&](const std::string_view& path, uint64_t size) {
            if (include_cb(path)) {
              ls_objects.emplace_back(path, size);
            }
            return true;
          });
    } else {
      ls_recursive(
          ctx, vfs, uri, [&](const std::string_view& path, uint64_t size) {
            ls_objects.emplace_back(path, size);
            return true;
          });
    }
    return ls_objects;
  }

 private:
  /** Private class to wrap C++ callback for passing to the C API. */
  class CallbackWrapper {
   public:
    CallbackWrapper(LsCallback& cb)
        : cb_(cb) {
    }

    bool operator()(const std::string_view& path, uint64_t size) {
      return cb_(path, size);
    }

   private:
    LsCallback& cb_;
  };

  /* ********************************* */
  /*       PRIVATE STATIC METHODS      */
  /* ********************************* */

  static int32_t ls_callback_wrapper(
      const char* path, size_t path_len, uint64_t object_size, void* data) {
    CallbackWrapper* cb = static_cast<CallbackWrapper*>(data);
    try {
      if ((*cb)({path, path_len}, object_size)) {
        return 1;
      } else {
        return 0;
      }
    } catch (...) {
      std::throw_with_nested(std::runtime_error("Error in user callback"));
    }
  }
};
}  // namespace tiledb

#endif  // TILEDB_VFS_EXPERIMENTAL_H
