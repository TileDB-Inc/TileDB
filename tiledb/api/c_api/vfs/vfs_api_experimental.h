/**
 * @file tiledb/api/c_api/vfs/vfs_api_experimental.h
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
 * This file declares the experimental VFS C API for TileDB.
 */

#ifndef TILEDB_VFS_API_EXPERIMENTAL_H
#define TILEDB_VFS_API_EXPERIMENTAL_H

#include "tiledb/api/c_api/api_external_common.h"
#include "tiledb/api/c_api/context/context_api_external.h"
#include "tiledb/api/c_api/vfs/vfs_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Typedef for ls_recursive callback function invoked on each object collected.
 *
 * @param path The path of a visited object for the relative filesystem.
 * @param path_len The length of the path.
 * @param object_size The size of the object at the current path.
 * @param data Data passed to the callback used to store collected results.
 */
typedef int32_t (*tiledb_ls_callback_t)(
    const char* path, size_t path_len, uint64_t object_size, void* data);

/**
 * Typedef for ls_recursive_v2 callback function invoked on each object
 * collected.
 *
 * @param path The path of a visited object for the relative filesystem.
 * @param path_len The length of the path.
 * @param object_size The size of the object at the current path.
 * @param is_dir 1 if the current object is a directory, else 0.
 * @param data Data passed to the callback used to store collected results.
 */
typedef int32_t (*tiledb_ls_callback_v2_t)(
    const char* path,
    size_t path_len,
    uint64_t object_size,
    uint8_t is_dir,
    void* data);

/**
 * Visits the children of `path` recursively, invoking the callback for each
 * entry. The callback should return 1 to continue traversal, 0 to stop, or -1
 * on error. The callback is responsible for writing gathered entries into the
 * `data` buffer, for example using a pointer to a user-defined struct.
 *
 * Currently LocalFS, S3, Azure, and GCS are supported. Objects and
 * directories will be collected for LocalFS. Only objects will be collected
 * for cloud storage backends such as S3, Azure, and GCS.
 *
 * **Example:**
 *
 * @code{.c}
 * int my_callback(
 *     const char* path, size_t path_length, uint64_t file_size, void* data) {
 *   MyCbStruct cb_data = static_cast<MyCbStruct*>(data);
 *   // Perform custom callback behavior here.
 *   return 1;  // Continue traversal to next entry.
 * }
 * MyCbStruct* cb_data = allocate_cb_struct();
 *
 * tiledb_vfs_ls_recursive(ctx, vfs, "s3://bucket/foo", my_callback, &cb_data);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] path The path in which the traversal will occur.
 * @param[in] callback
 * The callback function to be applied on every visited object.
 *     The callback should return `0` if the iteration must stop, and `1`
 *     if the iteration must continue. It takes as input the currently visited
 *     path, the length of the currently visited path, the size of the file, and
 *     user provided buffer for paths and object sizes in the form of a struct
 *     pointer. The callback returns `-1` upon error. Note that `path` in the
 *     callback will be an **absolute** path.
 * @param[in] data Data pointer passed into the callback for storing results.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_ls_recursive(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* path,
    tiledb_ls_callback_t callback,
    void* data) TILEDB_NOEXCEPT;

/**
 * Visits the children of `path` recursively, invoking the callback for each
 * entry. The callback should return 1 to continue traversal, 0 to stop, or -1
 * on error. The callback is responsible for writing gathered entries into the
 * `data` buffer, for example using a pointer to a user-defined struct.
 *
 * Currently LocalFS, S3, Azure, and GCS are supported. The results will
 * include objects and directories for all storage backends.
 *
 * The LsCallbackV2 used in this API adds an additional parameter for checking
 * if the current result is a directory. This can be used by the caller to
 * include or exclude directories as needed during traversal.
 *
 * **Example:**
 *
 * @code{.c}
 * int my_callback(
 *     const char* path,
 *     size_t path_length,
 *     uint64_t file_size,
 *     uint8_t is_dir,
 *     void* data) {
 *   MyCbStruct cb_data = static_cast<MyCbStruct*>(data);
 *   // Perform custom callback behavior here.
 *   return 1;  // Continue traversal to next entry.
 * }
 * MyCbStruct* cb_data = allocate_cb_struct();
 *
 * tiledb_vfs_ls_recursive_v2(
 *     ctx, vfs, "s3://bucket/foo", my_callback, &cb_data);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] path The path in which the traversal will occur.
 * @param[in] callback
 * The callback function to be applied on every visited object.
 *     The callback should return `0` if the iteration must stop, and `1`
 *     if the iteration must continue. It takes as input the currently visited
 *     path, the length of the currently visited path, the size of the file,
 *     a uint8 marking the object as a directory or not, and user provided
 *     buffer for paths and object sizes in the form of a struct pointer. The
 *     callback returns `-1` upon error. Note that `path` in the callback will
 *     be an **absolute** path.
 * @param[in] data Data pointer passed into the callback for storing results.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_ls_recursive_v2(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* path,
    tiledb_ls_callback_v2_t callback,
    void* data) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_VFS_API_EXPERIMENTAL_H
