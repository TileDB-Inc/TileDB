/**
 * @file tiledb/api/c_api/vfs/vfs_api_external.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file declares the C API for TileDB.
 */

#ifndef TILEDB_CAPI_VFS_EXTERNAL_H
#define TILEDB_CAPI_VFS_EXTERNAL_H

#include "../api_external_common.h"
#include "../context/context_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** C API carrier for a TileDB virtual filesystem */
typedef struct tiledb_vfs_handle_t tiledb_vfs_t;

/** C API carrier for a TileDB virtual filesystem */
typedef struct tiledb_vfs_fh_handle_t tiledb_vfs_fh_t;

/** VFS mode. */
typedef enum {
/** Helper macro for defining VFS mode enums. */
#define TILEDB_VFS_MODE_ENUM(id) TILEDB_##id
#include "vfs_api_enum.h"
#undef TILEDB_VFS_MODE_ENUM
} tiledb_vfs_mode_t;

/**
 * Returns a string representation of the given VFS mode.
 *
 * @param[in] vfs_mode VFS mode
 * @param[out] str
 *  Set to point to a constant string representation of the VFS mode
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_mode_to_str(
    tiledb_vfs_mode_t vfs_mode, const char** str) TILEDB_NOEXCEPT;

/**
 * Parses a VFS mode from the given string.
 *
 * @param[in] str String representation to parse
 * @param[out] vfs_mode Set to the parsed VFS mode
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_mode_from_str(
    const char* str, tiledb_vfs_mode_t* vfs_mode) TILEDB_NOEXCEPT;

/**
 * Creates a virtual filesystem object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_t* vfs;
 * tiledb_vfs_alloc(ctx, config, &vfs);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] config Configuration parameters.
 * @param[out] vfs The virtual filesystem object to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_alloc(
    tiledb_ctx_t* ctx,
    tiledb_config_t* config,
    tiledb_vfs_t** vfs) TILEDB_NOEXCEPT;

/**
 * Frees a virtual filesystem object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_free(&vfs);
 * @endcode
 *
 * @param[in, out] vfs The virtual filesystem object to be freed.
 */
TILEDB_EXPORT void tiledb_vfs_free(tiledb_vfs_t** vfs) TILEDB_NOEXCEPT;

/**
 * Retrieves the config from a VFS context.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_config_t* config;
 * tiledb_vfs_get_config(ctx, vfs, &config);
 * // Make sure to free the retrieved config
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The VFS object.
 * @param[out] config The config to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_get_config(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    tiledb_config_t** config) TILEDB_NOEXCEPT;

/**
 * Creates an object-store bucket.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_create_bucket(ctx, vfs, "s3://tiledb");
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] uri The URI of the bucket to be created.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_create_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) TILEDB_NOEXCEPT;

/**
 * Deletes an object-store bucket.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_delete_bucket(ctx, vfs, "s3://tiledb");
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] uri The URI of the bucket to be deleted.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_remove_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) TILEDB_NOEXCEPT;

/**
 * Deletes the contents of an object-store bucket.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_empty_bucket(ctx, vfs, "s3://tiledb");
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] uri The URI of the bucket to be emptied.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_empty_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) TILEDB_NOEXCEPT;

/**
 * Checks if an object-store bucket is empty.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t is_empty;
 * tiledb_vfs_is_empty_bucket(ctx, vfs, "s3://tiledb", &empty);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] uri The URI of the bucket.
 * @param[out] is_empty Sets it to `1` if the input bucket is empty,
 *     and `0` otherwise.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_is_empty_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, int32_t* is_empty)
    TILEDB_NOEXCEPT;

/**
 * Checks if an object-store bucket exists.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t exists;
 * tiledb_vfs_is_bucket(ctx, vfs, "s3://tiledb", &exists);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] uri The URI of the bucket.
 * @param[out] is_bucket Sets it to `1` if the input URI is a bucket, and `0`
 *     otherwise.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_is_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, int32_t* is_bucket)
    TILEDB_NOEXCEPT;

/**
 * Creates a directory.
 *
 * - On S3, this is a noop.
 * - On all other backends, if the directory exists, the function
 *   just succeeds without doing anything.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_create_dir(ctx, vfs, "s3://my_bucket/my_dir");
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] uri The URI of the directory to be created.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_create_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) TILEDB_NOEXCEPT;

/**
 * Checks if a directory exists.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t exists;
 * tiledb_vfs_is_dir(ctx, vfs, "s3://my_bucket/my_dir", &exists);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object.
 * @param uri The URI of the directory.
 * @param is_dir Sets it to `1` if the directory exists and `0`
 *     otherwise.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note For S3, this function will return `true` if there is an object
 *     with prefix `uri/` (TileDB will append `/` internally to `uri`
 *     only if it does not exist), and `false` othewise.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_is_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, int32_t* is_dir)
    TILEDB_NOEXCEPT;

/**
 * Removes a directory (recursively).
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_remove_dir(ctx, vfs, "s3://my_bucket/my_dir");
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] uri The uri of the directory to be removed
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_remove_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) TILEDB_NOEXCEPT;

/**
 * Checks if a file exists.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t exists;
 * tiledb_vfs_is_file(ctx, vfs, "s3://my_bucket/my_file", &is_file);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] uri The URI of the file.
 * @param[out] is_file Sets it to `1` if the file exists and `0` otherwise.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_is_file(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, int32_t* is_file)
    TILEDB_NOEXCEPT;

/**
 * Deletes a file.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_remove_file(ctx, vfs, "s3://my_bucket/my_file");
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] uri The URI of the file.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_remove_file(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) TILEDB_NOEXCEPT;

/**
 * Retrieves the size of a directory. This function is **recursive**, i.e.,
 * it will consider all files in the directory tree rooted at `uri`.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t dir_size;
 * tiledb_vfs_dir_size(ctx, vfs, "s3://my_bucket/my_dir", &dir_size);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] uri The URI of the directory.
 * @param[out] size The directory size to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_dir_size(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, uint64_t* size)
    TILEDB_NOEXCEPT;

/**
 * Retrieves the size of a file.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t file_size;
 * tiledb_vfs_file_size(ctx, vfs, "s3://my_bucket/my_file", &file_size);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] uri The URI of the file.
 * @param[out] size The file size to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_file_size(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, uint64_t* size)
    TILEDB_NOEXCEPT;

/**
 * Renames a file. If the destination file exists, it will be overwritten.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_move_file(
 * ctx, vfs, "s3://my_bucket/my_file", "s3://new_bucket/new_file");
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] old_uri The old URI.
 * @param[in] new_uri The new URI.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_move_file(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) TILEDB_NOEXCEPT;

/**
 * Renames a directory.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_move_dir(ctx, vfs, "s3://my_bucket/my_dir",
 * "s3://new_bucket/new_dir");
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] old_uri The old URI.
 * @param[in] new_uri The new URI.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_move_dir(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) TILEDB_NOEXCEPT;

/**
 * Copies a file. If the destination file exists, it will be overwritten.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_copy_file(
 * ctx, vfs, "s3://my_bucket/my_file", "s3://new_bucket/new_file");
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] old_uri The old URI.
 * @param[in] new_uri The new URI.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_copy_file(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) TILEDB_NOEXCEPT;

/**
 * Copies the files of a directory to another directory. If a file in the
 * destination directory exists, it will be overwritten.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_copy_dir(
 *  ctx, vfs, "s3://my_bucket/my_dir", "s3://new_bucket/new_dir");
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] old_uri The old URI.
 * @param[in] new_uri The new URI.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_copy_dir(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) TILEDB_NOEXCEPT;

/**
 * Prepares a file for reading/writing.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_fh_t* fh;
 * tiledb_vfs_open(ctx, vfs, "some_file", TILEDB_VFS_READ, &fh);
 * // Make sure to close and delete the created file handle
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] uri The URI of the file.
 * @param[in] mode The mode in which the file is opened:
 *     - `TILEDB_VFS_READ`:
 *       The file is opened for reading. An error is returned if the file
 *       does not exist.
 *     - `TILEDB_VFS_WRITE`:
 *       The file is opened for writing. If the file exists, it will be
 *       overwritten.
 *     - `TILEDB_VFS_APPEND`:
 *       The file is opened for writing. If the file exists, the write
 *       will start from the end of the file. Note that S3 does not
 *       support this operation and, thus, an error will be thrown in
 *       that case.
 * @param[out] fh The file handle that is created. This will be used in
 *     `tiledb_vfs_read`, `tiledb_vfs_write` and `tiledb_vfs_sync`.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` or `TILEDB_OOM` for error.
 *
 * @note If the file is closed after being opened, without having
 *     written any data to it, the file will not be created. If you
 *     wish to create an empty file, use `tiledb_vfs_touch`
 *     instead.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_open(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    tiledb_vfs_mode_t mode,
    tiledb_vfs_fh_t** fh) TILEDB_NOEXCEPT;

/**
 * Closes a file. This is flushes the buffered data into the file
 * when the file was opened in write (or append) mode. It is particularly
 * important to be called after S3 writes, as otherwise the writes will
 * not take effect.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_close(ctx, vfs, fh);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fh The file handle.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_vfs_close(tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh) TILEDB_NOEXCEPT;

/**
 * Reads from a file.
 *
 * **Example:**
 *
 * @code{.c}
 * char buffer[10000];
 * tiledb_vfs_read(ctx, fh, 100, buffer, 10000);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fh The URI file handle.
 * @param[in] offset The offset in the file where the read begins.
 * @param[out] buffer The buffer to read into.
 * @param[in] nbytes Number of bytes to read.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_read(
    tiledb_ctx_t* ctx,
    tiledb_vfs_fh_t* fh,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes) TILEDB_NOEXCEPT;

/**
 * Writes the contents of a buffer into a file. Note that this
 * function only **appends** data at the end of the file. If the
 * file does not exist, it will be created.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* msg = "This will be written to the file";
 * tiledb_vfs_write(ctx, fh, buffer, strlen(msg));
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fh The URI file handle.
 * @param[in] buffer The buffer to write from.
 * @param[in] nbytes Number of bytes to write.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_write(
    tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh, const void* buffer, uint64_t nbytes)
    TILEDB_NOEXCEPT;

/**
 * Syncs (flushes) a file.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_sync(ctx, fh);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fh The URI file handle.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note This has no effect for S3.
 */
TILEDB_EXPORT capi_return_t
tiledb_vfs_sync(tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh) TILEDB_NOEXCEPT;

/**
 * The function visits only the children of `path` (i.e., it does not
 * recursively continue to the children directories) and applies the `callback`
 * function using the input `data`.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_ls(ctx, vfs, "my_dir", NULL, NULL);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] path The path in which the traversal will occur.
 * @param[in,out] callback
 * The callback function to be applied on every visited object.
 *     The callback should return `0` if the iteration must stop, and `1`
 *     if the iteration must continue. It takes as input the currently visited
 *     path, the type of that path (e.g., array or group), and the data
 *     provided by the user for the callback. The callback returns `-1` upon
 *     error. Note that `path` in the callback will be an **absolute** path.
 * @param data The data passed in the callback as the last argument.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_ls(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* path,
    int32_t (*callback)(const char*, void*),
    void* data) TILEDB_NOEXCEPT;

/**
 * Frees a file handle.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_fh_free(&fh);
 * @endcode
 *
 * @param[in, out] fh The URI file handle.
 */
TILEDB_EXPORT void tiledb_vfs_fh_free(tiledb_vfs_fh_t** fh) TILEDB_NOEXCEPT;

/**
 * Checks if a file handle is closed.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t is_closed;
 * tiledb_vfs_fh_is_closed(ctx, fh, &is_closed);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fh The URI file handle.
 * @param[out] is_closed
 *  Set to `1` if the file handle is closed, and `0` otherwise.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_fh_is_closed(
    tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh, int32_t* is_closed) TILEDB_NOEXCEPT;

/**
 * Touches a file, i.e., creates a new empty file if it does not already exist.
 *
 * The access timestamps of the file are not guaranteed to change.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_vfs_touch(ctx, vfs, "my_empty_file");
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] vfs The virtual filesystem object.
 * @param[in] uri The URI of the file to be created.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_vfs_touch(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_VFS_EXTERNAL_H
