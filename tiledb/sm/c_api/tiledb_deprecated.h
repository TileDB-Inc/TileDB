/*
 * @file   tiledb_deprecated.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file declares the deprecated C API for TileDB.
 */

#ifndef TILEDB_DEPRECATED_H
#define TILEDB_DEPRECATED_H

#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Creates an array schema based on the properties of the provided URI
 * or a default schema if no URI is provided
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_t* schema;
 * tiledb_filestore_schema_create(ctx, "/path/file.pdf", &schema);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param uri The file URI.
 * @param array_schema The TileDB array schema to be created
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_filestore_schema_create(
    tiledb_ctx_t* ctx,
    const char* uri,
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT;

/**
 * Imports a file into a TileDB filestore array
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_t* schema;
 * tiledb_filestore_schema_create(ctx, path_to_file, &schema);
 * tiledb_array_create(ctx, path_to_array, schema);
 * tiledb_filestore_uri_import(ctx, path_to_array, path_to_file,
 * TILEDB_MIME_AUTODETECT);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param filestore_array_uri The array URI.
 * @param file_uri The file URI.
 * @param mime_type The mime type of the file
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_filestore_uri_import(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    const char* file_uri,
    tiledb_mime_type_t mime_type) TILEDB_NOEXCEPT;

/**
 * Exports a filestore array into a bare file
 * **Example:**
 *
 * @code{.c}
 * tiledb_filestore_uri_export(ctx, path_to_file, path_to_array);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file_uri The file URI.
 * @param filestore_array_uri The array URI.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_filestore_uri_export(
    tiledb_ctx_t* ctx,
    const char* file_uri,
    const char* filestore_array_uri) TILEDB_NOEXCEPT;

/**
 * Writes size bytes starting at address buf into filestore array
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_t* schema;
 * tiledb_filestore_schema_create(ctx, NULL, &schema);
 * tiledb_array_create(ctx, path_to_array, schema);
 * tiledb_filestore_buffer_import(ctx, path_to_array, buf, size,
 * TILEDB_MIME_AUTODETECT);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param filestore_array_uri The array URI.
 * @param buf The input buffer
 * @param size Number of bytes to be imported
 * @param mime_type The mime type of the data
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_filestore_buffer_import(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    void* buf,
    size_t size,
    tiledb_mime_type_t mime_type) TILEDB_NOEXCEPT;

/**
 * Dump the content of a filestore array into a buffer
 * **Example:**
 *
 * @code{.c}
 * size_t size = 1024;
 * void *buf = malloc(size);
 * tiledb_filestore_buffer_export(ctx, path_to_array, 0, buf, size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param filestore_array_uri The array URI.
 * @param offset The offset at which we should start exporting from the array
 * @param buf The buffer that will contain the filestore array content
 * @param size The number of bytes to be exported into the buffer
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_filestore_buffer_export(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    size_t offset,
    void* buf,
    size_t size) TILEDB_NOEXCEPT;

/**
 * Get the uncompressed size of a filestore array
 * **Example:**
 *
 * @code{.c}
 * size_t size;
 * tiledb_filestore_size(ctx, path_to_array, &size);
 * void *buf = malloc(size);
 * tiledb_filestore_buffer_export(ctx, path_to_array, 0, buf, size);
 * free(buf);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] filestore_array_uri The array URI.
 * @param[in] size The returned uncompressed size of the filestore array
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_filestore_size(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    size_t* size) TILEDB_NOEXCEPT;

/**
 * Get the string representation of a mime type enum
 *
 * @param mime_type The mime enum
 * @param str The resulted string representation
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_mime_type_to_str(
    tiledb_mime_type_t mime_type, const char** str) TILEDB_NOEXCEPT;

/**
 * Turn a string mime type into a TileDB enum
 *
 * @param str The mime type string
 * @param mime_type The resulted mime enum
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_mime_type_from_str(
    const char* str, tiledb_mime_type_t* mime_type) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_DEPRECATED_H
