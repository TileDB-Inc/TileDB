/**
 * @file   tiledb_experimental.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file declares experimental C API for TileDB.
 * Experimental APIs to do not fall under the API compatibility guarantees and
 * might change between TileDB versions
 */

#ifndef TILEDB_EXPERIMENTAL_H
#define TILEDB_EXPERIMENTAL_H

#include "tiledb.h"

/* ********************************* */
/*               MACROS              */
/* ********************************* */

#ifdef __cplusplus
extern "C" {
#endif

/** A TileDB array schema. */
typedef struct tiledb_array_schema_evolution_t tiledb_array_schema_evolution_t;

//TBD: remove me as temporary dependent code no longer needs!
/** TileDB file type. */
typedef struct tiledb_file_t tiledb_file_t;

/* ********************************* */
/*      ARRAY SCHEMA EVOLUTION       */
/* ********************************* */

/**
 * Creates a TileDB schema evolution object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_evolution_t* array_schema_evolution;
 * tiledb_array_schema_evolution_alloc(ctx, &array_schema_evolution);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The TileDB schema evolution to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_alloc(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t** array_schema_evolution);

/**
 * Destroys an array schema evolution, freeing associated memory.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_evolution_free(&array_schema_evolution);
 * @endcode
 *
 * @param array_schema_evolution The array schema evolution to be destroyed.
 */
TILEDB_EXPORT void tiledb_array_schema_evolution_free(
    tiledb_array_schema_evolution_t** array_schema_evolution);

/**
 * Adds an attribute to an array schema evolution.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_attribute_t* attr;
 * tiledb_attribute_alloc(ctx, "my_attr", TILEDB_INT32, &attr);
 * tiledb_array_schema_evolution_add_attribute(ctx, array_schema_evolution,
 * attr);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param attr The attribute to be added.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_attribute_t* attribute);

/**
 * Drops an attribute to an array schema evolution.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* attribute_name="a1";
 * tiledb_array_schema_evolution_drop_attribute(ctx, array_schema_evolution,
 * attribute_name);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param attribute_name The name of the attribute to be dropped.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_drop_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    const char* attribute_name);

/**
 * Sets timestamp range in an array schema evolution
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t timestamp = tiledb_timestamp_now_ms();
 * tiledb_array_schema_evolution_set_timestamp_range(ctx,
 * array_schema_evolution, timestamp, timestamp);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param attribute_name The name of the attribute to be dropped.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_set_timestamp_range(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    uint64_t lo,
    uint64_t hi);

/* ********************************* */
/*          ARRAY SCHEMA             */
/* ********************************* */

TILEDB_EXPORT int32_t tiledb_array_schema_timestamp_range(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    uint64_t* lo,
    uint64_t* hi);

/* ********************************* */
/*               ARRAY               */
/* ********************************* */

/**
 * Evolve array schema of an array.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* array_uri="test_array";
 * tiledb_array_evolve(ctx, array_uri,array_schema_evolution);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_uri The uri of the array.
 * @param array_schema_evolution The schema evolution.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_evolve(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_evolution_t* array_schema_evolution);

/**
 * Upgrades an array to the latest format version.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* array_uri="test_array";
 * tiledb_array_upgrade_version(ctx, array_uri);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_uri The uri of the array.
 * @param config Configuration parameters for the upgrade
 *     (`nullptr` means default, which will use the config from `ctx`).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_upgrade_version(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_config_t* config);

/* ********************************* */
/*               QUERY               */
/* ********************************* */

/**
 * Adds point ranges to the given dimension index of the subarray
 * Effectively `add_range(x_i, x_i)` for `count` points in the
 * target array, but set in bulk to amortize expensive steps.
 */
TILEDB_EXPORT int32_t tiledb_subarray_add_point_ranges(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    uint64_t count);

/**
 * Adds a set of point ranges along subarray dimension index. Each value
 * in the target array is added as `add_range(x,x)` for count elements.
 * The datatype of the range components must be the same as the type of
 * the dimension of the array in the query.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t dim_idx = 2;
 * int64_t ranges[] = { 20, 21, 25, 31}
 * tiledb_query_add_point_ranges(ctx, query, dim_idx, &ranges, 4);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query to add the range to.
 * @param dim_idx The index of the dimension to add the range to.
 * @param start The start of the ranges array.
 * @param count Number of ranges to add.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note The stride is currently unsupported. Use `nullptr` as the
 *     stride argument.
 */
TILEDB_DEPRECATED_EXPORT int32_t tiledb_query_add_point_ranges(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    uint32_t dim_idx,
    const void* start,
    uint64_t count);

/* ********************************* */
/*        QUERY STATUS DETAILS       */
/* ********************************* */

/** This should move to c_api/tiledb.h when stabilized */
typedef struct tiledb_query_status_details_t tiledb_query_status_details_t;

/** TileDB query status details type. */
typedef enum {
/** Helper macro for defining status details type enums. */
#define TILEDB_QUERY_STATUS_DETAILS_ENUM(id) TILEDB_##id
#include "tiledb_enum.h"
#undef TILEDB_QUERY_STATUS_DETAILS_ENUM
} tiledb_query_status_details_reason_t;

/** This should move to c_api/tiledb_struct_defs.h when stabilized */
struct tiledb_query_status_details_t {
  tiledb_query_status_details_reason_t incomplete_reason;
};

/**
 * Get extended query status details.
 *
 * The contained enumeration tiledb_query_status_details_reason_t
 * indicates extended information about a returned query status
 * in order to allow improved client-side handling of buffers and
 * potential resubmissions.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_query_status_details_t status_details;
 * tiledb_query_get_status_details(ctx, query, &status_details);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param query The query from which to retrieve status details.
 * @param status_details The tiledb_query_status_details_t struct.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_query_get_status_details(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_query_status_details_t* status);

/* ********************************* */
/*              FILE                 */
/* ********************************* */

#if 01

TILEDB_EXPORT int32_t tiledb_array_as_file_obtain(tiledb_ctx_t *ctx, tiledb_array_t **array, const char *array_uri, tiledb_config_t *config);
TILEDB_EXPORT int32_t tiledb_array_as_file_import(tiledb_ctx_t *ctx, tiledb_array_t *array, const char *input_uri_filename);
TILEDB_EXPORT int32_t tiledb_array_as_file_export(tiledb_ctx_t *ctx, tiledb_array_t *array, const char *output_uri_filename);

TILEDB_EXPORT int32_t tiledb_array_schema_create_default_blob_array (
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t** array_schema);

#else
//original story api
TILEDB_EXPORT int32_t tiledb_array_as_file_create(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_config_t* config);
TILEDB_EXPORT int32_t tiledb_array_as_file_import(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const char* input_uri_filename,
    tiledb_config_t* config);
TILEDB_EXPORT int32_t tiledb_array_as_file_export(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const char* output_uri_filename,
    tiledb_config_t* config);
#endif

#if 0
//The APIs from Seth's experimental implementation.

/**
 * Allocs the tiledb_file_t type
 *
 * @param ctx The TileDB context.
 * @param array_uri the uri of the array.
 * @param file The tiledb_file_t to be allocated.
 //* @param config Configuration parameters for the upgrade.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_alloc(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_file_t** file);

/**
 * Sets the file config.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * // Set the config for the given file.
 * tiledb_config_t* config;
 * tiledb_file_set_config(ctx, file, config);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file to set the config for.
 * @param config The config to be set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note The file does not need to be opened via `tiledb_file_open` to use
 *      this function.
 * @note The config should be set before opening an file.
 */
TILEDB_EXPORT int32_t tiledb_file_set_config(
    tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_config_t* config);

/**
 * Gets the file config.
 *
 * **Example:**
 *
 * @code{.c}
 * // Retrieve the file for the given array.
 * tiledb_config_t* config;
 * tiledb_file_get_config(ctx, file, config);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file to set the config for.
 * @param config Set to the retrieved config.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_get_config(
    tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_config_t* config);

/**
 *
 * Destroys an file object, freeing associated memory.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_free(&file);
 * @endcode
 * @param file The file object to destroy
 */
TILEDB_EXPORT void tiledb_file_free(tiledb_file_t** file);

/**
 * Create a file array with default schema
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_create_default(ctx, file);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The tiledb_file_t to be created.
 * @param config Configuration parameters for the upgrade.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_create_default(
    tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_config_t* config);

/**
 * Create a file array using heuristics based on a file at provided URI
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_create_from_uri(ctx, file, "input_file", NULL);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param input_uri URI to read file from
 * @param config TileDB Config for setting to create.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_create_from_uri(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    const char* input_uri,
    tiledb_config_t* config);

/**
 * Create a file array using heuristics based on a file from provided VFS
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 *
 * tiledb_vfs_t vfs*;
 * tiledb_vfs_alloc(ctx, &vfs);
 * tiledb_vfs_fh_t *vfs_fh;
 * tiledb_vfs_open(ctx, vfs, "some_file", TILEDB_VFS_READ, &fh);
 * tiledb_file_create_from_vfs_fh(ctx, file, vfs_fh, NULL);
 *
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param input vfs file handle to create from.
 * @param config TileDB Config for setting to create.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_create_from_vfs_fh(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    tiledb_vfs_fh_t* input,
    tiledb_config_t* config);

/**
 * Read a file into the file array from the given FILE handle
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * const char* mime_type;
 * uint32_t size = 0;
 * tiledb_file_get_mime_type(ctx, file, *mime_type, &size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param in FILE handle to read from
 * @param config TileDB Config for setting to create.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_store_fh(
    tiledb_ctx_t* ctx, tiledb_file_t* file, FILE* in, tiledb_config_t* config);

/**
 * Store raw bytes from byte array into file array
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * const char* mime_type;
 * uint32_t size = 0;
 * tiledb_file_get_mime_type(ctx, file, *mime_type, &size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param bytes
 * @param size
 * @param config TileDB Config for setting to create.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_store_buffer(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    void* bytes,
    uint64_t size,
    tiledb_config_t* config);

/**
 * Store file from URI into file array
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param config TileDB Config for setting to create.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_store_uri(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    const char*,
    tiledb_config_t* config);

/**
 * Store file from VFS File Handle into file array
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * const char* mime_type;
 * uint32_t size = 0;
 * tiledb_file_get_mime_type(ctx, file, *mime_type, &size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param config TileDB Config for setting to create.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_store_vfs_fh(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    tiledb_vfs_fh_t*,
    tiledb_config_t* config);

/**
 * Get the file MIME type
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * const char* mime_type;
 * uint32_t size = 0;
 * tiledb_file_get_mime_type(ctx, file, *mime_type, &size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param mime_type char* to set to mime_type
 * @param size length of mime string
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_get_mime_type(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    const char** mime_type,
    uint32_t size);

/**
 * Get the file MIME encoding
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * const char* mime_encoding;
 * uint32_t size = 0;
 * tiledb_file_get_mime_encoding(ctx, file, *mime_type, &size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param mime_type char* to set to mime encoding
 * @param size length of mime string
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_get_mime_encoding(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    const char** mime_type,
    uint32_t size);

/**
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * const char* name;
 * uint32_t size = 0;
 * tiledb_file_get_original_name(ctx, file, &name, &size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_get_original_name(
    tiledb_ctx_t* ctx, tiledb_file_t* file, const char** name, uint32_t* size);

/**
 *
 *  * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * const char* ext;
 * uint32_t size = 0;
 * tiledb_file_get_extension(ctx, file, &name, &size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_get_extension(
    tiledb_ctx_t* ctx, tiledb_file_t* file, const char** ext, uint32_t* size);

/**
 * Get Array Schema from file
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * tiledb_array_schema_t* schema;
 * tiledb_file_get_schema(ctx, file, &schema);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param array_schema
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_get_schema(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    tiledb_array_schema_t** array_schema);

/**
 * Export a file to a raw buffer
 *
 * * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * const char* file_uri = "some_file";
 * FILE* file_out = fopen(file_uri, "w+");
 * tiledb_file_export_fh(ctx, file, file_out, NULL);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param out FILE handle to write to
 * @param config TileDB Config object for export settings
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_export_fh(
    tiledb_ctx_t* ctx, tiledb_file_t* file, FILE* out, tiledb_config_t* config);

/**
 * Export a file to a raw buffer
 *
 * * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * uint64_t size = 0;
 * uint64_t file_offset = 0;
 * tiledb_file_get_size(ctx, file, &size);
 * void* buffer = malloc(size);
 * tiledb_file_export_buffer(ctx, file, bytes, &size, offset, NULL);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param bytes output buffer, alloc'ed by used.
 * @param size size to read
 * @param file_offset file_offset to read from
 * @param config TileDB Config object for export settings
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_export_buffer(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    void* bytes,
    uint64_t* size,
    uint64_t file_offset,
    tiledb_config_t* config);

/**
 * Export a file to the provided URI
 *
 * * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * tiledb_file_export_uri(ctx, file, "s3://tiledb_bucket/some_output_file",
 * NULL);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param output_uri output uri to save to
 * @param config TileDB Config object for export settings
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_export_uri(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    const char* output_uri,
    tiledb_config_t* config);

/**
 * Export a file to the opened VFS file handle
 *
 * * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * tiledb_vfs_t vfs*;
 * tiledb_vfs_alloc(ctx, &vfs);
 * tiledb_vfs_fh_t *vfs_fh;
 * tiledb_vfs_open(ctx, vfs, "some_file", TILEDB_VFS_WRITE, &fh);
 * tiledb_file_export_vfs_fh(ctx, file, output, NULL);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param output output vfs file handle
 * @param config TileDB Config object for export settings
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_export_vfs_fh(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    tiledb_vfs_fh_t* output,
    tiledb_config_t* config);

/**
 * Sets the starting timestamp to use when opening (and reopening) the file.
 * This is an inclusive bound. The default value is `0`.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_set_open_timestamp_start(ctx, file, 1234);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param timestamp_start The epoch timestamp in milliseconds.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_set_open_timestamp_start(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t timestamp_start);

/**
 * Sets the ending timestamp to use when opening (and reopening) the file.
 * This is an inclusive bound. The UINT64_MAX timestamp is a reserved timestamp
 * that will be interpretted as the current timestamp when an file is opened.
 * The default value is `UINT64_MAX`.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_set_open_timestamp_end(ctx, file, 5678);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param timestamp_end The epoch timestamp in milliseconds. Use UINT64_MAX for
 *   the current timestamp.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_set_open_timestamp_end(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t timestamp_end);

/**
 * Gets the starting timestamp used when opening (and reopening) the file.
 * This is an inclusive bound.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_set_open_timestamp_start(ctx, file, 1234);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * uint64_t timestamp_start;
 * tiledb_file_get_open_timestamp_start(ctx, file, &timestamp_start);
 * assert(timestamp_start == 1234);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param timestamp_start The output epoch timestamp in milliseconds.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_get_open_timestamp_start(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t* timestamp_start);

/**
 * Gets the ending timestamp used when opening (and reopening) the file.
 * This is an inclusive bound. If UINT64_MAX was set, this will return
 * the timestamp at the time the file was opened. If the file has not
 * yet been opened, it will return UINT64_MAX.`
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_set_open_timestamp_end(ctx, file, 5678);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * uint64_t timestamp_end;
 * tiledb_file_get_open_timestamp_end(ctx, file, &timestamp_end);
 * assert(timestamp_start == 5678);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param timestamp_end The output epoch timestamp in milliseconds.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_get_open_timestamp_end(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t* timestamp_end);

/**
 * Opens a TileDB file. The file is opened using a query type as input.
 * This is to indicate that queries created for this `tiledb_file_t`
 * object will inherit the query type. In other words, `tiledb_file_t`
 * objects are opened to receive only one type of queries.
 * They can always be closed and be re-opened with another query type.
 * Also there may be many different `tiledb_file_t`
 * objects created and opened with different query types.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "hdfs:///tiledb_files/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object to be opened.
 * @param query_type The type of queries the file object will be receiving.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note If the same file object is opened again without being closed,
 *     an error will be thrown.
 * @note The config should be set before opening an file.
 * @note If the file is to be opened at a specfic time interval, the
 *      `timestamp{start, end}` values should be set to a config that's set to
 *       the file object before opening the file.
 */
TILEDB_EXPORT int32_t tiledb_file_open(
    tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_query_type_t query_type);

/**
 * Closes a TileDB file.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "hdfs:///tiledb_files/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * tiledb_file_close(ctx, file);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object to be closed.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note If the file object has already been closed, the function has
 *     no effect.
 */
TILEDB_EXPORT int32_t tiledb_file_close(tiledb_ctx_t* ctx, tiledb_file_t* file);

/**
 * Get the size of the opened file
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "hdfs:///tiledb_files/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * uint64_t size = 0;
 * tiledb_file_get_size(ctx, file, &size);
 * tiledb_file_close(ctx, file);
 * @endcode
 *
 * @param ctx  The TileDB context.
 * @param file The file object to be closed.
 * @param size of the file
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t
tiledb_file_get_size(tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t* size);
#endif

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_EXPERIMENTAL_H
