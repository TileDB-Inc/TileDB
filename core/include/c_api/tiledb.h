/**
 * @file   tiledb.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file declares the C API for TileDB.
 */

#ifndef TILEDB_H
#define TILEDB_H

#include <stdint.h>
#include <stdio.h>

/* ********************************* */
/*               MACROS              */
/* ********************************* */

#ifdef __cplusplus
extern "C" {
#endif

/**@{*/
/** C Library export. */
#if (defined __GNUC__ && __GNUC__ >= 4) || defined __INTEL_COMPILER
#define TILEDB_EXPORT __attribute__((visibility("default")))
#elif defined _MSC_VER
#define TILEDB_EXPORT __declspec(dllexport)
#else
#define TILEDB_EXPORT
#pragma message("TILEDB_EXPORT is not defined for this compiler")
#endif
/**@}*/

/**@{*/
/** Deprecated symbol. */
#if (defined __GNUC__) || defined __INTEL_COMPILER
#define TILEDB_DEPRECATED __attribute__((deprecated, visibility("default")))
#elif defined _MSC_VER
#define TILEDB_DEPRECATED __declspec(deprecated)
#else
#define DEPRECATED
#pragma message("TILEDB_DEPRECATED is not defined for this compiler")
#endif
/**@}*/

/* ****************************** */
/*            CONSTANTS           */
/* ****************************** */

/**
 * @name Return codes
 */
/**@{*/
/** Success */
#define TILEDB_OK 0
/** General error */
#define TILEDB_ERR (-1)
/** Out of memory */
#define TILEDB_OOM (-2)
/**@}*/

/** Returns a special name indicating the coordinates attribute. */
TILEDB_EXPORT const char* tiledb_coords();

/** Returns a special value indicating a variable number of elements. */
TILEDB_EXPORT unsigned int tiledb_var_num();

/** Returns the maximum path length on the current platform. */
TILEDB_EXPORT unsigned int tiledb_max_path();

/**
 * @name Constants wrapping special functions
 */
/**@{*/
/** A special name indicating the coordinates attribute. */
#define TILEDB_COORDS tiledb_coords()
/** A special value indicating a variable number of elements. */
#define TILEDB_VAR_NUM tiledb_var_num()
/** The maximum path length on the current platform. */
#define TILEDB_MAX_PATH tiledb_max_path()
/**@}*/

/* ****************************** */
/*          TILEDB ENUMS          */
/* ****************************** */

/** TileDB object type. */
typedef enum {
/** Helper macro for defining object type enums. */
#define TILEDB_OBJECT_TYPE_ENUM(id) TILEDB_##id
#include "tiledb_enum.h"
#undef TILEDB_OBJECT_TYPE_ENUM
} tiledb_object_t;

/** TileDB query type. */
typedef enum {
/** Helper macro for defining query type enums. */
#define TILEDB_QUERY_TYPE_ENUM(id) TILEDB_##id
#include "tiledb_enum.h"
#undef TILEDB_QUERY_TYPE_ENUM
} tiledb_query_type_t;

/** Query status. */
typedef enum {
/** Helper macro for defining query status enums. */
#define TILEDB_QUERY_STATUS_ENUM(id) TILEDB_##id
#include "tiledb_enum.h"
#undef TILEDB_QUERY_STATUS_ENUM
} tiledb_query_status_t;

/** Filesystem type. */
typedef enum {
/** Helper macro for defining filesystem enums. */
#define TILEDB_FILESYSTEM_ENUM(id) TILEDB_##id
#include "tiledb_enum.h"
#undef TILEDB_FILESYSTEM_ENUM
} tiledb_filesystem_t;

/** TileDB datatype. */
typedef enum {
/** Helper macro for defining datatype enums. */
#define TILEDB_DATATYPE_ENUM(id) TILEDB_##id
#include "tiledb_enum.h"
#undef TILEDB_DATATYPE_ENUM
} tiledb_datatype_t;

/** Array type. */
typedef enum {
/** Helper macro for defining array type enums. */
#define TILEDB_ARRAY_TYPE_ENUM(id) TILEDB_##id
#include "tiledb_enum.h"
#undef TILEDB_ARRAY_TYPE_ENUM
} tiledb_array_type_t;

/** Tile or cell layout. */
typedef enum {
/** Helper macro for defining layout type enums. */
#define TILEDB_LAYOUT_ENUM(id) TILEDB_##id
#include "tiledb_enum.h"
#undef TILEDB_LAYOUT_ENUM
} tiledb_layout_t;

/** Compression type. */
typedef enum {
/** Helper macro for defining compressor enums. */
#define TILEDB_COMPRESSOR_ENUM(id) TILEDB_##id
#include "tiledb_enum.h"
#undef TILEDB_COMPRESSOR_ENUM
} tiledb_compressor_t;

/** Walk traversal order. */
typedef enum {
/** Helper macro for defining walk order enums. */
#define TILEDB_WALK_ORDER_ENUM(id) TILEDB_##id
#include "tiledb_enum.h"
#undef TILEDB_WALK_ORDER_ENUM
} tiledb_walk_order_t;

/** VFS mode. */
typedef enum {
/** Helper macro for defining VFS mode enums. */
#define TILEDB_VFS_MODE_ENUM(id) TILEDB_##id
#include "tiledb_enum.h"
#undef TILEDB_VFS_MODE_ENUM
} tiledb_vfs_mode_t;

/* ****************************** */
/*            CONSTANTS           */
/* ****************************** */

/**@{*/
/** Return code. */
#define TILEDB_OK 0      // Success
#define TILEDB_ERR (-1)  // General error
#define TILEDB_OOM (-2)  // Out of memory
/**@}*/

/** Returns a special name indicating the coordinates attribute. */
TILEDB_EXPORT const char* tiledb_coords();

/** Returns a special value indicating a variable number of elements. */
TILEDB_EXPORT unsigned int tiledb_var_num();

/** Returns the maximum path length on the current platform. */
TILEDB_EXPORT unsigned int tiledb_max_path();

/** Returns the input datatype size. */
TILEDB_EXPORT uint64_t tiledb_datatype_size(tiledb_datatype_t type);

/**
 * Returns the size (in bytes) of an offset (used in variable-sized
 * attributes).
 */
TILEDB_EXPORT uint64_t tiledb_offset_size();

/**@{*/
/** Constants wrapping special functions. */
#define TILEDB_COORDS tiledb_coords()
#define TILEDB_VAR_NUM tiledb_var_num()
#define TILEDB_MAX_PATH tiledb_max_path()
#define TILEDB_OFFSET_SIZE tiledb_offset_size()
/**@}*/

/* ****************************** */
/*            VERSION             */
/* ****************************** */

/**
 *  Retrieves the version of the TileDB library currently being used.
 *
 *  @param major Will store the major version number.
 *  @param minor Will store the minor version number.
 *  @param rev Will store the revision (patch) number.
 */
TILEDB_EXPORT void tiledb_version(int* major, int* minor, int* rev);

/* ********************************* */
/*           TILEDB TYPES            */
/* ********************************* */

/** A config object. */
typedef struct tiledb_config_t tiledb_config_t;

/** A config iterator. */
typedef struct tiledb_config_iter_t tiledb_config_iter_t;

/** A TileDB context. */
typedef struct tiledb_ctx_t tiledb_ctx_t;

/** A TileDB error. **/
typedef struct tiledb_error_t tiledb_error_t;

/** A TileDB attribute. */
typedef struct tiledb_attribute_t tiledb_attribute_t;

/** A TileDB array schema. */
typedef struct tiledb_array_schema_t tiledb_array_schema_t;

/** A TileDB dimension. */
typedef struct tiledb_dimension_t tiledb_dimension_t;

/** A TileDB domain. */
typedef struct tiledb_domain_t tiledb_domain_t;

/** A TileDB query. */
typedef struct tiledb_query_t tiledb_query_t;

/** A key-value store schema. */
typedef struct tiledb_kv_schema_t tiledb_kv_schema_t;

/** A key-value store object. */
typedef struct tiledb_kv_t tiledb_kv_t;

/** A key-value item. */
typedef struct tiledb_kv_item_t tiledb_kv_item_t;

/** A key-value store iterator. */
typedef struct tiledb_kv_iter_t tiledb_kv_iter_t;

/** A virtual filesystem object. */
typedef struct tiledb_vfs_t tiledb_vfs_t;

/** A virtual filesystem file handle. */
typedef struct tiledb_vfs_fh_t tiledb_vfs_fh_t;

/* ********************************* */
/*              ERROR                */
/* ********************************* */

/**
 * Returns the error message associated with a TileDB error object.
 *
 * @param err A TileDB error object.
 * @param errmsg A constant pointer to the error message.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_error_message(
    tiledb_error_t* err, const char** errmsg);

/**
 * Frees the resources associated with a TileDB error object.
 *
 * @param err The TileDB error object.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_error_free(tiledb_error_t* err);

/* ********************************* */
/*              CONFIG               */
/* ********************************* */

/**
 * Creates a TileDB config.
 *
 * @param config The config to be created.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_config_create(
    tiledb_config_t** config, tiledb_error_t** error);

/**
 * Frees a TileDB config.
 *
 * @param config The config to be freed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_config_free(tiledb_config_t* config);

/**
 * Sets a config parameter.
 *
 * **Parameters**
 *
 * - `sm.tile_cache_size` <br>
 *    The tile cache size in bytes. Any `uint64_t` value is acceptable.
 * - `sm.array_schema_cache_size` <br>
 *    The array schema cache size in bytes. Any `uint64_t` value is acceptable.
 * - `sm.fragment_metadata_cache_size` <br>
 *    The fragment metadata cache size in bytes. Any `uint64_t` value is
 *    acceptable.
 * - `vfs.s3.region` <br>
 *    The S3 region, if S3 is enabled.
 * - `vfs.s3.scheme` <br>
 *    The S3 scheme (`http` or `https`), if S3 is enabled.
 * - `vfs.s3.endpoint_override` <br>
 *    The S3 endpoint, if S3 is enabled.
 * - `vfs.s3.use_virtual_addressing` <br>
 *    The S3 use of virtual addressing (`true` or `false`), if S3 is enabled.
 * - `vfs.s3.file_buffer_size` <br>
 *    The file buffer size (in bytes) used in S3 writes, if S3 is enables. Any
 *    `uint64_t` value is acceptable.
 * - `vfs.s3.connect_timeout_ms` <br>
 *    The connection timeout in ms. Any `long` value is acceptable.
 * - `vfs.s3.request_timeout_ms` <br>
 *    The request timeout in ms. Any `long` value is acceptable.
 * - `vfs.hdfs.name_node"` <br>
 *    Name node for HDFS.
 * - `vfs.hdfs.username` <br>
 *    HDFS username.
 * - `vfs.hdfs.kerb_ticket_cache_path` <br>
 *    HDFS kerb ticket cache path.
 *
 * @param config The config object.
 * @param param The parameter to be set.
 * @param value The value of the parameter to be set.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_config_set(
    tiledb_config_t* config,
    const char* param,
    const char* value,
    tiledb_error_t** error);

/**
 * Gets a config parameter.
 *
 * @param config The config object.
 * @param param The parameter to be set.
 * @param value A pointer to the value of the parameter to be retrieved
 *    (`NULL` if it does not exist).
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_config_get(
    tiledb_config_t* config,
    const char* param,
    const char** value,
    tiledb_error_t** error);

/**
 * Loads config parameters from a (local) text file.
 *
 * @param config The config object.
 * @param filename The name of the file.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_config_load_from_file(
    tiledb_config_t* config, const char* filename, tiledb_error_t** error);

/**
 * Unsets a config parameter. Potentially useful upon errors, to remove a
 * non-existing parameter from the config.
 *
 * @param config The config object.
 * @param param The parameter to be unset.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_config_unset(
    tiledb_config_t* config, const char* param, tiledb_error_t** error);

/**
 * Saves the config parameters to a (local) text file.
 *
 * @param config The config object.
 * @param filename The name of the file.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_config_save_to_file(
    tiledb_config_t* config, const char* filename, tiledb_error_t** error);

/* ****************************** */
/*            CONFIG ITER         */
/* ****************************** */

/**
 * Creates an iterator on a config object.
 *
 * @param config A config object the iterator will operate on.
 * @param config_iter The config iterator to be created.
 * @param prefix If not `NULL`, only the config parameters starting
 *     with `prefix.*` will be iterated on. Moreover, the prefix will
 *     be stripped from the parameters. Otherwise, all parameters will
 *     be iterated on and their full name will be retrieved.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_config_iter_create(
    tiledb_config_t* config,
    tiledb_config_iter_t** config_iter,
    const char* prefix,
    tiledb_error_t** error);

/**
 * Frees a config iterator.
 *
 * @param config_iter The config iterator to be freed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_config_iter_free(tiledb_config_iter_t* config_iter);

/**
 * Retrieves the config param and value currently pointed by the iterator.
 *
 * @param config_iter The config iterator.
 * @param param The config parameter to be retrieved (`NULL` if the iterator
 *     is at the end).
 * @param value The config value to be retrieved (`NULL` if the iterator
 *     is at the end).
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_config_iter_here(
    tiledb_config_iter_t* config_iter,
    const char** param,
    const char** value,
    tiledb_error_t** error);

/**
 * Moves the iterator to the next param.
 *
 * @param config_iter The config iterator.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_config_iter_next(
    tiledb_config_iter_t* config_iter, tiledb_error_t** error);

/**
 * Checks if the iterator is done.
 *
 * @param config_iter The config iterator.
 * @param done Sets this to `true` if the iterator is done.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_config_iter_done(
    tiledb_config_iter_t* config_iter, int* done, tiledb_error_t** error);

/* ********************************* */
/*              CONTEXT              */
/* ********************************* */

/**
 * Creates a TileDB context, which contains the TileDB storage manager
 * that manages everything in the TileDB library.
 *
 * @param ctx The TileDB context to be created.
 * @param config The configuration parameters (`NULL` means default).
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_ctx_create(
    tiledb_ctx_t** ctx, tiledb_config_t* config);

/**
 * Destroys the TileDB context, freeing all associated memory and resources.
 *
 * @param ctx The TileDB context to be freed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_ctx_free(tiledb_ctx_t* ctx);

/**
 * Retrieves the config from a TileDB context.
 *
 * @param ctx The TileDB context to be created.
 * @param config The config to be retrieved.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_ctx_get_config(
    tiledb_ctx_t* ctx, tiledb_config_t** config);

/**
 * Retrieves the last TileDB error associated with a TileDB context.
 *
 * @param ctx The TileDB context.
 * @param err The last error, or `NULL` if no error has been raised.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_ctx_get_last_error(
    tiledb_ctx_t* ctx, tiledb_error_t** err);

/**
 * Checks if a given storage filesystem backend is supported.
 *
 * @param ctx The TileDB context.
 * @param fs The filesystem to be checked.
 * @param is_supported Sets it to `true` if the filesystem is supported, and
 * `false` otherwise.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_ctx_is_supported_fs(
    tiledb_ctx_t* ctx, tiledb_filesystem_t fs, int* is_supported);

/* ********************************* */
/*                GROUP              */
/* ********************************* */

/**
 * Creates a new TileDB group.
 *
 * @param ctx The TileDB context.
 * @param group_uri The group URI.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_group_create(tiledb_ctx_t* ctx, const char* group_uri);

/* ********************************* */
/*            ATTRIBUTE              */
/* ********************************* */

/**
 * Creates a TileDB attribute.
 *
 * @param ctx The TileDB context.
 * @param attr The TileDB attribute to be created.
 * @param name The attribute name.
 * @param type The attribute type.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_create(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t** attr,
    const char* name,
    tiledb_datatype_t type);

/**
 * Destroys a TileDB attribute, freeing associated memory.
 *
 * @param ctx The TileDB context.
 * @param attr The attribute to be destroyed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_free(
    tiledb_ctx_t* ctx, tiledb_attribute_t* attr);

/**
 * Sets a compressor for an attribute.
 *
 * @param ctx The TileDB context.
 * @param attr The target attribute.
 * @param compressor The compressor to be set.
 * @param compression_level The compression level (use -1 for default).
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_set_compressor(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    tiledb_compressor_t compressor,
    int compression_level);

/**
 * Sets the number of values per cell for an attribute.
 *
 * @param ctx The TileDB context.
 * @param attr The target attribute.
 * @param cell_val_num The number of values per cell.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_set_cell_val_num(
    tiledb_ctx_t* ctx, tiledb_attribute_t* attr, unsigned int cell_val_num);

/**
 * Retrieves the attribute name.
 *
 * @param ctx The TileDB context.
 * @param attr The attribute.
 * @param name The name to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_get_name(
    tiledb_ctx_t* ctx, const tiledb_attribute_t* attr, const char** name);

/**
 * Retrieves the attribute type.
 *
 * @param ctx The TileDB context.
 * @param attr The attribute.
 * @param type The type to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_get_type(
    tiledb_ctx_t* ctx, const tiledb_attribute_t* attr, tiledb_datatype_t* type);

/**
 * Retrieves the attribute compressor and the compression level.
 *
 * @param ctx The TileDB context.
 * @param attr The attribute.
 * @param compressor The compressor to be retrieved.
 * @param compression_level The compression level to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_get_compressor(
    tiledb_ctx_t* ctx,
    const tiledb_attribute_t* attr,
    tiledb_compressor_t* compressor,
    int* compression_level);

/**
 * Retrieves the number of values per cell for the attribute.
 *
 * @param ctx The TileDB context.
 * @param attr The attribute.
 * @param cell_val_num The number of values per cell to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_get_cell_val_num(
    tiledb_ctx_t* ctx,
    const tiledb_attribute_t* attr,
    unsigned int* cell_val_num);

/**
 * Retrieves the cell size for this attribute.
 *
 * @param ctx The TileDB context.
 * @param attr The attribute.
 * @param cell_size The cell size to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_get_cell_size(
    tiledb_ctx_t* ctx, const tiledb_attribute_t* attr, uint64_t* cell_size);

/**
 * Dumps the contents of an attribute in ASCII form to some output (e.g.,
 * file or stdout).
 *
 * @param ctx The TileDB context.
 * @param attr The attribute.
 * @param out The output.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_dump(
    tiledb_ctx_t* ctx, const tiledb_attribute_t* attr, FILE* out);

/* ********************************* */
/*               DOMAIN              */
/* ********************************* */

/**
 * Creates a TileDB domain.
 *
 * @param ctx The TileDB context.
 * @param domain The TileDB domain to be created.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_domain_create(
    tiledb_ctx_t* ctx, tiledb_domain_t** domain);

/**
 * Destroys a TileDB domain, freeing associated memory.
 *
 * @param ctx The TileDB context.
 * @param domain The domain to be destroyed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_domain_free(
    tiledb_ctx_t* ctx, tiledb_domain_t* domain);

/**
 * Retrieves the domain's type.
 *
 * @param ctx The TileDB context.
 * @param domain The domain.
 * @param type The type to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_domain_get_type(
    tiledb_ctx_t* ctx, const tiledb_domain_t* domain, tiledb_datatype_t* type);

/**
 * Retrieves the domain's rank (number of dimensions).
 *
 * @param ctx The TileDB context
 * @param domain The domain
 * @param rank THe rank to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_domain_get_rank(
    tiledb_ctx_t* ctx, const tiledb_domain_t* domain, unsigned int* rank);

/**
 * Adds a dimension to a TileDB domain.
 *
 * @param ctx The TileDB context.
 * @param domain The domain to add the dimension to.
 * @param dim The dimension to be added.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_domain_add_dimension(
    tiledb_ctx_t* ctx, tiledb_domain_t* domain, tiledb_dimension_t* dim);

/**
 * Retrieves a dimension object from a domain by index.
 *
 * @param ctx The TileDB context
 * @param domain The domain to add the dimension to.
 * @param index The index of domain dimension
 * @param dim The retrieved dimension object.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_domain_get_dimension_from_index(
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    unsigned int index,
    tiledb_dimension_t** dim);

/**
 * Retrieves a dimension object from a domain by name (key).
 *
 * @param ctx The TileDB context
 * @param domain The domain to add the dimension to.
 * @param name The name (key) of the requested dimension
 * @param dim The retrieved dimension object.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_domain_get_dimension_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    const char* name,
    tiledb_dimension_t** dim);

/**
 * Dumps the info of a domain in ASCII form to some output (e.g.,
 * file or stdout).
 *
 * @param ctx The TileDB context.
 * @param domain The domain.
 * @param out The output.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_domain_dump(
    tiledb_ctx_t* ctx, const tiledb_domain_t* domain, FILE* out);

/* ********************************* */
/*             DIMENSION             */
/* ********************************* */

/**
 * Creates a dimension.
 *
 * @param ctx The TileDB context.
 * @param dim The dimension to be created.
 * @param name The dimension name.
 * @param type The dimension type.
 * @param dim_domain The dimension domain.
 * @param tile_extent The dimension tile extent.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_create(
    tiledb_ctx_t* ctx,
    tiledb_dimension_t** dim,
    const char* name,
    tiledb_datatype_t type,
    const void* dim_domain,
    const void* tile_extent);

/**
 * Destroys a TileDB dimension, freeing associated memory.
 *
 * @param ctx The TileDB context.
 * @param dim The dimension to be destroyed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_free(
    tiledb_ctx_t* ctx, tiledb_dimension_t* dim);

/**
 * Retrieves the dimension name.
 *
 * @param ctx The TileDB context.
 * @param dim The dimension.
 * @param name The name to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_get_name(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, const char** name);

/**
 * Retrieves the dimension type.
 *
 * @param ctx The TileDB context.
 * @param dim The dimension.
 * @param type The type to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_get_type(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, tiledb_datatype_t* type);

/**
 * Retrieves the domain of the dimension.
 *
 * @param ctx The TileDB context.
 * @param dim The dimension.
 * @param domain The domain to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_get_domain(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, void** domain);

/**
 * Retrieves the tile extent of the dimension.
 *
 * @param ctx The TileDB context.
 * @param dim The dimension.
 * @param tile_extent The tile extent to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_get_tile_extent(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, void** tile_extent);

/**
 * Dumps the contents of a dimension in ASCII form to some output (e.g.,
 * file or stdout).
 *
 * @param ctx The TileDB context.
 * @param dim The dimension.
 * @param out The output.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_dump(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, FILE* out);

/* ********************************* */
/*            ARRAY SCHEMA           */
/* ********************************* */

/**
 * Creates a TileDB array schema object.
 *
 * @param ctx The TileDB context.
 * @param array_schema The TileDB array schema to be created.
 * @param array_type The array type.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_create(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t** array_schema,
    tiledb_array_type_t array_type);

/**
 * Destroys an array schema, freeing associated memory.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema to be destroyed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_free(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema);

/**
 * Adds an attribute to an array schema.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param attr The attribute to be added.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_attribute_t* attr);

/**
 * Sets a domain for the array schema.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param domain The domain to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_set_domain(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_domain_t* domain);

/**
 * Sets the tile capacity.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param capacity The capacity to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_set_capacity(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema, uint64_t capacity);

/**
 * Sets the cell order.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param cell_order The cell order to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_set_cell_order(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_layout_t cell_order);

/**
 * Sets the tile order.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param tile_order The tile order to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_set_tile_order(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_layout_t tile_order);

/**
 * Sets the compressor to use for the coordinates.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param compressor The coordinates compressor.
 * @param compression_level The coordinates compression level.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_set_coords_compressor(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_compressor_t compressor,
    int compression_level);

/**
 * Sets the compressor to use for the offsets of variable-sized attribute
 * values.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param compressor The offsets compressor.
 * @param compression_level The coordinates compression level.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_set_offsets_compressor(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_compressor_t compressor,
    int compression_level);

/**
 * Checks the correctness of the array schema.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @return TILEDB_OK if the array schema is correct and TILEDB_ERR upon any
 *     error.
 */
TILEDB_EXPORT int tiledb_array_schema_check(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema);

/**
 * Retrieves the schema of an array from the disk, creating an array schema
 * struct.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema to be retrieved, or NULL upon error.
 * @param array_uri The array whose schema will be retrieved.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_load(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t** array_schema,
    const char* array_uri);

/**
 * Retrieves the array type.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param array_type The array type to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_get_array_type(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_array_type_t* array_type);

/**
 * Retrieves the capacity.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param capacity The capacity to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_get_capacity(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint64_t* capacity);

/**
 * Retrieves the cell order.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param cell_order The cell order to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_get_cell_order(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_layout_t* cell_order);

/**
 * Retrieves the compressor info of the coordinates.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param compressor The compressor to be retrieved.
 * @param compression_level The compression level to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_get_coords_compressor(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_compressor_t* compressor,
    int* compression_level);

/**
 * Retrieves the compressor info of the offsets.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param compressor The compressor to be retrieved.
 * @param compression_level The compression level to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_get_offsets_compressor(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_compressor_t* compressor,
    int* compression_level);

/**
 * Retrieves the array domain.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param domain The array domain to be retrieved.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_get_domain(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_domain_t** domain);

/**
 * Retrieves the tile order.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param tile_order The tile order to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_get_tile_order(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_layout_t* tile_order);

/**
 * Retrieves the number of array attributes.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param attribute_num The number of attributes to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_get_attribute_num(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    unsigned int* attribute_num);

/**
 * Retrieves an attribute given its index.
 *
 * Attributes are ordered the same way they were defined
 * when constructing the array schema.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param index The index of the attribute to retrieve.
 * @param attr The attribute object to retrieve.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_get_attribute_from_index(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    unsigned int index,
    tiledb_attribute_t** attr);

/**
 * Retrieves an attribute given its name (key).
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param name The name (key) of the attribute to retrieve.
 * @param attr THe attribute object to retrieve.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_get_attribute_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    tiledb_attribute_t** attr);

/**
 * Dumps the array schema in ASCII format in the selected output.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param out The output.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_dump(
    tiledb_ctx_t* ctx, const tiledb_array_schema_t* array_schema, FILE* out);

/* ********************************* */
/*               QUERY               */
/* ********************************* */

/**
 * Creates a TileDB query object.
 *
 * @param ctx The TileDB context.
 * @param query The query object to be created.
 * @param array_uri The name of the array the query will focus on.
 * @param type The query type, which must be one of the following:
 *    - TILEDB_WRITE
 *    - TILEDB_READ
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_query_create(
    tiledb_ctx_t* ctx,
    tiledb_query_t** query,
    const char* array_uri,
    tiledb_query_type_t type);

/**
 * Indicates that the query will write or read a subarray, and provides
 * the appropriate information.
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param subarray The subarray in which the array read/write will be
 *     constrained on. It should be a sequence of [low, high] pairs (one
 *     pair per dimension). For the case of writes, this is meaningful only
 *     for dense arrays, and specifically dense writes. Note that `subarray`
 *     must have the same type as the domain.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_query_set_subarray(
    tiledb_ctx_t* ctx, tiledb_query_t* query, const void* subarray);

/**
 * Sets the buffers to the query, which will either hold the attribute
 * values to be written (if it is a write query), or will hold the
 * results from a read query.
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param attributes A set of the array attributes the read/write will be
 *     constrained on. Note that the coordinates have special attribute name
 *     TILEDB_COORDS. If it is set to `NULL`, then this means **all**
 *     attributes, in the way they were defined upon the array creation,
 *     including the special attributes of coordinates and keys.
 * @param attribute_num The number of the input attributes.
 * @param buffers The buffers that either have the input data to be written,
 *     or will hold the data to be read. Note that there is one buffer per
 *     fixed-sized attribute, and two buffers for each variable-sized
 *     attribute (the first holds the offsets, and the second the actual
 *     values).
 * @param buffer_sizes There must be an one-to-one correspondence with
 *     *buffers*. In the case of writes, they contain the sizes of *buffers*.
 *     In the case of reads, they initially contain the allocated sizes of
 *     *buffers*, but after the termination of the function they will contain
 *     the sizes of the useful (read) data in the buffers.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_query_set_buffers(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char** attributes,
    unsigned int attribute_num,
    void** buffers,
    uint64_t* buffer_sizes);

/**
 * Sets the layout of the cells to be written or read.
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param layout For a write query, this specifies the order of the cells
 *     provided by the user in the buffers. For a read query, this specifies
 *     the order of the cells that will be retrieved as results and stored
 *     in the user buffers. The layout can be one of the following:
 *    - TILEDB_COL_MAJOR:
 *      This means column-major order with respect to the subarray.
 *    - TILEDB_ROW_MAJOR:
 *      This means row-major order with respect to the subarray.
 *    - TILEDB_GLOBAL_ORDER:
 *      This means that cells are stored or retrieved in the array global
 *      cell order.
 *    - TILEDB_UNORDERED:
 *      This is applicable only to writes for sparse arrays, or for sparse
 *      writes to dense arrays. It specifies that the cells are unordered and,
 *      hence, TileDB must sort the cells in the global cell order prior to
 *      writing.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_query_set_layout(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_layout_t layout);

/**
 * Frees a TileDB query object.
 *
 * @param ctx The TileDB context.
 * @param query The query object to be deleted.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_query_free(tiledb_ctx_t* ctx, tiledb_query_t* query);

/**
 * Submits a TileDB query.
 *
 * @param ctx The TileDB context.
 * @param query The query to be submitted.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 *
 * @note This function essentially opens the array associated with the query.
 *     Some metadata structures are loaded in main memory for this array.
 *     In order to flush these data structures and free up memory, invoke
 *     *tiledb_query_free*.
 */
TILEDB_EXPORT int tiledb_query_submit(tiledb_ctx_t* ctx, tiledb_query_t* query);

/**
 * Submits a TileDB query in asynchronous mode.
 *
 * @param ctx The TileDB context.
 * @param query The query to be submitted.
 * @param callback The function to be called when the query completes.
 * @param callback_data The data to be passed to the callback function.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 *
 * @note This function essentially opens the array associated with the query.
 *     Some metadata structures are loaded in main memory for this array.
 *     In order to flush these data structures and free up memory, invoke
 *     *tiledb_query_free*.
 */
TILEDB_EXPORT int tiledb_query_submit_async(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    void (*callback)(void*),
    void* callback_data);

/**
 * Resets the query buffers.
 *
 * @param ctx The TileDB context.
 * @param query The query whose buffers are to be se.
 * @param buffers The buffers to be set.
 * @param buffer_sizes The corresponding buffer sizes.
 * @return TILEDB_OK upon success, and TILEDB_ERR upon error.
 */
TILEDB_EXPORT int tiledb_query_reset_buffers(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    void** buffers,
    uint64_t* buffer_sizes);

/**
 * Retrieves the status of a query.
 *
 * @param ctx The TileDB context.
 * @param query The query.
 * @param status The query status to be retrieved.
 * @return TILEDB_OK upon success, and TILEDB_ERR upon error.
 */
TILEDB_EXPORT int tiledb_query_get_status(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_query_status_t* status);

/**
 * Checks the query status for a particular attribute. This is because a
 * read query may be INCOMPLETE due to the fact that its corresponding
 * buffer did not have enough space to hold the results.
 *
 * @param ctx The TileDB context.
 * @param query The TileDB query.
 * @param attribute_name The name of the attribute to be checked.
 * @param status The query status for the input attribute.
 * @return TILEDB_OK upon success, and TILEDB_ERR upon error.
 */
TILEDB_EXPORT int tiledb_query_get_attribute_status(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* attribute_name,
    tiledb_query_status_t* status);

/* ********************************* */
/*               ARRAY               */
/* ********************************* */

/**
 * Creates a new TileDB array given an input schema.
 *
 * @param ctx The TileDB context.
 * @param array_uri The array name.
 * @param array_schema The array schema.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_create(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const tiledb_array_schema_t* array_schema);

/**
 * Consolidates the fragments of an array into a single fragment.
 *
 * @param ctx The TileDB context.
 * @param array_uri The name of the TileDB array to be consolidated.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_consolidate(
    tiledb_ctx_t* ctx, const char* array_uri);

/**
 * Retrieves the non-empty domain from an array. This is the union of the
 * non-empty domains of the array fragments.
 *
 * @param ctx The TileDB context
 * @param array_uri The array URI.
 * @param domain The domain to be retrieved.
 * @param is_empty `true` if the non-empty domain is empty (i.e., the
 *     array does not contain any data yet).
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_get_non_empty_domain(
    tiledb_ctx_t* ctx, const char* array_uri, void* domain, int* is_empty);

/**
 * Computes an upper bound on the buffer sizes required for a read
 * query, for a given subarray and set of attributes.
 *
 * @param ctx The TileDB context.
 * @param array_uri The array URI.
 * @param subarray The subarray to focus on. Note that it must have the same
 *     underlying type as the array domain.
 * @param attributes The attributes to focus on.
 * @param attribute_num The number of attributes.
 * @param buffer_sizes The buffer sizes to be retrieved. Note that one
 *     buffer size corresponds to a fixed-sized attributes, and two
 *     buffer sizes for a variable-sized attribute (the first is the
 *     size of the offsets, whereas the second is the size of the
 *     actual variable-sized cell values.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_compute_max_read_buffer_sizes(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const void* subarray,
    const char** attributes,
    unsigned attribute_num,
    uint64_t* buffer_sizes);

/* ********************************* */
/*          OBJECT MANAGEMENT        */
/* ********************************* */

/**
 * Returns the TileDB object type for a given resource path.
 *
 * @param ctx The TileDB context.
 * @param path The URI path to the TileDB resource.
 * @param type The type to be retrieved.
 * @return TILEDB_OK on success, TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_object_type(
    tiledb_ctx_t* ctx, const char* path, tiledb_object_t* type);

/**
 * Deletes a TileDB resource (group or array).
 *
 * @param ctx The TileDB context.
 * @param path The URI path to the tiledb resource.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_object_remove(tiledb_ctx_t* ctx, const char* path);

/**
 * Moves a TileDB resource (group or array).
 *
 * @param ctx The TileDB context.
 * @param old_path The old TileDB directory.
 * @param new_path The new TileDB directory.
 * @param force Move resource even if an existing resource exists at the given
 *     path
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_object_move(
    tiledb_ctx_t* ctx, const char* old_path, const char* new_path, int force);

/**
 * Walks (iterates) over the TileDB objects contained in *path*. The traversal
 * is done recursively in the order defined by the user. The user provides
 * a callback function which is applied on each of the visited TileDB objects.
 * The iteration continues for as long the callback returns non-zero, and stops
 * when the callback returns 0. Note that this function ignores any object
 * (e.g., file or directory) that is not TileDB-related.
 *
 * @param ctx The TileDB context.
 * @param path The path in which the traversal will occur.
 * @param order The order of the recursive traversal (e.g., pre-order or
 *     post-order.
 * @param callback The callback function to be applied on every visited object.
 *     The callback should return 0 if the iteration must stop, and 1
 *     if the iteration must continue. It takes as input the currently visited
 *     path, the type of that path (e.g., array or group), and the data
 *     provided by the user for the callback. The callback returns -1 upon
 *     error. Note that `path` in the callback will be an **absolute** path.
 * @param data The data passed in the callback as the last argument.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_object_walk(
    tiledb_ctx_t* ctx,
    const char* path,
    tiledb_walk_order_t order,
    int (*callback)(const char*, tiledb_object_t, void*),
    void* data);

/**
 * Similar to `tiledb_walk`, but now the function visits only the children of
 * `path` (i.e., it does not recursively continue to the children directories.
 *
 * @param ctx The TileDB context.
 * @param path The path in which the traversal will occur.
 * @param callback The callback function to be applied on every visited object.
 *     The callback should return 0 if the iteration must stop, and 1
 *     if the iteration must continue. It takes as input the currently visited
 *     path, the type of that path (e.g., array or group), and the data
 *     provided by the user for the callback. The callback returns -1 upon
 *     error. Note that `path` in the callback will be an **absolute** path.
 * @param data The data passed in the callback as the last argument.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_ls(
    tiledb_ctx_t* ctx,
    const char* path,
    int (*callback)(const char*, tiledb_object_t, void*),
    void* data);

/* ****************************** */
/*         KEY-VALUE SCHEMA       */
/* ****************************** */

/**
 * Creates a TileDB key-value schema object.
 *
 * @param ctx The TileDB context.
 * @param kv_schema The TileDB key-value schema to be created.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_schema_create(
    tiledb_ctx_t* ctx, tiledb_kv_schema_t** kv_schema);

/**
 * Destroys a key-value schema, freeing associated memory.
 *
 * @param ctx The TileDB context.
 * @param kv_schema The key-value schema to be destroyed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_schema_free(
    tiledb_ctx_t* ctx, tiledb_kv_schema_t* kv_schema);

/**
 * Adds an attribute to a key-value schema.
 *
 * @param ctx The TileDB context.
 * @param kv_schema The key-value schema.
 * @param attr The attribute to be added.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_schema_add_attribute(
    tiledb_ctx_t* ctx, tiledb_kv_schema_t* kv_schema, tiledb_attribute_t* attr);

/**
 * Checks the correctness of the key-value schema.
 *
 * @param ctx The TileDB context.
 * @param kv_schema The key-value schema.
 * @return TILEDB_OK if the key-value schema is correct and TILEDB_ERR upon any
 *     error.
 */
TILEDB_EXPORT int tiledb_kv_schema_check(
    tiledb_ctx_t* ctx, tiledb_kv_schema_t* kv_schema);

/**
 * Retrieves the schema of a key-value store from the disk, creating a
 * key-value schema struct.
 *
 * @param ctx The TileDB context.
 * @param kv_schema The key-value schema to be retrieved, or NULL upon error.
 * @param kv_uri The key-value store whose schema will be retrieved.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_schema_load(
    tiledb_ctx_t* ctx, tiledb_kv_schema_t** kv_schema, const char* kv_uri);

/**
 * Retrieves the number of array attributes.
 *
 * @param ctx The TileDB context.
 * @param kv_schema The key-value schema.
 * @param attribute_num The number of attributes to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_schema_get_attribute_num(
    tiledb_ctx_t* ctx,
    const tiledb_kv_schema_t* kv_schema,
    unsigned int* attribute_num);

/**
 * Retrieves an attribute given its index.
 *
 * Attributes are ordered the same way they were defined
 * when constructing the key-value schema.
 *
 * @param ctx The TileDB context.
 * @param kv_schema The key-value schema.
 * @param index The index of the attribute to retrieve.
 * @param attr The attribute object to retrieve.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_schema_get_attribute_from_index(
    tiledb_ctx_t* ctx,
    const tiledb_kv_schema_t* kv_schema,
    unsigned int index,
    tiledb_attribute_t** attr);

/**
 * Retrieves an attribute given its name (key).
 *
 * @param ctx The TileDB context.
 * @param kv_schema The key-value schema.
 * @param name The name (key) of the attribute to retrieve.
 * @param attr THe attribute object to retrieve.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_schema_get_attribute_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_kv_schema_t* kv_schema,
    const char* name,
    tiledb_attribute_t** attr);

/**
 * Dumps the key-value schema in ASCII format in the selected output.
 *
 * @param ctx The TileDB context.
 * @param kv_schema The key-value schema.
 * @param out The output.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_schema_dump(
    tiledb_ctx_t* ctx, const tiledb_kv_schema_t* kv_schema, FILE* out);

/* ****************************** */
/*          KEY-VALUE ITEM        */
/* ****************************** */

/**
 * Creates a key-value item.
 *
 * @param ctx The TileDB context.
 * @param kv_item The key-value item to be created.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_item_create(
    tiledb_ctx_t* ctx, tiledb_kv_item_t** kv_item);

/**
 * Frees a key-value item.
 *
 * @param ctx The TileDB context.
 * @param kv_item The key-value item to be freed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_item_free(
    tiledb_ctx_t* ctx, tiledb_kv_item_t* kv_item);

/**
 * Set the key in the key-value item.
 *
 * @param ctx The TileDB context.
 * @param kv_item The key-value item.
 * @param key The key to be set.
 * @param key_type The key type to be set.
 * @param key_size The key size (in bytes).
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_item_set_key(
    tiledb_ctx_t* ctx,
    tiledb_kv_item_t* kv_item,
    const void* key,
    tiledb_datatype_t key_type,
    uint64_t key_size);

/**
 * Sets a value for a particular attribute to the key-value item.
 * This function works for both fixed- and variable-sized
 * attributes.
 *
 * @param ctx The TileDB context.
 * @param kv_item The key-value item.
 * @param attribute The attribute name.
 * @param value The value to be added.
 * @param value_type The value type.
 * @param value_size The value size in bytes.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_item_set_value(
    tiledb_ctx_t* ctx,
    tiledb_kv_item_t* kv_item,
    const char* attribute,
    const void* value,
    tiledb_datatype_t value_type,
    uint64_t value_size);

/**
 * Gets the key in the key-value item.
 *
 * @param ctx The TileDB context.
 * @param kv_item The key-value item.
 * @param key The key to be retrieved.
 * @param key_type The key type to be retrieved.
 * @param key_size The key size (in bytes).
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_item_get_key(
    tiledb_ctx_t* ctx,
    tiledb_kv_item_t* kv_item,
    const void** key,
    tiledb_datatype_t* key_type,
    uint64_t* key_size);

/**
 * Gets the value and value size on a given attribute from a key-value item.
 *
 * @param ctx The TileDB context.
 * @param kv_item The key-value item.
 * @param attribute The attribute name.
 * @param value The value to be retrieved.
 * @param value_type The value type to be retrieved.
 * @param value_size The value size to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_item_get_value(
    tiledb_ctx_t* ctx,
    tiledb_kv_item_t* kv_item,
    const char* attribute,
    const void** value,
    tiledb_datatype_t* value_type,
    uint64_t* value_size);

/* ****************************** */
/*             KEY-VALUE          */
/* ****************************** */

/**
 * Creates a key-value store from the input key-value schema.
 *
 * @param ctx The TileDB context.
 * @param kv_uri The URI of the key-value store.
 * @param kv_schema The key-value store schema.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_create(
    tiledb_ctx_t* ctx, const char* kv_uri, const tiledb_kv_schema_t* kv_schema);

/**
 * Consolidates the fragments of a key-value store into a single fragment.
 *
 * @param ctx The TileDB context.
 * @param kv_uri The name of the TileDB key-value store to be consolidated.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_kv_consolidate(tiledb_ctx_t* ctx, const char* kv_uri);

/**
 * Sets the parameter that dictates the maximum number of written items
 * buffered in memory before a flush is initiated.
 *
 * @param ctx The TileDB context.
 * @param kv The key-value store.
 * @param max_items The number of maximum items to be set.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_kv_set_max_items(
    tiledb_ctx_t* ctx, tiledb_kv_t* kv, uint64_t max_items);

/**
 * Prepares a key-value store for reading/writing.
 *
 * @param ctx The TileDB context.
 * @param kv The key-value store.
 * @param kv_uri The URI of the key-value store.
 * @param attributes The attributes to focus on. `NULL` indicates all
 *     attributes. If the key-value object is used for writing key-value
 *     items, **all** attributes must be specified.
 * @param attribute_num The number of `attributes`.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_open(
    tiledb_ctx_t* ctx,
    tiledb_kv_t** kv,
    const char* kv_uri,
    const char** attributes,
    unsigned int attribute_num);

/**
 * Closes a key-value store and frees all memory. All bufferd written items
 * will be flushed to persistent storage.
 *
 * @param ctx The TileDB context.
 * @param kv The key-value store to be closed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_close(tiledb_ctx_t* ctx, tiledb_kv_t* kv);

/**
 * Adds a key-value item to a key-value store. The item is buffered
 * internally and periodically flushed to persistent storage.
 * `tiledb_kv_flush` forces flushing the buffered items to storage.
 *
 * @param ctx The TileDB context.
 * @param kv The key-value store.
 * @param kv_item The key-value item to be added to the store.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_add_item(
    tiledb_ctx_t* ctx, tiledb_kv_t* kv, tiledb_kv_item_t* kv_item);

/**
 * Flushes the buffered items to persistent storage.
 *
 * @param ctx The TileDB context.
 * @param kv The key-value store.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_flush(tiledb_ctx_t* ctx, tiledb_kv_t* kv);

/**
 * Retrieves a key-value item based on the input key. If the item with
 * the input key does not exist, `kv_item` is set to `NULL`.
 *
 * @param ctx The TileDB context.
 * @param kv The key-value store.
 * @param kv_item The key-value item.
 * @param key The key.
 * @param key_type The key type.
 * @param key_size The key size.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_get_item(
    tiledb_ctx_t* ctx,
    tiledb_kv_t* kv,
    tiledb_kv_item_t** kv_item,
    const void* key,
    tiledb_datatype_t key_type,
    uint64_t key_size);

/* ****************************** */
/*          KEY-VALUE ITER        */
/* ****************************** */

/**
 * Creates an iterator for a key-value store. This can be used only
 * for reading. This sets the pointer to the first key-value item.
 *
 * @param ctx The TileDB context.
 * @param kv_iter The key-value store iterator to be created.
 * @param kv_uri The URI of the key-value store.
 * @param attributes The attributes to focus on. `NULL` indicates all
 *     attributes. If the key-value object is used for writing key-value
 *     items, **all** attributes must be specified.
 * @param attribute_num The number of `attributes`.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_iter_create(
    tiledb_ctx_t* ctx,
    tiledb_kv_iter_t** kv_iter,
    const char* kv_uri,
    const char** attributes,
    unsigned int attribute_num);

/**
 * Frees a key-value store iterator.
 *
 * @param ctx The TileDB context.
 * @param kv_iter The key-value store iterator to be freed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_iter_free(
    tiledb_ctx_t* ctx, tiledb_kv_iter_t* kv_iter);

/**
 * Retrieves the key-value item currently pointed by the iterator.
 * Note that this function creates a new key-value item.
 *
 * @param ctx The TileDB context.
 * @param kv_iter The key-value store iterator.
 * @param kv_item The current key-value item to be retrieved.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_iter_here(
    tiledb_ctx_t* ctx, tiledb_kv_iter_t* kv_iter, tiledb_kv_item_t** kv_item);

/**
 * Moves the iterator to the next item.
 *
 * @param ctx The TileDB context.
 * @param kv_iter The key-value store iterator.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_iter_next(
    tiledb_ctx_t* ctx, tiledb_kv_iter_t* kv_iter);

/**
 * Checks if the iterator is done.
 *
 * @param ctx The TileDB context.
 * @param kv_iter The key-value store iterator.
 * @param done Sets this to `true` if the iterator is done.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_kv_iter_done(
    tiledb_ctx_t* ctx, tiledb_kv_iter_t* kv_iter, int* done);

/* ****************************** */
/*        VIRTUAL FILESYSTEM      */
/* ****************************** */

/**
 * Creates a virtual filesystem object.
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object to be created.
 * @param config Configuration parameters.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_create(
    tiledb_ctx_t* ctx, tiledb_vfs_t** vfs, tiledb_config_t* config);

/**
 * Frees a virtual filesystem object.
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object to be freed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_free(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

/**
 * Creates an object-store bucket.
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object.
 * @param uri The URI of the bucket to be created.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_create_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri);

/**
 * Deletes an object-store bucket.
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object.
 * @param uri The URI of the bucket to be deleted.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_remove_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri);

/**
 * Deletes the contents of an object-store bucket.
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object.
 * @param uri The URI of the bucket to be emptied.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_empty_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri);

/**
 * Checks if an object-store bucket is empty.
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object.
 * @param uri The URI of the bucket.
 * @param is_empty Sets it to `1` if the input bucket is empty.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_is_empty_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, int* is_empty);

/**
 * Checks if an object-store bucket exists.
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object.
 * @param uri The URI of the bucket.
 * @param is_bucket Sets it to `1` if the input URI is a bucket.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_is_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, int* is_bucket);

/**
 * Creates a directory.
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object.
 * @param uri The URI of the directory to be created.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_create_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri);

/**
 * Checks if a directory exists.
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object.
 * @param uri The URI of the directory.
 * @param is_dir Sets it to `true` if the directory exists and `false`
 *     otherwise.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_is_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, int* is_dir);

/**
 * Removes a directory (recursively).
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object.
 * @param uri The uri of the directory to be removed
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_remove_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri);

/**
 * Checks if a file exists.
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object.
 * @param uri The URI of the file.
 * @param is_file Sets it to `true` if the file exists and `false` otherwise.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_is_file(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, int* is_file);

/**
 * Deletes a file.
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object.
 * @param uri The URI of the file.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_remove_file(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri);

/**
 * Retrieves the size of a file.
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object.
 * @param uri The URI of the file.
 * @param size The file size to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_file_size(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, uint64_t* size);

/**
 * Renames a file or directory.
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object.
 * @param old_uri The old URI.
 * @param new_uri The new URI.
 * @param force Move the file/directory even if the destination URI exists.
 *     In the latter case, the destination URI is overwritten.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_move(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri,
    int force);

/**
 * Prepares a file for reading/writing.
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object.
 * @param uri The URI of the file.
 * @param mode The mode in which the file is opened:
 *     - TILEDB_VFS_READ:
 *       The file is opened for reading. An error is returned if the file
 *       does not exist.
 *     - TILEDB_VFS_WRITE:
 *       The file is opened for writing. If the file exists, it will be
 *       overwritten.
 *     - TILEDB_VFS_APPEND:
 *       The file is opened for writing. If the file exists, the write
 *       will start from the end of the file. Note that S3 does not
 *       support this operation and, thus, an error will be thrown in
 *       that case.
 * @param fh The file handle that is created. This will be used in
 *     `tiledb_vfs_read`, `tiledb_vfs_write` and `tiledb_vfs_sync`.
 * @return TILEDB_OK for success and TILEDB_ERR or TILEDB_OOM for error.
 *
 * @note If the file is closed after being opened, without having
 *     written any data to it, the file will not be created. If you
 *     wish to create an empty file, use `tiledb_vfs_touch`
 *     instead.
 */
TILEDB_EXPORT int tiledb_vfs_open(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    tiledb_vfs_mode_t mode,
    tiledb_vfs_fh_t** fh);

/**
 * Closes a file. This is flushes the buffered data into the file
 * when the file was opened in write (or append) mode. It is particularly
 * important to be called after S3 writes, as otherwise the writes will
 * not take effect.
 *
 * @param ctx The TileDB context.
 * @param fh The file handle.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_close(tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh);

/**
 * Reads from a file.
 *
 * @param ctx The TileDB context.
 * @param fh The URI file handle.
 * @param offset The offset in the file where the read begins.
 * @param buffer The buffer to read into.
 * @param nbytes Number of bytes to read.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_read(
    tiledb_ctx_t* ctx,
    tiledb_vfs_fh_t* fh,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes);

/**
 * Writes the contents of a buffer into a file. Note that this
 * function only **appends** data at the end of the file. If the
 * file does not exist, it will be created.
 *
 * @param ctx The TileDB context.
 * @param fh The URI file handle.
 * @param buffer The buffer to write from.
 * @param nbytes Number of bytes to write.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_write(
    tiledb_ctx_t* ctx,
    tiledb_vfs_fh_t* fh,
    const void* buffer,
    uint64_t nbytes);

/**
 * Syncs (flushes) a file.
 *
 * @param ctx The TileDB context.
 * @param fh The URI file handle.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 *
 * @note This has no effect for S3.
 */
TILEDB_EXPORT int tiledb_vfs_sync(tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh);

/**
 * Frees a file handle.
 *
 * @param ctx The TileDB context.
 * @param fh The URI file handle.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_fh_free(tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh);

/**
 * Checks if a file handle is closed.
 *
 * @param ctx The TileDB context.
 * @param fh The URI file handle.
 * @param is_closed Set to `true` if the file handle is closed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_fh_is_closed(
    tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh, int* is_closed);

/**
 * Touches a file, i.e., creates a new empty file.
 *
 * @param ctx The TileDB context.
 * @param vfs The virtual filesystem object.
 * @param uri The URI of the file to be created.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_vfs_touch(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri);

/* ****************************** */
/*              URI               */
/* ****************************** */

/**
 * Converts the given file URI to a null-terminated platform-native file path.
 *
 * @param ctx The TileDB context.
 * @param uri The URI to be converted.
 * @param path_out The buffer where the converted path string is stored.
 * @param path_length The length of the path buffer. On return, this is set to
 * the length of the converted path string, or 0 on error.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 *
 * @note The path_out buffer must be allocated according to the platform's
 * maximum path length (e.g. PATH_MAX), which includes space for the terminating
 * null character.
 */
TILEDB_EXPORT int tiledb_uri_to_path(
    tiledb_ctx_t* ctx, const char* uri, char* path_out, unsigned* path_length);

#undef TILEDB_EXPORT
#ifdef __cplusplus
}
#endif

#endif  // TILEDB_H
