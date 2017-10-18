/**
 * @file   tiledb.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include <unistd.h>
#include <cfloat>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <string>

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
#else
#define TILEDB_EXPORT
#endif
/**@}*/

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

/**@{*/
/** Constants wrapping special functions. */
#define TILEDB_COORDS tiledb_coords()
#define TILEDB_VAR_NUM tiledb_var_num()
/**@}*/

/* ****************************** */
/*          TILEDB ENUMS          */
/* ****************************** */

/** TileDB object type. */
typedef enum {
#define TILEDB_OBJECT_TYPE_ENUM(id) TILEDB_##id
#include "tiledb_enum.inc"
#undef TILEDB_OBJECT_TYPE_ENUM
} tiledb_object_t;

/** Query mode. */
typedef enum {
#define TILEDB_QUERY_TYPE_ENUM(id) TILEDB_##id
#include "tiledb_enum.inc"
#undef TILEDB_QUERY_TYPE_ENUM
} tiledb_query_type_t;

/** Query status. */
typedef enum {
#define TILEDB_QUERY_STATUS_ENUM(id) TILEDB_##id
#include "tiledb_enum.inc"
#undef TILEDB_QUERY_STATUS_ENUM
} tiledb_query_status_t;

/** Data type. */
typedef enum {
#define TILEDB_DATATYPE_ENUM(id) TILEDB_##id
#include "tiledb_enum.inc"
#undef TILEDB_DATATYPE_ENUM
} tiledb_datatype_t;

/** Data type. */
typedef enum {
#define TILEDB_ARRAY_TYPE_ENUM(id) TILEDB_##id
#include "tiledb_enum.inc"
#undef TILEDB_ARRAY_TYPE_ENUM
} tiledb_array_type_t;

/** Tile or cell layout. */
typedef enum {
#define TILEDB_LAYOUT_ENUM(id) TILEDB_##id
#include "tiledb_enum.inc"
#undef TILEDB_LAYOUT_ENUM
} tiledb_layout_t;

/** Compression type. */
typedef enum {
#define TILEDB_COMPRESSOR_ENUM(id) TILEDB_##id
#include "tiledb_enum.inc"
#undef TILEDB_COMPRESSOR_ENUM
} tiledb_compressor_t;

/** Walk traversal order. */
typedef enum {
#define TILEDB_WALK_ORDER_ENUM(id) TILEDB_##id
#include "tiledb_enum.inc"
#undef TILEDB_WALK_ORDER_ENUM
} tiledb_walk_order_t;

/* ****************************** */
/*            VERSION             */
/* ****************************** */

/**
 *  Retrieves the version of the TileDB library being currently used.
 *
 *  @param major Will store the major version number.
 *  @param minor Will store the minor version number.
 *  @param rev Will store the revision (patch) number.
 */
TILEDB_EXPORT void tiledb_version(int* major, int* minor, int* rev);

/* ********************************* */
/*           TILEDB TYPES            */
/* ********************************* */

/** A TileDB context. */
typedef struct tiledb_ctx_t tiledb_ctx_t;

/** A TileDB error. **/
typedef struct tiledb_error_t tiledb_error_t;

/** A TileDB attribute. */
typedef struct tiledb_attribute_t tiledb_attribute_t;

/** A TileDB attribute iterator. */
typedef struct tiledb_attribute_iter_t tiledb_attribute_iter_t;

/** A TileDB array metadata. */
typedef struct tiledb_array_metadata_t tiledb_array_metadata_t;

/** A TileDB dimension. */
typedef struct tiledb_dimension_t tiledb_dimension_t;

/** A TileDB dimension iterator. */
typedef struct tiledb_dimension_iter_t tiledb_dimension_iter_t;

/** A TileDB domain. */
typedef struct tiledb_domain_t tiledb_domain_t;

/** A TileDB query. */
typedef struct tiledb_query_t tiledb_query_t;

/* ********************************* */
/*              CONTEXT              */
/* ********************************* */

/**
 * Creates a TileDB context, which contains the TileDB storage manager
 * that manages everything in the TileDB library.
 *
 * @param ctx The TileDB context to be created.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_ctx_create(tiledb_ctx_t** ctx);

/**
 * Destroys the TileDB context, properly freeing-up all memory.
 *
 * @param ctx The TileDB context to be freed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_ctx_free(tiledb_ctx_t* ctx);

/* ********************************* */
/*              ERROR                */
/* ********************************* */

/**
 * Retrieves the last TileDB error associated with a TileDB context.
 *
 * @param ctx The TileDB context.
 * @param err The last error, or NULL if no error has been raised.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_error_last(tiledb_ctx_t* ctx, tiledb_error_t** err);

/**
 * Returns the error message associated with a TileDB error object.
 *
 * @param ctx The TileDB context.
 * @param err A TileDB error object.
 * @param errmsg A constant pointer to the error message.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_error_message(
    tiledb_ctx_t* ctx, tiledb_error_t* err, const char** errmsg);

/**
 * Free's the resources associated with a TileDB error object.
 *
 * @param ctx The TileDB context.
 * @param err The TileDB error object.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_error_free(tiledb_ctx_t* ctx, tiledb_error_t* err);

/* ********************************* */
/*                GROUP              */
/* ********************************* */

/**
 * Creates a new TileDB group.
 *
 * @param ctx The TileDB context.
 * @param group The group name.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_group_create(tiledb_ctx_t* ctx, const char* group);

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
 * Destroys a TileDB attribute, freeing-up memory.
 *
 * @param ctx The TileDB context.
 * @param attr The attribute to be destroyed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_free(
    tiledb_ctx_t* ctx, tiledb_attribute_t* attr);

/**
 * Sets a compressor to an attribute.
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
 * Retrieves the number of values per cell for this attribute.
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
 * @param type The type of all dimensions of the domain.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_domain_create(
    tiledb_ctx_t* ctx, tiledb_domain_t** domain, tiledb_datatype_t type);

/**
 * Destroys a TileDB domain, freeing-up memory.
 *
 * @param ctx The TileDB context.
 * @param domain The domain to be destroyed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_domain_free(
    tiledb_ctx_t* ctx, tiledb_domain_t* domain);

/**
 * Retrieves the dimensions type.
 *
 * @param ctx The TileDB context.
 * @param domain The domain.
 * @param type The type to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_domain_get_type(
    tiledb_ctx_t* ctx, const tiledb_domain_t* domain, tiledb_datatype_t* type);

/**
 * Adds a dimension to a TileDB domain.
 *
 * @param ctx The TileDB context.
 * @param domain The domain to add the dimension to.
 * @param name The dimension name.
 * @param dim_domain The dimension domain.
 * @param tile_extent The dimension tile extent.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_domain_add_dimension(
    tiledb_ctx_t* ctx,
    tiledb_domain_t* domain,
    const char* name,
    const void* dim_domain,
    const void* tile_extent);

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
 * Retrieves the domain of the dimension.
 *
 * @param ctx The TileDB context.
 * @param dim The dimension.
 * @param domain The domain to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_get_domain(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, const void** domain);

/**
 * Retrieves the tile extent of the dimension.
 *
 * @param ctx The TileDB context.
 * @param dim The dimension.
 * @param tile_extent The tile extent to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_get_tile_extent(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, const void** tile_extent);

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
/*        DIMENSION ITERATOR         */
/* ********************************* */

/**
 * Creates a dimensions iterator for the input domain.
 *
 * @param ctx The TileDB context.
 * @param domain The input array domain.
 * @param dim_it The dimension iterator to be created.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_iter_create(
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    tiledb_dimension_iter_t** dim_it);

/**
 * Frees a dimension iterator.
 *
 * @param ctx The TileDB context.
 * @param dim_it The dimension iterator to be freed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_iter_free(
    tiledb_ctx_t* ctx, tiledb_dimension_iter_t* dim_it);

/**
 * Checks if a dimension iterator has reached the end.
 *
 * @param ctx The TileDB context.
 * @param dim_it The dimension iterator.
 * @param done This is set to 1 if the iterator id done, and 0 otherwise.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_iter_done(
    tiledb_ctx_t* ctx, tiledb_dimension_iter_t* dim_it, int* done);

/**
 * Advances the dimension iterator.
 *
 * @param ctx The TileDB context.
 * @param dim_it The dimension iterator.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_iter_next(
    tiledb_ctx_t* ctx, tiledb_dimension_iter_t* dim_it);

/**
 * Retrieves a constant pointer to the current dimension pointed by the
 * iterator.
 *
 * @param ctx The TileDB context.
 * @param dim_it The dimension iterator.
 * @param dim The dimension pointer to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_iter_here(
    tiledb_ctx_t* ctx,
    tiledb_dimension_iter_t* dim_it,
    const tiledb_dimension_t** dim);

/**
 * Rewinds the iterator to point to the first dimension.
 *
 * @param ctx The TileDB context.
 * @param dim_it The dimension iterator.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_iter_first(
    tiledb_ctx_t* ctx, tiledb_dimension_iter_t* dim_it);

/* ********************************* */
/*           ARRAY METADATA          */
/* ********************************* */

/**
 * Creates a TileDB array metadata object.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The TileDB array metadata to be created.
 * @param array_name The array name.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_create(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t** array_metadata,
    const char* array_name);

/**
 * Destroys an array metadata, freeing-up memory.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata to be destroyed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_free(
    tiledb_ctx_t* ctx, tiledb_array_metadata_t* array_metadata);

/**
 * Adds an attribute to an array metadata.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata.
 * @param attr The attribute to be added.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t* array_metadata,
    tiledb_attribute_t* attr);

/**
 * Sets a domain to array metadata.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata.
 * @param domain The domain to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_set_domain(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t* array_metadata,
    tiledb_domain_t* domain);

/**
 * Sets the tile capacity.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata.
 * @param capacity The capacity to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_set_capacity(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t* array_metadata,
    uint64_t capacity);

/**
 * Sets the cell order.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata.
 * @param cell_order The cell order to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_set_cell_order(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t* array_metadata,
    tiledb_layout_t cell_order);

/**
 * Sets the tile order.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata.
 * @param tile_order The tile order to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_set_tile_order(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t* array_metadata,
    tiledb_layout_t tile_order);

/**
 * Sets the array type.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata.
 * @param array_type The array type to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_set_array_type(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t* array_metadata,
    tiledb_array_type_t array_type);

/**
 * Checks the correctness of the array metadata.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata.
 * @return TILEDB_OK if the array metadata is correct and TILEDB_ERR upon any
 *     error.
 */
TILEDB_EXPORT int tiledb_array_metadata_check(
    tiledb_ctx_t* ctx, tiledb_array_metadata_t* array_metadata);

/**
 * Retrieves the metadata of an array from the disk, creating an array metadata
 * struct.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata to be retrieved, or NULL upon error.
 * @param array_name The array whose metadata will be retrieved.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_load(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t** array_metadata,
    const char* array_name);

/**
 * Retrieves the array name.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata.
 * @param array_name The array name to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_get_array_name(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    const char** array_name);

/**
 * Retrieves the array type.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata.
 * @param array_type The array type to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_get_array_type(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    tiledb_array_type_t* array_type);

/**
 * Retrieves the capacity.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata.
 * @param capacity The capacity to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_get_capacity(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    uint64_t* capacity);

/**
 * Retrieves the cell order.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata.
 * @param cell_order The cell order to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_get_cell_order(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    tiledb_layout_t* cell_order);

/**
 * Retrieves the compressor info of the coordinates.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata.
 * @param coords_compressor The compressor to be retrieved.
 * @param coords_compression_level The compression level to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_get_coords_compressor(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    tiledb_compressor_t* coords_compressor,
    int* coords_compression_level);

/**
 * Retrieves the array domain.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata.
 * @param domain The array domain to be retrieved.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_get_domain(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    tiledb_domain_t** domain);

/**
 * Retrieves the tile order.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata.
 * @param tile_order The tile order to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_get_tile_order(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    tiledb_layout_t* tile_order);

/**
 * Dumps the array metadata in ASCII format in the selected output.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata.
 * @param out The output.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_metadata_dump(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    FILE* out);

/* ********************************* */
/*         ATTRIBUTE ITERATOR        */
/* ********************************* */

/**
 * Creates an attribute iterator for the input array metadata.
 *
 * @param ctx The TileDB context.
 * @param metadata The input array metadata.
 * @param attr_it The attribute iterator to be created.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_iter_create(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* metadata,
    tiledb_attribute_iter_t** attr_it);

/**
 * Frees an attribute iterator.
 *
 * @param ctx The TileDB context.
 * @param attr_it The attribute iterator to be freed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_iter_free(
    tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it);

/**
 * Checks if an attribute iterator has reached the end.
 *
 * @param ctx The TileDB context.
 * @param attr_it The attribute iterator.
 * @param done This is set to 1 if the iterator is done, and 0 otherwise.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_iter_done(
    tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it, int* done);

/**
 * Advances the attribute iterator.
 *
 * @param ctx The TileDB context.
 * @param attr_it The attribute iterator.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_iter_next(
    tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it);

/**
 * Retrieves a constant pointer to the current attribute pointed by the
 * iterator.
 *
 * @param ctx The TileDB context.
 * @param attr_it The attribute iterator.
 * @param attr The attribute pointer to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_iter_here(
    tiledb_ctx_t* ctx,
    tiledb_attribute_iter_t* attr_it,
    const tiledb_attribute_t** attr);

/**
 * Rewinds the iterator to point to the first attribute.
 *
 * @param ctx The TileDB context.
 * @param attr_it The attribute iterator.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_iter_first(
    tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it);

/* ********************************* */
/*               QUERY               */
/* ********************************* */

/**
 * Creates a TileDB query object.
 *
 * @param ctx The TileDB context.
 * @param query The query object to be created.
 * @param array_name The name of the array the query will focus on.
 * @param type The query type, which must be one of the following:
 *    - TILEDB_WRITE
 *    - TILEDB_READ
 * @param layout For a write query, this indicates the order of the cells
 *     provided by the user in the buffers. For a read query, this indicates
 *     the order of the cells that will be retrieved as results and stored
 *     in the user buffers. The layout can be one of the following:
 *    - TILEDB_COL_MAJOR <br>
 *      This means column-major order with respect to the subarray.
 *    - TILEDB_ROW_MAJOR <br>
 *      This means row-major order with respect to the subarray.
 *    - TILEDB_GLOBAL_ORDER <br>
 *      This means that cells are stored or retrieved in the array global
 *      cell order.
 *    - TILEDB_UNORDERED <br>
 *      This is applicable only to writes for sparse arrays, which indicates
 *      that the cells are unordered and, hence, TileDB must sort the cells
 *      in the global cell order.
 * @param subarray The subarray in which the array read/write will be
 *     constrained on. It should be a sequence of [low, high] pairs (one
 *     pair per dimension), whose type should be the same as that of the
 *     coordinates. If it is NULL, then the subarray is set to the entire
 *     array domain. For the case of writes, this is meaningful only for
 *     dense arrays, and specifically dense writes.
 * @param attributes A subset of the array attributes the read/write will be
 *     constrained on. Note that the coordinates have special attribute name
 *     tiledb_coords(). A NULL value indicates **all** attributes (including
 *     the coordinates as the last attribute in the case of sparse arrays).
 * @param attribute_num The number of the input attributes. If *attributes* is
 *     NULL, then this should be set to 0.
 * @param buffers The buffers that either have the input data to be written,
 *     or will hold the data to be read. There must be an one-to-one
 *     correspondence with *attributes*.
 * @param buffer_sizes There must be an one-to-one correspondence with
 *     *buffers*. In the case of writes, they contain the sizes of *buffers*.
 *     In the case of reads, they initially contain the allocated sizes of
 *     *buffers*, but after the termination of the function they will contain
 *     the sizes of the useful (read) data in the buffers.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_query_create(
    tiledb_ctx_t* ctx,
    tiledb_query_t** query,
    const char* array_name,
    tiledb_query_type_t type,
    tiledb_layout_t layout,
    const void* subarray,
    const char** attributes,
    unsigned int attribute_num,
    void** buffers,
    uint64_t* buffer_sizes);

/**
 * Deletes a TileDB query object.
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
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 *
 * @note This function essentially opens the array associated with the query.
 *     Some bookkeeping structures are loaded in main-memory for this array.
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
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 *
 * @note This function essentially opens the array associated with the query.
 *     Some bookkeeping structures are loaded in main-memory for this array.
 *     In order to flush these data structures and free up memory, invoke
 *     *tiledb_query_free*.
 */
TILEDB_EXPORT int tiledb_query_submit_async(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    void* (*callback)(void*),
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
 * Creates a new TileDB array given an input metadata.
 *
 * @param ctx The TileDB context.
 * @param array_metadata The array metadata.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_create(
    tiledb_ctx_t* ctx, const tiledb_array_metadata_t* array_metadata);

/**
 * Consolidates the fragments of an array into a single fragment.
 *
 * @param ctx The TileDB context.
 * @param array_name The name of the TileDB array to be consolidated.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_consolidate(
    tiledb_ctx_t* ctx, const char* array_name);

/* ********************************* */
/*        RESOURCE MANAGEMENT        */
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
TILEDB_EXPORT int tiledb_delete(tiledb_ctx_t* ctx, const char* path);

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
TILEDB_EXPORT int tiledb_move(
    tiledb_ctx_t* ctx, const char* old_path, const char* new_path, bool force);

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
TILEDB_EXPORT int tiledb_walk(
    tiledb_ctx_t* ctx,
    const char* path,
    tiledb_walk_order_t order,
    int (*callback)(const char*, tiledb_object_t, void*),
    void* data);

#undef TILEDB_EXPORT
#ifdef __cplusplus
}
#endif

#endif  // TILEDB_H
