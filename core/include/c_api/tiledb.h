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

#ifdef HAVE_MPI
#include <mpi.h>
#endif
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

/** Version. */
#define TILEDB_VERSION "0.6.1"
#define TILEDB_VERSION_MAJOR 0
#define TILEDB_VERSION_MINOR 6
#define TILEDB_VERSION_REVISION 1

/**@{*/
/** Return code. */
#define TILEDB_OK 0
#define TILEDB_ERR -1
#define TILEDB_OOM -2
/**@}*/

/**@{*/
/** MAC address interface. */
#if defined(__APPLE__) && defined(__MACH__)
#ifndef TILEDB_MAC_ADDRESS_INTERFACE
#define TILEDB_MAC_ADDRESS_INTERFACE en0
#endif
#else
#ifndef TILEDB_MAC_ADDRESS_INTERFACE
#define TILEDB_MAC_ADDRESS_INTERFACE eth0
#endif
#endif
/**@}*/

/** Returns a special name indicating the coordinates attribute. */
TILEDB_EXPORT const char* tiledb_coords();

/** Returns a special name indicating the key attribute. */
TILEDB_EXPORT const char* tiledb_key();

/** Returns a special value indicating a variable number of elements. */
TILEDB_EXPORT int tiledb_var_num();

/** Returns a special value indicating a variable size. */
TILEDB_EXPORT uint64_t tiledb_var_size();

/* ****************************** */
/*          TILEDB ENUMS          */
/* ****************************** */

/** TileDB object type. */
typedef enum {
#define TILEDB_OBJECT_ENUM(id) TILEDB_##id
#include "tiledb_enum.inc"
#undef TILEDB_OBJECT_ENUM
} tiledb_object_t;

/** Array mode. */
typedef enum {
#define TILEDB_QUERY_MODE_ENUM(id) TILEDB_ARRAY_##id
#include "tiledb_enum.inc"
#undef TILEDB_QUERY_MODE_ENUM
} tiledb_query_mode_t;

/** I/O method. */
typedef enum {
#define TILEDB_IO_METHOD_ENUM(id) TILEDB_IO_METHOD_##id
#include "tiledb_enum.inc"
#undef TILEDB_IO_METHOD_ENUM
} tiledb_io_t;

/** Asynchronous I/O (AIO) request status. */
typedef enum {
#define TILEDB_AIO_ENUM(id) TILEDB_AIO_##id
#include "tiledb_enum.inc"
#undef TILEDB_AIO_ENUM
} tiledb_aio_status_t;

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

/* ****************************** */
/*            VERSION             */
/* ****************************** */

/**
 *  Return the version of the tiledb library
 *  being currently used.
 *
 *  @param major Store the major version number
 *  @param minor Store the minor version number
 *  @param rev Store the revision (patch) number
 */
TILEDB_EXPORT void tiledb_version(int* major, int* minor, int* rev);

/* ********************************* */
/*           TILEDB TYPES            */
/* ********************************* */

/** The TileDB context, which maintains state for the TileDB modules. */
typedef struct tiledb_ctx_t tiledb_ctx_t;

/** Used to pass configuration parameters to TileDB. */
typedef struct tiledb_config_t tiledb_config_t;

/** Opaque struct describing a TileDB error. **/
typedef struct tiledb_error_t tiledb_error_t;

/** A TileDB basic array object. */
typedef struct tiledb_basic_array_t tiledb_basic_array_t;

/** A TileDB attribute. */
typedef struct tiledb_attribute_t tiledb_attribute_t;

/** A TileDB attribute iterator. */
typedef struct tiledb_attribute_iter_t tiledb_attribute_iter_t;

/** A TileDB dimension. */
typedef struct tiledb_dimension_t tiledb_dimension_t;

/** A TileDB dimension iterator. */
typedef struct tiledb_dimension_iter_t tiledb_dimension_iter_t;

/** A TileDB array schema. */
typedef struct tiledb_array_schema_t tiledb_array_schema_t;

/** A TileDB array object. */
typedef struct tiledb_array_t tiledb_array_t;

/** An asynchronous I/O request. */
typedef struct tiledb_aio_request_t tiledb_aio_request_t;

/* ********************************* */
/*              CONTEXT              */
/* ********************************* */

/**
 * Creates a TileDB context.
 *
 * @param ctx The TileDB context to be created.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_ctx_create(tiledb_ctx_t** ctx);

/**
 * Destroys the TileDB context, properly freeing-up memory.
 *
 * @param ctx The TileDB context to be freed.
 * @return void
 */
TILEDB_EXPORT void tiledb_ctx_free(tiledb_ctx_t* ctx);

/**
 * Sets a configuration to a TileDB context.
 *
 * @param ctx The TileDB context.
 * @param config The config to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 *
 * @note It is strongly recommended that this function is used before starting
 *    to use any arrays/groups, as messing with the configuration
 * during TileDB operations may lead to unexpected errors.
 */
TILEDB_EXPORT int tiledb_ctx_set_config(
    tiledb_ctx_t* ctx, tiledb_config_t* config);

/* ********************************* */
/*              CONFIG               */
/* ********************************* */

/**
 * Creates a TileDB configuration object.
 *
 * @param ctx The TileDB context.
 * @param config The configuration object to be created.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_config_create(
    tiledb_ctx_t* ctx, tiledb_config_t** config);

/**
 * Destroys a TileDB configuration object.
 *
 * @param config The configuration object to be destroyed.
 * @return void
 */
TILEDB_EXPORT void tiledb_config_free(tiledb_config_t* config);

/**
 * Sets the MPI communicator.
 *
 * @param ctx The TileDB context.
 * @param config The config object.
 * @param mpi_comm The MPI communicator to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
#ifdef HAVE_MPI
TILEDB_EXPORT int tiledb_config_set_mpi_comm(
    tiledb_ctx_t* ctx, tiledb_config_t* config, MPI_Comm* mpi_comm);
#endif

/**
 * Sets the read method.
 *
 * @param ctx The TileDB context.
 * @param config The config object.
 * @param read_method The read method to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_config_set_read_method(
    tiledb_ctx_t* ctx, tiledb_config_t* config, tiledb_io_t read_method);

/**
 * Sets the write method.
 *
 * @param ctx The TileDB context.
 * @param config The config object.
 * @param write_method The write method to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_config_set_write_method(
    tiledb_ctx_t* ctx, tiledb_config_t* config, tiledb_io_t write_method);

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
 * @param err The TileDB error object.
 * @return void
 */
TILEDB_EXPORT void tiledb_error_free(tiledb_error_t* err);

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
/*            BASIC ARRAY            */
/* ********************************* */

/**
 * Creates a basic array.
 *
 * @param ctx The TileDB context.
 * @param name The name of the basic array to be created.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_basic_array_create(
    tiledb_ctx_t* ctx, const char* name);

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
 * @param attr The attribute to be destroyed.
 * @return void
 */
TILEDB_EXPORT void tiledb_attribute_free(tiledb_attribute_t* attr);

/**
 * Sets a compressor to an attribute.
 *
 * @param ctx The TileDB context.
 * @param attr The target attribute.
 * @param compressor The compressor to be set.
 * @param compression level The compression level (use -1 for default).
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
 * @param int cell_val_num The number of values per cell.
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
 * @param cell_val_num The number of values per cell.
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
/*            DIMENSION              */
/* ********************************* */

/**
 * Creates a TileDB dimension.
 *
 * @param ctx The TileDB context.
 * @param dim The TileDB dimension to be created.
 * @param name The dimension name.
 * @param type The dimension type.
 * @param domain The dimension domain (low, high).
 * @param tile_extent The tile extent along this dimension.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_create(
    tiledb_ctx_t* ctx,
    tiledb_dimension_t** dim,
    const char* name,
    tiledb_datatype_t type,
    const void* domain,
    const void* tile_extent);

/**
 * Destroys a TileDB dimension, freeing-up memory.
 *
 * @param dim The dimension to be destroyed.
 * @return void
 */
TILEDB_EXPORT void tiledb_dimension_free(tiledb_dimension_t* dim);

/**
 * Sets a compressor for a dimension.
 *
 * @param ctx The TileDB context.
 * @param dim The target dimension.
 * @param compressor The compressor to be set.
 * @param compression_level The compression level (use -1 for default).
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_set_compressor(
    tiledb_ctx_t* ctx,
    tiledb_dimension_t* dim,
    tiledb_compressor_t compressor,
    int compression_level);

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
 * Retrieves the dimension compressor and the compression level.
 *
 * @param ctx The TileDB context.
 * @param dim The dimension.
 * @param compressor The compressor to be retrieved.
 * @param compression_level The compression level to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_get_compressor(
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    tiledb_compressor_t* compressor,
    int* compression_level);

/**
 * Returns the domain of the dimension.
 * @param ctx The TileDB context.
 * @param dim The dimension.
 * @param domain The domain to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_get_domain(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, const void** domain);

/**
 * Returns the tile extent of the dimension.
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
/*           ARRAY SCHEMA            */
/* ********************************* */

/**
 * Creates a TileDB array schema object.
 *
 * @param ctx The TileDB context.
 * @param array_schema The TileDB array schema to be created.
 * @param array_name The array name.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_create(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t** array_schema,
    const char* array_name);

/**
 * Destroys an array schema, freeing-up memory.
 *
 * @param array_schema The array schema to be destroyed.
 * @return void
 */
TILEDB_EXPORT void tiledb_array_schema_free(
    tiledb_array_schema_t* array_schema);

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
 * Adds a dimension to an array schema.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param dim The dimension to be added.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_add_dimension(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_dimension_t* dim);

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
 * Sets the array type.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param array_type The array type to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_set_array_type(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_array_type_t array_type);

/**
 * Checks the correctness of the array schema.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @return TILEDB_OK if the array schema is correct and TILEDB_ERR upon any
 * error.
 */
TILEDB_EXPORT int tiledb_array_schema_check(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema);

/**
 * Retrieves the schema of an array from disk, creating an array schema struct.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema to be retrieved, or NULL upon error.
 * @param array_name The array whose schema will be retrieved.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_load(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t** array_schema,
    const char* array_name);

/**
 * Retrieves the array name.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param array_name The array name to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_get_array_name(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char** array_name);

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
 * Dumps the array schema in ASCII format in the selected output.
 *
 * @param ctx The TileDB context.
 * @param array_schema The array schema.
 * @param out The output.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_schema_dump(
    tiledb_ctx_t* ctx, const tiledb_array_schema_t* array_schema, FILE* out);

/**
 * Creates an attribute iterator for the input array schema.
 *
 * @param ctx The TileDB context.
 * @param schema The input array schema.
 * @param attr_it The attribute iterator to be created.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_iter_create(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* schema,
    tiledb_attribute_iter_t** attr_it);

/**
 * Frees an attribute iterator.
 *
 * @param attr_it The attribute iterator to be freed.
 * @return void
 */
TILEDB_EXPORT void tiledb_attribute_iter_free(tiledb_attribute_iter_t* attr_it);

/**
 * Checks if an attribute iterator has reached the end.
 *
 * @param ctx The TileDB context.
 * @param attr_it The attribute iterator.
 * @param done This is set to 1 if the iterator id done, and 0 otherwise.
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
 * @return
 */
TILEDB_EXPORT int tiledb_attribute_iter_first(
    tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it);

/**
 * Creates a dimensions iterator for the input array schema.
 *
 * @param ctx The TileDB context.
 * @param array_schema The input array schema.
 * @param dim_it The attribute iterator to be created.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_dimension_iter_create(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_dimension_iter_t** dim_it);

/**
 * Frees a dimensions iterator.
 *
 * @param attr_it The attribute iterator to be freed.
 * @return void
 */
TILEDB_EXPORT void tiledb_dimension_iter_free(tiledb_dimension_iter_t* dim_it);

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
 * @return
 */
TILEDB_EXPORT int tiledb_dimension_iter_first(
    tiledb_ctx_t* ctx, tiledb_dimension_iter_t* dim_it);

/* ********************************* */
/*               ARRAY               */
/* ********************************* */

/**
 * Creates a new TileDB array.
 *
 * @param ctx The TileDB context.
 * @param tiledb_array_schema The array schema.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_create(
    tiledb_ctx_t* ctx, const tiledb_array_schema_t* tiledb_array_schema);

/**
 * Initializes a TileDB array.
 *
 * @param ctx The TileDB context.
 * @param tiledb_array The array object to be initialized. The function
 *     will allocate memory space for it.
 * @param array The directory of the array to be initialized.
 * @param mode The mode of the array. It must be one of the following:
 *    - TILEDB_ARRAY_WRITE
 *    - TILEDB_ARRAY_WRITE_SORTED_COL
 *    - TILEDB_ARRAY_WRITE_SORTED_ROW
 *    - TILEDB_ARRAY_WRITE_UNSORTED
 *    - TILEDB_ARRAY_READ
 *    - TILEDB_ARRAY_READ_SORTED_COL
 *    - TILEDB_ARRAY_READ_SORTED_ROW
 * @param subarray The subarray in which the array read/write will be
 *     constrained on. It should be a sequence of [low, high] pairs (one
 *     pair per dimension), whose type should be the same as that of the
 *     coordinates. If it is NULL, then the subarray is set to the entire
 *     array domain. For the case of writes, this is meaningful only for
 *     dense arrays, and specifically dense writes.
 * @param attributes A subset of the array attributes the read/write will be
 *     constrained on. Note that the coordinates have special attribute name
 *     TILEDB_COORDS. A NULL value indicates **all** attributes (including
 *     the coordinates as the last attribute in the case of sparse arrays).
 * @param attribute_num The number of the input attributes. If *attributes* is
 *     NULL, then this should be set to 0.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_init(
    tiledb_ctx_t* ctx,
    tiledb_array_t** tiledb_array,
    const char* array,
    tiledb_query_mode_t mode,
    const void* subarray,
    const char** attributes,
    int attribute_num);

/**
 * Retrieves the schema of an already initialized array.
 *
 * @param tiledb_array The TileDB array object (must already be initialized).
 * @param tiledb_array_schema The array schema to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_get_schema(
    const tiledb_array_t* tiledb_array,
    tiledb_array_schema_t* tiledb_array_schema);

/**
 * Performs a write operation to an array.
 * The array must be initialized in one of the following write modes,
 * each of which has a different behaviour:
 *    - TILEDB_ARRAY_WRITE: \n
 *      In this mode, the cell values are provided in the buffers respecting
 *      the cell order on the disk (specified in the array schema). It is
 *      practically an **append** operation,
 *      where the provided cell values are simply written at the end of
 *      their corresponding attribute files. This mode leads to the best
 *      performance. The user may invoke this function an arbitrary number
 *      of times, and all the writes will occur in the same fragment.
 *      Moreover, the buffers need not be synchronized, i.e., some buffers
 *      may have more cells than others when the function is invoked.
 *    - TILEDB_ARRAY_WRITE_SORTED_COL: \n
 *      In this mode, the cell values are provided in the buffer in column-major
 *      order with respect to the subarray used upon array initialization.
 *      TileDB will properly re-organize the cells so that they follow the
 *      array cell order for storage on the disk.
 *    - TILEDB_ARRAY_WRITE_SORTED_ROW: \n
 *      In this mode, the cell values are provided in the buffer in row-major
 *      order with respect to the subarray used upon array initialization.
 *      TileDB will properly re-organize the cells so that they follow the
 *      array cell order for storage on the disk.
 *    - TILEDB_ARRAY_WRITE_UNSORTED: \n
 *      This mode is applicable to sparse arrays, or when writing sparse updates
 *      to a dense array. One of the buffers holds the coordinates. The cells
 *      in this mode are given in an arbitrary, unsorted order (i.e., without
 *      respecting how the cells must be stored on the disk according to the
 *      array schema definition). Each invocation of this function internally
 *      sorts the cells and writes them to the disk in the proper order. In
 *      addition, each invocation creates a **new** fragment. Finally, the
 *      buffers in each invocation must be synchronized, i.e., they must have
 *      the same number of cell values across all attributes.
 *
 * @param tiledb_array The TileDB array object (must be already initialized).
 * @param buffers An array of buffers, one for each attribute. These must be
 *     provided in the same order as the attribute order specified in
 *     tiledb_array_init() or tiledb_array_reset_attributes(). The case of
 *     variable-sized attributes is special. Instead of providing a single
 *     buffer for such an attribute, **two** must be provided: the second
 *     holds the variable-sized cell values, whereas the first holds the
 *     start offsets of each cell in the second buffer.
 * @param buffer_sizes The sizes (in bytes) of the input buffers (there should
 *     be a one-to-one correspondence).
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_write(
    const tiledb_array_t* tiledb_array,
    const void** buffers,
    const size_t* buffer_sizes);

/**
 * Performs a read operation on an array.
 * The array must be initialized in one of the following read modes,
 * each of which has a different behaviour:
 *    - TILEDB_ARRAY_READ: \n
 *      In this mode, the cell values are stored in the buffers respecting
 *      the cell order on the disk (specified in the array schema). This mode
 *      leads to the best performance.
 *    - TILEDB_ARRAY_READ_SORTED_COL: \n
 *      In this mode, the cell values are stored in the buffers in column-major
 *      order with respect to the subarray used upon array initialization.
 *    - TILEDB_ARRAY_READ_SORTED_ROW: \n
 *      In this mode, the cell values are stored in the buffer in row-major
 *      order with respect to the subarray used upon array initialization.
 *
 * @param tiledb_array The TileDB array.
 * @param buffers An array of buffers, one for each attribute. These must be
 *     provided in the same order as the attributes specified in
 *     tiledb_array_init() or tiledb_array_reset_attributes(). The case of
 *     variable-sized attributes is special. Instead of providing a single
 *     buffer for such an attribute, **two** must be provided: the second
 *     will hold the variable-sized cell values, whereas the first holds the
 *     start offsets of each cell in the second buffer.
 * @param buffer_sizes The sizes (in bytes) allocated by the user for the input
 *     buffers (there is a one-to-one correspondence). The function will attempt
 *     to write as many results as can fit in the buffers, and potentially
 *     alter the buffer size to indicate the size of the *useful* data written
 *     in the buffer. If a buffer cannot hold all results, the function will
 *     still succeed, writing as much data as it can and turning on an overflow
 *     flag which can be checked with function tiledb_array_overflow(). The
 *     next invocation will resume from the point the previous one stopped,
 *     without inflicting a considerable performance penalty due to overflow.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_read(
    const tiledb_array_t* tiledb_array, void** buffers, size_t* buffer_sizes);

/**
 * Checks if a read operation for a particular attribute resulted in a
 * buffer overflow.
 *
 * @param tiledb_array The TileDB array.
 * @param attribute_id The id of the attribute for which the overflow is
 *     checked. This id corresponds to the position of the attribute name
 *     placed in the *attributes* input of tiledb_array_init(), or
 *     tiledb_array_reset_attributes() (the positions start from 0).
 *     If *attributes* was NULL in the
 *     above functions, then the attribute id corresponds to the order
 *     in which the attributes were defined in the array schema upon the
 *     array creation. Note that, in that case, the extra coordinates
 *     attribute corresponds to the last extra attribute, i.e., its id
 *     is *attribute_num*.
 * @return TILEDB_ERR for error, 1 for overflow, and 0 otherwise.
 */
TILEDB_EXPORT int tiledb_array_overflow(
    const tiledb_array_t* tiledb_array, int attribute_id);

/**
 * Consolidates the fragments of an array into a single fragment.
 *
 * @param ctx The TileDB context.
 * @param array The name of the TileDB array to be consolidated.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_consolidate(
    tiledb_ctx_t* ctx, const char* array);

/**
 * Finalizes a TileDB array, properly freeing its memory space.
 *
 * @param tiledb_array The array to be finalized.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_finalize(tiledb_array_t* tiledb_array);

/* ********************************* */
/*       DIRECTORY MANAGEMENT        */
/* ********************************* */

/**
 * Returns the type of the input directory.
 *
 * @param ctx The TileDB context.
 * @param dir The input directory.
 * @return It can be one of the following:
 *    - TILEDB_GROUP
 *    - TILEDB_ARRAY
 *    - TILEDB_METADATA
 *    - -1 (none of the above)
 */
TILEDB_EXPORT int tiledb_dir_type(tiledb_ctx_t* ctx, const char* dir);

/**
 * Clears a TileDB directory. The corresponding TileDB object
 * (group, array, or metadata) will still exist after the execution of the
 * function, but it will be empty (i.e., as if it was just created).
 *
 * @param ctx The TileDB context.
 * @param dir The TileDB directory to be cleared.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_clear(tiledb_ctx_t* ctx, const char* dir);

/**
 * Deletes a TileDB directory (group, array, or metadata) entirely.
 *
 * @param ctx The TileDB context.
 * @param dir The TileDB directory to be deleted.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_delete(tiledb_ctx_t* ctx, const char* dir);

/**
 * Moves a TileDB directory (group, array or metadata).
 *
 * @param ctx The TileDB context.
 * @param old_dir The old TileDB directory.
 * @param new_dir The new TileDB directory.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_move(
    tiledb_ctx_t* ctx, const char* old_dir, const char* new_dir);

/**
 * Lists all the TileDB objects in a directory, copying their names into the
 * input string buffers.
 *
 * @param ctx The TileDB context.
 * @param parent_dir The parent directory of the TileDB objects to be listed.
 * @param dirs An array of strings that will store the listed TileDB objects.
 *     Note that the user is responsible for allocating the appropriate memory
 *     space for this array of strings. A good idea is to allocate for each
 *     string TILEDB_NAME_MAX_LEN characters.
 * @param dir_types The types of the corresponding TileDB objects in *dirs*,
 *    which can be the following:
 *    - TILEDB_GROUP
 *    - TILEDB_ARRAY
 *    - TILEDB_METADATA
 * @param dir_num The number of elements allocated by the user for *dirs*. After
 *     the function terminates, this will hold the actual number of TileDB
 *     objects that were stored in *dirs*. If the number of
 *     allocated elements is smaller than the number of existing TileDB objects
 *     in the parent directory, the function will return an error.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_ls(
    tiledb_ctx_t* ctx,
    const char* parent_dir,
    char** dirs,
    tiledb_object_t* dir_types,
    int* dir_num);

/**
 * Counts the TileDB objects in a directory.
 *
 * @param ctx The TileDB context.
 * @param parent_dir The parent directory of the TileDB objects to be listed.
 * @param dir_num The number of TileDB objects to be returned.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_ls_c(
    tiledb_ctx_t* ctx, const char* parent_dir, int* dir_num);

/* ********************************* */
/*      ASYNCHRONOUS I/O (AIO)       */
/* ********************************* */

/**
 * Creates an AIO request.
 *
 * @param ctx The TileDB context
 * @param aio_request  The AIO request to be created.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_aio_request_create(
    tiledb_ctx_t* ctx, tiledb_aio_request_t** aio_request);

/**
 * Frees an AIO request.
 *
 * @param aio_request The AIO request to be freed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT void tiledb_aio_request_free(tiledb_aio_request_t* aio_request);

/**
 * Binds an array with the AIO request.
 *
 * @param ctx The TileDB context.
 * @param aio_request The AIO request to be set.
 * @param array The array to be set to the request.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_aio_request_set_array(
    tiledb_ctx_t* ctx,
    tiledb_aio_request_t* aio_request,
    tiledb_array_t* array);

/**
 * Sets buffers to the AIO request.
 *
 * @param ctx The TileDB context.
 * @param aio_request The AIO request to be set.
 * @param buffers The buffers to be set.
 * @param buffer_sizes The corresponding buffer sizes.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_aio_request_set_buffers(
    tiledb_ctx_t* ctx,
    tiledb_aio_request_t* aio_request,
    void** buffers,
    size_t* buffer_sizes);

/**
 * Sets a subarray to the AIO request.
 *
 * @param ctx The TileDB context.
 * @param aio_request The AIO request to be set.
 * @param subarray The subarray to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_aio_request_set_subarray(
    tiledb_ctx_t* ctx, tiledb_aio_request_t* aio_request, const void* subarray);

/**
 * Sets a callback handlde to the AIO request.
 *
 * @param ctx The TileDB context.
 * @param aio_request The AIO request to be set.
 * @param completion_handle The completion handle to be set.
 * @param completion_data The data to be passed to the completion handle.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_aio_request_set_callback(
    tiledb_ctx_t* ctx,
    tiledb_aio_request_t* aio_request,
    void* (*completion_handle)(void*),
    void* completion_data);

/**
 * Submits an asynchronous I/O request.
 *
 * @param ctx The TileDB context.
 * @param aio_request An asynchronous I/O request.
 * @return TILEDB_OK upon success, and TILEDB_ERR upon error.
 */
TILEDB_EXPORT int tiledb_array_aio_submit(
    tiledb_ctx_t* ctx, tiledb_aio_request_t* aio_request);

/**
 * Retrieves the status of the AIO request.
 *
 * @param ctx The TileDB context.
 * @param aio_request The AIO request.
 * @param aio_status The AIO status to be retrieved.
 * @return TILEDB_OK upon success, and TILEDB_ERR upon error.
 */
TILEDB_EXPORT int tiledb_aio_request_get_status(
    tiledb_ctx_t* ctx,
    tiledb_aio_request_t* aio_request,
    tiledb_aio_status_t* aio_status);

#undef TILEDB_EXPORT
#ifdef __cplusplus
}
#endif

#endif  // TILEDB_H
