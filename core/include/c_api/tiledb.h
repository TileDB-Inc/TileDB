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
#define TILEDB_VERSION "1.0.0"
#define TILEDB_VERSION_MAJOR 1
#define TILEDB_VERSION_MINOR 0
#define TILEDB_VERSION_REVISION 0

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

/** Returns a special name indicating the metadata key attribute. */
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
#define TILEDB_OBJECT_TYPE_ENUM(id) TILEDB_##id
#include "tiledb_enum.inc"
#undef TILEDB_OBJECT_TYPE_ENUM
} tiledb_object_type_t;

/** Array mode. */
typedef enum {
#define TILEDB_QUERY_TYPE_ENUM(id) TILEDB_##id
#include "tiledb_enum.inc"
#undef TILEDB_QUERY_TYPE_ENUM
} tiledb_query_type_t;

/** Metadata mode. */
typedef enum {
#define TILEDB_METADATA_ENUM(id) TILEDB_METADATA_##id
#include "tiledb_enum.inc"
#undef TILEDB_METADATA_ENUM
} tiledb_metadata_mode_t;

/** I/O method. */  // TODO: find another name
typedef enum {
#define TILEDB_IO_METHOD_ENUM(id) TILEDB_IO_METHOD_##id
#include "tiledb_enum.inc"
#undef TILEDB_IO_METHOD_ENUM
} tiledb_io_t;

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

/** Query status. */
typedef enum {
#define TILEDB_QUERY_STATUS_ENUM(id) TILEDB_##id
#include "tiledb_enum.inc"
#undef TILEDB_QUERY_STATUS_ENUM
} tiledb_query_status_t;

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

/** A TileDB array query. */
typedef struct tiledb_query_t tiledb_query_t;

/** A TileDB array object. */
typedef struct tiledb_array_t tiledb_array_t;

/** A TileDB array iterator. */
typedef struct tiledb_array_iter_t tiledb_array_iter_t;

/** Specifies the metadata schema. */
typedef struct tiledb_metadata_schema_t tiledb_metadata_schema_t;

/** A TileDB metadata object. */
typedef struct tiledb_metadata_t tiledb_metadata_t;

/** A TileDB metadata iterator. */
typedef struct tiledb_metadata_iter_t tiledb_metadata_iter_t;

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
 * @param config The configurator to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 *
 * @note It is strongly recommended that this function is used before starting
 *    to use any arrays/metadata/groups, as messing with the configuration
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
 * @param config The configurator.
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
 * @param config The configurator.
 * @param read_method The read method to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_config_set_read_method(
    tiledb_ctx_t* ctx, tiledb_config_t* config, tiledb_io_t read_method);

/**
 * Sets the write method.
 *
 * @param ctx The TileDB context.
 * @param config The configurator.
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
 * Creates an attribute iterator for the input schema.
 *
 * @param ctx The TileDB context.
 * @param schema The input array or metadata schema.
 * @param attr_it The attribute iterator to be created.
 * @param object_type This should be either TILEDB_METADATA or TILEDB_ARRAY.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_attribute_iter_create(
    tiledb_ctx_t* ctx,
    const void* schema,
    tiledb_attribute_iter_t** attr_it,
    tiledb_object_type_t object_type);

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
/*               QUERY               */
/* ********************************* */

TILEDB_EXPORT int tiledb_query_create(
    tiledb_ctx_t* ctx,
    tiledb_query_t** query,
    void* object,
    tiledb_object_type_t object_type,
    tiledb_query_type_t* query_type);

TILEDB_EXPORT void tiledb_query_free(tiledb_array_t* query);

TILEDB_EXPORT int tiledb_query_set_async(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    void* (*callback)(void*),
    void* callback_data);

TILEDB_EXPORT int tiledb_array_set_subarray(
    tiledb_ctx_t* ctx, tiledb_query_t* query, const void* subarray);

TILEDB_EXPORT int tiledb_query_set_attribute(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* attr,
    void* buffer,
    uint64_t buffer_size);

TILEDB_EXPORT int tiledb_query_set_var_attribute(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* attr,
    void* buffer,
    uint64_t buffer_size,
    void* buffer_var,
    uint64_t buffer_var_size);

TILEDB_EXPORT int tiledb_query_set_dimension(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* dim,
    void* buffer,
    uint64_t buffer_size);

TILEDB_EXPORT int tiledb_query_get_status(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_query_status_t* status);

TILEDB_EXPORT int tiledb_query_get_overflow(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* name,
    bool* overflow);

TILEDB_EXPORT int tiledb_query_check(tiledb_ctx_t* ctx, tiledb_query_t* query);

TILEDB_EXPORT int tiledb_query_process(
    tiledb_ctx_t* ctx, tiledb_query_t* query);

/* ********************************* */
/*               ARRAY               */
/* ********************************* */

TILEDB_EXPORT int tiledb_array_create(
    tiledb_ctx_t* ctx, const tiledb_array_schema_t* array_schema);

TILEDB_EXPORT int tiledb_array_open(
    tiledb_ctx_t* ctx, tiledb_array_t** tiledb_array, const char* array);

TILEDB_EXPORT int tiledb_array_close(
    tiledb_ctx_t* ctx, tiledb_array_t* tiledb_array);

TILEDB_EXPORT int tiledb_array_consolidate(
    tiledb_ctx_t* ctx, const char* array);

TILEDB_EXPORT int tiledb_array_sync(
    tiledb_ctx_t* ctx, tiledb_array_t* tiledb_array);

TILEDB_EXPORT int tiledb_array_sync_attribute(
    tiledb_ctx_t* ctx, tiledb_array_t* tiledb_array, const char* attr);

/* ********************************* */
/*           ARRAY ITERATOR          */
/* ********************************* */

/*
TILEDB_EXPORT int tiledb_array_iter_create(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_array_iter_t** it,
    tiledb_array_query_t* query);

TILEDB_EXPORT int tiledb_array_iter_free(tiledb_array_iter_t* it);

TILEDB_EXPORT int tiledb_array_iter_here(
    tiledb_ctx_t* ctx,
    tiledb_array_iter_t* it,
    int attribute_id,
    const void** value,
    uint64_t* value_size);

TILEDB_EXPORT int tiledb_array_iterator_next(
    tiledb_ctx_t* ctx,
    tiledb_array_iter_t* it);

TILEDB_EXPORT int tiledb_array_iterator_done(
    tiledb_ctx_t* ctx,
    tiledb_array_iter_t* it);

TILEDB_EXPORT void tiledb_array_iterator_free(
    tiledb_array_iter_t* it);

    */

/* ********************************* */
/*          METADATA SCHEMA          */
/* ********************************* */

/**
 * Creates a TileDB a metadata schema object.
 *
 * @param ctx The TileDB context.
 * @param metadata_schema The TileDB metadata schema to be created.
 * @param metadata_name The metadata name.
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_metadata_schema_create(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t** metadata_schema,
    const char* metadata_name);

/**
 * Destroys a metadata schema, freeing-up memory.
 *
 * @param metadata_schema The array schema to be destroyed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT void tiledb_metadata_schema_free(
    tiledb_metadata_schema_t* metadata_schema);

/**
 * Adds an attribute to a metadata schema.
 *
 * @param ctx The TileDB context.
 * @param metadata_schema The metadata schema.
 * @param attr The attribute to be added.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_metadata_schema_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t* metadata_schema,
    tiledb_attribute_t* attr);

/**
 * Sets the tile capacity.
 *
 * @param ctx The TileDB context.
 * @param metadata_schema The metadata schema.
 * @param capacity The capacity to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_metadata_schema_set_capacity(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t* metadata_schema,
    uint64_t capacity);

/**
 * Sets the cell order.
 *
 * @param ctx The TileDB context.
 * @param metadata_schema The metadata schema.
 * @param cell_order The cell order to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_metadata_schema_set_cell_order(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t* metadata_schema,
    tiledb_layout_t cell_order);

/**
 * Sets the tile order.
 *
 * @param ctx The TileDB context.
 * @param metadata_schema The metadata schema.
 * @param tile_order The tile order to be set.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_metadata_schema_set_tile_order(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t* metadata_schema,
    tiledb_layout_t tile_order);

/**
 * Checks the correctness of the metadata schema.
 *
 * @param ctx The TileDB context.
 * @param metadata_schema The metadata schema.
 * @return TILEDB_OK if the metadata schema is correct and TILEDB_ERR upon any
 * error.
 */
TILEDB_EXPORT int tiledb_metadata_schema_check(
    tiledb_ctx_t* ctx, tiledb_metadata_schema_t* metadata_schema);

/**
 * Retrieves the schema of a metadata object from disk.
 *
 * @param ctx The TileDB context.
 * @param metadata_schema The metadata schema to be retrieved, or NULL upon
 * error.
 * @param metadata_name The metadata whose schema will be retrieved.
 * @return TILEDB_OK if the metadata schema is correct and TILEDB_OOM or
 * TILEDB_ERR upon error.
 */
TILEDB_EXPORT int tiledb_metadata_schema_load(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t** metadata_schema,
    const char* metadata_name);

/**
 * Retrieves the metadata name.
 *
 * @param ctx The TileDB context.
 * @param metadata_schema The metadata schema.
 * @param metadata_name The metadata name to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_metadata_schema_get_metadata_name(
    tiledb_ctx_t* ctx,
    const tiledb_metadata_schema_t* metadata_schema,
    const char** metadata_name);

/**
 * Retrieves the capacity.
 *
 * @param ctx The TileDB context.
 * @param metadata_schema The metadata schema.
 * @param capacity The capacity to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_metadata_schema_get_capacity(
    tiledb_ctx_t* ctx,
    const tiledb_metadata_schema_t* metadata_schema,
    uint64_t* capacity);

/**
 * Retrieves the cell order.
 *
 * @param ctx The TileDB context.
 * @param metadata_schema The metadata schema.
 * @param cell_order The cell order to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_metadata_schema_get_cell_order(
    tiledb_ctx_t* ctx,
    const tiledb_metadata_schema_t* metadata_schema,
    tiledb_layout_t* cell_order);

/**
 * Retrieves the tile order.
 *
 * @param ctx The TileDB context.
 * @param metadata_schema The metadata schema.
 * @param tile_order The tile order to be retrieved.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_metadata_schema_get_tile_order(
    tiledb_ctx_t* ctx,
    const tiledb_metadata_schema_t* metadata_schema,
    tiledb_layout_t* tile_order);

/**
 * Dumps the metadata schema in ASCII format in the selected output.
 *
 * @param ctx The TileDB context.
 * @param metadata_schema The metadata schema.
 * @param out The output.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_metadata_schema_dump(
    tiledb_ctx_t* ctx,
    const tiledb_metadata_schema_t* metadata_schema,
    FILE* out);

/* ********************************* */
/*             METADATA              */
/* ********************************* */

/*

TILEDB_EXPORT int tiledb_metadata_query_create(
        tiledb_ctx_t* ctx,
        tiledb_metadata_t* metadata,
        tiledb_metadata_query_t** query);

TILEDB_EXPORT void tiledb_metadata_query_free(tiledb_metadata_query_t* query);

TILEDB_EXPORT int tiledb_metadata_query_set_io_mode(
        tiledb_ctx_t* ctx,
        tiledb_metadata_query_t* query,
        tiledb_io_mode_t io_mode);

TILEDB_EXPORT int tiledb_metadata_query_set_access_mode(
        tiledb_ctx_t* ctx,
        tiledb_array_query_t* query,
        tiledb_metadata_mode_t query_mode);

TILEDB_EXPORT int tiledb_metadata_query_add_attribute(
        tiledb_ctx_t* ctx,
        tiledb_metadata_query_t* query,
        const char* attr_name,
        void* buffer,
        size_t buffer_size);

TILEDB_EXPORT int tiledb_metadata_query_add_var_attribute(
        tiledb_ctx_t* ctx,
        tiledb_metadata_query_t* query,
        const char* attr,
        void* buffer,
        size_t buffer_size,
        void* buffer_var,
        size_t buffer_var_size);

TILEDB_EXPORT int tiledb_metadata_query_add_dimension(
        tiledb_ctx_t* ctx,
        tiledb_metadata_query_t* query,
        const char* dim,
        void* buffer,
        size_t buffer_size);

TILEDB_EXPORT int tiledb_metadata_open(
    tiledb_ctx_t* ctx,
    tiledb_metadata_t** tiledb_metadata,
    const char* metadata);

TILEDB_EXPORT int tiledb_metadata_submit_query(
    tiledb_ctx_t* ctx,
    const tiledb_metadata_t* tiledb_metadata,
    const char* keys,
    size_t keys_size,
    const void** buffers,
    const size_t* buffer_sizes);

TILEDB_EXPORT int tiledb_metadata_read(
    const tiledb_metadata_t* tiledb_metadata,
    const char* key,
    void** buffers,
    size_t* buffer_sizes);

TILEDB_EXPORT int tiledb_metadata_overflow(
    const tiledb_metadata_t* tiledb_metadata, int attribute_id);

TILEDB_EXPORT int tiledb_metadata_consolidate(
    tiledb_ctx_t* ctx, const char* metadata);

TILEDB_EXPORT int tiledb_metadata_finalize(tiledb_metadata_t* tiledb_metadata);

TILEDB_EXPORT int tiledb_metadata_iterator_init(
    tiledb_ctx_t* ctx,
    tiledb_metadata_iter_t** tiledb_metadata_it,
    const char* metadata,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes);

TILEDB_EXPORT int tiledb_metadata_iterator_get_value(
    tiledb_metadata_iter_t* tiledb_metadata_it,
    int attribute_id,
    const void** value,
    size_t* value_size);

TILEDB_EXPORT int tiledb_metadata_iterator_next(
    tiledb_metadata_iter_t* tiledb_metadata_it);

TILEDB_EXPORT int tiledb_metadata_iterator_end(
    tiledb_metadata_iter_t* tiledb_metadata_it);

TILEDB_EXPORT int tiledb_metadata_iterator_finalize(
    tiledb_metadata_iter_t* tiledb_metadata_it);
*/

/* ********************************* */
/*       DIRECTORY MANAGEMENT        */
/* ********************************* */

/* TODO
TILEDB_EXPORT int tiledb_dir_type(tiledb_ctx_t* ctx, const char* dir);


TILEDB_EXPORT int tiledb_clear(tiledb_ctx_t* ctx, const char* dir);


TILEDB_EXPORT int tiledb_delete(tiledb_ctx_t* ctx, const char* dir);

TILEDB_EXPORT int tiledb_move(
    tiledb_ctx_t* ctx, const char* old_dir, const char* new_dir);

TILEDB_EXPORT int tiledb_ls(
    tiledb_ctx_t* ctx,
    const char* parent_dir,
    char** dirs,
    tiledb_object_t* dir_types,
    int* dir_num);

TILEDB_EXPORT int tiledb_ls_c(
    tiledb_ctx_t* ctx, const char* parent_dir, int* dir_num);
*/

#undef TILEDB_EXPORT
#ifdef __cplusplus
}
#endif

#endif  // TILEDB_H
