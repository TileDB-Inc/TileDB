/**
 * @file   c_api.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
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

#ifndef __C_API_H__
#define __C_API_H__

#include "constants.h"
#ifdef HAVE_MPI
  #include <mpi.h>
#endif
#include <stdint.h>
#include <stddef.h>
#include <string>
#include <unistd.h>


/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/** Default error message. */
#define TILEDB_ERRMSG std::string("[TileDB] Error: ")

/** Maximum error message length. */
#define TILEDB_ERRMSG_MAX_LEN 2000




/* ********************************* */
/*               MACROS              */
/* ********************************* */

#ifdef __cplusplus
extern "C" {
#endif

#if (defined __GNUC__ && __GNUC__ >= 4) || defined __INTEL_COMPILER
#  define TILEDB_EXPORT __attribute__((visibility("default")))
#else
#  define TILEDB_EXPORT
#endif




/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

extern char tiledb_errmsg[TILEDB_ERRMSG_MAX_LEN];




/* ********************************* */
/*              CONFIG               */
/* ********************************* */

/** Used to pass congiguration parameters to TileDB. */
typedef struct TileDB_Config {
  /** 
   * The TileDB home directory. If it is set to "" (empty string) or NULL, the 
   * default home directory will be used, which is ~/.tiledb/. 
   */
  const char* home_;
#ifdef HAVE_MPI
  /** The MPI communicator. Use NULL if no MPI is used. */
  MPI_Comm* mpi_comm_; 
#endif
  /** 
   * The method for reading data from a file. 
   * It can be one of the following: 
   *    - TILEDB_IO_MMAP
   *      TileDB will use mmap.
   *    - TILEDB_IO_READ
   *      TileDB will use standard OS read.
   *    - TILEDB_IO_MPI
   *      TileDB will use MPI-IO read. 
   */
  int read_method_;
  /** 
   * The method for writing data to a file. 
   * It can be one of the following: 
   *    - TILEDB_IO_WRITE
   *      TileDB will use standard OS write.
   *    - TILEDB_IO_MPI
   *      TileDB will use MPI-IO write. 
   */
  int write_method_;
} TileDB_Config; 




/* ********************************* */
/*              CONTEXT              */
/* ********************************* */

/** The TileDB context, which maintains state for the TileDB modules. */
typedef struct TileDB_CTX TileDB_CTX; 

/** 
 * Initializes the TileDB context.  
 *
 * @param tiledb_ctx The TileDB context to be initialized.
 * @param tiledb_config TileDB configuration parameters. If it is NULL, 
 *     TileDB will use its default configuration parameters.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_ctx_init(
    TileDB_CTX** tiledb_ctx, 
    const TileDB_Config* tiledb_config);

/** 
 * Finalizes the TileDB context, properly freeing-up memory. 
 *
 * @param tiledb_ctx The TileDB context to be finalized.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_ctx_finalize(TileDB_CTX* tiledb_ctx);




/* ********************************* */
/*              WORKSPACE            */
/* ********************************* */

/**
 * Creates a new TileDB workspace.
 *
 * @param tiledb_ctx The TileDB context.
 * @param workspace The directory of the workspace to be created in the file 
 *     system. This directory should not be inside another TileDB workspace,
 *     group, array or metadata directory.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_workspace_create(
    const TileDB_CTX* tiledb_ctx,
    const char* workspace);



/* ********************************* */
/*                GROUP              */
/* ********************************* */

/**
 * Creates a new TileDB group.
 *
 * @param tiledb_ctx The TileDB context.
 * @param group The directory of the group to be created in the file system. 
 *     This should be a directory whose parent is a TileDB workspace or another 
 *     TileDB group.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_group_create(
    const TileDB_CTX* tiledb_ctx,
    const char* group);




/* ********************************* */
/*               ARRAY               */
/* ********************************* */

/** A TileDB array object. */
typedef struct TileDB_Array TileDB_Array;

/** The array schema. */
typedef struct TileDB_ArraySchema {
  /** 
   * The array name. It is a directory, whose parent must be a TileDB workspace,
   * or group.
   */
  char* array_name_;
  /** The attribute names. */
  char** attributes_;
  /** The number of attributes. */
  int attribute_num_;
  /** 
   * The tile capacity for the case of sparse fragments. If it is <=0,
   * TileDB will use its default.
   */
  int64_t capacity_;
  /** 
   * The cell order. It can be one of the following:
   *    - TILEDB_ROW_MAJOR
   *    - TILEDB_COL_MAJOR
   *    - TILEDB_HILBERT. 
   */
  int cell_order_;
  /**
   * Specifies the number of values per attribute for a cell. If it is NULL,
   * then each attribute has a single value per cell. If for some attribute
   * the number of values is variable (e.g., in the case off strings), then
   * TILEDB_VAR_NUM must be used.
   */
  int* cell_val_num_;
  /** 
   * The compression type for each attribute (plus one extra at the end for the
   * coordinates). It can be one of the following: 
   *    - TILEDB_NO_COMPRESSION
   *    - TILEDB_GZIP 
   *
   * If it is *NULL*, then the default TILEDB_NO_COMPRESSION is used for all
   * attributes.
   */
  int* compression_;
  /** 
   * Specifies if the array is dense (1) or sparse (0). If the array is dense, 
   * then the user must specify tile extents (see below).
   */
  int dense_;
  /** The dimension names. */
  char** dimensions_;
  /** The number of dimensions. */
  int dim_num_;
  /**  
   * The array domain. It should contain one [low, high] pair per dimension. 
   * The type of the values stored in this buffer should match the coordinates
   * type.
   */
  void* domain_;
  /** 
   * The tile extents. There should be one value for each dimension. The type of
   * the values stored in this buffer should match the coordinates type. It
   * can be NULL only for sparse arrays.
   */
  void* tile_extents_;
  /** 
   * The tile order. It can be one of the following:
   *    - TILEDB_ROW_MAJOR
   *    - TILEDB_COL_MAJOR. 
   */
  int tile_order_;
  /** 
   * The attribute types, plus an extra one in the end for the coordinates.
   * The attribute type can be one of the following: 
   *    - TILEDB_INT32
   *    - TILEDB_INT64
   *    - TILEDB_FLOAT32
   *    - TILEDB_FLOAT64
   *    - TILEDB_CHAR 
   *
   * The coordinate type can be one of the following: 
   *    - TILEDB_INT32
   *    - TILEDB_INT64
   *    - TILEDB_FLOAT32
   *    - TILEDB_FLOAT64
   */
  int* types_;
} TileDB_ArraySchema;

/**
 * Populates a TileDB array schema object.
 *
 * @oaram tiledb_array_schema The array schema to be populated.
 * @param array_name The array name.
 * @param attributes The attribute names.
 * @param attribute_num The number of attributes.
 * @param capacity The tile capacity.
 * @param cell_order The cell order.
 * @param cell_val_num The number of values per attribute per cell.
 * @param compression The compression type for each attribute (plus an extra one
 *     in the end for the coordinates).
 * @param dense Specifies if the array is dense (1) or sparse (0).
 * @param dimensions The dimension names.
 * @param dim_num The number of dimensions.
 * @param domain The array domain.
 * @param domain_len The length of *domain* in bytes.
 * @param tile_extents The tile extents.
 * @param tile_extents_len The length of *tile_extents* in bytes.
 * @param tile_order The tile order.
 * @param types The attribute types (plus one in the end for the coordinates).
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 * @see TileDB_ArraySchema
 */
TILEDB_EXPORT int tiledb_array_set_schema(
    TileDB_ArraySchema* tiledb_array_schema,
    const char* array_name,
    const char** attributes,
    int attribute_num,
    int64_t capacity,
    int cell_order,
    const int* cell_val_num,
    const int* compression,
    int dense,
    const char** dimensions,
    int dim_num,
    const void* domain,
    size_t domain_len,
    const void* tile_extents,
    size_t tile_extents_len,
    int tile_order,
    const int* types);

/**
 * Creates a new TileDB array.
 *
 * @param tiledb_ctx The TileDB context.
 * @param tiledb_array_schema The array schema. 
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_create(
    const TileDB_CTX* tiledb_ctx,
    const TileDB_ArraySchema* tiledb_array_schema);

/**
 * Initializes a TileDB array.
 *
 * @param tiledb_ctx The TileDB context.
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
    const TileDB_CTX* tiledb_ctx,
    TileDB_Array** tiledb_array,
    const char* array,
    int mode,
    const void* subarray,
    const char** attributes,
    int attribute_num);

/**
 * Resets the subarray used upon initialization of the array. This is useful
 * when the array is used for reading, and the user wishes to change the
 * query subarray without having to finalize and re-initialize the array.
 *
 * @param tiledb_array The TileDB array.
 * @param subarray The new subarray. It should be a sequence of [low, high]
 *     pairs (one pair per dimension), whose type should be the same as that of
 *     the coordinates. If it is NULL, then the subarray is set to the entire
 *     array domain. For the case of writes, this is meaningful only for
 *     dense arrays, and specifically dense writes.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_reset_subarray(
    const TileDB_Array* tiledb_array,
    const void* subarray);

/**
 * Resets the attributes used upon initialization of the array. 
 *
 * @param tiledb_array The TileDB array.
 * @param attributes The new attributes to focus on. If it is NULL, then
 *     all the attributes are used (including the coordinates in the case of
 *     sparse arrays, or sparse writes to dense arrays).
 * @param attribute_num The number of the attributes. If *attributes* is NULL,
 *     then this should be 0.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_reset_attributes(
    const TileDB_Array* tiledb_array,
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
    const TileDB_Array* tiledb_array,
    TileDB_ArraySchema* tiledb_array_schema);

/**
 * Retrieves the schema of an array from disk.
 *
 * @param tiledb_ctx The TileDB context.
 * @param array The directory of the array whose schema will be retrieved.
 * @param tiledb_array_schema The array schema to be retrieved. 
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_load_schema(
    const TileDB_CTX* tiledb_ctx,
    const char* array,
    TileDB_ArraySchema* tiledb_array_schema);

/**
 * Frees the input array schema struct, properly deallocating memory space.
 *
 * @param tiledb_array_schema The array schema to be freed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_free_schema(
    TileDB_ArraySchema* tiledb_array_schema);

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
    const TileDB_Array* tiledb_array,
    const void** buffers,
    const size_t* buffer_sizes);

/**
 * Performs a read operation on an array, which must be initialized with mode
 * TILEDB_ARRAY_READ, TILEDB_ARRAY_READ_SORTED_COL or 
 * TILEDB_ARRAY_READ_SORTED_ROW. The function retrieves the result cells that 
 * lie inside the subarray specified in tiledb_array_init() or 
 * tiledb_array_reset_subarray(). The results are written in input buffers 
 * provided by the user, which are also allocated by the user. Note that the
 * results are written in the buffers in the same order they appear on the
 * disk, which leads to maximum performance. 
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
    const TileDB_Array* tiledb_array,
    void** buffers,
    size_t* buffer_sizes);

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
    const TileDB_Array* tiledb_array,
    int attribute_id);

/**
 * Consolidates the fragments of an array into a single fragment. 
 * 
 * @param tiledb_ctx The TileDB context.
 * @param array The name of the TileDB array to be consolidated.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_consolidate(
    const TileDB_CTX* tiledb_ctx,
    const char* array);

/** 
 * Finalizes a TileDB array, properly freeing its memory space. 
 *
 * @param tiledb_array The array to be finalized.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_finalize(
    TileDB_Array* tiledb_array);

/** 
 * Syncs all currently written files in the input array. 
 *
 * @param tiledb_array The array to be synced.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_sync(
    TileDB_Array* tiledb_array);

/** 
 * Syncs the currently written files associated with the input attribute
 * in the input array. 
 *
 * @param tiledb_array The array to be synced.
 * @param attribute The name of the attribute to be synced.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_sync_attribute(
    TileDB_Array* tiledb_array,
    const char* attribute);


/** A TileDB array iterator. */
typedef struct TileDB_ArrayIterator TileDB_ArrayIterator;

/**
 * Initializes an array iterator for reading cells, potentially constraining it 
 * on a subset of attributes, as well as a subarray. The cells will be read
 * in the order they are stored on the disk, maximing performance. 
 *
 * @param tiledb_ctx The TileDB context.
 * @param tiledb_array_it The TileDB array iterator to be created. The function
 *     will allocate the appropriate memory space for the iterator. 
 * @param array The directory of the array the iterator is initialized for.
 * @param mode The read mode, which can be one of the following:
 *    - TILEDB_ARRAY_READ\n
 *      Reads the cells in the native order they are stored on the disk.
 *    - TILEDB_ARRAY_READ_SORTED_COL\n
 *      Reads the cells in column-major order within the specified subarray.
 *    - TILEDB_ARRAY_READ_SORTED_ROW\n
 *      Reads the cells in column-major order within the specified subarray.
 * @param subarray The subarray in which the array iterator will be
 *     constrained on. It should be a sequence of [low, high] pairs (one 
 *     pair per dimension), whose type should be the same as that of the
 *      coordinates. If it is NULL, then the subarray is set to the entire
 *     array domain. 
 * @param attributes A subset of the array attributes the iterator will be
 *     constrained on. Note that the coordinates have special attribute name
 *     TILEDB_COORDS. A NULL value indicates **all** attributes (including
 *     the coordinates as the last attribute in the case of sparse arrays).
 * @param attribute_num The number of the input attributes. If *attributes* is
 *     NULL, then this should be set to 0.
 * @param buffers This is an array of buffers similar to tiledb_array_read().
 *     It is the user that allocates and provides buffers that the iterator
 *     will use for internal buffering of the read cells. The iterator will
 *     read from the disk the relevant cells in batches, by fitting as many
 *     cell values as possible in the user buffers. This gives the user the
 *     flexibility to control the prefetching for optimizing performance 
 *     depending on the application. 
 * @param buffer_sizes The corresponding size (in bytes) of the allocated 
 *     memory space for *buffers*. The function will prefetch from the
 *     disk as many cells as can fit in the buffers, whenever it finishes
 *     iterating over the previously prefetched data.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_iterator_init(
    const TileDB_CTX* tiledb_ctx,
    TileDB_ArrayIterator** tiledb_array_it,
    const char* array,
    int mode,
    const void* subarray,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes);

/** 
 * Retrieves the current cell value for a particular attribute.
 *
 * @param tiledb_array_it The TileDB array iterator.
 * @param attribute_id The id of the attribute for which the cell value
 *     is retrieved. This id corresponds to the position of the attribute name
 *     placed in the *attributes* input of tiledb_array_iterator_init()
 *     (the positions start from 0). 
 *     If *attributes* was NULL in the above function, then the attribute id
 *     corresponds to the order in which the attributes were defined in the
 *     array schema upon the array creation. Note that, in that case, the extra
 *     coordinates attribute corresponds to the last extra attribute, i.e.,
 *     its id is *attribute_num*. 
 * @param value The cell value to be retrieved. Note that its type is the
 *     same as that defined in the array schema for the corresponding attribute.
 *     Note also that the function essentially returns a pointer to this value
 *     in the internal buffers of the iterator.
 * @param value_size The size (in bytes) of the retrieved value. Useful mainly
 *     for the case of variable-sized cells.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_iterator_get_value(
    TileDB_ArrayIterator* tiledb_array_it,
    int attribute_id, 
    const void** value,
    size_t* value_size);

/**
 * Advances the iterator by one cell.
 *
 * @param tiledb_array_it The TileDB array iterator.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_iterator_next(
    TileDB_ArrayIterator* tiledb_array_it);

/**
 * Checks if the the iterator has reached its end.
 *
 * @param tiledb_array_it The TileDB array iterator.
 * @return TILEDB_ERR for error, 1 for having reached the end, and 0 otherwise.
 */
TILEDB_EXPORT int tiledb_array_iterator_end(
    TileDB_ArrayIterator* tiledb_array_it);

/**
 * Finalizes an array iterator, properly freeing the allocating memory space.
 * 
 * @param tiledb_array_it The TileDB array iterator to be finalized.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_iterator_finalize(
    TileDB_ArrayIterator* tiledb_array_it);




/* ********************************* */
/*             METADATA              */
/* ********************************* */

/** Specifies the metadata schema. */
typedef struct TileDB_MetadataSchema {
  /** 
   * The metadata name. It is a directory, whose parent must be a TileDB
   * workspace, group, or array.
   */
  char* metadata_name_;
  /** The attribute names. */
  char** attributes_;
  /** The number of attributes. */
  int attribute_num_;
  /** 
   * The tile capacity. If it is <=0, TileDB will use its default.
   */
  int64_t capacity_;
  /**
   * Specifies the number of values per attribute for a cell. If it is NULL,
   * then each attribute has a single value per cell. If for some attribute
   * the number of values is variable (e.g., in the case off strings), then
   * TILEDB_VAR_NUM must be used.
   */
  int* cell_val_num_;
  /** 
   * The compression type for each attribute (plus one extra at the end for the
   * key). It can be one of the following: 
   *    - TILEDB_NO_COMPRESSION
   *    - TILEDB_GZIP 
   *
   * If it is *NULL*, then the default TILEDB_NO_COMPRESSION is used for all
   * attributes.
   */
  int* compression_;
  /** 
   * The attribute types.
   * The attribute type can be one of the following: 
   *    - TILEDB_INT32
   *    - TILEDB_INT64
   *    - TILEDB_FLOAT32
   *    - TILEDB_FLOAT64
   *    - TILEDB_CHAR. 
   */
  int* types_;
} TileDB_MetadataSchema;

/** A TileDB metadata object. */
typedef struct TileDB_Metadata TileDB_Metadata;

/**
 * Populates a TileDB metadata schema object.
 *
 * @param metadata_name The metadata name.
 * @param attributes The attribute names.
 * @param attribute_num The number of attributes.
 * @param capacity The tile capacity.
 * @param cell_val_num The number of values per attribute per cell.
 * @param compression The compression type for each attribute (plus an extra one
 *     in the end for the key).
 * @param types The attribute types.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 * @see TileDB_MetadataSchema
 */
TILEDB_EXPORT int tiledb_metadata_set_schema(
    TileDB_MetadataSchema* tiledb_metadata_schema,
    const char* metadata_name,
    const char** attributes,
    int attribute_num,
    int64_t capacity,
    const int* cell_val_num,
    const int* compression,
    const int* types);

/**
 * Creates a new TileDB metadata object.
 *
 * @param tiledb_ctx The TileDB context.
 * @param metadata_schema The metadata schema. 
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_metadata_create(
    const TileDB_CTX* tiledb_ctx,
    const TileDB_MetadataSchema* metadata_schema);

/**
 * Initializes a TileDB metadata object.
 *
 * @param tiledb_ctx The TileDB context.
 * @param tiledb_metadata The metadata object to be initialized. The function
 *     will allocate memory space for it.
 * @param metadata The directory of the metadata to be initialized.
 * @param mode The mode of the metadata. It must be one of the following:
 *    - TILEDB_METADATA_WRITE 
 *    - TILEDB_METADATA_READ 
 * @param attributes A subset of the metadata attributes the read/write will be
 *     constrained on. Note that the keys have a special attribute name
 *     called TILEDB_KEYS. A NULL value indicates **all** attributes (including
 *     the keys as an extra attribute in the end).
 * @param attribute_num The number of the input attributes. If *attributes* is
 *     NULL, then this should be set to 0.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_metadata_init(
    const TileDB_CTX* tiledb_ctx,
    TileDB_Metadata** tiledb_metadata,
    const char* metadata,
    int mode,
    const char** attributes,
    int attribute_num);

/**
 * Resets the attributes used upon initialization of the metadata. 
 *
 * @param tiledb_metadata The TileDB metadata.
 * @param attributes The new attributes to focus on. If it is NULL, then
 *     all the attributes are used.
 * @param attribute_num The number of the attributes. If *attributes* is NULL,
 *     then this should be 0.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_metadata_reset_attributes(
    const TileDB_Metadata* tiledb_metadata,
    const char** attributes,
    int attribute_num);

/**
 * Retrieves the schema of an already initialized metadata object.
 *
 * @param tiledb_ctx The TileDB context.
 * @param tiledb_metadata The TileDB metadata object (must already be 
 *     initialized). 
 * @param tiledb_metadata_schema The metadata schema to be retrieved. 
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_metadata_get_schema(
    const TileDB_Metadata* tiledb_metadata,
    TileDB_MetadataSchema* tiledb_metadata_schema);

/**
 * Retrieves the schema of a metadata object from disk.
 *
 * @param tiledb_ctx The TileDB context.
 * @param metadata The directory of the metadata whose schema will be retrieved.
 * @param tiledb_metadata_schema The metadata schema to be retrieved. 
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_metadata_load_schema(
    const TileDB_CTX* tiledb_ctx,
    const char* metadata,
    TileDB_MetadataSchema* tiledb_metadata_schema);

/**
 * Frees the input metadata schema struct, properly deallocating memory space.
 *
 * @param tiledb_metadata_schema The metadata schema to be freed.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_metadata_free_schema(
    TileDB_MetadataSchema* tiledb_metadata_schema);

/**
 * Performs a write operation to a metadata object. The metadata must be
 * initialized with mode TILEDB_METADATA_WRITE. This function behave very
 * similarly to tiledb_array_write() when the array is initialized with mode
 * TILEDB_ARRAY_WRITE_UNSORTED.
 * 
 * @param tiledb_metadata The TileDB metadata (must be already initialized).
 * @param keys The buffer holding the metadata keys. These keys must be
 *     strings, serialized one after the other in the *keys* buffer.
 * @param keys_size The size (in bytes) of buffer *keys*.
 * @param buffers An array of buffers, one for each attribute. These must be
 *     provided in the same order as the attributes specified in
 *     tiledb_metadata_init() or tiledb_metadata_reset_attributes(). The case of
 *     variable-sized attributes is special. Instead of providing a single
 *     buffer for such an attribute, **two** must be provided: the second
 *     holds the variable-sized values, whereas the first holds the
 *     start offsets of each value in the second buffer.
 * @param buffer_sizes The sizes (in bytes) of the input buffers (there is
 *     a one-to-one correspondence).
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_metadata_write(
    const TileDB_Metadata* tiledb_metadata,
    const char* keys,
    size_t keys_size,
    const void** buffers,
    const size_t* buffer_sizes);

/**
 * Performs a read operation on a metadata object, which must be initialized
 * with mode TILEDB_METADATA_READ. The read is performed on a single key. 
 * 
 * @param tiledb_metadata The TileDB metadata.
 * @param key The query key, which must be a string.
 * @param buffers An array of buffers, one for each attribute. These must be
 *     provided in the same order as the attributes specified in
 *     tiledb_metadata_init() or tiledb_metadata_reset_attributes(). The case of
 *     variable-sized attributes is special. Instead of providing a single
 *     buffer for such an attribute, **two** must be provided: the second
 *     will hold the variable-sized values, whereas the first holds the
 *     start offsets of each value in the second buffer.
 * @param buffer_sizes The sizes (in bytes) allocated by the user for the input
 *     buffers (there should be a one-to-one correspondence). The function will
 *     attempt to write the value corresponding to the key, and potentially
 *     alter the respective size in *buffer_sizes* to indicate the *useful*
 *     data written. If a buffer cannot
 *     hold the result, the function will still succeed, turning on an overflow
 *     flag which can be checked with function tiledb_metadata_overflow(). 
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_metadata_read(
    const TileDB_Metadata* tiledb_metadata,
    const char* key,
    void** buffers,
    size_t* buffer_sizes);

/**
 * Checks if a read operation for a particular attribute resulted in a
 * buffer overflow.
 * 
 * @param tiledb_metadata The TileDB metadata.
 * @param attribute_id The id of the attribute for which the overflow is
 *     checked. This id corresponds to the position of the attribute name
 *     placed in the *attributes* input of tiledb_metadata_init(), or 
 *     tiledb_metadata_reset_attributes(). The positions start from 0.
 *     If *attributes* was NULL in the
 *     above functions, then the attribute id corresponds to the order
 *     in which the attributes were defined in the metadata schema upon the
 *     metadata creation.  
 * @return TILEDB_ERR for error, 1 for overflow, and 0 otherwise.
 */
TILEDB_EXPORT int tiledb_metadata_overflow(
    const TileDB_Metadata* tiledb_metadata,
    int attribute_id);

/**
 * Consolidates the fragments of a metadata object into a single fragment. 
 * 
 * @param tiledb_ctx The TileDB context.
 * @param metadata The name of the TileDB metadata to be consolidated.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_metadata_consolidate(
    const TileDB_CTX* tiledb_ctx,
    const char* metadata);

/** 
 * Finalizes a TileDB metadata object, properly freeing the memory space. 
 *
 * @param tiledb_metadata The metadata to be finalized.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_metadata_finalize(
    TileDB_Metadata* tiledb_metadata);

/** A TileDB metadata iterator. */
typedef struct TileDB_MetadataIterator TileDB_MetadataIterator;

/**
 * Initializes a metadata iterator, potentially constraining it 
 * on a subset of attributes. The values will be read in the order they are
 * stored on the disk (which is random), maximing performance. 
 *
 * @param tiledb_ctx The TileDB context.
 * @param tiledb_metadata_it The TileDB metadata iterator to be created. The
 *     function will allocate the appropriate memory space for the iterator. 
 * @param metadata The directory of the metadata the iterator is initialized
 *     for.
 * @param attributes A subset of the metadata attributes the iterator will be
 *     constrained on. Note that the keys have a special value called 
 *     TILEDB_KEYS. A NULL value indicates **all** attributes.
 * @param attribute_num The number of the input attributes. If *attributes* is
 *     NULL, then this should be set to 0.
 * @param buffers This is an array of buffers similar to tiledb_metadata_read().
 *     It is the user that allocates and provides buffers that the iterator
 *     will use for internal buffering of the read values. The iterator will
 *     read from the disk the values in batches, by fitting as many
 *     values as possible in the user buffers. This gives the user the
 *     flexibility to control the prefetching for optimizing performance 
 *     depending on the application. 
 * @param buffer_sizes The corresponding sizes (in bytes) of the allocated 
 *     memory space for *buffers*. The function will prefetch from the
 *     disk as many values as can fit in the buffers, whenever it finishes
 *     iterating over the previously prefetched data.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_metadata_iterator_init(
    const TileDB_CTX* tiledb_ctx,
    TileDB_MetadataIterator** tiledb_metadata_it,
    const char* metadata,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes);

/** 
 * Retrieves the current value for a particular attribute.
 *
 * @param tiledb_metadata_it The TileDB metadata iterator.
 * @param attribute_id The id of the attribute for which the overflow is
 *     checked. This id corresponds to the position of the attribute name
 *     placed in the *attributes* input of tiledb_metadata_init(), or 
 *     tiledb_metadata_reset_attributes(). The positions start from 0.
 *     If *attributes* was NULL in the
 *     above functions, then the attribute id corresponds to the order
 *     in which the attributes were defined in the metadata schema upon the
 *     metadata creation. 
 * @param value The value to be retrieved. Note that its type is the
 *     same as that defined in the metadata schema. Note also that the function
 *     returns a pointer to this value in the internal buffers of the iterator.
 * @param value_size The size (in bytes) of the retrieved value.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_metadata_iterator_get_value(
    TileDB_MetadataIterator* tiledb_metadata_it,
    int attribute_id, 
    const void** value,
    size_t* value_size);

/**
 * Advances the iterator by one position.
 *
 * @param tiledb_metadata_it The TileDB metadata iterator.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_metadata_iterator_next(
    TileDB_MetadataIterator* tiledb_metadata_it);

/**
 * Checks if the the iterator has reached its end.
 *
 * @param tiledb_metadata_it The TileDB metadata iterator.
 * @return TILEDB_ERR for error, 1 for having reached the end, and 0 otherwise.
 */
TILEDB_EXPORT int tiledb_metadata_iterator_end(
    TileDB_MetadataIterator* tiledb_metadata_it);

/**
 * Finalizes the iterator, properly freeing the allocating memory space.
 * 
 * @param tiledb_metadata_it The TileDB metadata iterator.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_metadata_iterator_finalize(
    TileDB_MetadataIterator* tiledb_metadata_it);





/* ********************************* */
/*       DIRECTORY MANAGEMENT        */
/* ********************************* */

/**
 * Clears a TileDB directory. The corresponding TileDB object (workspace,
 * group, array, or metadata) will still exist after the execution of the
 * function, but it will be empty (i.e., as if it was just created).
 *
 * @param tiledb_ctx The TileDB context.
 * @param dir The TileDB directory to be cleared.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_clear( 
    const TileDB_CTX* tiledb_ctx, 
    const char* dir);

/**
 * Deletes a TileDB directory (workspace, group, array, or metadata) entirely. 
 *
 * @param tiledb_ctx The TileDB context.
 * @param dir The TileDB directory to be deleted.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_delete( 
    const TileDB_CTX* tiledb_ctx, 
    const char* dir);

/**
 * Moves a TileDB directory (workspace, group, array or metadata).
 *
 * @param tiledb_ctx The TileDB context.
 * @param old_dir The old TileDB directory.
 * @param new_dir The new TileDB directory.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_move( 
    const TileDB_CTX* tiledb_ctx, 
    const char* old_dir,
    const char* new_dir);

/**
 * Lists all TileDB workspaces, copying their directory names in the input
 * string buffers.
 *
 * @param tiledb_ctx The TileDB context.
 * @param workspaces An array of strings that will store the listed workspaces.
 *     Note that this should be pre-allocated by the user. If the size of
 *     each string is smaller than the corresponding workspace path name, the
 *     function will probably segfault. It is a good idea to allocate to each
 *     workspace string TILEDB_NAME_MAX_LEN characters. 
 * @param workspace_num The number of allocated elements of the *workspaces*
 *     string array. After the function execution, this will hold the actual
 *     number of workspaces written in the *workspaces* string array. If the
 *     number of allocated elements is smaller than the number of existing
 *     TileDB workspaces, the function will return an error.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_ls_workspaces(
    const TileDB_CTX* tiledb_ctx,
    char** workspaces,
    int* workspace_num);

/**
 * Counts the number of TileDB workspaces.
 *
 * @param tiledb_ctx The TileDB context.
 * @param workspace_num The number of TileDB workspaces to be returned.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_ls_workspaces_c(
    const TileDB_CTX* tiledb_ctx,
    int* workspace_num);

/**
 * Lists all the TileDB objects in a directory, copying their names into the 
 * input string buffers.
 *
 * @param tiledb_ctx The TileDB context.
 * @param parent_dir The parent directory of the TileDB objects to be listed.
 * @param dirs An array of strings that will store the listed TileDB objects.
 *     Note that the user is responsible for allocating the appropriate memory
 *     space for this array of strings. A good idea is to allocate for each
 *     string TILEDB_NAME_MAX_LEN characters.
 * @param dir_types The types of the corresponding TileDB objects in *dirs*, 
 *    which can be the following:
 *    - TILEDB_WORKSPACE
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
    const TileDB_CTX* tiledb_ctx,
    const char* parent_dir,
    char** dirs,
    int* dir_types,
    int* dir_num);

/**
 * Counts the TileDB objects in a directory.
 *
 * @param tiledb_ctx The TileDB context.
 * @param parent_dir The parent directory of the TileDB objects to be listed.
 * @param dir_num The number of TileDB objects to be returned. 
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_ls_c(
    const TileDB_CTX* tiledb_ctx,
    const char* parent_dir,
    int* dir_num);




/* ********************************* */
/*      ASYNCHRONOUS I/O (AIO)       */
/* ********************************* */

/** Describes an AIO (read or write) request. */
typedef struct TileDB_AIO_Request {
  /**
   * An array of buffers, one for each attribute. These must be
   * provided in the same order as the attributes specified in
   * tiledb_array_init() or tiledb_array_reset_attributes(). The case of
   * variable-sized attributes is special. Instead of providing a single
   * buffer for such an attribute, **two** must be provided: the second
   * holds the variable-sized cell values, whereas the first holds the
   * start offsets of each cell in the second buffer.
   */
  void** buffers_;
  /**
   * The sizes (in bytes) allocated by the user for the 
   * buffers (there is a one-to-one correspondence). In the case of reads,
   * the function will attempt
   * to write as many results as can fit in the buffers, and potentially
   * alter the buffer sizes to indicate the size of the *useful* data written
   * in the corresponding buffers. 
   */
  size_t* buffer_sizes_;
  /** Function to be called upon completion of the request. */
  void *(*completion_handle_) (void*);
  /** Data to be passed to the completion handle. */
  void* completion_data_;
  /** 
   * Applicable only to read requests.
   * Indicates whether a buffer has overflowed during a read request.
   * If it is NULL, it will be ignored. Otherwise, it must be an array
   * with as many elements as the number of attributes specified in
   * tiledb_array_init() or tiledb_array_reset_attributes(). 
   */
  bool* overflow_;
  /**
   * The status of the AIO request. It can be one of the following:
   *    - TILEDB_AIO_COMPLETED
   *      The request is completed.
   *    - TILEDB_AIO_INPROGRESS
   *      The request is still in progress.
   *    - TILEDB_AIO_OVERFLOW
   *      At least one of the input buffers overflowed (applicable only to AIO 
   *      read requests)
   *    - TILEDB_AIO_ERR 
   *      The request caused an error (and thus was canceled). 
   */
  int status_;
  /** 
   * The subarray in which the array read/write will be
   * constrained on. It should be a sequence of [low, high] pairs (one 
   * pair per dimension), whose type should be the same as that of the
   * coordinates. If it is NULL, then the subarray is set to the entire
   * array domain. For the case of writes, this is meaningful only for
   * dense arrays, and specifically dense writes.
   */
  const void* subarray_;
} TileDB_AIO_Request; 

/**
 * Issues an asynchronous read request. 
 *
 * @param tiledb_array An initialized TileDB array.
 * @param tiledb_aio_request An asynchronous read request.
 * @return TILEDB_OK upon success, and TILEDB_ERR upon error.
 *
 * @note If the same input request is in progress, the function will fail.
 *     Moreover, if the input request was issued in the past and caused an
 *     overflow, the new call will resume it IF there was no other request
 *     in between the two separate calls for the same input request.
 *     In other words, a new request that is different than the previous
 *     one resets the internal read state.
 */
TILEDB_EXPORT int tiledb_array_aio_read( 
    const TileDB_Array* tiledb_array,
    TileDB_AIO_Request* tiledb_aio_request);

/**
 * Issues an asynchronous write request. 
 *
 * @param tiledb_array An initialized TileDB array.
 * @param tiledb_aio_request An asynchronous write request.
 * @return TILEDB_OK upon success, and TILEDB_ERR upon error.
 */
TILEDB_EXPORT int tiledb_array_aio_write( 
    const TileDB_Array* tiledb_array,
    TileDB_AIO_Request* tiledb_aio_request);


#undef TILEDB_EXPORT
#ifdef __cplusplus
}
#endif

#endif
