/**
 * @file   c_api.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2015 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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

#include <stdint.h>
#include <stddef.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

#if (defined __GNUC__ && __GNUC__ >= 4) || defined __INTEL_COMPILER
#  define TILEDB_EXPORT __attribute__((visibility("default")))
#else
#  define TILEDB_EXPORT
#endif

/* ********************************* */
/*              CONTEXT              */
/* ********************************* */

/** The TileDB context. */
typedef struct TileDB_CTX TileDB_CTX; 

/** 
 * Initializes the TileDB context.  
 *
 * @param tiledb_ctx The TileDB context to be initialized.
 * @param config_filename The name of the configuration file. If it is NULL or
 *     not found, TileDB will use its default configuration parameters.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 * @see tiledb_ctx_finalize
 */
TILEDB_EXPORT int tiledb_ctx_init(
    TileDB_CTX** tiledb_ctx, 
    const char* config_filename);

/** 
 * Finalizes the TileDB context. 
 *
 * @param tiledb_ctx The TileDB context to be finalized.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 * @see tiledb_ctx_init
 */
TILEDB_EXPORT int tiledb_ctx_finalize(TileDB_CTX* tiledb_ctx);

/* ********************************* */
/*              WORKSPACE            */
/* ********************************* */

/**
 * Creates a new TileDB workspace.
 *
 * @param tiledb_ctx The TileDB context.
 * @param dir The directory to the workspace to be created in the file system.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_workspace_create(
    const TileDB_CTX* tiledb_ctx,
    const char* dir);

/* ********************************* */
/*                GROUP              */
/* ********************************* */

/**
 * Creates a new TileDB group.
 *
 * @param tiledb_ctx The TileDB context.
 * @param dir The directory of the group to be created in the file system. This
 *     should be a directory whose parent is a TileDB workspace or another 
 *     TileDB group.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_group_create(
    const TileDB_CTX* tiledb_ctx,
    const char* dir);

/* ********************************* */
/*               ARRAY               */
/* ********************************* */

/** Specifies the array schema. */
typedef struct TileDB_ArraySchema {
  /** 
   * The array name. It is a directory, whose parent is a TileDB workspace, 
   * group or array.
   */
  const char* array_name_;
  /** The attribute names. */
  const char** attributes_;
  /** The number of attributes. */
  int attribute_num_;
  /** 
   * The tile capacity (only applicable to irregular tiles). If it is <=0,
   * TileDB will use its default.
   */
  int64_t capacity_;
  /**
   * The cell order. The supported orders are **row-major**, **column-major**
   * and **hilbert**. If it is set to NULL, then TileDB will use its default.
   */
  const char* cell_order_;
  /** 
   * The type of compression. The supported compression types are **NONE** and
   * **GZIP**. The number of compression types given should be equal to the
   * number of attributes, plus one (the last one) for the coordinates. 
   * If it is NULL, then TileDB will use its default.
   */
  const char** compression_;
  /** The consolidation step. If it is 0, then TileDB will use its default. */
  int consolidation_step_;
  /** 
   * If it is equal to 0, the array is in sparse format; otherwise the array is
   * in sparse format. If the array is dense, then the user must specify tile
   * extents (see below) for regular tiles.
   */
  int dense_;
  /** The dimension names. */
  const char** dimensions_;
  /** The number of dimensions. */
  int dim_num_;
  /**  
   * The domain. It should contain one [lower, upper] pair per dimension. The
   * type of the values stored in this buffer should match the coordinates type.
   * If the  coordinates type is <b>char:var</b>, this field is ignored.
   */
  void* domain_;
  /** 
   * The tile extents (only applicable to regular tiles). There should be one 
   * value for each dimension. The type of the values stored in this buffer
   * should match the coordinates type. If it is NULL, then it means that the
   * array has irregular tiles (and, hence, it is sparse). If the coordinates
   * type is <b>char:var</b>, this field is ignored.
   */
  void* tile_extents_;
  /**
   * The tile order (only applicable to regular tiles). The supported orders are
   * **row-major**, **column-major** and **hilbert**. If it is set to NULL, then
   * TileDB will use its default.
   */
  const char* tile_order_;
  /** 
   * The attribute and coordinate types. There should be one type per 
   * attribute plus one (the last one) for the coordinates. The supported types
   * for the attributes are **int32**, **int64**, **float32**, **float64** and
   * **char**. If the user wishes to have any arbitrary **fixed** number N of
   * values for some attribute, he/she should append <b>:N</b> to the attribute 
   * type, e.g., <b>int32:3</b>. If the user wishes to have a **variable**
   * number of values for some attribute, he/she should append <b>:var</b> to 
   * the attribute type, e.g., <b>char:var</b>. The supported types for the 
   * coordinates are **int32**, **int64**, **float32**, **float64**,
   * and <b>char:var</b>.
   */
  const char** types_;
} TileDB_ArraySchema;

/** A TileDB Array. */
typedef struct TileDB_Array TileDB_Array;

/**
 * Creates a new array.
 *
 * @param tiledb_ctx The TileDB context.
 * @param array_schema The array schema. 
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_array_create(
    const TileDB_CTX* tiledb_ctx,
    const TileDB_ArraySchema* array_schema);

/**
 * Initializes a TileDB array.
 *
 * @param tiledb_ctx The TileDB context.
 * @param tiledb_array The array object to be initialized.
 * @param dir The directory of the array to be initialized.
 * @param mode The mode of the array. It must be one of the following:
 *    - TILEDB_WRITE 
 *    - TILEDB_WRITE_UNSORTED 
 *    - TILEDB_READ 
 *    - TILEDB_READ_REVERSE 
 * @param range The range in which the array read/write will be constrained.
 * @param attributes A subset of the array attributes the read/write will be
 *     constrained. A NULL value indicates **all** attributes.
 * @param attribute_num The number of the input attributes.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_init(
    const TileDB_CTX* tiledb_ctx,
    TileDB_Array** tiledb_array,
    const char* dir,
    int mode,
    const void* range,
    const char** attributes,
    int attribute_num);

/** 
 * Finalizes an TileDB array. 
 *
 * @param tiledb_array The array to be finalized.
 * @return TILEDB_OK on success, and TILEDB_ERR on error.
 */
TILEDB_EXPORT int tiledb_array_finalize(
    TileDB_Array* tiledb_array);

// TODO
TILEDB_EXPORT int tiledb_array_write(
    const TileDB_Array* tiledb_array,
    const void** buffers,
    const size_t* buffer_sizes);

// TODO
TILEDB_EXPORT int tiledb_array_read(
    const TileDB_Array* tiledb_array,
    void** buffers,
    size_t* buffer_sizes);

// TODO
TILEDB_EXPORT int tiledb_array_consolidate(
    const TileDB_CTX* tiledb_ctx, 
    const char* array_name);

/* ********************************* */
/*         CLEAR, DELETE, MOVE       */
/* ********************************* */

/**
 * Clears a TileDB directory. The corresponding TileDB structure (workspace,
 * group, array) will still exist after the execution of the function, but it
 * will be empty (i.e., as if it was just created).
 *
 * @param tiledb_ctx The TileDB context.
 * @param dir The directory to be cleared.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_clear( 
    const TileDB_CTX* tiledb_ctx, 
    const char* dir);

/**
 * Deletes a TileDB directory (workspace, group, or array) entirely. 
 *
 * @param tiledb_ctx The TileDB context.
 * @param dir The directory to be deleted.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_delete( 
    const TileDB_CTX* tiledb_ctx, 
    const char* dir);

/**
 * Moves a TileDB directory (workspace, group, or array).
 *
 * @param tiledb_ctx The TileDB context.
 * @param old_dir The old directory.
 * @param new_dir The new directory.
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_move( 
    const TileDB_CTX* tiledb_ctx, 
    const char* old_dir,
    const char* new_dir);


#undef TILEDB_EXPORT
#ifdef __cplusplus
}
#endif

#endif
