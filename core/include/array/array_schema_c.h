/**
 * @file   array_schema_c.h
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
 * A C-style struct that specifies the array schema.  
 */  

#ifndef __ARRAY_SCHEMA_C_H__
#define __ARRAY_SCHEMA_C_H__

#include <stdint.h>

/** Specifies the array schema. */
typedef struct ArraySchemaC {
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
   * coordinates. It can be one of the following: 
   *    - TILEDB_NO_COMPRESSION
   *    - TILEDB_GZIP. 
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
   * The array domain. It should contain one [lower, upper] pair per dimension. 
   * The type of the values stored in this buffer should match the coordinates
   * type.
   */
  void* domain_;
  /** 
   * The tile extents. There should be one value for each dimension. The type of
   * the values stored in this buffer should match the coordinates type. If it
   * is NULL (applicable only to sparse arrays), then it means that the
   * array has irregular tiles.
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
} ArraySchemaC;

#endif
