/**
 * @file   array_schema_c.h
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
 * A C-style struct that specifies the array schema.  
 */  

#ifndef __ARRAY_SCHEMA_C_H__
#define __ARRAY_SCHEMA_C_H__

#include <stdint.h>

/** Specifies the array schema. */
typedef struct ArraySchemaC {
  /** 
   * The array name. It is a directory, whose parent is a TileDB workspace, 
   * group or array.
   */
  char* array_name_;
  /** The attribute names. */
  char** attributes_;
  /** The number of attributes. */
  int attribute_num_;
  /** 
   * The tile capacity (only applicable to irregular tiles). If it is <=0,
   * TileDB will use its default.
   */
  int64_t capacity_;
  /** It can be TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR or TILEDB_HILBERT. */
  int cell_order_;
  // TODO
  int* cell_val_num_;
  /** It can be either TILEDB_NO_COMPRESSION or TILEDB_GZIP. */
  int* compression_;
  /** 
   * If it is equal to 0, the array is in sparse format; otherwise the array is
   * in sparse format. If the array is dense, then the user must specify tile
   * extents (see below) for regular tiles.
   */
  int dense_;
  /** The dimension names. */
  char** dimensions_;
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
  /** It can be TILEDB_ROW_MAJOR or TILEDB_COL_MAJOR. */
  int tile_order_;
  // TODO
  int* types_;
} ArraySchemaC;

#endif
