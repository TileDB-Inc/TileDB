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
   * The array name. It is a path to a directory, whose parent is a TileDB 
   * workspace, group or array.
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
   * The domain. It should contain one [lower, upper] pair per dimension. The type
   * of the values stored in this buffer should match the coordinates type. If the 
   * coordinates type is <b>char:var</b>, this field is ignored.
   */
  void* domain_;
  /** 
   * The tile extents (only applicable to regular tiles). There should be one 
   * value for each dimension. The type of the values stored in this buffer should 
   * match the coordinates type. If it is NULL, then it means that the array has
   * irregular tiles (and, hence, it is sparse). If the coordinates type is
   * <b>char:var</b>, this field is ignored.
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
} ArraySchemaC;

#endif
