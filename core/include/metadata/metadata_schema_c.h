/**
 * @file   metadata_schema_c.h
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
 * A C-style struct that specifies the metadata schema.  
 */  

#ifndef __METADATA_SCHEMA_C_H__
#define __METADATA_SCHEMA_C_H__

#include <stdint.h>

/** Specifies the metadata schema. */
typedef struct MetadataSchemaC {
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
   * key. It can be one of the following: 
   *    - TILEDB_NO_COMPRESSION
   *    - TILEDB_GZIP. 
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
} MetadataSchemaC;

#endif
