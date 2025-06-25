/**
 * @file   field_info.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file defines class FieldInfo.
 */

#ifndef TILEDB_FIELD_INFO_H
#define TILEDB_FIELD_INFO_H

#include "tiledb/common/common.h"
#include "tiledb/sm/enums/datatype.h"

namespace tiledb::sm {

class FieldInfo {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Default constructor.
   */
  FieldInfo()
      : var_sized_(false)
      , is_nullable_(false)
      , is_dense_dim_(false)
      , is_slab_dim_(false)
      , cell_val_num_(1)
      , type_(Datatype::UINT8) {};

  /**
   * Constructor.
   *
   * @param name Name of the field.
   * @param var_sized Is the field var sized?
   * @param is_nullable Is the field nullable?
   * @param cell_val_num Cell val num.
   * @param type Data type of the field
   */
  FieldInfo(
      const std::string name,
      const bool var_sized,
      const bool is_nullable,
      const unsigned cell_val_num,
      const Datatype type)
      : name_(name)
      , var_sized_(var_sized)
      , is_nullable_(is_nullable)
      , is_dense_dim_(false)
      , is_slab_dim_(false)
      , cell_val_num_(cell_val_num)
      , type_(type) {};

  /**
   * Constructor.
   *
   * @param name Name of the field.
   * @param var_sized Is the field var sized?
   * @param is_nullable Is the field nullable?
   * @param is_dense_dim Is the field nullable?
   * @param is_slab_dim Is the dense dimension the slab dimension?
   * @param cell_val_num Cell val num.
   * @param type Data type of the field
   */
  FieldInfo(
      const std::string name,
      const bool var_sized,
      const bool is_nullable,
      const bool is_dense_dim,
      const bool is_slab_dim,
      const unsigned cell_val_num,
      const Datatype type)
      : name_(name)
      , var_sized_(var_sized)
      , is_nullable_(is_nullable)
      , is_dense_dim_(is_dense_dim)
      , is_slab_dim_(is_slab_dim)
      , cell_val_num_(cell_val_num)
      , type_(type) {};

  /* ********************************* */
  /*         PUBLIC ATTRIBUTES         */
  /* ********************************* */

  /** Field name. */
  const std::string name_;

  /** Is the field var sized? */
  const bool var_sized_;

  /** Is the field nullable? */
  const bool is_nullable_;

  /** Is the field a dense dimension? */
  const bool is_dense_dim_;

  /** Is the dense dimension the cell slab dimension? */
  const bool is_slab_dim_;

  /** Cell val num. */
  const unsigned cell_val_num_;

  /** The data type of the field. */
  const Datatype type_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_FIELD_INFO_H
