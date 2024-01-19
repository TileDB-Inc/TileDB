/**
 * @file update_value.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2022 TileDB, Inc.
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
 * Defines the UpdateValue class.
 */

#ifndef TILEDB_UPDATE_VALUE_H
#define TILEDB_UPDATE_VALUE_H

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/common/types/untyped_datum.h"
#include "tiledb/sm/array_schema/array_schema.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class UpdateValue {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  UpdateValue() = delete;

  /** Constructor. */
  UpdateValue(
      std::string field_name,
      const void* update_value,
      const uint64_t update_value_size);

  /** Destructor. */
  ~UpdateValue();

  /** Copy constructor. */
  UpdateValue(const UpdateValue& rhs);

  /** Move constructor. */
  UpdateValue(UpdateValue&& rhs);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  DISABLE_COPY_ASSIGN(UpdateValue);
  DISABLE_MOVE_ASSIGN(UpdateValue);

  /** Returns the field name. */
  inline const std::string& field_name() const {
    return field_name_;
  }

  /** Returns the view. */
  inline const UntypedDatumView& view() const {
    return update_value_view_;
  }

  /**
   * Verifies that the object respects the array schema.
   *
   * @param array_schema The current array schema.
   */
  void check(const ArraySchema<ContextResources::resource_manager_type>& array_schema) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Name of the attribute to update.  */
  const std::string field_name_;

  /** The value data. */
  ByteVecValue update_value_data_;

  /** A view of the value data. */
  const UntypedDatumView update_value_view_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_UPDATE_VALUE_H
