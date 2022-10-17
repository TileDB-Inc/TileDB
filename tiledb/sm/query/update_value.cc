/**
 * @file update_value.cc
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
 * Implements the UpdateValue class.
 */

#include "tiledb/sm/query/update_value.h"
#include "tiledb/common/logger.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class UpdateValueStatusException : public StatusException {
 public:
  explicit UpdateValueStatusException(const std::string& message)
      : StatusException("UpdateValue", message) {
  }
};

UpdateValue::UpdateValue(
    std::string field_name,
    const void* update_value,
    const uint64_t update_value_size)
    : field_name_(field_name)
    , update_value_data_(update_value_size)
    , update_value_view_(
          (update_value != nullptr && update_value_size == 0 ?
               (void*)"" :
               update_value_data_.data()),
          update_value_data_.size()) {
  if (update_value_view_.size() != 0) {
    memcpy(update_value_data_.data(), update_value, update_value_size);
  }
}

UpdateValue::UpdateValue(const UpdateValue& rhs)
    : field_name_(rhs.field_name_)
    , update_value_data_(rhs.update_value_data_)
    , update_value_view_(
          (rhs.update_value_view_.content() != nullptr &&
                   rhs.update_value_view_.size() == 0 ?
               (void*)"" :
               update_value_data_.data()),
          update_value_data_.size()) {
}

UpdateValue::UpdateValue(UpdateValue&& rhs)
    : field_name_(rhs.field_name_)
    , update_value_data_(rhs.update_value_data_)
    , update_value_view_(
          (rhs.update_value_view_.content() != nullptr &&
                   rhs.update_value_view_.size() == 0 ?
               (void*)"" :
               update_value_data_.data()),
          update_value_data_.size()) {
}

UpdateValue::~UpdateValue() {
}

void UpdateValue::check(const ArraySchema& array_schema) const {
  const uint64_t update_value_size = update_value_data_.size();

  // Ensure field name exists.
  if (!array_schema.is_field(field_name_)) {
    throw UpdateValueStatusException("Field name doesn't exist");
  }

  // Ensure field is an attribute.
  if (!array_schema.is_attr(field_name_)) {
    throw UpdateValueStatusException("Can only update attributes");
  }

  const auto nullable = array_schema.is_nullable(field_name_);
  const auto var_size = array_schema.var_size(field_name_);
  const auto type = array_schema.type(field_name_);
  const auto cell_size = array_schema.cell_size(field_name_);
  const auto cell_val_num = array_schema.cell_val_num(field_name_);

  // Ensure that null value can only be used with nullable attributes.
  if (update_value_view_.content() == nullptr) {
    if ((!nullable) &&
        (type != Datatype::STRING_ASCII && type != Datatype::CHAR)) {
      throw UpdateValueStatusException(
          "Null value can only be used with nullable attributes");
    }
  }

  // Ensure that non string fixed size attributes store only one value per cell.
  if (cell_val_num != 1 && type != Datatype::STRING_ASCII &&
      type != Datatype::CHAR && (!var_size)) {
    throw UpdateValueStatusException(
        "Value node attribute must have one value per cell for non-string "
        "fixed size attributes: " +
        field_name_);
  }

  // Ensure that the condition value size matches the attribute's
  // value size.
  if (cell_size != constants::var_size && cell_size != update_value_size &&
      !(nullable && update_value_view_.content() == nullptr) &&
      type != Datatype::STRING_ASCII && type != Datatype::CHAR && (!var_size)) {
    throw UpdateValueStatusException(
        "Value node condition value size mismatch: " +
        std::to_string(cell_size) + " != " + std::to_string(update_value_size));
  }

  return;
}
}  // namespace sm
}  // namespace tiledb
