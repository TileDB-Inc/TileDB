/**
 * @file tiledb/sm/enum/dimension_label_type.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This defines the TileDB LabelType enum for dimension label types.
 */

#ifndef TILEDB_LABEL_TYPE_H
#define TILEDB_LABEL_TYPE_H

#include "tiledb/common/common.h"

using namespace tiledb::common;

namespace tiledb::sm {

/**
 * A label type for a dimension label.
 *
 * Note: this is not yet exposed in the C API.
 */
enum class LabelType : uint8_t {
  LABEL_UNIFORM = 0,
};

/** Returns the string representation of the input label type. */
inline const std::string& label_type_str(LabelType label_type) {
  switch (label_type) {
    case LabelType::LABEL_UNIFORM:
      return constants::dimension_label_uniform_str;
    default:
      return constants::empty_str;
  }
};

/** Returns the label type given a string representation. */
inline Status label_type_enum(
    const std::string& label_type_str, LabelType* label_type) {
  if (label_type_str == constants::dimension_label_uniform_str)
    *label_type = LabelType::LABEL_UNIFORM;
  else {
    return Status_Error("Invalid LabelType " + label_type_str);
  }
  return Status::Ok();
};

}  // namespace tiledb::sm

#endif
