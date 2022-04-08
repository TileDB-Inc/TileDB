/**
 * @file dynamic_typed_datum.h
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
 */

#ifndef TILEDB_COMMON_DYNAMIC_TYPED_DATUM_H
#define TILEDB_COMMON_DYNAMIC_TYPED_DATUM_H

#include <ostream>
#include "tiledb/common/common.h"
#include "tiledb/sm/enums/datatype.h"
#include "untyped_datum.h"
// Forward declarations
namespace tiledb::common {
class DynamicTypedDatumView;
}

/**
 * String conversion operation for DynamicTypedDatum implemented as stream
 * output.
 *
 * @param os Output stream
 * @param datum Datum to represent as a string
 * @return the output stream argument
 */
std::ostream& operator<<(
    std::ostream& os, const tdb::DynamicTypedDatumView& datum);

namespace tiledb::common {

/**
 * A non-owning view of a datum to be interpreted as a given type.
 */
class DynamicTypedDatumView {
  UntypedDatumView datum_;
  tiledb::sm::Datatype type_;
  friend std::ostream& ::operator<<(
      std::ostream&, const tdb::DynamicTypedDatumView&);

 public:
  DynamicTypedDatumView(UntypedDatumView d, tiledb::sm::Datatype t)
      : datum_(d)
      , type_(t) {
  }
  [[nodiscard]] const UntypedDatumView& datum() const {
    return datum_;
  }
  [[nodiscard]] inline tiledb::sm::Datatype type() const {
    return type_;
  }
  template <class T>
  [[nodiscard]] inline const T& value_as() const {
    return *static_cast<const T*>(datum_.content());
  }
};
}  // namespace tiledb::common

#endif  // TILEDB_COMMON_DYNAMIC_TYPED_DATUM_H
