/**
 * @file untyped_datum.h
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

#ifndef TILEDB_COMMON_UNTYPED_DATUM_H
#define TILEDB_COMMON_UNTYPED_DATUM_H

#include <ostream>

namespace tiledb::common {

/**
 * A non-owning view of a datum of any type.
 */
class UntypedDatumView {
  const void* datum_content_;
  size_t datum_size_;

 public:
  UntypedDatumView(const void* content, size_t size)
      : datum_content_(content)
      , datum_size_(size) {
  }
  UntypedDatumView(std::string_view ss)
      : datum_content_(ss.data())
      , datum_size_(ss.size()) {
  }

  [[nodiscard]] inline const void* content() const {
    return datum_content_;
  }
  [[nodiscard]] inline size_t size() const {
    return datum_size_;
  }
  template <class T>
  [[nodiscard]] inline const T& value_as() const {
    return *static_cast<const T*>(datum_content_);
  }
};
}  // namespace tiledb::common
#endif  // TILEDB_COMMON_UNTYPED_DATUM_H
