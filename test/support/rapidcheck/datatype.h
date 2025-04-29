/**
 * @file test/support/rapidcheck/datatype.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file defines rapidcheck generators for datatypes.
 */

#ifndef TILEDB_RAPIDCHECK_DATATYPE_H
#define TILEDB_RAPIDCHECK_DATATYPE_H

namespace rc {
using namespace tiledb::sm;

template <>
struct Arbitrary<Datatype> {
  static Gen<Datatype> arbitrary() {
    return rc::gen::element(
        Datatype::INT32,
        Datatype::INT64,
        Datatype::FLOAT32,
        Datatype::FLOAT64,
        Datatype::CHAR,
        Datatype::INT8,
        Datatype::UINT8,
        Datatype::INT16,
        Datatype::UINT16,
        Datatype::UINT32,
        Datatype::UINT64,
        Datatype::STRING_ASCII,
        Datatype::STRING_UTF8,
        Datatype::STRING_UTF16,
        Datatype::STRING_UTF32,
        Datatype::STRING_UCS2,
        Datatype::STRING_UCS4,
        Datatype::ANY,
        Datatype::DATETIME_YEAR,
        Datatype::DATETIME_MONTH,
        Datatype::DATETIME_WEEK,
        Datatype::DATETIME_DAY,
        Datatype::DATETIME_HR,
        Datatype::DATETIME_MIN,
        Datatype::DATETIME_SEC,
        Datatype::DATETIME_MS,
        Datatype::DATETIME_US,
        Datatype::DATETIME_NS,
        Datatype::DATETIME_PS,
        Datatype::DATETIME_FS,
        Datatype::DATETIME_AS,
        Datatype::TIME_HR,
        Datatype::TIME_MIN,
        Datatype::TIME_SEC,
        Datatype::TIME_MS,
        Datatype::TIME_US,
        Datatype::TIME_NS,
        Datatype::TIME_PS,
        Datatype::TIME_FS,
        Datatype::TIME_AS,
        Datatype::BLOB,
        Datatype::BOOL,
        Datatype::GEOM_WKB,
        Datatype::GEOM_WKT);
  };
};

[[maybe_unused]] static Gen<std::vector<uint8_t>> make_input_bytes(
    Datatype input_type) {
  auto gen_elt = rc::gen::container<std::vector<uint8_t>>(
      datatype_size(input_type), gen::arbitrary<uint8_t>());
  auto gen_container =
      gen::nonEmpty(gen::container<std::vector<std::vector<uint8_t>>>(gen_elt));
  return gen::map(
      gen_container, [input_type](std::vector<std::vector<uint8_t>> elts) {
        std::vector<uint8_t> flat;
        flat.reserve(elts.size() * datatype_size(input_type));
        for (const std::vector<uint8_t>& elt : elts) {
          flat.insert(flat.end(), elt.begin(), elt.end());
        }
        return flat;
      });
}

}  // namespace rc

#endif
