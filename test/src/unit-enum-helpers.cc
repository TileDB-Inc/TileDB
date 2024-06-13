/**
 * @file unit-enum-helpers.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB Inc.
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
 * Tests for enum conversions.
 */

#include <iostream>

#include "test/support/tdb_catch.h"
#include "tiledb/sm/enums/datatype.h"

using namespace tiledb::sm;

TEST_CASE(
    "Test datatype_max_integral_value",
    "[enums][datatype][max-integral-value]") {
  std::vector<std::pair<Datatype, uint64_t>> test_data = {
      {Datatype::BOOL, 255},
      {Datatype::INT8, 127},
      {Datatype::INT16, 32767},
      {Datatype::INT32, 2147483647},
      {Datatype::INT64, 9223372036854775807},

      {Datatype::UINT8, 255},
      {Datatype::UINT16, 65535},
      {Datatype::UINT32, 4294967295},
      {Datatype::UINT64, 18446744073709551615u}};

  for (auto& [dtype, max_size] : test_data) {
    std::cerr << "DTYPE: " << datatype_str(dtype) << std::endl;
    REQUIRE(datatype_max_integral_value(dtype) == max_size);
  }

  REQUIRE_THROWS(datatype_max_integral_value(Datatype::BLOB));
  REQUIRE_THROWS(datatype_max_integral_value(Datatype::GEOM_WKB));
  REQUIRE_THROWS(datatype_max_integral_value(Datatype::GEOM_WKT));
  REQUIRE_THROWS(datatype_max_integral_value(Datatype::FLOAT64));
  REQUIRE_THROWS(datatype_max_integral_value(Datatype::STRING_ASCII));
}

TEST_CASE("Test datatype_is_byte", "[enums][datatype][datatype_is_byte]") {
  auto datatype =
      GENERATE(Datatype::BLOB, Datatype::GEOM_WKB, Datatype::GEOM_WKT);

  CHECK(datatype_is_byte(datatype));
  CHECK(!datatype_is_byte(Datatype::BOOL));
}
