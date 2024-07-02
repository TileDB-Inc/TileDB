/**
 * @file unit_serializers.cc
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
 * Tests the `ValidityVector` class.
 */

#include "tiledb/storage_format/serialization/serializers.h"

#include <tdb_catch.h>
#include <catch2/catch_template_test_macros.hpp>

#include <iostream>

using namespace std;
using namespace tiledb::sm;

typedef tuple<
    unsigned char,  // Used for TILEDB_CHAR.
    char,
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    float,
    double>
    TypesUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "Serializer/Deserializer Test", "[Serializer]", TypesUnderTest) {
  SizeComputationSerializer size_computation_serializer;
  TestType data = 1;
  size_computation_serializer.write(data);
  REQUIRE(size_computation_serializer.size() == sizeof(TestType));

  std::vector<uint8_t> buff(size_computation_serializer.size());
  Serializer serializer(buff.data(), buff.size());
  serializer.write(data);

  Deserializer deserializer(buff.data(), buff.size());
  auto read = deserializer.read<TestType>();
  CHECK(data == read);
}
