/**
 * @file tiledb/sm/array_schema/unit_dimension_label_base_schema.cc
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
 * This file tests the DimensionLabel::BaseSchema class
 */

#include <array>
#include <catch.hpp>
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/enums/datatype.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

template <typename T>
void require_range_is_equal(const Range& expected, const Range& result) {
  if (expected.empty() || result.empty()) {
    REQUIRE((expected.empty() && result.empty()));
    return;
  }
  auto expected_data = static_cast<const T*>(expected.data());
  auto result_data = static_cast<const T*>(result.data());
  CHECK(expected_data[0] == result_data[0]);
  REQUIRE(expected_data[1] == result_data[1]);
}

template <typename T>
Range create_range(const T start, const T stop) {
  std::array<T, 2> array{start, stop};
  return {array.data(), 2 * sizeof(T)};
}

TEMPLATE_TEST_CASE_SIG(
    "Round trip dimension label base schema",
    "[dimension_label][serialize][deserialize]",
    ((typename TLABEL, Datatype DLABEL), TLABEL, DLABEL),
    (int8_t, Datatype::INT8),
    (uint8_t, Datatype::UINT8),
    (int16_t, Datatype::INT16),
    (uint16_t, Datatype::UINT16),
    (int32_t, Datatype::INT32),
    (uint32_t, Datatype::UINT32),
    (int64_t, Datatype::INT64),
    (uint64_t, Datatype::UINT64),
    (int64_t, Datatype::DATETIME_YEAR),
    (int64_t, Datatype::DATETIME_MONTH),
    (int64_t, Datatype::DATETIME_WEEK),
    (int64_t, Datatype::DATETIME_DAY),
    (int64_t, Datatype::DATETIME_HR),
    (int64_t, Datatype::DATETIME_MIN),
    (int64_t, Datatype::DATETIME_SEC),
    (int64_t, Datatype::DATETIME_MS),
    (int64_t, Datatype::DATETIME_US),
    (int64_t, Datatype::DATETIME_NS),
    (int64_t, Datatype::DATETIME_PS),
    (int64_t, Datatype::DATETIME_FS),
    (int64_t, Datatype::DATETIME_AS),
    (int64_t, Datatype::TIME_HR),
    (int64_t, Datatype::TIME_MIN),
    (int64_t, Datatype::TIME_SEC),
    (int64_t, Datatype::TIME_MS),
    (int64_t, Datatype::TIME_US),
    (int64_t, Datatype::TIME_NS),
    (int64_t, Datatype::TIME_PS),
    (int64_t, Datatype::TIME_FS),
    (int64_t, Datatype::TIME_AS),
    (float, Datatype::FLOAT32),
    (double, Datatype::FLOAT64)) {
  const auto index_domain = create_range<uint64_t>(0, 5);
  const auto label_domain = create_range<TLABEL>(10, 60);
  const uint32_t version{12};
  DimensionLabel::BaseSchema schema{
      "label", DLABEL, 1, label_domain, Datatype::UINT64, 1, index_domain};
  Buffer write_buffer;
  auto status = schema.serialize(&write_buffer, version);
  write_buffer.owns_data();
  INFO(status.to_string());
  REQUIRE(status.ok());
  ConstBuffer read_buffer(&write_buffer);
  auto&& [read_status, schema2] = DimensionLabel::BaseSchema::deserialize(
      &read_buffer,
      version,
      schema.index_datatype,
      schema.index_cell_val_num,
      schema.index_domain);
  INFO(read_status.to_string());
  REQUIRE(read_status.ok());
  REQUIRE(schema2.has_value());
  CHECK(schema.name == schema2->name);
  CHECK(schema.label_datatype == schema2->label_datatype);
  CHECK(schema.label_cell_val_num == schema2->label_cell_val_num);
  require_range_is_equal<TLABEL>(schema.label_domain, schema2->label_domain);
  REQUIRE(schema.index_datatype == schema2->index_datatype);
  REQUIRE(schema.index_cell_val_num == schema2->index_cell_val_num);
  require_range_is_equal<uint64_t>(schema.index_domain, schema2->index_domain);
}
