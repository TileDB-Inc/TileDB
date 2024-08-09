/**
 * @file tiledb/sm/serialization/test/unit_capnp_utils.cc
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
 * This file contains unit tests for the array schema
 */

#include <capnp/message.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/filter/bit_width_reduction_filter.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/filter.h"
#include "tiledb/sm/serialization/capnp_utils.h"

using namespace tiledb::common;
using namespace tiledb::sm;

/**
 * Check byte_vec_values are equal.
 */
void check_byte_vec_values(const ByteVecValue& val1, const ByteVecValue& val2) {
  CHECK(val1.size() == val2.size());
  auto nbyte = val2.size() / sizeof(uint8_t);
  const uint8_t* data1 = val1.data();
  const uint8_t* data2 = val2.data();
  for (size_t index{0}; index < nbyte; ++index) {
    CHECK(data1[index] == data2[index]);
  }
}

/**
 * Check filter pipelines are equal.
 *
 * Note: Only checks the number of filters and that the filters are
 * the correct type.
 */
void check_filter_pipelines(
    const FilterPipeline& pipeline1, const FilterPipeline& pipeline2) {
  CHECK(pipeline1.size() == pipeline2.size());
  CHECK(pipeline1.max_chunk_size() == pipeline2.max_chunk_size());
  for (decltype(pipeline1.size()) index{0}; index < pipeline1.size(); ++index) {
    auto filter1 = pipeline1.get_filter(index);
    auto filter2 = pipeline2.get_filter(index);
    CHECK(filter1->type() == filter2->type());
  }
}

TEST_CASE("Serialize and deserialize attribute", "[attribute][serialization]") {
  shared_ptr<Attribute> attr{nullptr};

  SECTION("Default attribute") {
    auto type = GENERATE(
        Datatype::INT32,
        Datatype::INT64,
        Datatype::FLOAT32,
        Datatype::FLOAT64,
        Datatype::INT8,
        Datatype::UINT8,
        Datatype::INT16,
        Datatype::UINT16,
        Datatype::UINT32,
        Datatype::UINT64,
        Datatype::STRING_ASCII,
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
        Datatype::TIME_AS);
    attr = make_shared<Attribute>(HERE(), "attr1", type);
  }
  SECTION("Nullable with non-default fill values") {
    double fill_value{2.0};
    attr = make_shared<Attribute>(HERE(), "attr1", Datatype::FLOAT64, true);
    attr->set_fill_value(&fill_value, sizeof(double), 1);
  }
  SECTION("Non-default filters pipeline") {
    attr = make_shared<Attribute>(HERE(), "attr1", Datatype::UINT64);
    FilterPipeline filters;
    filters.add_filter(CompressionFilter(Compressor::ZSTD, 2, attr->type()));
    filters.add_filter(BitWidthReductionFilter(attr->type()));
    attr->set_filter_pipeline(filters);
  }
  SECTION("Multiple cell values") {
    attr = make_shared<Attribute>(
        HERE(), "attr1", Datatype::INT32, 3, DataOrder::UNORDERED_DATA);
    int32_t fill_value[3]{1, -1, 0};
    attr->set_fill_value(&fill_value[0], 3 * sizeof(int32_t));
  }
  SECTION("Variable cell values") {
    attr = make_shared<Attribute>(
        HERE(),
        "attr1",
        Datatype::STRING_ASCII,
        constants::var_num,
        DataOrder::UNORDERED_DATA);
  }
  SECTION("Ordered data") {
    attr = make_shared<Attribute>(
        HERE(), "attr1", Datatype::FLOAT64, 1, DataOrder::DECREASING_DATA);
  }

  // Serialize
  ::capnp::MallocMessageBuilder message;
  tiledb::sm::serialization::capnp::Attribute::Builder builder =
      message.initRoot<tiledb::sm::serialization::capnp::Attribute>();
  tiledb::sm::serialization::attribute_to_capnp(attr.get(), &builder);
  auto attr_clone = tiledb::sm::serialization::attribute_from_capnp(builder);

  // Check attribute values.
  CHECK(attr->cell_val_num() == attr_clone->cell_val_num());
  CHECK(attr->nullable() == attr_clone->nullable());
  CHECK(attr->name() == attr_clone->name());
  CHECK(attr->type() == attr_clone->type());
  check_byte_vec_values(attr->fill_value(), attr_clone->fill_value());
  check_filter_pipelines(attr->filters(), attr_clone->filters());
  CHECK(attr->fill_value_validity() == attr_clone->fill_value_validity());
  CHECK(attr->order() == attr_clone->order());
}
