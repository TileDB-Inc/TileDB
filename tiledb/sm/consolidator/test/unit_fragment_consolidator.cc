/**
 * @file tiledb/sm/metadata/test/unit_fragment_consolidator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * This file defines a test `main()`
 */

#include <catch2/catch_test_macros.hpp>
#include "../fragment_consolidator.h"
#include "test/support/src/mem_helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/enums/array_type.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

shared_ptr<ArraySchema> make_schema(
    const bool sparse,
    const std::vector<Datatype> dim_types,
    const std::vector<Datatype> attr_types,
    const std::vector<bool> attr_nullable) {
  // Initialize the array schema.
  shared_ptr<ArraySchema> array_schema = make_shared<ArraySchema>(
      HERE(),
      sparse ? ArrayType::SPARSE : ArrayType::DENSE,
      tiledb::test::create_test_memory_tracker());

  // Create the domain/dimensions.
  auto memory_tracker = tiledb::test::create_test_memory_tracker();
  auto domain{make_shared<Domain>(HERE(), memory_tracker)};
  for (uint64_t d = 0; d < dim_types.size(); d++) {
    auto dim{make_shared<Dimension>(
        HERE(),
        "d" + std::to_string(d + 1),
        dim_types[d],
        tiledb::test::get_test_memory_tracker())};

    switch (dim_types[d]) {
      case Datatype::INT8: {
        int8_t bounds[2] = {1, 10};
        REQUIRE(dim->set_domain(&bounds).ok());
        break;
      }
      case Datatype::INT16: {
        int16_t bounds[2] = {1, 10};
        REQUIRE(dim->set_domain(&bounds).ok());
        break;
      }
      case Datatype::INT32: {
        int32_t bounds[2] = {1, 10};
        REQUIRE(dim->set_domain(&bounds).ok());
        break;
      }
      case Datatype::INT64: {
        int64_t bounds[2] = {1, 10};
        REQUIRE(dim->set_domain(&bounds).ok());
        break;
      }
      case Datatype::UINT8: {
        uint8_t bounds[2] = {1, 10};
        REQUIRE(dim->set_domain(&bounds).ok());
        break;
      }
      case Datatype::UINT16: {
        uint16_t bounds[2] = {1, 10};
        REQUIRE(dim->set_domain(&bounds).ok());
        break;
      }
      case Datatype::UINT32: {
        uint32_t bounds[2] = {1, 10};
        REQUIRE(dim->set_domain(&bounds).ok());
        break;
      }
      case Datatype::UINT64: {
        uint64_t bounds[2] = {1, 10};
        REQUIRE(dim->set_domain(&bounds).ok());
        break;
      }
      case Datatype::DATETIME_YEAR:
      case Datatype::DATETIME_MONTH:
      case Datatype::DATETIME_WEEK:
      case Datatype::DATETIME_DAY:
      case Datatype::DATETIME_HR:
      case Datatype::DATETIME_MIN:
      case Datatype::DATETIME_SEC:
      case Datatype::DATETIME_MS:
      case Datatype::DATETIME_US:
      case Datatype::DATETIME_NS:
      case Datatype::DATETIME_PS:
      case Datatype::DATETIME_FS:
      case Datatype::DATETIME_AS:
      case Datatype::TIME_HR:
      case Datatype::TIME_MIN:
      case Datatype::TIME_SEC:
      case Datatype::TIME_MS:
      case Datatype::TIME_US:
      case Datatype::TIME_NS:
      case Datatype::TIME_PS:
      case Datatype::TIME_FS:
      case Datatype::TIME_AS: {
        uint64_t bounds[2] = {1, 10};
        REQUIRE(dim->set_domain(&bounds).ok());
        break;
      }
      case Datatype::STRING_ASCII: {
        REQUIRE(dim->set_cell_val_num(constants::var_num).ok());
      }
      default: {
      }
    }

    REQUIRE(domain->add_dimension(dim).ok());
  }
  REQUIRE(array_schema->set_domain(domain).ok());

  // Create the attributes.
  for (uint64_t a = 0; a < attr_types.size(); a++) {
    Attribute attr("a" + std::to_string(a + 1), attr_types[a]);
    if (attr_types[a] == Datatype::STRING_ASCII) {
      attr.set_cell_val_num(constants::var_num);
    }
    attr.set_nullable(attr_nullable[a]);
    REQUIRE(
        array_schema->add_attribute(make_shared<Attribute>(HERE(), attr)).ok());
  }

  return array_schema;
}

TEST_CASE(
    "Fragment consolidator: test buffer creation",
    "[fragment_consolidator][create_buffers]") {
  stats::Stats statistics("default");
  shared_ptr<ArraySchema> schema = nullptr;
  std::vector<uint64_t> expected_sizes;
  std::unordered_map<std::string, uint64_t> avg_cell_sizes;
  bool with_timestamps = false;
  bool with_delete_meta = false;

  SECTION("int32 dim, int32 attr") {
    schema = make_schema(true, {Datatype::INT32}, {Datatype::INT32}, {false});
    expected_sizes = {1000, 1000};
  }

  SECTION("int32 dim, int32 attr, with timestamps") {
    schema = make_schema(true, {Datatype::INT64}, {Datatype::INT64}, {false});
    expected_sizes = {1000, 1000, 1000};
    with_timestamps = true;
  }

  SECTION("int32 dim, int32 attr, with timestamps and delete meta") {
    schema = make_schema(true, {Datatype::INT64}, {Datatype::INT64}, {false});
    expected_sizes = {1000, 1000, 1000, 1000, 1000};
    with_timestamps = true;
    with_delete_meta = true;
  }

  SECTION("int32 dim, var attr") {
    schema =
        make_schema(true, {Datatype::INT32}, {Datatype::STRING_ASCII}, {false});
    expected_sizes = {1496, 748, 748};
    avg_cell_sizes["a1"] = 4;
  }

  SECTION("int32 dim, nullable var attr") {
    schema =
        make_schema(true, {Datatype::INT32}, {Datatype::STRING_ASCII}, {true});
    expected_sizes = {1880, 940, 235, 940};
    avg_cell_sizes["a1"] = 4;
  }

  SECTION("int32/int64 dim, var attr") {
    schema = make_schema(
        true,
        {Datatype::INT32, Datatype::INT64},
        {Datatype::STRING_ASCII},
        {false});
    expected_sizes = {1328, 664, 664, 1328};
    avg_cell_sizes["a1"] = 4;
  }

  SECTION("int32/var dim, uint8 nullable attr") {
    schema = make_schema(
        true,
        {Datatype::INT32, Datatype::STRING_ASCII},
        {Datatype::UINT8},
        {true});
    expected_sizes = {166, 166, 664, 1328, 2656};
    avg_cell_sizes["d2"] = 16;
  }

  // Create buffers.
  FragmentConsolidationConfig cfg;
  cfg.with_timestamps_ = with_timestamps;
  cfg.with_delete_meta_ = with_delete_meta;
  cfg.buffer_size_ = 1000;

  FragmentConsolidationWorkspace cw(tiledb::test::get_test_memory_tracker());
  cw.resize_buffers(&statistics, cfg, *schema, avg_cell_sizes, 1);
  auto& buffers = cw.buffers();
  auto& buffer_sizes = cw.sizes();

  // Validate.
  CHECK(buffers.size() == expected_sizes.size());
  CHECK(buffer_sizes.size() == expected_sizes.size());
  for (uint64_t i = 0; i < expected_sizes.size(); i++) {
    CHECK(buffers[i].size() == expected_sizes[i]);
    CHECK(buffer_sizes[i] == expected_sizes[i]);
  }
}
