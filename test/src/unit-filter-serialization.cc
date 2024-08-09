/**
 * @file unit-filter-serialization.cc
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
 * Tests the `Filter` Cap'n proto serialization paths.
 */

#ifdef TILEDB_SERIALIZATION
#include <capnp/message.h>
#include "tiledb/sm/serialization/array_schema.h"
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/filter.h"
#include "tiledb/sm/filter/filter_create.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/filter/float_scaling_filter.h"

#include <catch2/catch_test_macros.hpp>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE(
    "Filter serialization: Test noop filter", "[filter][noop][serialization]") {
  Filter* f = FilterCreate::make(FilterType::FILTER_NONE);
  ::capnp::MallocMessageBuilder message;
  tiledb::sm::serialization::capnp::Filter::Builder filter_builder =
      message.initRoot<tiledb::sm::serialization::capnp::Filter>();
  REQUIRE(tiledb::sm::serialization::filter_to_capnp(f, &filter_builder).ok());

  auto&& [st_f, filter_noop]{
      tiledb::sm::serialization::filter_from_capnp(filter_builder)};
  REQUIRE(st_f.ok());
  REQUIRE(filter_noop.value()->type() == FilterType::FILTER_NONE);
}

TEST_CASE(
    "Filter serialization: Test default float scaling filter",
    "[filter][float-scaling][serialization]") {
  Filter* f = FilterCreate::make(FilterType::FILTER_SCALE_FLOAT);
  ::capnp::MallocMessageBuilder message;
  tiledb::sm::serialization::capnp::Filter::Builder filter_builder =
      message.initRoot<tiledb::sm::serialization::capnp::Filter>();
  REQUIRE(tiledb::sm::serialization::filter_to_capnp(f, &filter_builder).ok());

  auto&& [st_f, filter_clone]{
      tiledb::sm::serialization::filter_from_capnp(filter_builder)};
  REQUIRE(st_f.ok());

  double scale_clone = 0;
  double offset_clone = 0;
  uint64_t byte_width_clone = 0;
  REQUIRE(
      filter_clone.value()
          .get()
          ->get_option(FilterOption::SCALE_FLOAT_BYTEWIDTH, &byte_width_clone)
          .ok());
  REQUIRE(filter_clone.value()
              .get()
              ->get_option(FilterOption::SCALE_FLOAT_FACTOR, &scale_clone)
              .ok());
  REQUIRE(filter_clone.value()
              .get()
              ->get_option(FilterOption::SCALE_FLOAT_OFFSET, &offset_clone)
              .ok());

  CHECK(scale_clone == 1.0f);
  CHECK(offset_clone == 0.0f);
  CHECK(byte_width_clone == 8);
}

TEST_CASE(
    "Filter Serialization: Test float scaling filter with options",
    "[filter][float-scaling][serialization]") {
  Filter* f = FilterCreate::make(FilterType::FILTER_SCALE_FLOAT);
  double scale = 2.13;
  double offset = 1.5251;
  uint64_t byte_width = 4;
  REQUIRE(f->set_option(FilterOption::SCALE_FLOAT_BYTEWIDTH, &byte_width).ok());
  REQUIRE(f->set_option(FilterOption::SCALE_FLOAT_FACTOR, &scale).ok());
  REQUIRE(f->set_option(FilterOption::SCALE_FLOAT_OFFSET, &offset).ok());

  ::capnp::MallocMessageBuilder message;
  tiledb::sm::serialization::capnp::Filter::Builder filter_builder =
      message.initRoot<tiledb::sm::serialization::capnp::Filter>();
  REQUIRE(tiledb::sm::serialization::filter_to_capnp(f, &filter_builder).ok());

  auto&& [st_f, filter_clone]{
      tiledb::sm::serialization::filter_from_capnp(filter_builder)};
  REQUIRE(st_f.ok());

  double scale_clone = 0;
  double offset_clone = 0;
  uint64_t byte_width_clone = 0;
  REQUIRE(
      filter_clone.value()
          .get()
          ->get_option(FilterOption::SCALE_FLOAT_BYTEWIDTH, &byte_width_clone)
          .ok());
  REQUIRE(filter_clone.value()
              .get()
              ->get_option(FilterOption::SCALE_FLOAT_FACTOR, &scale_clone)
              .ok());
  REQUIRE(filter_clone.value()
              .get()
              ->get_option(FilterOption::SCALE_FLOAT_OFFSET, &offset_clone)
              .ok());

  CHECK(scale == scale_clone);
  CHECK(offset == offset_clone);
  CHECK(byte_width == byte_width_clone);
}
