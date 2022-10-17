/**
 * @file tiledb/sm/array_schema/test/unit_array_schema.cc
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
 * This file contains unit tests for the array schema
 */

#include <tdb_catch.h>

#include "tiledb/sm/array_schema/dimension_label_reference.h"
#include "tiledb/sm/array_schema/test/unit_array_schema_helper.h"

using namespace tiledb::common;
using namespace tiledb::sm;

TEST_CASE("Test repeating names", "[array_schema]") {
  SECTION("Label with dimension name okay") {
    std::vector<shared_ptr<Dimension>> dims{
        test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16)};
    std::vector<shared_ptr<Attribute>> attrs{
        test::make_attribute<float>("a", Datatype::UINT64, false, 1, 0)};
    uint64_t index_domain[2]{0, 15};
    uint64_t index_tile_extent{16};
    double label_domain[2]{-1.0, 1.0};
    double label_tile_extent{1};
    auto dim_label_schema = make_shared<DimensionLabelSchema>(
        HERE(),
        LabelOrder::INCREASING_LABELS,
        Datatype::UINT64,
        index_domain,
        &index_tile_extent,
        Datatype::FLOAT64,
        label_domain,
        &label_tile_extent);
    auto schema = test::make_array_schema(ArrayType::DENSE, dims, attrs);
    auto status =
        schema->add_dimension_label(0, "x", dim_label_schema, false, true);
    REQUIRE(status.ok());
    status = schema->check();
    REQUIRE(status.ok());
  }

  SECTION("Catch shared dimension/attribute name") {
    std::vector<shared_ptr<Dimension>> dims{
        test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16)};
    std::vector<shared_ptr<Attribute>> attrs{
        test::make_attribute<float>("x", Datatype::UINT64, false, 1, 0)};
    auto schema = test::make_array_schema(ArrayType::DENSE, dims, attrs);
    auto status = schema->check();
    INFO(status.to_string());
    REQUIRE(!status.ok());
  }

  SECTION("Catch repeating dimension name") {
    std::vector<shared_ptr<Dimension>> dims{
        test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16),
        test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16)};
    std::vector<shared_ptr<Attribute>> attrs{
        test::make_attribute<float>("a", Datatype::UINT64, false, 1, 0)};
    auto schema = test::make_array_schema(ArrayType::DENSE, dims, attrs);
    auto status = schema->check();
    INFO(status.to_string());
    REQUIRE(!status.ok());
  }

  SECTION("Catch repeating attribute name") {
    std::vector<shared_ptr<Dimension>> dims{
        test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16)};
    std::vector<shared_ptr<Attribute>> attrs{
        test::make_attribute<float>("a", Datatype::UINT64, false, 1, 0),
        test::make_attribute<float>("a", Datatype::UINT64, false, 1, 0)};
    auto schema = test::make_array_schema(ArrayType::DENSE, dims, attrs);
    auto status = schema->check();
    INFO(status.to_string());
    REQUIRE(!status.ok());
  }

  SECTION("Catch repeating label name shared with dim when adding label") {
    std::vector<shared_ptr<Dimension>> dims{
        test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16)};
    std::vector<shared_ptr<Attribute>> attrs{
        test::make_attribute<float>("a", Datatype::UINT64, false, 1, 0)};
    auto schema = test::make_array_schema(ArrayType::DENSE, dims, attrs);
    uint64_t index_domain[2]{0, 15};
    uint64_t index_tile_extent{16};
    uint64_t label_domain[2]{16, 31};
    uint64_t label_tile_extent{16};
    auto dim_label_schema = make_shared<DimensionLabelSchema>(
        HERE(),
        LabelOrder::INCREASING_LABELS,
        Datatype::UINT64,
        index_domain,
        &index_tile_extent,
        Datatype::UINT64,
        label_domain,
        &label_tile_extent);
    auto status =
        schema->add_dimension_label(0, "x", dim_label_schema, true, true);
    REQUIRE(status.ok());
    status = schema->add_dimension_label(0, "x", dim_label_schema, true, false);
    INFO(status.to_string());
    REQUIRE(!status.ok());
  }

  SECTION("Catch repeating label name shared with dim with check") {
    std::vector<shared_ptr<Dimension>> dims{
        test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16)};
    std::vector<shared_ptr<Attribute>> attrs{
        test::make_attribute<float>("a", Datatype::UINT64, false, 1, 0)};
    auto schema = test::make_array_schema(ArrayType::DENSE, dims, attrs);
    uint64_t index_domain[2]{0, 15};
    uint64_t index_tile_extent{16};
    uint64_t label_domain[2]{16, 31};
    uint64_t label_tile_extent{16};
    auto dim_label_schema = make_shared<DimensionLabelSchema>(
        HERE(),
        LabelOrder::INCREASING_LABELS,
        Datatype::UINT64,
        index_domain,
        &index_tile_extent,
        Datatype::UINT64,
        label_domain,
        &label_tile_extent);
    auto status =
        schema->add_dimension_label(0, "x", dim_label_schema, false, true);
    REQUIRE(status.ok());
    status = schema->add_dimension_label(0, "x", dim_label_schema, false, true);
    REQUIRE(status.ok());
    status = schema->check();
    INFO(status.to_string());
    REQUIRE(!status.ok());
  }

  SECTION("Catch repeating label name not shared with dim when adding label") {
    std::vector<shared_ptr<Dimension>> dims{
        test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16)};
    std::vector<shared_ptr<Attribute>> attrs{
        test::make_attribute<float>("a", Datatype::UINT64, false, 1, 0)};
    auto schema = test::make_array_schema(ArrayType::DENSE, dims, attrs);
    uint64_t index_domain[2]{0, 15};
    uint64_t index_tile_extent{16};
    uint64_t label_domain[2]{16, 31};
    uint64_t label_tile_extent{16};
    auto dim_label_schema = make_shared<DimensionLabelSchema>(
        HERE(),
        LabelOrder::INCREASING_LABELS,
        Datatype::UINT64,
        index_domain,
        &index_tile_extent,
        Datatype::UINT64,
        label_domain,
        &label_tile_extent);
    auto status =
        schema->add_dimension_label(0, "y", dim_label_schema, true, true);
    REQUIRE(status.ok());
    status = schema->add_dimension_label(0, "y", dim_label_schema, true, false);
    INFO(status.to_string());
    REQUIRE(!status.ok());
  }

  SECTION("Catch repeating label name not shared with dim with check") {
    std::vector<shared_ptr<Dimension>> dims{
        test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16)};
    std::vector<shared_ptr<Attribute>> attrs{
        test::make_attribute<float>("a", Datatype::UINT64, false, 1, 0)};
    auto schema = test::make_array_schema(ArrayType::DENSE, dims, attrs);
    uint64_t index_domain[2]{0, 15};
    uint64_t index_tile_extent{16};
    uint64_t label_domain[2]{16, 31};
    uint64_t label_tile_extent{16};
    auto dim_label_schema = make_shared<DimensionLabelSchema>(
        HERE(),
        LabelOrder::INCREASING_LABELS,
        Datatype::UINT64,
        index_domain,
        &index_tile_extent,
        Datatype::UINT64,
        label_domain,
        &label_tile_extent);
    auto status =
        schema->add_dimension_label(0, "y", dim_label_schema, true, true);
    REQUIRE(status.ok());
    status = schema->add_dimension_label(0, "y", dim_label_schema, false, true);
    REQUIRE(status.ok());
    status = schema->check();
    INFO(status.to_string());
    REQUIRE(!status.ok());
  }

  SECTION("Catch shared label/attribute name when adding label") {
    std::vector<shared_ptr<Dimension>> dims{
        test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16)};
    std::vector<shared_ptr<Attribute>> attrs{
        test::make_attribute<float>("a", Datatype::UINT64, false, 1, 0)};
    auto schema = test::make_array_schema(ArrayType::DENSE, dims, attrs);
    auto status = schema->check();
    uint64_t index_domain[2]{0, 15};
    uint64_t index_tile_extent{16};
    uint64_t label_domain[2]{16, 31};
    uint64_t label_tile_extent{16};
    auto dim_label_schema = make_shared<DimensionLabelSchema>(
        HERE(),
        LabelOrder::INCREASING_LABELS,
        Datatype::UINT64,
        index_domain,
        &index_tile_extent,
        Datatype::UINT64,
        label_domain,
        &label_tile_extent);
    status = schema->add_dimension_label(0, "a", dim_label_schema, true, false);
    INFO(status.to_string());
    REQUIRE(!status.ok());
  }

  SECTION("Catch shared label/attribute name with schema check") {
    std::vector<shared_ptr<Dimension>> dims{
        test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16)};
    std::vector<shared_ptr<Attribute>> attrs{
        test::make_attribute<float>("a", Datatype::UINT64, false, 1, 0)};
    auto schema = test::make_array_schema(ArrayType::DENSE, dims, attrs);
    auto status = schema->check();
    uint64_t index_domain[2]{0, 15};
    uint64_t index_tile_extent{16};
    uint64_t label_domain[2]{16, 31};
    uint64_t label_tile_extent{16};
    auto dim_label_schema = make_shared<DimensionLabelSchema>(
        HERE(),
        LabelOrder::INCREASING_LABELS,
        Datatype::UINT64,
        index_domain,
        &index_tile_extent,
        Datatype::UINT64,
        label_domain,
        &label_tile_extent);
    status = schema->add_dimension_label(0, "a", dim_label_schema, false, true);
    REQUIRE(status.ok());
    status = schema->check();
    INFO(status.to_string());
    REQUIRE(!status.ok());
  }

  SECTION("Catch shared label/dimension name when adding label") {
    std::vector<shared_ptr<Dimension>> dims{
        test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16),
        test::make_dimension<uint64_t>("y", Datatype::UINT64, 1, 0, 15, 16)};
    std::vector<shared_ptr<Attribute>> attrs{
        test::make_attribute<float>("a", Datatype::UINT64, false, 1, 0)};
    auto schema = test::make_array_schema(ArrayType::DENSE, dims, attrs);
    auto status = schema->check();
    uint64_t index_domain[2]{0, 15};
    uint64_t index_tile_extent{16};
    uint64_t label_domain[2]{16, 31};
    uint64_t label_tile_extent{16};
    auto dim_label_schema = make_shared<DimensionLabelSchema>(
        HERE(),
        LabelOrder::INCREASING_LABELS,
        Datatype::UINT64,
        index_domain,
        &index_tile_extent,
        Datatype::UINT64,
        label_domain,
        &label_tile_extent);
    status = schema->add_dimension_label(0, "y", dim_label_schema, true, false);
    INFO(status.to_string());
    REQUIRE(!status.ok());
  }

  SECTION("Catch shared label/dimension name with check") {
    std::vector<shared_ptr<Dimension>> dims{
        test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16),
        test::make_dimension<uint64_t>("y", Datatype::UINT64, 1, 0, 15, 16)};
    std::vector<shared_ptr<Attribute>> attrs{
        test::make_attribute<float>("a", Datatype::UINT64, false, 1, 0)};
    auto schema = test::make_array_schema(ArrayType::DENSE, dims, attrs);
    auto status = schema->check();
    uint64_t index_domain[2]{0, 15};
    uint64_t index_tile_extent{16};
    uint64_t label_domain[2]{16, 31};
    uint64_t label_tile_extent{16};
    auto dim_label_schema = make_shared<DimensionLabelSchema>(
        HERE(),
        LabelOrder::INCREASING_LABELS,
        Datatype::UINT64,
        index_domain,
        &index_tile_extent,
        Datatype::UINT64,
        label_domain,
        &label_tile_extent);
    status = schema->add_dimension_label(0, "y", dim_label_schema, false, true);
    REQUIRE(status.ok());
    status = schema->check();
    INFO(status.to_string());
    REQUIRE(!status.ok());
  }

  SECTION("Catch shared label/dimension name when adding label") {
    std::vector<shared_ptr<Dimension>> dims{
        test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16),
        test::make_dimension<uint64_t>("y", Datatype::UINT64, 1, 0, 15, 16)};
    std::vector<shared_ptr<Attribute>> attrs{
        test::make_attribute<float>("a", Datatype::UINT64, false, 1, 0)};
    auto schema = test::make_array_schema(ArrayType::DENSE, dims, attrs);
    auto status = schema->check();
    uint64_t index_domain[2]{0, 15};
    uint64_t index_tile_extent{16};
    uint64_t label_domain[2]{16, 31};
    uint64_t label_tile_extent{16};
    auto dim_label_schema = make_shared<DimensionLabelSchema>(
        HERE(),
        LabelOrder::INCREASING_LABELS,
        Datatype::UINT64,
        index_domain,
        &index_tile_extent,
        Datatype::UINT64,
        label_domain,
        &label_tile_extent);
    status = schema->add_dimension_label(0, "y", dim_label_schema, true, false);
    INFO(status.to_string());
    REQUIRE(!status.ok());
  }

  SECTION("Catch shared label/dimension name with check") {
    std::vector<shared_ptr<Dimension>> dims{
        test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16),
        test::make_dimension<uint64_t>("y", Datatype::UINT64, 1, 0, 15, 16)};
    std::vector<shared_ptr<Attribute>> attrs{
        test::make_attribute<float>("a", Datatype::UINT64, false, 1, 0)};
    auto schema = test::make_array_schema(ArrayType::DENSE, dims, attrs);
    auto status = schema->check();
    uint64_t index_domain[2]{0, 15};
    uint64_t index_tile_extent{16};
    uint64_t label_domain[2]{16, 31};
    uint64_t label_tile_extent{16};
    auto dim_label_schema = make_shared<DimensionLabelSchema>(
        HERE(),
        LabelOrder::INCREASING_LABELS,
        Datatype::UINT64,
        index_domain,
        &index_tile_extent,
        Datatype::UINT64,
        label_domain,
        &label_tile_extent);
    status =
        schema->add_dimension_label(0, "y", dim_label_schema, false, false);
    REQUIRE(status.ok());
    status = schema->check();
    INFO(status.to_string());
    REQUIRE(!status.ok());
  }
}

TEST_CASE(
    "Test catch mismatched dimension label definition",
    "[array_schema][dimension_label]") {
  std::vector<shared_ptr<Dimension>> dims{
      test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16)};
  std::vector<shared_ptr<Attribute>> attrs{
      test::make_attribute<float>("a", Datatype::UINT64, false, 1, 0)};
  auto schema = test::make_array_schema(ArrayType::DENSE, dims, attrs);
  auto status = schema->check();
  uint64_t index_domain[2]{0, 15};
  uint64_t index_tile_extent{16};
  uint64_t label_domain[2]{16, 31};
  uint64_t label_tile_extent{16};
  auto dim_label_schema = make_shared<DimensionLabelSchema>(
      HERE(),
      LabelOrder::INCREASING_LABELS,
      Datatype::INT64,
      index_domain,
      &index_tile_extent,
      Datatype::UINT64,
      label_domain,
      &label_tile_extent);
  status = schema->add_dimension_label(0, "x", dim_label_schema, true, false);
  REQUIRE(status.ok());
  status = schema->check();
  INFO(status.to_string());
  REQUIRE(!status.ok());
}

TEST_CASE(
    "Test URI generation for dimension labels",
    "[array_schema][dimension_label]") {
  // Create array schema with multiple dimension labels
  std::vector<shared_ptr<Dimension>> dims{
      test::make_dimension<uint64_t>("x", Datatype::UINT64, 1, 0, 15, 16)};
  std::vector<shared_ptr<Attribute>> attrs{
      test::make_attribute<float>("a", Datatype::UINT64, false, 1, 0)};
  auto schema = test::make_array_schema(ArrayType::DENSE, dims, attrs);
  auto status = schema->check();
  uint64_t index_domain[2]{0, 15};
  uint64_t index_tile_extent{16};
  uint64_t label_domain[2]{16, 31};
  uint64_t label_tile_extent{16};
  auto dim_label_schema = make_shared<DimensionLabelSchema>(
      HERE(),
      LabelOrder::INCREASING_LABELS,
      Datatype::UINT64,
      index_domain,
      &index_tile_extent,
      Datatype::UINT64,
      label_domain,
      &label_tile_extent);
  status = schema->add_dimension_label(0, "x", dim_label_schema, true, true);
  REQUIRE(status.ok());
  status = schema->add_dimension_label(0, "y", dim_label_schema, true, true);
  REQUIRE(status.ok());
  status = schema->add_dimension_label(0, "z", dim_label_schema, true, true);
  REQUIRE(status.ok());
  // Check dimension label schemas
  const auto& xref = schema->dimension_label_reference("x");
  REQUIRE(
      xref.uri().to_string() ==
      constants::array_dimension_labels_dir_name + "/l0");
  const auto& yref = schema->dimension_label_reference("y");
  REQUIRE(
      yref.uri().to_string() ==
      constants::array_dimension_labels_dir_name + "/l1");
  const auto& zref = schema->dimension_label_reference("z");
  REQUIRE(
      zref.uri().to_string() ==
      constants::array_dimension_labels_dir_name + "/l2");
}
