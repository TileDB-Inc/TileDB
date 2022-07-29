/**
 * @file tiledb/sm/array_schema/test/unit_dimension_label_schema.cc
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
 * This file contains unit tests for the DimensionLabelSchema class
 */

#include <catch.hpp>

#include "tiledb/sm/array_schema/test/unit_array_schema_helper.h"

using namespace tiledb::common;
using namespace tiledb::sm;

TEST_CASE(
    "Test dimension label schema construction", "[dimension_label_schema]") {
  // Create indexed array schema
  std::vector<shared_ptr<Dimension>> indexed_array_dims{
      test::make_dimension<uint64_t>("dim0", Datatype::UINT64, 1, 0, 10, 11)};
  std::vector<shared_ptr<Attribute>> indexed_array_attrs{
      test::make_attribute<uint64_t>("label0", Datatype::UINT64, false, 1, 0)};
  auto indexed_array_schema = test::make_array_schema(
      ArrayType::DENSE, indexed_array_dims, indexed_array_attrs);
  REQUIRE(indexed_array_schema->check().ok());
  // Create labelled array schema uint64_t label_domain[2] = {20, 30};
  std::vector<shared_ptr<Dimension>> labelled_array_dims{
      test::make_dimension<uint64_t>(
          "label0", Datatype::UINT64, 1, 10, 20, 11)};
  std::vector<shared_ptr<Attribute>> labelled_array_attrs{
      test::make_attribute<uint64_t>("dim0", Datatype::UINT64, false, 1, 0)};
  auto labelled_array_schema = test::make_array_schema(
      ArrayType::DENSE, labelled_array_dims, labelled_array_attrs);
  REQUIRE(labelled_array_schema->check().ok());
  // Create dimension label schema
  REQUIRE_NOTHROW(DimensionLabelSchema(
      LabelOrder::INCREASING_LABELS,
      indexed_array_schema,
      labelled_array_schema));
}

TEST_CASE(
    "Test invalid dimension label schema construction",
    "[dimension_label_schema]") {
  SECTION("Mismatched index definition") {
    // Create indexed array schema
    std::vector<shared_ptr<Dimension>> indexed_array_dims{
        test::make_dimension<uint64_t>("dim0", Datatype::UINT64, 1, 0, 10, 11)};
    std::vector<shared_ptr<Attribute>> indexed_array_attrs{
        test::make_attribute<uint64_t>(
            "label0", Datatype::UINT64, false, 1, 0)};
    auto indexed_array_schema = test::make_array_schema(
        ArrayType::DENSE, indexed_array_dims, indexed_array_attrs);
    REQUIRE(indexed_array_schema->check().ok());
    // Create labelled array schema uint64_t label_domain[2] = {20, 30};
    std::vector<shared_ptr<Dimension>> labelled_array_dims{
        test::make_dimension<uint64_t>(
            "label0", Datatype::UINT64, 1, 10, 20, 11)};
    std::vector<shared_ptr<Attribute>> labelled_array_attrs{
        test::make_attribute<uint64_t>("dim0", Datatype::UINT64, false, 2, 0)};
    auto labelled_array_schema = test::make_array_schema(
        ArrayType::DENSE, labelled_array_dims, labelled_array_attrs);
    REQUIRE(labelled_array_schema->check().ok());
    // Create dimension label schema
    REQUIRE_THROWS(DimensionLabelSchema(
        LabelOrder::INCREASING_LABELS,
        indexed_array_schema,
        labelled_array_schema));
  }

  SECTION("Mismatched label definition") {
    // Create indexed array schema
    std::vector<shared_ptr<Dimension>> indexed_array_dims{
        test::make_dimension<uint64_t>("dim0", Datatype::UINT64, 1, 0, 10, 11)};
    std::vector<shared_ptr<Attribute>> indexed_array_attrs{
        test::make_attribute<uint64_t>(
            "label0", Datatype::UINT64, false, 1, 0)};
    auto indexed_array_schema = test::make_array_schema(
        ArrayType::DENSE, indexed_array_dims, indexed_array_attrs);
    REQUIRE(indexed_array_schema->check().ok());
    // Create labelled array schema uint64_t label_domain[2] = {20, 30};
    std::vector<shared_ptr<Dimension>> labelled_array_dims{
        test::make_dimension<uint32_t>(
            "label0", Datatype::UINT32, 1, 10, 20, 11)};
    std::vector<shared_ptr<Attribute>> labelled_array_attrs{
        test::make_attribute<uint64_t>("dim0", Datatype::UINT64, false, 1, 0)};
    auto labelled_array_schema = test::make_array_schema(
        ArrayType::DENSE, labelled_array_dims, labelled_array_attrs);
    REQUIRE(labelled_array_schema->check().ok());
    // Create dimension label schema
    REQUIRE_THROWS(DimensionLabelSchema(
        LabelOrder::INCREASING_LABELS,
        indexed_array_schema,
        labelled_array_schema));
  }

  SECTION("Too many dimensions on index array") {
    // Create indexed array schema
    std::vector<shared_ptr<Dimension>> indexed_array_dims{
        test::make_dimension<uint64_t>("dim0", Datatype::UINT64, 1, 0, 10, 11),
        test::make_dimension<uint64_t>("dim1", Datatype::UINT64, 1, 0, 10, 11)};
    std::vector<shared_ptr<Attribute>> indexed_array_attrs{
        test::make_attribute<uint64_t>(
            "label0", Datatype::UINT64, false, 1, 0)};
    auto indexed_array_schema = test::make_array_schema(
        ArrayType::DENSE, indexed_array_dims, indexed_array_attrs);
    REQUIRE(indexed_array_schema->check().ok());
    // Create labelled array schema uint64_t label_domain[2] = {20, 30};
    std::vector<shared_ptr<Dimension>> labelled_array_dims{
        test::make_dimension<uint64_t>(
            "label0", Datatype::UINT64, 1, 10, 20, 11)};
    std::vector<shared_ptr<Attribute>> labelled_array_attrs{
        test::make_attribute<uint64_t>("dim0", Datatype::UINT64, false, 1, 0)};
    auto labelled_array_schema = test::make_array_schema(
        ArrayType::DENSE, labelled_array_dims, labelled_array_attrs);
    REQUIRE(labelled_array_schema->check().ok());
    // Create dimension label schema
    REQUIRE_THROWS(DimensionLabelSchema(
        LabelOrder::INCREASING_LABELS,
        indexed_array_schema,
        labelled_array_schema));
  }

  SECTION("Too many dimensions on label array") {
    // Create indexed array schema
    std::vector<shared_ptr<Dimension>> indexed_array_dims{
        test::make_dimension<uint64_t>("dim0", Datatype::UINT64, 1, 0, 10, 11)};
    std::vector<shared_ptr<Attribute>> indexed_array_attrs{
        test::make_attribute<uint64_t>(
            "label0", Datatype::UINT64, false, 1, 0)};
    auto indexed_array_schema = test::make_array_schema(
        ArrayType::DENSE, indexed_array_dims, indexed_array_attrs);
    REQUIRE(indexed_array_schema->check().ok());
    // Create labelled array schema uint64_t label_domain[2] = {20, 30};
    std::vector<shared_ptr<Dimension>> labelled_array_dims{
        test::make_dimension<uint64_t>(
            "label0", Datatype::UINT64, 1, 10, 20, 11),
        test::make_dimension<uint64_t>(
            "label1", Datatype::UINT64, 1, 10, 20, 11)};
    std::vector<shared_ptr<Attribute>> labelled_array_attrs{
        test::make_attribute<uint64_t>("dim0", Datatype::UINT64, false, 1, 0)};
    auto labelled_array_schema = test::make_array_schema(
        ArrayType::DENSE, labelled_array_dims, labelled_array_attrs);
    REQUIRE(labelled_array_schema->check().ok());
    // Create dimension label schema
    REQUIRE_THROWS(DimensionLabelSchema(
        LabelOrder::INCREASING_LABELS,
        indexed_array_schema,
        labelled_array_schema));
  }

  SECTION("Too many label attributes") {
    // Create indexed array schema
    std::vector<shared_ptr<Dimension>> indexed_array_dims{
        test::make_dimension<uint64_t>("dim0", Datatype::UINT64, 1, 0, 10, 11)};
    std::vector<shared_ptr<Attribute>> indexed_array_attrs{
        test::make_attribute<uint64_t>("label0", Datatype::UINT64, false, 1, 0),
        test::make_attribute<uint64_t>(
            "label1", Datatype::UINT64, false, 1, 0)};
    auto indexed_array_schema = test::make_array_schema(
        ArrayType::DENSE, indexed_array_dims, indexed_array_attrs);
    REQUIRE(indexed_array_schema->check().ok());
    // Create labelled array schema uint64_t label_domain[2] = {20, 30};
    std::vector<shared_ptr<Dimension>> labelled_array_dims{
        test::make_dimension<uint64_t>(
            "label0", Datatype::UINT64, 1, 10, 20, 11)};
    std::vector<shared_ptr<Attribute>> labelled_array_attrs{
        test::make_attribute<uint64_t>("dim0", Datatype::UINT64, false, 1, 0)};
    auto labelled_array_schema = test::make_array_schema(
        ArrayType::DENSE, labelled_array_dims, labelled_array_attrs);
    REQUIRE(labelled_array_schema->check().ok());
    // Create dimension label schema
    REQUIRE_THROWS(DimensionLabelSchema(
        LabelOrder::INCREASING_LABELS,
        indexed_array_schema,
        labelled_array_schema));
  }

  SECTION("Too many index attributes") {
    // Create indexed array schema
    std::vector<shared_ptr<Dimension>> indexed_array_dims{
        test::make_dimension<uint64_t>("dim0", Datatype::UINT64, 1, 0, 10, 11)};
    std::vector<shared_ptr<Attribute>> indexed_array_attrs{
        test::make_attribute<uint64_t>(
            "label0", Datatype::UINT64, false, 1, 0)};
    auto indexed_array_schema = test::make_array_schema(
        ArrayType::DENSE, indexed_array_dims, indexed_array_attrs);
    REQUIRE(indexed_array_schema->check().ok());
    // Create labelled array schema uint64_t label_domain[2] = {20, 30};
    std::vector<shared_ptr<Dimension>> labelled_array_dims{
        test::make_dimension<uint64_t>(
            "label0", Datatype::UINT64, 1, 10, 20, 11),
        test::make_dimension<uint64_t>(
            "label1", Datatype::UINT64, 1, 10, 20, 11)};
    std::vector<shared_ptr<Attribute>> labelled_array_attrs{
        test::make_attribute<uint64_t>("dim0", Datatype::UINT64, false, 1, 0)};
    auto labelled_array_schema = test::make_array_schema(
        ArrayType::DENSE, labelled_array_dims, labelled_array_attrs);
    REQUIRE(labelled_array_schema->check().ok());
    // Create dimension label schema
    REQUIRE_THROWS(DimensionLabelSchema(
        LabelOrder::INCREASING_LABELS,
        indexed_array_schema,
        labelled_array_schema));
  }
}

TEST_CASE(
    "Test DimensionLabelSchema::is_compatible_label",
    "[dimension_label_schema]") {
  // Create dimension label schema
  uint64_t index_domain[2] = {0, 15};
  uint64_t index_tile_extent = 8;
  double label_domain[2]{-1.0, 1.0};
  double label_tile_extent{2.0};
  auto dimension_label_schema = make_shared<DimensionLabelSchema>(
      HERE(),
      LabelOrder::INCREASING_LABELS,
      Datatype::UINT64,
      index_domain,
      &index_tile_extent,
      Datatype::FLOAT64,
      label_domain,
      &label_tile_extent);
  SECTION("Is valid") {
    auto dim =
        test::make_dimension<uint64_t>("dim", Datatype::UINT64, 1, 0, 15, 16);
    REQUIRE(dimension_label_schema->is_compatible_label(dim.get()));
  }
  SECTION("Datatype doesn't match") {
    auto dim =
        test::make_dimension<uint64_t>("dim", Datatype::INT64, 1, 0, 15, 16);
    REQUIRE(!dimension_label_schema->is_compatible_label(dim.get()));
  }
  SECTION("Domain doesn't match") {
    auto dim =
        test::make_dimension<uint64_t>("dim", Datatype::UINT64, 1, 16, 31, 16);
    REQUIRE(!dimension_label_schema->is_compatible_label(dim.get()));
  }
}
