/**
 * @file unit-QueryCondition.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * Tests the `QueryCondition` class.
 */

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/query/query_condition.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE(
    "QueryCondition: Test default constructor",
    "[QueryCondition][default_constructor]") {
  QueryCondition query_condition;
  REQUIRE(query_condition.empty());
  REQUIRE(query_condition.field_names().empty());

  ArraySchema array_schema;
  std::vector<ResultCellSlab> result_cell_slabs;
  REQUIRE(query_condition.apply(&array_schema, &result_cell_slabs, 1).ok());
}

TEST_CASE("QueryCondition: Test init", "[QueryCondition][value_constructor]") {
  std::string field_name = "foo";
  int value = 5;

  QueryCondition query_condition;
  REQUIRE(query_condition
              .init(
                  std::string(field_name),
                  &value,
                  sizeof(value),
                  QueryConditionOp::LT)
              .ok());
  REQUIRE(!query_condition.empty());
  REQUIRE(!query_condition.field_names().empty());
  REQUIRE(query_condition.field_names().count(field_name) == 1);
}

TEST_CASE(
    "QueryCondition: Test copy constructor",
    "[QueryCondition][copy_constructor]") {
  std::string field_name = "foo";
  int value = 5;

  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name),
                  &value,
                  sizeof(value),
                  QueryConditionOp::LT)
              .ok());
  QueryCondition query_condition2(query_condition1);
  REQUIRE(!query_condition2.empty());
  REQUIRE(!query_condition2.field_names().empty());
  REQUIRE(query_condition2.field_names().count(field_name) == 1);
}

TEST_CASE(
    "QueryCondition: Test move constructor",
    "[QueryCondition][move_constructor]") {
  std::string field_name = "foo";
  int value = 5;

  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name),
                  &value,
                  sizeof(value),
                  QueryConditionOp::LT)
              .ok());
  QueryCondition query_condition2(std::move(query_condition1));
  REQUIRE(!query_condition2.empty());
  REQUIRE(!query_condition2.field_names().empty());
  REQUIRE(query_condition2.field_names().count(field_name) == 1);
}

TEST_CASE(
    "QueryCondition: Test assignment operator",
    "[QueryCondition][assignment_operator]") {
  std::string field_name = "foo";
  int value = 5;

  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name),
                  &value,
                  sizeof(value),
                  QueryConditionOp::LT)
              .ok());
  QueryCondition query_condition2;
  query_condition2 = query_condition1;
  REQUIRE(!query_condition2.empty());
  REQUIRE(!query_condition2.field_names().empty());
  REQUIRE(query_condition2.field_names().count(field_name) == 1);
}

TEST_CASE(
    "QueryCondition: Test move-assignment operator",
    "[QueryCondition][move_assignment_operator]") {
  std::string field_name = "foo";
  int value = 5;

  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name),
                  &value,
                  sizeof(value),
                  QueryConditionOp::LT)
              .ok());
  QueryCondition query_condition2;
  query_condition2 = std::move(query_condition1);
  REQUIRE(!query_condition2.empty());
  REQUIRE(!query_condition2.field_names().empty());
  REQUIRE(query_condition2.field_names().count(field_name) == 1);
}

/**
 * Tests a comparison operator on all cells in a tile.
 *
 * @param op The relational query condition operator.
 * @param field_name The attribute name in the tile.
 * @param cells The number of cells in the tile.
 * @param result_tile The result tile.
 * @param values The values written to the tile.
 */
template <typename T>
void test_apply_cells(
    const QueryConditionOp op,
    const std::string& field_name,
    const uint64_t cells,
    ArraySchema* const array_schema,
    ResultTile* const result_tile,
    void* values);

/**
 * C-string template-specialization for `test_apply_cells`.
 */
template <>
void test_apply_cells<char*>(
    const QueryConditionOp op,
    const std::string& field_name,
    const uint64_t cells,
    ArraySchema* const array_schema,
    ResultTile* const result_tile,
    void* values) {
  const char* const cmp_value = "ae";
  QueryCondition query_condition;
  REQUIRE(query_condition
              .init(std::string(field_name), cmp_value, 2 * sizeof(char), op)
              .ok());
  // Run Check
  REQUIRE(query_condition.check(array_schema).ok());

  bool nullable = array_schema->attribute(field_name)->nullable();

  // Build expected indexes of cells that meet the query condition
  // criteria.
  std::vector<uint64_t> expected_cell_idx_vec;
  for (uint64_t i = 0; i < cells; ++i) {
    if (nullable && (i % 2 == 0))
      continue;

    switch (op) {
      case QueryConditionOp::LT:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) <
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::LE:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) <=
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GT:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) >
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GE:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) >=
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::EQ:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) ==
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::NE:
        if (std::string(&static_cast<char*>(values)[2 * i], 2) !=
            std::string(cmp_value, 2))
          expected_cell_idx_vec.emplace_back(i);
        break;
      default:
        REQUIRE(false);
    }
  }

  // Apply the query condition.
  ResultCellSlab result_cell_slab(result_tile, 0, cells);
  std::vector<ResultCellSlab> result_cell_slabs;
  result_cell_slabs.emplace_back(std::move(result_cell_slab));
  REQUIRE(query_condition.apply(array_schema, &result_cell_slabs, 1).ok());

  // Verify the result cell slabs contain the expected cells.
  auto expected_iter = expected_cell_idx_vec.begin();
  for (const auto& result_cell_slab : result_cell_slabs) {
    for (uint64_t cell_idx = result_cell_slab.start_;
         cell_idx < (result_cell_slab.start_ + result_cell_slab.length_);
         ++cell_idx) {
      REQUIRE(*expected_iter == cell_idx);
      ++expected_iter;
    }
  }

  if (nullable) {
    if (op == QueryConditionOp::EQ || op == QueryConditionOp::NE) {
      const uint64_t eq = op == QueryConditionOp::EQ ? 0 : 1;
      QueryCondition query_condition_eq_null;
      REQUIRE(
          query_condition_eq_null.init(std::string(field_name), nullptr, 0, op)
              .ok());
      // Run Check
      REQUIRE(query_condition_eq_null.check(array_schema).ok());

      ResultCellSlab result_cell_slab_eq_null(result_tile, 0, cells);
      std::vector<ResultCellSlab> result_cell_slabs_eq_null;
      result_cell_slabs_eq_null.emplace_back(
          std::move(result_cell_slab_eq_null));
      REQUIRE(query_condition_eq_null
                  .apply(array_schema, &result_cell_slabs_eq_null, 1)
                  .ok());

      REQUIRE(result_cell_slabs_eq_null.size() == (cells / 2));
      for (const auto& result_cell_slab : result_cell_slabs_eq_null) {
        REQUIRE((result_cell_slab.start_ % 2) == eq);
        REQUIRE(result_cell_slab.length_ == 1);
      }
    }

    return;
  }

  // Fetch the fill value.
  const void* fill_value;
  uint64_t fill_value_size;
  array_schema->attribute(field_name)
      ->get_fill_value(&fill_value, &fill_value_size);
  REQUIRE(fill_value_size == 2 * sizeof(char));

  // Build expected indexes of cells that meet the query condition
  // criteria with the fill value;
  std::vector<uint64_t> fill_expected_cell_idx_vec;
  for (uint64_t i = 0; i < cells; ++i) {
    switch (op) {
      case QueryConditionOp::LT:
        if (std::string(static_cast<const char*>(fill_value), 2) <
            std::string(cmp_value, 2))
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::LE:
        if (std::string(static_cast<const char*>(fill_value), 2) <=
            std::string(cmp_value, 2))
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GT:
        if (std::string(static_cast<const char*>(fill_value), 2) >
            std::string(cmp_value, 2))
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GE:
        if (std::string(static_cast<const char*>(fill_value), 2) >=
            std::string(cmp_value, 2))
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::EQ:
        if (std::string(static_cast<const char*>(fill_value), 2) ==
            std::string(cmp_value, 2))
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::NE:
        if (std::string(static_cast<const char*>(fill_value), 2) !=
            std::string(cmp_value, 2))
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      default:
        REQUIRE(false);
    }
  }

  // Apply the query condition with an empty result tile, which will
  // use the fill value.
  ResultCellSlab fill_result_cell_slab(nullptr, 0, cells);
  std::vector<ResultCellSlab> fill_result_cell_slabs;
  fill_result_cell_slabs.emplace_back(std::move(fill_result_cell_slab));
  REQUIRE(query_condition.apply(array_schema, &fill_result_cell_slabs, 1).ok());

  // Verify the fill result cell slabs contain the expected cells.
  auto fill_expected_iter = fill_expected_cell_idx_vec.begin();
  for (const auto& fill_result_cell_slab : fill_result_cell_slabs) {
    for (uint64_t cell_idx = fill_result_cell_slab.start_;
         cell_idx <
         (fill_result_cell_slab.start_ + fill_result_cell_slab.length_);
         ++cell_idx) {
      REQUIRE(*fill_expected_iter == cell_idx);
      ++fill_expected_iter;
    }
  }
}

/**
 * Non-specialized template type for `test_apply_cells`.
 */
template <typename T>
void test_apply_cells(
    const QueryConditionOp op,
    const std::string& field_name,
    const uint64_t cells,
    ArraySchema* const array_schema,
    ResultTile* const result_tile,
    void* values) {
  const T cmp_value = 5;
  QueryCondition query_condition;
  REQUIRE(
      query_condition.init(std::string(field_name), &cmp_value, sizeof(T), op)
          .ok());
  // Run Check
  REQUIRE(query_condition.check(array_schema).ok());

  // Build expected indexes of cells that meet the query condition
  // criteria.
  std::vector<uint64_t> expected_cell_idx_vec;
  for (uint64_t i = 0; i < cells; ++i) {
    switch (op) {
      case QueryConditionOp::LT:
        if (static_cast<T*>(values)[i] < cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::LE:
        if (static_cast<T*>(values)[i] <= cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GT:
        if (static_cast<T*>(values)[i] > cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GE:
        if (static_cast<T*>(values)[i] >= cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::EQ:
        if (static_cast<T*>(values)[i] == cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::NE:
        if (static_cast<T*>(values)[i] != cmp_value)
          expected_cell_idx_vec.emplace_back(i);
        break;
      default:
        REQUIRE(false);
    }
  }

  // Apply the query condition.
  ResultCellSlab result_cell_slab(result_tile, 0, cells);
  std::vector<ResultCellSlab> result_cell_slabs;
  result_cell_slabs.emplace_back(std::move(result_cell_slab));
  REQUIRE(query_condition.apply(array_schema, &result_cell_slabs, 1).ok());

  // Verify the result cell slabs contain the expected cells.
  auto expected_iter = expected_cell_idx_vec.begin();
  for (const auto& rcs : result_cell_slabs) {
    for (uint64_t cell_idx = rcs.start_; cell_idx < (rcs.start_ + rcs.length_);
         ++cell_idx) {
      REQUIRE(*expected_iter == cell_idx);
      ++expected_iter;
    }
  }

  // Fetch the fill value.
  const void* fill_value;
  uint64_t fill_value_size;
  array_schema->attribute(field_name)
      ->get_fill_value(&fill_value, &fill_value_size);
  REQUIRE(fill_value_size == sizeof(T));

  // Build expected indexes of cells that meet the query condition
  // criteria with the fill value;
  std::vector<uint64_t> fill_expected_cell_idx_vec;
  for (uint64_t i = 0; i < cells; ++i) {
    switch (op) {
      case QueryConditionOp::LT:
        if (*static_cast<const T*>(fill_value) < cmp_value)
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::LE:
        if (*static_cast<const T*>(fill_value) <= cmp_value)
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GT:
        if (*static_cast<const T*>(fill_value) > cmp_value)
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::GE:
        if (*static_cast<const T*>(fill_value) >= cmp_value)
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::EQ:
        if (*static_cast<const T*>(fill_value) == cmp_value)
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      case QueryConditionOp::NE:
        if (*static_cast<const T*>(fill_value) != cmp_value)
          fill_expected_cell_idx_vec.emplace_back(i);
        break;
      default:
        REQUIRE(false);
    }
  }

  // Apply the query condition with an empty result tile, which will
  // use the fill value.
  ResultCellSlab fill_result_cell_slab(nullptr, 0, cells);
  std::vector<ResultCellSlab> fill_result_cell_slabs;
  fill_result_cell_slabs.emplace_back(std::move(fill_result_cell_slab));
  REQUIRE(query_condition.apply(array_schema, &fill_result_cell_slabs, 1).ok());

  // Verify the fill result cell slabs contain the expected cells.
  auto fill_expected_iter = fill_expected_cell_idx_vec.begin();
  for (const auto& fill_result_cell_slab : fill_result_cell_slabs) {
    for (uint64_t cell_idx = fill_result_cell_slab.start_;
         cell_idx <
         (fill_result_cell_slab.start_ + fill_result_cell_slab.length_);
         ++cell_idx) {
      REQUIRE(*fill_expected_iter == cell_idx);
      ++fill_expected_iter;
    }
  }
}

/**
 * Tests each comparison operator on all cells in a tile.
 *
 * @param field_name The attribute name in the tile.
 * @param cells The number of cells in the tile.
 * @param result_tile The result tile.
 * @param values The values written to the tile.
 */
template <typename T>
void test_apply_operators(
    const std::string& field_name,
    const uint64_t cells,
    ArraySchema* const array_schema,
    ResultTile* const result_tile,
    void* values) {
  test_apply_cells<T>(
      QueryConditionOp::LT,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells<T>(
      QueryConditionOp::LE,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells<T>(
      QueryConditionOp::GT,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells<T>(
      QueryConditionOp::GE,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells<T>(
      QueryConditionOp::EQ,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
  test_apply_cells<T>(
      QueryConditionOp::NE,
      field_name,
      cells,
      array_schema,
      result_tile,
      values);
}

/**
 * Populates a tile and tests query condition comparisons against
 * each cell.
 *
 * @param field_name The attribute name in the tile.
 * @param cells The number of cells in the tile.
 * @param type The TILEDB data type of the attribute.
 * @param array_schema The array schema.
 * @param result_tile The result tile.
 */
template <typename T>
void test_apply_tile(
    const std::string& field_name,
    uint64_t cells,
    Datatype type,
    ArraySchema* array_schema,
    ResultTile* result_tile);

/**
 * C-string template-specialization for `test_apply_tile`.
 */
template <>
void test_apply_tile<char*>(
    const std::string& field_name,
    const uint64_t cells,
    const Datatype type,
    ArraySchema* const array_schema,
    ResultTile* const result_tile) {
  ResultTile::TileTuple* const tile_tuple = result_tile->tile_tuple(field_name);

  bool var_size = array_schema->attribute(field_name)->var_size();
  bool nullable = array_schema->attribute(field_name)->nullable();
  Tile* const tile =
      var_size ? &std::get<1>(*tile_tuple) : &std::get<0>(*tile_tuple);

  REQUIRE(tile->init_unfiltered(
                  constants::format_version,
                  type,
                  2 * cells * sizeof(char),
                  2 * sizeof(char),
                  0)
              .ok());

  char* values = static_cast<char*>(malloc(sizeof(char) * 2 * cells));
  for (uint64_t i = 0; i < cells; ++i) {
    values[i * 2] = 'a';
    values[(i * 2) + 1] = 'a' + static_cast<char>(i);
  }
  REQUIRE(tile->write(values, 2 * cells * sizeof(char)).ok());

  if (var_size) {
    Tile* const tile_offsets = &std::get<0>(*tile_tuple);
    REQUIRE(tile_offsets
                ->init_unfiltered(
                    constants::format_version,
                    constants::cell_var_offset_type,
                    10 * constants::cell_var_offset_size,
                    constants::cell_var_offset_size,
                    0)
                .ok());

    uint64_t* offsets =
        static_cast<uint64_t*>(malloc(sizeof(uint64_t) * cells));
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    REQUIRE(tile_offsets->write(offsets, cells * sizeof(uint64_t)).ok());
  }

  if (nullable) {
    Tile* const tile_validity = &std::get<2>(*tile_tuple);
    REQUIRE(tile_validity
                ->init_unfiltered(
                    constants::format_version,
                    constants::cell_validity_type,
                    10 * constants::cell_validity_size,
                    constants::cell_validity_size,
                    0)
                .ok());

    uint8_t* validity = static_cast<uint8_t*>(malloc(sizeof(uint8_t) * cells));
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE(tile_validity->write(validity, cells * sizeof(uint8_t)).ok());
  }

  test_apply_operators<char*>(
      field_name, cells, array_schema, result_tile, values);

  free(values);
}

/**
 * Non-specialized template type for `test_apply_tile`.
 */
template <typename T>
void test_apply_tile(
    const std::string& field_name,
    const uint64_t cells,
    const Datatype type,
    ArraySchema* const array_schema,
    ResultTile* const result_tile) {
  ResultTile::TileTuple* const tile_tuple = result_tile->tile_tuple(field_name);
  Tile* const tile = &std::get<0>(*tile_tuple);

  REQUIRE(
      tile->init_unfiltered(
              constants::format_version, type, cells * sizeof(T), sizeof(T), 0)
          .ok());
  T* values = static_cast<T*>(malloc(sizeof(T) * cells));
  for (uint64_t i = 0; i < cells; ++i) {
    values[i] = static_cast<T>(i);
  }
  REQUIRE(tile->write(values, cells * sizeof(T)).ok());

  test_apply_operators<T>(field_name, cells, array_schema, result_tile, values);

  free(values);
}

/**
 * Constructs a tile and tests query condition comparisons against
 * each cell.
 *
 * @param type The TILEDB data type of the attribute.
 * @param var_size Run the test with variable size attribute.
 * @param nullable Run the test with nullable attribute.
 */
template <typename T>
void test_apply(
    const Datatype type, bool var_size = false, bool nullable = false);

/**
 * C-string template-specialization for `test_apply`.
 */
template <>
void test_apply<char*>(const Datatype type, bool var_size, bool nullable) {
  REQUIRE(type == Datatype::STRING_ASCII);

  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const char* fill_value = "ac";

  // Initialize the array schema.
  ArraySchema array_schema;
  Attribute attr(field_name, type);
  REQUIRE(attr.set_nullable(nullable).ok());
  REQUIRE(attr.set_cell_val_num(var_size ? constants::var_num : 2).ok());

  if (!nullable) {
    REQUIRE(attr.set_fill_value(fill_value, 2 * sizeof(char)).ok());
  }

  REQUIRE(array_schema.add_attribute(&attr).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(domain.add_dimension(&dim).ok());
  REQUIRE(array_schema.set_domain(&domain).ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, &domain);
  result_tile.init_attr_tile(field_name);

  test_apply_tile<char*>(field_name, cells, type, &array_schema, &result_tile);
}

/**
 * Non-specialized template type for `test_apply`.
 */
template <typename T>
void test_apply(const Datatype type, bool var_size, bool nullable) {
  (void)var_size;
  (void)nullable;
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const T fill_value = 3;

  // Initialize the array schema.
  ArraySchema array_schema;
  Attribute attr(field_name, type);
  REQUIRE(attr.set_cell_val_num(1).ok());
  REQUIRE(attr.set_fill_value(&fill_value, sizeof(T)).ok());
  REQUIRE(array_schema.add_attribute(&attr).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(domain.add_dimension(&dim).ok());
  REQUIRE(array_schema.set_domain(&domain).ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, &domain);
  result_tile.init_attr_tile(field_name);

  test_apply_tile<T>(field_name, cells, type, &array_schema, &result_tile);
}

TEST_CASE("QueryCondition: Test apply", "[QueryCondition][apply]") {
  test_apply<int8_t>(Datatype::INT8);
  test_apply<uint8_t>(Datatype::UINT8);
  test_apply<int16_t>(Datatype::INT16);
  test_apply<uint16_t>(Datatype::UINT16);
  test_apply<int32_t>(Datatype::INT32);
  test_apply<uint32_t>(Datatype::UINT32);
  test_apply<int64_t>(Datatype::INT64);
  test_apply<uint64_t>(Datatype::UINT64);
  test_apply<float>(Datatype::FLOAT32);
  test_apply<double>(Datatype::FLOAT64);
  test_apply<char>(Datatype::CHAR);
  test_apply<int64_t>(Datatype::DATETIME_YEAR);
  test_apply<int64_t>(Datatype::DATETIME_MONTH);
  test_apply<int64_t>(Datatype::DATETIME_WEEK);
  test_apply<int64_t>(Datatype::DATETIME_DAY);
  test_apply<int64_t>(Datatype::DATETIME_HR);
  test_apply<int64_t>(Datatype::DATETIME_MIN);
  test_apply<int64_t>(Datatype::DATETIME_SEC);
  test_apply<int64_t>(Datatype::DATETIME_MS);
  test_apply<int64_t>(Datatype::DATETIME_US);
  test_apply<int64_t>(Datatype::DATETIME_NS);
  test_apply<int64_t>(Datatype::DATETIME_PS);
  test_apply<int64_t>(Datatype::DATETIME_FS);
  test_apply<int64_t>(Datatype::DATETIME_AS);
  test_apply<char*>(Datatype::STRING_ASCII);
  test_apply<char*>(Datatype::STRING_ASCII, true);
  test_apply<char*>(Datatype::STRING_ASCII, false, true);
}

TEST_CASE(
    "QueryCondition: Test combinations", "[QueryCondition][combinations]") {
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const Datatype type = Datatype::UINT64;

  // Initialize the array schema.
  ArraySchema array_schema;
  Attribute attr(field_name, type);
  REQUIRE(array_schema.add_attribute(&attr).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(domain.add_dimension(&dim).ok());
  REQUIRE(array_schema.set_domain(&domain).ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, &domain);
  result_tile.init_attr_tile(field_name);
  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);
  Tile* const tile = &std::get<0>(*tile_tuple);

  // Initialize and populate the data tile.
  REQUIRE(tile->init_unfiltered(
                  constants::format_version,
                  type,
                  cells * sizeof(uint64_t),
                  sizeof(uint64_t),
                  0)
              .ok());
  uint64_t* values = static_cast<uint64_t*>(malloc(sizeof(uint64_t) * cells));
  for (uint64_t i = 0; i < cells; ++i) {
    values[i] = i;
  }
  REQUIRE(tile->write(values, cells * sizeof(uint64_t)).ok());

  // Build a combined query for `> 3 AND <= 6`.
  uint64_t cmp_value_1 = 3;
  QueryCondition query_condition_1;
  REQUIRE(query_condition_1
              .init(
                  std::string(field_name),
                  &cmp_value_1,
                  sizeof(uint64_t),
                  QueryConditionOp::GT)
              .ok());
  // Run Check
  REQUIRE(query_condition_1.check(&array_schema).ok());
  uint64_t cmp_value_2 = 6;
  QueryCondition query_condition_2;
  REQUIRE(query_condition_2
              .init(
                  std::string(field_name),
                  &cmp_value_2,
                  sizeof(uint64_t),
                  QueryConditionOp::LE)
              .ok());
  // Run Check
  REQUIRE(query_condition_2.check(&array_schema).ok());
  QueryCondition query_condition_3;
  REQUIRE(query_condition_1
              .combine(
                  query_condition_2,
                  QueryConditionCombinationOp::AND,
                  &query_condition_3)
              .ok());

  ResultCellSlab result_cell_slab(&result_tile, 0, cells);
  std::vector<ResultCellSlab> result_cell_slabs;
  result_cell_slabs.emplace_back(std::move(result_cell_slab));

  REQUIRE(query_condition_3.apply(&array_schema, &result_cell_slabs, 1).ok());

  // Check that the cell slab now contains cell indexes 4, 5, and 6.
  REQUIRE(result_cell_slabs.size() == 1);
  REQUIRE(result_cell_slabs[0].start_ == 4);
  REQUIRE(result_cell_slabs[0].length_ == 3);

  free(values);
}

TEST_CASE(
    "QueryCondition: Test empty string", "[QueryCondition][empty_string]") {
  const std::string field_name = "foo";
  const uint64_t cells = 10;
  const char* fill_value = "ac";
  const Datatype type = Datatype::STRING_ASCII;
  bool var_size = true;
  bool nullable = GENERATE(true, false);
  QueryConditionOp op = GENERATE(QueryConditionOp::NE, QueryConditionOp::EQ);

  // Initialize the array schema.
  ArraySchema array_schema;
  Attribute attr(field_name, type);
  REQUIRE(attr.set_nullable(nullable).ok());
  REQUIRE(attr.set_cell_val_num(var_size ? constants::var_num : 2).ok());

  if (!nullable) {
    REQUIRE(attr.set_fill_value(fill_value, 2 * sizeof(char)).ok());
  }

  REQUIRE(array_schema.add_attribute(&attr).ok());
  Domain domain;
  Dimension dim("dim1", Datatype::UINT32);
  uint32_t bounds[2] = {1, cells};
  Range range(bounds, 2 * sizeof(uint32_t));
  REQUIRE(dim.set_domain(range).ok());
  REQUIRE(domain.add_dimension(&dim).ok());
  REQUIRE(array_schema.set_domain(&domain).ok());

  // Initialize the result tile.
  ResultTile result_tile(0, 0, &domain);
  result_tile.init_attr_tile(field_name);

  ResultTile::TileTuple* const tile_tuple = result_tile.tile_tuple(field_name);

  var_size = array_schema.attribute(field_name)->var_size();
  nullable = array_schema.attribute(field_name)->nullable();
  Tile* const tile =
      var_size ? &std::get<1>(*tile_tuple) : &std::get<0>(*tile_tuple);

  REQUIRE(tile->init_unfiltered(
                  constants::format_version,
                  type,
                  2 * (cells - 2) * sizeof(char),
                  2 * sizeof(char),
                  0)
              .ok());

  char* values = static_cast<char*>(malloc(
      sizeof(char) * 2 *
      (cells - 2)));  // static_cast<char*>(malloc(sizeof(char) * 2 * cells));

  // Empty strings are at idx 8 and 9
  for (uint64_t i = 0; i < (cells - 2); ++i) {
    values[i * 2] = 'a';
    values[(i * 2) + 1] = 'a' + static_cast<char>(i);
  }

  REQUIRE(tile->write(values, 2 * (cells - 2) * sizeof(char)).ok());

  if (var_size) {
    Tile* const tile_offsets = &std::get<0>(*tile_tuple);
    REQUIRE(tile_offsets
                ->init_unfiltered(
                    constants::format_version,
                    constants::cell_var_offset_type,
                    10 * constants::cell_var_offset_size,
                    constants::cell_var_offset_size,
                    0)
                .ok());

    uint64_t* offsets =
        static_cast<uint64_t*>(malloc(sizeof(uint64_t) * cells));
    uint64_t offset = 0;
    for (uint64_t i = 0; i < cells - 2; ++i) {
      offsets[i] = offset;
      offset += 2;
    }
    offsets[cells - 2] = offset;
    offsets[cells - 1] = offset;
    REQUIRE(tile_offsets->write(offsets, cells * sizeof(uint64_t)).ok());

    free(offsets);
  }

  if (nullable) {
    Tile* const tile_validity = &std::get<2>(*tile_tuple);
    REQUIRE(tile_validity
                ->init_unfiltered(
                    constants::format_version,
                    constants::cell_validity_type,
                    10 * constants::cell_validity_size,
                    constants::cell_validity_size,
                    0)
                .ok());

    uint8_t* validity = static_cast<uint8_t*>(malloc(sizeof(uint8_t) * cells));
    for (uint64_t i = 0; i < cells; ++i) {
      validity[i] = i % 2;
    }
    REQUIRE(tile_validity->write(validity, cells * sizeof(uint8_t)).ok());

    free(validity);
  }

  // Empty string as condition value
  const char* cmp_value = "";

  QueryCondition query_condition;
  REQUIRE(
      query_condition.init(std::string(field_name), &cmp_value, 0, op).ok());

  // Run Check
  REQUIRE(query_condition.check(&array_schema).ok());

  // Build expected indexes of cells that meet the query condition
  // criteria.
  std::vector<uint64_t> expected_cell_idx_vec;
  for (uint64_t i = 0; i < cells; ++i) {
    switch (op) {
      case QueryConditionOp::EQ:
        if ((nullable && (i % 2 == 0)) || (i >= 8)) {
          expected_cell_idx_vec.emplace_back(i);
        } else if (
            std::string(&static_cast<char*>(values)[2 * i], 2) ==
            std::string(cmp_value)) {
          expected_cell_idx_vec.emplace_back(i);
        }
        break;
      case QueryConditionOp::NE:
        if ((nullable && (i % 2 == 0)) || (i >= 8)) {
          continue;
        } else if (
            std::string(&static_cast<char*>(values)[2 * i], 2) !=
            std::string(cmp_value)) {
          expected_cell_idx_vec.emplace_back(i);
        }
        break;
      default:
        REQUIRE(false);
    }
  }

  // Apply the query condition.
  ResultCellSlab result_cell_slab(&result_tile, 0, cells);
  std::vector<ResultCellSlab> result_cell_slabs;
  result_cell_slabs.emplace_back(std::move(result_cell_slab));
  REQUIRE(query_condition.apply(&array_schema, &result_cell_slabs, 1).ok());

  // Verify the result cell slabs contain the expected cells.
  auto expected_iter = expected_cell_idx_vec.begin();
  for (const auto& result_cell_slab : result_cell_slabs) {
    for (uint64_t cell_idx = result_cell_slab.start_;
         cell_idx < (result_cell_slab.start_ + result_cell_slab.length_);
         ++cell_idx) {
      REQUIRE(*expected_iter == cell_idx);
      ++expected_iter;
    }
  }

  free(values);
}