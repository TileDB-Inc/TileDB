/**
 * @file unit-cppapi-query-condition.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB Inc.
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
 * Tests the C++ API for query condition related functions.
 */

#include <test/support/catch/array_schema.h>
#include <test/support/tdb_catch.h>

#include "test/support/src/array_helpers.h"
#include "test/support/src/helpers.h"
#include "tiledb/common/unreachable.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/query/query_condition.h"

#include <numeric>

using namespace tiledb;
using namespace tiledb::test;

TEST_CASE("Query condition null test", "[query-condition]") {
  const auto array_type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  const auto attr_datatype = GENERATE_CPPAPI_ALL_DATATYPES();
  const uint32_t attr_cell_val_num =
      GENERATE(1, 4, tiledb::sm::constants::var_num);
  const bool attr_nullable = GENERATE(true, false);

  const bool is_var = (attr_cell_val_num == tiledb::sm::constants::var_num);
  const size_t value_size = tiledb::sm::datatype_size(attr_datatype);
  if (attr_datatype == tiledb::sm::Datatype::ANY && !is_var) {
    // not supported
    return;
  }

  Context& ctx = vanilla_context_cpp();
  std::string uri("query_condition_null_test");

  DYNAMIC_SECTION(
      "Null test query condition: (array_type, datatype, cell_val_num, "
      "nullable) = (" +
      std::string(array_type == TILEDB_SPARSE ? "SPARSE" : "DENSE") + ", " +
      tiledb::sm::datatype_str(attr_datatype) + ", " +
      (is_var ? "VAR" : std::to_string(attr_cell_val_num)) + ", " +
      std::to_string(attr_nullable) + ")") {
    std::vector<uint8_t> fill_value;
    fill_value.resize(is_var ? value_size : attr_cell_val_num * value_size);
    std::iota(fill_value.begin(), fill_value.end(), '1');

    // create array
    {
      ArraySchema schema(ctx, array_type);

      auto dim = Dimension::create<uint32_t>(ctx, "id", {{1, 4}});
      auto dom = Domain(ctx);
      dom.add_dimension(dim);
      schema.set_domain(dom);

      auto att = Attribute::create(
                     ctx, "a", static_cast<tiledb_datatype_t>(attr_datatype))
                     .set_cell_val_num(attr_cell_val_num)
                     .set_fill_value(fill_value.data(), fill_value.size())
                     .set_nullable(attr_nullable);
      schema.add_attribute(att);

      Array::create(uri, schema);
    }

    test::DeleteArrayGuard delguard(ctx.ptr().get(), uri.c_str());

    // prepare data
    std::vector<uint32_t> w_dimension = {1, 2, 3};
    std::vector<uint64_t> w_offsets;
    std::vector<uint8_t> w_values;
    std::vector<uint8_t> w_validity = {1, 0, 1};
    if (is_var) {
      w_offsets = {0, value_size, value_size};
      w_values.resize(3 * 2 * value_size);
      std::iota(w_values.begin(), w_values.end(), 'B');
    } else {
      w_values.resize(3 * attr_cell_val_num * value_size);
      std::iota(w_values.begin(), w_values.end(), 'C');
    }

    // insert data
    {
      Array array(ctx, uri, TILEDB_WRITE);
      Query query(ctx, array);

      if (array_type == TILEDB_SPARSE) {
        query.set_data_buffer("id", w_dimension);
      } else {
        Subarray subarray(ctx, array);
        subarray.add_range<uint32_t>(0, 1, 3);
        query.set_subarray(subarray);
      }

      if (is_var) {
        query.set_data_buffer("a", static_cast<void*>(w_values.data()), 3 * 2)
            .set_offsets_buffer("a", w_offsets);
      } else {
        query.set_data_buffer(
            "a", static_cast<void*>(w_values.data()), 3 * attr_cell_val_num);
      }
      if (attr_nullable) {
        query.set_validity_buffer("a", w_validity);
      }

      REQUIRE(query.submit() == Query::Status::COMPLETE);
    }

    // then read with query condition
    const auto eq_op = GENERATE(TILEDB_EQ, TILEDB_NE);
    const std::string qc_attr = GENERATE("id", "a");

    std::set<tiledb::sm::Layout> layouts = {
        tiledb::sm::Layout::UNORDERED,
        tiledb::sm::Layout::ROW_MAJOR,
        tiledb::sm::Layout::COL_MAJOR,
        tiledb::sm::Layout::GLOBAL_ORDER};

    if (!(attr_cell_val_num == 1 || is_var)) {
      // wrong results for some reason
      layouts.erase(tiledb::sm::Layout::ROW_MAJOR);
      layouts.erase(tiledb::sm::Layout::COL_MAJOR);
    }
    if (array_type == TILEDB_DENSE) {
      // assertion failure
      layouts.erase(tiledb::sm::Layout::UNORDERED);
    }

    const auto layout = GENERATE_COPY(from_range(layouts));

    DYNAMIC_SECTION(
        tiledb::sm::layout_str(layout) + ": " + qc_attr +
        std::string(eq_op == TILEDB_EQ ? " IS" : " IS NOT") + " NULL") {
      Array array(ctx, uri, TILEDB_READ);
      Query query(ctx, array);
      query.set_layout(static_cast<tiledb_layout_t>(layout));

      std::vector<uint32_t> r_dimension(3);

      const size_t num_var_values_per_cell = 8;
      std::vector<uint8_t> r_values(
          3 * (is_var ? num_var_values_per_cell * value_size :
                        attr_cell_val_num * value_size));
      std::vector<uint64_t> r_offsets(3);
      std::vector<uint8_t> r_validity(3);

      QueryCondition qc(ctx);
      qc.init(qc_attr, nullptr, 0, eq_op);
      query.set_condition(qc).set_data_buffer("id", r_dimension);
      if (is_var) {
        query
            .set_data_buffer(
                "a",
                static_cast<void*>(r_values.data()),
                3 * num_var_values_per_cell)
            .set_offsets_buffer("a", r_offsets);
      } else {
        query.set_data_buffer(
            "a", static_cast<void*>(r_values.data()), 3 * attr_cell_val_num);
      }
      if (attr_nullable) {
        query.set_validity_buffer("a", r_validity);
      }

      if (array_type == TILEDB_DENSE) {
        Subarray subarray(ctx, array);
        subarray.add_range<uint32_t>(0, 1, 3);
        query.set_subarray(subarray);
      }

      REQUIRE(query.submit() == Query::Status::COMPLETE);

      auto table = query.result_buffer_elements();
      r_dimension.resize(table["id"].second);

      if (is_var) {
        r_validity.resize(table["a"].first);
        r_offsets.resize(table["a"].first);
        r_values.resize(table["a"].second * value_size);
      } else {
        r_validity.resize(table["a"].second / attr_cell_val_num);
        r_offsets.clear();
        r_values.resize(table["a"].second * value_size);
      }

      std::vector<uint8_t> expect_values;
      auto expect_cell = [&](size_t cell) {
        if (is_var) {
          expect_values.insert(
              expect_values.end(),
              w_values.begin() + w_offsets[cell],
              w_values.begin() + (cell + 1 == w_offsets.size() ?
                                      w_values.size() :
                                      w_offsets[cell + 1]));
        } else {
          expect_values.insert(
              expect_values.end(),
              w_values.begin() + (cell + 0) * attr_cell_val_num * value_size,
              w_values.begin() + (cell + 1) * attr_cell_val_num * value_size);
        }
      };
      auto expect_fill = [&]() {
        expect_values.insert(
            expect_values.end(), fill_value.begin(), fill_value.end());
      };

      if (qc_attr == "a" && attr_nullable) {
        // (value, NULL, value)
        if (array_type == TILEDB_SPARSE) {
          if (eq_op == TILEDB_NE) {
            // (value, value)
            CHECK(
                r_dimension ==
                std::vector<uint32_t>{w_dimension[0], w_dimension[2]});
            CHECK(r_validity == std::vector<uint8_t>{1, 1});

            std::vector<uint8_t> expect;
            if (is_var) {
              CHECK(
                  r_offsets ==
                  std::vector<uint64_t>{w_offsets[0], w_offsets[2]});
            }
            expect_cell(0);
            expect_cell(2);
            CHECK(r_values == expect_values);
          } else {
            // (NULL)
            CHECK(r_dimension == std::vector<uint32_t>{w_dimension[1]});
            CHECK(r_validity == std::vector<uint8_t>{0});
            if (is_var) {
              CHECK(r_offsets == std::vector<uint64_t>{0});
            }
            expect_cell(1);
            CHECK(r_values == expect_values);
          }
        } else {
          // we always will have three values, the filtered ones are replaced
          // with fill value
          if (eq_op == TILEDB_NE) {
            // (value, fill, value)
            CHECK(r_validity == std::vector<uint8_t>{1, 0, 1});
            if (is_var) {
              CHECK(
                  r_offsets ==
                  std::vector<uint64_t>{0, value_size, 2 * value_size});
            }
            expect_cell(0);
            expect_fill();
            expect_cell(2);
            CHECK(r_values == expect_values);
          } else {
            // (fill, value, fill)
            CHECK(r_validity == std::vector<uint8_t>{0, 0, 0});
            if (is_var) {
              CHECK(
                  r_offsets ==
                  std::vector<uint64_t>{0, value_size, value_size});
            }
            expect_fill();
            expect_cell(1);
            expect_fill();
            CHECK(r_values == expect_values);
          }
        }
      } else {
        if (eq_op == TILEDB_NE) {
          // no NULLs, this is always true, we should see all cells
          CHECK(r_dimension == w_dimension);
          if (attr_nullable) {
            CHECK(r_validity == w_validity);
          }
          if (is_var) {
            CHECK(r_offsets == w_offsets);
          }
          CHECK(r_values == w_values);
        } else {
          // EQ NULL will filter all rows
          if (array_type == TILEDB_SPARSE) {
            // they actually will be filtered
            CHECK(r_dimension.empty());
            CHECK(r_validity.empty());
            CHECK(r_offsets.empty());
            CHECK(r_values.empty());
          } else {
            // they will be replaced with fill values
            if (attr_nullable) {
              CHECK(r_validity == std::vector<uint8_t>{0, 0, 0});
            }
            if (is_var) {
              CHECK(
                  r_offsets ==
                  std::vector<uint64_t>{0, value_size, 2 * value_size});
            }
            expect_fill();
            expect_fill();
            expect_fill();
            CHECK(r_values == expect_values);
          }
        }
      }
    }
  }
}

TEST_CASE(
    "Query condition string comparison with null byte", "[query-condition]") {
  const auto array_type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  const auto attr_datatype = GENERATE(
      tiledb::sm::Datatype::CHAR,
      tiledb::sm::Datatype::STRING_ASCII,
      tiledb::sm::Datatype::STRING_UTF8);

  Context& ctx = vanilla_context_cpp();
  std::string uri("query_condition_string_comparison_null_byte");

  // prepare data
  const std::string fill_string("\0fill_value", 11);
  const std::vector<std::string> strings = {
      std::string("\0bar", 4),
      std::string("\0foo", 4),
      std::string("\0foobar", 7)};
  std::vector<uint32_t> dimension;
  std::vector<char> a_input;
  std::vector<uint64_t> a_input_offsets = {0};

  for (uint64_t i = 0; i < strings.size(); i++) {
    dimension.push_back(i);
    const auto& s = strings[i];
    a_input.insert(a_input.end(), s.begin(), s.end());
    a_input_offsets.push_back(a_input_offsets.back() + s.size());
  }
  a_input_offsets.erase(a_input_offsets.end() - 1);

  DYNAMIC_SECTION(
      "(array_type, datatype) = (" +
      std::string(array_type == TILEDB_SPARSE ? "SPARSE" : "DENSE") + ", " +
      tiledb::sm::datatype_str(attr_datatype) + ")") {
    // create array
    {
      ArraySchema schema(ctx, array_type);

      auto dim = Dimension::create<uint32_t>(
          ctx, "id", {{0, static_cast<uint32_t>(strings.size())}});
      auto dom = Domain(ctx);
      dom.add_dimension(dim);
      schema.set_domain(dom);

      auto att = Attribute::create(
                     ctx, "a", static_cast<tiledb_datatype_t>(attr_datatype))
                     .set_cell_val_num(tiledb::sm::constants::var_num)
                     .set_fill_value(fill_string.data(), fill_string.size());
      schema.add_attribute(att);

      Array::create(uri, schema);
    }

    test::DeleteArrayGuard delguard(ctx.ptr().get(), uri.c_str());

    // insert data
    {
      Array array(ctx, uri, TILEDB_WRITE);
      Query query(ctx, array);

      if (array_type == TILEDB_SPARSE) {
        query.set_data_buffer("id", dimension);
      } else {
        Subarray subarray(ctx, array);
        subarray.add_range<uint32_t>(
            0, 0, static_cast<uint32_t>(strings.size() - 1));
        query.set_subarray(subarray);
      }

      query.set_data_buffer("a", a_input)
          .set_offsets_buffer("a", a_input_offsets);

      REQUIRE(query.submit() == Query::Status::COMPLETE);
    }

    // read back, applying condition
    const auto eq_op = GENERATE(
        sm::QueryConditionOp::LT,
        sm::QueryConditionOp::LE,
        sm::QueryConditionOp::EQ,
        sm::QueryConditionOp::GE,
        sm::QueryConditionOp::GT,
        sm::QueryConditionOp::NE);
    const auto cmp_idx = GENERATE(0, 1, 2);

    DYNAMIC_SECTION(
        "(eq_op, cmp_idx) = (" + query_condition_op_str(eq_op) + ", " +
        std::to_string(cmp_idx) + ")") {
      Array array(ctx, uri, TILEDB_READ);
      Query query(ctx, array);

      std::vector<uint32_t> r_dimension(dimension.size());
      std::vector<char> a_output(256);
      std::vector<uint64_t> a_output_offsets(a_input_offsets.size());

      QueryCondition qc(ctx);
      qc.init(
          "a",
          strings[cmp_idx].data(),
          strings[cmp_idx].size(),
          static_cast<tiledb_query_condition_op_t>(eq_op));

      query.set_condition(qc)
          .set_data_buffer("id", r_dimension)
          .set_data_buffer("a", a_output)
          .set_offsets_buffer("a", a_output_offsets);

      if (array_type == TILEDB_DENSE) {
        Subarray subarray(ctx, array);
        subarray.add_range<uint32_t>(
            0, 0, static_cast<uint32_t>(strings.size() - 1));
        query.set_subarray(subarray);
      }

      REQUIRE(query.submit() == Query::Status::COMPLETE);

      auto table = query.result_buffer_elements();
      r_dimension.resize(table["id"].second);
      a_output_offsets.resize(table["a"].first);
      a_output.resize(table["a"].second);

      std::vector<std::string> strings_out;
      for (uint64_t i = 0; i < r_dimension.size(); i++) {
        const uint64_t end =
            (i + 1 == r_dimension.size() ? a_output.size() :
                                           a_output_offsets[i + 1]);
        strings_out.push_back(
            std::string(&a_output[a_output_offsets[i]], &a_output[end]));
      }

      std::vector<std::string> expect_strings;
      for (uint64_t i = 0; i < strings.size(); i++) {
        switch (eq_op) {
          case sm::QueryConditionOp::LT:
            if (strings[i] < strings[cmp_idx]) {
              expect_strings.push_back(strings[i]);
            } else if (array_type == TILEDB_DENSE) {
              expect_strings.push_back(fill_string);
            }
            break;
          case sm::QueryConditionOp::LE:
            if (strings[i] <= strings[cmp_idx]) {
              expect_strings.push_back(strings[i]);
            } else if (array_type == TILEDB_DENSE) {
              expect_strings.push_back(fill_string);
            }
            break;
          case sm::QueryConditionOp::EQ:
            if (strings[i] == strings[cmp_idx]) {
              expect_strings.push_back(strings[i]);
            } else if (array_type == TILEDB_DENSE) {
              expect_strings.push_back(fill_string);
            }
            break;
          case sm::QueryConditionOp::GE:
            if (strings[i] >= strings[cmp_idx]) {
              expect_strings.push_back(strings[i]);
            } else if (array_type == TILEDB_DENSE) {
              expect_strings.push_back(fill_string);
            }
            break;
          case sm::QueryConditionOp::GT:
            if (strings[i] > strings[cmp_idx]) {
              expect_strings.push_back(strings[i]);
            } else if (array_type == TILEDB_DENSE) {
              expect_strings.push_back(fill_string);
            }
            break;
          case sm::QueryConditionOp::NE:
            if (strings[i] != strings[cmp_idx]) {
              expect_strings.push_back(strings[i]);
            } else if (array_type == TILEDB_DENSE) {
              expect_strings.push_back(fill_string);
            }
            break;
          default:
            stdx::unreachable();
        }
      }
      CHECK(expect_strings == strings_out);
    }
  }
}
