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

#include <test/support/tdb_catch.h>

#include "test/support/src/array_helpers.h"
#include "tiledb/sm/cpp_api/tiledb"

#include <numeric>

using namespace tiledb;

TEST_CASE("Query condition null test constant folding", "[query-condition]") {
  const auto array_type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  const bool attr_nullable = GENERATE(true, false);
  const auto eq_op = GENERATE(TILEDB_EQ, TILEDB_NE);

  constexpr size_t num_rows = 4;

  Context ctx;
  std::string uri("query_condition_null_constant_fold");

  DYNAMIC_SECTION(
      "Null test query condition: (array_type, attr nullable, eq_op) = (" +
      std::string(array_type == TILEDB_SPARSE ? "SPARSE" : "DENSE") + ", " +
      std::to_string(attr_nullable) + ", " +
      (eq_op == TILEDB_EQ ? "EQ" : "NE") + ")") {
    // create array
    {
      ArraySchema schema(ctx, array_type);

      auto dim = Dimension::create<uint32_t>(ctx, "id", {{1, 4}});
      auto dom = Domain(ctx);
      dom.add_dimension(dim);
      schema.set_domain(dom);

      auto att_integral = Attribute::create<int64_t>(ctx, "a");
      int64_t fill = 12345;
      att_integral.set_fill_value(&fill, sizeof(int64_t));
      att_integral.set_nullable(attr_nullable);
      schema.add_attribute(att_integral);

      auto att_string = Attribute::create<std::string>(ctx, "s");
      att_string.set_nullable(attr_nullable);
      if (!attr_nullable) {
        att_string.set_fill_value("foobar", strlen("foobar"));
      }
      schema.add_attribute(att_string);

      Array::create(uri, schema);
    }

    test::DeleteArrayGuard delguard(ctx.ptr().get(), uri.c_str());

    // insert data
    {
      Array array(ctx, uri, TILEDB_WRITE);
      Query query(ctx, array);

      std::vector<uint32_t> ids = {1, 2, 3, 4};
      if (array_type == TILEDB_SPARSE) {
        query.set_data_buffer("id", ids);
      } else {
        Subarray subarray(ctx, array);
        subarray.add_range<uint32_t>(0, 1, num_rows);
        query.set_subarray(subarray);
      }

      std::vector<int64_t> values_a = {10, 20, 30, 40};

      std::string strs_s[] = {"ten", "twenty", "thirty", "forty"};
      std::vector<uint64_t> offsets_s = {0, 3, 9, 15};
      std::vector<char> values_s;
      for (const auto& s : strs_s) {
        values_s.insert(values_s.end(), s.begin(), s.end());
      }

      std::vector<uint8_t> always_valid = {1, 1, 1, 1};

      query.set_data_buffer("a", values_a)
          .set_data_buffer("s", values_s)
          .set_offsets_buffer("s", offsets_s);
      if (attr_nullable) {
        query.set_validity_buffer("a", always_valid);
        query.set_validity_buffer("s", always_valid);
      }

      REQUIRE(query.submit() == Query::Status::COMPLETE);
    }

    // then read with query condition
    const std::string qc_attr = GENERATE("id", "a", "s");
    SECTION("Filter attribute " + qc_attr) {
      Array array(ctx, uri, TILEDB_READ);
      Query query(ctx, array);

      std::vector<uint32_t> values_id(num_rows);
      std::vector<int64_t> values_a(num_rows);
      std::vector<uint64_t> offsets_s(num_rows);
      std::vector<char> values_s(num_rows * 16);

      std::vector<uint8_t> validity_a(num_rows);
      std::vector<uint8_t> validity_s(num_rows);

      QueryCondition qc(ctx);
      qc.init(qc_attr, nullptr, 0, eq_op);
      query.set_condition(qc)
          .set_data_buffer("id", values_id)
          .set_data_buffer("a", values_a)
          .set_data_buffer("s", values_s)
          .set_offsets_buffer("s", offsets_s);

      if (attr_nullable) {
        query.set_validity_buffer("a", validity_a)
            .set_validity_buffer("s", validity_s);
      }

      if (array_type == TILEDB_DENSE) {
        Subarray subarray(ctx, array);
        subarray.add_range<uint32_t>(0, 1, num_rows);
        query.set_subarray(subarray);
      }

      REQUIRE(query.submit() == Query::Status::COMPLETE);

      auto table = query.result_buffer_elements();
      values_id.resize(table["id"].second);
      validity_a.resize(table["a"].second);
      values_a.resize(table["a"].second);
      offsets_s.resize(table["s"].first);
      validity_s.resize(table["s"].first);
      values_s.resize(table["s"].second);

      if (eq_op == TILEDB_NE) {
        // NE NULL is always true, we will see all cells regardless of
        // configuration
        if (attr_nullable) {
          CHECK(validity_a == std::vector<uint8_t>{1, 1, 1, 1});
          CHECK(validity_s == std::vector<uint8_t>{1, 1, 1, 1});
        }
        CHECK(values_id.size() == num_rows);
        CHECK(values_a.size() == num_rows);
        CHECK(offsets_s.size() == num_rows);
        CHECK(values_id == std::vector<uint32_t>{1, 2, 3, 4});
        CHECK(values_a == std::vector<int64_t>{10, 20, 30, 40});
        CHECK(offsets_s == std::vector<uint64_t>{0, 3, 9, 15});
        CHECK(
            std::string(values_s.data(), table["s"].second) ==
            "tentwentythirtyforty");
      } else if (array_type == TILEDB_SPARSE) {
        // EQ NULL for sparse will filter all rows, we should see no data
        if (attr_nullable) {
          CHECK(validity_a.empty());
          CHECK(validity_s.empty());
        }
        CHECK(values_id.empty());
        CHECK(values_a.empty());
        CHECK(values_s.empty());
      } else if (attr_nullable) {
        // EQ NULL for dense with nullable sets null, return does not seem to be
        // defined (expectation was the fill value, but:
        // 1. fill value cannot be set on nullable attributes
        // 2. even if you cheat and set fill value *before* nullable, it does
        // something else for str)
        CHECK(validity_a == std::vector<uint8_t>{0, 0, 0, 0});
        CHECK(validity_s == std::vector<uint8_t>{0, 0, 0, 0});
        CHECK(values_id.size() == num_rows);
        CHECK(values_a.size() == num_rows);
        CHECK(offsets_s.size() == num_rows);
        CHECK(values_id == std::vector<uint32_t>{1, 2, 3, 4});
        // no check for the attributes per above notice
      } else {
        // EQ NULL for dense with nullable will return fill values
        CHECK(values_id.size() == num_rows);
        CHECK(values_a.size() == num_rows);
        CHECK(offsets_s.size() == num_rows);
        CHECK(values_id == std::vector<uint32_t>{1, 2, 3, 4});
        CHECK(values_a == std::vector<int64_t>{12345, 12345, 12345, 12345});
        CHECK(offsets_s == std::vector<uint64_t>{0, 6, 12, 18});
        CHECK(
            std::string(values_s.data(), table["s"].second) ==
            "foobarfoobarfoobarfoobar");
      }
    }
  }
}
