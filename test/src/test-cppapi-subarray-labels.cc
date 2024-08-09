/**
 * @file test-cppapi-dimension-label.cc
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
 * Tests the DimensionLabel C++ API
 */

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

#include <catch2/catch_test_macros.hpp>

using namespace tiledb::test;

TEST_CASE(
    "Subarray: Experimental C++ API", "[cppapi][Subarray][DimensionLabel]") {
  // Create temporary directory, array name, and context.
  TemporaryDirectoryFixture tmpdir{};
  auto array_name = tmpdir.fullpath("array_with_labels");
  tiledb::Context ctx{tmpdir.get_ctx(), false};

  // Create array and open for querying.
  tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
  tiledb::Domain domain(ctx);
  auto d1 = tiledb::Dimension::create<uint64_t>(ctx, "d1", {1, 16}, 8);
  auto d2 = tiledb::Dimension::create<uint64_t>(ctx, "d2", {1, 16}, 8);
  domain.add_dimensions(d1, d2);
  schema.set_domain(domain);
  auto a1 = tiledb::Attribute::create<double>(ctx, "a1");
  schema.add_attribute(a1);
  tiledb::ArraySchemaExperimental::add_dimension_label(
      ctx, schema, 0, "fixed_label", TILEDB_INCREASING_DATA, TILEDB_FLOAT64);
  tiledb::ArraySchemaExperimental::add_dimension_label(
      ctx,
      schema,
      0,
      "string_label",
      TILEDB_INCREASING_DATA,
      TILEDB_STRING_ASCII);
  tiledb::Array::create(array_name, schema);
  tiledb::Array array{ctx, array_name, TILEDB_READ};

  SECTION("Subarray with fixed-length label") {
    // Create the subarray.
    tiledb::Subarray subarray{ctx, array};

    // Add fixed length ranges.
    tiledb::SubarrayExperimental::add_label_range<double>(
        ctx, subarray, "fixed_label", 0.0, 1.0);
    tiledb::SubarrayExperimental::add_label_range<double>(
        ctx, subarray, "fixed_label", 1.5, 3.0);

    // Check the number or ranges.
    auto nrange = tiledb::SubarrayExperimental::label_range_num(
        ctx, subarray, "fixed_label");
    CHECK(nrange == 2);

    // Get a range back.
    std::array<double, 3> expected_range{1.5, 3.0, 0.0};
    std::array<double, 3> range =
        tiledb::SubarrayExperimental::label_range<double>(
            ctx, subarray, "fixed_label", 1);
    CHECK(range == expected_range);
  }

  SECTION("Subarray with var-length label") {
    // Create the subarray.
    tiledb::Subarray subarray{ctx, array};

    // Add variable length range.
    std::string start{"alpha"};
    std::string end{"beta"};
    tiledb::SubarrayExperimental::add_label_range(
        ctx, subarray, "string_label", start, end);

    // Get the number of ranges.
    auto nrange = tiledb::SubarrayExperimental::label_range_num(
        ctx, subarray, "string_label");
    CHECK(nrange == 1);

    // Get a range back.
    std::array<std::string, 2> expected_range{"alpha", "beta"};
    std::array<std::string, 2> range =
        tiledb::SubarrayExperimental::label_range(
            ctx, subarray, "string_label", 0);
    CHECK(range == expected_range);
  }
}
