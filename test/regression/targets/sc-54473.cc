/**
 * @file   sc-54473.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 */

#include <stdio.h>
#include <stdlib.h>
#include <tiledb/tiledb.h>
#include <iostream>
#include <optional>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>
#include <vector>

#include <test/support/tdb_catch.h>

using namespace tiledb;

static void create_or_replace_array(
    Context& ctx, const char* array_name, int tile_capacity) {
  if (Object::object(ctx, array_name).type() != Object::Type::Invalid) {
    Object::remove(ctx, array_name);
  }

  auto dx = Dimension::create<uint64_t>(ctx, "x", {{1, 100}}, 10);

  // Create domain
  Domain domain(ctx);
  domain.add_dimension(dx);

  // Create a single attribute "a"
  Attribute a(ctx, "a", TILEDB_UINT64);
  a.set_nullable(true);

  // Create array schema
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_capacity(tile_capacity);
  schema.set_domain(domain);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  schema.add_attribute(a);

  // Create array
  Array::create(array_name, schema);
}

static void write_array(Context& ctx, const char* array_name) {
  // Data
  std::vector<uint64_t> x = {1, 2, 3, 4, 5, 6, 7, 8};
  std::vector<uint64_t> atts = {
      3000000000000000000,
      3000000000000000001,
      3000000000000000002,
      3000000000000000003,
      3000000000000000004,
      3000000000000000005,
      3000000000000000006,
      3000000000000000007};

  std::vector<uint8_t> a_validity = {1, 1, 1, 1, 1, 1, 1, 1};

  // Open array for writing
  Array array(ctx, array_name, TILEDB_WRITE);

  Query query(ctx, array);
  query.set_data_buffer("x", x).set_data_buffer("a", atts).set_validity_buffer(
      "a", a_validity);

  query.submit();
  array.close();
}

static std::optional<uint64_t> query_sum(Context& ctx, const char* array_name) {
  Array array(ctx, array_name, TILEDB_READ);

  Query query(ctx, array);
  query.set_layout(TILEDB_UNORDERED);

  QueryChannel default_channel = QueryExperimental::get_default_channel(query);

  ChannelOperation op_sum =
      QueryExperimental::create_unary_aggregate<SumOperator>(query, "a");
  default_channel.apply_aggregate("Sum", op_sum);

  std::vector<uint64_t> sum(1);
  std::vector<uint8_t> sum_validity(1);
  query.set_data_buffer("Sum", sum).set_validity_buffer("Sum", sum_validity);

  query.submit();
  query.finalize();

  if (sum_validity[0]) {
    return sum[0];
  } else {
    return std::nullopt;
  }
}

bool array_exists(Context& ctx, const char* uri);

TEST_CASE(
    "SC-54473 sum aggregate overflow unchecked in tile metadata",
    "[regression][bug][sc-54473][!shouldfail]") {
  Context ctx;

  const int tile_capacity = GENERATE(1, 2, 4, 8, 16);

  DYNAMIC_SECTION("Tile capacity: " + std::to_string(tile_capacity)) {
    std::string uri(
        "sc-54473-sum-overflow-tile-capacity-" + std::to_string(tile_capacity));
    create_or_replace_array(ctx, uri.c_str(), tile_capacity);
    write_array(ctx, uri.c_str());

    const auto sum = query_sum(ctx, uri.c_str());

    // EXPECTATION:
    // We should always detect overflow and return NULL.
    //
    // REALITY:
    // We do check overflow when adding the tile metadata sums,
    // but we do not check overflow when computing the tile
    // metadata sums. As a result we do see NULL here when the
    // tile capacity is larger.
    CHECK(!sum.has_value());
  }
}
