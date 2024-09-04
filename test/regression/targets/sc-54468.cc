/**
 * @file   sc-54468.cc
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
    Context& ctx, const char* array_name, bool is_nullable) {
  if (Object::object(ctx, array_name).type() != Object::Type::Invalid) {
    Object::remove(ctx, array_name);
  }

  auto dx = Dimension::create<uint64_t>(ctx, "x", {{1, 100}}, 10);

  // Create domain
  Domain domain(ctx);
  domain.add_dimension(dx);

  // Create a single attribute "a" so each (i,j) cell can store a character
  Attribute a(ctx, "a", TILEDB_UINT64);
  a.set_nullable(is_nullable);

  // Create array schema
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  schema.add_attribute(a);

  // Create array
  Array::create(array_name, schema);
}

static void write_array(
    Context& ctx, const char* array_name, bool is_nullable) {
  // Data
  std::vector<uint64_t> x = {1, 2, 3, 4, 5, 6, 7, 8};
  std::vector<uint64_t> atts = {10, 20, 30, 40, 50, 60, 70, 80};

  std::vector<uint8_t> a_validity = {1, 1, 1, 1, 1, 1, 1, 1};

  // Open array for writing
  Array array(ctx, array_name, TILEDB_WRITE);

  Query query(ctx, array);
  query.set_data_buffer("x", x).set_data_buffer("a", atts);
  if (is_nullable) {
    query.set_validity_buffer("a", a_validity);
  }

  query.submit();
  array.close();
}

static std::pair<std::optional<uint64_t>, std::optional<uint64_t>>
query_min_max(
    Context& ctx,
    const char* array_name,
    bool is_nullable,
    std::optional<std::pair<uint64_t, uint64_t>> subarray) {
  // note, use C API because the CPP API doesn't seem to have Min yet
  Array array(ctx, array_name, TILEDB_READ);

  Query query(ctx, array);
  query.set_layout(TILEDB_UNORDERED);
  if (subarray) {
    Subarray s(ctx, array);
    s.add_range(0, subarray->first, subarray->second);
    query.set_subarray(s);
  }

  QueryChannel default_channel = QueryExperimental::get_default_channel(query);

  ChannelOperation op_min =
      QueryExperimental::create_unary_aggregate<MinOperator>(query, "a");
  default_channel.apply_aggregate("Min", op_min);

  ChannelOperation op_max =
      QueryExperimental::create_unary_aggregate<MaxOperator>(query, "a");
  default_channel.apply_aggregate("Max", op_max);

  std::vector<uint64_t> min(1);
  std::vector<uint8_t> min_validity(1);
  query.set_data_buffer("Min", min);
  if (is_nullable) {
    query.set_validity_buffer("Min", min_validity);
  }

  std::vector<uint64_t> max(1);
  std::vector<uint8_t> max_validity(1);
  query.set_data_buffer("Max", max);
  if (is_nullable) {
    query.set_validity_buffer("Max", max_validity);
  }

  query.submit();
  query.finalize();

  std::optional<uint64_t> maybe_min, maybe_max;
  if (!is_nullable || min_validity[0]) {
    maybe_min = min[0];
  }
  if (!is_nullable || max_validity[0]) {
    maybe_max = max[0];
  }
  return std::make_pair(maybe_min, maybe_max);
}

bool array_exists(Context& ctx, const char* uri);

TEST_CASE(
    "SC-54468 min/max aggregate on empty nullable attribute",
    "[regression][bug][sc-54468]") {
  Context ctx;
  std::string uri("sc-54468-empty-min-max-nullable");

  const bool is_attribute_nullable = true;

  create_or_replace_array(ctx, uri.c_str(), is_attribute_nullable);

  const auto extrema =
      query_min_max(ctx, uri.c_str(), is_attribute_nullable, std::nullopt);
  const auto min = extrema.first;
  const auto max = extrema.second;

  CHECK(!min.has_value());
  CHECK(!max.has_value());
}

TEST_CASE(
    "SC-54468 min/max aggregate on empty non-nullable attribute",
    "[regression][bug][sc-54468][!shouldfail]") {
  Context ctx;
  std::string uri("sc-54468-empty-min-max-not-nullable");

  const bool is_attribute_nullable = false;

  create_or_replace_array(ctx, uri.c_str(), is_attribute_nullable);

  const auto extrema =
      query_min_max(ctx, uri.c_str(), is_attribute_nullable, std::nullopt);
  const auto min = extrema.first;
  const auto max = extrema.second;

  // EXPECTATION:
  // In SQL the min/max functions return NULL if there are no
  // non-NULL values in the input.
  // In this example the arrays are empty, so there are no
  // non-NULL values, so to be compliant with SQL (which is what
  // most novice users would expect) we must return NULL.
  //
  // REALITY:
  // We don't do that and return 0.
  // It is an error to set validity buffers on the Min/Max
  // operation output because the underlying attribute "a"
  // is not nullable.
  CHECK(!min.has_value());
  CHECK(!max.has_value());
}

TEST_CASE(
    "SC-54468 min/max aggregate on nullable attribute, no results pass filters"
    "[regression][bug][sc-54468]") {
  Context ctx;
  std::string uri("sc-54468-filtered-min-max-nullable");

  const bool is_attribute_nullable = true;

  create_or_replace_array(ctx, uri.c_str(), is_attribute_nullable);
  write_array(ctx, uri.c_str(), is_attribute_nullable);

  // subarray filters all data
  const auto extrema = query_min_max(
      ctx, uri.c_str(), is_attribute_nullable, std::make_pair(10, 20));
  const auto min = extrema.first;
  const auto max = extrema.second;

  CHECK(!min.has_value());
  CHECK(!max.has_value());
}

TEST_CASE(
    "SC-54468 min/max aggregate on non-nullable attribute, no results pass "
    "filters"
    "[regression][bug][sc-54468][!shouldfail]") {
  Context ctx;
  std::string uri("sc-54468-filtered-min-max-not-nullable");

  const bool is_attribute_nullable = false;

  create_or_replace_array(ctx, uri.c_str(), is_attribute_nullable);
  write_array(ctx, uri.c_str(), is_attribute_nullable);

  const auto extrema = query_min_max(
      ctx, uri.c_str(), is_attribute_nullable, std::make_pair(10, 20));
  const auto min = extrema.first;
  const auto max = extrema.second;

  // EXPECTATION:
  // In SQL the min/max functions return NULL if there are no
  // non-NULL values in the input.
  // In this example the subarray filters out cells, so there are no
  // non-NULL values, so to be compliant with SQL (which is what
  // most novice users would expect) we must return NULL.
  //
  // REALITY:
  // We don't do that and return 0.
  // It is an error to set validity buffers on the Min/Max
  // operation output because the underlying attribute "a"
  // is not nullable.
  CHECK(!min.has_value());
  CHECK(!max.has_value());
}
