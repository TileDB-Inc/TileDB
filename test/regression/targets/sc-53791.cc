/**
 * @file   sc-53791.cc
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
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>
#include <vector>

#include <catch2/catch_template_test_macros.hpp>

using namespace tiledb;

static void create_array(Context& ctx, const char* array_name) {
  auto dx = Dimension::create<uint64_t>(ctx, "x", {{1, 100}}, 10);

  // Create domain
  Domain domain(ctx);
  domain.add_dimension(dx);

  // Create a single attribute "a" so each (i,j) cell can store a character
  Attribute a(ctx, "a", TILEDB_UINT64);
  a.set_cell_val_num(TILEDB_VAR_NUM);
  a.set_nullable(true);

  // Create array schema
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  schema.add_attribute(a);

  // Create array
  Array::create(array_name, schema);
}

static void write_array(Context& ctx, const char* array_name) {
  // Open array for writing
  Array array(ctx, array_name, TILEDB_WRITE);

  // Data
  std::vector<uint64_t> x = {1};
  std::vector<uint64_t> a = {0};
  std::vector<uint64_t> a_offsets = {0};
  std::vector<uint8_t> a_validity = {1};

  Query query(ctx, array);
  query.set_data_buffer("x", x)
      .set_data_buffer("a", a)
      .set_offsets_buffer("a", a_offsets)
      .set_validity_buffer("a", a_validity);

  query.submit();
  query.finalize();
  array.close();
}

static std::pair<uint64_t, uint64_t> query_null_count(
    Context& ctx, const char* array_name) {
  // note, use C API because the CPP API doesn't seem to have Min yet
  Array array(ctx, array_name, TILEDB_READ);

  Query query(ctx, array);
  query.set_layout(TILEDB_UNORDERED);

  QueryChannel default_channel = QueryExperimental::get_default_channel(query);
  default_channel.apply_aggregate("Count", CountOperation());
  ChannelOperation operation =
      QueryExperimental::create_unary_aggregate<NullCountOperator>(query, "a");
  default_channel.apply_aggregate("Null Count", operation);

  std::vector<uint64_t> count(1);
  query.set_data_buffer("Count", count);

  std::vector<uint64_t> null_count(1);
  query.set_data_buffer("Null Count", null_count);

  /*
   * FIXME: this gives an error
   *
   * Error: Caught std::exception: FragmentMetadata: Trying to
   * access tile min metadata that's not present
   */
  query.submit();
  query.finalize();

  return std::make_pair(count[0], null_count[0]);
}

bool array_exists(Context& ctx, const char* uri);

TEST_CASE(
    "SC-53791 var value TILEDB_UINT64 does not work",
    "[regression][bug][sc-53791][!shouldfail]") {
  Context ctx;
  std::string uri("sc-53791-uint64-var");

  if (!array_exists(ctx, uri.c_str())) {
    create_array(ctx, uri.c_str());
    write_array(ctx, uri.c_str());
  }

  /*
   * See the FIXME above. `submit` throws an exception
   * due to missing data but we expect it to return normally.
   *
   * When the bug is fixed, delete `CHECK_NOTHROW` and `if (false)`.
   */
  CHECK_NOTHROW(query_null_count(ctx, uri.c_str()));
  if (false) {
    const auto counts = query_null_count(ctx, uri.c_str());
    REQUIRE(counts.first == 1);
    REQUIRE(counts.second == 0);
  }
}
