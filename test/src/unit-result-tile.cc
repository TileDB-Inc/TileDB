/**
 * @file unit-result-tile.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * Tests for the ResultTile classes.
 */
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"

#include "test/src/helpers.h"
#include "tiledb/sm/query/sparse_index_reader_base.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <catch.hpp>

using namespace tiledb::sm;
using namespace tiledb::test;

/* ********************************* */
/*                TESTS              */
/* ********************************* */

TEST_CASE(
    "ResultTileWithBitmap: result_num_between_pos and "
    "pos_with_given_result_sum test",
    "[resulttilewithbitmap][pos_with_given_result_sum][pos_with_given_result_"
    "sum]") {
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx) == TILEDB_OK);

  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions and domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx, &domain);
  REQUIRE(rc == TILEDB_OK);

  int dim_domain[] = {1, 4};
  int tile_extent = 2;
  tiledb_dimension_t* d;
  rc = tiledb_dimension_alloc(
      ctx, "d", TILEDB_INT32, &dim_domain[0], &tile_extent, &d);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx, domain, d);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_free(&d);

  // Set domain to schema
  rc = tiledb_array_schema_set_domain(ctx, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_domain_free(&domain);

  ResultTileWithBitmap<uint8_t> tile(
      0, 0, *(array_schema->array_schema_.get()));
  tile.bitmap_result_num_ = 100;

  // Check the function with an empty bitmap.
  CHECK(tile.result_num_between_pos(2, 10) == 8);
  CHECK(tile.pos_with_given_result_sum(2, 8) == 9);

  // Check the functions with a bitmap.
  tile.bitmap_.resize(100, 1);
  CHECK(tile.result_num_between_pos(2, 10) == 8);
  CHECK(tile.pos_with_given_result_sum(2, 8) == 9);

  tile.bitmap_result_num_ = 99;
  tile.bitmap_[6] = 0;
  CHECK(tile.result_num_between_pos(2, 10) == 7);
  CHECK(tile.pos_with_given_result_sum(2, 8) == 10);

  tiledb_ctx_free(&ctx);
}