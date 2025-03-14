/**
 * @file   unit-capi-filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
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
 * Tests the C API filter.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::test;

TEST_CASE("C API: Test filter list on attribute", "[capi][filter-list]") {
  tiledb_ctx_t* const ctx = vanilla_context_c();

  tiledb_filter_t* filter;
  auto rc = tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  int level = 5;
  rc = tiledb_filter_set_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx, &filter_list);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  rc = tiledb_filter_list_add_filter(ctx, filter_list, filter);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  rc = tiledb_filter_list_set_max_chunk_size(ctx, filter_list, 1024);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &attr);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  rc = tiledb_attribute_set_filter_list(ctx, attr, filter_list);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  tiledb_filter_list_t* filter_list_out;
  rc = tiledb_attribute_get_filter_list(ctx, attr, &filter_list_out);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  unsigned nfilters;
  rc = tiledb_filter_list_get_nfilters(ctx, filter_list_out, &nfilters);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(nfilters == 1);

  tiledb_filter_t* filter_out;
  rc = tiledb_filter_list_get_filter_from_index(
      ctx, filter_list_out, 0, &filter_out);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(filter_out != nullptr);

  level = 0;
  rc = tiledb_filter_get_option(
      ctx, filter_out, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(level == 5);

  uint32_t max_chunk_size;
  rc = tiledb_filter_list_get_max_chunk_size(
      ctx, filter_list_out, &max_chunk_size);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(max_chunk_size == 1024);

  tiledb_filter_free(&filter_out);
  tiledb_filter_list_free(&filter_list_out);

  // Clean up
  tiledb_attribute_free(&attr);
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&filter_list);
}
