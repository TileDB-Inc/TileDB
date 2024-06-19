/**
 * @file test-cppapi-current-domain.cc
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
 *
 * @section DESCRIPTION
 *
 * Tests the currentDomain C++ API
 */

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

#include <test/support/tdb_catch.h>

using namespace tiledb::test;

TEST_CASE_METHOD(
    TemporaryDirectoryFixture,
    "C++ API: CurrentDomain - Basic",
    "[cppapi][ArraySchema][currentDomain]") {
  // Create the C++ context.
  tiledb::Context ctx_{ctx, false};

  // Create domain
  tiledb::Domain domain(ctx_);
  auto d1 = tiledb::Dimension::create<int32_t>(ctx_, "x", {{0, 100}}, 10);
  auto d2 = tiledb::Dimension::create<int32_t>(ctx_, "y", {{0, 100}}, 10);
  domain.add_dimension(d1);
  domain.add_dimension(d2);

  // Create NDRectangle
  tiledb::NDRectangle ndrect(ctx_, domain);

  // add ranges
  int range_one[] = {10, 20};
  ndrect.set_range(0, range_one[0], range_one[1]);

  int range_two[] = {30, 40};
  ndrect.set_range(1, range_two[0], range_two[1]);

  // Get ranges
  auto range = ndrect.range<int>(0);
  CHECK(range[0] == 10);
  CHECK(range[1] == 20);
  range = ndrect.range<int>(1);
  CHECK(range[0] == 30);
  CHECK(range[1] == 40);

  // Create a currentDomain
  tiledb::CurrentDomain current_domain(ctx_);

  // set the NDRectangle
  current_domain.set_ndrectangle(ndrect);

  CHECK_FALSE(current_domain.is_empty());

  auto rect = current_domain.ndrectangle();

  // Check ranges
  range = rect.range<int>(0);
  CHECK(range[0] == 10);
  CHECK(range[1] == 20);
  range = rect.range<int>(1);
  CHECK(range[0] == 30);
  CHECK(range[1] == 40);
}

TEST_CASE_METHOD(
    TemporaryDirectoryFixture,
    "C++ API: CurrentDomain - Add to ArraySchema",
    "[cppapi][ArraySchema][currentDomain]") {
  tiledb::Context ctx;
  // Create domain.
  tiledb::Domain domain(ctx);
  auto d = tiledb::Dimension::create<int32_t>(ctx, "d", {{1, 999}}, 2);
  domain.add_dimension(d);

  // Create array schema.
  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(tiledb::Attribute::create<int>(ctx, "a"));

  // Create NDRectangle
  tiledb::NDRectangle ndrect(ctx, domain);

  // Set ranges
  int range_one[] = {10, 20};
  ndrect.set_range(0, range_one[0], range_one[1]);

  // Create the currentDomain
  tiledb::CurrentDomain current_domain(ctx);

  // Set the NDrectangle
  current_domain.set_ndrectangle(ndrect);

  CHECK_NOTHROW(tiledb::ArraySchemaExperimental::set_current_domain(
      ctx, schema, current_domain));

  auto cd = tiledb::ArraySchemaExperimental::current_domain(ctx, schema);

  CHECK(!cd.is_empty());

  // Check if ranges are the same
  CHECK(cd.ndrectangle().range<int>(0) == ndrect.range<int>(0));

  CHECK_THROWS(cd.ndrectangle().range<int>(1));
}
