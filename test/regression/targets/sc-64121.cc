/**
 * @file   sc-64121.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
#include <tiledb/tiledb>

#include <test/support/tdb_catch.h>

using namespace tiledb;

TEST_CASE(
    "SC-64121 open array with unrecognized files",
    "[regression][bug][sc-64121]") {
  Context ctx;

  std::string uri("sc-64121");

  ArraySchema schema(ctx, TILEDB_SPARSE);
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<uint64_t>(ctx, "x", {{1, 100}}, 10));
  schema.set_domain(domain);
  schema.add_attribute(Attribute(ctx, "a", TILEDB_UINT64));
  Array::create(uri, schema);

  VFS vfs(ctx);
  vfs.touch(uri + "/data.bin");

  REQUIRE_NOTHROW(Array(ctx, uri, TILEDB_READ));
}
