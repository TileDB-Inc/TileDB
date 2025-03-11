/**
 * @file tiledb/sm/storage_manager/test/unit_static_context.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2025 TileDB, Inc.
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
 * This file defines unit tests for the checking StorageManager and GlobalState
 * lifetimes.
 */

#include <test/support/tdb_catch.h>
#if defined(DELETE)
#undef DELETE
#endif
#include <tiledb/sm/array/array.h>
#include <tiledb/sm/array_schema/dimension.h>
#include <tiledb/sm/enums/array_type.h>
#include <tiledb/sm/enums/encryption_type.h>
#include <tiledb/sm/storage_manager/context.h>
#include <tiledb/sm/subarray/subarray.h>

#include <test/support/src/helpers.h>
#include <test/support/src/mem_helpers.h>

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;
using namespace tiledb::type;

TEST_CASE("Static Context", "[context]") {
  // While non-obvious from this implementation, the issue here is that the
  // destructor of the `static std::optional<Context>` is run after the other
  // static instance finalizers for GlobalState and Logger. This ordering ends
  // up causing a segfault when the `Context` attempts to use those resources
  // after they have been destructed.
  //
  // Thus, the actual assertion of this test is that we don't segfault which
  // is reported as an error by Catch2.
  static tiledb::Config cfg;
  static std::optional<tiledb::Context> ctx;
  ctx = std::make_optional(tiledb::Context(cfg));
}
