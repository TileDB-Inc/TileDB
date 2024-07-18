/**
 * @file tiledb/sm/array_schema/test/unit_array_schema_operations.cc
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
 * This file contains unit tests for the array schema operations.
 */

#include <tdb_catch.h>

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/array_schema_operations.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/storage_manager/context.h"

using namespace tiledb::common;
using namespace tiledb::sm;

TEST_CASE(
    "Array schema operations: store array schema",
    "[!mayfail][array_schema_operations][store_array_schema]") {
  Config config;
  Context ctx(config);
  auto schema = make_shared<ArraySchema>(
      HERE(), ArrayType::DENSE, ctx.resources().ephemeral_memory_tracker());
  EncryptionKey key;

  // Note: This should fail because no URI has been set on the schema.
  CHECK_THROWS(store_array_schema(ctx.resources(), schema, key));
}
