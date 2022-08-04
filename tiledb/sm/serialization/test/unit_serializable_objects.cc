/**
 * @file unit_serializable_objects.cc
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
 * Tests the serializable object classes and also give a visual representation
 * of what gets serialized.
 */

#define TILEDB_SERIALIZATION

#include "tiledb/common/common.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/serialization/serializable_subarray.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/subarray/subarray.h"

#include <test/support/tdb_catch.h>

using namespace tiledb::sm;

TEST_CASE(
    "SerializableSubarray: Test serialization",
    "[SerializableSubarray][serialization]") {
  // Create a StorageManager.
  Config config;
  stats::Stats stats("");
  auto logger = make_shared<Logger>(HERE(), "foo");
  ContextResources resources(config, logger, 1, 1, "");
  StorageManager sm(resources, make_shared<Logger>(HERE(), ""), config);

  // Open an array.
  static const std::string array_name =
      std::string(TILEDB_TEST_INPUTS_DIR) + "/arrays/non_split_coords_v1_4_0";
  Array array(URI(array_name), &sm);
  REQUIRE(array.open(QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0)
              .ok());

  Subarray subarray(
      &array, &stats, make_shared<Logger>(HERE(), ""), false, &sm);
  auto serializable_subarray = subarray.serializable_subarray();

  // clang-format off
  std::string expected_json =
  "{"
    "\"layout\":\"unordered\","
    "\"ranges\":["
      "{"
        "\"type\":\"INT32\","
        "\"hasDefaultRange\":true,"
        "\"buffer\":[0,0,0,0,16,39,0,0],"
        "\"bufferSizes\":[\"8\"],"
        "\"bufferStartSizes\":[\"0\"]"
      "},"
      "{"
        "\"type\":\"INT32\","
        "\"hasDefaultRange\":true,"
        "\"buffer\":[0,0,0,0,16,39,0,0],"
        "\"bufferSizes\":[\"8\"],"
        "\"bufferStartSizes\":[\"0\"]"
      "}"
    "],"
    "\"stats\":{}"
  "}";

  // clang-format on
  CHECK(expected_json == serializable_subarray.to_json());
}