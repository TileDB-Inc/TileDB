/**
 * @file tiledb/sm/metadata/test/unit_metadata.cc
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
 * This file defines a test `main()`
 */

#include <catch.hpp>
#include "../metadata.h"
#include "tiledb/common/common.h"
#include "tiledb/common/dynamic_memory/dynamic_memory.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/misc/uuid.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

template <class T, int n>
inline T& buffer_metadata(void* p) {
  return *static_cast<T*>(static_cast<void*>(static_cast<char*>(p) + n));
}

TEST_CASE(
    "Metadata: Test metadata deserialization", "[metadata][deserialization]") {
  std::vector<shared_ptr<Buffer>> metadata_buffs;

  // key_1:a, value_1:100,200
  std::string key_1 = "key1";
  auto key_1_size = static_cast<uint32_t>(key_1.size());
  std::vector<int> value_1_vector{100, 200};

  // key_2:key_2, value_2:1.1(double)
  std::string key_2 = "key2";
  uint32_t key_2_size = static_cast<uint32_t>(key_2.size());
  uint32_t value_2_size = 1;
  double value_2 = 1.0;

  // key_3:key_3, value_3:strmetadata
  std::string key_3 = "key3";
  uint32_t key_3_size = static_cast<uint32_t>(key_2.size());
  std::string value_3 = "strmetadata";
  uint32_t value_3_size = static_cast<uint32_t>(value_3.size());

  char serialized_buffer_1[22];
  char* p_1 = &serialized_buffer_1[0];
  // set key_1:value_1 integer metadata
  buffer_metadata<uint32_t, 0>(p_1) = static_cast<uint32_t>(key_1.size());
  std::memcpy(&buffer_metadata<char, 4>(p_1), key_1.c_str(), key_1_size);
  buffer_metadata<char, 8>(p_1) = 0;
  buffer_metadata<char, 9>(p_1) = static_cast<char>(Datatype::INT32);
  buffer_metadata<uint32_t, 10>(p_1) = (uint32_t)value_1_vector.size();
  buffer_metadata<int32_t, 14>(p_1) = value_1_vector[0];
  buffer_metadata<int32_t, 18>(p_1) = value_1_vector[1];
  metadata_buffs.push_back(make_shared<Buffer>(
      HERE(), &serialized_buffer_1, sizeof(serialized_buffer_1)));

  char serialized_buffer_2[22];
  char* p_2 = &serialized_buffer_2[0];
  // set key_2:value_2 double metadata
  buffer_metadata<uint32_t, 0>(p_2) = static_cast<uint32_t>(key_2.size());
  std::memcpy(&buffer_metadata<char, 4>(p_2), key_2.c_str(), key_2_size);
  buffer_metadata<char, 8>(p_2) = 0;
  buffer_metadata<char, 9>(p_2) = (char)Datatype::FLOAT64;
  buffer_metadata<uint32_t, 10>(p_2) = value_2_size;
  buffer_metadata<double, 14>(p_2) = value_2;
  metadata_buffs.push_back(make_shared<Buffer>(
      HERE(), &serialized_buffer_2, sizeof(serialized_buffer_2)));

  char serialized_buffer_3[25];
  char* p_3 = &serialized_buffer_3[0];
  // set key_3:value_3 string metadata
  buffer_metadata<uint32_t, 0>(p_3) = static_cast<uint32_t>(key_3.size());
  std::memcpy(&buffer_metadata<char, 4>(p_3), key_3.c_str(), key_3_size);
  buffer_metadata<char, 8>(p_3) = 0;
  buffer_metadata<char, 9>(p_3) = (char)Datatype::STRING_ASCII;
  buffer_metadata<uint32_t, 10>(p_3) = value_3_size;
  std::memcpy(&buffer_metadata<char, 14>(p_3), value_3.c_str(), value_3_size);
  metadata_buffs.push_back(make_shared<Buffer>(
      HERE(), &serialized_buffer_3, sizeof(serialized_buffer_3)));

  auto&& [st_meta, meta]{Metadata::deserialize(metadata_buffs)};

  REQUIRE(st_meta.ok());

  Datatype type;
  uint32_t v_num;

  // Read key_1 metadata
  const int32_t* v_1;
  meta.value()->get("key1", &type, &v_num, (const void**)(&v_1));
  CHECK(type == Datatype::INT32);
  CHECK(v_num == (uint32_t)(value_1_vector.size()));
  CHECK(*(v_1) == 100);
  CHECK(*(v_1 + 1) == 200);

  // Read key_2 metadata
  const double* v_2;
  meta.value()->get("key2", &type, &v_num, (const void**)(&v_2));
  CHECK(type == Datatype::FLOAT64);
  CHECK(v_num == value_2_size);
  CHECK(*(v_2) == value_2);

  // Read key_3 metadata
  const char* v_3;
  meta.value()->get("key3", &type, &v_num, (const void**)(&v_3));
  CHECK(type == Datatype::STRING_ASCII);
  CHECK(v_num == value_3_size);
  CHECK(std::string(v_3, value_3_size) == value_3);
}
