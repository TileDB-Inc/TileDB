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

#include <test/support/tdb_catch.h>
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
  std::vector<shared_ptr<Tile>> metadata_tiles;

  Metadata metadata_to_serialize1, metadata_to_serialize2,
      metadata_to_serialize3;

  // key_1:a, value_1:100,200
  std::string key_1 = "key1";
  std::vector<int> value_1_vector{100, 200};

  // key_2:key_2, value_2:1.1(double)
  std::string key_2 = "key2";
  uint32_t value_2_size = 1;
  double value_2 = 1.0;

  // key_3:key_3, value_3:strmetadata
  std::string key_3 = "key3";
  std::string value_3 = "strmetadata";
  uint32_t value_3_size = static_cast<uint32_t>(value_3.size());

  CHECK(metadata_to_serialize1
            .put(key_1.c_str(), Datatype::INT32, 2, value_1_vector.data())
            .ok());

  SizeComputationSerializer size_computation_serializer1;
  metadata_to_serialize1.serialize(size_computation_serializer1);
  Tile tile1{Tile::from_generic(size_computation_serializer1.size())};

  Serializer serializer1(tile1.data(), tile1.size());
  metadata_to_serialize1.serialize(serializer1);

  CHECK(
      metadata_to_serialize2.put(key_2.c_str(), Datatype::FLOAT64, 1, &value_2)
          .ok());

  SizeComputationSerializer size_computation_serializer2;
  metadata_to_serialize2.serialize(size_computation_serializer2);
  Tile tile2{Tile::from_generic(size_computation_serializer2.size())};

  Serializer serializer2(tile2.data(), tile2.size());
  metadata_to_serialize2.serialize(serializer2);

  CHECK(metadata_to_serialize3
            .put(
                key_3.c_str(),
                Datatype::STRING_ASCII,
                value_3_size,
                value_3.data())
            .ok());

  SizeComputationSerializer size_computation_serializer3;
  metadata_to_serialize3.serialize(size_computation_serializer3);
  Tile tile3{Tile::from_generic(size_computation_serializer3.size())};

  Serializer serializer3(tile3.data(), tile3.size());
  metadata_to_serialize3.serialize(serializer3);

  metadata_tiles.resize(3);
  metadata_tiles[0] = tdb::make_shared<Tile>(HERE(), std::move(tile1));
  metadata_tiles[1] = tdb::make_shared<Tile>(HERE(), std::move(tile2));
  metadata_tiles[2] = tdb::make_shared<Tile>(HERE(), std::move(tile3));

  auto meta{Metadata::deserialize(metadata_tiles)};

  Datatype type;
  uint32_t v_num;

  // Read key_1 metadata
  const int32_t* v_1;
  CHECK(meta.get("key1", &type, &v_num, (const void**)(&v_1)).ok());
  CHECK(type == Datatype::INT32);
  CHECK(v_num == (uint32_t)(value_1_vector.size()));
  CHECK(*(v_1) == 100);
  CHECK(*(v_1 + 1) == 200);

  // Read key_2 metadata
  const double* v_2;
  CHECK(meta.get("key2", &type, &v_num, (const void**)(&v_2)).ok());
  CHECK(type == Datatype::FLOAT64);
  CHECK(v_num == value_2_size);
  CHECK(*(v_2) == value_2);

  // Read key_3 metadata
  const char* v_3;
  CHECK(meta.get("key3", &type, &v_num, (const void**)(&v_3)).ok());
  CHECK(type == Datatype::STRING_ASCII);
  CHECK(v_num == value_3_size);
  CHECK(std::string(v_3, value_3_size) == value_3);
}
