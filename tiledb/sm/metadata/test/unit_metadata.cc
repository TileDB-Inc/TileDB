/**
 * @file tiledb/sm/metadata/test/unit_metadata.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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

#include <test/support/src/mem_helpers.h>
#include <test/support/tdb_catch.h>
#include "../metadata.h"
#include "test/support/src/mem_helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/common/dynamic_memory/dynamic_memory.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/tdb_time.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;
using tiledb::test::create_test_memory_tracker;

template <class T, int n>
inline T& buffer_metadata(void* p) {
  return *static_cast<T*>(static_cast<void*>(static_cast<char*>(p) + n));
}

TEST_CASE("Metadata: Constructor validation", "[metadata][constructor]") {
  auto tracker = create_test_memory_tracker();

  SECTION("memory_tracker") {
    REQUIRE_NOTHROW(Metadata(tracker));
  }
}

TEST_CASE(
    "Metadata: Test metadata deserialization", "[metadata][deserialization]") {
  auto tracker = create_test_memory_tracker();
  std::vector<shared_ptr<Tile>> metadata_tiles;

  Metadata metadata_to_serialize1(tracker), metadata_to_serialize2(tracker),
      metadata_to_serialize3(tracker), meta(tracker);

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

  metadata_to_serialize1.put(
      key_1.c_str(), Datatype::INT32, 2, value_1_vector.data());

  SizeComputationSerializer size_computation_serializer1;
  metadata_to_serialize1.serialize(size_computation_serializer1);
  auto tile1{
      WriterTile::from_generic(size_computation_serializer1.size(), tracker)};

  Serializer serializer1(tile1->data(), tile1->size());
  metadata_to_serialize1.serialize(serializer1);

  metadata_to_serialize2.put(key_2.c_str(), Datatype::FLOAT64, 1, &value_2);

  SizeComputationSerializer size_computation_serializer2;
  metadata_to_serialize2.serialize(size_computation_serializer2);
  auto tile2{
      WriterTile::from_generic(size_computation_serializer2.size(), tracker)};

  Serializer serializer2(tile2->data(), tile2->size());
  metadata_to_serialize2.serialize(serializer2);

  metadata_to_serialize3.put(
      key_3.c_str(), Datatype::STRING_ASCII, value_3_size, value_3.data());

  SizeComputationSerializer size_computation_serializer3;
  metadata_to_serialize3.serialize(size_computation_serializer3);
  auto tile3{
      WriterTile::from_generic(size_computation_serializer3.size(), tracker)};

  Serializer serializer3(tile3->data(), tile3->size());
  metadata_to_serialize3.serialize(serializer3);

  metadata_tiles.resize(3);
  metadata_tiles[0] = tdb::make_shared<Tile>(
      HERE(),
      tile1->format_version(),
      tile1->type(),
      tile1->cell_size(),
      0,
      tile1->size(),
      tile1->filtered_buffer().data(),
      tile1->filtered_buffer().size(),
      tracker);
  memcpy(metadata_tiles[0]->data(), tile1->data(), tile1->size());

  metadata_tiles[1] = tdb::make_shared<Tile>(
      HERE(),
      tile2->format_version(),
      tile2->type(),
      tile2->cell_size(),
      0,
      tile2->size(),
      tile2->filtered_buffer().data(),
      tile2->filtered_buffer().size(),
      tracker);
  memcpy(metadata_tiles[1]->data(), tile2->data(), tile2->size());

  metadata_tiles[2] = tdb::make_shared<Tile>(
      HERE(),
      tile3->format_version(),
      tile3->type(),
      tile3->cell_size(),
      0,
      tile3->size(),
      tile3->filtered_buffer().data(),
      tile3->filtered_buffer().size(),
      tracker);
  memcpy(metadata_tiles[2]->data(), tile3->data(), tile3->size());

  meta = Metadata::deserialize(metadata_tiles);
  Datatype type;
  uint32_t v_num;

  // Read key_1 metadata
  const int32_t* v_1;
  meta.get("key1", &type, &v_num, (const void**)(&v_1));
  CHECK(type == Datatype::INT32);
  CHECK(v_num == (uint32_t)(value_1_vector.size()));
  CHECK(*(v_1) == 100);
  CHECK(*(v_1 + 1) == 200);

  // Read key_2 metadata
  const double* v_2;
  meta.get("key2", &type, &v_num, (const void**)(&v_2));
  CHECK(type == Datatype::FLOAT64);
  CHECK(v_num == value_2_size);
  CHECK(*(v_2) == value_2);

  // Read key_3 metadata
  const char* v_3;
  meta.get("key3", &type, &v_num, (const void**)(&v_3));
  CHECK(type == Datatype::STRING_ASCII);
  CHECK(v_num == value_3_size);
  CHECK(std::string(v_3, value_3_size) == value_3);
}
