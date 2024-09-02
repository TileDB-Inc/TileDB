/**
 * @file tiledb/sm/array_schema/test/unit_tile_domain.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Tests for class TileDomain.
 */

#include <test/support/tdb_catch.h>
#include "src/mem_helpers.h"
#include "tiledb/sm/array_schema/tile_domain.h"

using namespace tiledb::sm;

TEST_CASE("TileDomain: Test 1D", "[TileDomain][1d]") {
  int32_t tile_extent_v = 10;
  auto memory_tracker = tiledb::test::create_test_memory_tracker();
  std::vector<ByteVecValue> tile_extents(1);
  tile_extents[0].assign_as<int32_t>(tile_extent_v);
  Layout layout = Layout::ROW_MAJOR;

  auto size = 2 * sizeof(int32_t);
  int32_t ds_vec[] = {15, 35};
  int32_t domain_vec[] = {1, 100};
  NDRange ds = {Range(ds_vec, size)};
  NDRange domain = {Range(domain_vec, size)};

  TileDomain<int32_t> tile_domain(0, domain, ds, tile_extents, layout);
  const auto& td = tile_domain.tile_domain();
  CHECK(td.size() == 2);
  CHECK(td[0] == 1);
  CHECK(td[1] == 3);

  int32_t tile_coords[] = {2};
  auto tile_pos = tile_domain.tile_pos(tile_coords);
  CHECK(tile_pos == 1);

  tile_coords[0] = 0;
  tile_pos = tile_domain.tile_pos(tile_coords);
  CHECK(tile_pos == UINT64_MAX);

  tile_coords[0] = 1;
  auto start_coords = tile_domain.start_coords(tile_coords);
  CHECK(start_coords == std::vector<int32_t>({11}));
}

TEST_CASE(
    "TileDomain: Test 2D, row-major, complete",
    "[TileDomain][2d][row][complete]") {
  std::vector<int32_t> domain_vec = {1, 10, 1, 10};
  std::vector<int32_t> domain_slice = {1, 10, 1, 10};
  std::vector<int32_t> tile_extents_vec = {2, 5};
  auto memory_tracker = tiledb::test::create_test_memory_tracker();
  std::vector<ByteVecValue> tile_extents(2);
  tile_extents[0].assign_as<int32_t>(tile_extents_vec[0]);
  tile_extents[1].assign_as<int32_t>(tile_extents_vec[1]);
  Layout layout = Layout::ROW_MAJOR;

  auto size = 2 * (sizeof(int32_t));
  NDRange ds = {Range(&domain_slice[0], size), Range(&domain_slice[2], size)};
  NDRange domain = {Range(&domain_vec[0], size), Range(&domain_vec[2], size)};

  TileDomain<int32_t> tile_domain(0, domain, ds, tile_extents, layout);
  const auto& td = tile_domain.tile_domain();
  CHECK(td.size() == 4);
  CHECK(td[0] == 0);
  CHECK(td[1] == 4);
  CHECK(td[2] == 0);
  CHECK(td[3] == 1);

  int32_t tile_coords[] = {0, 0};
  auto tile_pos = tile_domain.tile_pos(tile_coords);
  CHECK(tile_pos == 0);

  tile_coords[0] = 3;
  tile_coords[1] = 0;
  tile_pos = tile_domain.tile_pos(tile_coords);
  CHECK(tile_pos == 6);

  tile_coords[0] = 5;
  tile_coords[1] = 1;
  tile_pos = tile_domain.tile_pos(tile_coords);
  CHECK(tile_pos == UINT64_MAX);

  tile_coords[0] = 1;
  tile_coords[1] = 1;
  auto start_coords = tile_domain.start_coords(tile_coords);
  CHECK(start_coords == std::vector<int32_t>({3, 6}));
}

TEST_CASE(
    "TileDomain: Test 2D, row-major, partial",
    "[TileDomain][2d][row][partial]") {
  std::vector<int32_t> domain_vec = {1, 10, 1, 10};
  std::vector<int32_t> domain_slice = {4, 10, 2, 8};
  std::vector<int32_t> tile_extents_vec = {2, 5};
  auto memory_tracker = tiledb::test::create_test_memory_tracker();
  std::vector<ByteVecValue> tile_extents(2);
  tile_extents[0].assign_as<int32_t>(tile_extents_vec[0]);
  tile_extents[1].assign_as<int32_t>(tile_extents_vec[1]);
  Layout layout = Layout::ROW_MAJOR;

  auto size = 2 * (sizeof(int32_t));
  NDRange ds = {Range(&domain_slice[0], size), Range(&domain_slice[2], size)};
  NDRange domain = {Range(&domain_vec[0], size), Range(&domain_vec[2], size)};

  TileDomain<int32_t> tile_domain(0, domain, ds, tile_extents, layout);
  const auto& td = tile_domain.tile_domain();
  CHECK(td.size() == 4);
  CHECK(td[0] == 1);
  CHECK(td[1] == 4);
  CHECK(td[2] == 0);
  CHECK(td[3] == 1);

  int32_t tile_coords[] = {0, 0};
  auto tile_pos = tile_domain.tile_pos(tile_coords);
  CHECK(tile_pos == UINT64_MAX);

  tile_coords[0] = 3;
  tile_coords[1] = 1;
  tile_pos = tile_domain.tile_pos(tile_coords);
  CHECK(tile_pos == 5);

  tile_coords[0] = 4;
  tile_coords[1] = 0;
  tile_pos = tile_domain.tile_pos(tile_coords);
  CHECK(tile_pos == 6);
}

TEST_CASE(
    "TileDomain: Test 2D, col-major, complete",
    "[TileDomain][2d][col][complete]") {
  std::vector<int32_t> domain_vec = {1, 10, 1, 10};
  std::vector<int32_t> domain_slice = {1, 10, 1, 10};
  std::vector<int32_t> tile_extents_vec = {2, 5};
  auto memory_tracker = tiledb::test::create_test_memory_tracker();
  std::vector<ByteVecValue> tile_extents(2);
  tile_extents[0].assign_as<int32_t>(tile_extents_vec[0]);
  tile_extents[1].assign_as<int32_t>(tile_extents_vec[1]);
  Layout layout = Layout::COL_MAJOR;

  auto size = 2 * (sizeof(int32_t));
  NDRange ds = {Range(&domain_slice[0], size), Range(&domain_slice[2], size)};
  NDRange domain = {Range(&domain_vec[0], size), Range(&domain_vec[2], size)};

  TileDomain<int32_t> tile_domain(0, domain, ds, tile_extents, layout);
  const auto& td = tile_domain.tile_domain();
  CHECK(td.size() == 4);
  CHECK(td[0] == 0);
  CHECK(td[1] == 4);
  CHECK(td[2] == 0);
  CHECK(td[3] == 1);

  int32_t tile_coords[] = {0, 0};
  auto tile_pos = tile_domain.tile_pos(tile_coords);
  CHECK(tile_pos == 0);

  tile_coords[0] = 3;
  tile_coords[1] = 0;
  tile_pos = tile_domain.tile_pos(tile_coords);
  CHECK(tile_pos == 3);

  tile_coords[0] = 5;
  tile_coords[1] = 1;
  tile_pos = tile_domain.tile_pos(tile_coords);
  CHECK(tile_pos == UINT64_MAX);
}

TEST_CASE(
    "TileDomain: Test 2D, col-major, partial",
    "[TileDomain][2d][col][partial]") {
  std::vector<int32_t> domain_vec = {1, 10, 1, 10};
  std::vector<int32_t> domain_slice = {4, 10, 2, 8};
  std::vector<int32_t> tile_extents_vec = {2, 5};
  auto memory_tracker = tiledb::test::create_test_memory_tracker();
  std::vector<ByteVecValue> tile_extents(2);
  tile_extents[0].assign_as<int32_t>(tile_extents_vec[0]);
  tile_extents[1].assign_as<int32_t>(tile_extents_vec[1]);
  Layout layout = Layout::COL_MAJOR;

  auto size = 2 * (sizeof(int32_t));
  NDRange ds = {Range(&domain_slice[0], size), Range(&domain_slice[2], size)};
  NDRange domain = {Range(&domain_vec[0], size), Range(&domain_vec[2], size)};

  TileDomain<int32_t> tile_domain(0, domain, ds, tile_extents, layout);
  const auto& td = tile_domain.tile_domain();
  CHECK(td.size() == 4);
  CHECK(td[0] == 1);
  CHECK(td[1] == 4);
  CHECK(td[2] == 0);
  CHECK(td[3] == 1);

  int32_t tile_coords[] = {0, 0};
  auto tile_pos = tile_domain.tile_pos(tile_coords);
  CHECK(tile_pos == UINT64_MAX);

  tile_coords[0] = 3;
  tile_coords[1] = 1;
  tile_pos = tile_domain.tile_pos(tile_coords);
  CHECK(tile_pos == 6);

  tile_coords[0] = 4;
  tile_coords[1] = 0;
  tile_pos = tile_domain.tile_pos(tile_coords);
  CHECK(tile_pos == 3);
}

TEST_CASE(
    "TileDomain: Test 2D, tile subarray", "[TileDomain][2d][tile_subarray]") {
  std::vector<int32_t> domain_vec = {1, 10, 11, 20};
  std::vector<int32_t> domain_slice = {4, 10, 12, 18};
  std::vector<int32_t> tile_extents_vec = {2, 5};
  auto memory_tracker = tiledb::test::create_test_memory_tracker();
  std::vector<ByteVecValue> tile_extents(2);
  tile_extents[0].assign_as<int32_t>(tile_extents_vec[0]);
  tile_extents[1].assign_as<int32_t>(tile_extents_vec[1]);
  Layout layout = Layout::COL_MAJOR;

  auto size = 2 * (sizeof(int32_t));
  NDRange ds = {Range(&domain_slice[0], size), Range(&domain_slice[2], size)};
  NDRange domain = {Range(&domain_vec[0], size), Range(&domain_vec[2], size)};

  TileDomain<int32_t> tile_domain(0, domain, ds, tile_extents, layout);

  int32_t tile_coords[] = {0, 0};
  auto tile_subarray = tile_domain.tile_subarray(tile_coords);
  CHECK(tile_subarray[0] == 1);
  CHECK(tile_subarray[1] == 2);
  CHECK(tile_subarray[2] == 11);
  CHECK(tile_subarray[3] == 15);

  tile_coords[0] = 1;
  tile_coords[1] = 1;
  tile_subarray = tile_domain.tile_subarray(tile_coords);
  CHECK(tile_subarray[0] == 3);
  CHECK(tile_subarray[1] == 4);
  CHECK(tile_subarray[2] == 16);
  CHECK(tile_subarray[3] == 20);
}

TEST_CASE(
    "TileDomain: Test 2D, tile overlap", "[TileDomain][2d][tile_overlap]") {
  std::vector<int32_t> domain_vec = {1, 10, 11, 20};
  std::vector<int32_t> domain_slice = {2, 10, 12, 18};
  std::vector<int32_t> tile_extents_vec = {2, 5};
  auto memory_tracker = tiledb::test::create_test_memory_tracker();
  std::vector<ByteVecValue> tile_extents(2);
  tile_extents[0].assign_as<int32_t>(tile_extents_vec[0]);
  tile_extents[1].assign_as<int32_t>(tile_extents_vec[1]);
  Layout layout = Layout::COL_MAJOR;

  auto size = 2 * (sizeof(int32_t));
  NDRange ds = {Range(&domain_slice[0], size), Range(&domain_slice[2], size)};
  NDRange domain = {Range(&domain_vec[0], size), Range(&domain_vec[2], size)};

  TileDomain<int32_t> tile_domain(0, domain, ds, tile_extents, layout);

  int32_t tile_coords[] = {0, 0};
  auto tile_overlap = tile_domain.tile_overlap(tile_coords);
  CHECK(tile_overlap[0] == 2);
  CHECK(tile_overlap[1] == 2);
  CHECK(tile_overlap[2] == 12);
  CHECK(tile_overlap[3] == 15);

  tile_coords[0] = 1;
  tile_coords[1] = 1;
  tile_overlap = tile_domain.tile_overlap(tile_coords);
  CHECK(tile_overlap[0] == 3);
  CHECK(tile_overlap[1] == 4);
  CHECK(tile_overlap[2] == 16);
  CHECK(tile_overlap[3] == 18);

  tile_coords[0] = 10;
  tile_coords[1] = 1;
  tile_overlap = tile_domain.tile_overlap(tile_coords);
  CHECK(tile_overlap.empty());
}

TEST_CASE(
    "TileDomain: Test 2D, in tile domain", "[TileDomain][2d][in_tile_domain]") {
  std::vector<int32_t> domain_vec = {1, 10, 11, 20};
  std::vector<int32_t> domain_slice = {2, 10, 12, 18};
  std::vector<int32_t> tile_extents_vec = {2, 5};
  auto memory_tracker = tiledb::test::create_test_memory_tracker();
  std::vector<ByteVecValue> tile_extents(2);
  tile_extents[0].assign_as<int32_t>(tile_extents_vec[0]);
  tile_extents[1].assign_as<int32_t>(tile_extents_vec[1]);
  Layout layout = Layout::COL_MAJOR;

  auto size = 2 * (sizeof(int32_t));
  NDRange ds = {Range(&domain_slice[0], size), Range(&domain_slice[2], size)};
  NDRange domain = {Range(&domain_vec[0], size), Range(&domain_vec[2], size)};

  TileDomain<int32_t> tile_domain(0, domain, ds, tile_extents, layout);

  int32_t tile_coords[] = {0, 0};
  CHECK(tile_domain.in_tile_domain(tile_coords));
  tile_coords[0] = 1;
  tile_coords[1] = 1;
  CHECK(tile_domain.in_tile_domain(tile_coords));
  tile_coords[0] = 10;
  tile_coords[1] = 1;
  CHECK(!tile_domain.in_tile_domain(tile_coords));
}

TEST_CASE("TileDomain: Test 2D, covers", "[TileDomain][2d][covers]") {
  std::vector<int32_t> domain_vec = {1, 10, 1, 10};
  std::vector<int32_t> domain_slice_1 = {2, 6, 2, 8};
  std::vector<int32_t> domain_slice_2 = {3, 6, 1, 7};
  std::vector<int32_t> tile_extents_vec = {2, 5};
  auto memory_tracker = tiledb::test::create_test_memory_tracker();
  std::vector<ByteVecValue> tile_extents(2);
  tile_extents[0].assign_as<int32_t>(tile_extents_vec[0]);
  tile_extents[1].assign_as<int32_t>(tile_extents_vec[1]);
  Layout layout = Layout::COL_MAJOR;

  auto size = 2 * (sizeof(int32_t));
  NDRange domain = {Range(&domain_vec[0], size), Range(&domain_vec[2], size)};
  NDRange ds1 = {
      Range(&domain_slice_1[0], size), Range(&domain_slice_1[2], size)};
  NDRange ds2 = {
      Range(&domain_slice_2[0], size), Range(&domain_slice_2[2], size)};

  TileDomain<int32_t> tile_domain_1(1, domain, ds1, tile_extents, layout);

  TileDomain<int32_t> tile_domain_2(2, domain, ds2, tile_extents, layout);

  int32_t tile_coords[] = {0, 0};
  CHECK(!tile_domain_1.covers(tile_coords, tile_domain_2));
  CHECK(!tile_domain_2.covers(tile_coords, tile_domain_1));

  tile_coords[0] = 1;
  tile_coords[1] = 0;
  CHECK(!tile_domain_1.covers(tile_coords, tile_domain_2));
  CHECK(tile_domain_2.covers(tile_coords, tile_domain_1));

  tile_coords[0] = 2;
  tile_coords[1] = 0;
  CHECK(!tile_domain_1.covers(tile_coords, tile_domain_2));
  CHECK(tile_domain_2.covers(tile_coords, tile_domain_1));

  tile_coords[0] = 2;
  tile_coords[1] = 1;
  CHECK(tile_domain_1.covers(tile_coords, tile_domain_2));
  CHECK(!tile_domain_2.covers(tile_coords, tile_domain_1));
}
