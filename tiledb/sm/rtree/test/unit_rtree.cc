/**
 * @file unit_rtree.cc
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
 * Tests the `RTree` class.
 */

#include "test/support/src/mem_helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/rtree/rtree.h"

#include <catch2/catch_test_macros.hpp>
#include <iostream>

using namespace tiledb::sm;
using tiledb::test::create_test_memory_tracker;

// `mbrs` contains a flattened vector of values (low, high)
// per dimension per MBR
template <class T, unsigned D>
tdb::pmr::vector<NDRange> create_mbrs(
    const std::vector<T>& mbrs, shared_ptr<MemoryTracker> tracker) {
  assert(mbrs.size() % 2 * D == 0);

  uint64_t mbr_num = (uint64_t)(mbrs.size() / (2 * D));
  tdb::pmr::vector<NDRange> ret(
      mbr_num, tracker->get_resource(MemoryType::RTREE));
  uint64_t r_size = 2 * sizeof(T);
  for (uint64_t m = 0; m < mbr_num; ++m) {
    ret[m].resize(D);
    for (unsigned d = 0; d < D; ++d) {
      ret[m][d] = Range(&mbrs[2 * D * m + 2 * d], r_size);
    }
  }

  return {ret, tracker->get_resource(MemoryType::RTREE)};
}

template <class T1, class T2>
tdb::pmr::vector<NDRange> create_mbrs(
    const std::vector<T1>& r1,
    const std::vector<T2>& r2,
    shared_ptr<MemoryTracker> tracker) {
  assert(r1.size() == r2.size());

  uint64_t mbr_num = (uint64_t)(r1.size() / 2);
  tdb::pmr::vector<NDRange> ret(
      mbr_num, tracker->get_resource(MemoryType::RTREE));
  uint64_t r1_size = 2 * sizeof(T1);
  uint64_t r2_size = 2 * sizeof(T2);
  for (uint64_t m = 0; m < mbr_num; ++m) {
    ret[m].resize(2);
    ret[m][0] = Range(&r1[2 * m], r1_size);
    ret[m][1] = Range(&r2[2 * m], r2_size);
  }

  return {ret, tracker->get_resource(MemoryType::RTREE)};
}

Domain create_domain(
    const std::vector<std::string>& dim_names,
    const std::vector<Datatype>& dim_types,
    const std::vector<const void*>& dim_domains,
    const std::vector<const void*>& dim_tile_extents,
    shared_ptr<MemoryTracker> memory_tracker) {
  assert(!dim_names.empty());
  assert(dim_names.size() == dim_types.size());
  assert(dim_names.size() == dim_domains.size());
  assert(dim_names.size() == dim_tile_extents.size());

  std::vector<shared_ptr<Dimension>> dimensions;
  for (size_t d = 0; d < dim_names.size(); ++d) {
    uint32_t cell_val_num =
        (datatype_is_string(dim_types[d])) ? constants::var_num : 1;
    Range range;
    if (dim_domains[d] != nullptr) {
      range = Range(dim_domains[d], 2 * datatype_size(dim_types[d]));
    }
    ByteVecValue tile_extent;
    if (dim_tile_extents[d] != nullptr) {
      auto tile_extent_size = datatype_size(dim_types[d]);
      tile_extent.resize(tile_extent_size);
      std::memcpy(tile_extent.data(), dim_tile_extents[d], tile_extent_size);
    }
    shared_ptr<Dimension> dim = make_shared<Dimension>(
        HERE(),
        dim_names[d],
        dim_types[d],
        cell_val_num,
        range,
        FilterPipeline(),
        tile_extent,
        tiledb::test::get_test_memory_tracker());
    dimensions.emplace_back(std::move(dim));
  }

  return Domain(
      Layout::ROW_MAJOR, dimensions, Layout::ROW_MAJOR, memory_tracker);
}

TEST_CASE("RTree: Test R-Tree, basic functions", "[rtree][basic]") {
  // Empty tree
  auto tracker = create_test_memory_tracker();
  RTree rtree0(nullptr, 0, tracker);
  CHECK(rtree0.height() == 0);
  CHECK(rtree0.dim_num() == 0);
  CHECK(rtree0.domain() == nullptr);
  CHECK(rtree0.fanout() == 0);

  std::vector<bool> is_default(2, false);

  // 1D
  int32_t dim_dom[] = {1, 1000};
  int32_t dim_extent = 10;
  Domain dom1 = create_domain(
      {"d"}, {Datatype::INT32}, {dim_dom}, {&dim_extent}, tracker);
  const Domain d1 = create_domain(
      {"d"}, {Datatype::INT32}, {dim_dom}, {&dim_extent}, tracker);
  auto mbrs_1d = create_mbrs<int32_t, 1>({1, 3, 5, 10, 20, 22}, tracker);
  RTree rtree1(&d1, 3, tracker);
  CHECK_THROWS(rtree1.set_leaf(0, mbrs_1d[0]));
  rtree1.set_leaf_num(mbrs_1d.size());
  for (size_t m = 0; m < mbrs_1d.size(); ++m)
    rtree1.set_leaf(m, mbrs_1d[m]);
  CHECK_THROWS(rtree1.set_leaf_num(1));
  rtree1.build_tree();
  CHECK_THROWS(rtree1.set_leaf(0, mbrs_1d[0]));
  CHECK(rtree1.height() == 2);
  CHECK(rtree1.dim_num() == 1);
  CHECK(rtree1.subtree_leaf_num(0) == 3);
  CHECK(rtree1.subtree_leaf_num(1) == 1);
  CHECK(rtree1.subtree_leaf_num(2) == 0);
  CHECK(rtree1.leaf(0) == mbrs_1d[0]);
  CHECK(rtree1.leaf(1) == mbrs_1d[1]);
  CHECK(rtree1.leaf(2) == mbrs_1d[2]);

  NDRange range1(1);
  NDRange mbr1(1);
  int32_t mbr1_r[] = {5, 10};
  mbr1[0] = Range(mbr1_r, 2 * sizeof(int32_t));
  int32_t r1_no_left[] = {0, 1};
  int32_t r1_left[] = {4, 7};
  int32_t r1_exact[] = {5, 10};
  int32_t r1_full[] = {4, 11};
  int32_t r1_contained[] = {6, 7};
  int32_t r1_right[] = {7, 11};
  int32_t r1_no_right[] = {11, 15};
  range1[0] = Range(r1_no_left, 2 * sizeof(int32_t));
  double ratio1 = dom1.overlap_ratio(range1, is_default, mbr1);
  CHECK(ratio1 == 0.0);
  range1[0] = Range(r1_left, 2 * sizeof(int32_t));
  ratio1 = dom1.overlap_ratio(range1, is_default, mbr1);
  CHECK(ratio1 == 3.0 / 6);
  range1[0] = Range(r1_exact, 2 * sizeof(int32_t));
  ratio1 = dom1.overlap_ratio(range1, is_default, mbr1);
  CHECK(ratio1 == 1.0);
  range1[0] = Range(r1_full, 2 * sizeof(int32_t));
  ratio1 = dom1.overlap_ratio(range1, is_default, mbr1);
  CHECK(ratio1 == 1.0);
  range1[0] = Range(r1_contained, 2 * sizeof(int32_t));
  ratio1 = dom1.overlap_ratio(range1, is_default, mbr1);
  CHECK(ratio1 == 2.0 / 6);
  range1[0] = Range(r1_right, 2 * sizeof(int32_t));
  ratio1 = dom1.overlap_ratio(range1, is_default, mbr1);
  CHECK(ratio1 == 4.0 / 6);
  range1[0] = Range(r1_no_right, 2 * sizeof(int32_t));
  ratio1 = dom1.overlap_ratio(range1, is_default, mbr1);
  CHECK(ratio1 == 0.0);

  // 2D
  int64_t dim_dom_2[] = {1, 1000};
  int64_t dim_extent_2 = 10;
  Domain dom2 = create_domain(
      {"d1", "d2"},
      {Datatype::INT64, Datatype::INT64},
      {dim_dom_2, dim_dom_2},
      {&dim_extent_2, &dim_extent_2},
      tracker);
  const Domain d2 = create_domain(
      {"d1", "d2"},
      {Datatype::INT64, Datatype::INT64},
      {dim_dom_2, dim_dom_2},
      {&dim_extent_2, &dim_extent_2},
      tracker);
  auto mbrs_2d = create_mbrs<int64_t, 2>(
      {1, 3, 5, 10, 20, 22, 24, 25, 11, 15, 30, 31}, tracker);
  RTree rtree2(&d2, 5, tracker);
  rtree2.set_leaves(mbrs_2d);
  rtree2.build_tree();
  CHECK(rtree2.height() == 2);
  CHECK(rtree2.dim_num() == 2);
  CHECK(rtree2.fanout() == 5);
  CHECK(rtree2.leaf(0) == mbrs_2d[0]);
  CHECK(rtree2.leaf(1) == mbrs_2d[1]);
  CHECK(rtree2.leaf(2) == mbrs_2d[2]);

  NDRange range2(2);
  int64_t mbr2_r[] = {5, 10, 2, 9};
  NDRange mbr2(2);
  mbr2[0] = Range(&mbr2_r[0], 2 * sizeof(int64_t));
  mbr2[1] = Range(&mbr2_r[2], 2 * sizeof(int64_t));
  int64_t r2_no[] = {6, 7, 10, 12};
  int64_t r2_full[] = {4, 11, 2, 9};
  int64_t r2_partial[] = {7, 11, 4, 5};
  range2[0] = Range(&r2_no[0], 2 * sizeof(int64_t));
  range2[1] = Range(&r2_no[2], 2 * sizeof(int64_t));
  double ratio2 = dom2.overlap_ratio(range2, is_default, mbr2);
  CHECK(ratio2 == 0.0);
  range2[0] = Range(&r2_full[0], 2 * sizeof(int64_t));
  range2[1] = Range(&r2_full[2], 2 * sizeof(int64_t));
  ratio2 = dom2.overlap_ratio(range2, is_default, mbr2);
  CHECK(ratio2 == 1.0);
  range2[0] = Range(&r2_partial[0], 2 * sizeof(int64_t));
  range2[1] = Range(&r2_partial[2], 2 * sizeof(int64_t));
  ratio2 = dom2.overlap_ratio(range2, is_default, mbr2);
  CHECK(ratio2 == (4.0 / 6) * (2.0 / 8));

  // Float datatype
  float dim_dom_f[] = {1.0, 1000.0};
  float dim_extent_f = 10.0;
  auto mbrs_f =
      create_mbrs<float, 1>({1.0f, 3.0f, 5.0f, 10.0f, 20.0f, 22.0f}, tracker);
  Domain dom2f = create_domain(
      {"d"}, {Datatype::FLOAT32}, {dim_dom_f}, {&dim_extent_f}, tracker);
  const Domain d2f = create_domain(
      {"d"}, {Datatype::FLOAT32}, {dim_dom_f}, {&dim_extent_f}, tracker);
  RTree rtreef(&d2f, 5, tracker);
  rtreef.set_leaves(mbrs_f);
  rtreef.build_tree();

  NDRange rangef(1);
  float mbrf_r[] = {5.0f, 10.0f};
  NDRange mbrf(1);
  mbrf[0] = Range(mbrf_r, 2 * sizeof(float));
  float rf_no_left[] = {0.0, 1.0};
  float rf_left[] = {4.0, 7.0};
  float rf_exact[] = {5.0, 10.0};
  float rf_full[] = {4.0, 11.0};
  float rf_right[] = {7.0, 11.0};
  float rf_no_right[] = {11.0, 15.0};
  rangef[0] = Range(rf_no_left, 2 * sizeof(float));
  double ratiof = dom2f.overlap_ratio(rangef, is_default, mbrf);
  CHECK(ratiof == 0.0);
  rangef[0] = Range(rf_left, 2 * sizeof(float));
  ratiof = dom2f.overlap_ratio(rangef, is_default, mbrf);
  CHECK(ratiof == 2.0 / 5);
  rangef[0] = Range(rf_exact, 2 * sizeof(float));
  ratiof = dom2f.overlap_ratio(rangef, is_default, mbrf);
  CHECK(ratiof == 1.0);
  rangef[0] = Range(rf_full, 2 * sizeof(float));
  ratiof = dom2f.overlap_ratio(rangef, is_default, mbrf);
  CHECK(ratiof == 1.0);
  rangef[0] = Range(rf_right, 2 * sizeof(float));
  ratiof = dom2f.overlap_ratio(rangef, is_default, mbrf);
  CHECK(ratiof == 3.0 / 5);
  rangef[0] = Range(rf_no_right, 2 * sizeof(float));
  ratiof = dom2f.overlap_ratio(rangef, is_default, mbrf);
  CHECK(ratiof == 0.0);
}

TEST_CASE("RTree: Test 1D R-tree, height 2", "[rtree][1d][2h]") {
  // Build tree
  auto tracker = create_test_memory_tracker();
  std::vector<bool> is_default(1, false);
  int32_t dim_dom[] = {1, 1000};
  int32_t dim_extent = 10;
  Domain dom1 = create_domain(
      {"d"}, {Datatype::INT32}, {dim_dom}, {&dim_extent}, tracker);
  auto mbrs = create_mbrs<int32_t, 1>({1, 3, 5, 10, 20, 22}, tracker);
  RTree rtree(&dom1, 3, tracker);
  rtree.set_leaves(mbrs);
  rtree.build_tree();
  CHECK(rtree.height() == 2);
  CHECK(rtree.dim_num() == 1);
  CHECK(rtree.fanout() == 3);

  // Subtree leaf num
  CHECK(rtree.subtree_leaf_num(0) == 3);
  CHECK(rtree.subtree_leaf_num(1) == 1);
  CHECK(rtree.subtree_leaf_num(2) == 0);

  // Tile overlap
  NDRange range(1);
  int32_t r_no[] = {25, 30};
  int32_t r_full[] = {0, 22};
  int32_t r_partial[] = {6, 21};
  range[0] = Range(r_no, 2 * sizeof(int32_t));
  auto overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());
  range[0] = Range(r_full, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 2);
  range[0] = Range(r_partial, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == 5.0 / 6);
  CHECK(overlap.tiles_[1].first == 2);
  CHECK(overlap.tiles_[1].second == 2.0 / 3);
}

TEST_CASE("RTree: Test 1D R-tree, height 3", "[rtree][1d][3h]") {
  // Build tree
  auto tracker = create_test_memory_tracker();
  std::vector<bool> is_default(1, false);
  int32_t dim_dom[] = {1, 1000};
  int32_t dim_extent = 10;
  auto mbrs = create_mbrs<int32_t, 1>(
      {1, 3, 5, 10, 20, 22, 30, 35, 36, 38, 40, 49, 50, 51, 65, 69}, tracker);
  Domain dom1 = create_domain(
      {"d"}, {Datatype::INT32}, {dim_dom}, {&dim_extent}, tracker);
  RTree rtree(&dom1, 3, tracker);
  rtree.set_leaves(mbrs);
  rtree.build_tree();
  CHECK(rtree.height() == 3);
  CHECK(rtree.dim_num() == 1);
  CHECK(rtree.fanout() == 3);

  // Subtree leaf num
  CHECK(rtree.subtree_leaf_num(0) == 9);
  CHECK(rtree.subtree_leaf_num(1) == 3);
  CHECK(rtree.subtree_leaf_num(2) == 1);
  CHECK(rtree.subtree_leaf_num(3) == 0);

  // Tile overlap
  NDRange range(1);
  int32_t r_no[] = {0, 0};
  int32_t r_full[] = {1, 69};
  int32_t r_only_tiles[] = {10, 20};
  int32_t r_only_ranges[] = {30, 69};
  int32_t r_tiles_and_ranges[] = {1, 32};
  range[0] = Range(r_no, 2 * sizeof(int32_t));
  auto overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());
  range[0] = Range(r_full, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 7);
  range[0] = Range(r_only_tiles, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == 1.0 / 6);
  CHECK(overlap.tiles_[1].first == 2);
  CHECK(overlap.tiles_[1].second == 1.0 / 3);
  range[0] = Range(r_only_ranges, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 2);
  CHECK(overlap.tile_ranges_[0].first == 3);
  CHECK(overlap.tile_ranges_[0].second == 5);
  CHECK(overlap.tile_ranges_[1].first == 6);
  CHECK(overlap.tile_ranges_[1].second == 7);
  range[0] = Range(r_tiles_and_ranges, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 2);
  CHECK(overlap.tiles_.size() == 1);
  CHECK(overlap.tiles_[0].first == 3);
  CHECK(overlap.tiles_[0].second == 3.0 / 6);
}

TEST_CASE("RTree: Test 2D R-tree, height 2", "[rtree][2d][2h]") {
  // Build tree
  auto tracker = create_test_memory_tracker();
  std::vector<bool> is_default(2, false);
  int32_t dim_dom[] = {1, 1000};
  int32_t dim_extent = 10;
  Domain dom1 = create_domain(
      {"d1", "d2"},
      {Datatype::INT32, Datatype::INT32},
      {dim_dom, dim_dom},
      {&dim_extent, &dim_extent},
      tracker);
  auto mbrs = create_mbrs<int32_t, 2>(
      {1, 3, 2, 4, 5, 7, 6, 9, 10, 12, 10, 15}, tracker);
  RTree rtree(&dom1, 3, tracker);
  rtree.set_leaves(mbrs);
  rtree.build_tree();
  CHECK(rtree.height() == 2);
  CHECK(rtree.dim_num() == 2);
  CHECK(rtree.fanout() == 3);

  // Subtree leaf num
  CHECK(rtree.subtree_leaf_num(0) == 3);
  CHECK(rtree.subtree_leaf_num(1) == 1);
  CHECK(rtree.subtree_leaf_num(2) == 0);

  // Tile overlap
  NDRange range(2);
  int32_t r_no[] = {25, 30, 1, 10};
  int32_t r_full[] = {1, 20, 1, 20};
  int32_t r_partial[] = {5, 12, 8, 12};
  range[0] = Range(&r_no[0], 2 * sizeof(int32_t));
  range[1] = Range(&r_no[2], 2 * sizeof(int32_t));
  auto overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());
  range[0] = Range(&r_full[0], 2 * sizeof(int32_t));
  range[1] = Range(&r_full[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 2);
  range[0] = Range(&r_partial[0], 2 * sizeof(int32_t));
  range[1] = Range(&r_partial[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == 2.0 / 4);
  CHECK(overlap.tiles_[1].first == 2);
  CHECK(overlap.tiles_[1].second == 3.0 / 6);
}

TEST_CASE("RTree: Test 2D R-tree, height 3", "[rtree][2d][3h]") {
  // Build tree
  auto tracker = create_test_memory_tracker();
  std::vector<bool> is_default(2, false);
  int32_t dim_dom[] = {1, 1000};
  int32_t dim_extent = 10;
  Domain dom1 = create_domain(
      {"d1", "d2"},
      {Datatype::INT32, Datatype::INT32},
      {dim_dom, dim_dom},
      {&dim_extent, &dim_extent},
      tracker);
  auto mbrs = create_mbrs<int32_t, 2>(
      {1,  3,  2,  4,  5,  7,  6,  9,  10, 12, 10, 15, 11, 15, 20, 22, 16, 16,
       23, 23, 19, 20, 24, 26, 25, 28, 30, 32, 30, 35, 35, 37, 40, 42, 40, 42},
      tracker);
  RTree rtree(&dom1, 3, tracker);
  rtree.set_leaves(mbrs);
  rtree.build_tree();
  CHECK(rtree.height() == 3);
  CHECK(rtree.dim_num() == 2);
  CHECK(rtree.fanout() == 3);

  // Subtree leaf num
  CHECK(rtree.subtree_leaf_num(0) == 9);
  CHECK(rtree.subtree_leaf_num(1) == 3);
  CHECK(rtree.subtree_leaf_num(2) == 1);
  CHECK(rtree.subtree_leaf_num(3) == 0);

  // Tile overlap
  NDRange range(2);
  int32_t r_no[] = {0, 0, 0, 0};
  int32_t r_full[] = {1, 50, 1, 50};
  int32_t r_only_tiles[] = {10, 14, 12, 21};
  int32_t r_only_ranges[] = {11, 42, 20, 42};
  int32_t r_tiles_and_ranges[] = {19, 50, 25, 50};
  range[0] = Range(&r_no[0], 2 * sizeof(int32_t));
  range[1] = Range(&r_no[2], 2 * sizeof(int32_t));
  auto overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());
  range[0] = Range(&r_full[0], 2 * sizeof(int32_t));
  range[1] = Range(&r_full[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 8);
  range[0] = Range(&r_only_tiles[0], 2 * sizeof(int32_t));
  range[1] = Range(&r_only_tiles[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 2);
  CHECK(overlap.tiles_[0].second == 4.0 / 6);
  CHECK(overlap.tiles_[1].first == 3);
  CHECK(overlap.tiles_[1].second == (4.0 / 5) * (2.0 / 3));
  range[0] = Range(&r_only_ranges[0], 2 * sizeof(int32_t));
  range[1] = Range(&r_only_ranges[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 2);
  CHECK(overlap.tile_ranges_[0].first == 3);
  CHECK(overlap.tile_ranges_[0].second == 5);
  CHECK(overlap.tile_ranges_[1].first == 6);
  CHECK(overlap.tile_ranges_[1].second == 8);
  range[0] = Range(&r_tiles_and_ranges[0], 2 * sizeof(int32_t));
  range[1] = Range(&r_tiles_and_ranges[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 6);
  CHECK(overlap.tile_ranges_[0].second == 8);
  CHECK(overlap.tiles_.size() == 1);
  CHECK(overlap.tiles_[0].first == 5);
  CHECK(overlap.tiles_[0].second == 2.0 / 3);
}

TEST_CASE(
    "RTree: Test R-Tree, heterogeneous (uint8, int32), basic functions",
    "[rtree][basic][heter]") {
  // Create RTree with dimensions uint8, int32
  auto tracker = create_test_memory_tracker();
  std::vector<bool> is_default(2, false);
  uint8_t uint8_dom[] = {0, 10};
  int32_t int32_dom[] = {5, 10};
  uint8_t uint8_extent = 2;
  int32_t int32_extent = 2;
  Domain dom1 = create_domain(
      {"d1", "d2"},
      {Datatype::UINT8, Datatype::INT32},
      {uint8_dom, int32_dom},
      {&uint8_extent, &int32_extent},
      tracker);
  const Domain d1 = create_domain(
      {"d1", "d2"},
      {Datatype::UINT8, Datatype::INT32},
      {uint8_dom, int32_dom},
      {&uint8_extent, &int32_extent},
      tracker);
  auto mbrs =
      create_mbrs<uint8_t, int32_t>({0, 1, 3, 5}, {5, 6, 7, 9}, tracker);
  RTree rtree(&d1, 5, tracker);
  rtree.set_leaves(mbrs);
  rtree.build_tree();
  CHECK(rtree.height() == 2);
  CHECK(rtree.dim_num() == 2);
  CHECK(rtree.fanout() == 5);
  CHECK(rtree.leaf(0) == mbrs[0]);
  CHECK(rtree.leaf(1) == mbrs[1]);

  // Check no domain overlap
  NDRange range_no(2);
  uint8_t uint8_r_no[] = {6, 7};
  int32_t int32_r_no[] = {1, 10};
  range_no[0] = Range(uint8_r_no, sizeof(uint8_r_no));
  range_no[1] = Range(int32_r_no, sizeof(int32_r_no));
  double ratio = dom1.overlap_ratio(range_no, is_default, mbrs[0]);
  CHECK(ratio == 0.0);

  // Check full domain overlap
  NDRange range_full(2);
  uint8_t uint8_r_full[] = {0, 10};
  int32_t int32_r_full[] = {1, 10};
  range_full[0] = Range(uint8_r_full, sizeof(uint8_r_full));
  range_full[1] = Range(int32_r_full, sizeof(int32_r_full));
  ratio = dom1.overlap_ratio(range_full, is_default, mbrs[0]);
  CHECK(ratio == 1.0);
  ratio = dom1.overlap_ratio(range_full, is_default, mbrs[1]);
  CHECK(ratio == 1.0);

  // Check partial domain overlap
  NDRange range_part(2);
  uint8_t uint8_r_part[] = {1, 1};
  int32_t int32_r_part[] = {5, 5};
  range_part[0] = Range(uint8_r_part, sizeof(uint8_r_part));
  range_part[1] = Range(int32_r_part, sizeof(int32_r_part));
  ratio = dom1.overlap_ratio(range_part, is_default, mbrs[0]);
  CHECK(ratio == 0.25);
}

TEST_CASE(
    "RTree: Test R-Tree, heterogeneous (uint64, float32), basic functions",
    "[rtree][basic][heter]") {
  // Create RTree with dimensions uint64, float32
  auto tracker = create_test_memory_tracker();
  std::vector<bool> is_default(2, false);
  uint64_t uint64_dom[] = {0, 10};
  float float_dom[] = {0.1f, 0.9f};
  uint64_t uint64_extent = 2;
  float float_extent = 0.1f;
  Domain dom1 = create_domain(
      {"d1", "d2"},
      {Datatype::UINT64, Datatype::FLOAT32},
      {uint64_dom, float_dom},
      {&uint64_extent, &float_extent},
      tracker);
  const Domain d1 = create_domain(
      {"d1", "d2"},
      {Datatype::UINT64, Datatype::FLOAT32},
      {uint64_dom, float_dom},
      {&uint64_extent, &float_extent},
      tracker);
  auto mbrs =
      create_mbrs<uint64_t, float>({0, 1, 3, 5}, {.5f, .6f, .7f, .9f}, tracker);
  RTree rtree(&d1, 5, tracker);
  rtree.set_leaves(mbrs);
  rtree.build_tree();
  CHECK(rtree.height() == 2);
  CHECK(rtree.dim_num() == 2);
  CHECK(rtree.fanout() == 5);
  CHECK(rtree.leaf(0) == mbrs[0]);
  CHECK(rtree.leaf(1) == mbrs[1]);

  // Check no domain overlap
  NDRange range_no(2);
  uint64_t uint64_r_no[] = {6, 7};
  float float_r_no[] = {.1f, .9f};
  range_no[0] = Range(uint64_r_no, sizeof(uint64_r_no));
  range_no[1] = Range(float_r_no, sizeof(float_r_no));
  double ratio = dom1.overlap_ratio(range_no, is_default, mbrs[0]);
  CHECK(ratio == 0.0);

  // Check full domain overlap
  NDRange range_full(2);
  uint64_t uint64_r_full[] = {0, 10};
  float float_r_full[] = {.1f, 1.0f};
  range_full[0] = Range(uint64_r_full, sizeof(uint64_r_full));
  range_full[1] = Range(float_r_full, sizeof(float_r_full));
  ratio = dom1.overlap_ratio(range_full, is_default, mbrs[0]);
  CHECK(ratio == 1.0);
  ratio = dom1.overlap_ratio(range_full, is_default, mbrs[1]);
  CHECK(ratio == 1.0);

  // Check partial domain overlap
  NDRange range_part(2);
  uint64_t uint64_r_part[] = {1, 1};
  float float_r_part[] = {.5f, .55f};
  range_part[0] = Range(uint64_r_part, sizeof(uint64_r_part));
  range_part[1] = Range(float_r_part, sizeof(float_r_part));
  ratio = dom1.overlap_ratio(range_part, is_default, mbrs[0]);
  CHECK(ratio == 0.25);
}

TEST_CASE(
    "RTree: Test 2D R-tree, height 2, heterogeneous (uint8, int32)",
    "[rtree][2d][2h][heter]") {
  // Create RTree with dimensions uint8, int32
  auto tracker = create_test_memory_tracker();
  std::vector<bool> is_default(2, false);
  uint8_t uint8_dom[] = {0, 200};
  int32_t int32_dom[] = {5, 100};
  uint8_t uint8_extent = 2;
  int32_t int32_extent = 2;
  Domain dom = create_domain(
      {"d1", "d2"},
      {Datatype::UINT8, Datatype::INT32},
      {uint8_dom, int32_dom},
      {&uint8_extent, &int32_extent},
      tracker);
  auto mbrs = create_mbrs<uint8_t, int32_t>(
      {0, 1, 3, 5, 11, 20}, {5, 6, 7, 9, 11, 30}, tracker);
  RTree rtree(&dom, 3, tracker);
  rtree.set_leaves(mbrs);
  rtree.build_tree();
  CHECK(rtree.height() == 2);
  CHECK(rtree.dim_num() == 2);
  CHECK(rtree.fanout() == 3);
  CHECK(rtree.leaf(0) == mbrs[0]);
  CHECK(rtree.leaf(1) == mbrs[1]);
  CHECK(rtree.leaf(2) == mbrs[2]);

  // Subtree leaf num
  CHECK(rtree.subtree_leaf_num(0) == 3);
  CHECK(rtree.subtree_leaf_num(1) == 1);
  CHECK(rtree.subtree_leaf_num(2) == 0);

  // Check no tile overlap
  NDRange range_no(2);
  uint8_t uint8_r_no[] = {6, 7};
  int32_t int32_r_no[] = {1, 10};
  range_no[0] = Range(uint8_r_no, sizeof(uint8_r_no));
  range_no[1] = Range(int32_r_no, sizeof(int32_r_no));
  auto overlap = rtree.get_tile_overlap(range_no, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());

  // Check full tile overlap
  NDRange range_full(2);
  uint8_t uint8_r_full[] = {0, 100};
  int32_t int32_r_full[] = {1, 100};
  range_full[0] = Range(uint8_r_full, sizeof(uint8_r_full));
  range_full[1] = Range(int32_r_full, sizeof(int32_r_full));
  overlap = rtree.get_tile_overlap(range_full, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 2);

  // Check partial tile overlap
  NDRange range_part(2);
  uint8_t uint8_r_part[] = {4, 15};
  int32_t int32_r_part[] = {7, 20};
  range_part[0] = Range(uint8_r_part, sizeof(uint8_r_part));
  range_part[1] = Range(int32_r_part, sizeof(int32_r_part));
  overlap = rtree.get_tile_overlap(range_part, is_default);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == 2.0 / 3);
  CHECK(overlap.tiles_[1].first == 2);
  CHECK(overlap.tiles_[1].second == .25);
}

TEST_CASE(
    "RTree: Test 2D R-tree, height 3, heterogeneous (uint8, int32)",
    "[rtree][2d][2h][heter]") {
  // Create RTree with dimensions uint8, int32
  auto tracker = create_test_memory_tracker();
  std::vector<bool> is_default(2, false);
  uint8_t uint8_dom[] = {0, 200};
  int32_t int32_dom[] = {5, 100};
  uint8_t uint8_extent = 2;
  int32_t int32_extent = 2;
  Domain dom = create_domain(
      {"d1", "d2"},
      {Datatype::UINT8, Datatype::INT32},
      {uint8_dom, int32_dom},
      {&uint8_extent, &int32_extent},
      tracker);
  auto mbrs = create_mbrs<uint8_t, int32_t>(
      {0, 1, 3, 5, 11, 20, 21, 26}, {5, 6, 7, 9, 11, 30, 31, 40}, tracker);
  RTree rtree(&dom, 2, tracker);
  rtree.set_leaves(mbrs);
  rtree.build_tree();
  CHECK(rtree.height() == 3);
  CHECK(rtree.dim_num() == 2);
  CHECK(rtree.fanout() == 2);
  CHECK(rtree.leaf(0) == mbrs[0]);
  CHECK(rtree.leaf(1) == mbrs[1]);
  CHECK(rtree.leaf(2) == mbrs[2]);
  CHECK(rtree.leaf(3) == mbrs[3]);

  // Subtree leaf num
  CHECK(rtree.subtree_leaf_num(0) == 4);
  CHECK(rtree.subtree_leaf_num(1) == 2);
  CHECK(rtree.subtree_leaf_num(2) == 1);
  CHECK(rtree.subtree_leaf_num(3) == 0);

  // Check no tile overlap
  NDRange range_no(2);
  uint8_t uint8_r_no[] = {6, 7};
  int32_t int32_r_no[] = {1, 10};
  range_no[0] = Range(uint8_r_no, sizeof(uint8_r_no));
  range_no[1] = Range(int32_r_no, sizeof(int32_r_no));
  auto overlap = rtree.get_tile_overlap(range_no, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());

  // Check full tile overlap
  NDRange range_full(2);
  uint8_t uint8_r_full[] = {0, 100};
  int32_t int32_r_full[] = {1, 100};
  range_full[0] = Range(uint8_r_full, sizeof(uint8_r_full));
  range_full[1] = Range(int32_r_full, sizeof(int32_r_full));
  overlap = rtree.get_tile_overlap(range_full, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 3);

  // Check partial tile overlap, only tiles
  NDRange range_part(2);
  uint8_t uint8_r_part[] = {4, 15};
  int32_t int32_r_part[] = {7, 20};
  range_part[0] = Range(uint8_r_part, sizeof(uint8_r_part));
  range_part[1] = Range(int32_r_part, sizeof(int32_r_part));
  overlap = rtree.get_tile_overlap(range_part, is_default);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == 2.0 / 3);
  CHECK(overlap.tiles_[1].first == 2);
  CHECK(overlap.tiles_[1].second == .25);

  // Check partial tile overlap, only ranges
  NDRange range_ranges(2);
  uint8_t uint8_r_ranges[] = {11, 26};
  int32_t int32_r_ranges[] = {11, 40};
  range_ranges[0] = Range(uint8_r_ranges, sizeof(uint8_r_ranges));
  range_ranges[1] = Range(int32_r_ranges, sizeof(int32_r_ranges));
  overlap = rtree.get_tile_overlap(range_ranges, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 2);
  CHECK(overlap.tile_ranges_[0].second == 3);

  // Check partial tile overlap, both tiles and ranges
  NDRange range_mixed(2);
  uint8_t uint8_r_mixed[] = {4, 26};
  int32_t int32_r_mixed[] = {8, 40};
  range_mixed[0] = Range(uint8_r_mixed, sizeof(uint8_r_mixed));
  range_mixed[1] = Range(int32_r_mixed, sizeof(int32_r_mixed));
  overlap = rtree.get_tile_overlap(range_mixed, is_default);
  CHECK(overlap.tiles_.size() == 1);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == (2.0 / 3) * (2.0 / 3));
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 2);
  CHECK(overlap.tile_ranges_[0].second == 3);
}

// `mbrs` contains a flattened vector of values (low, high)
// per dimension per MBR
template <unsigned D>
tdb::pmr::vector<NDRange> create_str_mbrs(
    const std::vector<std::string>& mbrs, shared_ptr<MemoryTracker> tracker) {
  assert(mbrs.size() % 2 * D == 0);

  uint64_t mbr_num = (uint64_t)(mbrs.size() / (2 * D));
  tdb::pmr::vector<NDRange> ret(
      mbr_num, tracker->get_resource(MemoryType::RTREE));
  for (uint64_t m = 0; m < mbr_num; ++m) {
    ret[m].resize(D);
    for (unsigned d = 0; d < D; ++d) {
      const auto& start = mbrs[2 * D * m + 2 * d];
      const auto& end = mbrs[2 * D * m + 2 * d + 1];
      ret[m][d] = Range(start.data(), start.size(), end.data(), end.size());
    }
  }

  return {ret, tracker->get_resource(MemoryType::RTREE)};
}

// `mbrs` contains a flattened vector of values (low, high)
// per dimension per MBR
tdb::pmr::vector<NDRange> create_str_int32_mbrs(
    const std::vector<std::string>& mbrs_str,
    const std::vector<int32_t> mbrs_int,
    shared_ptr<MemoryTracker> tracker) {
  assert(mbrs_str.size() == mbrs_int.size());
  assert(mbrs_str.size() % 2 == 0);

  uint64_t mbr_num = (uint64_t)(mbrs_str.size() / 2);
  tdb::pmr::vector<NDRange> ret(
      mbr_num, tracker->get_resource(MemoryType::RTREE));
  for (uint64_t m = 0; m < mbr_num; ++m) {
    ret[m].resize(2);
    const auto& start = mbrs_str[2 * m];
    const auto& end = mbrs_str[2 * m + 1];
    ret[m][0] = Range(start.data(), start.size(), end.data(), end.size());
    int32_t range[] = {mbrs_int[2 * m], mbrs_int[2 * m + 1]};
    ret[m][1] = Range(range, sizeof(range));
  }

  return {ret, tracker->get_resource(MemoryType::RTREE)};
}

TEST_CASE(
    "RTree: Test 1D R-tree, string dims, height 2",
    "[rtree][1d][string-dims][2h]") {
  // Build tree
  auto tracker = create_test_memory_tracker();
  std::vector<bool> is_default(1, false);
  Domain dom1 = create_domain(
      {"d"}, {Datatype::STRING_ASCII}, {nullptr}, {nullptr}, tracker);
  auto mbrs =
      create_str_mbrs<1>({"aa", "b", "eee", "g", "gggg", "ii"}, tracker);

  RTree rtree(&dom1, 3, tracker);
  rtree.set_leaves(mbrs);
  rtree.build_tree();
  CHECK(rtree.height() == 2);
  CHECK(rtree.dim_num() == 1);
  CHECK(rtree.fanout() == 3);

  // Subtree leaf num
  CHECK(rtree.subtree_leaf_num(0) == 3);
  CHECK(rtree.subtree_leaf_num(1) == 1);
  CHECK(rtree.subtree_leaf_num(2) == 0);

  // No overlap
  NDRange range(1);
  std::string r_no_start = "c";
  std::string r_no_end = "dd";
  range[0] = Range(
      r_no_start.data(), r_no_start.size(), r_no_end.data(), r_no_end.size());
  auto overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());

  // Full overlap
  std::string r_full_start = "a";
  std::string r_full_end = "iii";
  range[0] = Range(
      r_full_start.data(),
      r_full_start.size(),
      r_full_end.data(),
      r_full_end.size());
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 2);

  // Partial overlap
  std::string r_partial_start = "b";
  std::string r_partial_end = "f";
  range[0] = Range(
      r_partial_start.data(),
      r_partial_start.size(),
      r_partial_end.data(),
      r_partial_end.size());
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 0);
  CHECK(overlap.tiles_[0].second == 1.0 / 2);
  CHECK(overlap.tiles_[1].first == 1);
  CHECK(overlap.tiles_[1].second == 2.0 / 3);

  // Partial overlap
  r_partial_start = "eek";
  r_partial_end = "fff";
  range[0] = Range(
      r_partial_start.data(),
      r_partial_start.size(),
      r_partial_end.data(),
      r_partial_end.size());
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 1);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == 2.0 / 3);
}

TEST_CASE(
    "RTree: Test 1D R-tree, string dims, height 3",
    "[rtree][1d][string-dims][3h]") {
  // Build tree
  auto tracker = create_test_memory_tracker();
  std::vector<bool> is_default(1, false);
  Domain dom1 = create_domain(
      {"d"}, {Datatype::STRING_ASCII}, {nullptr}, {nullptr}, tracker);
  auto mbrs = create_str_mbrs<1>(
      {"aa",
       "b",
       "eee",
       "g",
       "gggg",
       "ii",
       "jj",
       "l",
       "mm",
       "mmn",
       "oo",
       "oop"},
      tracker);

  RTree rtree(&dom1, 3, tracker);
  rtree.set_leaves(mbrs);
  rtree.build_tree();
  CHECK(rtree.height() == 3);
  CHECK(rtree.dim_num() == 1);
  CHECK(rtree.fanout() == 3);

  // Subtree leaf num
  CHECK(rtree.subtree_leaf_num(0) == 9);
  CHECK(rtree.subtree_leaf_num(1) == 3);
  CHECK(rtree.subtree_leaf_num(2) == 1);
  CHECK(rtree.subtree_leaf_num(3) == 0);

  // No overlap
  NDRange range(1);
  std::string r_no_start = "c";
  std::string r_no_end = "dd";
  range[0] = Range(
      r_no_start.data(), r_no_start.size(), r_no_end.data(), r_no_end.size());
  auto overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());

  // Full overlap
  std::string r_full_start = "a";
  std::string r_full_end = "oopp";
  range[0] = Range(
      r_full_start.data(),
      r_full_start.size(),
      r_full_end.data(),
      r_full_end.size());
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 5);

  // Partial overlap
  std::string r_partial_start = "b";
  std::string r_partial_end = "f";
  range[0] = Range(
      r_partial_start.data(),
      r_partial_start.size(),
      r_partial_end.data(),
      r_partial_end.size());
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 0);
  CHECK(overlap.tiles_[0].second == 1.0 / 2);
  CHECK(overlap.tiles_[1].first == 1);
  CHECK(overlap.tiles_[1].second == 2.0 / 3);

  // Partial overlap mixed
  r_partial_start = "h";
  r_partial_end = "p";
  range[0] = Range(
      r_partial_start.data(),
      r_partial_start.size(),
      r_partial_end.data(),
      r_partial_end.size());
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 3);
  CHECK(overlap.tile_ranges_[0].second == 5);
  CHECK(overlap.tiles_.size() == 1);
  CHECK(overlap.tiles_[0].first == 2);
  CHECK(overlap.tiles_[0].second == 2.0 / 3);
}

TEST_CASE(
    "RTree: Test 2D R-tree, string dims, height 2",
    "[rtree][2d][string-dims][2h]") {
  // Build tree
  auto tracker = create_test_memory_tracker();
  std::vector<bool> is_default(2, false);
  Domain dom = create_domain(
      {"d1", "d2"},
      {Datatype::STRING_ASCII, Datatype::STRING_ASCII},
      {nullptr, nullptr},
      {nullptr, nullptr},
      tracker);
  auto mbrs = create_str_mbrs<2>(
      {"aa", "b", "eee", "g", "gggg", "ii", "jj", "lll", "m", "n", "oo", "qqq"},
      tracker);

  RTree rtree(&dom, 3, tracker);
  rtree.set_leaves(mbrs);
  rtree.build_tree();
  CHECK(rtree.height() == 2);
  CHECK(rtree.dim_num() == 2);
  CHECK(rtree.fanout() == 3);

  // Subtree leaf num
  CHECK(rtree.subtree_leaf_num(0) == 3);
  CHECK(rtree.subtree_leaf_num(1) == 1);
  CHECK(rtree.subtree_leaf_num(2) == 0);

  // No overlap
  NDRange range(2);
  std::string r_no_start = "c";
  std::string r_no_end = "dd";
  range[0] = Range(
      r_no_start.data(), r_no_start.size(), r_no_end.data(), r_no_end.size());
  range[1] = Range(
      r_no_start.data(), r_no_start.size(), r_no_end.data(), r_no_end.size());
  auto overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());

  // Full overlap
  std::string r_full_start_1 = "a";
  std::string r_full_end_1 = "nn";
  range[0] = Range(
      r_full_start_1.data(),
      r_full_start_1.size(),
      r_full_end_1.data(),
      r_full_end_1.size());
  std::string r_full_start_2 = "e";
  std::string r_full_end_2 = "r";
  range[1] = Range(
      r_full_start_2.data(),
      r_full_start_2.size(),
      r_full_end_2.data(),
      r_full_end_2.size());
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 2);

  // Partial overlap
  std::string r_partial_start_1 = "h";
  std::string r_partial_end_1 = "i";
  range[0] = Range(
      r_partial_start_1.data(),
      r_partial_start_1.size(),
      r_partial_end_1.data(),
      r_partial_end_1.size());
  std::string r_partial_start_2 = "j";
  std::string r_partial_end_2 = "k";
  range[1] = Range(
      r_partial_start_2.data(),
      r_partial_start_2.size(),
      r_partial_end_2.data(),
      r_partial_end_2.size());
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 1);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == (2.0 / 3) * (2.0 / 3));

  // Partial overlap
  r_partial_start_1 = "b";
  r_partial_end_1 = "gggg";
  range[0] = Range(
      r_partial_start_1.data(),
      r_partial_start_1.size(),
      r_partial_end_1.data(),
      r_partial_end_1.size());
  r_partial_start_2 = "eee";
  r_partial_end_2 = "lll";
  range[1] = Range(
      r_partial_start_2.data(),
      r_partial_start_2.size(),
      r_partial_end_2.data(),
      r_partial_end_2.size());
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 0);
  CHECK(overlap.tiles_[0].second == 1.0 / 2);
  CHECK(overlap.tiles_[1].first == 1);
  CHECK(overlap.tiles_[1].second == 1.0 / 3);
}

TEST_CASE(
    "RTree: Test 2D R-tree (string, int), height 2",
    "[rtree][2d][string-dims][heter][2h]") {
  // Build tree
  std::vector<bool> is_default(2, false);
  int32_t dom_int32[] = {1, 20};
  int32_t tile_extent = 5;
  auto tracker = create_test_memory_tracker();
  Domain dom = create_domain(
      {"d1", "d2"},
      {Datatype::STRING_ASCII, Datatype::INT32},
      {nullptr, dom_int32},
      {nullptr, &tile_extent},
      tracker);
  auto mbrs = create_str_int32_mbrs(
      {"aa", "b", "eee", "g", "gggg", "ii"}, {1, 5, 7, 8, 10, 14}, tracker);

  RTree rtree(&dom, 3, tracker);
  rtree.set_leaves(mbrs);
  rtree.build_tree();
  CHECK(rtree.height() == 2);
  CHECK(rtree.dim_num() == 2);
  CHECK(rtree.fanout() == 3);

  // Subtree leaf num
  CHECK(rtree.subtree_leaf_num(0) == 3);
  CHECK(rtree.subtree_leaf_num(1) == 1);
  CHECK(rtree.subtree_leaf_num(2) == 0);

  // No overlap
  NDRange range(2);
  std::string r_no_start = "c";
  std::string r_no_end = "dd";
  range[0] = Range(
      r_no_start.data(), r_no_start.size(), r_no_end.data(), r_no_end.size());
  int32_t r_no[] = {1, 20};
  range[1] = Range(r_no, sizeof(r_no));
  auto overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());

  // Full overlap
  std::string r_full_start_1 = "a";
  std::string r_full_end_1 = "nn";
  range[0] = Range(
      r_full_start_1.data(),
      r_full_start_1.size(),
      r_full_end_1.data(),
      r_full_end_1.size());
  int32_t r_full[] = {1, 20};
  range[1] = Range(r_full, sizeof(r_full));
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 2);

  // Partial overlap
  std::string r_partial_start_1 = "h";
  std::string r_partial_end_1 = "i";
  range[0] = Range(
      r_partial_start_1.data(),
      r_partial_start_1.size(),
      r_partial_end_1.data(),
      r_partial_end_1.size());
  int32_t r_partial[] = {11, 12};
  range[1] = Range(r_partial, sizeof(r_partial));
  overlap = rtree.get_tile_overlap(range, is_default);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 1);
  CHECK(overlap.tiles_[0].first == 2);
  CHECK(overlap.tiles_[0].second == (2.0 / 3) * (2.0 / 5));

  /*

  // Partial overlap
  r_partial_start_1 = "b";
  r_partial_end_1 = "gggg";
  range[0] = Range(
      r_partial_start_1.data(),
      r_partial_start_1.size(),
      r_partial_end_1.data(),
      r_partial_end_1.size());
  r_partial_start_2 = "eee";
  r_partial_end_2 = "lll";
  range[1] = Range(
      r_partial_start_2.data(),
      r_partial_start_2.size(),
      r_partial_end_2.data(),
      r_partial_end_2.size());
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 0);
  CHECK(overlap.tiles_[0].second == 1.0 / 2);
  CHECK(overlap.tiles_[1].first == 1);
  CHECK(overlap.tiles_[1].second == 1.0 / 3);
  */
}
