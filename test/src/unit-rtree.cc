/**
 * @file unit-rtree.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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

#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/rtree/rtree.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::sm;

// `mbrs` contains a flattened vector of values (low, high)
// per dimension per MBR
template <class T, unsigned D>
std::vector<NDRange> create_mbrs(const std::vector<T>& mbrs) {
  assert(mbrs.size() % 2 * D == 0);

  uint64_t mbr_num = (uint64_t)(mbrs.size() / (2 * D));
  std::vector<NDRange> ret(mbr_num);
  uint64_t r_size = 2 * sizeof(T);
  for (uint64_t m = 0; m < mbr_num; ++m) {
    ret[m].resize(D);
    for (unsigned d = 0; d < D; ++d) {
      ret[m][d].set_range(&mbrs[2 * D * m + 2 * d], r_size);
    }
  }

  return ret;
}

Domain create_domain(
    const std::vector<std::string>& dim_names,
    const std::vector<Datatype>& dim_types,
    const std::vector<const void*>& dim_domains,
    const std::vector<const void*>& dim_tile_extents) {
  assert(!dim_names.empty());
  assert(dim_names.size() == dim_types.size());
  assert(dim_names.size() == dim_domains.size());
  assert(dim_names.size() == dim_tile_extents.size());

  Domain domain(dim_types[0]);
  for (size_t d = 0; d < dim_names.size(); ++d) {
    Dimension dim(dim_names[d], dim_types[d]);
    dim.set_domain(dim_domains[d]);
    dim.set_tile_extent(dim_tile_extents[d]);
    domain.add_dimension(&dim);
  }
  domain.init(Layout::ROW_MAJOR, Layout::ROW_MAJOR);

  return domain;
}

TEST_CASE("RTree: Test R-Tree, basic functions", "[rtree][basic]") {
  // Empty tree
  RTree rtree0;
  CHECK(rtree0.height() == 0);
  CHECK(rtree0.dim_num() == 0);
  CHECK(rtree0.domain() == nullptr);
  CHECK(rtree0.fanout() == 0);

  // 1D
  int32_t dim_dom[] = {1, 1000};
  int32_t dim_extent = 10;
  Domain dom1 =
      create_domain({"d"}, {Datatype::INT32}, {dim_dom}, {&dim_extent});
  std::vector<NDRange> mbrs_1d = create_mbrs<int32_t, 1>({1, 3, 5, 10, 20, 22});
  RTree rtree1(&dom1, 3);
  CHECK(!rtree1.set_leaf(0, mbrs_1d[0]).ok());
  rtree1.set_leaf_num(mbrs_1d.size());
  for (size_t m = 0; m < mbrs_1d.size(); ++m)
    CHECK(rtree1.set_leaf(m, mbrs_1d[m]).ok());
  CHECK(!rtree1.set_leaf_num(1).ok());
  rtree1.build_tree();
  CHECK(!rtree1.set_leaf(0, mbrs_1d[0]).ok());
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
  mbr1[0].set_range(mbr1_r, 2 * sizeof(int32_t));
  int32_t r1_no_left[] = {0, 1};
  int32_t r1_left[] = {4, 7};
  int32_t r1_exact[] = {5, 10};
  int32_t r1_full[] = {4, 11};
  int32_t r1_contained[] = {6, 7};
  int32_t r1_right[] = {7, 11};
  int32_t r1_no_right[] = {11, 15};
  range1[0].set_range(r1_no_left, 2 * sizeof(int32_t));
  double ratio1 = dom1.overlap_ratio(range1, mbr1);
  CHECK(ratio1 == 0.0);
  range1[0].set_range(r1_left, 2 * sizeof(int32_t));
  ratio1 = dom1.overlap_ratio(range1, mbr1);
  CHECK(ratio1 == 3.0 / 6);
  range1[0].set_range(r1_exact, 2 * sizeof(int32_t));
  ratio1 = dom1.overlap_ratio(range1, mbr1);
  CHECK(ratio1 == 1.0);
  range1[0].set_range(r1_full, 2 * sizeof(int32_t));
  ratio1 = dom1.overlap_ratio(range1, mbr1);
  CHECK(ratio1 == 1.0);
  range1[0].set_range(r1_contained, 2 * sizeof(int32_t));
  ratio1 = dom1.overlap_ratio(range1, mbr1);
  CHECK(ratio1 == 2.0 / 6);
  range1[0].set_range(r1_right, 2 * sizeof(int32_t));
  ratio1 = dom1.overlap_ratio(range1, mbr1);
  CHECK(ratio1 == 4.0 / 6);
  range1[0].set_range(r1_no_right, 2 * sizeof(int32_t));
  ratio1 = dom1.overlap_ratio(range1, mbr1);
  CHECK(ratio1 == 0.0);

  // 2D
  int64_t dim_dom_2[] = {1, 1000};
  int64_t dim_extent_2 = 10;
  Domain dom2 = create_domain(
      {"d1", "d2"},
      {Datatype::INT64, Datatype::INT64},
      {dim_dom_2, dim_dom_2},
      {&dim_extent_2, &dim_extent_2});
  std::vector<NDRange> mbrs_2d =
      create_mbrs<int64_t, 2>({1, 3, 5, 10, 20, 22, 24, 25, 11, 15, 30, 31});
  RTree rtree2(&dom2, 5);
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
  mbr2[0].set_range(&mbr2_r[0], 2 * sizeof(int64_t));
  mbr2[1].set_range(&mbr2_r[2], 2 * sizeof(int64_t));
  int64_t r2_no[] = {6, 7, 10, 12};
  int64_t r2_full[] = {4, 11, 2, 9};
  int64_t r2_partial[] = {7, 11, 4, 5};
  range2[0].set_range(&r2_no[0], 2 * sizeof(int64_t));
  range2[1].set_range(&r2_no[2], 2 * sizeof(int64_t));
  double ratio2 = dom2.overlap_ratio(range2, mbr2);
  CHECK(ratio2 == 0.0);
  range2[0].set_range(&r2_full[0], 2 * sizeof(int64_t));
  range2[1].set_range(&r2_full[2], 2 * sizeof(int64_t));
  ratio2 = dom2.overlap_ratio(range2, mbr2);
  CHECK(ratio2 == 1.0);
  range2[0].set_range(&r2_partial[0], 2 * sizeof(int64_t));
  range2[1].set_range(&r2_partial[2], 2 * sizeof(int64_t));
  ratio2 = dom2.overlap_ratio(range2, mbr2);
  CHECK(ratio2 == (4.0 / 6) * (2.0 / 8));

  // Float datatype
  float dim_dom_f[] = {1.0, 1000.0};
  float dim_extent_f = 10.0;
  std::vector<NDRange> mbrs_f =
      create_mbrs<float, 1>({1.0f, 3.0f, 5.0f, 10.0f, 20.0f, 22.0f});
  Domain dom2f =
      create_domain({"d"}, {Datatype::FLOAT32}, {dim_dom_f}, {&dim_extent_f});
  RTree rtreef(&dom2f, 5);
  rtreef.set_leaves(mbrs_f);
  rtreef.build_tree();

  NDRange rangef(1);
  float mbrf_r[] = {5.0f, 10.0f};
  NDRange mbrf(1);
  mbrf[0].set_range(mbrf_r, 2 * sizeof(float));
  float rf_no_left[] = {0.0, 1.0};
  float rf_left[] = {4.0, 7.0};
  float rf_exact[] = {5.0, 10.0};
  float rf_full[] = {4.0, 11.0};
  float rf_right[] = {7.0, 11.0};
  float rf_no_right[] = {11.0, 15.0};
  rangef[0].set_range(rf_no_left, 2 * sizeof(float));
  double ratiof = dom2f.overlap_ratio(rangef, mbrf);
  CHECK(ratiof == 0.0);
  rangef[0].set_range(rf_left, 2 * sizeof(float));
  ratiof = dom2f.overlap_ratio(rangef, mbrf);
  CHECK(ratiof == 2.0 / 5);
  rangef[0].set_range(rf_exact, 2 * sizeof(float));
  ratiof = dom2f.overlap_ratio(rangef, mbrf);
  CHECK(ratiof == 1.0);
  rangef[0].set_range(rf_full, 2 * sizeof(float));
  ratiof = dom2f.overlap_ratio(rangef, mbrf);
  CHECK(ratiof == 1.0);
  rangef[0].set_range(rf_right, 2 * sizeof(float));
  ratiof = dom2f.overlap_ratio(rangef, mbrf);
  CHECK(ratiof == 3.0 / 5);
  rangef[0].set_range(rf_no_right, 2 * sizeof(float));
  ratiof = dom2f.overlap_ratio(rangef, mbrf);
  CHECK(ratiof == 0.0);
}

TEST_CASE("RTree: Test 1D R-tree, height 2", "[rtree][1d][2h]") {
  // Build tree
  int32_t dim_dom[] = {1, 1000};
  int32_t dim_extent = 10;
  Domain dom1 =
      create_domain({"d"}, {Datatype::INT32}, {dim_dom}, {&dim_extent});
  std::vector<NDRange> mbrs = create_mbrs<int32_t, 1>({1, 3, 5, 10, 20, 22});
  RTree rtree(&dom1, 3);
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
  range[0].set_range(r_no, 2 * sizeof(int32_t));
  auto overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());
  range[0].set_range(r_full, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 2);
  range[0].set_range(r_partial, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == 5.0 / 6);
  CHECK(overlap.tiles_[1].first == 2);
  CHECK(overlap.tiles_[1].second == 2.0 / 3);
}

TEST_CASE("RTree: Test 1D R-tree, height 3", "[rtree][1d][3h]") {
  // Build tree
  int32_t dim_dom[] = {1, 1000};
  int32_t dim_extent = 10;
  std::vector<NDRange> mbrs = create_mbrs<int32_t, 1>(
      {1, 3, 5, 10, 20, 22, 30, 35, 36, 38, 40, 49, 50, 51, 65, 69});
  Domain dom1 =
      create_domain({"d"}, {Datatype::INT32}, {dim_dom}, {&dim_extent});
  RTree rtree(&dom1, 3);
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
  range[0].set_range(r_no, 2 * sizeof(int32_t));
  auto overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());
  range[0].set_range(r_full, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 7);
  range[0].set_range(r_only_tiles, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == 1.0 / 6);
  CHECK(overlap.tiles_[1].first == 2);
  CHECK(overlap.tiles_[1].second == 1.0 / 3);
  range[0].set_range(r_only_ranges, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 2);
  CHECK(overlap.tile_ranges_[0].first == 3);
  CHECK(overlap.tile_ranges_[0].second == 5);
  CHECK(overlap.tile_ranges_[1].first == 6);
  CHECK(overlap.tile_ranges_[1].second == 7);
  range[0].set_range(r_tiles_and_ranges, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 2);
  CHECK(overlap.tiles_.size() == 1);
  CHECK(overlap.tiles_[0].first == 3);
  CHECK(overlap.tiles_[0].second == 3.0 / 6);
}

TEST_CASE("RTree: Test 2D R-tree, height 2", "[rtree][2d][2h]") {
  // Build tree
  int32_t dim_dom[] = {1, 1000};
  int32_t dim_extent = 10;
  Domain dom2 = create_domain(
      {"d1", "d2"},
      {Datatype::INT32, Datatype::INT32},
      {dim_dom, dim_dom},
      {&dim_extent, &dim_extent});
  std::vector<NDRange> mbrs =
      create_mbrs<int32_t, 2>({1, 3, 2, 4, 5, 7, 6, 9, 10, 12, 10, 15});
  RTree rtree(&dom2, 3);
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
  range[0].set_range(&r_no[0], 2 * sizeof(int32_t));
  range[1].set_range(&r_no[2], 2 * sizeof(int32_t));
  auto overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());
  range[0].set_range(&r_full[0], 2 * sizeof(int32_t));
  range[1].set_range(&r_full[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 2);
  range[0].set_range(&r_partial[0], 2 * sizeof(int32_t));
  range[1].set_range(&r_partial[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == 2.0 / 4);
  CHECK(overlap.tiles_[1].first == 2);
  CHECK(overlap.tiles_[1].second == 3.0 / 6);
}

TEST_CASE("RTree: Test 2D R-tree, height 3", "[rtree][2d][3h]") {
  // Build tree
  int32_t dim_dom[] = {1, 1000};
  int32_t dim_extent = 10;
  Domain dom2 = create_domain(
      {"d1", "d2"},
      {Datatype::INT32, Datatype::INT32},
      {dim_dom, dim_dom},
      {&dim_extent, &dim_extent});
  std::vector<NDRange> mbrs = create_mbrs<int32_t, 2>(
      {1,  3,  2,  4,  5,  7,  6,  9,  10, 12, 10, 15, 11, 15, 20, 22, 16, 16,
       23, 23, 19, 20, 24, 26, 25, 28, 30, 32, 30, 35, 35, 37, 40, 42, 40, 42});
  RTree rtree(&dom2, 3);
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
  range[0].set_range(&r_no[0], 2 * sizeof(int32_t));
  range[1].set_range(&r_no[2], 2 * sizeof(int32_t));
  auto overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());
  range[0].set_range(&r_full[0], 2 * sizeof(int32_t));
  range[1].set_range(&r_full[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 8);
  range[0].set_range(&r_only_tiles[0], 2 * sizeof(int32_t));
  range[1].set_range(&r_only_tiles[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 2);
  CHECK(overlap.tiles_[0].second == 4.0 / 6);
  CHECK(overlap.tiles_[1].first == 3);
  CHECK(overlap.tiles_[1].second == (4.0 / 5) * (2.0 / 3));
  range[0].set_range(&r_only_ranges[0], 2 * sizeof(int32_t));
  range[1].set_range(&r_only_ranges[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 2);
  CHECK(overlap.tile_ranges_[0].first == 3);
  CHECK(overlap.tile_ranges_[0].second == 5);
  CHECK(overlap.tile_ranges_[1].first == 6);
  CHECK(overlap.tile_ranges_[1].second == 8);
  range[0].set_range(&r_tiles_and_ranges[0], 2 * sizeof(int32_t));
  range[1].set_range(&r_tiles_and_ranges[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 6);
  CHECK(overlap.tile_ranges_[0].second == 8);
  CHECK(overlap.tiles_.size() == 1);
  CHECK(overlap.tiles_[0].first == 5);
  CHECK(overlap.tiles_[0].second == 2.0 / 3);
}
