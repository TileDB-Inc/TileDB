/**
 * @file unit-rtree.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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

#include "tiledb/sm/rtree/rtree.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE("RTree: Test R-Tree, basic functions", "[rtree][rtree-empty]") {
  // Empty tree
  RTree rtree0;
  CHECK(rtree0.height() == 0);
  CHECK(rtree0.dim_num() == 0);
  CHECK(rtree0.fanout() == 0);
  int r0[] = {1, 10};
  std::vector<const int*> range0;
  range0.push_back(&r0[0]);
  auto tile_overlap = rtree0.get_tile_overlap<int>(range0);
  CHECK(tile_overlap.tile_ranges_.empty());
  CHECK(tile_overlap.tiles_.empty());

  // 1D
  std::vector<void*> mbrs;
  int m[] = {1, 3, 5, 10, 20, 22};
  for (int i = 0; i < 3; ++i)
    mbrs.push_back(&m[2 * i]);
  RTree rtree1(Datatype::INT32, 1, 3, mbrs);
  CHECK(rtree1.height() == 2);
  CHECK(rtree1.subtree_leaf_num(0) == 3);
  CHECK(rtree1.subtree_leaf_num(1) == 1);
  CHECK(rtree1.subtree_leaf_num(2) == 0);
  std::vector<const int*> range1;
  int mbr1[] = {5, 10};
  int r1_no_left[] = {0, 1};
  int r1_left[] = {4, 7};
  int r1_exact[] = {5, 10};
  int r1_full[] = {4, 11};
  int r1_contained[] = {6, 7};
  int r1_right[] = {7, 11};
  int r1_no_right[] = {11, 15};
  range1.push_back(&r1_no_left[0]);
  double ratio1 = rtree1.range_overlap<int>(range1, mbr1);
  CHECK(ratio1 == 0.0);
  range1.clear();
  range1.push_back(&r1_left[0]);
  ratio1 = rtree1.range_overlap<int>(range1, mbr1);
  CHECK(ratio1 == 3.0 / 6);
  range1.clear();
  range1.push_back(&r1_exact[0]);
  ratio1 = rtree1.range_overlap<int>(range1, mbr1);
  CHECK(ratio1 == 1.0);
  range1.clear();
  range1.push_back(&r1_full[0]);
  ratio1 = rtree1.range_overlap<int>(range1, mbr1);
  CHECK(ratio1 == 1.0);
  range1.clear();
  range1.push_back(&r1_contained[0]);
  ratio1 = rtree1.range_overlap<int>(range1, mbr1);
  CHECK(ratio1 == 2.0 / 6);
  range1.clear();
  range1.push_back(&r1_right[0]);
  ratio1 = rtree1.range_overlap<int>(range1, mbr1);
  CHECK(ratio1 == 4.0 / 6);
  range1.clear();
  range1.push_back(&r1_no_right[0]);
  ratio1 = rtree1.range_overlap<int>(range1, mbr1);
  CHECK(ratio1 == 0.0);

  // 2D
  RTree rtree2(Datatype::INT64, 2, 5, mbrs);
  CHECK(rtree2.height() == 2);
  CHECK(rtree2.dim_num() == 2);
  CHECK(rtree2.fanout() == 5);
  CHECK(rtree2.type() == Datatype::INT64);
  std::vector<const int64_t*> range2;
  int64_t mbr2[] = {5, 10, 2, 9};
  int64_t r2_no[] = {6, 7, 10, 12};
  int64_t r2_full[] = {4, 11, 2, 9};
  int64_t r2_partial[] = {7, 11, 4, 5};
  range2.push_back(&r2_no[0]);
  range2.push_back(&r2_no[2]);
  double ratio2 = rtree2.range_overlap<int64_t>(range2, mbr2);
  CHECK(ratio2 == 0.0);
  range2.clear();
  range2.push_back(&r2_full[0]);
  range2.push_back(&r2_full[2]);
  ratio2 = rtree2.range_overlap<int64_t>(range2, mbr2);
  CHECK(ratio2 == 1.0);
  range2.clear();
  range2.push_back(&r2_partial[0]);
  range2.push_back(&r2_partial[2]);
  ratio2 = rtree2.range_overlap<int64_t>(range2, mbr2);
  CHECK(ratio2 == (4.0 / 6) * (2.0 / 8));

  // Float datatype
  RTree rtreef(Datatype::FLOAT32, 1, 5, mbrs);
  std::vector<const float*> rangef;
  float mbrf[] = {5.0f, 10.0f};
  float rf_no_left[] = {0.0, 1.0};
  float rf_left[] = {4.0, 7.0};
  float rf_exact[] = {5.0, 10.0};
  float rf_full[] = {4.0, 11.0};
  float rf_right[] = {7.0, 11.0};
  float rf_no_right[] = {11.0, 15.0};
  rangef.push_back(&rf_no_left[0]);
  double ratiof = rtree1.range_overlap<float>(rangef, mbrf);
  CHECK(ratiof == 0.0);
  rangef.clear();
  rangef.push_back(&rf_left[0]);
  ratiof = rtreef.range_overlap<float>(rangef, mbrf);
  CHECK(ratiof == 2.0 / 5);
  rangef.clear();
  rangef.push_back(&rf_exact[0]);
  ratiof = rtreef.range_overlap<float>(rangef, mbrf);
  CHECK(ratiof == 1.0);
  rangef.clear();
  rangef.push_back(&rf_full[0]);
  ratiof = rtreef.range_overlap<float>(rangef, mbrf);
  CHECK(ratiof == 1.0);
  rangef.clear();
  rangef.push_back(&rf_right[0]);
  ratiof = rtreef.range_overlap<float>(rangef, mbrf);
  CHECK(ratiof == 3.0 / 5);
  rangef.clear();
  rangef.push_back(&rf_no_right[0]);
  ratiof = rtreef.range_overlap<float>(rangef, mbrf);
  CHECK(ratiof == 0.0);
}

TEST_CASE("RTree: Test 1D R-tree, height 2", "[rtree][rtree-1d][rtree-1d-2]") {
  int m[] = {1, 3, 5, 10, 20, 22};
  std::vector<void*> mbrs;
  for (int i = 0; i < 3; ++i)
    mbrs.push_back(&m[2 * i]);

  // Build tree
  RTree rtree(Datatype::INT32, 1, 3, mbrs);
  CHECK(rtree.height() == 2);
  CHECK(rtree.dim_num() == 1);
  CHECK(rtree.fanout() == 3);
  CHECK(rtree.type() == Datatype::INT32);

  // Subtree leaf num
  CHECK(rtree.subtree_leaf_num(0) == 3);
  CHECK(rtree.subtree_leaf_num(1) == 1);
  CHECK(rtree.subtree_leaf_num(2) == 0);

  // Tile overlap
  std::vector<const int*> range;
  int r_no[] = {25, 30};
  int r_full[] = {0, 22};
  int r_partial[] = {6, 21};
  range.push_back(&r_no[0]);
  auto overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());
  range.clear();
  range.push_back(&r_full[0]);
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 2);
  range.clear();
  range.push_back(&r_partial[0]);
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == 5.0 / 6);
  CHECK(overlap.tiles_[1].first == 2);
  CHECK(overlap.tiles_[1].second == 2.0 / 3);
}

TEST_CASE("RTree: Test 1D R-tree, height 3", "[rtree][rtree-1d][rtree-1d-3]") {
  int m[] = {1, 3, 5, 10, 20, 22, 30, 35, 36, 38, 40, 49, 50, 51, 65, 69};
  std::vector<void*> mbrs;
  for (size_t i = 0; i < sizeof(m) / (2 * sizeof(int)); ++i)
    mbrs.push_back(&m[2 * i]);

  // Build tree
  RTree rtree(Datatype::INT32, 1, 3, mbrs);
  CHECK(rtree.height() == 3);
  CHECK(rtree.dim_num() == 1);
  CHECK(rtree.fanout() == 3);
  CHECK(rtree.type() == Datatype::INT32);

  // Subtree leaf num
  CHECK(rtree.subtree_leaf_num(0) == 9);
  CHECK(rtree.subtree_leaf_num(1) == 3);
  CHECK(rtree.subtree_leaf_num(2) == 1);
  CHECK(rtree.subtree_leaf_num(3) == 0);

  // Tile overlap
  std::vector<const int*> range;
  int r_no[] = {0, 0};
  int r_full[] = {1, 69};
  int r_only_tiles[] = {10, 20};
  int r_only_ranges[] = {30, 69};
  int r_tiles_and_ranges[] = {1, 32};
  range.push_back(&r_no[0]);
  auto overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());
  range.clear();
  range.push_back(&r_full[0]);
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 7);
  range.clear();
  range.push_back(&r_only_tiles[0]);
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == 1.0 / 6);
  CHECK(overlap.tiles_[1].first == 2);
  CHECK(overlap.tiles_[1].second == 1.0 / 3);
  range.clear();
  range.push_back(&r_only_ranges[0]);
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 2);
  CHECK(overlap.tile_ranges_[0].first == 3);
  CHECK(overlap.tile_ranges_[0].second == 5);
  CHECK(overlap.tile_ranges_[1].first == 6);
  CHECK(overlap.tile_ranges_[1].second == 7);
  range.clear();
  range.push_back(&r_tiles_and_ranges[0]);
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 2);
  CHECK(overlap.tiles_.size() == 1);
  CHECK(overlap.tiles_[0].first == 3);
  CHECK(overlap.tiles_[0].second == 3.0 / 6);
}

TEST_CASE("RTree: Test 2D R-tree, height 2", "[rtree][rtree-2d][rtree-2d-2]") {
  int m[] = {1, 3, 2, 4, 5, 7, 6, 9, 10, 12, 10, 15};
  std::vector<void*> mbrs;
  for (size_t i = 0; i < sizeof(m) / (4 * sizeof(int)); ++i)
    mbrs.push_back(&m[4 * i]);

  // Build tree
  RTree rtree(Datatype::INT32, 2, 3, mbrs);
  CHECK(rtree.height() == 2);
  CHECK(rtree.dim_num() == 2);
  CHECK(rtree.fanout() == 3);
  CHECK(rtree.type() == Datatype::INT32);

  // Subtree leaf num
  CHECK(rtree.subtree_leaf_num(0) == 3);
  CHECK(rtree.subtree_leaf_num(1) == 1);
  CHECK(rtree.subtree_leaf_num(2) == 0);

  // Tile overlap
  std::vector<const int*> range;
  int r_no[] = {25, 30, 1, 10};
  int r_full[] = {1, 20, 1, 20};
  int r_partial[] = {5, 12, 8, 12};
  range.push_back(&r_no[0]);
  range.push_back(&r_no[2]);
  auto overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());
  range.clear();
  range.push_back(&r_full[0]);
  range.push_back(&r_full[2]);
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 2);
  range.clear();
  range.push_back(&r_partial[0]);
  range.push_back(&r_partial[2]);
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == 2.0 / 4);
  CHECK(overlap.tiles_[1].first == 2);
  CHECK(overlap.tiles_[1].second == 3.0 / 6);
}

TEST_CASE("RTree: Test 2D R-tree, height 3", "[rtree][rtree-2d][rtree-2d-3]") {
  int m[] = {1,  3,  2,  4,  5,  7,  6,  9,  10, 12, 10, 15,
             11, 15, 20, 22, 16, 16, 23, 23, 19, 20, 24, 26,
             25, 28, 30, 32, 30, 35, 35, 37, 40, 42, 40, 42};
  std::vector<void*> mbrs;
  for (size_t i = 0; i < sizeof(m) / (4 * sizeof(int)); ++i)
    mbrs.push_back(&m[4 * i]);

  // Build tree
  RTree rtree(Datatype::INT32, 2, 3, mbrs);
  CHECK(rtree.height() == 3);
  CHECK(rtree.dim_num() == 2);
  CHECK(rtree.fanout() == 3);
  CHECK(rtree.type() == Datatype::INT32);

  // Subtree leaf num
  CHECK(rtree.subtree_leaf_num(0) == 9);
  CHECK(rtree.subtree_leaf_num(1) == 3);
  CHECK(rtree.subtree_leaf_num(2) == 1);
  CHECK(rtree.subtree_leaf_num(3) == 0);

  // Tile overlap
  std::vector<const int*> range;
  int r_no[] = {0, 0, 0, 0};
  int r_full[] = {1, 50, 1, 50};
  int r_only_tiles[] = {10, 14, 12, 21};
  int r_only_ranges[] = {11, 42, 20, 42};
  int r_tiles_and_ranges[] = {19, 50, 25, 50};
  range.push_back(&r_no[0]);
  range.push_back(&r_no[2]);
  auto overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());
  range.clear();
  range.push_back(&r_full[0]);
  range.push_back(&r_full[2]);
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 8);
  range.clear();
  range.push_back(&r_only_tiles[0]);
  range.push_back(&r_only_tiles[2]);
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 2);
  CHECK(overlap.tiles_[0].second == 4.0 / 6);
  CHECK(overlap.tiles_[1].first == 3);
  CHECK(overlap.tiles_[1].second == (4.0 / 5) * (2.0 / 3));
  range.clear();
  range.push_back(&r_only_ranges[0]);
  range.push_back(&r_only_ranges[2]);
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 2);
  CHECK(overlap.tile_ranges_[0].first == 3);
  CHECK(overlap.tile_ranges_[0].second == 5);
  CHECK(overlap.tile_ranges_[1].first == 6);
  CHECK(overlap.tile_ranges_[1].second == 8);
  range.clear();
  range.push_back(&r_tiles_and_ranges[0]);
  range.push_back(&r_tiles_and_ranges[2]);
  overlap = rtree.get_tile_overlap(range);
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 6);
  CHECK(overlap.tile_ranges_[0].second == 8);
  CHECK(overlap.tiles_.size() == 1);
  CHECK(overlap.tiles_[0].first == 5);
  CHECK(overlap.tiles_[0].second == 2.0 / 3);
}
