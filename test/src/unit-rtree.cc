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
  std::vector<void*> mbrs_1d;
  int m1[] = {1, 3, 5, 10, 20, 22};
  mbrs_1d.resize(3);
  for (int i = 0; i < 3; ++i)
    mbrs_1d[i] = &m1[2 * i];
  RTree rtree1(&dom1, 3, mbrs_1d);
  CHECK(rtree1.height() == 2);
  CHECK(rtree1.dim_num() == 1);
  CHECK(rtree1.subtree_leaf_num(0) == 3);
  CHECK(rtree1.subtree_leaf_num(1) == 1);
  CHECK(rtree1.subtree_leaf_num(2) == 0);
  CHECK(!std::memcmp(rtree1.leaf(0), &m1[0], 2 * sizeof(int)));
  CHECK(!std::memcmp(rtree1.leaf(1), &m1[2], 2 * sizeof(int)));
  CHECK(!std::memcmp(rtree1.leaf(2), &m1[4], 2 * sizeof(int)));

  NDRange range1(1);
  int32_t mbr1[] = {5, 10};
  int32_t r1_no_left[] = {0, 1};
  int32_t r1_left[] = {4, 7};
  int32_t r1_exact[] = {5, 10};
  int32_t r1_full[] = {4, 11};
  int32_t r1_contained[] = {6, 7};
  int32_t r1_right[] = {7, 11};
  int32_t r1_no_right[] = {11, 15};
  range1[0].set_range(r1_no_left, 2 * sizeof(int32_t));
  double ratio1 = rtree1.range_overlap<int>(range1, mbr1);
  CHECK(ratio1 == 0.0);
  range1[0].set_range(r1_left, 2 * sizeof(int32_t));
  ratio1 = rtree1.range_overlap<int>(range1, mbr1);
  CHECK(ratio1 == 3.0 / 6);
  range1[0].set_range(r1_exact, 2 * sizeof(int32_t));
  ratio1 = rtree1.range_overlap<int>(range1, mbr1);
  CHECK(ratio1 == 1.0);
  range1[0].set_range(r1_full, 2 * sizeof(int32_t));
  ratio1 = rtree1.range_overlap<int>(range1, mbr1);
  CHECK(ratio1 == 1.0);
  range1[0].set_range(r1_contained, 2 * sizeof(int32_t));
  ratio1 = rtree1.range_overlap<int>(range1, mbr1);
  CHECK(ratio1 == 2.0 / 6);
  range1[0].set_range(r1_right, 2 * sizeof(int32_t));
  ratio1 = rtree1.range_overlap<int>(range1, mbr1);
  CHECK(ratio1 == 4.0 / 6);
  range1[0].set_range(r1_no_right, 2 * sizeof(int32_t));
  ratio1 = rtree1.range_overlap<int>(range1, mbr1);
  CHECK(ratio1 == 0.0);

  // 2D
  int64_t dim_dom_2[] = {1, 1000};
  int64_t dim_extent_2 = 10;
  Domain dom2 = create_domain(
      {"d1", "d2"},
      {Datatype::INT64, Datatype::INT64},
      {dim_dom_2, dim_dom_2},
      {&dim_extent_2, &dim_extent_2});
  std::vector<void*> mbrs_2d;
  int64_t m2[] = {1, 3, 5, 10, 20, 22, 24, 25, 11, 15, 30, 31};
  mbrs_2d.resize(3);
  for (int i = 0; i < 3; ++i)
    mbrs_2d[i] = &m2[4 * i];
  RTree rtree2(&dom2, 5, mbrs_2d);
  CHECK(rtree2.height() == 2);
  CHECK(rtree2.dim_num() == 2);
  CHECK(rtree2.fanout() == 5);
  CHECK(!std::memcmp(rtree2.leaf(0), &m2[0], 4 * sizeof(int64_t)));
  CHECK(!std::memcmp(rtree2.leaf(1), &m2[4], 4 * sizeof(int64_t)));
  CHECK(!std::memcmp(rtree2.leaf(2), &m2[8], 4 * sizeof(int64_t)));

  NDRange range2(2);
  int64_t mbr2[] = {5, 10, 2, 9};
  int64_t r2_no[] = {6, 7, 10, 12};
  int64_t r2_full[] = {4, 11, 2, 9};
  int64_t r2_partial[] = {7, 11, 4, 5};
  range2[0].set_range(&r2_no[0], 2 * sizeof(int64_t));
  range2[1].set_range(&r2_no[2], 2 * sizeof(int64_t));
  double ratio2 = rtree2.range_overlap<int64_t>(range2, mbr2);
  CHECK(ratio2 == 0.0);
  range2[0].set_range(&r2_full[0], 2 * sizeof(int64_t));
  range2[1].set_range(&r2_full[2], 2 * sizeof(int64_t));
  ratio2 = rtree2.range_overlap<int64_t>(range2, mbr2);
  CHECK(ratio2 == 1.0);
  range2[0].set_range(&r2_partial[0], 2 * sizeof(int64_t));
  range2[1].set_range(&r2_partial[2], 2 * sizeof(int64_t));
  ratio2 = rtree2.range_overlap<int64_t>(range2, mbr2);
  CHECK(ratio2 == (4.0 / 6) * (2.0 / 8));

  // Float datatype
  std::vector<void*> mbrs_f;
  float mf[] = {1.0f, 3.0f, 5.0f, 10.0f, 20.0f, 22.0f};
  mbrs_f.resize(3);
  for (int i = 0; i < 3; ++i)
    mbrs_f[i] = &mf[2 * i];
  float dim_dom_f[] = {1.0, 1000.0};
  float dim_extent_f = 10.0;
  Domain dom2f =
      create_domain({"d"}, {Datatype::FLOAT32}, {dim_dom_f}, {&dim_extent_f});
  RTree rtreef(&dom2f, 5, mbrs_f);

  NDRange rangef(1);
  float mbrf[] = {5.0f, 10.0f};
  float rf_no_left[] = {0.0, 1.0};
  float rf_left[] = {4.0, 7.0};
  float rf_exact[] = {5.0, 10.0};
  float rf_full[] = {4.0, 11.0};
  float rf_right[] = {7.0, 11.0};
  float rf_no_right[] = {11.0, 15.0};
  rangef[0].set_range(rf_no_left, 2 * sizeof(float));
  double ratiof = rtree1.range_overlap<float>(rangef, mbrf);
  CHECK(ratiof == 0.0);
  rangef[0].set_range(rf_left, 2 * sizeof(float));
  ratiof = rtreef.range_overlap<float>(rangef, mbrf);
  CHECK(ratiof == 2.0 / 5);
  rangef[0].set_range(rf_exact, 2 * sizeof(float));
  ratiof = rtreef.range_overlap<float>(rangef, mbrf);
  CHECK(ratiof == 1.0);
  rangef[0].set_range(rf_full, 2 * sizeof(float));
  ratiof = rtreef.range_overlap<float>(rangef, mbrf);
  CHECK(ratiof == 1.0);
  rangef[0].set_range(rf_right, 2 * sizeof(float));
  ratiof = rtreef.range_overlap<float>(rangef, mbrf);
  CHECK(ratiof == 3.0 / 5);
  rangef[0].set_range(rf_no_right, 2 * sizeof(float));
  ratiof = rtreef.range_overlap<float>(rangef, mbrf);
  CHECK(ratiof == 0.0);
}

TEST_CASE("RTree: Test 1D R-tree, height 2", "[rtree][1d][2h]") {
  int m[] = {1, 3, 5, 10, 20, 22};
  std::vector<void*> mbrs;
  mbrs.resize(3);
  for (int i = 0; i < 3; ++i)
    mbrs[i] = &m[2 * i];

  // Build tree
  int32_t dim_dom[] = {1, 1000};
  int32_t dim_extent = 10;
  Domain dom1 =
      create_domain({"d"}, {Datatype::INT32}, {dim_dom}, {&dim_extent});
  RTree rtree(&dom1, 3, mbrs);
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
  auto overlap = rtree.get_tile_overlap<int32_t>(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());
  range[0].set_range(r_full, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap<int32_t>(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 2);
  range[0].set_range(r_partial, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap<int32_t>(range);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == 5.0 / 6);
  CHECK(overlap.tiles_[1].first == 2);
  CHECK(overlap.tiles_[1].second == 2.0 / 3);
}

TEST_CASE("RTree: Test 1D R-tree, height 3", "[rtree][1d][3h]") {
  int m[] = {1, 3, 5, 10, 20, 22, 30, 35, 36, 38, 40, 49, 50, 51, 65, 69};
  std::vector<void*> mbrs;
  for (size_t i = 0; i < sizeof(m) / (2 * sizeof(int)); ++i)
    mbrs.push_back(&m[2 * i]);

  // Build tree
  int32_t dim_dom[] = {1, 1000};
  int32_t dim_extent = 10;
  Domain dom1 =
      create_domain({"d"}, {Datatype::INT32}, {dim_dom}, {&dim_extent});
  RTree rtree(&dom1, 3, mbrs);
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
  auto overlap = rtree.get_tile_overlap<int32_t>(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());
  range[0].set_range(r_full, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap<int32_t>(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 7);
  range[0].set_range(r_only_tiles, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap<int32_t>(range);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == 1.0 / 6);
  CHECK(overlap.tiles_[1].first == 2);
  CHECK(overlap.tiles_[1].second == 1.0 / 3);
  range[0].set_range(r_only_ranges, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap<int32_t>(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 2);
  CHECK(overlap.tile_ranges_[0].first == 3);
  CHECK(overlap.tile_ranges_[0].second == 5);
  CHECK(overlap.tile_ranges_[1].first == 6);
  CHECK(overlap.tile_ranges_[1].second == 7);
  range[0].set_range(r_tiles_and_ranges, 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap<int32_t>(range);
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 2);
  CHECK(overlap.tiles_.size() == 1);
  CHECK(overlap.tiles_[0].first == 3);
  CHECK(overlap.tiles_[0].second == 3.0 / 6);
}

TEST_CASE("RTree: Test 2D R-tree, height 2", "[rtree][2d][2h]") {
  int m[] = {1, 3, 2, 4, 5, 7, 6, 9, 10, 12, 10, 15};
  std::vector<void*> mbrs;
  for (size_t i = 0; i < sizeof(m) / (4 * sizeof(int)); ++i)
    mbrs.push_back(&m[4 * i]);

  // Build tree
  int32_t dim_dom[] = {1, 1000};
  int32_t dim_extent = 10;
  Domain dom2 = create_domain(
      {"d1", "d2"},
      {Datatype::INT32, Datatype::INT32},
      {dim_dom, dim_dom},
      {&dim_extent, &dim_extent});
  RTree rtree(&dom2, 3, mbrs);
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
  auto overlap = rtree.get_tile_overlap<int32_t>(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());
  range[0].set_range(&r_full[0], 2 * sizeof(int32_t));
  range[1].set_range(&r_full[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap<int32_t>(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 2);
  range[0].set_range(&r_partial[0], 2 * sizeof(int32_t));
  range[1].set_range(&r_partial[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap<int32_t>(range);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 1);
  CHECK(overlap.tiles_[0].second == 2.0 / 4);
  CHECK(overlap.tiles_[1].first == 2);
  CHECK(overlap.tiles_[1].second == 3.0 / 6);
}

TEST_CASE("RTree: Test 2D R-tree, height 3", "[rtree][2d][3h]") {
  int m[] = {1,  3,  2,  4,  5,  7,  6,  9,  10, 12, 10, 15,
             11, 15, 20, 22, 16, 16, 23, 23, 19, 20, 24, 26,
             25, 28, 30, 32, 30, 35, 35, 37, 40, 42, 40, 42};
  std::vector<void*> mbrs;
  for (size_t i = 0; i < sizeof(m) / (4 * sizeof(int)); ++i)
    mbrs.push_back(&m[4 * i]);

  // Build tree
  int32_t dim_dom[] = {1, 1000};
  int32_t dim_extent = 10;
  Domain dom2 = create_domain(
      {"d1", "d2"},
      {Datatype::INT32, Datatype::INT32},
      {dim_dom, dim_dom},
      {&dim_extent, &dim_extent});
  RTree rtree(&dom2, 3, mbrs);
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
  auto overlap = rtree.get_tile_overlap<int32_t>(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.empty());
  range[0].set_range(&r_full[0], 2 * sizeof(int32_t));
  range[1].set_range(&r_full[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap<int32_t>(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 0);
  CHECK(overlap.tile_ranges_[0].second == 8);
  range[0].set_range(&r_only_tiles[0], 2 * sizeof(int32_t));
  range[1].set_range(&r_only_tiles[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap<int32_t>(range);
  CHECK(overlap.tile_ranges_.empty());
  CHECK(overlap.tiles_.size() == 2);
  CHECK(overlap.tiles_[0].first == 2);
  CHECK(overlap.tiles_[0].second == 4.0 / 6);
  CHECK(overlap.tiles_[1].first == 3);
  CHECK(overlap.tiles_[1].second == (4.0 / 5) * (2.0 / 3));
  range[0].set_range(&r_only_ranges[0], 2 * sizeof(int32_t));
  range[1].set_range(&r_only_ranges[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap<int32_t>(range);
  CHECK(overlap.tiles_.empty());
  CHECK(overlap.tile_ranges_.size() == 2);
  CHECK(overlap.tile_ranges_[0].first == 3);
  CHECK(overlap.tile_ranges_[0].second == 5);
  CHECK(overlap.tile_ranges_[1].first == 6);
  CHECK(overlap.tile_ranges_[1].second == 8);
  range[0].set_range(&r_tiles_and_ranges[0], 2 * sizeof(int32_t));
  range[1].set_range(&r_tiles_and_ranges[2], 2 * sizeof(int32_t));
  overlap = rtree.get_tile_overlap<int32_t>(range);
  CHECK(overlap.tile_ranges_.size() == 1);
  CHECK(overlap.tile_ranges_[0].first == 6);
  CHECK(overlap.tile_ranges_[0].second == 8);
  CHECK(overlap.tiles_.size() == 1);
  CHECK(overlap.tiles_[0].first == 5);
  CHECK(overlap.tiles_[0].second == 2.0 / 3);
}
