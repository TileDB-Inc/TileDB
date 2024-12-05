/**
 * @file   comparators.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * Defines custom comparators to be used for arranging tiles according to
 * their MBR (minimum bounding rectangle) found in fragment metadata.
 */

#ifndef TILEDB_TILE_COMPARATORS_H
#define TILEDB_TILE_COMPARATORS_H

#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/query/readers/result_tile.h"

namespace tiledb::sm {

int cell_order_cmp_NDRange(
    const Domain& domain, unsigned int d, const NDRange& a, const NDRange& b) {
  const auto& dim = *domain.dimension_ptr(d);
  if (dim.var_size()) {
    UntypedDatumView v1(a[d].start_str().data(), a[d].start_str().size());
    UntypedDatumView v2(b[d].start_str().data(), a[d].start_str().size());
    return domain.cell_order_cmp(d, v1, v2);
  } else {
    UntypedDatumView v1(a[d].start_fixed(), a[d].start_size());
    UntypedDatumView v2(b[d].start_fixed(), a[d].start_size());
    return domain.cell_order_cmp(d, v1, v2);
  }
}

template <Layout TILE_ORDER, Layout CELL_ORDER>
class GlobalOrderMBRCmp {
 public:
  explicit GlobalOrderMBRCmp(
      const Domain& domain,
      const std::vector<std::shared_ptr<FragmentMetadata>>& fragment_metadata)
      : domain_(domain)
      , fragment_metadata_(fragment_metadata) {
    static_assert(
        TILE_ORDER == Layout::ROW_MAJOR || TILE_ORDER == Layout::COL_MAJOR);
    static_assert(
        CELL_ORDER == Layout::ROW_MAJOR || CELL_ORDER == Layout::COL_MAJOR);
  }

  bool operator()(const NDRange& left_mbr, const NDRange& right_mbr) const {
    for (unsigned di = 0; di < domain_.dim_num(); ++di) {
      const unsigned d =
          (TILE_ORDER == Layout::ROW_MAJOR ? di : (domain_.dim_num() - di - 1));

      // Not applicable to var-sized dimensions
      if (domain_.dimension_ptr(d)->var_size())
        continue;

      auto res =
          domain_.tile_order_cmp(d, left_mbr[d].data(), right_mbr[d].data());
      if (res < 0) {
        return true;
      } else if (res > 0) {
        return false;
      }
      // else same tile on dimension d --> continue
    }

    // then cell order
    for (unsigned di = 0; di < domain_.dim_num(); ++di) {
      const unsigned d =
          (CELL_ORDER == Layout::ROW_MAJOR ? di : (domain_.dim_num() - di - 1));
      auto res = cell_order_cmp_NDRange(domain_, d, left_mbr, right_mbr);

      if (res < 0) {
        return true;
      } else if (res > 0) {
        return false;
      }
      // else same tile on dimension d --> continue
    }

    // NB: some other comparators care about timestamps here, we will not bother
    // (for now?)
    return 0;
  }

  bool operator()(const ResultTileId& left, const ResultTileId& right) const {
    const auto& left_mbr =
        fragment_metadata_[left.fragment_idx_]->mbr(left.tile_idx_);
    const auto& right_mbr =
        fragment_metadata_[right.fragment_idx_]->mbr(right.tile_idx_);

    return (*this)(left_mbr, right_mbr);
  }

 private:
  const Domain& domain_;

  const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata_;
};

}  // namespace tiledb::sm

#endif
