/**
 * @file relevant_fragments_generator.cc
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
 * This file implements class RelevantFragmentsGenerator.
 */

#include "tiledb/sm/subarray/relevant_fragment_generator.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/sm/subarray/subarray_tile_overlap.h"
#include "tiledb/type/range/range.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

RelevantFragmentGenerator::RelevantFragmentGenerator(
    const shared_ptr<OpenedArray> opened_array,
    const Subarray& subarray,
    stats::Stats* stats)
    : stats_(stats)
    , array_(opened_array)
    , subarray_(subarray) {
  auto dim_num = array_->array_schema_latest().dim_num();
  auto fragment_num = array_->fragment_metadata().size();

  // Create a fragment bytemap for each dimension. Each
  // non-zero byte represents an overlap between a fragment
  // and at least one range in the corresponding dimension.
  fragment_bytemaps_.resize(dim_num);
  for (uint32_t d = 0; d < dim_num; ++d) {
    fragment_bytemaps_[d].resize(fragment_num, subarray_.is_default(d) ? 1 : 0);
  }
}

bool RelevantFragmentGenerator::update_range_coords(
    const SubarrayTileOverlap* tile_overlap) {
  // Fetch the calibrated, multi-dimensional coordinates from the
  // flattened (total order) range indexes. In this context,
  // "calibration" implies that the coordinates contain the minimum
  // n-dimensional space to encapsulate all ranges within `tile_overlap`.
  std::vector<uint64_t> new_start_coords;
  std::vector<uint64_t> new_end_coords;
  auto range_idx_start =
      tile_overlap == nullptr ? 0 : tile_overlap->range_idx_start();
  auto range_idx_end = tile_overlap == nullptr ? subarray_.range_num() - 1 :
                                                 tile_overlap->range_idx_end();
  subarray_.get_expanded_coordinates(
      range_idx_start, range_idx_end, &new_start_coords, &new_end_coords);

  // If the calibrated coordinates have not changed from
  // the last call to this function, the computed relevant
  // fragments will not change.
  if (new_start_coords == start_coords_ && new_end_coords == end_coords_) {
    return false;
  }

  // Store the current calibrated coordinates.
  start_coords_ = new_start_coords;
  end_coords_ = new_end_coords;

  return true;
}

RelevantFragments RelevantFragmentGenerator::compute_relevant_fragments(
    ThreadPool* const compute_tp) {
  auto timer_se = stats_->start_timer("compute_relevant_frags");
  auto dim_num = array_->array_schema_latest().dim_num();
  auto fragment_num = array_->fragment_metadata().size();

  // Populate the fragment bytemap for each dimension in parallel.
  throw_if_not_ok(parallel_for(compute_tp, 0, dim_num, [&](const uint32_t d) {
    if (subarray_.is_default(d))
      return Status::Ok();

    return compute_relevant_fragments_for_dim(
        compute_tp,
        d,
        fragment_num,
        start_coords_,
        end_coords_,
        &fragment_bytemaps_[d]);
  }));

  // Recalculate relevant fragments.
  return RelevantFragments(dim_num, fragment_num, fragment_bytemaps_);
}

Status RelevantFragmentGenerator::compute_relevant_fragments_for_dim(
    ThreadPool* const compute_tp,
    const dimension_size_type dim_idx,
    const size_t fragment_num,
    const std::vector<uint64_t>& start_coords,
    const std::vector<uint64_t>& end_coords,
    std::vector<uint8_t>* const frag_bytemap) const {
  const auto meta = array_->fragment_metadata();
  auto dim{array_->array_schema_latest().dimension_ptr(dim_idx)};

  return parallel_for(compute_tp, 0, fragment_num, [&](const uint64_t f) {
    // We're done when we have already determined fragment `f` to
    // be relevant for this dimension.
    if ((*frag_bytemap)[f] == 1) {
      return Status::Ok();
    }

    // The fragment `f` is relevant to this dimension's fragment bytemap
    // if it overlaps with any range between the start and end coordinates
    // on this dimension.
    const type::Range& frag_range = meta[f]->non_empty_domain()[dim_idx];
    for (uint64_t r = start_coords[dim_idx]; r <= end_coords[dim_idx]; ++r) {
      const type::Range& query_range = subarray_.ranges_for_dim(dim_idx)[r];

      if (dim->overlap(frag_range, query_range)) {
        (*frag_bytemap)[f] = 1;
        break;
      }
    }

    return Status::Ok();
  });
}

}  // namespace sm
}  // namespace tiledb
