/**
 * @file fragment_info_helpers.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file provides declarations and definitions of functionality which
 * may be common to tests inspecting fragment info and fragment metadata.
 */

#ifndef TILEDB_TEST_FRAGMENT_INFO_HELPERS_H
#define TILEDB_TEST_FRAGMENT_INFO_HELPERS_H

#include <test/support/assert_helpers.h>
#include <test/support/src/array_schema_templates.h>

#include "tiledb/api/c_api/fragment_info/fragment_info_api_internal.h"
#include "tiledb/sm/cpp_api/context.h"
#include "tiledb/sm/cpp_api/fragment_info.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/fragment/single_fragment_info.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/tile/test/arithmetic.h"

#include <numeric>
#include <vector>

namespace tiledb::test {

template <templates::DimensionType D, typename Asserter>
std::vector<std::vector<templates::Domain<D>>>
collect_and_validate_fragment_domains(
    const Context& ctx,
    sm::Layout tile_order,
    const std::string& array_name,
    const std::span<const D> tile_extents,
    const sm::NDRange& expect_domain,
    uint64_t max_fragment_size) {
  const uint64_t num_dimensions = expect_domain.size();

  FragmentInfo finfo(ctx, array_name);
  finfo.load();

  // collect fragment domains
  std::vector<std::vector<templates::Domain<D>>> fragment_domains;
  for (uint32_t f = 0; f < finfo.fragment_num(); f++) {
    std::vector<templates::Domain<uint64_t>> this_fragment_domain;
    for (uint64_t d = 0; d < num_dimensions; d++) {
      D bounds[2];
      finfo.get_non_empty_domain(f, d, &bounds[0]);
      this_fragment_domain.push_back(
          templates::Domain<D>(bounds[0], bounds[1]));
    }
    fragment_domains.push_back(this_fragment_domain);
  }

  // the fragments are not always emitted in the same order, sort them
  auto domain_cmp = [&](const auto& left, const auto& right) {
    for (uint64_t di = 0; di < num_dimensions; di++) {
      const uint64_t d =
          (tile_order == sm::Layout::ROW_MAJOR ? di : num_dimensions - di - 1);
      if (left[d].lower_bound < right[d].lower_bound) {
        return true;
      } else if (left[d].lower_bound > right[d].lower_bound) {
        return false;
      } else if (left[d].upper_bound < right[d].upper_bound) {
        return true;
      } else if (left[d].upper_bound > right[d].upper_bound) {
        return false;
      }
    }
    return false;
  };
  std::vector<uint32_t> fragments_in_order(finfo.fragment_num());
  std::iota(fragments_in_order.begin(), fragments_in_order.end(), 0);
  std::sort(
      fragments_in_order.begin(),
      fragments_in_order.end(),
      [&](const uint32_t f_left, const uint32_t f_right) -> bool {
        const auto& left = fragment_domains[f_left];
        const auto& right = fragment_domains[f_right];
        return domain_cmp(left, right);
      });
  std::sort(fragment_domains.begin(), fragment_domains.end(), domain_cmp);

  // validate fragment domains
  ASSERTER(!fragment_domains.empty());

  // fragment domains should be contiguous in global order and cover the whole
  // subarray
  uint64_t subarray_tile_offset = 0;
  for (uint32_t f = 0; f < fragments_in_order.size(); f++) {
    const sm::NDRange& internal_domain =
        finfo.ptr()
            ->fragment_info()
            ->single_fragment_info_vec()[fragments_in_order[f]]
            .non_empty_domain();

    const uint64_t f_num_tiles =
        compute_num_tiles<D>(tile_extents, internal_domain);
    const std::optional<uint64_t> f_start_tile = compute_start_tile<D>(
        tile_order, tile_extents, expect_domain, internal_domain);

    ASSERTER(f_start_tile == subarray_tile_offset);
    subarray_tile_offset += f_num_tiles;
  }
  ASSERTER(
      subarray_tile_offset ==
      compute_num_tiles<D>(tile_extents, expect_domain));

  auto meta_size = [&](uint32_t f) -> uint64_t {
    return finfo.ptr()
        ->fragment_info()
        ->single_fragment_info_vec()[f]
        .meta()
        ->fragment_meta_size();
  };

  // validate fragment size - no fragment should be larger than max requested
  // size
  for (uint32_t f : fragments_in_order) {
    const uint64_t fsize = finfo.fragment_size(f);
    const uint64_t fmetasize = meta_size(f);
    ASSERTER(fsize <= max_fragment_size + fmetasize);
  }

  // validate fragment size - we wrote the largest possible fragments (no two
  // adjacent should be under max fragment size)
  for (uint32_t fi = 1; fi < fragments_in_order.size(); fi++) {
    const uint32_t fprev = fragments_in_order[fi - 1];
    const uint32_t fcur = fragments_in_order[fi];
    const uint64_t combined_size =
        finfo.fragment_size(fprev) + finfo.fragment_size(fcur);
    const uint64_t combined_meta_size = meta_size(fprev) + meta_size(fcur);
    ASSERTER(combined_size > max_fragment_size + combined_meta_size);
  }

  return fragment_domains;
}

}  // namespace tiledb::test

#endif
