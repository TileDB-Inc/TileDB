/**
 * @file   bitsort_filter.h
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
 * This file defines the bit sort filter class.
 */

#ifndef TILEDB_BITSORT_FILTER_H
#define TILEDB_BITSORT_FILTER_H

 
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/filter/bitsort_filter_type.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/filter.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;
class PreallocatedBuffer;

/** Handles bit sorting!
 * TODO: comment more
 */
class BitSortFilter : public Filter {
 public:
  /**
   * Default constructor.
   */
  BitSortFilter()
      : Filter(FilterType::FILTER_BITSORT) {
  }

  /** Dumps the filter details in ASCII format in the selected output. */
  void dump(FILE* out) const override;

  /**
   * Run forward. TODO: COMMENT
   */
  Status run_forward(
      const Tile& tile,
      BitSortFilterMetadataType &pair,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  Status run_forward(
      const Tile& tile,
      Tile* const tile_offsets,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Run reverse. TODO: comment
   */
  Status run_reverse(
      const Tile& tile,
      BitSortFilterMetadataType &pair,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const;

  Status run_reverse(
      const Tile& tile,
      Tile* const tile_offsets,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const override;

  /** Returns a new clone of this filter. */
  BitSortFilter* clone_impl() const override;

 private:
  /**
   * Run forward, templated on the tile type.
   */

  template <typename T>
  Status run_forward(
      BitSortFilterMetadataType &pair,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /**
   * Run reverse, templated on the tile type.
   */
  template <typename T>
  Status run_reverse(
      BitSortFilterMetadataType &pair,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /**
   * TODO: comment part
   */
  template <typename T>
  Status sort_part(const ConstBuffer* input_buffer, Buffer* output_buffer, uint32_t start, std::vector<std::pair<T, uint64_t>> &sorted_elements)  const;

  template <typename T, typename W>
  Status rewrite_dim_tile_forward(
      const std::vector<std::pair<T, uint64_t>>& elements, Tile *dim_tile) const;

  /**
   * TODO: comment
   */
  template <typename T>
  Status unsort_part(
      const std::vector<uint64_t>& positions,
      ConstBuffer* input_buffer,
      Buffer* output_buffer) const;

  Status rewrite_dim_tile_reverse(
      Tile* dim_tile, uint64_t i, const Domain &domain, std::optional<std::reference_wrapper<std::vector<uint64_t>>> positions_opt) const;
  
  template <typename T>
  Status rewrite_dim_tile_reverse(
      Tile* dim_tile, uint64_t i, const Domain &domain, std::optional<std::reference_wrapper<std::vector<uint64_t>>> positions_opt) const;
};

};  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_BITSORT_FILTER_H