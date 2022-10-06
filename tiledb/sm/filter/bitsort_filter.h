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
 * This file defines the bit sort filter class. The bitsort filter takes in
 * any fixed size, non-nullable input data and sorts it according to its
 * input values, as integers, on write. Then on read, this filter restores
 * the original order of the data.
 *
 * In order to prevent storing positions on write, this filter instead
 * rewrites the dimension tiles, and uses the dimension tile information
 * to store the positions in the sort. Then, in the read, the bitsort filter
 * will sort the data in global order.
 */

#ifndef TILEDB_BITSORT_FILTER_H
#define TILEDB_BITSORT_FILTER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/bitsort_filter_type.h"
#include "tiledb/sm/filter/filter.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;
class PreallocatedBuffer;

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
   * @brief Run forward. Takes input data parts, and per part it stores
   * the input values, sorted in bit order (ascending).
   *
   * @return Status
   */
  Status run_forward(
      const Tile& tile,
      std::vector<Tile*>& dim_tiles,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /**
   * @brief Dummy run forward to satisfy the Filter class requirements.
   *
   * @return Status_FilterError. This function should never be called.
   */
  Status run_forward(
      const Tile& tile,
      Tile* const tile_offsets,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * @brief Run reverse. Takes output data parts, which should be sorted
   * in bit order, and re-sorts them into global order.
   *
   * @return Status
   */
  Status run_reverse(
      const Tile& tile,
      BitSortFilterMetadataType& pair,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const;

  /**
   * @brief Dummy run reverse to satisfy the Filter class requirements.
   *
   * @return Status_FilterError. This function should never be called.
   */
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
   * Run forward, templated on the attribute tile type.
   */
  template <typename AttrType>
  Status run_forward(
      std::vector<Tile*>& dim_tiles,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /**
   * Run reverse, templated on the attribute tile type.
   */
  template <typename AttrType>
  Status run_reverse(
      BitSortFilterMetadataType& pair,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /**
   * @brief Sorts the input buffer by bit order and stores the positions
   * of the sort in the vector sorted_elements.
   *
   * @return Status
   */
  template <typename AttrType>
  Status sort_part(
      const ConstBuffer* input_buffer,
      Buffer* output_buffer,
      uint32_t start,
      std::vector<std::pair<AttrType, uint64_t>>& sorted_elements) const;

  /**
   * @brief Given attribute type T and dimension type W, rewrites the dimension
   * tile given the positions of the attributes.
   *
   * @return Status
   */
  template <typename AttrType, typename DimType>
  Status rewrite_dim_tile_forward(
      const std::vector<std::pair<AttrType, uint64_t>>& elements,
      Tile* dim_tile) const;

  /**
   * @brief Unsorts the input buffer and restores the array by sorting the
   * positions in global order, then using this order to rewrite the dimension
   * tiles. It also writes the original array to the output buffer.
   *
   * @return Status
   */
  template <typename AttrType>
  Status unsort_part(
      const std::vector<uint64_t>& positions,
      ConstBuffer* input_buffer,
      Buffer* output_buffer) const;

  /**
   * @brief Rewrites the dimension tiles given the positions of the attributes.
   *
   * @return Status
   */
  Status rewrite_dim_tiles_reverse(
      BitSortFilterMetadataType& pair, std::vector<uint64_t>& positions) const;

  /**
   * @brief Given the dimension type T, rewrites the dimension tile given the
   * positions of the attributes.
   *
   * @return Status
   */
  template <typename DimType>
  Status rewrite_dim_tile_reverse(
      Tile* dim_tile, std::vector<uint64_t>& positions) const;
};

};  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_BITSORT_FILTER_H