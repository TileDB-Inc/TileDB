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
 * This file declares the bitsort filter class. The bitsort filter takes in
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
  /** Default Constructor. */
  BitSortFilter()
      : Filter(FilterType::FILTER_BITSORT) {
  }

  /** Dumps the filter details in ASCII format in the selected output. */
  void dump(FILE* out) const override;

  /**
   * @brief Run forward. Takes input data parts, and per part it stores
   * the input values, sorted in bit order (ascending).
   *
   * @param tile Current tile on which the filter is being run.
   * @param support_data Offsets tile of the current tile on which the filter is
   * being run.
   * @param input_metadata Buffer with metadata for `input`.
   * @param input Buffer with data to be filtered.
   * @param output_metadata Buffer with metadata for filtered data.
   * @param output Buffer with filtered data (unused by in-place filters).
   * @return Status
   */
  Status run_forward(
      const WriterTile& tile,
      void* const support_data,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * @brief Run reverse. Takes output data parts, which should be sorted
   * in bit order, and re-sorts them into global order.
   *
   * @param tile Current attribute tile on which the filter is being run.
   * @param bitsort_metadata Contains the dimension tiles of the schema and
   * a comparison function in order to sort the dimension tiles in global order.
   * @param input_metadata Buffer with metadata for `input`.
   * @param input Buffer with data to be filtered.
   * @param output_metadata Buffer with metadata for filtered data.
   * @param output Buffer with filtered data.
   * @return Status
   */
  Status run_reverse(
      const Tile& tile,
      void* support_data,
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
   *
   * @tparam AttrType Attribute tile type.
   * @param dim_tiles Dimension tiles of the schema.
   * @param input_metadata Buffer with metadata for `input`.
   * @param input Buffer with data to be filtered.
   * @param output_metadata Buffer with metadata for filtered data.
   * @param output Buffer with filtered data.
   * @return Status
   */
  template <typename AttrType>
  Status run_forward(
      const std::vector<WriterTile*>& dim_tiles,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /**
   * Run reverse, templated on the attribute tile type.
   *
   * @tparam AttrType Attribute tile type.
   * @param bitsort_metadata Contains the dimension tiles of the schema and
   * a comparison function in order to sort the dimension tiles in global order.
   * @param input_metadata Buffer with metadata for `input`.
   * @param input Buffer with data to be filtered.
   * @param output_metadata Buffer with metadata for filtered data.
   * @param output Buffer with filtered data.
   * @return Status
   */
  template <typename AttrType>
  Status run_reverse(
      BitSortFilterMetadataType& bitsort_metadata,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /**
   * @brief Rewrites the dimension tile given the positions of the attributes.
   *
   * @tparam DimType Dimension tile type.
   * @param positions Vector with the sorted positions of the attribute data.
   * @param dim_tile Dimension tile to rewrite.
   */
  template <typename DimType>
  void run_forward_dim_tile(
      const std::vector<size_t>& positions, WriterTile* dim_tile) const;

  /**
   * @brief Unsorts the input buffer and restores the array by sorting the
   * positions in global order, then using this order to rewrite the dimension
   * tiles. It also writes the original array to the output buffer.
   *
   * @tparam AttrType Attribute tile type.
   * @param positions Vector with the sorted positions of the attribute data.
   * @param input_buffer Input buffer containing the sorted attribute data
   * for one part.
   * @param output_buffer Output buffer for all data in the input.
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
   * @param bitsort_metadata Contains the dimension tiles of the schema and
   * a comparison function in order to sort the dimension tiles in global order.
   * @param positions Vector for storing positions of the attribute data sorted
   * in global order.
   * @return Status
   */
  Status run_reverse_dim_tiles(
      BitSortFilterMetadataType& bitsort_metadata,
      std::vector<uint64_t>& positions) const;

  /**
   * @brief Rewrites the dimension tile given the positions of the attributes.
   *
   * @tparam DimType Dimension tile type.
   * @param dim_tile Dimension tile to rewrite.
   * @param positions Vector with the sorted positions of the attribute data.
   */
  template <typename DimType>
  void run_reverse_dim_tile(
      Tile* dim_tile, std::vector<uint64_t>& positions) const;
};

};  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_BITSORT_FILTER_H