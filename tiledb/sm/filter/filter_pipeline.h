/**
 * @file   filter_pipeline.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file declares class FilterPipeline.
 */

#ifndef TILEDB_FILTER_PIPELINE_H
#define TILEDB_FILTER_PIPELINE_H

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/misc/status.h"

#include <memory>
#include <vector>

namespace tiledb {
namespace sm {

/** Forward declarations for easier includes. */

class Filter;
class Tile;

/**
 * A filter pipeline is an ordered set of operations (filters) that
 * process/modify tile data. The pipeline is run "forward" during writes and
 * in "reverse" during reads.
 */
class FilterPipeline {
 public:
  /** Constructor. Initializes an empty pipeline. */
  FilterPipeline();

  /** Destructor. */
  ~FilterPipeline() = default;

  /** Copy constructor. */
  FilterPipeline(const FilterPipeline& other);

  /** Copy-assign operator. */
  FilterPipeline& operator=(const FilterPipeline& other);

  /**
   * Deleted (unimplemented) rest of "rule of 5" functions.
   * @{
   */
  FilterPipeline(FilterPipeline&& other) = delete;
  FilterPipeline& operator=(FilterPipeline&& other) = delete;
  /** @} */

  /**
   * Adds a copy of the given filter to the end of this pipeline.
   *
   * @param filter Filter to add
   * @return Status
   */
  Status add_filter(const Filter& filter);

  /** Returns pointer to the current Tile being processed by run/run_reverse. */
  const Tile* current_tile() const;

  /**
   * Populates the filter pipeline from the data in the input binary buffer.
   *
   * @param buff The buffer to deserialize from.
   * @return Status
   */
  Status deserialize(ConstBuffer* buff);

  /**
   * Returns pointer to the first instance of a filter in the pipeline with the
   * given filter subclass type.
   *
   * @tparam T Subclass type of Filter to get
   * @return Pointer to filter instance in the pipeline, or nullptr if not
   *    found.
   */
  template <typename T>
  T* get_filter() const {
    static_assert(
        std::is_base_of<Filter, T>::value,
        "Template type must derive from Filter.");
    for (auto& filter : filters_) {
      auto* ptr = dynamic_cast<T*>(filter.get());
      if (ptr != nullptr)
        return ptr;
    }
    return nullptr;
  }

  /**
   * Returns a pointer to the filter in the pipeline at the given index.
   *
   * @param index Index of filter
   * @return Pointer to filter instance in the pipeline
   */
  Filter* get_filter(unsigned index) const;

  /** Returns the maximum tile chunk size. */
  uint32_t max_chunk_size() const;

  /**
   * Runs the full pipeline on the given tile in the "forward" direction. The
   * forward direction is used during writes, and processes unfiltered (e.g.
   * uncompressed) Tile data into filtered (e.g. compressed) Tile data.
   *
   * The tile is processed in chunks. The byte layout of the tile's buffer after
   * passing through the pipeline is, regardless of the specific filters:
   *
   *   number_of_chunks (uint64_t)
   *   chunk0 (uint8_t[])
   *   chunk1 (uint8_t[])
   *   ...
   *   chunkN (uint8_t[])
   *
   * Each chunk can be a different size. The byte layout of a chunk is:
   *
   *   chunk_orig_length (uint32_t)
   *   chunk_filtered_length (uint32_t)
   *   chunk_data (uint8_t[])
   *
   * The chunk_orig_length value indicates the original unfiltered chunk
   * length, i.e. the length the chunk will be after it has passed through the
   * pipeline in reverse. The chunk_data contains the filtered chunk data, of
   * chunk_filtered_length bytes, which is the chunk after it has passed through
   * the entire pipeline.
   *
   * Each filter in the pipeline transforms the chunk_data bytes
   * arbitrarily. Each filter knows how to interpret the blob of bytes given to
   * it as input when running in reverse.
   *
   * The given Tile's underlying buffer is modified to contain the filtered
   * data.
   *
   * @param tile Tile to filter.
   * @return Status
   */
  Status run_forward(Tile* tile) const;

  /**
   * Runs the pipeline in reverse on the given filtered tile. This is used
   * during reads, and processes filtered Tile data (e.g. compressed) into
   * unfiltered Tile data.
   *
   * This expects as input a Tile in the following byte format:
   *
   *   number_of_chunks (uint64_t)
   *   chunk0_orig_len (uint32_t)
   *   chunk0_data_len (uint32_t)
   *   chunk0_data (uint8_t[])
   *   chunk1_orig_len (uint32_t)
   *   chunk1_data_len (uint32_t)
   *   chunk1_data (uint8_t[])
   *   ...
   *   chunkN_orig_len (uint32_t)
   *   chunkN_data_len (uint32_t)
   *   chunkN_data (uint8_t[])
   *
   * And modifies the Tile's buffer to contain the unfiltered byte array:
   *
   *   tile_data (uint8_t[])
   *
   * The length of tile_data will be the sum of all chunkI_orig_len for I in 0
   * to N.
   *
   * @param tile Tile to filter
   * @return Status
   */
  Status run_reverse(Tile* tile) const;

  /**
   * Serializes the pipeline metadata into a binary buffer.
   *
   * @param buff The buffer to serialize the data into.
   * @return Status
   */
  Status serialize(Buffer* buff) const;

  /** Sets the maximum tile chunk size. */
  void set_max_chunk_size(uint32_t max_chunk_size);

  /** Returns the number of filters in the pipeline. */
  unsigned size() const;

  /** Swaps the contents of this pipeline with the given pipeline. */
  void swap(FilterPipeline& other);

 private:
  /** The ordered list of filters comprising the pipeline. */
  std::vector<std::unique_ptr<Filter>> filters_;

  /**
   * The current tile being processed by run()/run_reverse(). This is mutable
   * because it is the only state modified by those const functions.
   */
  mutable const Tile* current_tile_;

  /** The max chunk size allowed within tiles. */
  uint32_t max_chunk_size_;

  /**
   * Compute chunks of the given tile, used in the forward direction.
   *
   * @param tile Tile to compute chunks for
   * @param chunks Output parameter storing the computed chunks
   * @return Status
   */
  Status compute_tile_chunks(
      Tile* tile, std::vector<std::pair<void*, uint32_t>>* chunks) const;

  /**
   * Run the given list of chunks forward through the pipeline.
   *
   * @param chunks Chunks to process
   * @param output Buffer where output of last stage will be written.
   * @return Status
   */
  Status filter_chunks_forward(
      const std::vector<std::pair<void*, uint32_t>>& chunks,
      Buffer* output) const;

  /**
   * Run the given list of chunks in reverse through the pipeline.
   *
   * @param chunks Chunks to process. Format is
   *    (data ptr, filtered size, original size).
   * @param output Buffer where output of last stage will be written.
   * @return Status
   */
  Status filter_chunks_reverse(
      const std::vector<std::tuple<void*, uint32_t, uint32_t>>& chunks,
      Buffer* output) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILTER_PIPELINE_H
