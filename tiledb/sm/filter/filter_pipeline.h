/**
 * @file   filter_pipeline.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#include <memory>
#include <utility>
#include <vector>

#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/filter/filter.h"
#include "tiledb/sm/filter/filter_buffer.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/tile/chunked_buffer.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;
class EncryptionKey;
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

  /** Move constructor. */
  FilterPipeline(FilterPipeline&& other);

  /** Copy-assign operator. */
  FilterPipeline& operator=(const FilterPipeline& other);

  /** Move-assign operator. */
  FilterPipeline& operator=(FilterPipeline&& other);

  /**
   * Adds a copy of the given filter to the end of this pipeline.
   *
   * @param filter Filter to add
   * @return Status
   */
  Status add_filter(const Filter& filter);

  /**
  * Adds a copy of the given filter to the end of this pipeline.
  *
  * @param filter Filter to add
  * @return Status
  */
  Status prepend_filter(const Filter& filter);

  /** Clears the pipeline (removes all filters. */
  void clear();

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
   * Dumps the filter pipeline details in ASCII format in the selected
   * output.
   */
  void dump(FILE* out) const;

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
   *   chunk0
   *   chunk1
   *   ...
   *   chunkN
   *
   * Each chunk can be a different size. The byte layout of a chunk is:
   *
   *   chunk_orig_length (uint32_t)
   *   chunk_filtered_length (uint32_t)
   *   chunk_metadata_length (uint32_t)
   *   chunk_metadata (uint8_t[])
   *   chunk_filtered_data (uint8_t[])
   *
   * The chunk_orig_length value indicates the original unfiltered chunk
   * length, i.e. the length the chunk will be after it has passed through the
   * pipeline in reverse. The chunk_filtered_data contains the filtered chunk
   * data, of chunk_filtered_length bytes, which is the chunk after it has
   * passed through the entire pipeline. The chunk_metadata contains any
   * metadata that was added by the filters in the pipeline (may be empty).
   *
   * Each filter in the pipeline transforms the chunk_data bytes
   * arbitrarily. Each filter knows how to interpret the blob of bytes given to
   * it as input when running in reverse.
   *
   * The given Tile's underlying buffer is modified to contain the filtered
   * data.
   *
   * @param tile Tile to filter.
   * @param compute_tp The thread pool for compute-bound tasks.
   * @return Status
   */
  Status run_forward(
      stats::Stats* writer_stats, Tile* tile, ThreadPool* compute_tp) const;

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
   *   chunk0_metadata_len (uint32_t)
   *   chunk0_metadata (uint8_t[])
   *   chunk0_data (uint8_t[])
   *   chunk1_orig_len (uint32_t)
   *   chunk1_data_len (uint32_t)
   *   chunk1_metadata_len (uint32_t)
   *   chunk1_metadata (uint8_t[])
   *   chunk1_data (uint8_t[])
   *   ...
   *   chunkN_orig_len (uint32_t)
   *   chunkN_data_len (uint32_t)
   *   chunkN_metadata_len (uint32_t)
   *   chunkN_metadata (uint8_t[])
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
   * @param compute_tp The thread pool for compute-bound tasks.
   * @param config The global config.
   * @param result_cell_slab_ranges optional list of result cell slab ranges. If
   *   this is non-NULL, we will only run the filter pipeline reversal on chunks
   *   that intersect with at least one range element in this list.
   * @return Status
   */
  Status run_reverse(
      stats::Stats* reader_stats,
      Tile* tile,
      ThreadPool* compute_tp,
      const Config& config,
      const std::vector<std::pair<uint64_t, uint64_t>>*
          result_cell_slab_ranges = nullptr) const;

  /**
   * The var-length overload of `run_reverse`.
   */
  Status run_reverse(
      stats::Stats* reader_stats,
      Tile* tile,
      Tile* tile_var,
      ThreadPool* compute_tp,
      const Config& config,
      const std::vector<std::pair<uint64_t, uint64_t>>*
          result_cell_slab_ranges = nullptr) const;

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

  /** Returns true if the pipeline is empty. */
  bool empty() const;

  /** Swaps the contents of this pipeline with the given pipeline. */
  void swap(FilterPipeline& other);

  /**
   * Helper method to append an encryption filter to the given filter pipeline.
   *
   * @param pipeline Pipeline which may be modified.
   * @param encryption_key Encryption key for filter.
   * @return Status
   */
  static Status append_encryption_filter(
      FilterPipeline* pipeline, const EncryptionKey& encryption_key);

  /**
   * Helper method to prepend a conversion filter to the front of a given filter pipeline.
   *
   * @param pipeline Pipeline which may be modified.
   * @param query_datatype The datatype for read or write.
   * @param store_datatype The datatype for storage
   * @return Status
   */
  static Status prepend_conversion_filter(
    FilterPipeline* pipeline, Datatype query_datatype, Datatype store_datatype);

 private:
  /** A pair of FilterBuffers. */
  typedef std::pair<FilterBuffer, FilterBuffer> FilterBufferPair;

  /** The ordered list of filters comprising the pipeline. */
  std::vector<tdb_unique_ptr<Filter>> filters_;

  /**
   * The current tile being processed by run()/run_reverse(). This is mutable
   * because it is the only state modified by those const functions.
   */
  mutable const Tile* current_tile_;

  /** The max chunk size allowed within tiles. */
  uint32_t max_chunk_size_;

  /**
   * Run the given list of chunks forward through the pipeline.
   *
   * @param input chunked buffer to process.
   * @param output buffer where output of the last stage
   *    will be written.
   * @param compute_tp The thread pool for compute-bound tasks.
   * @return Status
   */
  Status filter_chunks_forward(
      const ChunkedBuffer& input, Buffer* output, ThreadPool* compute_tp) const;

  /**
   * Run the given list of chunks in reverse through the pipeline.
   *
   * @param input Filtered chunk buffers to reverse.
   * @param output Chunked buffer where output of the last stage
   *    will be written.
   * @param compute_tp The thread pool for compute-bound tasks.
   * @param unfiltering_all True if all all input chunks will be unfiltered.
   *    This must be consistent with the skip parameter values in 'input'.
   * @param config The global config.
   * @return Status
   */
  Status filter_chunks_reverse(
      const std::vector<std::tuple<void*, uint32_t, uint32_t, uint32_t, bool>>&
          input,
      ChunkedBuffer* output,
      ThreadPool* compute_tp,
      bool unfiltering_all,
      const Config& config) const;

  /**
   * Returns true if we should skip the filter pipeline reversal on a chunk
   * with fixed-size cells.
   *
   * @param chunk_length The unfiltered (original) length of the chunk.
   * @param cells_processed This is an opaque context variable between
   * invocations.
   * @param cell_size Fixed cell size.
   * @param cs_it An opaque context variable to iterate the cell slab ranges.
   * @param cs_end The end-iterator for the cell slab ranges.
   * @param skip The return value for whether the chunk reversal should be
   * skipped.
   * @return Status
   */
  Status skip_chunk_reversal_fixed(
      uint64_t chunk_length,
      uint64_t* cells_processed,
      uint64_t cell_size,
      std::vector<std::pair<uint64_t, uint64_t>>::const_iterator* cs_it,
      const std::vector<std::pair<uint64_t, uint64_t>>::const_iterator& cs_end,
      bool* skip) const;

  /**
   * Returns true if we should skip the filter pipeline reversal on a chunk
   * with var-sized cells.
   *
   * @param chunk_length The unfiltered (original) length of the chunk.
   * @param d_off The contiguous buffer to the offset tile.
   * @param d_off_len The number of uint64_t values in `d_off`.
   * @param cells_processed This is an opaque context variable between
   * invocations.
   * @param cells_size_processed This is an opaque context variable between
   * invocations.
   * @param cs_it An opaque context variable to iterate the cell slab ranges.
   * @param cs_end The end-iterator for the cell slab ranges.
   * @param skip The return value for whether the chunk reversal should be
   * skipped.
   * @return Status
   */
  Status skip_chunk_reversal_var(
      uint64_t chunk_length,
      const uint64_t* d_off,
      uint64_t d_off_size,
      uint64_t* cells_processed,
      uint64_t* cells_size_processed,
      std::vector<std::pair<uint64_t, uint64_t>>::const_iterator* cs_it,
      const std::vector<std::pair<uint64_t, uint64_t>>::const_iterator& cs_end,
      bool* skip) const;

  /**
   * Common implementation between 'skip_chunk_reversal_fixed' and
   * 'skip_chunk_reversal_var'.
   *
   * @param chunk_cell_start The starting cell index of the chunk.
   * @param chunk_cell_end The inclusive ending cell index of the chunk.
   * @param cs_it An opaque context variable to iterate the cell slab ranges.
   * @param cs_end The end-iterator for the cell slab ranges.
   * @param skip The return value for whether the chunk reversal should be
   * skipped.
   * @return Status
   */
  Status skip_chunk_reversal_common(
      uint64_t chunk_cell_start,
      uint64_t chunk_cell_end,
      std::vector<std::pair<uint64_t, uint64_t>>::const_iterator* cs_it,
      const std::vector<std::pair<uint64_t, uint64_t>>::const_iterator& cs_end,
      bool* skip) const;

  /**
   * The internal work routine for `run_reverse`.
   *
   * @param tile Tile to filter
   * @param compute_tp The thread pool for compute-bound tasks.
   * @param config The global config.
   * @param skip_fn The function to determine if the given tile should be
   *   skipped.
   * @return Status
   */
  Status run_reverse_internal(
      stats::Stats* reader_stats,
      Tile* tile,
      ThreadPool* compute_tp,
      const Config& config,
      std::function<Status(uint64_t, bool*)>* skip_fn) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILTER_PIPELINE_H
