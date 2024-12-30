/**
 * @file   filter_pipeline.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/filter/filter.h"
#include "tiledb/sm/filter/filter_buffer.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/tile/filtered_buffer.h"

using namespace tiledb::common;

namespace tiledb::sm {

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

  /** Constructor.
   *
   * @param max_chunk_size.
   * @param filters The vector of filters.
   */
  FilterPipeline(
      uint32_t max_chunk_size, const std::vector<shared_ptr<Filter>>& filters);

  /** Destructor. */
  ~FilterPipeline() = default;

  /** Copy constructor. */
  FilterPipeline(const FilterPipeline& other);

  /** Copy constructor. */
  FilterPipeline(const FilterPipeline& other, const Datatype on_disk_type);

  /** Move constructor. */
  FilterPipeline(FilterPipeline&& other);

  /** Copy-assign operator. */
  FilterPipeline& operator=(const FilterPipeline& other);

  /** Move-assign operator. */
  FilterPipeline& operator=(FilterPipeline&& other);

  /**
   * Adds a copy of the given filter to the end of this pipeline.
   *
   * @param filter Filter to add.
   */
  void add_filter(const Filter& filter);

  /**
   * Adds a copy of the given filter to the end of this pipeline with the given
   * internal type.
   *
   * @param filter Filter to add.
   */
  void add_filter(const Filter& filter, const Datatype new_type);

  /** Clears the pipeline (removes all filters. */
  void clear();

  /** Checks that all filters in a pipeline have compatible types */
  static void check_filter_types(
      const FilterPipeline& pipeline,
      const Datatype first_input_type,
      bool is_var = false);

  /**
   * Populates the filter pipeline from the data in the input binary buffer.
   *
   * @param deserializer The deserializer to deserialize from.
   * @param version Array schema version
   * @return FilterPipeline
   */
  static FilterPipeline deserialize(
      Deserializer& deserializer, const uint32_t version, Datatype datatype);

  /**
   * Checks that two filters have compatible input / output types.
   * Checks fail if the first filter outputs a type not accepted by the second
   * filter as input.
   */
  static void ensure_compatible(
      const Filter& first, const Filter& second, Datatype first_input_type);

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
   * Checks if a certain filter type exists in the filter pipeline
   *
   * @param filter_type The filter type to search for
   * @return True if found, false otherwise
   */
  bool has_filter(const FilterType& filter_type) const;

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
   * @param offsets_tile Offets tile for tile to filter.
   * @param compute_tp The thread pool for compute-bound tasks.
   * @param chunking True if the tile should be cut into chunks before
   * filtering, false if not.
   */
  void run_forward(
      stats::Stats* writer_stats,
      WriterTile* tile,
      WriterTile* offsets_tile,
      ThreadPool* compute_tp,
      bool chunking = true) const;

  /**
   * Runs the pipeline in reverse on the given generic tile.
   *
   * @param reader_stats Stats to record in the function.
   * @param tile Current tile on which the filter pipeline is being run.
   * @param config Global config.
   */
  void run_reverse_generic_tile(
      stats::Stats* stats, Tile& tile, const Config& config) const;

  /**
   * Run the given chunk range in reverse through the pipeline.
   *
   * @param reader_stats Stats to record in the function.
   * @param tile Current tile on which the filter pipeline is being run.
   * @param offsets_tile Offsets tile to unfilter, null if it will be unfilered
   * separately.
   * @param chunk_data The tile chunk info, buffers and offsets.
   * @param min_chunk_index The chunk range index to start from.
   * @param max_chunk_index The chunk range index to end at.
   * @param concurrency_level The maximum level of concurrency.
   * @param config The global config.
   * @return Status.
   */
  Status run_reverse(
      stats::Stats* const reader_stats,
      Tile* const tile,
      Tile* const offsets_tile,
      const ChunkData& chunk_data,
      const uint64_t min_chunk_index,
      const uint64_t max_chunk_index,
      uint64_t concurrency_level,
      const Config& config) const;

  /**
   * Serializes the pipeline metadata into a binary buffer.
   *
   * @param serializer The object the filter pipeline is serialized into.
   * @return Status
   */
  void serialize(Serializer& serializer) const;

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
   * Checks if the offsets tiles of an attribute/dimension should be skipped
   * from being written. This happens in filters that encode the offsets
   * alongside the data.
   *
   * @param type Datatype of the input attribute/dimension
   * @param version Array schema version
   * @return True if we can skip offsets filtering
   */
  bool skip_offsets_filtering(
      const Datatype type,
      const format_version_t version = constants::format_version) const;

  /**
   * Checks if an attribute/dimension needs to be filtered in chunks or as a
   * whole
   *
   * @param is_var True if checking for a var-sized attribute/dimension, false
   * if not
   * @param version Array schema version
   * @param type Datatype of the input attribute/dimension
   * @return True if chunking needs to be used, false if not
   */
  bool use_tile_chunking(
      const bool is_var, const uint32_t version, const Datatype type) const;

  std::vector<shared_ptr<Filter>> filters() const;

 private:
  /** A pair of FilterBuffers. */
  typedef std::pair<FilterBuffer, FilterBuffer> FilterBufferPair;

  /** The ordered list of filters comprising the pipeline. */
  std::vector<shared_ptr<Filter>> filters_;

  /** The max chunk size allowed within tiles. */
  uint32_t max_chunk_size_{0};

  /**
   * Get the chunk offsets for a var sized tile so that integral cells are
   * within a chunk.
   *
   * Heuristic is the following when determining to add a cell:
   *   - If the cell fits in the buffer, add it.
   *   - If it doesn't fit and new size < 150% capacity, add it.
   *   - If it doesn't fit and current size < 50% capacity, add it.
   *
   * @param chunk_size Target chunk size.
   * @param tile Var tile.
   * @param offsets_tile Offsets tile.
   * @return Status, chunk offsets vector.
   */
  tuple<Status, optional<std::vector<uint64_t>>> get_var_chunk_sizes(
      uint32_t chunk_size,
      WriterTile* const tile,
      WriterTile* const offsets_tile) const;

  /**
   * Run the given buffer forward through the pipeline.
   *
   * @param tile Current tile on which the filter pipeline is being run.
   * @param offsets_tile Current offsets tile for var sized attributes.
   * @param input buffer to process.
   * @param chunk_size chunk size.
   * @param chunk_offsets chunk offsets computed for var sized attributes.
   * @param output buffer where output of the last stage
   *    will be written.
   * @param compute_tp The thread pool for compute-bound tasks.
   * @return Status
   */
  Status filter_chunks_forward(
      const WriterTile& tile,
      WriterTile* const offsets_tile,
      uint32_t chunk_size,
      std::vector<uint64_t>& chunk_offsets,
      FilteredBuffer& output,
      ThreadPool* const compute_tp) const;
};

}  // namespace tiledb::sm

#endif  // TILEDB_FILTER_PIPELINE_H

/** Converts the filter into a string representation. */
std::ostream& operator<<(
    std::ostream& os, const tiledb::sm::FilterPipeline& filter_pipeline);
