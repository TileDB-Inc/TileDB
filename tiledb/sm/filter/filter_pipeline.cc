/**
 * @file   filter_pipeline.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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
 * This file defines class FilterPipeline.
 */

#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/encryption_aes256gcm_filter.h"
#include "tiledb/sm/filter/filter.h"
#include "tiledb/sm/filter/filter_storage.h"
#include "tiledb/sm/filter/noop_filter.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/tile/tile.h"

namespace tiledb {
namespace sm {

FilterPipeline::FilterPipeline() {
  current_tile_ = nullptr;
  max_chunk_size_ = constants::max_tile_chunk_size;
}

FilterPipeline::FilterPipeline(const FilterPipeline& other) {
  for (auto& filter : other.filters_) {
    add_filter(*filter);
  }
  current_tile_ = other.current_tile_;
  max_chunk_size_ = other.max_chunk_size_;
}

FilterPipeline::FilterPipeline(FilterPipeline&& other) {
  swap(other);
}

FilterPipeline& FilterPipeline::operator=(const FilterPipeline& other) {
  // Call copy constructor
  FilterPipeline copy(other);
  // Swap with the temporary copy
  swap(copy);
  return *this;
}

FilterPipeline& FilterPipeline::operator=(FilterPipeline&& other) {
  swap(other);
  return *this;
}

Status FilterPipeline::add_filter(const Filter& filter) {
  std::unique_ptr<Filter> copy(filter.clone());
  copy->set_pipeline(this);
  filters_.push_back(std::move(copy));
  return Status::Ok();
}

void FilterPipeline::clear() {
  filters_.clear();
}

Status FilterPipeline::compute_tile_chunks(
    Tile* tile, std::vector<std::pair<void*, uint32_t>>* chunks) const {
  // For coordinate tiles, we treat each dimension separately (chunks won't
  // cross dimension boundaries, since the coordinates have been split).
  // Attribute tiles are treated as a whole.
  auto dim_num = tile->stores_coords() ? tile->dim_num() : 1;
  auto dim_tile_size = tile->size() / dim_num;
  auto dim_cell_size = tile->cell_size() / dim_num;

  // Compute a chunk size as a multiple of the cell size, ensuring that the
  // chunk contains always at least 1 cell.
  uint64_t chunk_size = std::min((uint64_t)max_chunk_size_, dim_tile_size);
  chunk_size = chunk_size / dim_cell_size * dim_cell_size;
  chunk_size = std::max(chunk_size, dim_cell_size);
  if (chunk_size > std::numeric_limits<uint32_t>::max())
    return LOG_STATUS(
        Status::FilterError("Filter error; chunk size exceeds uint32_t"));

  // Compute number of chunks
  auto chunk_num = dim_tile_size / (chunk_size) +
                   uint64_t(bool(dim_tile_size % (chunk_size)));

  // Compute the chunks
  uint64_t offset = 0;
  for (uint64_t i = 0; i < dim_num; i++) {
    for (uint64_t j = 0; j < chunk_num; j++) {
      // Compute the actual size of the chunk (may be smaller at the end of the
      // tile if the chunk size doesn't evenly divide).
      auto size = static_cast<uint32_t>(
          std::min(chunk_size, dim_tile_size - j * chunk_size));
      chunks->emplace_back(
          static_cast<char*>(tile->internal_data()) + offset, size);
      offset += size;
    }
  }

  return Status::Ok();
}

const Tile* FilterPipeline::current_tile() const {
  return current_tile_;
}

Status FilterPipeline::filter_chunks_forward(
    const std::vector<std::pair<void*, uint32_t>>& chunks,
    Buffer* output) const {
  // Vector storing the input and output of the final pipeline stage for each
  // chunk.
  std::vector<std::pair<FilterBufferPair, FilterBufferPair>> final_stage_io(
      chunks.size());

  // Run each chunk through the entire pipeline.
  auto statuses = parallel_for(0, chunks.size(), [&](uint64_t i) {
    // TODO(ttd): can we instead allocate one FilterStorage per thread?
    // or make it threadsafe?
    FilterStorage storage;
    FilterBuffer input_data(&storage), output_data(&storage);
    FilterBuffer input_metadata(&storage), output_metadata(&storage);

    // First filter's input is the original chunk.
    const auto& chunk_input = chunks[i];
    RETURN_NOT_OK(input_data.init(chunk_input.first, chunk_input.second));

    // Apply the filters sequentially.
    for (auto it = filters_.begin(), ite = filters_.end(); it != ite; ++it) {
      auto& f = *it;

      // Clear and reset I/O buffers
      input_data.reset_offset();
      input_data.set_read_only(true);
      input_metadata.reset_offset();
      input_metadata.set_read_only(true);

      output_data.clear();
      output_metadata.clear();

      RETURN_NOT_OK(f->run_forward(
          &input_metadata, &input_data, &output_metadata, &output_data));

      input_data.set_read_only(false);
      input_data.swap(output_data);
      input_metadata.set_read_only(false);
      input_metadata.swap(output_metadata);
      // Next input (input_buffers) now stores this output (output_buffers).
    }

    // Save the finished chunk (last stage's output). This is safe to do because
    // when the local FilterStorage goes out of scope, it will not free the
    // buffers saved here as their shared_ptr counters will not be zero.
    // However, as the output may have been a view on the input, we do need to
    // save both here to prevent the input buffer from being freed.
    auto& io = final_stage_io[i];
    auto& io_input = io.first;
    auto& io_output = io.second;
    io_input.first.swap(input_metadata);
    io_input.second.swap(input_data);
    io_output.first.swap(output_metadata);
    io_output.second.swap(output_data);

    return Status::Ok();
  });

  // Check statuses
  for (auto st : statuses)
    RETURN_NOT_OK(st);

  // Compute the destination offset of each processed chunk in the final output
  // buffer.
  uint64_t offset = output->offset();
  uint64_t total_processed_size = 0;
  std::vector<uint64_t> offsets(final_stage_io.size());
  for (uint64_t i = 0; i < final_stage_io.size(); i++) {
    auto& final_stage_output_metadata = final_stage_io[i].first.first;
    auto& final_stage_output_data = final_stage_io[i].first.second;

    // Check the size doesn't exceed the limit (should never happen).
    if (final_stage_output_data.size() > std::numeric_limits<uint32_t>::max() ||
        final_stage_output_metadata.size() >
            std::numeric_limits<uint32_t>::max())
      return LOG_STATUS(Status::FilterError(
          "Filter error; filtered chunk size exceeds uint32_t"));

    // Leave space for the chunk sizes and the data itself.
    auto space_required = 3 * sizeof(uint32_t) +
                          final_stage_output_data.size() +
                          final_stage_output_metadata.size();
    offsets[i] = offset;
    offset += space_required;
    total_processed_size += space_required;
  }

  // Concatenate all processed chunks into the final output buffer.
  RETURN_NOT_OK(output->realloc(output->size() + total_processed_size));
  statuses = parallel_for(0, final_stage_io.size(), [&](uint64_t i) {
    auto& final_stage_output_metadata = final_stage_io[i].first.first;
    auto& final_stage_output_data = final_stage_io[i].first.second;
    auto filtered_size = (uint32_t)final_stage_output_data.size();
    auto orig_chunk_size = chunks[i].second;
    auto metadata_size = (uint32_t)final_stage_output_metadata.size();
    void* dest = output->data(offsets[i]);
    uint64_t dest_offset = 0;

    // Write the original (unfiltered) chunk size
    std::memcpy((char*)dest + dest_offset, &orig_chunk_size, sizeof(uint32_t));
    dest_offset += sizeof(uint32_t);
    // Write the filtered chunk size
    std::memcpy((char*)dest + dest_offset, &filtered_size, sizeof(uint32_t));
    dest_offset += sizeof(uint32_t);
    // Write the metadata size
    std::memcpy((char*)dest + dest_offset, &metadata_size, sizeof(uint32_t));
    dest_offset += sizeof(uint32_t);
    // Write the chunk metadata
    RETURN_NOT_OK(
        final_stage_output_metadata.copy_to((char*)dest + dest_offset));
    dest_offset += metadata_size;
    // Write the chunk data
    RETURN_NOT_OK(final_stage_output_data.copy_to((char*)dest + dest_offset));
    return Status::Ok();
  });

  // Check statuses
  for (auto st : statuses)
    RETURN_NOT_OK(st);

  // Ensure the final size is set to the concatenated size.
  output->advance_offset(total_processed_size);
  output->advance_size(total_processed_size);

  return Status::Ok();
}

Status FilterPipeline::filter_chunks_reverse(
    const std::vector<std::tuple<void*, uint32_t, uint32_t, uint32_t>>& chunks,
    Buffer* output) const {
  // Precompute the offsets for the final chunks in the shared output buffer.
  std::vector<uint64_t> chunk_dest_offsets(chunks.size());
  uint64_t chunk_dest_offset = 0;
  for (uint64_t i = 0; i < chunks.size(); i++) {
    chunk_dest_offsets[i] = chunk_dest_offset;
    chunk_dest_offset += std::get<2>(chunks[i]);
  }

  // Run each chunk through the entire pipeline.
  auto statuses = parallel_for(0, chunks.size(), [&](uint64_t i) {
    const auto& chunk_input = chunks[i];
    uint32_t filtered_chunk_len = std::get<1>(chunk_input);
    uint32_t orig_chunk_len = std::get<2>(chunk_input);
    uint32_t metadata_len = std::get<3>(chunk_input);
    void* metadata = std::get<0>(chunk_input);
    void* chunk_data = (char*)metadata + metadata_len;

    // TODO(ttd): can we instead allocate one FilterStorage per thread?
    // or make it threadsafe?
    FilterStorage storage;
    FilterBuffer input_data(&storage), output_data(&storage);
    FilterBuffer input_metadata(&storage), output_metadata(&storage);

    // First filter's input is the filtered chunk data.
    RETURN_NOT_OK(input_metadata.init(metadata, metadata_len));
    RETURN_NOT_OK(input_data.init(chunk_data, filtered_chunk_len));

    // If the pipeline is empty, just copy input to output.
    if (filters_.empty()) {
      RETURN_NOT_OK(input_data.copy_to(output->data(chunk_dest_offsets[i])));
      return Status::Ok();
    }

    // Apply the filters sequentially in reverse.
    for (int64_t filter_idx = (int64_t)filters_.size() - 1; filter_idx >= 0;
         filter_idx--) {
      auto& f = filters_[filter_idx];

      // Clear and reset I/O buffers
      input_data.reset_offset();
      input_data.set_read_only(true);
      input_metadata.reset_offset();
      input_metadata.set_read_only(true);

      output_data.clear();
      output_metadata.clear();

      // Final filter: output directly into the shared output buffer.
      bool last_filter = filter_idx == 0;
      if (last_filter) {
        void* dest = output->data(chunk_dest_offsets[i]);
        RETURN_NOT_OK(output_data.set_fixed_allocation(dest, orig_chunk_len));
      }

      RETURN_NOT_OK(f->run_reverse(
          &input_metadata, &input_data, &output_metadata, &output_data));

      input_data.set_read_only(false);
      input_metadata.set_read_only(false);

      if (!last_filter) {
        input_data.swap(output_data);
        input_metadata.swap(output_metadata);
        // Next input (input_buffers) now stores this output (output_buffers).
      }
    }

    return Status::Ok();
  });

  // Check statuses
  for (auto st : statuses)
    RETURN_NOT_OK(st);

  // Ensure the final size is set to the sum of unfiltered chunk sizes.
  output->set_offset(chunk_dest_offset);
  output->set_size(chunk_dest_offset);

  return Status::Ok();
}

Filter* FilterPipeline::get_filter(unsigned index) const {
  if (index >= filters_.size())
    return nullptr;

  return filters_[index].get();
}

uint32_t FilterPipeline::max_chunk_size() const {
  return max_chunk_size_;
}

Status FilterPipeline::run_forward(Tile* tile) const {
  STATS_FUNC_IN(filter_pipeline_run_forward);

  current_tile_ = tile;

  // Compute the chunks.
  std::vector<std::pair<void*, uint32_t>> chunks;
  RETURN_NOT_OK(compute_tile_chunks(tile, &chunks));
  uint64_t num_chunks = chunks.size();
  if (num_chunks == 0)
    return Status::FilterError("Filter error; tile has 0 chunks.");

  // Allocate a buffer to hold the end result (the concatentated, filtered
  // chunks), and write the number of chunks.
  Buffer filtered_tile;
  filtered_tile.realloc(tile->buffer()->size());
  RETURN_NOT_OK(filtered_tile.write(&num_chunks, sizeof(uint64_t)));

  // Run the filters over all the chunks into the filtered_tile buffer.
  RETURN_NOT_OK(filter_chunks_forward(chunks, &filtered_tile));

  // Replace the tile's buffer with the filtered buffer.
  RETURN_NOT_OK(tile->buffer()->swap(filtered_tile));

  return Status::Ok();

  STATS_FUNC_OUT(filter_pipeline_run_forward);
}

Status FilterPipeline::run_reverse(Tile* tile) const {
  STATS_FUNC_IN(filter_pipeline_run_reverse);

  auto tile_buff = tile->buffer();
  if (tile_buff == nullptr)
    return LOG_STATUS(
        Status::FilterError("Filter error; tile has null buffer."));

  current_tile_ = tile;

  // First make a pass over the tile to get the chunk information.
  tile_buff->reset_offset();
  uint64_t num_chunks;
  RETURN_NOT_OK(tile_buff->read(&num_chunks, sizeof(uint64_t)));
  std::vector<std::tuple<void*, uint32_t, uint32_t, uint32_t>> chunks(
      num_chunks);
  uint64_t total_orig_size = 0;
  for (uint64_t i = 0; i < num_chunks; i++) {
    uint32_t filtered_chunk_size, orig_chunk_size, metadata_size;
    RETURN_NOT_OK(tile_buff->read(&orig_chunk_size, sizeof(uint32_t)));
    RETURN_NOT_OK(tile_buff->read(&filtered_chunk_size, sizeof(uint32_t)));
    RETURN_NOT_OK(tile_buff->read(&metadata_size, sizeof(uint32_t)));
    chunks[i] = std::make_tuple(
        tile_buff->cur_data(),
        filtered_chunk_size,
        orig_chunk_size,
        metadata_size);
    tile_buff->advance_offset(metadata_size + filtered_chunk_size);
    total_orig_size += orig_chunk_size;
  }
  assert(tile_buff->offset() == tile_buff->size());

  // Allocate a buffer to hold the end result (the assembled, unfiltered
  // chunks).
  Buffer unfiltered_tile;
  RETURN_NOT_OK(unfiltered_tile.realloc(total_orig_size));

  // Run the filters in reverse over all the chunks into the unfiltered_tile
  // buffer.
  RETURN_NOT_OK(filter_chunks_reverse(chunks, &unfiltered_tile));

  // Replace the tile's buffer with the unfiltered buffer.
  RETURN_NOT_OK(tile->buffer()->swap(unfiltered_tile));

  // Zip the coords.
  if (tile->stores_coords()) {
    // Note that format version < 2 only split the coordinates when compression
    // was used. See https://github.com/TileDB-Inc/TileDB/issues/1053
    bool using_compression = get_filter<CompressionFilter>() != nullptr;
    if (tile->format_version() > 1 || using_compression) {
      tile->zip_coordinates();
    }
  }

  return Status::Ok();

  STATS_FUNC_OUT(filter_pipeline_run_reverse);
}

// ===== FORMAT =====
// max_chunk_size (uint32_t)
// num_filters (uint32_t)
// filter0 metadata (see Filter::serialize)
// filter1...
Status FilterPipeline::serialize(Buffer* buff) const {
  RETURN_NOT_OK(buff->write(&max_chunk_size_, sizeof(uint32_t)));
  auto num_filters = static_cast<uint32_t>(filters_.size());
  RETURN_NOT_OK(buff->write(&num_filters, sizeof(uint32_t)));

  for (const auto& f : filters_) {
    // For compatibility with the old attribute compressor API: if the filter
    // is a compression filter but with no compression, serialize the filter
    // as a no-op filter instead.
    auto as_compression = dynamic_cast<CompressionFilter*>(f.get());
    if (as_compression != nullptr && f->type() == FilterType::FILTER_NONE) {
      auto noop = std::unique_ptr<NoopFilter>(new NoopFilter);
      RETURN_NOT_OK(noop->serialize(buff));
    } else {
      RETURN_NOT_OK(f->serialize(buff));
    }
  }

  return Status::Ok();
}

Status FilterPipeline::deserialize(ConstBuffer* buff) {
  // Remove any old filters.
  clear();

  RETURN_NOT_OK(buff->read(&max_chunk_size_, sizeof(uint32_t)));
  uint32_t num_filters;
  RETURN_NOT_OK(buff->read(&num_filters, sizeof(uint32_t)));

  for (uint32_t i = 0; i < num_filters; i++) {
    Filter* filter;
    RETURN_NOT_OK(Filter::deserialize(buff, &filter));
    RETURN_NOT_OK_ELSE(add_filter(*filter), delete filter);
    delete filter;
  }

  return Status::Ok();
}

void FilterPipeline::set_max_chunk_size(uint32_t max_chunk_size) {
  max_chunk_size_ = max_chunk_size;
}

unsigned FilterPipeline::size() const {
  return static_cast<unsigned>(filters_.size());
}

void FilterPipeline::swap(FilterPipeline& other) {
  filters_.swap(other.filters_);

  for (auto& f : filters_)
    f->set_pipeline(this);

  for (auto& f : other.filters_)
    f->set_pipeline(&other);

  std::swap(current_tile_, other.current_tile_);
  std::swap(max_chunk_size_, other.max_chunk_size_);
}

Status FilterPipeline::append_encryption_filter(
    FilterPipeline* pipeline, const EncryptionKey& encryption_key) {
  switch (encryption_key.encryption_type()) {
    case EncryptionType ::NO_ENCRYPTION:
      return Status::Ok();
    case EncryptionType::AES_256_GCM:
      return pipeline->add_filter(EncryptionAES256GCMFilter(encryption_key));
    default:
      return LOG_STATUS(Status::FilterError(
          "Error appending encryption filter; unknown type."));
  }
}

}  // namespace sm
}  // namespace tiledb
