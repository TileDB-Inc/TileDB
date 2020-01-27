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

const Tile* FilterPipeline::current_tile() const {
  return current_tile_;
}

Status FilterPipeline::filter_chunks_forward(
    const ChunkedBuffer& input, ChunkedBuffer* output) const {
  assert(output);

  // We will only filter chunks that contain data at or below
  // the logical chunked buffer size.
  size_t populated_nchunks = input.nchunks();
  if (input.size() != input.capacity()) {
    populated_nchunks = 0;
    for (uint64_t i = 0; i < input.nchunks(); ++i) {
      uint32_t chunk_buffer_size;
      RETURN_NOT_OK(input.internal_buffer_size(i, &chunk_buffer_size));
      if (chunk_buffer_size == 0) {
        break;
      }
      ++populated_nchunks;
    }
  }

  // Vector storing the input and output of the final pipeline stage for each
  // chunk.
  std::vector<std::pair<FilterBufferPair, FilterBufferPair>> final_stage_io(
      populated_nchunks);

  // Run each chunk through the entire pipeline.
  auto statuses = parallel_for(0, populated_nchunks, [&](uint64_t i) {
    // TODO(ttd): can we instead allocate one FilterStorage per thread?
    // or make it threadsafe?
    FilterStorage storage;
    FilterBuffer input_data(&storage), output_data(&storage);
    FilterBuffer input_metadata(&storage), output_metadata(&storage);

    // First filter's input is the original chunk.
    void* chunk_buffer = nullptr;
    RETURN_NOT_OK(input.internal_buffer(i, &chunk_buffer));
    uint32_t chunk_buffer_size;
    RETURN_NOT_OK(input.internal_buffer_size(i, &chunk_buffer_size));
    RETURN_NOT_OK(input_data.init(chunk_buffer, chunk_buffer_size));

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

  uint64_t total_processed_size = 0;
  std::vector<uint32_t> var_chunk_sizes(final_stage_io.size());
  uint64_t offset = 0;
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
    const uint32_t space_required = 3 * sizeof(uint32_t) +
                                    final_stage_output_data.size() +
                                    final_stage_output_metadata.size();

    total_processed_size += space_required;
    var_chunk_sizes[i] = space_required;
    offsets[i] = offset;
    offset += space_required;
  }

  Buffer output_buffer;
  RETURN_NOT_OK(output_buffer.realloc(total_processed_size));
  statuses = parallel_for(0, final_stage_io.size(), [&](uint64_t i) {
    auto& final_stage_output_metadata = final_stage_io[i].first.first;
    auto& final_stage_output_data = final_stage_io[i].first.second;
    auto filtered_size = (uint32_t)final_stage_output_data.size();
    uint32_t orig_chunk_size;
    RETURN_NOT_OK(input.internal_buffer_size(i, &orig_chunk_size));
    auto metadata_size = (uint32_t)final_stage_output_metadata.size();
    void* dest = output_buffer.data(offsets[i]);
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

  // Slice the contigious 'output_buffer' into variable-sized chunks.
  output_buffer.disown_data();
  auto st = Tile::buffer_to_contigious_var_chunks(
      output_buffer.data(),
      total_processed_size,
      std::move(var_chunk_sizes),
      output);
  if (!st.ok()) {
    free(output_buffer.data());
    output->clear();
  }

  return Status::Ok();
}

Status FilterPipeline::filter_chunks_reverse(
    const ChunkedBuffer& input, ChunkedBuffer* output) const {
  assert(output);

  // We will only filter chunks that contain data at or below
  // the logical chunked buffer size.
  size_t populated_nchunks = input.nchunks();
  if (input.size() != input.capacity()) {
    populated_nchunks = 0;
    for (uint64_t i = 0; i < input.nchunks(); ++i) {
      uint32_t chunk_buffer_size;
      RETURN_NOT_OK(input.internal_buffer_size(i, &chunk_buffer_size));
      if (chunk_buffer_size == 0) {
        break;
      }
      ++populated_nchunks;
    }
  }

  int64_t chunk_size = 0;
  uint64_t total_size = 0;
  std::vector<uint32_t> chunk_sizes(populated_nchunks);
  for (size_t i = 0; i < populated_nchunks; ++i) {
    void* chunk_input;
    RETURN_NOT_OK(input.internal_buffer(i, &chunk_input));
    const uint32_t orig_chunk_len = *reinterpret_cast<uint32_t*>(chunk_input);
    chunk_sizes[i] = orig_chunk_len;
    total_size += orig_chunk_len;

    if (i == 0) {
      chunk_size = orig_chunk_len;
    } else if (orig_chunk_len != chunk_size) {
      chunk_size = -1;
    }
  }

  // Initialize the output chunked buffer. For zipped coordinate tiles, we will
  // store them in a contigious buffer because we will not perform selective
  // decompression on them.
  const ChunkedBuffer::BufferAddressing buffer_addressing =
      current_tile_->stores_coords() ?
          ChunkedBuffer::BufferAddressing::CONTIGIOUS :
          ChunkedBuffer::BufferAddressing::DISCRETE;
  if (chunk_size == -1) {
    RETURN_NOT_OK(
        output->init_var_size(buffer_addressing, std::move(chunk_sizes)));
  } else {
    RETURN_NOT_OK(
        output->init_fixed_size(buffer_addressing, total_size, chunk_size));
  }

  // We will perform lazy allocation for discrete chunk buffers. For contigious
  // chunk buffers, we will allocate the buffer now.
  if (buffer_addressing == ChunkedBuffer::BufferAddressing::CONTIGIOUS) {
    void* buffer = malloc(total_size);
    if (buffer == nullptr) {
      return Status::FilterError("malloc() failed");
    }
    output->set_contigious(buffer);
  }

  // Run each chunk through the entire pipeline.
  auto statuses = parallel_for(0, populated_nchunks, [&](uint64_t i) {
    void* chunk_input;
    RETURN_NOT_OK(input.internal_buffer(i, &chunk_input));

    uint64_t offset = 0;
    const uint32_t orig_chunk_len =
        *reinterpret_cast<uint32_t*>(static_cast<char*>(chunk_input) + offset);
    offset += sizeof(uint32_t);
    const uint32_t filtered_chunk_len =
        *reinterpret_cast<uint32_t*>(static_cast<char*>(chunk_input) + offset);
    offset += sizeof(uint32_t);
    const uint32_t metadata_len =
        *reinterpret_cast<uint32_t*>(static_cast<char*>(chunk_input) + offset);
    offset += sizeof(uint32_t);
    void* const metadata =
        static_cast<void*>(static_cast<char*>(chunk_input) + offset);
    void* const chunk_data =
        static_cast<void*>(static_cast<char*>(metadata) + metadata_len);

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
      void* output_chunk_buffer;
      if (buffer_addressing == ChunkedBuffer::BufferAddressing::DISCRETE) {
        RETURN_NOT_OK(output->alloc_discrete(i, &output_chunk_buffer));
      } else {
        RETURN_NOT_OK(output->internal_buffer(i, &output_chunk_buffer));
      }
      RETURN_NOT_OK(input_data.copy_to(output_chunk_buffer));
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
        void* output_chunk_buffer;
        if (buffer_addressing == ChunkedBuffer::BufferAddressing::DISCRETE) {
          RETURN_NOT_OK(output->alloc_discrete(i, &output_chunk_buffer));
        } else {
          RETURN_NOT_OK(output->internal_buffer(i, &output_chunk_buffer));
        }
        RETURN_NOT_OK(output_data.set_fixed_allocation(
            output_chunk_buffer, orig_chunk_len));
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

  // Since we did not use the 'write' interface above, the 'output' size
  // will still be 0. We wrote the entire capacity of the output buffer,
  // set it here.
  RETURN_NOT_OK(output->set_size(output->capacity()));

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

  // Run the filters over all the chunks and store the result in
  // 'filtered_buffer_chunks'.
  ChunkedBuffer filtered_chunked_buffer;
  const Status st =
      filter_chunks_forward(*tile->chunked_buffer(), &filtered_chunked_buffer);
  if (!st.ok()) {
    filtered_chunked_buffer.free();
    return st;
  }

  // Replace the tile's buffer with the filtered buffer.
  tile->chunked_buffer()->swap(&filtered_chunked_buffer);

  // Now that we've swapped 'filtered_chunked_buffer' into the tile,
  // we can safely free 'filtered_chunked_buffer' which now contains
  // the unfiltered chunked buffer.
  filtered_chunked_buffer.free();

  return Status::Ok();

  STATS_FUNC_OUT(filter_pipeline_run_forward);
}

Status FilterPipeline::run_reverse(Tile* tile) const {
  STATS_FUNC_IN(filter_pipeline_run_reverse);

  current_tile_ = tile;

  ChunkedBuffer* const chunked_buffer = tile->chunked_buffer();
  if (chunked_buffer == nullptr)
    return LOG_STATUS(
        Status::FilterError("Filter error; tile has null buffer."));

  // Run the filters in reverse over all the chunks and store the
  // result in 'filtered_buffer_chunks'.
  ChunkedBuffer unfiltered_chunked_buffer;
  const Status st =
      filter_chunks_reverse(*chunked_buffer, &unfiltered_chunked_buffer);
  if (!st.ok()) {
    unfiltered_chunked_buffer.free();
    return st;
  }

  // Replace the tile's buffer with the unfiltered buffer.
  tile->chunked_buffer()->swap(&unfiltered_chunked_buffer);

  // Now that we've swapped 'unfiltered_chunked_buffer' into the tile,
  // we can safely free 'unfiltered_chunked_buffer' which now contains
  // the unfiltered chunked buffer.
  unfiltered_chunked_buffer.free();

  // Zip the coords.
  if (tile->stores_coords()) {
    // Note that format version < 2 only split the coordinates when compression
    // was used. See https://github.com/TileDB-Inc/TileDB/issues/1053
    bool using_compression = get_filter<CompressionFilter>() != nullptr;
    if (tile->format_version() > 1 || using_compression) {
      RETURN_NOT_OK(tile->zip_coordinates());
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
