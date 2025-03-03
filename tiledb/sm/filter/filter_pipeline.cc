/**
 * @file   filter_pipeline.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
#include "filter_create.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/encryption_aes256gcm_filter.h"
#include "tiledb/sm/filter/filter.h"
#include "tiledb/sm/filter/filter_storage.h"
#include "tiledb/sm/filter/noop_filter.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/tile/tile.h"
#include "webp_filter.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
class FilterPipelineStatusException : public StatusException {
 public:
  explicit FilterPipelineStatusException(const std::string& msg)
      : StatusException("FilterPipeline", msg) {
  }
};

FilterPipeline::FilterPipeline()
    : max_chunk_size_(constants::max_tile_chunk_size) {
}

FilterPipeline::FilterPipeline(
    uint32_t max_chunk_size, const std::vector<shared_ptr<Filter>>& filters)
    : filters_(filters)
    , max_chunk_size_(max_chunk_size) {
}

// Unlike move constructors, copy constructors must not use default,
// because individual filters are being copied by calling clone.
FilterPipeline::FilterPipeline(const FilterPipeline& other) {
  for (auto& filter : other.filters_) {
    add_filter(*filter);
  }
  max_chunk_size_ = other.max_chunk_size_;
}

FilterPipeline::FilterPipeline(
    const FilterPipeline& other, const Datatype on_disk_type) {
  auto current_type = on_disk_type;
  for (auto& filter : other.filters_) {
    add_filter(*filter, current_type);
    current_type = filters_.back()->output_datatype(current_type);
  }
  max_chunk_size_ = other.max_chunk_size_;
}

FilterPipeline::FilterPipeline(FilterPipeline&& other) = default;

FilterPipeline& FilterPipeline::operator=(const FilterPipeline& other) {
  // Call copy constructor
  FilterPipeline copy(other);
  // Swap with the temporary copy
  swap(copy);
  return *this;
}

FilterPipeline& FilterPipeline::operator=(FilterPipeline&& other) = default;

void FilterPipeline::add_filter(const Filter& filter) {
  shared_ptr<Filter> copy(filter.clone());
  filters_.push_back(std::move(copy));
}

void FilterPipeline::add_filter(const Filter& filter, const Datatype new_type) {
  shared_ptr<Filter> copy(filter.clone(new_type));
  filters_.push_back(std::move(copy));
}

void FilterPipeline::clear() {
  filters_.clear();
}

void FilterPipeline::check_filter_types(
    const FilterPipeline& pipeline,
    const Datatype first_input_type,
    bool is_var) {
  if (pipeline.filters_.empty()) {
    return;
  }

  if ((first_input_type == Datatype::STRING_ASCII ||
       first_input_type == Datatype::STRING_UTF8) &&
      is_var && pipeline.size() > 1) {
    if (pipeline.has_filter(FilterType::FILTER_RLE) &&
        pipeline.get_filter(0)->type() != FilterType::FILTER_RLE) {
      throw FilterPipelineStatusException(
          "RLE filter must be the first filter to apply when used on a "
          "variable length string attribute");
    }
    if (pipeline.has_filter(FilterType::FILTER_DICTIONARY) &&
        pipeline.get_filter(0)->type() != FilterType::FILTER_DICTIONARY) {
      throw FilterPipelineStatusException(
          "Dictionary filter must be the first filter to apply when used on a "
          "variable length string attribute");
    }
  }

  // ** Modern checks using Filter output type **
  pipeline.get_filter(0)->ensure_accepts_datatype(first_input_type);
  auto input_type = pipeline.get_filter(0)->output_datatype(first_input_type);
  for (unsigned i = 1; i < pipeline.size(); ++i) {
    ensure_compatible(
        *pipeline.get_filter(i - 1), *pipeline.get_filter(i), input_type);
    input_type = pipeline.get_filter(i)->output_datatype(input_type);
  }
}

tuple<Status, optional<std::vector<uint64_t>>>
FilterPipeline::get_var_chunk_sizes(
    uint32_t chunk_size,
    WriterTile* const tile,
    WriterTile* const offsets_tile) const {
  std::vector<uint64_t> chunk_offsets;
  if (offsets_tile != nullptr) {
    uint64_t num_offsets = offsets_tile->size_as<offsets_t>();
    auto offsets = offsets_tile->data_as<offsets_t>();

    uint64_t current_size = 0;
    uint64_t min_size = chunk_size / 2;
    uint64_t max_size = chunk_size + chunk_size / 2;
    chunk_offsets.emplace_back(0);
    for (uint64_t c = 0; c < num_offsets; c++) {
      auto cell_size = c == num_offsets - 1 ? tile->size() - offsets[c] :
                                              offsets[c + 1] - offsets[c];

      // Time for a new chunk?
      auto new_size = current_size + cell_size;
      if (new_size > chunk_size) {
        // Do we add this cell to this chunk?
        if (current_size <= min_size || new_size <= max_size) {
          if (new_size > std::numeric_limits<uint32_t>::max()) {
            return {
                LOG_STATUS(Status_FilterError("Chunk size exceeds uint32_t")),
                nullopt};
          }
          chunk_offsets.emplace_back(offsets[c] + cell_size);
          current_size = 0;
        } else {  // Start a new chunk.
          chunk_offsets.emplace_back(offsets[c]);

          // This cell belong in its own chunk.
          if (cell_size > chunk_size) {
            if (cell_size > std::numeric_limits<uint32_t>::max()) {
              return {
                  LOG_STATUS(Status_FilterError("Chunk size exceeds uint32_t")),
                  nullopt};
            }

            if (c != num_offsets - 1)
              chunk_offsets.emplace_back(offsets[c] + cell_size);
            current_size = 0;
          } else {  // Start a new chunk.
            current_size = cell_size;
          }
        }
      } else {
        current_size += cell_size;
      }
    }
  }

  return {Status::Ok(), std::move(chunk_offsets)};
}

Status FilterPipeline::filter_chunks_forward(
    const WriterTile& tile,
    WriterTile* const offsets_tile,
    uint32_t chunk_size,
    std::vector<uint64_t>& chunk_offsets,
    FilteredBuffer& output,
    ThreadPool* const compute_tp) const {
  bool var_sizes = chunk_offsets.size() > 0;
  uint64_t last_buffer_size = chunk_size;
  uint64_t nchunks = 1;
  // if chunking will be used
  if (tile.size() != chunk_size) {
    nchunks = var_sizes ? chunk_offsets.size() : tile.size() / chunk_size;
    last_buffer_size = var_sizes ? tile.size() - chunk_offsets[nchunks - 1] :
                                   tile.size() % chunk_size;
    if (!var_sizes) {
      if (last_buffer_size != 0) {
        nchunks++;
      } else {
        last_buffer_size = chunk_size;
      }
    }
  }

  // Vector storing the input and output of the final pipeline stage for each
  // chunk.
  std::vector<std::pair<FilterBufferPair, FilterBufferPair>> final_stage_io(
      nchunks);

  // Run each chunk through the entire pipeline.
  auto status = parallel_for(compute_tp, 0, nchunks, [&](uint64_t i) {
    // TODO(ttd): can we instead allocate one FilterStorage per thread?
    // or make it threadsafe?
    FilterStorage storage;
    FilterBuffer input_data(&storage), output_data(&storage);
    FilterBuffer input_metadata(&storage), output_metadata(&storage);

    // First filter's input is the original chunk.
    uint64_t offset = var_sizes ? chunk_offsets[i] : i * chunk_size;
    void* chunk_buffer = tile.data_as<char>() + offset;
    uint32_t chunk_buffer_size =
        i == nchunks - 1 ? last_buffer_size :
        var_sizes        ? chunk_offsets[i + 1] - chunk_offsets[i] :
                           chunk_size;
    RETURN_NOT_OK(input_data.init(chunk_buffer, chunk_buffer_size));

    // Apply the filters sequentially.
    for (auto it = filters_.begin(), ite = filters_.end(); it != ite; ++it) {
      auto& f = *it;

      // Clear and reset I/O buffers
      input_data.reset_offset();
      input_data.set_read_only(true);
      input_metadata.reset_offset();
      input_metadata.set_read_only(true);

      throw_if_not_ok(output_data.clear());
      throw_if_not_ok(output_metadata.clear());

      f->init_compression_resource_pool(compute_tp->concurrency_level());

      f->run_forward(
          tile,
          offsets_tile,
          &input_metadata,
          &input_data,
          &output_metadata,
          &output_data);

      input_data.set_read_only(false);
      throw_if_not_ok(input_data.swap(output_data));
      input_metadata.set_read_only(false);
      throw_if_not_ok(input_metadata.swap(output_metadata));
      // Next input (input_buffers) now stores this output (output_buffers).
    }

    // Save the finished chunk (last stage's output). This is safe to do
    // because when the local FilterStorage goes out of scope, it will not
    // free the buffers saved here as their shared_ptr counters will not
    // be zero. However, as the output may have been a view on the input, we
    // do need to save both here to prevent the input buffer from being
    // freed.
    auto& io = final_stage_io[i];
    auto& io_input = io.first;
    auto& io_output = io.second;
    throw_if_not_ok(io_input.first.swap(input_metadata));
    throw_if_not_ok(io_input.second.swap(input_data));
    throw_if_not_ok(io_output.first.swap(output_metadata));
    throw_if_not_ok(io_output.second.swap(output_data));
    return Status::Ok();
  });

  RETURN_NOT_OK(status);

  uint64_t total_processed_size = 0;
  std::vector<uint32_t> var_chunk_sizes(final_stage_io.size());
  uint64_t offset = sizeof(uint64_t);
  std::vector<uint64_t> offsets(final_stage_io.size());
  for (uint64_t i = 0; i < final_stage_io.size(); i++) {
    auto& final_stage_output_metadata = final_stage_io[i].first.first;
    auto& final_stage_output_data = final_stage_io[i].first.second;

    // Check the size doesn't exceed the limit (should never happen).
    if (final_stage_output_data.size() > std::numeric_limits<uint32_t>::max() ||
        final_stage_output_metadata.size() >
            std::numeric_limits<uint32_t>::max())
      return LOG_STATUS(Status_FilterError(
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

  // Allocate enough space in 'output' to store the leading uint64_t
  // prefix containing the number of chunks and the 'total_processed_size'.
  output.expand(sizeof(uint64_t) + total_processed_size);

  // Write the leading prefix that contains the number of chunks.
  memcpy(output.data(), &nchunks, sizeof(uint64_t));

  // Concatenate all processed chunks into the final output buffer.
  status = parallel_for(compute_tp, 0, final_stage_io.size(), [&](uint64_t i) {
    auto& final_stage_output_metadata = final_stage_io[i].first.first;
    auto& final_stage_output_data = final_stage_io[i].first.second;
    auto filtered_size = (uint32_t)final_stage_output_data.size();
    uint32_t orig_chunk_size =
        i == final_stage_io.size() - 1 ? last_buffer_size :
        var_sizes ? chunk_offsets[i + 1] - chunk_offsets[i] :
                    chunk_size;
    auto metadata_size = (uint32_t)final_stage_output_metadata.size();
    void* dest = output.data() + offsets[i];
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
    throw_if_not_ok(
        final_stage_output_metadata.copy_to((char*)dest + dest_offset));
    dest_offset += metadata_size;
    // Write the chunk data
    throw_if_not_ok(final_stage_output_data.copy_to((char*)dest + dest_offset));
    return Status::Ok();
  });

  RETURN_NOT_OK(status);

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

void FilterPipeline::run_forward(
    stats::Stats* const writer_stats,
    WriterTile* const tile,
    WriterTile* const offsets_tile,
    ThreadPool* const compute_tp,
    bool use_chunking) const {
  throw_if_not_ok(
      tile ? Status::Ok() : Status_Error("invalid argument: null Tile*"));

  writer_stats->add_counter("write_filtered_byte_num", tile->size());

  uint32_t chunk_size = 0;
  if (use_chunking) {
    chunk_size =
        WriterTile::compute_chunk_size(tile->size(), tile->cell_size());
  } else {
    chunk_size = tile->size();
  }

  // Get the chunk sizes for var size attributes.
  auto&& [st, chunk_offsets] =
      get_var_chunk_sizes(chunk_size, tile, offsets_tile);
  if (!st.ok()) {
    tile->filtered_buffer().clear();
    throw_if_not_ok(st);
  }

  // Run the filters over all the chunks and store the result in
  // 'filtered_buffer'.
  st = filter_chunks_forward(
      *tile,
      offsets_tile,
      chunk_size,
      *chunk_offsets,
      tile->filtered_buffer(),
      compute_tp);
  if (!st.ok()) {
    tile->filtered_buffer().clear();
    throw_if_not_ok(st);
  }

  // The contents of 'buffer' have been filtered and stored
  // in 'filtered_buffer'. We can safely free 'buffer'.
  tile->clear_data();
}

void FilterPipeline::run_reverse_generic_tile(
    stats::Stats* stats, Tile& tile, const Config& config) const {
  ChunkData chunk_data;
  tile.load_chunk_data(chunk_data);
  for (unsigned c = 0; c < chunk_data.filtered_chunks_.size(); c++) {
    throw_if_not_ok(
        run_reverse(stats, &tile, nullptr, chunk_data, c, c + 1, 1, config));
  }
  tile.clear_filtered_buffer();
}

Status FilterPipeline::run_reverse(
    stats::Stats* const reader_stats,
    Tile* const tile,
    Tile* const offsets_tile,
    const ChunkData& chunk_data,
    const uint64_t min_chunk_index,
    const uint64_t max_chunk_index,
    uint64_t concurrency_level,
    const Config& config) const {
  // Run each chunk through the entire pipeline.
  for (size_t i = min_chunk_index; i < max_chunk_index; i++) {
    auto& chunk = chunk_data.filtered_chunks_[i];
    FilterStorage storage;
    FilterBuffer input_data(&storage), output_data(&storage);
    FilterBuffer input_metadata(&storage), output_metadata(&storage);

    // First filter's input is the filtered chunk data.
    RETURN_NOT_OK(input_metadata.init(
        chunk.filtered_metadata_, chunk.filtered_metadata_size_));
    RETURN_NOT_OK(
        input_data.init(chunk.filtered_data_, chunk.filtered_data_size_));

    // If the pipeline is empty, just copy input to output.
    if (filters_.empty()) {
      void* output_chunk_buffer =
          tile->data_as<char>() + chunk_data.chunk_offsets_[i];
      RETURN_NOT_OK(input_data.copy_to(output_chunk_buffer));
      continue;
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

      throw_if_not_ok(output_data.clear());
      throw_if_not_ok(output_metadata.clear());

      // Final filter: output directly into the shared output buffer.
      bool last_filter = filter_idx == 0;
      if (last_filter) {
        void* output_chunk_buffer =
            tile->data_as<char>() + chunk_data.chunk_offsets_[i];
        RETURN_NOT_OK(output_data.set_fixed_allocation(
            output_chunk_buffer, chunk.unfiltered_data_size_));
        reader_stats->add_counter(
            "read_unfiltered_byte_num", chunk.unfiltered_data_size_);
      }

      f->init_decompression_resource_pool(concurrency_level);

      RETURN_NOT_OK(f->run_reverse(
          *tile,
          offsets_tile,
          &input_metadata,
          &input_data,
          &output_metadata,
          &output_data,
          config));

      input_data.set_read_only(false);
      input_metadata.set_read_only(false);

      if (!last_filter) {
        throw_if_not_ok(input_data.swap(output_data));
        throw_if_not_ok(input_metadata.swap(output_metadata));
        // Next input (input_buffers) now stores this output (output_buffers).
      }
    }
  }

  return Status::Ok();
}

// ===== FORMAT =====
// max_chunk_size (uint32_t)
// num_filters (uint32_t)
// filter0 metadata (see Filter::serialize)
// filter1...
void FilterPipeline::serialize(Serializer& serializer) const {
  serializer.write<uint32_t>(max_chunk_size_);
  auto num_filters = static_cast<uint32_t>(filters_.size());
  serializer.write<uint32_t>(num_filters);

  for (const auto& f : filters_) {
    // For compatibility with the old attribute compressor API: if the filter
    // is a compression filter but with no compression, serialize the filter
    // as a no-op filter instead.
    auto as_compression = dynamic_cast<CompressionFilter*>(f.get());
    if (as_compression != nullptr && f->type() == FilterType::FILTER_NONE) {
      auto noop =
          tdb_unique_ptr<NoopFilter>(tdb_new(NoopFilter, Datatype::ANY));
      noop->serialize(serializer);
    } else {
      f->serialize(serializer);
    }
  }
}

FilterPipeline FilterPipeline::deserialize(
    Deserializer& deserializer, const uint32_t version, Datatype datatype) {
  auto max_chunk_size = deserializer.read<uint32_t>();
  auto num_filters = deserializer.read<uint32_t>();
  std::vector<shared_ptr<Filter>> filters;

  for (uint32_t i = 0; i < num_filters; i++) {
    auto filter{FilterCreate::deserialize(deserializer, version, datatype)};
    datatype = filter->output_datatype(datatype);
    filters.push_back(std::move(filter));
  }

  return FilterPipeline(max_chunk_size, filters);
}

void FilterPipeline::ensure_compatible(
    const Filter& first, const Filter& second, Datatype first_input_type) {
  auto first_output_type = first.output_datatype(first_input_type);
  if (!second.accepts_input_datatype(first_output_type)) {
    throw FilterPipelineStatusException(
        "Filter " + filter_type_str(first.type()) + " produces " +
        datatype_str(first_output_type) + " but second filter " +
        filter_type_str(second.type()) + " does not accept this type.");
  }
}

bool FilterPipeline::has_filter(const FilterType& filter_type) const {
  for (auto& f : filters_) {
    if (f->type() == filter_type)
      return true;
  }
  return false;
}

std::vector<shared_ptr<Filter>> FilterPipeline::filters() const {
  return filters_;
}

void FilterPipeline::set_max_chunk_size(uint32_t max_chunk_size) {
  max_chunk_size_ = max_chunk_size;
}

unsigned FilterPipeline::size() const {
  return static_cast<unsigned>(filters_.size());
}

bool FilterPipeline::empty() const {
  return filters_.empty();
}

void FilterPipeline::swap(FilterPipeline& other) {
  filters_.swap(other.filters_);
  std::swap(max_chunk_size_, other.max_chunk_size_);
}

Status FilterPipeline::append_encryption_filter(
    FilterPipeline* pipeline, const EncryptionKey& encryption_key) {
  switch (encryption_key.encryption_type()) {
    case EncryptionType::NO_ENCRYPTION:
      return Status::Ok();
    case EncryptionType::AES_256_GCM:
      pipeline->add_filter(
          EncryptionAES256GCMFilter(encryption_key, Datatype::ANY));
      return Status::Ok();
    default:
      return LOG_STATUS(Status_FilterError(
          "Error appending encryption filter; unknown type."));
  }
}

bool FilterPipeline::skip_offsets_filtering(
    const Datatype type, const uint32_t version) const {
  if (((version >= 12 && type == Datatype::STRING_ASCII) ||
       (version >= 17 && type == Datatype::STRING_UTF8)) &&
      has_filter(FilterType::FILTER_RLE)) {
    return true;
  } else if (
      ((version >= 13 && type == Datatype::STRING_ASCII) ||
       (version >= 17 && type == Datatype::STRING_UTF8)) &&
      has_filter(FilterType::FILTER_DICTIONARY)) {
    return true;
  }

  return false;
}

bool FilterPipeline::use_tile_chunking(
    const bool is_var, const uint32_t version, const Datatype type) const {
  if (max_chunk_size_ == 0) {
    return false;
  } else if (
      is_var &&
      (type == Datatype::STRING_ASCII || type == Datatype::STRING_UTF8)) {
    if (version >= 12 && has_filter(FilterType::FILTER_RLE)) {
      return false;
    } else if (version >= 13 && has_filter(FilterType::FILTER_DICTIONARY)) {
      return false;
    }
  } else if (has_filter(FilterType::FILTER_WEBP)) {
    return false;
  }

  return true;
}

}  // namespace sm
}  // namespace tiledb

std::ostream& operator<<(
    std::ostream& os, const tiledb::sm::FilterPipeline& fp) {
  for (const auto& filter : fp.filters()) {
    os << std::endl << "  > ";
    os << *filter;
  }
  return os;
}
