/**
 * @file   bitsort_filter.cc
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
 * This file implements the bitsort filter class.
 */

#include "tiledb/common/status.h"
#include "tiledb/common/types/untyped_datum.h"
#include "tiledb/sm/filter/bitsort_filter.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/filter/filter_buffer.h"
#include "tiledb/sm/filter/filter_storage.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/writers/domain_buffer.h"

#include <algorithm>
#include <cmath>
#include <functional>
#include <iostream> // TODO GET RID OF
#include <numeric>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include <stdio.h> // TODO REMOVE

using namespace tiledb::common;

namespace tiledb {
namespace sm {

template<typename T>
void print_tile(Tile *tile) {
  T *tile_data = static_cast<T*>(tile->data());
  size_t num_cells = tile->size_as<T>();
  for (size_t i = 0; i < num_cells; ++i) {
    std::cout << tile_data[i] << " ";
  }
  std::cout << "\n";
}

void print_tile(Tile *tile) {
  auto tile_type = tile->type();
  switch (tile_type) {
    case Datatype::INT8: {
      print_tile<int8_t>(tile);
    } break;
    case Datatype::INT16: {
      print_tile<int16_t>(tile);
    } break;
    case Datatype::INT32: {
      print_tile<int32_t>(tile);
    } break;
    case Datatype::INT64: {
      print_tile<int64_t>(tile);
    } break;
    case Datatype::FLOAT32: {
      print_tile<float>(tile);
    } break;
    case Datatype::FLOAT64: {
      print_tile<double>(tile);
    } break;
    default: {
      std::cout << "invalid tile type: " << datatype_str(tile_type) << "\n";
    } break;
  }
}

template<typename T>
void print_tile_filtered(Tile *tile) {
  FilteredBuffer &fb = tile->filtered_buffer();
  T *tile_data = reinterpret_cast<T*>(fb.data());
  size_t num_cells = fb.size() / sizeof(T);
  for (size_t i = 0; i < num_cells; ++i) {
    std::cout << tile_data[i] << " ";
  }
  std::cout << "\n";
}

void print_tile_filtered(Tile *tile) {
  auto tile_type = tile->type();
  switch (tile_type) {
    case Datatype::INT8: {
      print_tile_filtered<int8_t>(tile);
    } break;
    case Datatype::INT16: {
      print_tile_filtered<int16_t>(tile);
    } break;
    case Datatype::INT32: {
      print_tile_filtered<int32_t>(tile);
    } break;
    case Datatype::INT64: {
      print_tile_filtered<int64_t>(tile);
    } break;
    case Datatype::FLOAT32: {
      print_tile_filtered<float>(tile);
    } break;
    case Datatype::FLOAT64: {
      print_tile_filtered<double>(tile);
    } break;
    default: {
      std::cout << "invalid tile type: " << datatype_str(tile_type) << "\n";
    } break;
  }
}

void BitSortFilter::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;
  fprintf(out, "BitSortFilter");
}

Status BitSortFilter::run_forward(
    const Tile& tile,
    Tile* const tile_offsets,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  (void)tile;
  (void)tile_offsets;
  (void)input_metadata;
  (void)input;
  (void)output_metadata;
  (void)output;
  return Status_FilterError("BitSortFilter: Do not call (forward)");
}

Status BitSortFilter::run_reverse(
      const Tile& tile,
      Tile* const tile_offsets,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const {
  (void)tile;
  (void)tile_offsets;
  (void)input_metadata;
  (void)input;
  (void)output_metadata;
  (void)output;
  (void)config;
  return Status_FilterError("BitSortFilter: Do not call (reverse)");
}

/**
 * Run forward. TODO: COMMENT
 */
Status BitSortFilter::run_forward(
    const Tile& tile,
    BitSortFilterMetadataType &pair,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {

  auto tile_type = tile.type();
  switch (datatype_size(tile_type)) {
    case sizeof(uint8_t): {
      return run_forward<uint8_t>(
          pair, input_metadata, input, output_metadata, output);
    }
    case sizeof(uint16_t): {
      return run_forward<uint16_t>(
         pair, input_metadata, input, output_metadata, output);
    }
    case sizeof(uint32_t): {
      return run_forward<uint32_t>(
          pair, input_metadata, input, output_metadata, output);
    }
    case sizeof(uint64_t): {
      return run_forward<uint64_t>(
          pair, input_metadata, input, output_metadata, output);
    }
    default: {
      return Status_FilterError(
          "BitSortFilter::run_forward: datatype does not have an appropriate "
          "size");
    }
  } 

  return Status_FilterError("BitSortFilter::run_forward: invalid datatype.");
}

template <typename T>
Status BitSortFilter::run_forward(
    BitSortFilterMetadataType &pair,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  // Output size does not change with this filter.
  RETURN_NOT_OK(output->prepend_buffer(input->size()));
  Buffer* output_buf = output->buffer_ptr(0);
  assert(output_buf != nullptr);

  // Write the metadata
  auto parts = input->buffers();
  auto num_parts = (uint32_t)parts.size();
  uint32_t metadata_size = sizeof(uint32_t) + num_parts * sizeof(uint32_t);
  RETURN_NOT_OK(output_metadata->append_view(input_metadata));
  RETURN_NOT_OK(output_metadata->prepend_buffer(metadata_size));
  RETURN_NOT_OK(output_metadata->write(&num_parts, sizeof(uint32_t)));

  // Sort all parts
  std::vector<uint32_t> offsets;
  uint32_t total_size = 0;
  for (const auto& part : parts) {
    auto part_size = (uint32_t)part.size();
    offsets.push_back(total_size);
    total_size += part_size;
  }

  std::vector<std::pair<T, uint64_t>> sorted_elements(total_size / sizeof(T));
  for (uint64_t i = 0; i < num_parts; ++i) {
    const auto &part = parts[i];
    auto part_size = static_cast<uint32_t>(part.size());
    RETURN_NOT_OK(output_metadata->write(&part_size, sizeof(uint32_t)));
    RETURN_NOT_OK(sort_part<T>(&part, output_buf, offsets[i], sorted_elements));
  }

  // Rewrite each of the dimension tile data with the new sort order.
  std::vector<Tile*> &dim_tiles = pair.first.get();
  std::cout << "printing out dim_tiles in step 1\n";
  for (auto* dim_tile : dim_tiles) {
    print_tile(dim_tile);
  }


  for (auto* dim_tile : dim_tiles) {
    Datatype tile_type = dim_tile->type();
    // TODO: return_not_ok doesn't work here...
    switch (datatype_size(tile_type)) {
      case sizeof(uint8_t): {
        rewrite_dim_tile_forward<T, uint8_t>(sorted_elements, dim_tile);
      } break;
      case sizeof(uint16_t): {
        rewrite_dim_tile_forward<T, uint16_t>(sorted_elements, dim_tile);
      } break;
      case sizeof(uint32_t): {
        rewrite_dim_tile_forward<T, uint32_t>(sorted_elements, dim_tile);
      } break;
      case sizeof(uint64_t): {
        rewrite_dim_tile_forward<T, uint64_t>(sorted_elements, dim_tile);
      } break;
      default: {
        return Status_FilterError(
          "BitSortFilter::sort_part: dimension datatype does not have an appropriate "
          "size");
      }
    }
  }

  std::cout << "printing out dim_tiles in step 2\n";
  for (auto* dim_tile : dim_tiles) {
    print_tile(dim_tile);
  }

  return Status::Ok();
}

template <typename T>
Status BitSortFilter::sort_part(const ConstBuffer* input_buffer, Buffer* output_buffer, uint32_t start, std::vector<std::pair<T, uint64_t>> &sorted_elements) const {
  // Read in the data.
  uint32_t s = input_buffer->size();
  assert(s % sizeof(T) == 0);
  uint32_t num_elems_in_part = s / sizeof(T);

  if (num_elems_in_part == 0) {
    return Status::Ok();
  }

  const T* part_array = static_cast<const T*>(input_buffer->data());

  for (uint32_t i = 0; i < num_elems_in_part; ++i) {
    sorted_elements[start + i] = std::make_pair(part_array[i], start + i);
  }

  // Sort the data.
  std::sort(sorted_elements.begin() + start, sorted_elements.begin() + start + num_elems_in_part);

  // Write in the sorted order to output.
  for (uint32_t j = 0; j < num_elems_in_part; ++j) {
    T value = sorted_elements[start + j].first;
    RETURN_NOT_OK(output_buffer->write(&value, sizeof(T)));

    if (j != num_elems_in_part - 1) {
      output_buffer->advance_offset(sizeof(T));
    }
  }

  return Status::Ok();
}

template <typename T, typename W>
Status BitSortFilter::rewrite_dim_tile_forward(const std::vector<std::pair<T, uint64_t>> &elements, Tile *dim_tile) const {
  uint64_t elements_size = elements.size();
  std::vector<W> tile_data_vec(elements_size);
  W *tile_data = static_cast<W*>(dim_tile->data());

  for (uint64_t i = 0; i < elements_size; ++i) {
    tile_data_vec[i] = tile_data[elements[i].second];
  }

  // Overwrite the tile.
  RETURN_NOT_OK(dim_tile->write(tile_data_vec.data(), 0, sizeof(W) * elements_size));
  FilteredBuffer &filtered_buffer = dim_tile->filtered_buffer();
  filtered_buffer.expand(elements_size * sizeof(W));
  memcpy(filtered_buffer.data(), tile_data_vec.data(), sizeof(W) * elements_size);

  return Status::Ok();
}

/**
 * Run reverse. TODO: comment
 */
Status BitSortFilter::run_reverse(
    const Tile& tile,
    BitSortFilterMetadataType &pair,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config& config) const {

  (void)config;
  auto tile_type = tile.type();
  switch (datatype_size(tile_type)) {
    case sizeof(uint8_t): {
      return run_reverse<uint8_t>(
          pair, input_metadata, input, output_metadata, output);
    }
    case sizeof(uint16_t): {
      return run_reverse<uint16_t>(
          pair, input_metadata, input, output_metadata, output);
    }
    case sizeof(uint32_t): {
      return run_reverse<uint32_t>(
          pair, input_metadata, input, output_metadata, output);
    }
    case sizeof(uint64_t): {
      return run_reverse<uint64_t>(
          pair, input_metadata, input, output_metadata, output);
    }
    default: {
      return Status_FilterError(
          "BitSortFilter::run_reverse: datatype does not have an appropriate "
          "size");
    }
  }

  return Status_FilterError("BitSortFilter::run_reverse: invalid datatype.");
}

template <typename T>
Status BitSortFilter::run_reverse(
    BitSortFilterMetadataType &pair,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const  {
  // Get number of parts
  uint32_t num_parts;
  RETURN_NOT_OK(input_metadata->read(&num_parts, sizeof(uint32_t)));

  RETURN_NOT_OK(output->prepend_buffer(input->size()));
  Buffer* output_buf = output->buffer_ptr(0);
  assert(output_buf != nullptr);

  // Determine the positions vector.
  std::vector<Tile*> &dim_tiles = pair.first.get();
  std::cout << "printing out dim_tiles in step 3\n";
  for (auto* dim_tile : dim_tiles) {
    print_tile_filtered(dim_tile);
  }

  std::vector<uint64_t> positions;
  rewrite_dim_tiles_reverse(pair, positions);

  for (uint32_t i = 0; i < num_parts; i++) {
    uint32_t part_size;
    RETURN_NOT_OK(input_metadata->read(&part_size, sizeof(uint32_t)));
    ConstBuffer part(nullptr, 0);
    RETURN_NOT_OK(input->get_const_buffer(part_size, &part));

    RETURN_NOT_OK(unsort_part<T>(positions, &part, output_buf));

    if (output_buf->owns_data()) {
      output_buf->advance_size(part_size);
    }
    output_buf->advance_offset(part_size);
    input->advance_offset(part_size);
  }

  // Output metadata is a view on the input metadata, skipping what was used
  // by this filter.
  auto md_offset = input_metadata->offset();
  RETURN_NOT_OK(output_metadata->append_view(
      input_metadata, md_offset, input_metadata->size() - md_offset));

  std::cout << "printing out dim_tiles in step 4\n";
  for (auto* dim_tile : dim_tiles) {
    print_tile(dim_tile);
  }

  return Status::Ok();
}

template <typename T>
Status BitSortFilter::unsort_part(
    const std::vector<uint64_t> &positions, ConstBuffer* input_buffer, Buffer* output_buffer) const {
  // Read in data.
  uint32_t s = input_buffer->size();
  assert(s % sizeof(T) == 0);
  uint32_t num_elems_in_part = s / sizeof(T);

  if (num_elems_in_part == 0) {
    return Status::Ok();
  }

  const T* input_array = static_cast<const T*>(input_buffer->data());
  T* output_array = static_cast<T*>(output_buffer->cur_data());

  // Write in data in original order.
  for (uint32_t i = 0; i < num_elems_in_part; ++i) {
    output_array[i] = input_array[positions[i]];
  }

  return Status::Ok();
}

Status BitSortFilter::rewrite_dim_tiles_reverse(BitSortFilterMetadataType &pair, std::vector<uint64_t> &positions) const {
  std::vector<Tile*> &dim_tiles = pair.first.get();
  const Domain &domain = pair.second.get();

  std::vector<uint64_t> dim_data_sizes(domain.dim_num(), 0);
  std::unordered_map<std::string, QueryBuffer> dim_data_map;
  for (size_t i = 0; i < domain.dim_num(); ++i) {
    const std::string &dim_name = domain.dimension_ptr(i)->name();
    auto &filtered_buffer = dim_tiles[i]->filtered_buffer();
    dim_data_sizes[i] = filtered_buffer.size();
    QueryBuffer dim_query_buffer{filtered_buffer.data(), nullptr, &dim_data_sizes[i], nullptr};
    dim_data_map.insert(std::make_pair(dim_name, dim_query_buffer));
  }

  DomainBuffersView domain_buffs{domain, dim_data_map};

  /// TODO: determine if this needs to take the query layout into account
  auto cmp_fn = [&domain_buffs, &domain](const uint64_t &a_idx, const uint64_t &b_idx){
    auto a{domain_buffs.domain_ref_at(domain, a_idx)};
    auto b{domain_buffs.domain_ref_at(domain, b_idx)};
    auto tile_cmp = domain.tile_order_cmp(a, b);

    if (tile_cmp == -1) return true;
    else if (tile_cmp == 1) return false;

    auto cell_cmp = domain.cell_order_cmp(a, b);
    return cell_cmp == -1;
  };

  for (uint64_t i = 0; i < dim_tiles[0]->cell_num(); ++i) {
    positions.push_back(i);
  }

  std::sort(positions.begin(), positions.end(), cmp_fn);

  std::cout << "printing out positions...\n";
  for (uint64_t i = 0; i < positions.size(); ++i) {
    std::cout << positions[i] << " ";
  }
  std::cout << std::endl;

  // Rewrite the individual tiles with the position vector.
  for (auto *dim_tile : dim_tiles) {
    Datatype tile_type = dim_tile->type();
    // TODO: return_not_ok doesn't work here...
    switch (datatype_size(tile_type)) {
      case sizeof(uint8_t): {
        rewrite_dim_tile_reverse<uint8_t>(dim_tile, positions);
      } break;
      case sizeof(uint16_t): {
        rewrite_dim_tile_reverse<uint16_t>(dim_tile, positions);
      } break;
      case sizeof(uint32_t): {
        rewrite_dim_tile_reverse<uint32_t>(dim_tile, positions);
      } break;
      case sizeof(uint64_t): {
        rewrite_dim_tile_reverse<uint64_t>(dim_tile, positions);
      } break;
      default: {
        return Status_FilterError(
          "BitSortFilter::rewrite_dim_tiles_reverse: dimension datatype does not have an appropriate "
          "size");
      }
    }
  }

  return Status::Ok();
}

template<typename T>
Status BitSortFilter::rewrite_dim_tile_reverse(Tile *dim_tile, std::vector<uint64_t> &positions) const {
  uint64_t positions_size = positions.size();
  std::vector<T> tile_data_vec(positions_size);
  FilteredBuffer &filtered_buffer = dim_tile->filtered_buffer();
  T *tile_data = reinterpret_cast<T*>(filtered_buffer.data());

  for (uint64_t i = 0; i < positions_size; ++i) {
    tile_data_vec[i] = tile_data[positions[i]];
  }

  // Overwrite the tile.
  filtered_buffer.expand(positions_size * sizeof(T));
  memcpy(dim_tile->data(), tile_data_vec.data(), sizeof(T) * positions_size);

  return Status::Ok();
}

BitSortFilter* BitSortFilter::clone_impl() const {
  return tdb_new(BitSortFilter);
}

}  // namespace sm
}  // namespace tiledb