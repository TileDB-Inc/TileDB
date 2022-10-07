/**
 * @file bitsort_filter.cc
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
#include <iostream> // TODO GONE
#include <numeric>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

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
  return Status_FilterError("BitSortFilter: Do not call default version of run_forward.");
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
  return Status_FilterError("BitSortFilter: Do not call default version of of run_reverse.");
}

Status BitSortFilter::run_forward(
    const Tile& tile,
    std::vector<Tile*> &dim_tiles,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {

  // Since run_forward interprets the filter's data as integers, we case on
  // the size of the type and pass in the corresponding integer type into
  // a templated function.
  auto tile_type = tile.type();
  switch (datatype_size(tile_type)) {
    case sizeof(uint8_t): {
      return run_forward<uint8_t>(
          dim_tiles, input_metadata, input, output_metadata, output);
    }
    case sizeof(uint16_t): {
      return run_forward<uint16_t>(
         dim_tiles, input_metadata, input, output_metadata, output);
    }
    case sizeof(uint32_t): {
      return run_forward<uint32_t>(
          dim_tiles, input_metadata, input, output_metadata, output);
    }
    case sizeof(uint64_t): {
      return run_forward<uint64_t>(
          dim_tiles, input_metadata, input, output_metadata, output);
    }
    default: {
      return Status_FilterError(
          "BitSortFilter::run_forward: datatype does not have an appropriate "
          "size");
    }
  }
}

template <typename AttrType>
Status BitSortFilter::run_forward(
    std::vector<Tile*> &dim_tiles,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  // Output size does not change with this filter.
  RETURN_NOT_OK(output->prepend_buffer(input->size()));
  Buffer* output_buf = output->buffer_ptr(0);
  assert(output_buf != nullptr);

  // Write the metadata.
  auto parts = input->buffers();
  auto num_parts = (uint32_t)parts.size();
  uint32_t metadata_size = sizeof(uint32_t) + num_parts * sizeof(uint32_t);
  RETURN_NOT_OK(output_metadata->append_view(input_metadata));
  RETURN_NOT_OK(output_metadata->prepend_buffer(metadata_size));
  RETURN_NOT_OK(output_metadata->write(&num_parts, sizeof(uint32_t)));

  // Create a vector to store the attribute data with their respective positions.
  // Then for each part of the input, collect this data into the sorted_elements vector.
  std::vector<std::pair<AttrType, uint64_t>> sorted_elements;
  uint32_t total_size = 0;
  for (uint64_t i = 0; i < num_parts; ++i) {
    const auto &part = parts[i];
    auto part_size = static_cast<uint32_t>(part.size());
    RETURN_NOT_OK(output_metadata->write(&part_size, sizeof(uint32_t)));
    RETURN_NOT_OK(collect_part_data<AttrType>(&part, (total_size / sizeof(AttrType)), sorted_elements));
    total_size += part_size;
  }

  // Sort all the attribute tile data by bit order, which is equivalent to sorting an
  // unsigned integer type in ascending order.
  std::sort(sorted_elements.begin(), sorted_elements.end());

  // Write in the sorted order to output.
  uint32_t num_elements = total_size / sizeof(AttrType);
  for (uint32_t j = 0; j < num_elements; ++j) {
    AttrType value = sorted_elements[j].first;
    RETURN_NOT_OK(output_buf->write(&value, sizeof(AttrType)));

    if (j != num_elements - 1) {
      output_buf->advance_offset(sizeof(AttrType));
    }
  }

  // Since run_forward_dim_tile is only moving around dimension data and 
  // reordering it, the type becomes irrelevant and only the size of the type
  // matters, so the code cases on the size of the type and passes in the
  // corresponding integer type into a templated function.
  for (auto* dim_tile : dim_tiles) {
    Datatype tile_type = dim_tile->type();
    switch (datatype_size(tile_type)) {
      case sizeof(uint8_t): {
        run_forward_dim_tile<AttrType, uint8_t>(sorted_elements, dim_tile);
      } break;
      case sizeof(uint16_t): {
        run_forward_dim_tile<AttrType, uint16_t>(sorted_elements, dim_tile);
      } break;
      case sizeof(uint32_t): {
        run_forward_dim_tile<AttrType, uint32_t>(sorted_elements, dim_tile);
      } break;
      case sizeof(uint64_t): {
        run_forward_dim_tile<AttrType, uint64_t>(sorted_elements, dim_tile);
      } break;
      default: {
        return Status_FilterError(
          "BitSortFilter::sort_part: dimension datatype does not have an appropriate "
          "size");
      }
    }
  }

  return Status::Ok();
}

template <typename AttrType>
Status BitSortFilter::collect_part_data(const ConstBuffer* input_buffer, uint32_t offset, std::vector<std::pair<AttrType, uint64_t>> &sorted_elements) const {
  // Read in the data.
  uint32_t buff_size = input_buffer->size();
  assert(buff_size % sizeof(AttrType) == 0);
  uint32_t num_elems_in_part = buff_size / sizeof(AttrType);

  if (num_elems_in_part == 0) {
    return Status::Ok();
  }

  // Store the elements and their respective positions in the entire array in the sorted_elements vector.
  sorted_elements.reserve(offset + num_elems_in_part);
  const AttrType* input_data = static_cast<const AttrType*>(input_buffer->data());
  for (uint32_t i = 0; i < num_elems_in_part; ++i) {
    sorted_elements.emplace_back(std::make_pair(input_data[i], offset + i));
  }

  return Status::Ok();
}

template <typename AttrType, typename DimType>
Status BitSortFilter::run_forward_dim_tile(const std::vector<std::pair<AttrType, uint64_t>> &positions, Tile *dim_tile) const {
  // Obtain the pointer to the data the code modifies.
  uint64_t positions_size = positions.size();
  DimType *tile_data = static_cast<DimType*>(dim_tile->data());

  // Obtain the pointer to the filtered data buffer.
  FilteredBuffer &filtered_buffer = dim_tile->filtered_buffer();
  filtered_buffer.expand(positions_size * sizeof(DimType));
  DimType *filtered_buffer_data = static_cast<DimType*>((void*)filtered_buffer.data());

  // Keep track of the data we should write to the tile.
  for (uint64_t i = 0; i < positions_size; ++i) {
    filtered_buffer_data[i] = tile_data[positions[i].second];
  }

  return Status::Ok();
}

Status BitSortFilter::run_reverse(
    const Tile& tile,
    BitSortFilterMetadataType& bitsort_metadata,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config& config) const {
  (void)config;

  // Since run_reverse interprets the filter's data as integers, we case on
  // the size of the type and pass in the corresponding integer type into
  // a templated function.
  auto tile_type = tile.type();
  switch (datatype_size(tile_type)) {
    case sizeof(uint8_t): {
      return run_reverse<uint8_t>(
          bitsort_metadata, input_metadata, input, output_metadata, output);
    }
    case sizeof(uint16_t): {
      return run_reverse<uint16_t>(
          bitsort_metadata, input_metadata, input, output_metadata, output);
    }
    case sizeof(uint32_t): {
      return run_reverse<uint32_t>(
          bitsort_metadata, input_metadata, input, output_metadata, output);
    }
    case sizeof(uint64_t): {
      return run_reverse<uint64_t>(
          bitsort_metadata, input_metadata, input, output_metadata, output);
    }
    default: {
      return Status_FilterError(
          "BitSortFilter::run_reverse: datatype does not have an appropriate "
          "size");
    }
  }
}

template <typename AttrType>
Status BitSortFilter::run_reverse(
    BitSortFilterMetadataType &bitsort_metadata,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const  {

  std::vector<Tile*> &dim_tiles = bitsort_metadata.first.get();
  for (auto *tile : dim_tiles) {
    print_tile_filtered(tile);
  }

  // Get number of parts.
  uint32_t num_parts;
  RETURN_NOT_OK(input_metadata->read(&num_parts, sizeof(uint32_t)));

  RETURN_NOT_OK(output->prepend_buffer(input->size()));
  Buffer* output_buf = output->buffer_ptr(0);
  assert(output_buf != nullptr);

  // Determine the positions vector and rewrite the dimension tiles using these positions.
  std::vector<uint64_t> positions;
  run_reverse_dim_tiles(bitsort_metadata, positions);

  // Unsort the attribute data with the positions determined by the dimension global sorting.
  std::cout << "printing unsorted attr data\n";
  for (uint32_t i = 0; i < num_parts; i++) {
    uint32_t part_size;
    RETURN_NOT_OK(input_metadata->read(&part_size, sizeof(uint32_t)));
    ConstBuffer part(nullptr, 0);
    RETURN_NOT_OK(input->get_const_buffer(part_size, &part));

    RETURN_NOT_OK(unsort_part<AttrType>(positions, &part, output_buf));

    if (output_buf->owns_data()) {
      output_buf->advance_size(part_size);
    }
    output_buf->advance_offset(part_size);
    input->advance_offset(part_size);
  }
  std::cout << std::endl;

  // Output metadata is a view on the input metadata, skipping what was used
  // by this filter.
  auto md_offset = input_metadata->offset();
  RETURN_NOT_OK(output_metadata->append_view(
      input_metadata, md_offset, input_metadata->size() - md_offset));

  return Status::Ok();
}

template <typename AttrType>
Status BitSortFilter::unsort_part(
    const std::vector<uint64_t> &positions, ConstBuffer* input_buffer, Buffer* output_buffer) const {
  // Read in data.
  uint32_t buff_size = input_buffer->size();
  assert(buff_size % sizeof(AttrType) == 0);
  uint32_t num_elems_in_part = buff_size / sizeof(AttrType);

  if (num_elems_in_part == 0) {
    return Status::Ok();
  }

  const AttrType* input_array = static_cast<const AttrType*>(input_buffer->data());
  AttrType* output_array = static_cast<AttrType*>(output_buffer->cur_data());

  // Write in data in original order.
  for (uint32_t i = 0; i < num_elems_in_part; ++i) {
    output_array[i] = input_array[positions[i]];
    std::cout << output_array[i] << " ";
  }

  return Status::Ok();
}

Status BitSortFilter::run_reverse_dim_tiles(BitSortFilterMetadataType &bitsort_metadata, std::vector<uint64_t> &positions) const {
  std::vector<Tile*> &dim_tiles = bitsort_metadata.first.get();
  auto cmp_fn = bitsort_metadata.second;

  for (uint64_t i = 0; i < dim_tiles[0]->cell_num(); ++i) {
    positions.push_back(i);
  }

  // Sort the dimension data and determine the positions of the dimension tile data.
  std::sort(positions.begin(), positions.end(), cmp_fn);

  // Rewrite the individual tiles with the position vector.
  for (auto *dim_tile : dim_tiles) {
  // Since run_reverse_dim_tile is only moving around dimension data and 
  // reordering it, the type becomes irrelevant and only the size of the type
  // matters, so the code cases on the size of the type and passes in the
  // corresponding integer type into a templated function.
    Datatype tile_type = dim_tile->type();
    switch (datatype_size(tile_type)) {
      case sizeof(uint8_t): {
        run_reverse_dim_tile<uint8_t>(dim_tile, positions);
      } break;
      case sizeof(uint16_t): {
        run_reverse_dim_tile<uint16_t>(dim_tile, positions);
      } break;
      case sizeof(uint32_t): {
        run_reverse_dim_tile<uint32_t>(dim_tile, positions);
      } break;
      case sizeof(uint64_t): {
        run_reverse_dim_tile<uint64_t>(dim_tile, positions);
      } break;
      default: {
        return Status_FilterError(
          "BitSortFilter::run_reverse_dim_tiles: dimension datatype does not have an appropriate "
          "size");
      }
    }
  }

  return Status::Ok();
}

template<typename DimType>
Status BitSortFilter::run_reverse_dim_tile(Tile *dim_tile, std::vector<uint64_t> &positions) const {
  // Obtain the pointer to the data the code modifies.
  uint64_t positions_size = positions.size();
  std::vector<DimType> tile_data_vec(positions_size);
  FilteredBuffer &filtered_buffer = dim_tile->filtered_buffer();
  DimType *tile_data = reinterpret_cast<DimType*>(filtered_buffer.data());

  // Keep track of the data we should write to the tile.
  for (uint64_t i = 0; i < positions_size; ++i) {
    tile_data_vec[i] = tile_data[positions[i]];
  }

  // Overwrite the tile.
  memcpy(dim_tile->data(), tile_data_vec.data(), sizeof(DimType) * positions_size);

  return Status::Ok();
}

BitSortFilter* BitSortFilter::clone_impl() const {
  return tdb_new(BitSortFilter);
}

}  // namespace sm
}  // namespace tiledb