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

#include <algorithm>
#include <cmath>
#include <functional>
#include <iostream>
#include <numeric>
#include <optional>
#include <utility>
#include <vector>

#include <stdio.h> // TODO REMOVE

using namespace tiledb::common;

namespace tiledb {
namespace sm {

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
    case sizeof(int8_t): {
      return run_forward<int8_t>(
          pair, input_metadata, input, output_metadata, output);
    }
    case sizeof(int16_t): {
      return run_forward<int16_t>(
         pair, input_metadata, input, output_metadata, output);
    }
    case sizeof(int32_t): {
      return run_forward<int32_t>(
          pair, input_metadata, input, output_metadata, output);
    }
    case sizeof(int64_t): {
      return run_forward<int64_t>(
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

  std::vector<std::pair<T, uint64_t>> sorted_elements(total_size);
  for (uint64_t i = 0; i < num_parts; ++i) {
    const auto &part = parts[i];
    auto part_size = static_cast<uint32_t>(part.size());
    RETURN_NOT_OK(output_metadata->write(&part_size, sizeof(uint32_t)));
    RETURN_NOT_OK(sort_part<T>(&part, output_buf, offsets[i], sorted_elements));
  }

  // Rewrite each of the dimension tile data with the new sort order.
  std::vector<Tile*> &dim_tiles = pair.first.get();
  for (auto* dim_tile : dim_tiles) {
    Datatype tile_type = dim_tile->type();
    // TODO: return_not_ok doesn't work here...
    switch (datatype_size(tile_type)) {
      case sizeof(int8_t): {
        rewrite_dim_tile_forward<T, int8_t>(sorted_elements, dim_tile);
      } break;
      case sizeof(int16_t): {
        rewrite_dim_tile_forward<T, int16_t>(sorted_elements, dim_tile);
      } break;
      case sizeof(int32_t): {
        rewrite_dim_tile_forward<T, int32_t>(sorted_elements, dim_tile);
      } break;
      case sizeof(int64_t): {
        rewrite_dim_tile_forward<T, int64_t>(sorted_elements, dim_tile);
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
    case sizeof(int8_t): {
      return run_reverse<int8_t>(
          pair, input_metadata, input, output_metadata, output);
    }
    case sizeof(int16_t): {
      return run_reverse<int16_t>(
          pair, input_metadata, input, output_metadata, output);
    }
    case sizeof(int32_t): {
      return run_reverse<int32_t>(
          pair, input_metadata, input, output_metadata, output);
    }
    case sizeof(int64_t): {
      return run_reverse<int64_t>(
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
  
  std::vector<Tile*> &dim_tiles = pair.first.get();

  // Get number of parts
  uint32_t num_parts;
  RETURN_NOT_OK(input_metadata->read(&num_parts, sizeof(uint32_t)));

  RETURN_NOT_OK(output->prepend_buffer(input->size()));
  Buffer* output_buf = output->buffer_ptr(0);
  assert(output_buf != nullptr);

  // Get the domain.
  const Domain &domain = pair.second.get();

  // Determine the positions vector.
  std::vector<uint64_t> positions;
  for (uint64_t i = 0; i < dim_tiles.size(); ++i) {
    if (i == 0) {
      auto positions_ref = std::ref(positions);
      rewrite_dim_tile_reverse(dim_tiles[i], i, domain, positions_ref);
    } else {
      rewrite_dim_tile_reverse(dim_tiles[i], i, domain, std::nullopt);
    }
  }

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
    output_array[positions[i]] = input_array[i];
  }

  return Status::Ok();
}

Status BitSortFilter::rewrite_dim_tile_reverse(Tile *dim_tile, uint64_t i, const Domain &domain, std::optional<std::reference_wrapper<std::vector<uint64_t>>> positions_opt) const {
  Datatype tile_type = dim_tile->type();
  switch (tile_type) {
    case Datatype::INT8: {
      rewrite_dim_tile_reverse<int8_t>(dim_tile, i, domain, positions_opt);
    } break;
    case Datatype::INT16: {
      rewrite_dim_tile_reverse<int16_t>(dim_tile, i, domain, positions_opt);
    } break;
    case Datatype::INT32: {
      rewrite_dim_tile_reverse<int32_t>(dim_tile, i, domain, positions_opt);
    } break;
    case Datatype::INT64: {
      rewrite_dim_tile_reverse<int64_t>(dim_tile, i, domain, positions_opt);
    } break;
    case Datatype::FLOAT32: {
      rewrite_dim_tile_reverse<float>(dim_tile, i, domain, positions_opt);
    } break;
    case Datatype::FLOAT64: {
      rewrite_dim_tile_reverse<double>(dim_tile, i, domain, positions_opt);
    } break;
    default: {
      return Status_FilterError("BitSortFilter::unsort_part: unsupported dimension type.");
    } break;
  }

  return Status::Ok();
}

template<typename T>
Status BitSortFilter::rewrite_dim_tile_reverse(Tile *dim_tile, uint64_t i, const Domain &domain, std::optional<std::reference_wrapper<std::vector<uint64_t>>> positions_opt) const {
  std::vector<std::pair<T, uint32_t>> dimension_vector;

  T* dim_tile_data = static_cast<T*>(dim_tile->data());
  for (uint32_t i = 0; i < dim_tile->size_as<T>(); ++i) {
    dimension_vector.push_back(std::make_pair(dim_tile_data[i], i));
  }

  auto comparison_function = [&domain, &i](const std::pair<T, uint32_t> &a, const std::pair<T, uint32_t> &b) {
        UntypedDatumView a_datum(&a.first, sizeof(T));
        UntypedDatumView b_datum(&b.first, sizeof(T));
        
        int result = domain.cell_order_cmp(i, a_datum, b_datum);
        if (result <= 0) return false;
        return true;
    };

  std::sort(dimension_vector.begin(), dimension_vector.end(), comparison_function);

  if (positions_opt) {
    std::vector<uint64_t> &positions = positions_opt.value().get();
    for (uint32_t i = 0; i < dim_tile->size_as<T>(); ++i) {
      positions.push_back(dimension_vector[i].second);
    }
  }

  return Status::Ok();
}

BitSortFilter* BitSortFilter::clone_impl() const {
  return tdb_new(BitSortFilter);
}

}  // namespace sm
}  // namespace tiledb