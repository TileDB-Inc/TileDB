/**
 * @file   tile_metadata_generator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 WriterTileDB, Inc.
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
 * This file implements class TileMetadataGenerator.
 */

#include "tiledb/sm/tile/tile_metadata_generator.h"
#include "tiledb/common/assert.h"
#include "tiledb/sm/tile/writer_tile_tuple.h"

using namespace tiledb::common;

namespace tiledb::sm {

/* ****************************** */
/*    STRUCTURED BINDINGS APIS    */
/* ****************************** */

template <typename T>
void Sum<T, int64_t>::sum(
    const WriterTile& tile, uint64_t start, uint64_t end, ByteVec& sum) {
  // Get pointers to the data and cell num.
  auto values = tile.data_as<T>();
  auto sum_data = reinterpret_cast<int64_t*>(sum.data());

  // Process cell by cell, swallowing overflow exception.
  for (uint64_t c = start; c < end; c++) {
    auto value = static_cast<int64_t>(values[c]);
    if (*sum_data > 0 && value > 0 &&
        (*sum_data > std::numeric_limits<int64_t>::max() - value)) {
      *sum_data = std::numeric_limits<int64_t>::max();
      break;
    }

    if (*sum_data < 0 && value < 0 &&
        (*sum_data < std::numeric_limits<int64_t>::min() - value)) {
      *sum_data = std::numeric_limits<int64_t>::min();
      break;
    }

    *sum_data += value;
  }
}

template <typename T>
void Sum<T, uint64_t>::sum(
    const WriterTile& tile, uint64_t start, uint64_t end, ByteVec& sum) {
  // Get pointers to the data and cell num.
  auto values = tile.data_as<T>();
  auto sum_data = reinterpret_cast<uint64_t*>(sum.data());

  // Process cell by cell, swallowing overflow exception.
  for (uint64_t c = start; c < end; c++) {
    auto value = static_cast<uint64_t>(values[c]);
    if (*sum_data > std::numeric_limits<uint64_t>::max() - value) {
      *sum_data = std::numeric_limits<uint64_t>::max();
      break;
    }

    *sum_data += value;
  }
}

template <typename T>
void Sum<T, double>::sum(
    const WriterTile& tile, uint64_t start, uint64_t end, ByteVec& sum) {
  // Get pointers to the data and cell num.
  auto values = tile.data_as<T>();
  auto sum_data = reinterpret_cast<double*>(sum.data());

  // Process cell by cell, swallowing overflow exception.
  for (uint64_t c = start; c < end; c++) {
    auto value = static_cast<double>(values[c]);
    if ((*sum_data < 0.0) == (value < 0.0) &&
        std::abs(*sum_data) >
            std::numeric_limits<double>::max() - std::abs(value)) {
      *sum_data = *sum_data < 0.0 ? std::numeric_limits<double>::lowest() :
                                    std::numeric_limits<double>::max();
      break;
    }

    *sum_data += value;
  }
}

template <typename T>
void Sum<T, int64_t>::sum_nullable(
    const WriterTile& tile,
    const WriterTile& validity_tile,
    uint64_t start,
    uint64_t end,
    ByteVec& sum) {
  // Get pointers to the data and cell num.
  auto values = tile.data_as<T>();
  auto validity_values = validity_tile.data_as<uint8_t>();
  auto sum_data = reinterpret_cast<int64_t*>(sum.data());

  // Process cell by cell, swallowing overflow exception.
  for (uint64_t c = start; c < end; c++) {
    if (validity_values[c] != 0) {
      auto value = static_cast<int64_t>(values[c]);
      if (*sum_data > 0 && value > 0 &&
          (*sum_data > std::numeric_limits<int64_t>::max() - value)) {
        *sum_data = std::numeric_limits<int64_t>::max();
        break;
      }

      if (*sum_data < 0 && value < 0 &&
          (*sum_data < std::numeric_limits<int64_t>::min() - value)) {
        *sum_data = std::numeric_limits<int64_t>::min();
        break;
      }

      *sum_data += value;
    }
  }
}

template <typename T>
void Sum<T, uint64_t>::sum_nullable(
    const WriterTile& tile,
    const WriterTile& validity_tile,
    uint64_t start,
    uint64_t end,
    ByteVec& sum) {
  // Get pointers to the data and cell num.
  auto values = tile.data_as<T>();
  auto validity_values = validity_tile.data_as<uint8_t>();
  auto sum_data = reinterpret_cast<uint64_t*>(sum.data());

  // Process cell by cell, swallowing overflow exception.
  for (uint64_t c = start; c < end; c++) {
    if (validity_values[c] != 0) {
      auto value = static_cast<uint64_t>(values[c]);
      if (*sum_data > std::numeric_limits<uint64_t>::max() - value) {
        *sum_data = std::numeric_limits<uint64_t>::max();
        break;
      }

      *sum_data += value;
    }
  }
}

template <typename T>
void Sum<T, double>::sum_nullable(
    const WriterTile& tile,
    const WriterTile& validity_tile,
    uint64_t start,
    uint64_t end,
    ByteVec& sum) {
  // Get pointers to the data and cell num.
  auto values = tile.data_as<T>();
  auto validity_values = validity_tile.data_as<uint8_t>();
  auto sum_data = reinterpret_cast<double*>(sum.data());

  // Process cell by cell, swallowing overflow exception.
  for (uint64_t c = start; c < end; c++) {
    if (validity_values[c] != 0) {
      auto value = static_cast<double>(values[c]);
      if ((*sum_data < 0.0) == (value < 0.0) &&
          std::abs(*sum_data) >
              std::numeric_limits<double>::max() - std::abs(value)) {
        *sum_data = *sum_data < 0.0 ? std::numeric_limits<double>::lowest() :
                                      std::numeric_limits<double>::max();
        break;
      }

      *sum_data += value;
    }
  }
}

/* ****************************** */
/*           STATIC API           */
/* ****************************** */

bool TileMetadataGenerator::has_min_max_metadata(
    const Datatype type,
    const bool is_dim,
    const bool var_size,
    const uint64_t cell_val_num) {
  // No mix max for dims, we have rtrees.
  if (is_dim) {
    return false;
  }

  // No min max for var size data other than strings.
  if (var_size && type != Datatype::CHAR && type != Datatype::STRING_ASCII) {
    return false;
  }

  // No min max for fixed cells with more than one value other than strings.
  if (cell_val_num != 1 && type != Datatype::CHAR &&
      type != Datatype::STRING_ASCII) {
    return false;
  }

  // No min max for any, byte-type, or non ascii strings.
  switch (type) {
    case Datatype::ANY:
    case Datatype::BLOB:
    case Datatype::GEOM_WKB:
    case Datatype::GEOM_WKT:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
      return false;

    default:
      return true;
  }
}

bool TileMetadataGenerator::has_sum_metadata(
    const Datatype type, const bool var_size, const uint64_t cell_val_num) {
  // No sum for var sized attributes or cells with more than one value.
  if (var_size || cell_val_num != 1)
    return false;

  // No sum for any, byte-type, or non ascii strings.
  switch (type) {
    case Datatype::ANY:
    case Datatype::BLOB:
    case Datatype::GEOM_WKB:
    case Datatype::GEOM_WKT:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::STRING_ASCII:
      return false;

    default:
      return true;
  }
}

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

TileMetadataGenerator::TileMetadataGenerator(
    const Datatype type,
    const bool is_dim,
    const bool var_size,
    const uint64_t cell_size,
    const uint64_t cell_val_num)
    : is_dim_(is_dim)
    , var_size_(var_size)
    , type_(type)
    , min_(nullptr)
    , min_size_(0)
    , max_(nullptr)
    , max_size_(0)
    , global_order_min_(nullptr)
    , global_order_min_size_(0)
    , global_order_max_(nullptr)
    , global_order_max_size_(0)
    , sum_(sizeof(uint64_t))
    , null_count_(0)
    , cell_size_(cell_size)
    , has_min_max_(has_min_max_metadata(type, is_dim, var_size, cell_val_num))
    , has_sum_(has_sum_metadata(type, var_size, cell_val_num)) {
}

/* ****************************** */
/*               API              */
/* ****************************** */

void TileMetadataGenerator::process_full_tile(const WriterTileTuple& tile) {
  uint64_t cell_num = tile.cell_num();
  process_cell_slab(tile, 0, cell_num);
}

void TileMetadataGenerator::process_cell_slab(
    const WriterTileTuple& tile, uint64_t start, uint64_t end) {
  if (!var_size_) {
    // Switch depending on datatype.
    switch (type_) {
      case Datatype::INT8:
        process_cell_range<int8_t>(tile, start, end);
        break;
      case Datatype::INT16:
        process_cell_range<int16_t>(tile, start, end);
        break;
      case Datatype::INT32:
        process_cell_range<int32_t>(tile, start, end);
        break;
      case Datatype::INT64:
        process_cell_range<int64_t>(tile, start, end);
        break;
      case Datatype::BOOL:
      case Datatype::UINT8:
        process_cell_range<uint8_t>(tile, start, end);
        break;
      case Datatype::UINT16:
        process_cell_range<uint16_t>(tile, start, end);
        break;
      case Datatype::UINT32:
        process_cell_range<uint32_t>(tile, start, end);
        break;
      case Datatype::UINT64:
        process_cell_range<uint64_t>(tile, start, end);
        break;
      case Datatype::FLOAT32:
        process_cell_range<float>(tile, start, end);
        break;
      case Datatype::FLOAT64:
        process_cell_range<double>(tile, start, end);
        break;
      case Datatype::DATETIME_YEAR:
      case Datatype::DATETIME_MONTH:
      case Datatype::DATETIME_WEEK:
      case Datatype::DATETIME_DAY:
      case Datatype::DATETIME_HR:
      case Datatype::DATETIME_MIN:
      case Datatype::DATETIME_SEC:
      case Datatype::DATETIME_MS:
      case Datatype::DATETIME_US:
      case Datatype::DATETIME_NS:
      case Datatype::DATETIME_PS:
      case Datatype::DATETIME_FS:
      case Datatype::DATETIME_AS:
      case Datatype::TIME_HR:
      case Datatype::TIME_MIN:
      case Datatype::TIME_SEC:
      case Datatype::TIME_MS:
      case Datatype::TIME_US:
      case Datatype::TIME_NS:
      case Datatype::TIME_PS:
      case Datatype::TIME_FS:
      case Datatype::TIME_AS:
        process_cell_range<int64_t>(tile, start, end);
        break;
      case Datatype::STRING_ASCII:
      case Datatype::CHAR:
        process_cell_range<char>(tile, start, end);
        break;
      case Datatype::BLOB:
      case Datatype::GEOM_WKB:
      case Datatype::GEOM_WKT:
        process_cell_range<std::byte>(tile, start, end);
        break;
      default:
        break;
    }
  } else {
    process_cell_range_var(tile, start, end);
  }
}

void TileMetadataGenerator::set_tile_metadata(WriterTileTuple& tile) {
  tile.set_metadata(
      min_,
      min_size_,
      max_,
      max_size_,
      global_order_min_,
      global_order_min_size_,
      global_order_max_,
      global_order_max_size_,
      sum_,
      null_count_);
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

template <class T>
void TileMetadataGenerator::min_max(
    const WriterTile& tile, uint64_t start, uint64_t end) {
  // Get pointer to the data and cell num.
  auto values = tile.data_as<T>();

  // Initialize defaults.
  if (min_ == nullptr) {
    min_ = static_cast<const void*>(&metadata_generator_type_data<T>::min);
    max_ = static_cast<const void*>(&metadata_generator_type_data<T>::max);
  }

  // Process cell by cell.
  for (uint64_t c = start; c < end; c++) {
    min_ = *static_cast<const T*>(min_) < values[c] ? min_ : &values[c];
    max_ = *static_cast<const T*>(max_) > values[c] ? max_ : &values[c];
  }
}

template <>
void TileMetadataGenerator::min_max<char>(
    const WriterTile& tile, uint64_t start, uint64_t end) {
  // For strings, return null for empty tiles.
  auto size = tile.size();
  if (size == 0) {
    return;
  }

  // Get pointer to the data, set the min max to the first value.
  auto data = tile.data_as<char>();
  if (min_ == nullptr) {
    min_ = data + start * cell_size_;
    max_ = data + start * cell_size_;
    start++;
  }

  // Process all cells, starting at the second value.
  auto value = data + start * cell_size_;
  for (uint64_t c = start; c < end; c++) {
    min_ = strncmp((const char*)min_, (const char*)value, cell_size_) > 0 ?
               value :
               min_;
    max_ = strncmp((const char*)max_, (const char*)value, cell_size_) < 0 ?
               value :
               max_;
    value += cell_size_;
  }
}

template <class T>
void TileMetadataGenerator::min_max_nullable(
    const WriterTile& tile,
    const WriterTile& validity_tile,
    uint64_t start,
    uint64_t end) {
  auto values = tile.data_as<T>();
  auto validity_values = validity_tile.data_as<uint8_t>();

  // Initialize defaults.
  if (min_ == nullptr) {
    min_ = (void*)&metadata_generator_type_data<T>::min;
    max_ = (void*)&metadata_generator_type_data<T>::max;
  }

  // Process cell by cell.
  for (uint64_t c = start; c < end; c++) {
    const bool is_null = validity_values[c] == 0;
    min_ = (is_null || (*static_cast<const T*>(min_) < values[c])) ? min_ :
                                                                     &values[c];
    max_ = (is_null || (*static_cast<const T*>(max_) > values[c])) ? max_ :
                                                                     &values[c];
    null_count_ += is_null;
  }
}

template <>
void TileMetadataGenerator::min_max_nullable<char>(
    const WriterTile& tile,
    const WriterTile& validity_tile,
    uint64_t start,
    uint64_t end) {
  // Get pointers to the data and cell num.
  auto value = tile.data_as<char>() + cell_size_ * start;
  auto validity_values = validity_tile.data_as<uint8_t>();

  // Process cell by cell.
  for (uint64_t c = start; c < end; c++) {
    const bool is_null = validity_values[c] == 0;
    min_ = !is_null &&
                   (min_ == nullptr ||
                    strncmp((const char*)min_, (const char*)value, cell_size_) >
                        0) ?
               value :
               min_;
    max_ = !is_null &&
                   (max_ == nullptr ||
                    strncmp((const char*)max_, (const char*)value, cell_size_) <
                        0) ?
               value :
               max_;
    value += cell_size_;
    null_count_ += is_null;
  }
}

template <class T>
void TileMetadataGenerator::process_cell_range(
    const WriterTileTuple& tile, uint64_t start, uint64_t end) {
  min_size_ = max_size_ = cell_size_;
  const auto& fixed_tile = tile.fixed_tile();

  // Fixed size attribute, non nullable
  if (!tile.nullable()) {
    if (has_min_max_) {
      min_max<T>(fixed_tile, start, end);
    }

    if (is_dim_) {
      iassert(end > start);
      global_order_min_ = &tile.fixed_tile().data_as<T>()[start];
      global_order_max_ = &tile.fixed_tile().data_as<T>()[end - 1];
      global_order_min_size_ = global_order_max_size_ = sizeof(T);
    }

    if (has_sum_) {
      Sum<T, typename metadata_generator_type_data<T>::sum_type>::sum(
          fixed_tile, start, end, sum_);
    }
  } else {  // Fixed size attribute, nullable.
    const auto& validity_tile = tile.validity_tile();
    auto validity_value = validity_tile.data_as<uint8_t>();
    if (has_min_max_) {
      min_max_nullable<T>(fixed_tile, validity_tile, start, end);
    } else {
      auto cell_num = tile.cell_num();
      for (uint64_t c = 0; c < cell_num; c++) {
        auto is_null = *validity_value == 0;
        null_count_ += (uint64_t)is_null;
        validity_value++;
      }
    }

    if (is_dim_) {
      /*
    uint64_t* max_sizes = reinterpret_cast<uint64_t*>(
        loaded_metadata_ptr_->tile_global_order_min_buffer()[dim].data());

    const uint64_t fixed_offset = tile / sizeof(uint64_t);
    max_sizes[fixed_offset] =
        data.var_tile().size() - source_offsets[data.cell_num() - 1];
    if (data.cell_num() == 1) {
      min_sizes[fixed_offset] = max_sizes[fixed_offset];
    } else {
      min_sizes[fixed_offset] = source_offsets[1] - source_offsets[0];
    }
    */
      throw std::logic_error("TODO");
    }

    if (has_sum_) {
      Sum<T, typename metadata_generator_type_data<T>::sum_type>::sum_nullable(
          fixed_tile, validity_tile, start, end, sum_);
    }
  }
}

void TileMetadataGenerator::process_cell_range_var(
    const WriterTileTuple& tile, uint64_t start, uint64_t end) {
  iassert(tile.var_size());

  const auto& offset_tile = tile.offset_tile();
  const auto& var_tile = tile.var_tile();

  // Handle empty tile.
  if (!has_min_max_ || offset_tile.size() == 0) {
    return;
  }

  // Get pointers to the data and cell num.
  auto offset_value = offset_tile.data_as<offsets_t>() + start;
  auto var_data = var_tile.data_as<char>();
  auto cell_num = tile.cell_num();

  // Var size attribute, non nullable.
  if (!tile.nullable()) {
    if (min_ == nullptr) {
      min_ = &var_data[*offset_value];
      max_ = &var_data[*offset_value];
      min_size_ = max_size_ = start == cell_num - 1 ?
                                  var_tile.size() - *offset_value :
                                  offset_value[1] - *offset_value;
      offset_value++;
      start++;
    }

    for (uint64_t c = start; c < end; c++) {
      auto value = var_data + *offset_value;
      auto size = c == cell_num - 1 ? var_tile.size() - *offset_value :
                                      offset_value[1] - *offset_value;
      min_max_var(value, size);
      offset_value++;
    }
  } else {  // Var size attribute, nullable.
    const auto& validity_tile = tile.validity_tile();
    auto validity_value = validity_tile.data_as<uint8_t>();

    for (uint64_t c = start; c < end; c++) {
      auto is_null = *validity_value == 0;
      if (!is_null) {
        auto value = var_data + *offset_value;
        auto size = c == cell_num - 1 ? var_tile.size() - *offset_value :
                                        offset_value[1] - *offset_value;
        if (min_ == nullptr && max_ == nullptr) {
          min_ = value;
          max_ = value;
          min_size_ = size;
          max_size_ = size;
        } else {
          min_max_var(value, size);
        }
      }
      offset_value++;

      null_count_ += (uint64_t)is_null;
      validity_value++;
    }
  }
}

inline void TileMetadataGenerator::min_max_var(
    const char* value, const uint64_t size) {
  iassert(value != nullptr);

  // Process min.
  size_t min_size = std::min<size_t>(min_size_, size);
  int cmp = strncmp(static_cast<const char*>(min_), value, min_size);
  if (cmp != 0) {
    if (cmp > 0) {
      min_ = value;
      min_size_ = size;
    }
  } else {
    if (size < min_size_) {
      min_ = value;
      min_size_ = size;
    }
  }

  // Process max.
  min_size = std::min<size_t>(max_size_, size);
  cmp = strncmp(static_cast<const char*>(max_), value, min_size);
  if (cmp != 0) {
    if (cmp < 0) {
      max_ = value;
      max_size_ = size;
    }
  } else {
    if (size > max_size_) {
      max_ = value;
      max_size_ = size;
    }
  }
}

}  // namespace tiledb::sm
