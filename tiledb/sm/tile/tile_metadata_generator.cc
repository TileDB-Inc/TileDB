/**
 * @file   tile_metadata_generator.cc
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
 * This file implements class TileMetadataGenerator.
 */

#include "tiledb/sm/tile/tile_metadata_generator.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*    STRUCTURED BINDINGS APIS    */
/* ****************************** */

template <typename T>
ByteVec Sum<T, int64_t>::sum(Tile* tile) {
  assert(tile != nullptr);

  // Zero sum.
  ByteVec ret(8, 0);

  // Get pointers to the data and cell num.
  auto values = tile->data_as<T>();
  auto cell_num = tile->size_as<T>();
  auto sum_data = reinterpret_cast<int64_t*>(ret.data());

  // Process cell by cell, swallowing overflow exception.
  for (uint64_t c = 0; c < cell_num; c++) {
    auto value = static_cast<int64_t>(*static_cast<T*>(&values[c]));
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

  return ret;
}

template <typename T>
ByteVec Sum<T, uint64_t>::sum(Tile* tile) {
  assert(tile != nullptr);

  // Zero sum.
  ByteVec ret(8, 0);

  // Get pointers to the data and cell num.
  auto values = tile->data_as<T>();
  auto cell_num = tile->size_as<T>();
  auto sum_data = reinterpret_cast<uint64_t*>(ret.data());

  // Process cell by cell, swallowing overflow exception.
  for (uint64_t c = 0; c < cell_num; c++) {
    auto value = static_cast<uint64_t>(*static_cast<T*>(&values[c]));
    if (*sum_data > std::numeric_limits<uint64_t>::max() - value) {
      *sum_data = std::numeric_limits<uint64_t>::max();
      break;
    }

    *sum_data += value;
  }

  return ret;
}

template <typename T>
ByteVec Sum<T, double>::sum(Tile* tile) {
  assert(tile != nullptr);

  // Zero sum.
  ByteVec ret(8, 0);

  // Get pointers to the data and cell num.
  auto values = tile->data_as<T>();
  auto cell_num = tile->size_as<T>();
  auto sum_data = reinterpret_cast<double*>(ret.data());

  // Process cell by cell, swallowing overflow exception.
  for (uint64_t c = 0; c < cell_num; c++) {
    auto value = static_cast<double>(*static_cast<T*>(&values[c]));
    if ((*sum_data < 0.0) == (value < 0.0) &&
        std::abs(*sum_data) >
            std::numeric_limits<double>::max() - std::abs(value)) {
      *sum_data = *sum_data < 0.0 ? std::numeric_limits<double>::lowest() :
                                    std::numeric_limits<double>::max();
      break;
    }

    *sum_data += value;
  }

  return ret;
}

template <typename T>
ByteVec Sum<T, int64_t>::sum_nullable(Tile* tile, Tile* tile_validity) {
  assert(tile != nullptr);

  // Zero sum.
  ByteVec ret(8, 0);

  // Get pointers to the data and cell num.
  auto values = tile->data_as<T>();
  auto validity_values = tile_validity->data_as<uint8_t>();
  auto cell_num = tile->size_as<T>();
  auto sum_data = reinterpret_cast<int64_t*>(ret.data());

  // Process cell by cell, swallowing overflow exception.
  for (uint64_t c = 0; c < cell_num; c++) {
    if (validity_values[c] != 0) {
      auto value = static_cast<int64_t>(*static_cast<T*>(&values[c]));
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

  return ret;
}

template <typename T>
ByteVec Sum<T, uint64_t>::sum_nullable(Tile* tile, Tile* tile_validity) {
  assert(tile != nullptr);

  // Zero sum.
  ByteVec ret(8, 0);

  // Get pointers to the data and cell num.
  auto values = tile->data_as<T>();
  auto validity_values = tile_validity->data_as<uint8_t>();
  auto cell_num = tile->size_as<T>();
  auto sum_data = reinterpret_cast<uint64_t*>(ret.data());

  // Process cell by cell, swallowing overflow exception.
  for (uint64_t c = 0; c < cell_num; c++) {
    if (validity_values[c] != 0) {
      auto value = static_cast<uint64_t>(*static_cast<T*>(&values[c]));
      if (*sum_data > std::numeric_limits<uint64_t>::max() - value) {
        *sum_data = std::numeric_limits<uint64_t>::max();
        break;
      }

      *sum_data += value;
    }
  }

  return ret;
}

template <typename T>
ByteVec Sum<T, double>::sum_nullable(Tile* tile, Tile* tile_validity) {
  assert(tile != nullptr);

  // Zero sum.
  ByteVec ret(8, 0);

  // Get pointers to the data and cell num.
  auto values = tile->data_as<T>();
  auto validity_values = tile_validity->data_as<uint8_t>();
  auto cell_num = tile->size_as<T>();
  auto sum_data = reinterpret_cast<double*>(ret.data());

  // Process cell by cell, swallowing overflow exception.
  for (uint64_t c = 0; c < cell_num; c++) {
    if (validity_values[c] != 0) {
      auto value = static_cast<double>(*static_cast<T*>(&values[c]));
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

  return ret;
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

  // No min max for any, blob, or non ascii strings.
  switch (type) {
    case Datatype::ANY:
    case Datatype::BLOB:
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

  // No sum for any, blob, or non ascii strings.
  switch (type) {
    case Datatype::ANY:
    case Datatype::BLOB:
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

template <class T>
const std::tuple<void*, void*> TileMetadataGenerator::min_max(
    const Tile* tile, const uint64_t cell_size) {
  assert(tile != nullptr);

  // Initialize defaults.
  void* min = (void*)&metadata_generator_type_data<T>::min;
  void* max = (void*)&metadata_generator_type_data<T>::max;

  // Get pointer to the data and cell num.
  auto values = tile->data_as<T>();
  auto cell_num = tile->size() / cell_size;

  // Process cell by cell.
  for (uint64_t c = 0; c < cell_num; c++) {
    min = *static_cast<T*>(min) < values[c] ? min : &values[c];
    max = *static_cast<T*>(max) > values[c] ? max : &values[c];
  }

  return {min, max};
}

template <>
const std::tuple<void*, void*> TileMetadataGenerator::min_max<char>(
    const Tile* tile, const uint64_t cell_size) {
  assert(tile != nullptr);

  // For strings, return null for empty tiles.
  auto size = tile->size();
  if (size == 0)
    return {nullptr, nullptr};

  // Get pointer to the data, set the min max to the first value.
  auto data = tile->data_as<char>();
  void* min = data;
  void* max = data;

  // Process all cells, starting at the second value.
  auto value = data + cell_size;
  auto cell_num = size / cell_size;
  for (uint64_t c = 1; c < cell_num; c++) {
    min = strncmp((const char*)min, (const char*)value, size) > 0 ? value : min;
    max = strncmp((const char*)max, (const char*)value, size) < 0 ? value : max;
    value += cell_size;
  }

  return {min, max};
}

template <class T>
const std::tuple<void*, void*, uint64_t>
TileMetadataGenerator::min_max_nullable(
    const Tile* tile, const Tile* tile_validity, const uint64_t cell_size) {
  assert(tile != nullptr);

  // Initialize defaults.
  void* min = (void*)&metadata_generator_type_data<T>::min;
  void* max = (void*)&metadata_generator_type_data<T>::max;
  uint64_t null_count = 0;

  // Get pointers to the data and cell num.
  auto values = tile->data_as<T>();
  auto validity_values = tile_validity->data_as<uint8_t>();
  auto cell_num = tile->size() / cell_size;

  // Process cell by cell.
  for (uint64_t c = 0; c < cell_num; c++) {
    const bool is_null = validity_values[c] == 0;
    min = (is_null || (*static_cast<T*>(min) < values[c])) ? min : &values[c];
    max = (is_null || (*static_cast<T*>(max) > values[c])) ? max : &values[c];
    null_count += is_null;
  }

  return {min, max, null_count};
}

template <>
const std::tuple<void*, void*, uint64_t>
TileMetadataGenerator::min_max_nullable<char>(
    const Tile* tile, const Tile* tile_validity, const uint64_t cell_size) {
  assert(tile != nullptr);

  // Initialize to null.
  void* min = nullptr;
  void* max = nullptr;
  uint64_t null_count = 0;

  // Get pointers to the data and cell num.
  auto value = tile->data_as<char>();
  auto validity_values = tile_validity->data_as<uint8_t>();
  auto cell_num = tile->size() / cell_size;

  // Process cell by cell.
  for (uint64_t c = 0; c < cell_num; c++) {
    const bool is_null = validity_values[c] == 0;
    min =
        !is_null &&
                (min == nullptr ||
                 strncmp((const char*)min, (const char*)value, cell_size) > 0) ?
            value :
            min;
    max =
        !is_null &&
                (max == nullptr ||
                 strncmp((const char*)max, (const char*)value, cell_size) < 0) ?
            value :
            max;
    value += cell_size;
    null_count += is_null;
  }

  return {min, max, null_count};
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
    : type_(type)
    , min_(nullptr)
    , min_size_(0)
    , max_(nullptr)
    , max_size_(0)
    , null_count_(0)
    , cell_size_(cell_size) {
  has_min_max_ = has_min_max_metadata(type, is_dim, var_size, cell_val_num);
  has_sum_ = has_sum_metadata(type, var_size, cell_val_num);

  sum_.resize(sizeof(uint64_t));
}

/* ****************************** */
/*               API              */
/* ****************************** */

std::tuple<
    const void*,
    uint64_t,
    const void*,
    uint64_t,
    const ByteVec*,
    uint64_t>
TileMetadataGenerator::metadata() const {
  return {min_, min_size_, max_, max_size_, &sum_, null_count_};
}

void TileMetadataGenerator::process_tile(
    Tile* tile, Tile* tile_var, Tile* tile_validity) {
  min_ = nullptr;
  min_size_ = 0;
  max_ = nullptr;
  max_size_ = 0;
  null_count_ = 0;

  if (tile_var == nullptr) {
    // Switch depending on datatype.
    switch (type_) {
      case Datatype::INT8:
        process_tile<int8_t>(tile, tile_validity);
        break;
      case Datatype::INT16:
        process_tile<int16_t>(tile, tile_validity);
        break;
      case Datatype::INT32:
        process_tile<int32_t>(tile, tile_validity);
        break;
      case Datatype::INT64:
        process_tile<int64_t>(tile, tile_validity);
        break;
      case Datatype::UINT8:
        process_tile<uint8_t>(tile, tile_validity);
        break;
      case Datatype::UINT16:
        process_tile<uint16_t>(tile, tile_validity);
        break;
      case Datatype::UINT32:
        process_tile<uint32_t>(tile, tile_validity);
        break;
      case Datatype::UINT64:
        process_tile<uint64_t>(tile, tile_validity);
        break;
      case Datatype::FLOAT32:
        process_tile<float>(tile, tile_validity);
        break;
      case Datatype::FLOAT64:
        process_tile<double>(tile, tile_validity);
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
        process_tile<int64_t>(tile, tile_validity);
        break;
      case Datatype::STRING_ASCII:
        process_tile<char>(tile, tile_validity);
        break;
      default:
        break;
    }
  } else {
    process_tile_var(tile, tile_var, tile_validity);
  }
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

template <class T>
void TileMetadataGenerator::process_tile(Tile* tile, Tile* tile_validity) {
  assert(tile != nullptr);
  min_size_ = max_size_ = cell_size_;

  // Fixed size attribute, non nullable
  if (tile_validity == nullptr) {
    if (has_min_max_) {
      auto&& [min, max] = min_max<T>(tile, cell_size_);
      min_ = min;
      max_ = max;
    }

    if (has_sum_) {
      sum_ =
          Sum<T, typename metadata_generator_type_data<T>::sum_type>::sum(tile);
    }
  } else {  // Fixed size attribute, nullable.
    auto validity_value = tile_validity->data_as<uint8_t>();
    if (has_min_max_) {
      auto&& [min, max, nc] =
          min_max_nullable<T>(tile, tile_validity, cell_size_);
      min_ = min;
      max_ = max;
      null_count_ = nc;
    } else {
      auto cell_num = tile->size() / cell_size_;
      for (uint64_t c = 0; c < cell_num; c++) {
        auto is_null = *validity_value == 0;
        null_count_ += (uint64_t)is_null;
        validity_value++;
      }
    }

    if (has_sum_) {
      sum_ = Sum<T, typename metadata_generator_type_data<T>::sum_type>::
          sum_nullable(tile, tile_validity);
    }
  }
}

void TileMetadataGenerator::process_tile_var(
    Tile* tile, Tile* tile_var, Tile* tile_validity) {
  assert(tile != nullptr);
  assert(tile_var != nullptr);

  // Handle empty tile.
  if (!has_min_max_ || tile->size() == 0) {
    return;
  }

  // Get pointers to the data and cell num.
  auto offset_value = tile->data_as<uint64_t>();
  auto var_data = tile_var->data_as<char>();
  auto cell_num = tile->size() / constants::cell_var_offset_size;

  // Var size attribute, non nullable.
  if (tile_validity == nullptr) {
    offset_value++;
    min_ = var_data;
    max_ = var_data;
    min_size_ = max_size_ = cell_num == 1 ? tile_var->size() : *offset_value;

    for (uint64_t c = 1; c < cell_num; c++) {
      auto value = var_data + *offset_value;
      auto size = c == cell_num - 1 ? tile_var->size() - *offset_value :
                                      offset_value[1] - *offset_value;
      min_max_var(value, size);
      offset_value++;
    }
  } else {  // Var size attribute, nullable.
    auto validity_value = tile_validity->data_as<uint8_t>();

    for (uint64_t c = 0; c < cell_num; c++) {
      auto is_null = *validity_value == 0;
      if (!is_null) {
        auto value = var_data + *offset_value;
        auto size = c == cell_num - 1 ? tile_var->size() - *offset_value :
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
  assert(value != nullptr);

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

}  // namespace sm
}  // namespace tiledb
