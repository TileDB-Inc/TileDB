/**
 * @file enumeration.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file implements class Enumeration.
 */

#include <iostream>
#include <sstream>

#include "tiledb/sm/misc/uuid.h"

#include "enumeration.h"

namespace tiledb::sm {

/** Class for locally generated status exceptions. */
class EnumerationException : public StatusException {
 public:
  explicit EnumerationException(const std::string& msg)
      : StatusException("Enumeration", msg) {
  }
};

Enumeration::Enumeration(
    const std::string& name,
    const std::string& path_name,
    Datatype type,
    uint32_t cell_val_num,
    bool ordered,
    const void* data,
    uint64_t data_size,
    const void* offsets,
    uint64_t offsets_size)
    : name_(name)
    , path_name_(path_name)
    , type_(type)
    , cell_val_num_(cell_val_num)
    , ordered_(ordered)
    , data_(data_size)
    , offsets_(offsets_size) {
  ensure_datatype_is_valid(type);

  if (name.empty()) {
    throw EnumerationException("Enumeration name must not be empty");
  }

  if (path_name_.empty()) {
    std::string tmp_uuid;
    throw_if_not_ok(uuid::generate_uuid(&tmp_uuid, false));
    path_name_ =
        "__" + tmp_uuid + "_" + std::to_string(constants::enumerations_version);
  }

  if (name.find("/") != std::string::npos) {
    throw EnumerationException(
        "Enumeration name must not contain path separators");
  }

  if (cell_val_num == 0) {
    throw EnumerationException("Invalid cell_val_num in Enumeration");
  }

  if (data == nullptr || data_size == 0) {
    throw EnumerationException("No attribute value data supplied.");
  }

  if (var_size() && (offsets == nullptr || offsets_size == 0)) {
    throw EnumerationException(
        "Variable length datatype defined but offsets are not present");
  } else if (!var_size() && (offsets != nullptr || offsets_size > 0)) {
    throw EnumerationException(
        "Fixed length datatype defined but offsets are present");
  }

  if (var_size()) {
    if (offsets_size % sizeof(uint64_t) != 0) {
      throw EnumerationException(
          "Invalid offsets size is not a multiple of sizeof(uint64_t)");
    }
    auto offset_values = static_cast<const uint64_t*>(offsets);
    uint64_t last_offset = (offsets_size / sizeof(uint64_t)) - 1;
    if (offset_values[last_offset] > data_size) {
      throw EnumerationException(
          "Provided data buffer size is too small for the provided offsets.");
    }
  } else {
    if (data_size % cell_size() != 0) {
      throw EnumerationException(
          "Invalid data size is not a multiple of the cell size.");
    }
  }

  throw_if_not_ok(data_.write(data, 0, data_size));

  if (offsets_size > 0) {
    throw_if_not_ok(offsets_.write(offsets, 0, offsets_size));
  }

  generate_value_map();
}

shared_ptr<const Enumeration> Enumeration::deserialize(
    Deserializer& deserializer) {
  auto disk_version = deserializer.read<uint32_t>();
  if (disk_version > constants::enumerations_version) {
    throw EnumerationException(
        "Invalid Enumeration version '" + std::to_string(disk_version) +
        "' is newer than supported enumeration version '" +
        std::to_string(constants::enumerations_version) + "'");
  }

  auto name_size = deserializer.read<uint32_t>();
  std::string name(deserializer.get_ptr<char>(name_size), name_size);

  auto path_name_size = deserializer.read<uint32_t>();
  std::string path_name(
      deserializer.get_ptr<char>(path_name_size), path_name_size);

  auto type = deserializer.read<uint8_t>();
  auto cell_val_num = deserializer.read<uint32_t>();
  auto ordered = deserializer.read<bool>();

  auto data_size = deserializer.read<uint64_t>();
  const void* data = deserializer.get_ptr<void>(data_size);

  uint64_t offsets_size = 0;
  const void* offsets = nullptr;

  if (cell_val_num == constants::var_num) {
    offsets_size = deserializer.read<uint64_t>();
    offsets = deserializer.get_ptr<void>(offsets_size);
  }

  return create(
      name,
      path_name,
      static_cast<Datatype>(type),
      cell_val_num,
      ordered,
      data,
      data_size,
      offsets,
      offsets_size);
}

void Enumeration::serialize(Serializer& serializer) const {
  serializer.write<uint32_t>(constants::enumerations_version);

  auto name_size = static_cast<uint32_t>(name_.size());
  serializer.write<uint32_t>(name_size);
  serializer.write(name_.data(), name_size);

  auto path_name_size = static_cast<uint32_t>(path_name_.size());
  serializer.write<uint32_t>(path_name_size);
  serializer.write(path_name_.data(), path_name_size);

  serializer.write<uint8_t>(static_cast<uint8_t>(type_));
  serializer.write<uint32_t>(cell_val_num_);
  serializer.write<bool>(ordered_);
  serializer.write<uint64_t>(data_.size());
  serializer.write(data_.data(), data_.size());

  if (var_size()) {
    serializer.write<uint64_t>(offsets_.size());
    serializer.write(offsets_.data(), offsets_.size());
  } else {
    assert(cell_val_num_ < constants::var_num);
    assert(offsets_.size() == 0);
  }
}

uint64_t Enumeration::index_of(UntypedDatumView value) const {
  std::string_view value_view(
      static_cast<const char*>(value.content()), value.size());

  auto iter = value_map_.find(value_view);
  if (iter == value_map_.end()) {
    return constants::enumeration_missing_value;
  }

  return iter->second;
}

void Enumeration::dump(FILE* out) const {
  if (out == nullptr) {
    out = stdout;
  }
  std::stringstream ss;
  ss << "### Enumeration ###" << std::endl;
  ss << "- Name: " << name_ << std::endl;
  ss << "- Type: " << datatype_str(type_) << std::endl;
  ss << "- Cell Val Num: " << cell_val_num_ << std::endl;
  ss << "- Ordered: " << (ordered_ ? "true" : "false") << std::endl;
  ss << "- Element Count: " << value_map_.size() << std::endl;
  fprintf(out, "%s", ss.str().c_str());
}

void Enumeration::generate_value_map() {
  auto char_data = data_.data_as<char>();
  if (var_size()) {
    auto offsets = offsets_.data_as<uint64_t>();
    uint64_t num_offsets = offsets_.size() / sizeof(uint64_t);

    for (uint64_t i = 0; i < num_offsets; i++) {
      uint64_t length = 0;
      if (i < num_offsets - 1) {
        length = offsets[i + 1] - offsets[i];
      } else {
        length = data_.size() - offsets[i];
      }

      auto sv = std::string_view(char_data + offsets[i], length);
      add_value_to_map(sv, i);
    }
  } else {
    uint64_t i = 0;
    auto stride = cell_size();
    while (i * stride < data_.size()) {
      auto sv = std::string_view(char_data + i * stride, stride);
      add_value_to_map(sv, i);
      i += 1;
    }
  }
}

void Enumeration::add_value_to_map(std::string_view& sv, uint64_t index) {
  if (value_map_.find(sv) != value_map_.end()) {
    throw EnumerationException(
        "Invalid duplicated value in enumeration '" + std::string(sv) + "'");
  }
  value_map_[sv] = index;
}

}  // namespace tiledb::sm
