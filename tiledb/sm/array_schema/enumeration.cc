/**
 * @file enumeration.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB, Inc.
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

#include "tiledb/common/memory_tracker.h"
#include "tiledb/common/random/random_label.h"

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
    uint64_t offsets_size,
    shared_ptr<MemoryTracker> memory_tracker)
    : memory_tracker_(memory_tracker)
    , name_(name)
    , path_name_(path_name)
    , type_(type)
    , cell_val_num_(cell_val_num)
    , ordered_(ordered)
    , data_(data_size)
    , offsets_(offsets_size)
    , value_map_(memory_tracker_->get_resource(MemoryType::ENUMERATION)) {
  ensure_datatype_is_valid(type);

  if (name.empty()) {
    throw EnumerationException("Enumeration name must not be empty");
  }

  if (path_name_.empty()) {
    path_name_ = "__" + tiledb::common::random_label() + "_" +
                 std::to_string(constants::enumerations_version);
  }

  if (path_name.find("/") != std::string::npos) {
    throw EnumerationException(
        "Enumeration path name must not contain path separators");
  }

  if (cell_val_num == 0) {
    throw EnumerationException("Invalid cell_val_num in Enumeration");
  }

  // Check if we're creating an empty enumeration and bail.
  auto data_empty = (data == nullptr && data_size == 0);
  auto offsets_empty = (offsets == nullptr && offsets_size == 0);
  if (data_empty && offsets_empty) {
    // This is an empty enumeration so we're done checking for argument
    // validity.
    return;
  }

  if (var_size()) {
    if (offsets == nullptr) {
      throw EnumerationException(
          "Var sized enumeration values require a non-null offsets pointer.");
    }

    if (offsets_size == 0) {
      throw EnumerationException(
          "Var sized enumeration values require a non-zero offsets size.");
    }

    if (offsets_size % constants::cell_var_offset_size != 0) {
      throw EnumerationException(
          "Invalid offsets size is not a multiple of sizeof(uint64_t)");
    }

    // Setup some temporary aliases for quick reference
    auto offset_values = static_cast<const uint64_t*>(offsets);
    auto num_offsets = offsets_size / constants::cell_var_offset_size;

    // Check for the edge case of a single value so we can handle the case of
    // having a single empty value.
    if (num_offsets == 1 && offset_values[0] == 0) {
      // If data is nullptr and data_size > 0, then the user appears to have
      // intended to provided us with a non-empty value.
      if (data_size > 0 && data == nullptr) {
        throw EnumerationException(
            "Invalid data buffer; must not be nullptr when data_size "
            "is non-zero.");
      }
      // Else, data_size is zero and we don't care what data is. We ignore the
      // check for data_size == 0 && data != nullptr here because a common
      // use case with our APIs is to use a std::string to contain all of the
      // var data which AFAIK never returns nullptr.
    } else {
      // We have more than one string which requires a non-nullptr data and
      // non-zero data size that is greater than or equal to the last
      // offset provided.
      if (data == nullptr) {
        // Users need to be able to create an enumeration containing one value
        // which is the empty string.
        //
        // The std::vector<uint8_t> foo(0, 0) constructor at our callsite
        // results in a foo.data() which is nullptr and foo.size() which is
        // zero. So if data is nullptr, we only fail if data_size is not zero.
        if (data_size != 0) {
          throw EnumerationException(
              "Invalid data input, nullptr provided when the provided offsets "
              "require data.");
        }
      }

      if (data_size < offset_values[num_offsets - 1]) {
        throw EnumerationException(
            "Invalid data input, data_size is smaller than the last provided "
            "offset.");
      }
    }
  } else {  // !var_sized()
    if (offsets != nullptr) {
      throw EnumerationException(
          "Fixed length value type defined but offsets is not nullptr.");
    }

    if (offsets_size != 0) {
      throw EnumerationException(
          "Fixed length value type defined but offsets size is non-zero.");
    }

    if (data == nullptr) {
      throw EnumerationException(
          "Invalid data buffer must not be nullptr for fixed sized data.");
    }

    if (data_size == 0) {
      throw EnumerationException(
          "Invalid data size; must be non-zero for fixed size data.");
    }

    if (data_size % cell_size() != 0) {
      throw EnumerationException(
          "Invalid data size is not a multiple of the cell size.");
    }
  }

  // std::memcpy with nullptr in either src or dest can lead to UB
  // data_size != 0 guarantees internal data buffer address to be not null
  if (data != nullptr && data_size != 0) {
    throw_if_not_ok(data_.write(data, 0, data_size));
  }
  if (offsets != nullptr) {
    throw_if_not_ok(offsets_.write(offsets, 0, offsets_size));
  }

  generate_value_map();
}

shared_ptr<const Enumeration> Enumeration::deserialize(
    Deserializer& deserializer, shared_ptr<MemoryTracker> memory_tracker) {
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
  const void* data = nullptr;

  if (data_size > 0) {
    data = deserializer.get_ptr<void>(data_size);
  }

  uint64_t offsets_size = 0;
  const void* offsets = nullptr;

  if (cell_val_num == constants::var_num) {
    offsets_size = deserializer.read<uint64_t>();
    if (offsets_size > 0) {
      offsets = deserializer.get_ptr<void>(offsets_size);
    }
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
      offsets_size,
      memory_tracker);
}

shared_ptr<const Enumeration> Enumeration::extend(
    const void* data,
    uint64_t data_size,
    const void* offsets,
    uint64_t offsets_size) const {
  if (data == nullptr && data_size == 0) {
    // This is OK
    //
    // Users need to be able to create an enumeration containing
    // one value which is the empty string.
    //
    // The std::vector<uint8_t> foo(0, 0) constructor at our callsite results
    // in a foo.data() which is nullptr and foo.size() which is zero.
  } else {
    if (data == nullptr) {
      throw EnumerationException(
          "Unable to extend an enumeration without a data buffer.");
    }

    if (data_size == 0) {
      throw EnumerationException(
          "Unable to extend an enumeration with a zero sized data buffer.");
    }
  }

  if (var_size()) {
    if (offsets == nullptr) {
      throw EnumerationException(
          "The offsets buffer is required for this enumeration extension.");
    }

    if (offsets_size == 0) {
      throw EnumerationException(
          "The offsets buffer for this enumeration extension must "
          "have a non-zero size.");
    }

    if (offsets_size % sizeof(uint64_t) != 0) {
      throw EnumerationException(
          "Invalid offsets size is not a multiple of sizeof(uint64_t)");
    }
  } else {
    if (offsets != nullptr) {
      throw EnumerationException(
          "Offsets buffer provided when extending a fixed sized enumeration.");
    }
    if (offsets_size != 0) {
      throw EnumerationException(
          "Offsets size is non-zero when extending a fixed sized enumeration.");
    }
  }

  Buffer new_data(data_.size() + data_size);
  throw_if_not_ok(new_data.write(data_.data(), data_.size()));
  if (data_size > 0) {
    throw_if_not_ok(new_data.write(data, data_size));
  }

  const void* new_offsets_ptr = nullptr;
  uint64_t new_offsets_size = 0;

  Buffer new_offsets(offsets_.size() + offsets_size);

  if (var_size()) {
    // First we write our existing offsets
    throw_if_not_ok(new_offsets.write(offsets_.data(), offsets_.size()));

    // All new offsets have to be rewritten to be relative to the length
    // of the current data array.
    const uint64_t* offsets_arr = static_cast<const uint64_t*>(offsets);
    uint64_t num_offsets = offsets_size / sizeof(uint64_t);
    for (uint64_t i = 0; i < num_offsets; i++) {
      uint64_t new_offset = offsets_arr[i] + data_.size();
      throw_if_not_ok(new_offsets.write(&new_offset, sizeof(uint64_t)));
    }

    new_offsets_ptr = new_offsets.data();
    new_offsets_size = new_offsets.size();
  }

  return create(
      name_,
      "",
      type_,
      cell_val_num_,
      ordered_,
      new_data.data(),
      new_data.size(),
      new_offsets_ptr,
      new_offsets_size,
      memory_tracker_);
}

bool Enumeration::is_extension_of(shared_ptr<const Enumeration> other) const {
  if (name_ != other->name()) {
    return false;
  }

  if (type_ != other->type()) {
    return false;
  }

  if (cell_val_num_ != other->cell_val_num()) {
    return false;
  }

  if (ordered_ != other->ordered()) {
    return false;
  }

  auto other_data = other->data();
  // Not <=, since a single empty string can be added as an extension.
  if (data_.size() < other_data.size()) {
    return false;
  }

  if (std::memcmp(data_.data(), other_data.data(), other_data.size()) != 0) {
    return false;
  }

  if (var_size()) {
    auto other_offsets = other->offsets();
    if (offsets_.size() <= other_offsets.size()) {
      return false;
    }

    if (std::memcmp(
            offsets_.data(), other_offsets.data(), other_offsets.size()) != 0) {
      return false;
    }
  }

  return true;
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
  if (data_.size() > 0) {
    serializer.write(data_.data(), data_.size());
  }

  if (var_size()) {
    serializer.write<uint64_t>(offsets_.size());
    if (offsets_.size() > 0) {
      serializer.write(offsets_.data(), offsets_.size());
    }
  } else {
    assert(cell_val_num_ < constants::var_num);
    assert(offsets_.size() == 0);
  }
}

uint64_t Enumeration::index_of(const void* data, uint64_t size) const {
  std::string_view value_view(static_cast<const char*>(data), size);

  auto iter = value_map_.find(value_view);
  if (iter == value_map_.end()) {
    return constants::enumeration_missing_value;
  }

  return iter->second;
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
  const auto res = value_map_.insert(std::make_pair(sv, index));
  if (!res.second) {
    throw EnumerationException(
        "Invalid duplicated value in enumeration '" + std::string(sv) + "'");
  }
}

}  // namespace tiledb::sm

std::ostream& operator<<(
    std::ostream& os, const tiledb::sm::Enumeration& enumeration) {
  os << "### Enumeration ###" << std::endl;
  os << "- Name: " << enumeration.name() << std::endl;
  os << "- Type: " << datatype_str(enumeration.type()) << std::endl;
  os << "- Cell Val Num: " << enumeration.cell_val_num() << std::endl;
  os << "- Ordered: " << (enumeration.ordered() ? "true" : "false")
     << std::endl;
  os << "- Element Count: " << enumeration.value_map().size() << std::endl;

  return os;
}
