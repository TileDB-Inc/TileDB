/**
 * @file dictionary.cc
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
 * This file implements class Dictionary.
 */

#include <cassert>
#include <iostream>
#include <sstream>

#include "tiledb/sm/array_schema/dictionary.h"

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::sm {

/** Class for locally generated status exceptions. */
class DictionaryException : public StatusException {
 public:
  explicit DictionaryException(const std::string& msg)
      : StatusException("Dictionary", msg) {
  }
};

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Dictionary::Dictionary(Datatype type, uint32_t cell_val_num, bool nullable, bool ordered)
  : type_(type)
  , cell_val_num_(cell_val_num)
  , nullable_(nullable)
  , ordered_(ordered) {

  if (cell_val_num == 0) {
    cell_val_num = type_ == Datatype::ANY ? constants::var_num : 1;
  }
}

Dictionary::Dictionary(
    Datatype type,
    uint32_t cell_val_num,
    bool nullable,
    bool ordered,
    Buffer data,
    Buffer offsets,
    Buffer validity)
  : type_(type)
  , cell_val_num_(cell_val_num)
  , nullable_(nullable)
  , ordered_(ordered)
  , data_(data)
  , offsets_(offsets)
  , validity_(validity) {
}

Dictionary::Dictionary(const Dictionary& dict)
  : type_(dict.type_)
  , cell_val_num_(dict.cell_val_num_)
  , nullable_(dict.nullable_)
  , ordered_(dict.ordered_)
  , data_(dict.data_)
  , offsets_(dict.offsets_)
  , validity_(dict.validity_) {
}

Dictionary::Dictionary(Dictionary&& dict) noexcept {
  swap(dict);
}

Dictionary& Dictionary::operator=(const Dictionary& dict) {
  Dictionary tmp(dict);
  swap(tmp);
  return *this;
}

Dictionary& Dictionary::operator=(Dictionary&& dict) {
  swap(dict);
  return *this;
}

Datatype Dictionary::type() const {
  return type_;
}

void Dictionary::set_cell_val_num(uint32_t cv_num) {
  cell_val_num_ = cv_num;
}

uint32_t Dictionary::cell_val_num() const {
  return cell_val_num_;
}

bool Dictionary::var_size() const {
  return cell_val_num_ == constants::var_num;
}

void Dictionary::set_nullable(bool nullable) {
  nullable_ = nullable;
}

bool Dictionary::nullable() const {
  return nullable_;
}

void Dictionary::set_ordered(bool ordered) {
  ordered_ = ordered;
}

bool Dictionary::ordered() const {
  return ordered_;
}

void Dictionary::set_data_buffer(void* const buffer, uint64_t buffer_size) {
  data_.clear();
  throw_if_not_ok(data_.realloc(buffer_size));
  throw_if_not_ok(data_.write(buffer, buffer_size));
}

void Dictionary::get_data_buffer(void** buffer, uint64_t* buffer_size) {
  *buffer = data_.data(0);
  *buffer_size = data_.size();
}

void Dictionary::set_offsets_buffer(void* const buffer, uint64_t buffer_size) {
  offsets_.clear();
  throw_if_not_ok(offsets_.realloc(buffer_size));
  throw_if_not_ok(offsets_.write(buffer, buffer_size));
}

void Dictionary::get_offsets_buffer(void** buffer, uint64_t* buffer_size) {
  *buffer = offsets_.data(0);
  *buffer_size = offsets_.size();
}

void Dictionary::set_validity_buffer(void* const buffer, uint64_t buffer_size) {
  validity_.clear();
  throw_if_not_ok(validity_.realloc(buffer_size));
  throw_if_not_ok(validity_.write(buffer, buffer_size));
}

void Dictionary::get_validity_buffer(void** buffer, uint64_t* buffer_size) {
  *buffer = validity_.data(0);
  *buffer_size = validity_.size();
}

shared_ptr<Dictionary> Dictionary::deserialize(Deserializer& deserializer, uint32_t version) {
  if (version < constants::dictionaries_min_version) {
    throw DictionaryException("No dictionary support in version: " + std::to_string(version));
  }

  // Datatype datatype = static_cast<Datatype>(type);

  auto type = deserializer.read<uint8_t>();
  auto cell_val_num = deserializer.read<uint32_t>();
  auto nullable = deserializer.read<bool>();
  auto ordered = deserializer.read<bool>();

  Buffer data;
  Buffer offsets;
  Buffer validity;

  auto size = deserializer.read<uint64_t>();
  throw_if_not_ok(data.realloc(size));
  deserializer.read(data.data(0), size);
  data.set_size(size);
  data.set_offset(size);

  if (cell_val_num == constants::var_num) {
    size = deserializer.read<uint64_t>();
    throw_if_not_ok(offsets.realloc(size));
    deserializer.read(offsets.data(0), size);
    offsets.set_size(size);
    offsets.set_offset(size);
  }

  if (nullable) {
    size = deserializer.read<uint64_t>();
    throw_if_not_ok(validity.realloc(size));
    deserializer.read(validity.data(0), size);
    validity.set_size(size);
    validity.set_offset(size);
  }

  return std::make_shared<Dictionary>(
      static_cast<Datatype>(type),
      cell_val_num,
      nullable,
      ordered,
      data,
      offsets,
      validity);
}

void Dictionary::serialize(Serializer& serializer, uint32_t version) const {
  if (version < constants::dictionaries_min_version) {
    throw DictionaryException("No dictionary support in version: " + std::to_string(version));
  }

  serializer.write<uint8_t>(static_cast<uint8_t>(type_));
  serializer.write<uint32_t>(cell_val_num_);
  serializer.write<bool>(nullable_);
  serializer.write<bool>(ordered_);

  serializer.write<uint64_t>(data_.size());
  serializer.write(data_.data(0), data_.size());

  if (var_size()) {
    serializer.write<uint64_t>(offsets_.size());
    serializer.write(offsets_.data(0), offsets_.size());
  }

  if (nullable_) {
    serializer.write<uint64_t>(validity_.size());
    serializer.write(validity_.data(0), validity_.size());
  }
}

void Dictionary::swap(Dictionary& other) {
  std::swap(type_, other.type_);
  std::swap(cell_val_num_, other.cell_val_num_);
  std::swap(nullable_, other.nullable_);
  std::swap(ordered_, other.ordered_);
  std::swap(data_, other.data_);
  std::swap(offsets_, other.offsets_);
  std::swap(validity_, other.validity_);
}

void Dictionary::dump(FILE* output) const {
  if (output == nullptr) {
    output = stderr;
  }

  fprintf(stderr, "Dictionary");
}

} // namespace tiledb::sm
