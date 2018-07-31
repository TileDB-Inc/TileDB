/**
 * @file   tile.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file implements class Tile.
 */

#include "tiledb/sm/tile/tile.h"
#include "tiledb/sm/misc/logger.h"

#include <iostream>

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Tile::Tile() {
  buffer_ = nullptr;
  cell_size_ = 0;
  compressor_ = Compressor::NO_COMPRESSION;
  compression_level_ = -1;
  dim_num_ = 0;
  owns_buff_ = true;
  type_ = Datatype::INT32;
}

Tile::Tile(unsigned int dim_num)
    : dim_num_(dim_num) {
  buffer_ = nullptr;
  cell_size_ = 0;
  compressor_ = Compressor::NO_COMPRESSION;
  compression_level_ = -1;
  owns_buff_ = true;
  type_ = Datatype::INT32;
}

Tile::Tile(
    Datatype type,
    Compressor compressor,
    int compression_level,
    uint64_t cell_size,
    unsigned int dim_num,
    Buffer* buff,
    bool owns_buff)
    : buffer_(buff)
    , cell_size_(cell_size)
    , compressor_(compressor)
    , compression_level_(compression_level)
    , dim_num_(dim_num)
    , owns_buff_(owns_buff)
    , type_(type) {
}

Tile::Tile(const Tile& tile) {
  owns_buff_ = false;
  *this = tile;
}

Tile::~Tile() {
  if (owns_buff_)
    delete buffer_;
}

/* ****************************** */
/*               API              */
/* ****************************** */

uint64_t Tile::cell_num() const {
  return size() / cell_size_;
}

Status Tile::init(
    Datatype type,
    Compressor compressor,
    uint64_t cell_size,
    unsigned int dim_num) {
  cell_size_ = cell_size;
  compressor_ = compressor;
  dim_num_ = dim_num;
  type_ = type;

  buffer_ = new Buffer();
  if (buffer_ == nullptr)
    return LOG_STATUS(
        Status::TileError("Cannot initialize tile; Buffer allocation failed"));

  return Status::Ok();
}

Status Tile::init(
    Datatype type,
    Compressor compressor,
    int compression_level,
    uint64_t tile_size,
    uint64_t cell_size,
    unsigned int dim_num) {
  cell_size_ = cell_size;
  compressor_ = compressor;
  compression_level_ = compression_level;
  dim_num_ = dim_num;
  type_ = type;

  buffer_ = new Buffer();
  if (buffer_ == nullptr)
    return LOG_STATUS(
        Status::TileError("Cannot initialize tile; Buffer allocation failed"));
  RETURN_NOT_OK(buffer_->realloc(tile_size));

  return Status::Ok();
}

void Tile::advance_offset(uint64_t nbytes) {
  buffer_->advance_offset(nbytes);
}

Buffer* Tile::buffer() const {
  return buffer_;
}

uint64_t Tile::cell_size() const {
  return cell_size_;
}

Compressor Tile::compressor() const {
  return compressor_;
}

int Tile::compression_level() const {
  return compression_level_;
}

void* Tile::cur_data() const {
  return buffer_->cur_data();
}

void* Tile::data() const {
  return buffer_->data();
}

unsigned int Tile::dim_num() const {
  return dim_num_;
}

void Tile::disown_buff() {
  owns_buff_ = false;
}

bool Tile::empty() const {
  return (buffer_ == nullptr) || (buffer_->size() == 0);
}

bool Tile::full() const {
  return (buffer_->size() != 0) &&
         (buffer_->offset() == buffer_->alloced_size());
}

uint64_t Tile::offset() const {
  return buffer_->offset();
}

Status Tile::realloc(uint64_t nbytes) {
  return buffer_->realloc(nbytes);
}

Status Tile::read(void* buffer, uint64_t nbytes) {
  RETURN_NOT_OK(buffer_->read(buffer, nbytes));

  return Status::Ok();
}

void Tile::reset() {
  reset_offset();
  reset_size();
}

void Tile::reset_offset() {
  buffer_->reset_offset();
}

void Tile::reset_size() {
  buffer_->reset_size();
}

void Tile::set_offset(uint64_t offset) {
  buffer_->set_offset(offset);
}

void Tile::set_size(uint64_t size) {
  buffer_->set_size(size);
}

uint64_t Tile::size() const {
  return buffer_->size();
}
void Tile::split_coordinates() {
  assert(dim_num_ > 0);

  // For easy reference
  uint64_t tile_size = buffer_->size();
  uint64_t coord_size = cell_size_ / dim_num_;
  uint64_t cell_num = tile_size / cell_size_;
  auto tile_c = (char*)buffer_->data();
  uint64_t ptr = 0, ptr_tmp = 0;

  // Create a tile clone
  auto tile_tmp = (char*)std::malloc(tile_size);
  std::memcpy(tile_tmp, tile_c, tile_size);

  // Split coordinates
  for (unsigned int j = 0; j < dim_num_; ++j) {
    ptr_tmp = j * coord_size;
    for (uint64_t i = 0; i < cell_num; ++i) {
      std::memcpy(tile_c + ptr, tile_tmp + ptr_tmp, coord_size);
      ptr += coord_size;
      ptr_tmp += cell_size_;
    }
  }

  // Clean up
  std::free((void*)tile_tmp);
}

bool Tile::stores_coords() const {
  return dim_num_ > 0;
}

Datatype Tile::type() const {
  return type_;
}

Status Tile::write(ConstBuffer* buf) {
  buffer_->write(buf);

  return Status::Ok();
}

Status Tile::write(ConstBuffer* buf, uint64_t nbytes) {
  RETURN_NOT_OK(buffer_->write(buf, nbytes));

  return Status::Ok();
}

Status Tile::write(const void* data, uint64_t nbytes) {
  return buffer_->write(data, nbytes);
}

Status Tile::write_with_shift(ConstBuffer* buf, uint64_t offset) {
  buffer_->write_with_shift(buf, offset);

  return Status::Ok();
}

void Tile::zip_coordinates() {
  assert(dim_num_ > 0);

  // For easy reference
  uint64_t tile_size = buffer_->size();
  uint64_t coord_size = cell_size_ / dim_num_;
  uint64_t cell_num = tile_size / cell_size_;
  auto tile_c = (char*)buffer_->data();
  uint64_t ptr = 0, ptr_tmp = 0;

  // Create a tile clone
  auto tile_tmp = (char*)std::malloc(tile_size);
  std::memcpy(tile_tmp, tile_c, tile_size);

  // Zip coordinates
  for (unsigned int j = 0; j < dim_num_; ++j) {
    ptr = j * coord_size;
    for (uint64_t i = 0; i < cell_num; ++i) {
      std::memcpy(tile_c + ptr, tile_tmp + ptr_tmp, coord_size);
      ptr += cell_size_;
      ptr_tmp += coord_size;
    }
  }

  // Clean up
  std::free((void*)tile_tmp);
}

Tile& Tile::operator=(const Tile& tile) {
  if (owns_buff_) {
    delete buffer_;
    buffer_ = nullptr;
  }

  cell_size_ = tile.cell_size_;
  compressor_ = tile.compressor_;
  compression_level_ = tile.compression_level_;
  dim_num_ = tile.dim_num_;
  owns_buff_ = tile.owns_buff_;
  type_ = tile.type_;

  if (!tile.owns_buff_) {
    buffer_ = tile.buffer_;
  } else {
    if (tile.buffer_ == nullptr)
      buffer_ = nullptr;
    else {
      if (buffer_ == nullptr)
        buffer_ = new Buffer();
      *buffer_ = *tile.buffer_;
    }
  }

  return *this;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

/**
 * Implement json serialization for Tile
 *
 * @param j json object to store serialized data in
 * @param t Tile to serialize
 */
void to_json(nlohmann::json& j, const Tile t) {
  j = {{"type", datatype_str(t.type())},
       {"compressor", compressor_str(t.compressor())},
       {"compressor_level", t.compression_level()},
       //{"tile_size", t.size()},
       {"cell_size", t.cell_size()},
       {"dim_num", t.dim_num()}};

  if (t.buffer() != nullptr) {
    switch (t.type()) {
      case tiledb::sm::Datatype::INT8:
        j["buffer"] = std::vector<int8_t>(
            static_cast<int8_t*>(t.buffer()->data()),
            static_cast<int8_t*>(t.buffer()->data()) + t.buffer()->size());
        break;
      case tiledb::sm::Datatype::STRING_ASCII:
      case tiledb::sm::Datatype::STRING_UTF8:
      case tiledb::sm::Datatype::UINT8:
        j["buffer"] = std::vector<uint8_t>(
            static_cast<uint8_t*>(t.buffer()->data()),
            static_cast<uint8_t*>(t.buffer()->data()) + t.buffer()->size());
        break;
      case tiledb::sm::Datatype::INT16:
        j["buffer"] = std::vector<int16_t>(
            static_cast<int16_t*>(t.buffer()->data()),
            static_cast<int16_t*>(t.buffer()->data()) + t.buffer()->size());
        break;
      case tiledb::sm::Datatype::STRING_UTF16:
      case tiledb::sm::Datatype::STRING_UCS2:
      case tiledb::sm::Datatype::UINT16:
        j["buffer"] = std::vector<uint16_t>(
            static_cast<uint16_t*>(t.buffer()->data()),
            static_cast<uint16_t*>(t.buffer()->data()) + t.buffer()->size());
        break;
      case tiledb::sm::Datatype::INT32:
        j["buffer"] = std::vector<int32_t>(
            static_cast<int32_t*>(t.buffer()->data()),
            static_cast<int32_t*>(t.buffer()->data()) + t.buffer()->size());
        break;
      case tiledb::sm::Datatype::STRING_UTF32:
      case tiledb::sm::Datatype::STRING_UCS4:
      case tiledb::sm::Datatype::UINT32:
        j["buffer"] = std::vector<uint32_t>(
            static_cast<uint32_t*>(t.buffer()->data()),
            static_cast<uint32_t*>(t.buffer()->data()) + t.buffer()->size());
        break;
      case tiledb::sm::Datatype::INT64:
        j["buffer"] = std::vector<int64_t>(
            static_cast<int64_t*>(t.buffer()->data()),
            static_cast<int64_t*>(t.buffer()->data()) + t.buffer()->size());
        break;
      case tiledb::sm::Datatype::UINT64:
        j["buffer"] = std::vector<uint64_t>(
            static_cast<uint64_t*>(t.buffer()->data()),
            static_cast<uint64_t*>(t.buffer()->data()) + t.buffer()->size());
        break;
      case tiledb::sm::Datatype::FLOAT32:
        j["buffer"] = std::vector<float>(
            static_cast<float*>(t.buffer()->data()),
            static_cast<float*>(t.buffer()->data()) + t.buffer()->size());
        break;
      case tiledb::sm::Datatype::FLOAT64:
        j["buffer"] = std::vector<double>(
            static_cast<double*>(t.buffer()->data()),
            static_cast<double*>(t.buffer()->data()) + t.buffer()->size());
        break;
      default:
        break;
    }
  }
};

/**
 * Implement json de-serialization for Tile
 *
 * @param j  json containing serialized data
 * @param t Tile to deserialize to
 */
void from_json(const nlohmann::json& j, Tile& t) {
  tiledb::sm::Datatype datatype = tiledb::sm::Datatype::ANY;
  auto st = datatype_enum(j.at("type"), &datatype);
  if (!st.ok())
    LOG_STATUS(st);
  tiledb::sm::Compressor compressor = tiledb::sm::Compressor::NO_COMPRESSION;

  st = compressor_enum(j.at("compressor"), &compressor);
  if (!st.ok())
    LOG_STATUS(st);

  if (j.count("buffer") && !j.at("buffer").empty()) {
    uint64_t buffer_size = 0;
    void* buffer_array;
    switch (t.type()) {
      case tiledb::sm::Datatype::INT8: {
        std::vector<int8_t> vec = j.at("buffer");
        buffer_size = datatype_size(t.type()) * vec.size();
        buffer_array = malloc(buffer_size);
        memcpy(buffer_array, vec.data(), buffer_size);
        break;
      }
      case tiledb::sm::Datatype::STRING_ASCII:
      case tiledb::sm::Datatype::STRING_UTF8:
      case tiledb::sm::Datatype::UINT8: {
        std::vector<int8_t> vec = j.at("buffer");
        buffer_size = datatype_size(t.type()) * vec.size();
        buffer_array = malloc(buffer_size);
        memcpy(buffer_array, vec.data(), buffer_size);
        break;
      }
      case tiledb::sm::Datatype::INT16: {
        std::vector<int16_t> vec = j.at("buffer");
        buffer_size = datatype_size(t.type()) * vec.size();
        buffer_array = malloc(buffer_size);
        memcpy(buffer_array, vec.data(), buffer_size);
        break;
      }
      case tiledb::sm::Datatype::STRING_UTF16:
      case tiledb::sm::Datatype::STRING_UCS2:
      case tiledb::sm::Datatype::UINT16: {
        std::vector<uint16_t> vec = j.at("buffer");
        buffer_size = datatype_size(t.type()) * vec.size();
        buffer_array = malloc(buffer_size);
        memcpy(buffer_array, vec.data(), buffer_size);
        break;
      }
      case tiledb::sm::Datatype::INT32: {
        std::vector<int32_t> vec = j.at("buffer");
        buffer_size = datatype_size(t.type()) * vec.size();
        buffer_array = malloc(buffer_size);
        memcpy(buffer_array, vec.data(), buffer_size);
        break;
      }
      case tiledb::sm::Datatype::STRING_UTF32:
      case tiledb::sm::Datatype::STRING_UCS4:
      case tiledb::sm::Datatype::UINT32: {
        std::vector<uint32_t> vec = j.at("buffer");
        buffer_size = datatype_size(t.type()) * vec.size();
        buffer_array = malloc(buffer_size);
        memcpy(buffer_array, vec.data(), buffer_size);
        break;
      }
      case tiledb::sm::Datatype::INT64: {
        std::vector<int64_t> vec = j.at("buffer");
        buffer_size = datatype_size(t.type()) * vec.size();
        buffer_array = malloc(buffer_size);
        memcpy(buffer_array, vec.data(), buffer_size);
        break;
      }
      case tiledb::sm::Datatype::UINT64: {
        std::vector<uint64_t> vec = j.at("buffer");
        buffer_size = datatype_size(t.type()) * vec.size();
        buffer_array = malloc(buffer_size);
        memcpy(buffer_array, vec.data(), buffer_size);
        break;
      }
      case tiledb::sm::Datatype::FLOAT32: {
        std::vector<float> vec = j.at("buffer");
        buffer_size = datatype_size(t.type()) * vec.size();
        buffer_array = malloc(buffer_size);
        memcpy(buffer_array, vec.data(), buffer_size);
        break;
      }
      case tiledb::sm::Datatype::FLOAT64: {
        std::vector<double> vec = j.at("buffer");
        buffer_size = datatype_size(t.type()) * vec.size();
        buffer_array = malloc(buffer_size);
        memcpy(buffer_array, vec.data(), buffer_size);
        break;
      }
      default:
        break;
    }
    tiledb::sm::Buffer* buff = new Buffer(buffer_array, buffer_size, true);
    t = tiledb::sm::Tile(
        datatype,
        compressor,
        j.at("compressor_level").get<int64_t>(),
        // j.at("tile_size").get<uint64_t>(),
        j.at("cell_size").get<int64_t>(),
        j.at("dim_num").get<int64_t>(),
        buff,
        true);
  } else {
    t.init(
        datatype,
        compressor,
        j.at("compressor_level").get<int64_t>(),
        0,  // tile_size
        j.at("cell_size").get<int64_t>(),
        j.at("dim_num").get<int64_t>());
  }
};

}  // namespace sm
}  // namespace tiledb
