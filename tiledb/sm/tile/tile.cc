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
#include "tiledb/sm/misc/stats.h"

#include <iostream>

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Tile::Tile() {
  buffer_ = nullptr;
  cell_size_ = 0;
  dim_num_ = 0;
  filtered_ = false;
  owns_buff_ = true;
  pre_filtered_size_ = 0;
  type_ = Datatype::INT32;
}

Tile::Tile(unsigned int dim_num)
    : dim_num_(dim_num) {
  buffer_ = nullptr;
  cell_size_ = 0;
  filtered_ = false;
  owns_buff_ = true;
  pre_filtered_size_ = 0;
  type_ = Datatype::INT32;
}

Tile::Tile(
    Datatype type,
    uint64_t cell_size,
    unsigned int dim_num,
    Buffer* buff,
    bool owns_buff)
    : buffer_(buff)
    , cell_size_(cell_size)
    , dim_num_(dim_num)
    , filtered_(false)
    , owns_buff_(owns_buff)
    , pre_filtered_size_(0)
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

Status Tile::capnp(::Tile::Builder* tileBuilder) const {
  STATS_FUNC_IN(serialization_tile_capnp);
  tileBuilder->setType(datatype_str(this->type()));
  tileBuilder->setCellSize(this->cell_size());
  tileBuilder->setDimNum(this->dim_num());

  ::Tile::Buffer::Builder bufferBuilder = tileBuilder->initBuffer();

  if (this->buffer() != nullptr) {
    uint64_t type_size = datatype_size(this->type());
    switch (this->type()) {
      case tiledb::sm::Datatype::INT8:
        bufferBuilder.setInt8(kj::arrayPtr(
            static_cast<int8_t*>(this->buffer()->data()),
            this->buffer()->size() / type_size));
        break;
      case tiledb::sm::Datatype::STRING_ASCII:
      case tiledb::sm::Datatype::STRING_UTF8:
      case tiledb::sm::Datatype::UINT8:
        bufferBuilder.setUint8(kj::arrayPtr(
            static_cast<uint8_t*>(this->buffer()->data()),
            this->buffer()->size() / type_size));
        break;
      case tiledb::sm::Datatype::INT16:
        bufferBuilder.setInt16(kj::arrayPtr(
            static_cast<int16_t*>(this->buffer()->data()),
            this->buffer()->size() / type_size));
        break;
      case tiledb::sm::Datatype::STRING_UTF16:
      case tiledb::sm::Datatype::STRING_UCS2:
      case tiledb::sm::Datatype::UINT16:
        bufferBuilder.setUint16(kj::arrayPtr(
            static_cast<uint16_t*>(this->buffer()->data()),
            this->buffer()->size() / type_size));
        break;
      case tiledb::sm::Datatype::INT32:
        bufferBuilder.setInt32(kj::arrayPtr(
            static_cast<int32_t*>(this->buffer()->data()),
            this->buffer()->size() / type_size));
        break;
      case tiledb::sm::Datatype::STRING_UTF32:
      case tiledb::sm::Datatype::STRING_UCS4:
      case tiledb::sm::Datatype::UINT32:
        bufferBuilder.setUint32(kj::arrayPtr(
            static_cast<uint32_t*>(this->buffer()->data()),
            this->buffer()->size() / type_size));
        break;
      case tiledb::sm::Datatype::INT64:
        bufferBuilder.setInt64(kj::arrayPtr(
            static_cast<int64_t*>(this->buffer()->data()),
            this->buffer()->size() / type_size));
        break;
      case tiledb::sm::Datatype::UINT64:
        bufferBuilder.setUint64(kj::arrayPtr(
            static_cast<uint64_t*>(this->buffer()->data()),
            this->buffer()->size() / type_size));
        break;
      case tiledb::sm::Datatype::FLOAT32:
        bufferBuilder.setFloat32(kj::arrayPtr(
            static_cast<float*>(this->buffer()->data()),
            this->buffer()->size() / type_size));
        break;
      case tiledb::sm::Datatype::FLOAT64:
        bufferBuilder.setFloat64(kj::arrayPtr(
            static_cast<double*>(this->buffer()->data()),
            this->buffer()->size() / type_size));
        break;
      default:
        break;
    }
  }

  return Status::Ok();
  STATS_FUNC_OUT(serialization_tile_capnp);
}

uint64_t Tile::cell_num() const {
  return size() / cell_size_;
}

Status Tile::init(Datatype type, uint64_t cell_size, unsigned int dim_num) {
  cell_size_ = cell_size;
  dim_num_ = dim_num;
  type_ = type;

  buffer_ = new Buffer();
  if (buffer_ == nullptr)
    return LOG_STATUS(
        Status::TileError("Cannot initialize tile; Buffer allocation failed"));

  return Status::Ok();
}

Status Tile::from_capnp(::Tile::Reader* tileReader) {
  STATS_FUNC_IN(serialization_tile_from_capnp);
  Status status = Status::Ok();

  Datatype datatype = Datatype::ANY;
  status = datatype_enum(tileReader->getType().cStr(), &datatype);
  if (!status.ok())
    return status;

  type_ = datatype;
  cell_size_ = tileReader->getCellSize();

  dim_num_ = tileReader->getDimNum();

  if (tileReader->getBuffer().totalSize().wordCount > 0) {
    uint64_t type_size = datatype_size(this->type());
    ::Tile::Buffer::Reader bufferReader = tileReader->getBuffer();
    switch (this->type()) {
      case tiledb::sm::Datatype::INT8: {
        if (bufferReader.hasInt8()) {
          auto buffer = bufferReader.getInt8();
          std::vector<int8_t>* new_buffer =
              new std::vector<int8_t>(buffer.size());
          for (size_t i = 0; i < buffer.size(); i++)
            (*new_buffer)[i] = buffer[i];

          // Set buffer
          this->buffer_ = new Buffer(
              new_buffer->data(), new_buffer->size() * type_size, true);
        }
        break;
      }
      case tiledb::sm::Datatype::STRING_ASCII:
      case tiledb::sm::Datatype::STRING_UTF8:
      case tiledb::sm::Datatype::UINT8: {
        if (bufferReader.hasUint8()) {
          auto buffer = bufferReader.getUint8();
          std::vector<uint8_t>* new_buffer =
              new std::vector<uint8_t>(buffer.size());
          for (size_t i = 0; i < buffer.size(); i++)
            (*new_buffer)[i] = buffer[i];

          // Set buffer
          this->buffer_ = new Buffer(
              new_buffer->data(), new_buffer->size() * type_size, true);
        }
        break;
      }
      case tiledb::sm::Datatype::INT16: {
        if (bufferReader.hasInt16()) {
          auto buffer = bufferReader.getInt16();
          std::vector<int16_t>* new_buffer =
              new std::vector<int16_t>(buffer.size());
          for (size_t i = 0; i < buffer.size(); i++)
            (*new_buffer)[i] = buffer[i];

          // Set buffer
          this->buffer_ = new Buffer(
              new_buffer->data(), new_buffer->size() * type_size, true);
        }
        break;
      }
      case tiledb::sm::Datatype::STRING_UTF16:
      case tiledb::sm::Datatype::STRING_UCS2:
      case tiledb::sm::Datatype::UINT16: {
        if (bufferReader.hasUint16()) {
          auto buffer = bufferReader.getUint16();
          std::vector<uint16_t>* new_buffer =
              new std::vector<uint16_t>(buffer.size());
          for (size_t i = 0; i < buffer.size(); i++)
            (*new_buffer)[i] = buffer[i];

          // Set buffer
          this->buffer_ = new Buffer(
              new_buffer->data(), new_buffer->size() * type_size, true);
        }
        break;
      }
      case tiledb::sm::Datatype::INT32: {
        if (bufferReader.hasInt32()) {
          auto buffer = bufferReader.getInt32();
          std::vector<int32_t>* new_buffer =
              new std::vector<int32_t>(buffer.size());
          for (size_t i = 0; i < buffer.size(); i++)
            (*new_buffer)[i] = buffer[i];

          // Set buffer
          this->buffer_ = new Buffer(
              new_buffer->data(), new_buffer->size() * type_size, true);
        }
        break;
      }
      case tiledb::sm::Datatype::STRING_UTF32:
      case tiledb::sm::Datatype::STRING_UCS4:
      case tiledb::sm::Datatype::UINT32: {
        if (bufferReader.hasUint32()) {
          auto buffer = bufferReader.getUint32();
          std::vector<uint32_t>* new_buffer =
              new std::vector<uint32_t>(buffer.size());
          for (size_t i = 0; i < buffer.size(); i++)
            (*new_buffer)[i] = buffer[i];

          // Set buffer
          this->buffer_ = new Buffer(
              new_buffer->data(), new_buffer->size() * type_size, true);
        }
        break;
      }
      case tiledb::sm::Datatype::INT64: {
        if (bufferReader.hasInt64()) {
          auto buffer = bufferReader.getInt64();
          std::vector<int64_t>* new_buffer =
              new std::vector<int64_t>(buffer.size());
          for (size_t i = 0; i < buffer.size(); i++)
            (*new_buffer)[i] = buffer[i];

          // Set buffer
          this->buffer_ = new Buffer(
              new_buffer->data(), new_buffer->size() * type_size, true);
        }
        break;
      }
      case tiledb::sm::Datatype::UINT64: {
        if (bufferReader.hasUint64()) {
          auto buffer = bufferReader.getUint64();
          std::vector<uint64_t>* new_buffer =
              new std::vector<uint64_t>(buffer.size());
          for (size_t i = 0; i < buffer.size(); i++)
            (*new_buffer)[i] = buffer[i];

          // Set buffer
          this->buffer_ = new Buffer(
              new_buffer->data(), new_buffer->size() * type_size, true);
        }
        break;
      }
      case tiledb::sm::Datatype::FLOAT32: {
        if (bufferReader.hasFloat32()) {
          auto buffer = bufferReader.getFloat32();
          std::vector<float>* new_buffer =
              new std::vector<float>(buffer.size());
          for (size_t i = 0; i < buffer.size(); i++)
            (*new_buffer)[i] = buffer[i];

          // Set buffer
          this->buffer_ = new Buffer(
              new_buffer->data(), new_buffer->size() * type_size, true);
        }
        break;
      }
      case tiledb::sm::Datatype::FLOAT64: {
        if (bufferReader.hasFloat64()) {
          auto buffer = bufferReader.getFloat64();
          std::vector<double>* new_buffer =
              new std::vector<double>(buffer.size());
          for (size_t i = 0; i < buffer.size(); i++)
            (*new_buffer)[i] = buffer[i];

          // Set buffer
          this->buffer_ = new Buffer(
              new_buffer->data(), new_buffer->size() * type_size, true);
        }
        break;
      }
      default:
        break;
    }
  }

  return status;
  STATS_FUNC_OUT(serialization_tile_from_capnp);
}

Status Tile::init(
    Datatype type,
    uint64_t tile_size,
    uint64_t cell_size,
    unsigned int dim_num) {
  cell_size_ = cell_size;
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

bool Tile::filtered() const {
  return filtered_;
}

bool Tile::full() const {
  return (buffer_->size() != 0) &&
         (buffer_->offset() == buffer_->alloced_size());
}

uint64_t Tile::offset() const {
  return buffer_->offset();
}

uint64_t Tile::pre_filtered_size() const {
  return pre_filtered_size_;
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

void Tile::set_filtered(bool filtered) {
  filtered_ = filtered;
}

void Tile::set_offset(uint64_t offset) {
  buffer_->set_offset(offset);
}

void Tile::set_pre_filtered_size(uint64_t pre_filtered_size) {
  pre_filtered_size_ = pre_filtered_size;
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
  dim_num_ = tile.dim_num_;
  filtered_ = tile.filtered_;
  owns_buff_ = tile.owns_buff_;
  pre_filtered_size_ = tile.pre_filtered_size_;
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

}  // namespace sm
}  // namespace tiledb
