/**
 * @file   tile.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include "tile.h"
#include "logger.h"

#include <iostream>

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Tile::Tile(unsigned int dim_num) {
  buffer_ = nullptr;
  cell_size_ = 0;
  compressor_ = Compressor::NO_COMPRESSION;
  compression_level_ = -1;
  dim_num_ = dim_num;
  offset_ = 0;
  tile_size_ = 0;
  type_ = Datatype::INT32;
}

Tile::Tile(
    Datatype type,
    Compressor compressor,
    int compression_level,
    uint64_t tile_size,
    uint64_t cell_size,
    unsigned int dim_num)
    : cell_size_(cell_size)
    , compressor_(compressor)
    , compression_level_(compression_level)
    , dim_num_(dim_num)
    , tile_size_(tile_size)
    , type_(type) {
  buffer_ = nullptr;
  offset_ = 0;
}

Tile::Tile(
    Datatype type,
    Compressor compressor,
    uint64_t cell_size,
    unsigned int dim_num)
    : cell_size_(cell_size)
    , compressor_(compressor)
    , dim_num_(dim_num)
    , type_(type) {
  buffer_ = nullptr;
  offset_ = 0;
  tile_size_ = 0;
}

Tile::~Tile() {
  delete buffer_;
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status Tile::alloc(uint64_t size) {
  if (buffer_ == nullptr)
    buffer_ = new Buffer(size);
  else {
    buffer_->realloc(size);
    buffer_->reset_offset();
  }
  tile_size_ = size;

  return Status::Ok();
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

void* Tile::data() const {
  if (buffer_ == nullptr)
    return nullptr;

  return buffer_->data();
}

unsigned int Tile::dim_num() const {
  return dim_num_;
}

bool Tile::empty() const {
  return buffer_ == nullptr || buffer_->offset() == 0;
}

bool Tile::full() const {
  if (buffer_ == nullptr)
    return false;

  return buffer_->offset() == buffer_->size();
}

uint64_t Tile::offset() const {
  return offset_;
}

Status Tile::read(void* buffer, uint64_t nbytes) {
  if (buffer_ == nullptr)
    return LOG_STATUS(
        Status::BufferError("Cannot read from tile; Invalid buffer"));

  RETURN_NOT_OK(buffer_->read(buffer, nbytes));
  offset_ = buffer_->offset();

  return Status::Ok();
}

void Tile::reset_offset() {
  if (buffer_ != nullptr)
    buffer_->reset_offset();
  offset_ = 0;
}

void Tile::set_offset(uint64_t offset) {
  if (buffer_ != nullptr)
    buffer_->set_offset(offset);
  offset_ = offset;
}

uint64_t Tile::size() const {
  return tile_size_;
}
void Tile::split_coordinates() {
  assert(dim_num_ > 0);

  // For easy reference
  uint64_t coord_size = cell_size_ / dim_num_;
  uint64_t cell_num = tile_size_ / cell_size_;
  auto tile_c = (char*)buffer_->data();
  uint64_t ptr = 0, ptr_tmp = 0;

  // Create a tile clone
  auto tile_tmp = (char*)malloc(tile_size_);
  std::memcpy(tile_tmp, tile_c, tile_size_);

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
  if (buffer_ == nullptr)
    buffer_ = new Buffer(tile_size_);

  if (buffer_->size() == 0)
    LOG_STATUS(
        Status::TileError("Cannot write into tile; Buffer allocation failed"));

  buffer_->write(buf);
  offset_ = buffer_->offset();

  return Status::Ok();
}

Status Tile::write(ConstBuffer* buf, uint64_t nbytes) {
  if (buffer_ == nullptr)
    buffer_ = new Buffer(tile_size_);

  if (buffer_->size() == 0)
    LOG_STATUS(
        Status::TileError("Cannot write into tile; Buffer allocation failed"));

  RETURN_NOT_OK(buffer_->write(buf, nbytes));
  offset_ = buffer_->offset();

  return Status::Ok();
}

Status Tile::write_with_shift(ConstBuffer* buf, uint64_t offset) {
  if (buffer_ == nullptr)
    buffer_ = new Buffer(tile_size_);

  if (buffer_->size() == 0)
    LOG_STATUS(
        Status::TileError("Cannot write into tile; Buffer allocation failed"));

  buffer_->write_with_shift(buf, offset);
  offset_ = buffer_->offset();

  return Status::Ok();
}

void Tile::zip_coordinates() {
  assert(dim_num_ > 0);

  // For easy reference
  uint64_t coord_size = cell_size_ / dim_num_;
  uint64_t cell_num = tile_size_ / cell_size_;
  auto tile_c = (char*)buffer_->data();
  uint64_t ptr = 0, ptr_tmp = 0;

  // Create a tile clone
  auto tile_tmp = (char*)malloc(tile_size_);
  std::memcpy(tile_tmp, tile_c, tile_size_);

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

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

}  // namespace tiledb