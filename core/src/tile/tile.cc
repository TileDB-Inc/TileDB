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
    , type_(type) {
  buffer_ = new Buffer();
  buffer_->realloc(tile_size);
  owns_buff_ = true;
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
  buffer_ = new Buffer();
  compression_level_ = -1;
  owns_buff_ = true;
}

Tile::~Tile() {
  if (owns_buff_)
    delete buffer_;
}

/* ****************************** */
/*               API              */
/* ****************************** */

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
  return buffer_->size() == 0;
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
  auto tile_tmp = (char*)malloc(tile_size);
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
  auto tile_tmp = (char*)malloc(tile_size);
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

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

}  // namespace tiledb