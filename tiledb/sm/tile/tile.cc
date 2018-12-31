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

Tile::Tile(const Tile& tile)
    : Tile() {
  // Make a deep-copy clone
  auto clone = tile.clone(true);
  // Swap with the clone
  swap(clone);
}

Tile::Tile(Tile&& tile)
    : Tile() {
  // Swap with the argument
  swap(tile);
}

Tile::~Tile() {
  if (owns_buff_)
    delete buffer_;
}

Tile& Tile::operator=(const Tile& tile) {
  // Free existing buffer if owned.
  if (owns_buff_) {
    delete buffer_;
    buffer_ = nullptr;
    owns_buff_ = false;
  }

  // Make a deep-copy clone
  auto clone = tile.clone(true);
  // Swap with the clone
  swap(clone);

  return *this;
}

Tile& Tile::operator=(Tile&& tile) {
  // Swap with the argument
  swap(tile);

  return *this;
}

/* ****************************** */
/*               API              */
/* ****************************** */

uint64_t Tile::cell_num() const {
  return size() / cell_size_;
}

Status Tile::init(
    uint32_t format_version,
    Datatype type,
    uint64_t cell_size,
    unsigned int dim_num) {
  cell_size_ = cell_size;
  dim_num_ = dim_num;
  type_ = type;
  format_version_ = format_version;

  buffer_ = new Buffer();
  if (buffer_ == nullptr)
    return LOG_STATUS(
        Status::TileError("Cannot initialize tile; Buffer allocation failed"));

  return Status::Ok();
}

Status Tile::init(
    uint32_t format_version,
    Datatype type,
    uint64_t tile_size,
    uint64_t cell_size,
    unsigned int dim_num) {
  cell_size_ = cell_size;
  dim_num_ = dim_num;
  type_ = type;
  format_version_ = format_version;

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

Tile Tile::clone(bool deep_copy) const {
  Tile clone;
  clone.cell_size_ = cell_size_;
  clone.dim_num_ = dim_num_;
  clone.filtered_ = filtered_;
  clone.format_version_ = format_version_;
  clone.pre_filtered_size_ = pre_filtered_size_;
  clone.type_ = type_;

  if (deep_copy) {
    clone.owns_buff_ = owns_buff_;
    if (owns_buff_ && buffer_ != nullptr) {
      clone.buffer_ = new Buffer();
      // Calls Buffer copy-assign, which calls memcpy.
      *clone.buffer_ = *buffer_;
    } else {
      // this->buffer_ is either nullptr, or not owned. Just copy the pointer.
      clone.buffer_ = buffer_;
    }
  } else {
    clone.owns_buff_ = false;
    clone.buffer_ = buffer_;
  }

  return clone;
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

uint32_t Tile::format_version() const {
  return format_version_;
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

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void Tile::swap(Tile& tile) {
  // Note swapping buffer pointers here.
  std::swap(buffer_, tile.buffer_);
  std::swap(cell_size_, tile.cell_size_);
  std::swap(dim_num_, tile.dim_num_);
  std::swap(filtered_, tile.filtered_);
  std::swap(format_version_, tile.format_version_);
  std::swap(owns_buff_, tile.owns_buff_);
  std::swap(pre_filtered_size_, tile.pre_filtered_size_);
  std::swap(type_, tile.type_);
}

}  // namespace sm
}  // namespace tiledb
