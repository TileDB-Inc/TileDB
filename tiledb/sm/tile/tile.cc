/**
 * @file   tile.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/logger.h"

#include <iostream>

namespace tiledb {
namespace sm {

/* ****************************** */
/*           STATIC API           */
/* ****************************** */

Status Tile::compute_chunk_size(
    const uint64_t tile_size,
    const uint32_t tile_dim_num,
    const uint64_t tile_cell_size,
    uint32_t* const chunk_size) {
  const uint32_t dim_num = tile_dim_num > 0 ? tile_dim_num : 1;
  const uint64_t dim_tile_size = tile_size / dim_num;
  const uint64_t dim_cell_size = tile_cell_size / dim_num;

  uint64_t chunk_size64 =
      std::min(constants::max_tile_chunk_size, dim_tile_size);
  chunk_size64 = chunk_size64 / dim_cell_size * dim_cell_size;
  chunk_size64 = std::max(chunk_size64, dim_cell_size);
  if (chunk_size64 > std::numeric_limits<uint32_t>::max()) {
    return LOG_STATUS(Status::TileError("Chunk size exceeds uint32_t"));
  }

  *chunk_size = chunk_size64;
  return Status::Ok();
}

Status Tile::buffer_to_contigious_fixed_chunks(
    const Buffer& buffer,
    const uint32_t tile_dim_num,
    const uint64_t tile_cell_size,
    ChunkedBuffer* chunked_buffer) {
  return buffer_to_contigious_fixed_chunks(
      buffer.data(),
      buffer.size(),
      tile_dim_num,
      tile_cell_size,
      chunked_buffer);
}

Status Tile::buffer_to_contigious_fixed_chunks(
    void* buffer,
    const uint64_t buffer_size,
    const uint32_t tile_dim_num,
    const uint64_t tile_cell_size,
    ChunkedBuffer* chunked_buffer) {
  // Calculate the chunk size for 'buff'.
  uint32_t chunk_size;
  RETURN_NOT_OK(compute_chunk_size(
      buffer_size, tile_dim_num, tile_cell_size, &chunk_size));

  // Initialize contigious, fixed size 'chunked_buffer_'.
  RETURN_NOT_OK(chunked_buffer->init_fixed_size(
      ChunkedBuffer::BufferAddressing::CONTIGIOUS, buffer_size, chunk_size));

  RETURN_NOT_OK(chunked_buffer->set_contigious(buffer));
  RETURN_NOT_OK(chunked_buffer->set_size(buffer_size));

  return Status::Ok();
}

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Tile::Tile() {
  chunked_buffer_ = nullptr;
  offset_ = 0;
  cell_size_ = 0;
  dim_num_ = 0;
  owns_chunked_buffer_ = true;
  pre_filtered_size_ = 0;
  format_version_ = 0;
  type_ = Datatype::INT32;
}

Tile::Tile(
    const Datatype type,
    const uint64_t cell_size,
    const unsigned int dim_num,
    ChunkedBuffer* const chunked_buffer,
    const bool owns_buff)
    : chunked_buffer_(chunked_buffer)
    , offset_(0)
    , cell_size_(cell_size)
    , dim_num_(dim_num)
    , format_version_(0)
    , owns_chunked_buffer_(owns_buff)
    , pre_filtered_size_(0)
    , type_(type) {
}

Tile::Tile(
    const uint32_t format_version,
    const Datatype type,
    const uint64_t cell_size,
    const unsigned int dim_num,
    ChunkedBuffer* const chunked_buffer,
    const bool owns_buff)
    : chunked_buffer_(chunked_buffer)
    , offset_(0)
    , cell_size_(cell_size)
    , dim_num_(dim_num)
    , format_version_(format_version)
    , owns_chunked_buffer_(owns_buff)
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
  if (owns_chunked_buffer_ && chunked_buffer_ != nullptr) {
    chunked_buffer_->free();
    delete chunked_buffer_;
  }
}

Tile& Tile::operator=(const Tile& tile) {
  // Free existing buffer if owned.
  if (owns_chunked_buffer_) {
    if (chunked_buffer_) {
      chunked_buffer_->free();
      delete chunked_buffer_;
      chunked_buffer_ = nullptr;
    }
    owns_chunked_buffer_ = false;
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

Status Tile::init_unfiltered(
    uint32_t format_version,
    Datatype type,
    uint64_t tile_size,
    uint64_t cell_size,
    unsigned int dim_num) {
  cell_size_ = cell_size;
  dim_num_ = dim_num;
  type_ = type;
  format_version_ = format_version;

  chunked_buffer_ = new ChunkedBuffer();
  if (chunked_buffer_ == nullptr)
    return LOG_STATUS(Status::TileError(
        "Cannot initialize tile; ChunkedBuffer allocation failed"));

  uint32_t chunk_size;
  RETURN_NOT_OK(
      compute_chunk_size(tile_size, dim_num, cell_size_, &chunk_size));
  RETURN_NOT_OK(chunked_buffer_->init_fixed_size(
      ChunkedBuffer::BufferAddressing::DISCRETE, tile_size, chunk_size));

  return Status::Ok();
}

Status Tile::init_filtered(
    uint32_t format_version,
    Datatype type,
    uint64_t cell_size,
    unsigned int dim_num) {
  cell_size_ = cell_size;
  dim_num_ = dim_num;
  type_ = type;
  format_version_ = format_version;

  chunked_buffer_ = new ChunkedBuffer();
  if (chunked_buffer_ == nullptr)
    return LOG_STATUS(Status::TileError(
        "Cannot initialize tile; ChunkedBuffer allocation failed"));

  return Status::Ok();
}

void Tile::advance_offset(uint64_t nbytes) {
  offset_ += nbytes;
}

ChunkedBuffer* Tile::chunked_buffer() const {
  return chunked_buffer_;
}

Tile Tile::clone(bool deep_copy) const {
  Tile clone;
  clone.cell_size_ = cell_size_;
  clone.dim_num_ = dim_num_;
  clone.format_version_ = format_version_;
  clone.pre_filtered_size_ = pre_filtered_size_;
  clone.type_ = type_;
  clone.offset_ = offset_;
  clone.filtered_buffer_ = filtered_buffer_;

  if (deep_copy) {
    clone.owns_chunked_buffer_ = owns_chunked_buffer_;
    if (owns_chunked_buffer_ && chunked_buffer_ != nullptr) {
      clone.chunked_buffer_ = new ChunkedBuffer();
      // Calls ChunkedBuffer copy-assign, which performs a deep copy.
      *clone.chunked_buffer_ = *chunked_buffer_;
    } else {
      clone.chunked_buffer_ = chunked_buffer_;
    }
  } else {
    clone.owns_chunked_buffer_ = false;
    clone.chunked_buffer_ = chunked_buffer_;
  }

  return clone;
}

uint64_t Tile::cell_size() const {
  return cell_size_;
}

unsigned int Tile::dim_num() const {
  return dim_num_;
}

void Tile::disown_buff() {
  owns_chunked_buffer_ = false;
}

bool Tile::owns_buff() const {
  return owns_chunked_buffer_;
}

bool Tile::empty() const {
  assert(!filtered());
  return (chunked_buffer_ == nullptr) || (chunked_buffer_->size() == 0);
}

bool Tile::filtered() const {
  assert(!(filtered_buffer_.alloced_size() > 0 && chunked_buffer_->size() > 0));
  return filtered_buffer_.alloced_size() > 0;
}

Buffer* Tile::filtered_buffer() {
  return &filtered_buffer_;
}

uint32_t Tile::format_version() const {
  return format_version_;
}

bool Tile::full() const {
  assert(!filtered());
  return !empty() && offset_ >= chunked_buffer_->capacity();
}

uint64_t Tile::offset() const {
  return offset_;
}

uint64_t Tile::pre_filtered_size() const {
  return pre_filtered_size_;
}

Status Tile::read(void* buffer, uint64_t nbytes) {
  assert(!filtered());
  RETURN_NOT_OK(chunked_buffer_->read(buffer, nbytes, offset_));
  offset_ += nbytes;

  return Status::Ok();
}

Status Tile::read(
    void* const buffer, const uint64_t nbytes, const uint64_t offset) const {
  assert(!filtered());
  return chunked_buffer_->read(buffer, nbytes, offset);
}

void Tile::reset() {
  reset_offset();
  reset_size();
}

void Tile::reset_offset() {
  offset_ = 0;
}

void Tile::reset_size() {
  assert(!filtered());
  chunked_buffer_->set_size(0);
}

void Tile::set_offset(uint64_t offset) {
  offset_ = offset;
}

void Tile::set_pre_filtered_size(uint64_t pre_filtered_size) {
  pre_filtered_size_ = pre_filtered_size;
}

uint64_t Tile::size() const {
  assert(!filtered());
  return (chunked_buffer_ == nullptr) ? 0 : chunked_buffer_->size();
}

bool Tile::stores_coords() const {
  return dim_num_ > 0;
}

Datatype Tile::type() const {
  return type_;
}

Status Tile::write(ConstBuffer* buf) {
  assert(!filtered());
  RETURN_NOT_OK(chunked_buffer_->write(buf->cur_data(), buf->size(), offset_));
  offset_ += buf->size();

  return Status::Ok();
}

Status Tile::write(ConstBuffer* buf, uint64_t nbytes) {
  assert(!filtered());
  RETURN_NOT_OK(chunked_buffer_->write(buf->cur_data(), nbytes, offset_));
  offset_ += nbytes;

  return Status::Ok();
}

Status Tile::write(const void* data, uint64_t nbytes) {
  assert(!filtered());
  RETURN_NOT_OK(chunked_buffer_->write(data, nbytes, offset_));
  offset_ += nbytes;

  return Status::Ok();
}

Status Tile::zip_coordinates() {
  assert(dim_num_ > 0);

  // For easy reference
  const uint64_t tile_size = chunked_buffer_->size();
  const uint64_t coord_size = cell_size_ / dim_num_;
  const uint64_t cell_num = tile_size / cell_size_;

  // Coordinate tiles are always contigiously allocated.
  assert(
      chunked_buffer_->buffer_addressing() ==
      ChunkedBuffer::BufferAddressing::CONTIGIOUS);

  // Fetch the internal, contigious buffer.
  void* buffer;
  RETURN_NOT_OK(chunked_buffer_->get_contigious(&buffer));
  char* const tile_c = static_cast<char*>(buffer);

  // Create a tile clone
  char* const tile_tmp = static_cast<char*>(std::malloc(tile_size));
  assert(tile_tmp);
  std::memcpy(tile_tmp, tile_c, tile_size);

  // Zip coordinates
  uint64_t ptr_tmp = 0;
  for (unsigned int j = 0; j < dim_num_; ++j) {
    uint64_t ptr = j * coord_size;
    for (uint64_t i = 0; i < cell_num; ++i) {
      std::memcpy(tile_c + ptr, tile_tmp + ptr_tmp, coord_size);
      ptr += cell_size_;
      ptr_tmp += coord_size;
    }
  }

  // Clean up
  std::free((void*)tile_tmp);

  return Status::Ok();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void Tile::swap(Tile& tile) {
  // Note swapping buffer pointers here.
  std::swap(filtered_buffer_, tile.filtered_buffer_);
  std::swap(chunked_buffer_, tile.chunked_buffer_);
  std::swap(offset_, tile.offset_);
  std::swap(cell_size_, tile.cell_size_);
  std::swap(dim_num_, tile.dim_num_);
  std::swap(format_version_, tile.format_version_);
  std::swap(owns_chunked_buffer_, tile.owns_chunked_buffer_);
  std::swap(pre_filtered_size_, tile.pre_filtered_size_);
  std::swap(type_, tile.type_);
}

}  // namespace sm
}  // namespace tiledb
