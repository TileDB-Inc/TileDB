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

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Tile::Tile() {
  buffer_ = nullptr;
  buffer_var_ = nullptr;
  buffer_size_ = 0;
  buffer_var_size_ = 0;
  buffer_offset_ = 0;
  buffer_var_offset_ = 0;
  tile_size_ = 0;
}

Tile::Tile(uint64_t tile_size)
    : tile_size_(tile_size) {
  buffer_ = nullptr;
  buffer_var_ = nullptr;
  buffer_size_ = 0;
  buffer_var_size_ = 0;
  buffer_offset_ = 0;
  buffer_var_offset_ = 0;
}

Tile::~Tile() {
  if (buffer_ != nullptr)
    free(buffer_);
  if (buffer_var_ != nullptr)
    free(buffer_);
}

/* ****************************** */
/*               API              */
/* ****************************** */

void* Tile::buffer() const {
  return buffer_;
}

uint64_t Tile::buffer_size() const {
  return buffer_size_;
}

bool Tile::full() const {
  return buffer_offset_ == buffer_size_;
}

void Tile::reset() {
  buffer_offset_ = 0;
  buffer_var_offset_ = 0;
}

Status Tile::write(ConstBuffer* buf) {
  if (buffer_ == nullptr) {
    buffer_ = malloc(tile_size_);
    buffer_size_ = tile_size_;
  }

  if (buffer_ == nullptr)
    LOG_STATUS(
        Status::TileError("Cannot write into tile; Buffer allocation failed"));

  uint64_t bytes_read =
      buf->read((char*)buffer_ + buffer_offset_, buffer_size_ - buffer_offset_);
  buffer_offset_ += bytes_read;

  return Status::Ok();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

}  // namespace tiledb