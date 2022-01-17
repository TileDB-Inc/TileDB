/**
 * @file   writer_tile.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file implements class WriterTile.
 */

#include "tiledb/sm/tile/writer_tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

WriterTile::WriterTile()
    : Tile()
    , pre_filtered_size_(0)
    , min_size_(0)
    , max_size_(0)
    , null_count_(0) {
}

/* ****************************** */
/*               API              */
/* ****************************** */

uint64_t WriterTile::pre_filtered_size() const {
  return pre_filtered_size_;
}

void WriterTile::set_pre_filtered_size(uint64_t pre_filtered_size) {
  pre_filtered_size_ = pre_filtered_size;
}

void* WriterTile::min() const {
  return (void*)min_.data();
}

void* WriterTile::max() const {
  return (void*)max_.data();
}

std::tuple<
    const void*,
    uint64_t,
    const void*,
    uint64_t,
    const ByteVec*,
    uint64_t>
WriterTile::metadata() const {
  return {min_.data(), min_size_, max_.data(), max_size_, &sum_, null_count_};
}

void WriterTile::set_metadata(const std::tuple<
                              const void*,
                              uint64_t,
                              const void*,
                              uint64_t,
                              const ByteVec*,
                              uint64_t>& md) {
  const auto& [min, min_size, max, max_size, sum, null_count] = md;
  assert(sum != nullptr);

  min_.resize(min_size);
  min_size_ = min_size;
  if (min != nullptr) {
    memcpy(min_.data(), min, min_size);
  }

  max_.resize(max_size);
  max_size_ = max_size;
  if (max != nullptr) {
    memcpy(max_.data(), max, max_size);
  }

  sum_ = *sum;
  null_count_ = null_count;
}

Status WriterTile::write_var(
    const void* data, uint64_t offset, uint64_t nbytes) {
  if (size_ - offset < nbytes) {
    auto new_alloc_size = size_ == 0 ? offset + nbytes : size_;
    while (new_alloc_size < offset + nbytes)
      new_alloc_size *= 2;

    auto new_data = static_cast<char*>(tdb_realloc(data_, new_alloc_size));
    if (new_data == nullptr) {
      return LOG_STATUS(Status_TileError(
          "Cannot reallocate buffer; Memory allocation failed"));
    }
    data_ = new_data;
    size_ = new_alloc_size;
  }

  return write(data, offset, nbytes);
}

WriterTile WriterTile::clone() const {
  WriterTile clone;
  clone.pre_filtered_size_ = pre_filtered_size_;
  clone.min_ = min_;
  clone.max_ = max_;
  clone.sum_ = sum_;
  clone.null_count_ = null_count_;
  clone.cell_size_ = cell_size_;
  clone.dim_num_ = dim_num_;
  clone.format_version_ = format_version_;
  clone.type_ = type_;
  clone.filtered_buffer_ = filtered_buffer_;

  if (data_ != nullptr) {
    clone.data_ = static_cast<char*>(tdb_malloc(size_));
    memcpy(clone.data_, data_, size_);
  } else {
    clone.data_ = data_;
  }
  clone.size_ = size_;

  return clone;
}

}  // namespace sm
}  // namespace tiledb
