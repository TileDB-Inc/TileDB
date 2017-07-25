/**
 * @file   attribute_buffer.cc
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
 * This file implements class AttributeBuffer.
 */

#include "dimension_buffer.h"
#include "logger.h"

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

DimensionBuffer::DimensionBuffer() {
  dim_ = nullptr;
  buf_ = nullptr;
}

DimensionBuffer::~DimensionBuffer() {
  if (buf_ != nullptr)
    delete buf_;
}

/* ****************************** */
/*               API              */
/* ****************************** */

bool DimensionBuffer::overflow() const {
  return buf_->overflow();
}

Status DimensionBuffer::set(void* buffer, uint64_t buffer_size) {
  if (dim_ == nullptr)
    return LOG_STATUS(Status::DimensionBufferError(
        "Cannot set buffers; dimension has not been set"));

  if (buf_ != nullptr)
    delete buf_;
  buf_ = new Buffer(buffer, buffer_size);

  // Success
  return Status::Ok();
}

Status DimensionBuffer::set(
    const Dimension* dim, void* buffer, uint64_t buffer_size) {
  if (dim == nullptr)
    return LOG_STATUS(
        Status::DimensionBufferError("Cannot set buffers; dimension is null"));

  dim_ = dim;

  if (buf_ != nullptr)
    delete buf_;
  buf_ = new Buffer(buffer, buffer_size);

  // Success
  return Status::Ok();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

}  // namespace tiledb
