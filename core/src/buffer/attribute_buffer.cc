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

#include "attribute_buffer.h"
#include "logger.h"

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

AttributeBuffer::AttributeBuffer() {
  attr_ = nullptr;
  buf_ = nullptr;
  buf_var_ = nullptr;
}

AttributeBuffer::~AttributeBuffer() {
  if (buf_ != nullptr)
    delete buf_;
  if (buf_var_ != nullptr)
    delete buf_var_;
}

/* ****************************** */
/*               API              */
/* ****************************** */

const Attribute* AttributeBuffer::attribute() const {
  return attr_;
}

bool AttributeBuffer::overflow() const {
  return buf_->overflow() || buf_var_->overflow();
}

Status AttributeBuffer::set(void* buffer, uint64_t buffer_size) {
  if (attr_ == nullptr)
    return LOG_STATUS(Status::AttributeBufferError(
        "Cannot set buffers; attribute has not been set"));

  if (attr_->var_size())
    return LOG_STATUS(Status::AttributeBufferError(
        "Cannot set buffers; attribute must be fixed-sized"));

  if (buf_ != nullptr)
    delete buf_;
  buf_ = new Buffer(buffer, buffer_size);

  // Success
  return Status::Ok();
}

Status AttributeBuffer::set(
    const Attribute* attr, void* buffer, uint64_t buffer_size) {
  if (attr == nullptr)
    return LOG_STATUS(
        Status::AttributeBufferError("Cannot set buffers; attribute is null"));

  if (attr->var_size())
    return LOG_STATUS(Status::AttributeBufferError(
        "Cannot set buffers; attribute must be fixed-sized"));

  attr_ = attr;

  if (buf_ != nullptr)
    delete buf_;
  buf_ = new Buffer(buffer, buffer_size);

  // Success
  return Status::Ok();
}

Status AttributeBuffer::set(
    void* buffer,
    uint64_t buffer_size,
    void* buffer_var,
    uint64_t buffer_var_size) {
  if (attr_ == nullptr)
    return LOG_STATUS(Status::AttributeBufferError(
        "Cannot set buffers; attribute has not been set"));

  if (!attr_->var_size())
    return LOG_STATUS(Status::AttributeBufferError(
        "Cannot set buffers; attribute must be variable-sized"));

  if (buf_ != nullptr)
    delete buf_;
  buf_ = new Buffer(buffer, buffer_size);

  if (buf_var_ != nullptr)
    delete buf_var_;
  buf_var_ = new Buffer(buffer_var, buffer_var_size);

  // Success
  return Status::Ok();
}

Status AttributeBuffer::set(
    const Attribute* attr,
    void* buffer,
    uint64_t buffer_size,
    void* buffer_var,
    uint64_t buffer_var_size) {
  if (attr == nullptr)
    return LOG_STATUS(
        Status::AttributeBufferError("Cannot set buffers; attribute is null"));

  if (!attr->var_size())
    return LOG_STATUS(Status::AttributeBufferError(
        "Cannot set buffers; attribute must be variable-sized"));

  attr_ = attr;

  if (buf_ != nullptr)
    delete buf_;
  buf_ = new Buffer(buffer, buffer_size);

  if (buf_var_ != nullptr)
    delete buf_var_;
  buf_var_ = new Buffer(buffer_var, buffer_var_size);

  // Success
  return Status::Ok();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

}  // namespace tiledb
