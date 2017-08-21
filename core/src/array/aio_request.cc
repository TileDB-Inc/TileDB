/**
 * @file   aio_request.cc
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
 * This file implements the AIORequest class.
 */

#include "aio_request.h"

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

AIORequest::AIORequest() {
  query_ = nullptr;
  buffers_ = nullptr;
  buffer_sizes_ = nullptr;
  completion_handle_ = nullptr;
  completion_data_ = nullptr;
  overflow_ = nullptr;
  status_ = nullptr;
  subarray_ = nullptr;
  id_ = 0;
}

AIORequest::~AIORequest() = default;

/* ****************************** */
/*           ACCESSORS             */
/* ****************************** */

Query* AIORequest::query() const {
  return query_;
}

void** AIORequest::buffers() const {
  return buffers_;
}

size_t* AIORequest::buffer_sizes() const {
  return buffer_sizes_;
}

bool AIORequest::has_callback() const {
  return completion_handle_ != nullptr;
}

void AIORequest::exec_callback() const {
  (*completion_handle_)(completion_data_);
}

size_t AIORequest::id() const {
  return id_;
}

QueryMode AIORequest::mode() const {
  return mode_;
}

bool* AIORequest::overflow() const {
  return overflow_;
}

AIOStatus AIORequest::status() const {
  return *status_;
}

const void* AIORequest::subarray() const {
  return subarray_;
}

/* ****************************** */
/*           MUTATORS             */
/* ****************************** */

void AIORequest::set_query(Query* query) {
  query_ = query;
}

void AIORequest::set_buffers(void** buffers) {
  buffers_ = buffers;
}

void AIORequest::set_buffer_sizes(size_t* buffer_sizes) {
  buffer_sizes_ = buffer_sizes;
}

void AIORequest::set_callback(
    void* (*completion_handle)(void*), void* completion_data) {
  completion_handle_ = completion_handle;
  completion_data_ = completion_data;
}

void AIORequest::set_mode(QueryMode mode) {
  mode_ = mode;
}

void AIORequest::set_status(AIOStatus status) {
  *status_ = status;
}

void AIORequest::set_status(AIOStatus* status) {
  status_ = status;
}

void AIORequest::set_subarray(const void* subarray) {
  subarray_ = subarray;
}

void AIORequest::set_overflow(bool* overflow) {
  overflow_ = overflow;
}

void AIORequest::set_overflow(int i, bool overflow) {
  overflow_[i] = overflow;
}

}  // namespace tiledb
