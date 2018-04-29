/**
 * @file   query.cc
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
 * This file implements class Query.
 */

#include "tiledb/sm/query/query.h"
#include "tiledb/sm/misc/logger.h"

#include <cassert>
#include <iostream>

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Query::Query(QueryType type)
    : type_(type) {
  callback_ = nullptr;
  callback_data_ = nullptr;
  status_ = QueryStatus::INPROGRESS;
}

Query::~Query() = default;

/* ****************************** */
/*               API              */
/* ****************************** */

const ArraySchema* Query::array_schema() const {
  if (type_ == QueryType::WRITE)
    return writer_.array_schema();
  return reader_.array_schema();
}

Status Query::compute_subarrays(
    void* subarray, std::vector<void*>* subarrays) const {
  if (type_ != QueryType::READ)
    return LOG_STATUS(
        Status::Error("Cannot compute subarrays for write queries"));
  return reader_.compute_subarrays(subarray, subarrays);
}

Status Query::finalize() {
  if (type_ == QueryType::READ)
    return reader_.finalize();
  return writer_.finalize();
}

unsigned Query::fragment_num() const {
  if (type_ == QueryType::WRITE)
    return 0;
  return reader_.fragment_num();
}

std::vector<URI> Query::fragment_uris() const {
  std::vector<URI> uris;
  if (type_ == QueryType::WRITE)
    return uris;
  return reader_.fragment_uris();
}

Status Query::init() {
  if (type_ == QueryType::READ)
    return reader_.init();
  return writer_.init();
}

URI Query::last_fragment_uri() const {
  if (type_ == QueryType::WRITE)
    return URI();
  return reader_.last_fragment_uri();
}

Layout Query::layout() const {
  if (type_ == QueryType::WRITE)
    return writer_.layout();
  return reader_.layout();
}

Status Query::process() {
  status_ = QueryStatus::INPROGRESS;

  Status st = Status::Ok();
  if (type_ == QueryType::READ)
    st = reader_.read();
  else  // WRITE MODE
    st = writer_.write();

  if (st.ok()) {  // Success
    if (callback_ != nullptr)
      callback_(callback_data_);
    status_ = QueryStatus::COMPLETED;
  } else {  // Error
    status_ = QueryStatus::FAILED;
  }

  return st;
}

void Query::set_array_schema(const ArraySchema* array_schema) {
  if (type_ == QueryType::READ)
    reader_.set_array_schema(array_schema);
  else
    writer_.set_array_schema(array_schema);
}

Status Query::set_buffers(
    const char** attributes,
    unsigned int attribute_num,
    void** buffers,
    uint64_t* buffer_sizes) {
  if (type_ == QueryType::WRITE)
    return writer_.set_buffers(
        attributes, attribute_num, buffers, buffer_sizes);
  return reader_.set_buffers(attributes, attribute_num, buffers, buffer_sizes);
}

void Query::set_buffers(void** buffers, uint64_t* buffer_sizes) {
  if (type_ == QueryType::WRITE)
    writer_.set_buffers(buffers, buffer_sizes);
  reader_.set_buffers(buffers, buffer_sizes);
}

void Query::set_callback(
    const std::function<void(void*)>& callback, void* callback_data) {
  callback_ = callback;
  callback_data_ = callback_data;
}

void Query::set_fragment_metadata(
    const std::vector<FragmentMetadata*>& fragment_metadata) {
  if (type_ == QueryType::READ)
    reader_.set_fragment_metadata(fragment_metadata);
}

Status Query::set_layout(Layout layout) {
  if (type_ == QueryType::WRITE)
    return writer_.set_layout(layout);
  return reader_.set_layout(layout);
}

void Query::set_storage_manager(StorageManager* storage_manager) {
  if (type_ == QueryType::WRITE)
    writer_.set_storage_manager(storage_manager);
  else
    reader_.set_storage_manager(storage_manager);
}

Status Query::set_subarray(const void* subarray) {
  if (type_ == QueryType::WRITE)
    return writer_.set_subarray(subarray);
  return reader_.set_subarray(subarray);
}

QueryStatus Query::status() const {
  return status_;
}

QueryType Query::type() const {
  return type_;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

}  // namespace sm
}  // namespace tiledb
