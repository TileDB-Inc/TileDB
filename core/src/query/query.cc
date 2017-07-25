/**
 * @file   query.cc
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
 * This file implements the Query class.
 */

#include "query.h"
#include "logger.h"

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Query::Query(Array* array) {
  set_default();
  array_ = array;
  query_type_ = QueryType::READ;
}

Query::Query(Metadata* metadata) {
  set_default();
  metadata_ = metadata;
}

Query::~Query() {
  if (subarray_ != nullptr)
    free(subarray_);
  for (auto& abuf : attribute_buffers_)
    delete abuf;
  for (auto& dbuf : dimension_buffers_)
    delete dbuf;
}

/* ****************************** */
/*               API              */
/* ****************************** */

const Array* Query::array() const {
  return array_;
}

ArrayType Query::array_type() const {
  return array_->array_schema()->array_type();
}

bool Query::async() const {
  return async_;
}

const std::vector<AttributeBuffer*>& Query::attribute_buffers() const {
  return attribute_buffers_;
}

FragmentMetadata* Query::fragment_metadata() const {
  return fragment_metadata_;
}

Status Query::check() const {
  if (subarray_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Invalid query; unspecified subarray"));

  if (attribute_buffers_.empty() && dimension_buffers_.empty())
    return LOG_STATUS(Status::QueryError(
        "Invalid query; unspecified attribute/dimension buffers"));

  // Check subarray against query type
  // TODO

  return Status::Ok();
}

bool Query::completed() const {
  return completed_;
}

const std::vector<DimensionBuffer*>& Query::dimension_buffers() const {
  return dimension_buffers_;
}

Status Query::overflow(const char* name, bool* overflow) {
  // Check if the attribute buffer exists
  AttributeBuffer* abuf = attribute_buffer(name);
  if (abuf != nullptr) {
    *overflow = abuf->overflow();
    return Status::Ok();
  }

  // Check if the dimension buffer exists
  DimensionBuffer* dbuf = dimension_buffer(name);
  if (dbuf != nullptr) {
    *overflow = dbuf->overflow();
    return Status::Ok();
  }

  // Error
  return LOG_STATUS(Status::QueryError("Invalid attribute or dimension name"));
}

QueryType Query::query_type() const {
  return query_type_;
}

void Query::set_async(void* (*callback)(void*), void* callback_data) {
  async_ = true;
  callback_ = callback;
  callback_data_ = callback_data;
}

Status Query::set_attribute_buffer(
    const char* name, void* buffer, uint64_t buffer_size) {
  // Check if the attribute buffer exists already
  AttributeBuffer* abuf = attribute_buffer(name);
  if (abuf != nullptr) {
    Status st = abuf->set(buffer, buffer_size);
    return !st.ok() ? LOG_STATUS(st) : st;
  }

  // Find attribute
  const Attribute* attr = nullptr;
  if (array_ != nullptr)
    attr = array_->array_schema()->attribute(name);
  else  // Metadata
    attr = metadata_->metadata_schema()->attribute(name);
  if (attr == nullptr)
    return LOG_STATUS(Status::QueryError("Invalid attribute; unknown name"));

  // Create and append attribute buffer
  abuf = new AttributeBuffer();
  Status st = abuf->set(attr, buffer, buffer_size);
  if (!st.ok()) {
    delete abuf;
    return LOG_STATUS(st);
  }
  attribute_buffers_.push_back(abuf);
  attribute_buffers_map_[name] = abuf;
  completed_map_[attr] = false;

  // Success
  return Status::Ok();
}

Status Query::set_attribute_buffer(
    const char* name,
    void* buffer,
    uint64_t buffer_size,
    void* buffer_var,
    uint64_t buffer_var_size) {
  // Check if the attribute buffer exists already
  AttributeBuffer* abuf = attribute_buffer(name);
  if (abuf != nullptr) {
    Status st = abuf->set(buffer, buffer_size, buffer_var, buffer_var_size);
    return !st.ok() ? LOG_STATUS(st) : st;
  }

  // Find attribute
  const Attribute* attr = nullptr;
  if (array_ != nullptr)
    attr = array_->array_schema()->attribute(name);
  else  // Metadata
    attr = metadata_->metadata_schema()->attribute(name);
  if (attr == nullptr)
    return LOG_STATUS(Status::QueryError("Invalid attribute; unknown name"));

  // Create and append attribute buffer
  abuf = new AttributeBuffer();
  Status st = abuf->set(attr, buffer, buffer_size, buffer_var, buffer_var_size);
  if (!st.ok()) {
    delete abuf;
    return LOG_STATUS(st);
  }
  attribute_buffers_.push_back(abuf);
  attribute_buffers_map_[name] = abuf;
  completed_map_[attr] = false;

  // Success
  return Status::Ok();
}

void Query::set_fragment_metadata(FragmentMetadata* fragment_metadata) {
  if (fragment_metadata_ != nullptr)
    delete fragment_metadata_;
  fragment_metadata_ = fragment_metadata;
}

Status Query::set_dimension_buffer(
    const char* name, void* buffer, uint64_t buffer_size) {
  // Sanity check
  if (metadata_ != nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set dimension in a metadata query"));

  // Check if the dimension buffer exists already
  DimensionBuffer* dbuf = dimension_buffer(name);
  if (dbuf != nullptr) {
    Status st = dbuf->set(buffer, buffer_size);
    return !st.ok() ? LOG_STATUS(st) : st;
  }

  // Find dimension
  assert(array_ != nullptr);
  const Dimension* dim = array_->array_schema()->dimension(name);
  if (dim == nullptr)
    return LOG_STATUS(Status::QueryError("Invalid dimension; unknown name"));

  // Create and append dimension buffer
  dbuf = new DimensionBuffer();
  Status st = dbuf->set(dim, buffer, buffer_size);
  if (!st.ok()) {
    delete dbuf;
    return LOG_STATUS(st);
  }
  dimension_buffers_.push_back(dbuf);
  dimension_buffers_map_[name] = dbuf;

  // Success
  return Status::Ok();
}

void Query::set_status(QueryStatus status) {
  status_ = status;
}

void Query::set_completed(const Attribute* attr) {
  // TODO: this should apply only to WRITE_UNSORTED

  // Mark attribute as completed
  completed_map_[attr] = true;

  // Check if every attribute is completed
  completed_ = true;
  for (auto& c : completed_map_) {
    if (!c.second) {
      completed_ = false;
      break;
    }
  }

  if (completed_)
    set_status(QueryStatus::COMPLETED);
}

Status Query::set_subarray(const void* subarray) {
  // Sanity check
  if (metadata_ != nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set subarray to a metadata query"));

  // Delete previous subarray
  if (subarray_ != nullptr)
    free(subarray_);

  // Allocate memory
  assert(array_ != nullptr);
  uint64_t subarray_size = array_->array_schema()->subarray_size();
  subarray_ = malloc(subarray_size);
  if (subarray_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Failed to allocate memory for subarray"));

  // Copy subarray
  memcpy(subarray_, subarray, subarray_size);

  // Success
  return Status::Ok();
}

QueryStatus Query::status() const {
  return status_;
}

void Query::set_query_type(QueryType query_type) {
  query_type_ = query_type;
}

const void* Query::subarray() const {
  return subarray_;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

AttributeBuffer* Query::attribute_buffer(const std::string& name) {
  std::map<std::string, AttributeBuffer*>::iterator it =
      attribute_buffers_map_.find(name);
  if (it == attribute_buffers_map_.end())
    return nullptr;
  else
    return it->second;
}

DimensionBuffer* Query::dimension_buffer(const std::string& name) {
  std::map<std::string, DimensionBuffer*>::iterator it =
      dimension_buffers_map_.find(name);
  if (it == dimension_buffers_map_.end())
    return nullptr;
  else
    return it->second;
}

void Query::set_default() {
  array_ = nullptr;
  async_ = false;
  tile_metadata_ = nullptr;
  callback_ = nullptr;
  callback_data_ = nullptr;
  completed_ = false;
  metadata_ = nullptr;
  status_ = QueryStatus::UNSUBMITTED;
  subarray_ = nullptr;
}

};  // namespace tiledb
