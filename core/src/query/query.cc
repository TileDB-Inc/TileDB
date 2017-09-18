/**
 * @file   query.h
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
 * This file implements class Query.
 */

#include "query.h"
#include "logger.h"
#include "utils.h"

#include <sys/time.h>

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

namespace tiledb {

Query::Query() {
  subarray_ = nullptr;
  array_read_state_ = nullptr;
  array_sorted_read_state_ = nullptr;
  array_sorted_write_state_ = nullptr;
  callback_ = nullptr;
  callback_data_ = nullptr;
  storage_manager_ = nullptr;
}

Query::~Query() {
  if (subarray_ != nullptr)
    free(subarray_);

  delete array_read_state_;
  delete array_sorted_read_state_;
  delete array_sorted_write_state_;

  clear_fragments();
}

/* ****************************** */
/*               API              */
/* ****************************** */

void Query::add_coords() {
  int attribute_num = array_schema_->attribute_num();
  bool has_coords = false;

  for (auto id : attribute_ids_) {
    if (id == attribute_num) {
      has_coords = true;
      break;
    }
  }

  if (!has_coords)
    attribute_ids_.emplace_back(attribute_num);
}

const ArraySchema* Query::array_schema() const {
  return array_schema_;
}

Status Query::async_process() {
  Status st;
  if (is_read_type(type_))
    st = read();
  else  // WRITE MODE
    st = write();

  if (st.ok()) {  // Success
    // Check for overflow (applicable only to reads)
    if ((type_ == QueryType::READ && array_read_state_->overflow()) ||
        ((type_ == QueryType::READ_SORTED_COL ||
          type_ == QueryType::READ_SORTED_ROW) &&
         array_sorted_read_state_->overflow()))
      set_status(QueryStatus::INCOMPLETE);
    else  // Completion
      set_status(QueryStatus::COMPLETED);

    // Invoke the callback
    if (callback_ != nullptr)
      (*callback_)(callback_data_);
  } else {  // Error
    set_status(QueryStatus::FAILED);
  }

  return st;
}

const std::vector<unsigned int>& Query::attribute_ids() const {
  return attribute_ids_;
}

Status Query::clear_fragments() {
  for (auto& fragment : fragments_) {
    RETURN_NOT_OK(fragment->finalize());
    delete fragment;
  }

  fragments_.clear();

  return Status::Ok();
}

Status Query::coords_buffer_i(int* coords_buffer_i) const {
  int buffer_i = 0;
  auto attribute_id_num = (int)attribute_ids_.size();
  int attribute_num = array_schema_->attribute_num();
  for (int i = 0; i < attribute_id_num; ++i) {
    if (attribute_ids_[i] == attribute_num) {
      *coords_buffer_i = buffer_i;
      break;
    }
    if (!array_schema_->var_size(attribute_ids_[i]))  // FIXED CELLS
      ++buffer_i;
    else  // VARIABLE-SIZED CELLS
      buffer_i += 2;
  }

  // Coordinates are missing
  if (*coords_buffer_i == -1)
    return LOG_STATUS(
        Status::QueryError("Cannot find coordinates buffer index"));

  // Success
  return Status::Ok();
}

Status Query::finalize() {
  if(array_sorted_read_state_ != nullptr)
    RETURN_NOT_OK(array_sorted_read_state_->finalize());
  if(array_sorted_write_state_ != nullptr)
    RETURN_NOT_OK(array_sorted_write_state_->finalize());
  return clear_fragments();
}

const std::vector<Fragment*>& Query::fragments() const {
  return fragments_;
}

const std::vector<FragmentMetadata*>& Query::fragment_metadata() const {
  return fragment_metadata_;
}

unsigned int Query::fragment_num() const {
  return (unsigned int)fragments_.size();
}

Status Query::init(
    StorageManager* storage_manager,
    const ArraySchema* array_schema,
    const std::vector<FragmentMetadata*>& fragment_metadata,
    QueryType type,
    const void* subarray,
    const char** attributes,
    unsigned int attribute_num,
    void** buffers,
    uint64_t* buffer_sizes) {
  storage_manager_ = storage_manager;
  array_schema_ = array_schema;
  type_ = type;
  status_ = QueryStatus::INPROGRESS;
  buffers_ = buffers;
  buffer_sizes_ = buffer_sizes;
  fragment_metadata_ = fragment_metadata;

  RETURN_NOT_OK(set_attributes(attributes, attribute_num));
  RETURN_NOT_OK(set_subarray(subarray));
  RETURN_NOT_OK(init_fragments(fragment_metadata));
  RETURN_NOT_OK(init_states());

  return Status::Ok();
}

Status Query::init(
    StorageManager* storage_manager,
    const ArraySchema* array_schema,
    const std::vector<FragmentMetadata*>& fragment_metadata,
    QueryType type,
    const void* subarray,
    const std::vector<unsigned int>& attribute_ids,
    void** buffers,
    uint64_t* buffer_sizes,
    bool add_coords) {
  storage_manager_ = storage_manager;
  array_schema_ = array_schema;
  type_ = type;
  attribute_ids_ = attribute_ids;
  status_ = QueryStatus::INPROGRESS;
  buffers_ = buffers;
  buffer_sizes_ = buffer_sizes;
  fragment_metadata_ = fragment_metadata;

  if (add_coords)
    this->add_coords();

  RETURN_NOT_OK(set_subarray(subarray));
  RETURN_NOT_OK(init_fragments(fragment_metadata));
  RETURN_NOT_OK(init_states());

  return Status::Ok();
}

bool Query::overflow() const {
  // Not applicable to writes
  if (!is_read_type(type_))
    return false;

  // Check overflow
  if (array_sorted_read_state_ != nullptr)
    return array_sorted_read_state_->overflow();

  return array_read_state_->overflow();
}

bool Query::overflow(int attribute_id) const {
  assert(is_read_type(type_));

  // Trivial case
  if (fragments_.empty())
    return false;

  // Check overflow
  if (array_sorted_read_state_ != nullptr)
    return array_sorted_read_state_->overflow(attribute_id);

  return array_read_state_->overflow(attribute_id);
}

Status Query::overflow(
    const char* attribute_name, unsigned int* overflow) const {
  unsigned int attribute_id;
  RETURN_NOT_OK(array_schema_->attribute_id(attribute_name, &attribute_id));

  *overflow = 0;
  for (auto id : attribute_ids_) {
    if (id == attribute_id) {
      *overflow = 1;
      break;
    }
  }

  return Status::Ok();
}

Status Query::read() {
  // Check if there are no fragments
  unsigned int buffer_i = 0;
  auto attribute_id_num = (unsigned int)attribute_ids_.size();
  if (fragments_.empty()) {
    for (unsigned int i = 0; i < attribute_id_num; ++i) {
      // Update all sizes to 0
      buffer_sizes_[buffer_i] = 0;
      if (!array_schema_->var_size(attribute_ids_[i]))
        ++buffer_i;
      else
        buffer_i += 2;
    }
    return Status::Ok();
  }

  // Handle sorted modes
  if (type_ == QueryType::READ_SORTED_COL ||
      type_ == QueryType::READ_SORTED_ROW)
    return array_sorted_read_state_->read(buffers_, buffer_sizes_);

  // mode_ == TILEDB_ARRAY_READ
  return array_read_state_->read(buffers_, buffer_sizes_);
}

Status Query::read(void** buffers, uint64_t* buffer_sizes) {
  return array_read_state_->read(buffers, buffer_sizes);
}

void Query::set_buffers(void** buffers, uint64_t* buffer_sizes) {
  buffers_ = buffers;
  buffer_sizes_ = buffer_sizes;
}

void Query::set_callback(void* (*callback)(void*), void* callback_data) {
  callback_ = callback;
  callback_data_ = callback_data;
}

void Query::set_status(QueryStatus status) {
  status_ = status;
}

QueryStatus Query::status() const {
  return status_;
}

StorageManager* Query::storage_manager() const {
  return storage_manager_;
}

const void* Query::subarray() const {
  return subarray_;
}

QueryType Query::type() const {
  return type_;
}

Status Query::write() {
  // Write based on mode
  if (type_ == QueryType::WRITE_SORTED_COL ||
      type_ == QueryType::WRITE_SORTED_ROW) {
    RETURN_NOT_OK(array_sorted_write_state_->write(buffers_, buffer_sizes_));
  } else if (type_ == QueryType::WRITE || type_ == QueryType::WRITE_UNSORTED) {
    RETURN_NOT_OK(write(buffers_, buffer_sizes_));
  } else {
    assert(0);
  }

  // In all modes except WRITE, the fragment must be finalized
  if (type_ != QueryType::WRITE)
    clear_fragments();

  return Status::Ok();
}

Status Query::write(void** buffers, uint64_t* buffer_sizes) {
  // Sanity checks
  if (!is_write_type(type_)) {
    return LOG_STATUS(
        Status::QueryError("Cannot write to array_schema; Invalid mode"));
  }

  // Create and initialize a new fragment
  if (fragment_num() == 0) {
    // Get new fragment name
    std::string new_fragment_name = this->new_fragment_name();
    if (new_fragment_name.empty()) {
      return LOG_STATUS(Status::QueryError("Cannot produce new fragment name"));
    }


    // Create new fragment
    auto fragment = new Fragment(this);
    fragments_.push_back(fragment);
    RETURN_NOT_OK(fragment->init(URI(new_fragment_name), subarray_));
  }

  // Dispatch the write command to the new fragment
  RETURN_NOT_OK(fragments_[0]->write(buffers, buffer_sizes));

  // Success
  return Status::Ok();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status Query::init_fragments(
    const std::vector<FragmentMetadata*>& fragment_metadata) {
  if (is_write_type(type_)) {
    RETURN_NOT_OK(new_fragment());
  } else if (is_read_type(type_)) {
    RETURN_NOT_OK(open_fragments(fragment_metadata));
  }

  return Status::Ok();
}

Status Query::init_states() {
  // Initialize new fragment if needed
  if (type_ == QueryType::WRITE_SORTED_COL ||
      type_ == QueryType::WRITE_SORTED_ROW) {
    array_sorted_write_state_ = new ArraySortedWriteState(this);
    Status st = array_sorted_write_state_->init();
    if (!st.ok()) {
      delete array_sorted_write_state_;
      array_sorted_write_state_ = nullptr;
      return st;
    }
  } else if (type_ == QueryType::READ) {
    array_read_state_ = new ArrayReadState(this);
  } else if (
      type_ == QueryType::READ_SORTED_COL ||
      type_ == QueryType::READ_SORTED_ROW) {
    array_read_state_ = new ArrayReadState(this);
    array_sorted_read_state_ = new ArraySortedReadState(this);
    Status st = array_sorted_read_state_->init();
    if (!st.ok()) {
      delete array_sorted_read_state_;
      array_sorted_read_state_ = nullptr;
      return st;
    }
  }

  return Status::Ok();
}

Status Query::new_fragment() {
  // Get new fragment name
  std::string new_fragment_name = this->new_fragment_name();

  if (new_fragment_name.empty())
    return LOG_STATUS(Status::QueryError("Cannot produce new fragment name"));

  // Create new fragment
  auto fragment = new Fragment(this);
  RETURN_NOT_OK_ELSE(
      fragment->init(URI(new_fragment_name), subarray_), delete fragment);
  fragments_.push_back(fragment);

  return Status::Ok();
}

std::string Query::new_fragment_name() const {
  // For easy reference
  unsigned name_max_len = constants::name_max_len;

  struct timeval tp = {};
  gettimeofday(&tp, nullptr);
  uint64_t ms = (uint64_t)tp.tv_sec * 1000L + tp.tv_usec / 1000;
  pthread_t self = pthread_self();
  uint64_t tid = 0;
  memcpy(&tid, &self, std::min(sizeof(self), sizeof(tid)));
  char fragment_name[name_max_len];

  // Generate fragment name
  int n = sprintf(
      fragment_name,
      "%s/.__%llu_%llu",
      array_schema_->array_uri().to_string().c_str(),
      tid,
      ms);

  // Handle error
  if (n < 0)
    return "";

  // Return
  return std::string(fragment_name);
}

Status Query::open_fragments(const std::vector<FragmentMetadata*>& metadata) {
  // Create a fragment object for each fragment directory
  for (auto meta : metadata) {
    auto fragment = new Fragment(this);
    RETURN_NOT_OK(fragment->init(meta->fragment_uri(), meta));
    fragments_.emplace_back(fragment);
  }
  return Status::Ok();
}

Status Query::set_attributes(
    const char** attributes, unsigned int attribute_num) {
  // Get attributes
  std::vector<std::string> attributes_vec;
  if (attributes == nullptr) {  // Default: all attributes
    attributes_vec = array_schema_->attribute_names();
    if (array_schema_->dense() && type_ != QueryType::WRITE_UNSORTED)
      // Remove coordinates attribute for dense arrays,
      // unless in TILEDB_WRITE_UNSORTED mode
      attributes_vec.pop_back();
  } else {  // Custom attributes
    // Get attributes
    unsigned name_max_len = constants::name_max_len;
    for (unsigned int i = 0; i < attribute_num; ++i) {
      // Check attribute name length
      if (attributes[i] == nullptr || strlen(attributes[i]) > name_max_len)
        return LOG_STATUS(Status::QueryError("Invalid attribute name length"));
      attributes_vec.emplace_back(attributes[i]);
    }

    // Sanity check on duplicates
    if (utils::has_duplicates(attributes_vec))
      return LOG_STATUS(Status::QueryError(
          "Cannot initialize array_schema; Duplicate attributes"));
  }

  // Set attribute ids
  RETURN_NOT_OK(
      array_schema_->get_attribute_ids(attributes_vec, attribute_ids_));

  return Status::Ok();
}

Status Query::set_subarray(const void* subarray) {
  uint64_t subarray_size = 2 * array_schema_->coords_size();

  if (subarray_ == nullptr)
    subarray_ = malloc(subarray_size);

  if (subarray_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Memory allocation for subarray failed"));

  if (subarray == nullptr)
    std::memcpy(subarray_, array_schema_->domain(), subarray_size);
  else
    std::memcpy(subarray_, subarray, subarray_size);

  return Status::Ok();
}

}  // namespace tiledb
