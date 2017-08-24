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
}

Query::Query(const Query* query, QueryMode mode, const void* subarray) {
  subarray_ = nullptr;
  array_read_state_ = nullptr;
  array_sorted_read_state_ = nullptr;
  array_sorted_write_state_ = nullptr;
  mode_ = mode;
  attribute_ids_ = query->attribute_ids_;
  array_ = query->array_;

  if (!set_subarray(subarray).ok())
    return;

  if (!array_->array_schema()->dense())
    add_coords();

  Status st;
  if (mode_ == QueryMode::WRITE)
    new_fragment();
  else
    open_fragments(query->fragments_);

  init_states();
}

Query::~Query() {
  if (subarray_ != nullptr)
    free(subarray_);

  delete array_read_state_;
  delete array_sorted_read_state_;
  delete array_sorted_write_state_;

  // TODO: handle status here
  clear_fragments();
}

/* ****************************** */
/*               API              */
/* ****************************** */

void Query::add_coords() {
  int attribute_num = array_->array_schema()->attribute_num();
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

void Query::clear_fragments() {
  for (auto& fragment : fragments_) {
    // TODO: return Status
    fragment->finalize();
    delete fragment;
  }

  fragments_.clear();
}

Status Query::set_subarray(const void* subarray) {
  uint64_t subarray_size = 2 * array_->array_schema()->coords_size();

  if (subarray_ == nullptr)
    subarray_ = malloc(subarray_size);

  if (subarray_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Memory allocation for subarray failed"));

  if (subarray == nullptr)
    memcpy(subarray_, array_->array_schema()->domain(), subarray_size);
  else
    memcpy(subarray_, subarray, subarray_size);

  return Status::Ok();
}

Status Query::init(
    Array* array,
    QueryMode mode,
    const void* subarray,
    const char** attributes,
    int attribute_num,
    const std::vector<std::string>& fragment_names,
    const std::vector<FragmentMetadata*>& bookkeeping) {
  array_ = array;
  mode_ = mode;

  RETURN_NOT_OK(set_attributes(attributes, attribute_num));
  RETURN_NOT_OK(set_subarray(subarray));
  RETURN_NOT_OK(init_fragments(fragment_names, bookkeeping));
  RETURN_NOT_OK(init_states());

  return Status::Ok();
}

Status Query::set_attributes(const char** attributes, int attribute_num) {
  // Get attributes
  std::vector<std::string> attributes_vec;
  const ArraySchema* array_schema = array_->array_schema();
  if (attributes == nullptr) {  // Default: all attributes
    attributes_vec = array_schema->attributes();
    if (array_schema->dense() && mode_ != QueryMode::WRITE_UNSORTED)
      // Remove coordinates attribute for dense arrays,
      // unless in TILEDB_WRITE_UNSORTED mode
      attributes_vec.pop_back();
  } else {  // Custom attributes
    // Get attributes
    unsigned name_max_len = constants::name_max_len;
    for (int i = 0; i < attribute_num; ++i) {
      // Check attribute name length
      if (attributes[i] == nullptr || strlen(attributes[i]) > name_max_len)
        return LOG_STATUS(Status::QueryError("Invalid attribute name length"));
      attributes_vec.emplace_back(attributes[i]);
    }

    // Sanity check on duplicates
    if (utils::has_duplicates(attributes_vec))
      return LOG_STATUS(
          Status::QueryError("Cannot initialize array; Duplicate attributes"));
  }

  // Set attribute ids
  RETURN_NOT_OK(
      array_schema->get_attribute_ids(attributes_vec, attribute_ids_));

  return Status::Ok();
}

Status Query::init_states() {
  // Initialize new fragment if needed
  if (mode_ == QueryMode::WRITE_SORTED_COL ||
      mode_ == QueryMode::WRITE_SORTED_ROW) {
    array_sorted_write_state_ = new ArraySortedWriteState(this);
    Status st = array_sorted_write_state_->init();
    if (!st.ok()) {
      delete array_sorted_write_state_;
      array_sorted_write_state_ = nullptr;
      return st;
    }
  } else if (mode_ == QueryMode::READ) {
    array_read_state_ = new ArrayReadState(this);
  } else if (
      mode_ == QueryMode::READ_SORTED_COL ||
      mode_ == QueryMode::READ_SORTED_ROW) {
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

Status Query::init_fragments(
    const std::vector<std::string>& fragment_names,
    const std::vector<FragmentMetadata*>& bookkeeping) {
  if (mode_ == QueryMode::WRITE) {
    RETURN_NOT_OK(new_fragment());
  } else if (is_read_mode(mode_)) {
    RETURN_NOT_OK(open_fragments(fragment_names, bookkeeping));
  }

  return Status::Ok();
}

Status Query::new_fragment() {
  // Get new fragment name
  std::string new_fragment_name = this->new_fragment_name();
  if (new_fragment_name == "")
    return LOG_STATUS(Status::QueryError("Cannot produce new fragment name"));

  // Create new fragment
  Fragment* fragment = new Fragment(this);
  RETURN_NOT_OK_ELSE(
      fragment->init(new_fragment_name, subarray_), delete fragment);
  fragments_.push_back(fragment);

  return Status::Ok();
}

const std::vector<int>& Query::attribute_ids() const {
  return attribute_ids_;
}

Status Query::coords_buffer_i(int* coords_buffer_i) const {
  int buffer_i = 0;
  auto attribute_id_num = (int)attribute_ids_.size();
  int attribute_num = array_->array_schema()->attribute_num();
  for (int i = 0; i < attribute_id_num; ++i) {
    if (attribute_ids_[i] == attribute_num) {
      *coords_buffer_i = buffer_i;
      break;
    }
    if (!array_->array_schema()->var_size(attribute_ids_[i]))  // FIXED CELLS
      ++buffer_i;
    else  // VARIABLE-SIZED CELLS
      buffer_i += 2;
  }

  // Coordinates are missing
  if (*coords_buffer_i == -1)
    return LOG_STATUS(
        Status::ArrayError("Cannot find coordinates buffer index"));

  // Success
  return Status::Ok();
}

int Query::fragment_num() const {
  return (int)fragments_.size();
}

std::vector<Fragment*> Query::fragments() const {
  return fragments_;
}

bool Query::overflow() const {
  // Not applicable to writes
  if (!is_read_mode(mode_))
    return false;

  // Check overflow
  if (array_sorted_read_state_ != nullptr)
    return array_sorted_read_state_->overflow();
  else
    return array_read_state_->overflow();
}

bool Query::overflow(int attribute_id) const {
  assert(is_read_mode(mode_));

  // Trivial case
  if (fragments_.size() == 0)
    return false;

  // Check overflow
  if (array_sorted_read_state_ != nullptr)
    return array_sorted_read_state_->overflow(attribute_id);
  else
    return array_read_state_->overflow(attribute_id);
}

QueryMode Query::mode() const {
  return mode_;
}

const void* Query::subarray() const {
  return subarray_;
}

Status Query::write_default(const void** buffers, const size_t* buffer_sizes) {
  // Sanity checks
  if (!is_write_mode(mode_)) {
    return LOG_STATUS(
        Status::ArrayError("Cannot write to array; Invalid mode"));
  }

  // Create and initialize a new fragment
  if (fragment_num() == 0) {
    // Get new fragment name
    std::string new_fragment_name = this->new_fragment_name();
    if (new_fragment_name == "") {
      return LOG_STATUS(Status::ArrayError("Cannot produce new fragment name"));
    }

    // Create new fragment
    Fragment* fragment = new Fragment(this);
    fragments_.push_back(fragment);
    RETURN_NOT_OK(fragment->init(new_fragment_name, subarray_));
  }

  // Dispatch the write command to the new fragment
  RETURN_NOT_OK(fragments_[0]->write(buffers, buffer_sizes));

  // Success
  return Status::Ok();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

std::string Query::new_fragment_name() const {
  // For easy reference
  unsigned name_max_len = constants::name_max_len;

  struct timeval tp;
  gettimeofday(&tp, nullptr);
  uint64_t ms = (uint64_t)tp.tv_sec * 1000L + tp.tv_usec / 1000;
  pthread_t self = pthread_self();
  uint64_t tid = 0;
  memcpy(&tid, &self, std::min(sizeof(self), sizeof(tid)));
  char fragment_name[name_max_len];

  // Get MAC address
  std::string mac = utils::get_mac_addr();
  if (mac == "")
    return "";

  // Generate fragment name
  int n = sprintf(
      fragment_name,
      "%s/.__%s%llu_%llu",
      array_->array_schema()->array_uri().c_str(),
      mac.c_str(),
      tid,
      ms);

  // Handle error
  if (n < 0)
    return "";

  // Return
  return fragment_name;
}

Status Query::open_fragments(
    const std::vector<std::string>& fragment_names,
    const std::vector<FragmentMetadata*>& book_keeping) {
  // Sanity check
  assert(fragment_names.size() == book_keeping.size());

  // Create a fragment object for each fragment directory
  int fragment_num = fragment_names.size();
  for (int i = 0; i < fragment_num; ++i) {
    Fragment* fragment = new Fragment(this);
    RETURN_NOT_OK(fragment->init(fragment_names[i], book_keeping[i]));
    fragments_.emplace_back(fragment);
  }
  return Status::Ok();
}

Status Query::open_fragments(const std::vector<Fragment*>& fragments) {
  for (auto& fragment : fragments) {
    // Create a fragment object for each fragment directory
    Fragment* new_fragment = new Fragment(this);
    RETURN_NOT_OK(
        new_fragment->init(fragment->fragment_uri(), fragment->bookkeeping()));
    fragments_.emplace_back(new_fragment);
  }
  return Status::Ok();
}

}  // namespace tiledb
