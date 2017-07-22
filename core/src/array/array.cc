/**
 * @file   array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file implements the Array class.
 */

#include "array.h"
#include <sys/time.h>
#include "logger.h"
#include "utils.h"

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Array::Array() {
  array_read_state_ = nullptr;
  array_sorted_read_state_ = nullptr;
  array_sorted_write_state_ = nullptr;
  array_schema_ = nullptr;
  array_clone_ = nullptr;
  storage_manager_ = nullptr;
}

Array::~Array() {
  // Applicable to both arrays and array clones
  std::vector<Fragment*>::iterator it = fragments_.begin();
  for (; it != fragments_.end(); ++it)
    if (*it != nullptr)
      delete *it;
  if (array_read_state_ != nullptr)
    delete array_read_state_;
  if (array_sorted_read_state_ != nullptr)
    delete array_sorted_read_state_;
  if (array_sorted_write_state_ != nullptr)
    delete array_sorted_write_state_;

  // Applicable only to non-clones
  if (array_clone_ != nullptr) {
    delete array_clone_;
    if (array_schema_ != nullptr)
      delete array_schema_;
  }
}

/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

StorageManager* Array::storage_manager() const {
  return storage_manager_;
}

Status Array::aio_handle_request(AIORequest* aio_request) {
  Status st;
  if (read_mode()) {  // READ MODE
    // Invoke the read
    if (aio_request->mode() == ArrayMode::READ) {
      // Reset the subarray only if this request does not continue from the last
      if (aio_last_handled_request_ != aio_request->id())
        reset_subarray_soft(aio_request->subarray());

      // Read
      st = read_default(aio_request->buffers(), aio_request->buffer_sizes());
    } else {
      // This may initiate a series of new AIO requests
      // Reset the subarray hard this time (updating also the subarray
      // of the ArraySortedReadState object.
      if (aio_last_handled_request_ != aio_request->id())
        reset_subarray(aio_request->subarray());

      // Read
      st = read(aio_request->buffers(), aio_request->buffer_sizes());
    }
  } else {  // WRITE MODE
    // Invoke the write
    if (aio_request->mode() == ArrayMode::WRITE ||
        aio_request->mode() == ArrayMode::WRITE_UNSORTED) {
      // Reset the subarray only if this request does not continue from the last
      if (aio_last_handled_request_ != aio_request->id())
        reset_subarray_soft(aio_request->subarray());

      // Write
      st = write_default(
          (const void**)aio_request->buffers(),
          (const size_t*)aio_request->buffer_sizes());
    } else {
      // This may initiate a series of new AIO requests
      // Reset the subarray hard this time (updating also the subarray
      // of the ArraySortedWriteState object.
      if (aio_last_handled_request_ != aio_request->id())
        reset_subarray(aio_request->subarray());

      // Write
      st = write(
          (const void**)aio_request->buffers(),
          (const size_t*)aio_request->buffer_sizes());
    }
  }

  if (st.ok()) {  // Success
    // Check for overflow (applicable only to reads)
    if (aio_request->mode() == ArrayMode::READ &&
        array_read_state_->overflow()) {
      aio_request->set_status(AIOStatus::OFLOW);
      if (aio_request->overflow() != nullptr) {
        for (int i = 0; i < int(attribute_ids_.size()); ++i)
          aio_request->set_overflow(
              i, array_read_state_->overflow(attribute_ids_[i]));
      }
    } else if (
        (aio_request->mode() == ArrayMode::READ_SORTED_COL ||
         aio_request->mode() == ArrayMode::READ_SORTED_ROW) &&
        array_sorted_read_state_->overflow()) {
      aio_request->set_status(AIOStatus::OFLOW);
      if (aio_request->overflow() != nullptr) {
        for (int i = 0; i < int(attribute_ids_.size()); ++i)
          aio_request->set_overflow(
              i, array_sorted_read_state_->overflow(attribute_ids_[i]));
      }
    } else {  // Completion
      aio_request->set_status(AIOStatus::COMPLETED);
    }

    // Invoke the callback
    if (aio_request->has_callback())
      aio_request->exec_callback();
  } else {  // Error
    aio_request->set_status(AIOStatus::ERROR);
  }

  aio_last_handled_request_ = aio_request->id();

  return st;
}

Array* Array::array_clone() const {
  return array_clone_;
}

const ArraySchema* Array::array_schema() const {
  return array_schema_;
}

const std::vector<int>& Array::attribute_ids() const {
  return attribute_ids_;
}

const Configurator* Array::config() const {
  return config_;
}

int Array::fragment_num() const {
  return fragments_.size();
}

std::vector<Fragment*> Array::fragments() const {
  return fragments_;
}

ArrayMode Array::mode() const {
  return mode_;
}

bool Array::overflow() const {
  // Not applicable to writes
  if (!read_mode())
    return false;

  // Check overflow
  if (array_sorted_read_state_ != nullptr)
    return array_sorted_read_state_->overflow();
  else
    return array_read_state_->overflow();
}

bool Array::overflow(int attribute_id) const {
  assert(read_mode());

  // Trivial case
  if (fragments_.size() == 0)
    return false;

  // Check overflow
  if (array_sorted_read_state_ != nullptr)
    return array_sorted_read_state_->overflow(attribute_id);
  else
    return array_read_state_->overflow(attribute_id);
}

Status Array::read(void** buffers, size_t* buffer_sizes) {
  // Sanity checks
  if (!read_mode()) {
    return LOG_STATUS(
        Status::ArrayError("Cannot read from array; Invalid mode"));
  }

  // Check if there are no fragments
  int buffer_i = 0;
  int attribute_id_num = attribute_ids_.size();
  if (fragments_.size() == 0) {
    for (int i = 0; i < attribute_id_num; ++i) {
      // Update all sizes to 0
      buffer_sizes[buffer_i] = 0;
      if (!array_schema_->var_size(attribute_ids_[i]))
        ++buffer_i;
      else
        buffer_i += 2;
    }
    return Status::Ok();
  }

  // Handle sorted modes
  if (mode_ == ArrayMode::READ_SORTED_COL ||
      mode_ == ArrayMode::READ_SORTED_ROW) {
    return array_sorted_read_state_->read(buffers, buffer_sizes);
  } else {  // mode_ == TILEDB_ARRAY_READ
    return read_default(buffers, buffer_sizes);
  }
}

Status Array::read_default(void** buffers, size_t* buffer_sizes) {
  return array_read_state_->read(buffers, buffer_sizes);
}

bool Array::read_mode() const {
  return is_read_mode(mode_);
}

bool Array::write_mode() const {
  return is_write_mode(mode_);
}

/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

Status Array::consolidate(
    Fragment*& new_fragment, std::vector<std::string>& old_fragment_names) {
  // Trivial case
  if (fragments_.size() == 1)
    return Status::Ok();

  // Get new fragment name
  std::string new_fragment_name = this->new_fragment_name();
  if (new_fragment_name == "") {
    return LOG_STATUS(Status::ArrayError("Cannot produce new fragment name"));
  }

  // Create new fragment
  new_fragment = new Fragment(this);
  RETURN_NOT_OK(
      new_fragment->init(new_fragment_name, ArrayMode::WRITE));

  // Consolidate on a per-attribute basis
  Status st;
  for (int i = 0; i < array_schema_->attribute_num() + 1; ++i) {
    st = consolidate(new_fragment, i);
    if (!st.ok()) {
      utils::delete_fragment(new_fragment->fragment_name());
      delete new_fragment;
      return st;
    }
  }

  // Get old fragment names
  int fragment_num = fragments_.size();
  for (int i = 0; i < fragment_num; ++i)
    old_fragment_names.push_back(fragments_[i]->fragment_name());

  return Status::Ok();
}

Status Array::consolidate(Fragment* new_fragment, int attribute_id) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  uint64_t consolidation_buffer_size =
      Configurator::consolidation_buffer_size();

  // Do nothing if the array is dense for the coordinates attribute
  if (array_schema_->dense() && attribute_id == attribute_num)
    return Status::Ok();

  // Prepare buffers
  void** buffers;
  size_t* buffer_sizes;

  // Count the number of variable attributes
  int var_attribute_num = array_schema_->var_attribute_num();

  // Populate the buffers
  int buffer_num = attribute_num + 1 + var_attribute_num;
  buffers = (void**)malloc(buffer_num * sizeof(void*));
  buffer_sizes = (size_t*)malloc(buffer_num * sizeof(size_t));
  int buffer_i = 0;
  for (int i = 0; i < attribute_num + 1; ++i) {
    if (i == attribute_id) {
      buffers[buffer_i] = malloc(consolidation_buffer_size);
      buffer_sizes[buffer_i] = consolidation_buffer_size;
      ++buffer_i;
      if (array_schema_->var_size(i)) {
        buffers[buffer_i] = malloc(consolidation_buffer_size);
        buffer_sizes[buffer_i] = consolidation_buffer_size;
        ++buffer_i;
      }
    } else {
      buffers[buffer_i] = nullptr;
      buffer_sizes[buffer_i] = 0;
      ++buffer_i;
      if (array_schema_->var_size(i)) {
        buffers[buffer_i] = nullptr;
        buffer_sizes[buffer_i] = 0;
        ++buffer_i;
      }
    }
  }

  // Read and write attribute until there is no overflow
  Status st_write;
  Status st_read;
  do {
    // Read
    st_read = read(buffers, buffer_sizes);
    if (!st_read.ok())
      break;

    // Write
    st_write =
        new_fragment->write((const void**)buffers, (const size_t*)buffer_sizes);
    if (!st_write.ok())
      break;
  } while (overflow(attribute_id));

  // Clean up
  for (int i = 0; i < buffer_num; ++i) {
    if (buffers[i] != nullptr)
      free(buffers[i]);
  }
  free(buffers);
  free(buffer_sizes);

  // Error
  if (!st_write.ok()) {
    return st_write;
  }
  if (!st_read.ok()) {
    return st_read;
  }
  return Status::Ok();
}

Status Array::finalize() {
  // Initializations
  Status st;
  int fragment_num = fragments_.size();
  bool fg_error = false;
  for (int i = 0; i < fragment_num; ++i) {
    st = fragments_[i]->finalize();
    if (!st.ok())
      fg_error = true;
    delete fragments_[i];
  }
  fragments_.clear();

  // Clean the array read state
  if (array_read_state_ != nullptr) {
    delete array_read_state_;
    array_read_state_ = nullptr;
  }

  // Clean the array sorted read state
  if (array_sorted_read_state_ != nullptr) {
    delete array_sorted_read_state_;
    array_sorted_read_state_ = nullptr;
  }

  // Clean the array sorted write state
  if (array_sorted_write_state_ != nullptr) {
    delete array_sorted_write_state_;
    array_sorted_write_state_ = nullptr;
  }

  // Finalize the clone
  Status st_clone;
  if (array_clone_ != nullptr)
    st_clone = array_clone_->finalize();

  // Errors
  if (!st.ok()) {
    return st;
  }
  if (!st_clone.ok()) {
    return st_clone;
  }
  if (fg_error) {
    return LOG_STATUS(Status::ArrayError("error finalizing fragment"));
  }
  return Status::Ok();
}

Status Array::init(
    StorageManager* storage_manager,
    const ArraySchema* array_schema,
    const std::vector<std::string>& fragment_names,
    const std::vector<BookKeeping*>& book_keeping,
    ArrayMode mode,
    const char** attributes,
    int attribute_num,
    const Configurator* config,
    Array* array_clone) {
  // For easy reference
  unsigned name_max_len = Configurator::name_max_len();

  // Set mode
  mode_ = mode;

  // Set array clone
  array_clone_ = array_clone;
  storage_manager_ = storage_manager;

  // Sanity check on mode
  if (!read_mode() && !write_mode()) {
    return LOG_STATUS(
        Status::ArrayError("Cannot initialize array; Invalid array mode"));
  }

  // Set config
  config_ = config;

  // Get attributes
  std::vector<std::string> attributes_vec;
  if (attributes == nullptr) {  // Default: all attributes
    attributes_vec = array_schema->attributes();
    if (array_schema->dense() && mode != ArrayMode::WRITE_UNSORTED)
      // Remove coordinates attribute for dense arrays,
      // unless in TILEDB_WRITE_UNSORTED mode
      attributes_vec.pop_back();
  } else {  // Custom attributes
    // Get attributes
    bool coords_found = false;
    bool sparse = !array_schema->dense();
    for (int i = 0; i < attribute_num; ++i) {
      // Check attribute name length
      if (attributes[i] == nullptr || strlen(attributes[i]) > name_max_len)
        return LOG_STATUS(Status::ArrayError("Invalid attribute name length"));
      attributes_vec.emplace_back(attributes[i]);
      if (!strcmp(attributes[i], Configurator::coords()))
        coords_found = true;
    }

    // Sanity check on duplicates
    if (utils::has_duplicates(attributes_vec)) {
      return LOG_STATUS(
          Status::ArrayError("Cannot initialize array; Duplicate attributes"));
    }

    // For the case of the clone sparse array, append coordinates if they do
    // not exist already
    if (sparse && array_clone == nullptr && !coords_found &&
        !utils::is_metadata(array_schema->array_name()))
      attributes_vec.emplace_back(Configurator::coords());
  }

  // Set attribute ids
  RETURN_NOT_OK(
      array_schema->get_attribute_ids(attributes_vec, attribute_ids_));

  // Set array schema
  array_schema_ = array_schema;

  // Initialize new fragment if needed
  if (write_mode()) {  // WRITE MODE
    // Get new fragment name
    std::string new_fragment_name = this->new_fragment_name();
    if (new_fragment_name == "") {
      return LOG_STATUS(Status::ArrayError("Cannot produce new fragment name"));
    }

    // Create new fragment
    Fragment* fragment = new Fragment(this);
    fragments_.push_back(fragment);
    Status st = fragment->init(new_fragment_name, mode_);
    if (!st.ok()) {
      array_schema_ = nullptr;
      return st;
    }

    // Create ArraySortedWriteState
    if (mode_ == ArrayMode::WRITE_SORTED_COL ||
        mode_ == ArrayMode::WRITE_SORTED_ROW) {
      array_sorted_write_state_ = new ArraySortedWriteState(this);
      Status st = array_sorted_write_state_->init();
      if (!st.ok()) {
        delete array_sorted_write_state_;
        array_sorted_write_state_ = nullptr;
        return st;
      }
    } else {
      array_sorted_write_state_ = nullptr;
    }
  } else {  // READ MODE
    // Open fragments
    Status st = open_fragments(fragment_names, book_keeping);
    if (!st.ok()) {
      array_schema_ = nullptr;
      return st;
    }

    // Create ArrayReadState
    array_read_state_ = new ArrayReadState(this);

    // Create ArraySortedReadState
    if (mode_ != ArrayMode::READ) {
      array_sorted_read_state_ = new ArraySortedReadState(this);
      Status st = array_sorted_read_state_->init();
      if (!st.ok()) {
        delete array_sorted_read_state_;
        array_sorted_read_state_ = nullptr;
        return st;
      }
    } else {
      array_sorted_read_state_ = nullptr;
    }
  }

  // Initialize the AIO-related members
  aio_last_handled_request_ = -1;

  // Return
  return Status::Ok();
}

Status Array::reset_attributes(const char** attributes, int attribute_num) {
  // For easy reference
  unsigned name_max_len = Configurator::name_max_len();

  // Get attributes
  std::vector<std::string> attributes_vec;
  if (attributes == nullptr) {  // Default: all attributes
    attributes_vec = array_schema_->attributes();
    if (array_schema_->dense())  // Remove coordinates attribute for dense
      attributes_vec.pop_back();
  } else {  //  Custom attributes
    // Copy attribute names
    for (int i = 0; i < attribute_num; ++i) {
      // Check attribute name length
      if (attributes[i] == nullptr || strlen(attributes[i]) > name_max_len)
        return LOG_STATUS(Status::ArrayError("Invalid attribute name length"));
      attributes_vec.emplace_back(attributes[i]);
    }

    // Sanity check on duplicates
    if (utils::has_duplicates(attributes_vec)) {
      return LOG_STATUS(
          Status::ArrayError("Cannot reset attributes; Duplicate attributes"));
    }
  }

  // Set attribute ids
  RETURN_NOT_OK(
      array_schema_->get_attribute_ids(attributes_vec, attribute_ids_));

  return Status::Ok();
}

Status Array::reset_subarray(const void* subarray) {
  // Sanity check
  assert(read_mode() || write_mode());

  // For easy referencd
  int fragment_num = fragments_.size();

  // Finalize fragments if in write mode
  if (write_mode()) {
    // Finalize and delete fragments
    for (int i = 0; i < fragment_num; ++i) {
      fragments_[i]->finalize();
      delete fragments_[i];
    }
    fragments_.clear();
  }

  // Re-set or re-initialize fragments
  if (write_mode()) {  // WRITE MODE
    // Finalize last fragment
    if (fragments_.size() != 0) {
      assert(fragments_.size() == 1);
      RETURN_NOT_OK(fragments_[0]->finalize());
      delete fragments_[0];
      fragments_.clear();
    }

    // Re-initialize ArraySortedWriteState
    if (array_sorted_write_state_ != nullptr)
      delete array_sorted_write_state_;
    if (mode_ == ArrayMode::WRITE_SORTED_COL ||
        mode_ == ArrayMode::WRITE_SORTED_ROW) {
      array_sorted_write_state_ = new ArraySortedWriteState(this);
      Status st = array_sorted_write_state_->init();
      if (!st.ok()) {
        delete array_sorted_write_state_;
        array_sorted_write_state_ = nullptr;
        return st;
      }
    } else {
      array_sorted_write_state_ = nullptr;
    }

    // Get new fragment name
    std::string new_fragment_name = this->new_fragment_name();
    if (new_fragment_name == "") {
      return LOG_STATUS(
          Status::ArrayError("Cannot generate new fragment name"));
    }

    // Create new fragment
    Fragment* fragment = new Fragment(this);
    fragments_.push_back(fragment);
    RETURN_NOT_OK(fragment->init(new_fragment_name, mode_));

  } else {  // READ MODE
    // Re-initialize the read state of the fragments
    for (int i = 0; i < fragment_num; ++i)
      fragments_[i]->reset_read_state();

    // Re-initialize array read state
    if (array_read_state_ != nullptr) {
      delete array_read_state_;
      array_read_state_ = nullptr;
    }
    array_read_state_ = new ArrayReadState(this);

    // Re-initialize ArraySortedReadState
    if (array_sorted_read_state_ != nullptr)
      delete array_sorted_read_state_;
    if (mode_ != ArrayMode::READ) {
      array_sorted_read_state_ = new ArraySortedReadState(this);
      Status st = array_sorted_read_state_->init();
      if (!st.ok()) {
        delete array_sorted_read_state_;
        array_sorted_read_state_ = nullptr;
        return st;
      }
    } else {
      array_sorted_read_state_ = nullptr;
    }
  }

  // Success
  return Status::Ok();
}

Status Array::reset_subarray_soft(const void* subarray) {
  // Sanity check
  assert(read_mode() || write_mode());

  // For easy referencd
  int fragment_num = fragments_.size();

  // Finalize fragments if in write mode
  if (write_mode()) {
    // Finalize and delete fragments
    for (int i = 0; i < fragment_num; ++i) {
      fragments_[i]->finalize();
      delete fragments_[i];
    }
    fragments_.clear();
  }

  // Re-set or re-initialize fragments
  if (write_mode()) {  // WRITE MODE
    // Do nothing
  } else {  // READ MODE
    // Re-initialize the read state of the fragments
    for (int i = 0; i < fragment_num; ++i)
      fragments_[i]->reset_read_state();

    // Re-initialize array read state
    if (array_read_state_ != nullptr) {
      delete array_read_state_;
      array_read_state_ = nullptr;
    }
    array_read_state_ = new ArrayReadState(this);
  }

  return Status::Ok();
}

Status Array::sync() {
  // Sanity check
  if (!write_mode()) {
    return LOG_STATUS(Status::ArrayError("Cannot sync array; Invalid mode"));
  }

  // Sanity check
  assert(fragments_.size() == 1);

  // Sync fragment
  RETURN_NOT_OK(fragments_[0]->sync());

  return Status::Ok();
}

Status Array::sync_attribute(const std::string& attribute) {
  // Sanity checks
  if (!write_mode()) {
    return LOG_STATUS(
        Status::ArrayError("Cannot sync attribute; Invalid mode"));
  }

  // Sanity check
  assert(fragments_.size() == 1);

  // Sync fragment
  RETURN_NOT_OK(fragments_[0]->sync_attribute(attribute));

  return Status::Ok();
}

Status Array::write(const void** buffers, const size_t* buffer_sizes) {
  // Sanity checks
  if (!write_mode()) {
    return LOG_STATUS(
        Status::ArrayError("Cannot write to array; Invalid mode"));
  }

  // Write based on mode
  if (mode_ == ArrayMode::WRITE_SORTED_COL ||
      mode_ == ArrayMode::WRITE_SORTED_ROW) {
    RETURN_NOT_OK(array_sorted_write_state_->write(buffers, buffer_sizes));
  } else if (mode_ == ArrayMode::WRITE || mode_ == ArrayMode::WRITE_UNSORTED) {
    RETURN_NOT_OK(write_default(buffers, buffer_sizes));
  } else {
    assert(0);
  }
  // In all modes except TILEDB_ARRAY_WRITE, the fragment must be finalized
  if (mode_ != ArrayMode::WRITE) {
    RETURN_NOT_OK(fragments_[0]->finalize());
    delete fragments_[0];
    fragments_.clear();
  }

  return Status::Ok();
}

Status Array::write_default(const void** buffers, const size_t* buffer_sizes) {
  // Sanity checks
  if (!write_mode()) {
    return LOG_STATUS(
        Status::ArrayError("Cannot write to array; Invalid mode"));
  }

  // Create and initialize a new fragment
  if (fragments_.size() == 0) {
    // Get new fragment name
    std::string new_fragment_name = this->new_fragment_name();
    if (new_fragment_name == "") {
      return LOG_STATUS(Status::ArrayError("Cannot produce new fragment name"));
    }

    // Create new fragment
    Fragment* fragment = new Fragment(this);
    fragments_.push_back(fragment);
    RETURN_NOT_OK(fragment->init(new_fragment_name, mode_, subarray_));
  }

  // Dispatch the write command to the new fragment
  RETURN_NOT_OK(fragments_[0]->write(buffers, buffer_sizes));

  // Success
  return Status::Ok();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

std::string Array::new_fragment_name() const {
  // For easy reference
  unsigned name_max_len = Configurator::name_max_len();

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
      array_schema_->array_name().c_str(),
      mac.c_str(),
      tid,
      ms);

  // Handle error
  if (n < 0)
    return "";

  // Return
  return fragment_name;
}

Status Array::open_fragments(
    const std::vector<std::string>& fragment_names,
    const std::vector<BookKeeping*>& book_keeping) {
  // Sanity check
  assert(fragment_names.size() == book_keeping.size());

  // Create a fragment object for each fragment directory
  int fragment_num = fragment_names.size();
  for (int i = 0; i < fragment_num; ++i) {
    Fragment* fragment = new Fragment(this);
    fragments_.push_back(fragment);
    RETURN_NOT_OK(fragment->init(fragment_names[i], book_keeping[i]));
  }
  return Status::Ok();
}

};  // namespace tiledb
