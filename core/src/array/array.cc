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
#include "logger.h"
#include "utils.h"

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Array::Array() {
  array_schema_ = nullptr;
  storage_manager_ = nullptr;
  query_ = nullptr;
}

Array::~Array() {
  delete query_;
  delete array_schema_;
}

/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

StorageManager* Array::storage_manager() const {
  return storage_manager_;
}

Status Array::aio_handle_request(AIORequest* aio_request) {
  Status st;
  Query* query = aio_request->query();

  if (is_read_mode(aio_request->mode())) {  // READ MODE
    // Invoke the read
    if (aio_request->mode() == QueryMode::READ) {
      st = read_default(
          query, aio_request->buffers(), aio_request->buffer_sizes());
    } else {
      st = read(query, aio_request->buffers(), aio_request->buffer_sizes());
    }
  } else {  // WRITE MODE
    // Invoke the write
    if (aio_request->mode() == QueryMode::WRITE ||
        aio_request->mode() == QueryMode::WRITE_UNSORTED) {
      st = write_default(
          query,
          (const void**)aio_request->buffers(),
          (const size_t*)aio_request->buffer_sizes());
    } else {
      st = write(
          query,
          (const void**)aio_request->buffers(),
          (const size_t*)aio_request->buffer_sizes());
    }
  }

  const std::vector<int>& attribute_ids = query->attribute_ids();

  if (st.ok()) {  // Success
    // Check for overflow (applicable only to reads)
    if (aio_request->mode() == QueryMode::READ &&
        query->array_read_state()->overflow()) {
      aio_request->set_status(AIOStatus::OFLOW);
      if (aio_request->overflow() != nullptr) {
        for (int i = 0; i < int(attribute_ids.size()); ++i)
          aio_request->set_overflow(
              i, query->array_read_state()->overflow(attribute_ids[i]));
      }
    } else if (
        (aio_request->mode() == QueryMode::READ_SORTED_COL ||
         aio_request->mode() == QueryMode::READ_SORTED_ROW) &&
        query->array_sorted_read_state()->overflow()) {
      aio_request->set_status(AIOStatus::OFLOW);
      if (aio_request->overflow() != nullptr) {
        for (int i = 0; i < int(attribute_ids.size()); ++i)
          aio_request->set_overflow(
              i, query->array_sorted_read_state()->overflow(attribute_ids[i]));
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

  return st;
}

const ArraySchema* Array::array_schema() const {
  return array_schema_;
}

const Config* Array::config() const {
  return config_;
}

Status Array::read(Query* query, void** buffers, size_t* buffer_sizes) {
  // Sanity checks
  if (!is_read_mode(query->mode())) {
    return LOG_STATUS(
        Status::ArrayError("Cannot read from array; Invalid mode"));
  }

  // Check if there are no fragments
  int buffer_i = 0;
  int attribute_id_num = query->attribute_ids().size();
  if (query->fragment_num() == 0) {
    for (int i = 0; i < attribute_id_num; ++i) {
      // Update all sizes to 0
      buffer_sizes[buffer_i] = 0;
      if (!array_schema_->var_size(query->attribute_ids()[i]))
        ++buffer_i;
      else
        buffer_i += 2;
    }
    return Status::Ok();
  }

  // Handle sorted modes
  if (query->mode() == QueryMode::READ_SORTED_COL ||
      query->mode() == QueryMode::READ_SORTED_ROW) {
    return query->array_sorted_read_state()->read(buffers, buffer_sizes);
  } else {  // mode_ == TILEDB_ARRAY_READ
    return read_default(query, buffers, buffer_sizes);
  }
}

Status Array::read_default(Query* query, void** buffers, size_t* buffer_sizes) {
  return query->array_read_state()->read(buffers, buffer_sizes);
}

/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

Status Array::consolidate(
    Fragment*& new_fragment, std::vector<uri::URI>* old_fragments) {
  /* TODO

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
    RETURN_NOT_OK(new_fragment->init(new_fragment_name));

    // Consolidate on a per-attribute basis
    Status st;
    for (int i = 0; i < array_schema_->attribute_num() + 1; ++i) {
      st = consolidate(new_fragment, i);
      if (!st.ok()) {
        utils::delete_fragment(new_fragment->fragment_uri());
        delete new_fragment;
        return st;
      }
    }

    // Get old fragment names
    int fragment_num = fragments_.size();
    for (int i = 0; i < fragment_num; ++i)
      old_fragments->push_back(fragments_[i]->fragment_uri());
      */

  return Status::Ok();
}

Status Array::consolidate(Fragment* new_fragment, int attribute_id) {
  /* TODO
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  uint64_t consolidation_buffer_size =
      constants::consolidation_buffer_size;

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

*/
  return Status::Ok();
}

Status Array::finalize() {
  return Status::Ok();
}

Status Array::init(
    StorageManager* storage_manager,
    const ArraySchema* array_schema,
    const std::vector<std::string>& fragment_names,
    const std::vector<FragmentMetadata*>& book_keeping,
    QueryMode mode,
    const char** attributes,
    int attribute_num,
    const void* subarray,
    const Config* config) {
  config_ = config;
  array_schema_ = array_schema;
  storage_manager_ = storage_manager;

  query_ = new Query();
  return query_->init(
      this,
      mode,
      subarray,
      attributes,
      attribute_num,
      fragment_names,
      book_keeping);
}

Status Array::write(
    Query* query, const void** buffers, const size_t* buffer_sizes) {
  QueryMode mode = query->mode();

  // Sanity checks
  if (!is_write_mode(mode)) {
    return LOG_STATUS(
        Status::ArrayError("Cannot write to array; Invalid mode"));
  }

  // Write based on mode
  if (mode == QueryMode::WRITE_SORTED_COL ||
      mode == QueryMode::WRITE_SORTED_ROW) {
    RETURN_NOT_OK(
        query->array_sorted_write_state()->write(buffers, buffer_sizes));
  } else if (mode == QueryMode::WRITE || mode == QueryMode::WRITE_UNSORTED) {
    RETURN_NOT_OK(write_default(query, buffers, buffer_sizes));
  } else {
    assert(0);
  }

  // In all modes except WRITE, the fragment must be finalized
  if (mode != QueryMode::WRITE)
    query->clear_fragments();

  return Status::Ok();
}

Status Array::write_default(
    Query* query, const void** buffers, const size_t* buffer_sizes) {
  return query->write_default(buffers, buffer_sizes);
}

};  // namespace tiledb
