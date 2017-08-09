/**
 * @file   fragment.cc
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
 * This file implements the Fragment class.
 */

#include "fragment.h"
#include "filesystem.h"
#include "logger.h"
#include "utils.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Fragment::Fragment(const Array* array)
    : array_(array) {
  read_state_ = nullptr;
  write_state_ = nullptr;
  book_keeping_ = nullptr;
}

Fragment::~Fragment() {
  if (write_state_ != nullptr)
    delete write_state_;

  if (read_state_ != nullptr)
    delete read_state_;

  if (book_keeping_ != nullptr && !read_mode())
    delete book_keeping_;
}

/* ****************************** */
/*            ACCESSORS           */
/* ****************************** */

uri::URI Fragment::attr_uri(int attribute_id) const {
  const Attribute* attr = array_->array_schema()->Attributes()[attribute_id];
  return fragment_uri_.join_path(attr->name() + Configurator::file_suffix());
}

uri::URI Fragment::attr_var_uri(int attribute_id) const {
  const Attribute* attr = array_->array_schema()->Attributes()[attribute_id];
  return fragment_uri_.join_path(
      attr->name() + "_var" + Configurator::file_suffix());
}

uri::URI Fragment::coords_uri() const {
  return fragment_uri_.join_path(
      std::string(Configurator::coords()) + Configurator::file_suffix());
}

const Array* Fragment::array() const {
  return array_;
}

int64_t Fragment::cell_num_per_tile() const {
  return (dense_) ? array_->array_schema()->cell_num_per_tile() :
                    array_->array_schema()->capacity();
}

bool Fragment::dense() const {
  return dense_;
}

const uri::URI& Fragment::fragment_uri() const {
  return fragment_uri_;
}

ArrayMode Fragment::mode() const {
  return mode_;
}

inline bool Fragment::read_mode() const {
  return is_read_mode(mode_);
}

ReadState* Fragment::read_state() const {
  return read_state_;
}

size_t Fragment::tile_size(int attribute_id) const {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  bool var_size = array_schema->var_size(attribute_id);
  uint64_t cell_var_offset_size = Configurator::cell_var_offset_size();

  int64_t cell_num_per_tile =
      (dense_) ? array_schema->cell_num_per_tile() : array_schema->capacity();

  return (var_size) ? cell_num_per_tile * cell_var_offset_size :
                      cell_num_per_tile * array_schema->cell_size(attribute_id);
}

inline bool Fragment::write_mode() const {
  return is_write_mode(mode_);
}

/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

Status Fragment::finalize() {
  if (write_state_ != nullptr) {  // WRITE
    assert(book_keeping_ != NULL);
    Status st_ws = write_state_->finalize();
    Status st_bk = book_keeping_->finalize();
    Status st_rn;
    if (utils::fragment_exists(fragment_uri())) {
      st_rn = filesystem::rename_fragment(fragment_uri());
    }
    // Errors
    if (!st_ws.ok()) {
      return st_ws;
    }
    if (!st_bk.ok()) {
      return st_bk;
    }
    if (!st_rn.ok()) {
      return st_rn;
    }
  }
  // READ - nothing to be done
  return Status::Ok();
}

Status Fragment::init(
    const uri::URI& uri, ArrayMode mode, const void* subarray) {
  // Set fragment name and mode
  fragment_uri_ = uri;
  mode_ = mode;

  // Sanity check
  if (!write_mode()) {
    return LOG_STATUS(
        Status::FragmentError("Cannot initialize fragment;  Invalid mode"));
  }

  // Check if the fragment is dense or not
  dense_ = true;
  const std::vector<int>& attribute_ids = array_->attribute_ids();
  int id_num = attribute_ids.size();
  int attribute_num = array_->array_schema()->attribute_num();
  for (int i = 0; i < id_num; ++i) {
    if (attribute_ids[i] == attribute_num) {
      dense_ = false;
      break;
    }
  }

  // Initialize book-keeping and read/write state
  book_keeping_ = new BookKeeping(array_->array_schema(), dense_, uri, mode_);
  read_state_ = nullptr;
  Status st = book_keeping_->init(subarray);
  if (!st.ok()) {
    delete book_keeping_;
    book_keeping_ = nullptr;
    write_state_ = nullptr;
    return st;
  }
  write_state_ = new WriteState(this, book_keeping_);

  // Success
  return Status::Ok();
}

Status Fragment::init(const uri::URI& uri, BookKeeping* book_keeping) {
  // Set member attributes
  fragment_uri_ = uri;
  mode_ = array_->mode();
  book_keeping_ = book_keeping;
  dense_ = book_keeping_->dense();
  write_state_ = nullptr;
  read_state_ = new ReadState(this, book_keeping_);

  // Success
  return Status::Ok();
}

void Fragment::reset_read_state() {
  read_state_->reset();
}

Status Fragment::sync() {
  // Sanity check
  assert(write_state_ != NULL);

  // Sync
  RETURN_NOT_OK(write_state_->sync());
  return Status::Ok();
}

Status Fragment::sync_attribute(const std::string& attribute) {
  // Sanity check
  assert(write_state_ != NULL);

  // Sync attribute
  RETURN_NOT_OK(write_state_->sync_attribute(attribute));
  return Status::Ok();
}

Status Fragment::write(const void** buffers, const size_t* buffer_sizes) {
  // Forward the write command to the write state
  RETURN_NOT_OK(write_state_->write(buffers, buffer_sizes));
  return Status::Ok();
}

}  // namespace tiledb
