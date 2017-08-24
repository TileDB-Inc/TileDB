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
#include "../../include/vfs/filesystem.h"
#include "logger.h"
#include "utils.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Fragment::Fragment(Query* query)
    : query_(query) {
  read_state_ = nullptr;
  write_state_ = nullptr;
  metadata_ = nullptr;
}

Fragment::~Fragment() {
  if (write_state_ != nullptr) {
    delete write_state_;
    delete metadata_;
  }

  delete read_state_;
}

/* ****************************** */
/*            ACCESSORS           */
/* ****************************** */

uri::URI Fragment::attr_uri(int attribute_id) const {
  const Attribute* attr =
      query_->array()->array_schema()->Attributes()[attribute_id];
  return fragment_uri_.join_path(attr->name() + constants::file_suffix);
}

uri::URI Fragment::attr_var_uri(int attribute_id) const {
  const Attribute* attr =
      query_->array()->array_schema()->Attributes()[attribute_id];
  return fragment_uri_.join_path(
      attr->name() + "_var" + constants::file_suffix);
}

uri::URI Fragment::coords_uri() const {
  return fragment_uri_.join_path(
      std::string(constants::coords) + constants::file_suffix);
}

const Array* Fragment::array() const {
  return query_->array();
}

int64_t Fragment::cell_num_per_tile() const {
  return (dense_) ? query_->array()->array_schema()->cell_num_per_tile() :
                    query_->array()->array_schema()->capacity();
}

bool Fragment::dense() const {
  return dense_;
}

const uri::URI& Fragment::fragment_uri() const {
  return fragment_uri_;
}

ReadState* Fragment::read_state() const {
  return read_state_;
}

size_t Fragment::tile_size(int attribute_id) const {
  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  bool var_size = array_schema->var_size(attribute_id);
  uint64_t cell_var_offset_size = constants::cell_var_offset_size;

  int64_t cell_num_per_tile =
      (dense_) ? array_schema->cell_num_per_tile() : array_schema->capacity();

  return (var_size) ? cell_num_per_tile * cell_var_offset_size :
                      cell_num_per_tile * array_schema->cell_size(attribute_id);
}

/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

Status Fragment::finalize() {
  if (write_state_ != nullptr) {  // WRITE
    assert(metadata_ != NULL);
    Status st_ws = write_state_->finalize();
    Status st_bk = metadata_->flush();
    Status st_rn;
    if (utils::fragment_exists(fragment_uri())) {
      st_rn = vfs::rename_fragment(fragment_uri());
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

Status Fragment::init(const uri::URI& uri, const void* subarray) {
  // Set fragment name and mode
  fragment_uri_ = uri;

  // Check if the fragment is dense or not
  dense_ = true;
  const std::vector<int>& attribute_ids = query_->attribute_ids();
  int id_num = attribute_ids.size();
  int attribute_num = query_->array()->array_schema()->attribute_num();
  for (int i = 0; i < id_num; ++i) {
    if (attribute_ids[i] == attribute_num) {
      dense_ = false;
      break;
    }
  }

  // Initialize metadata and read/write state
  metadata_ =
      new FragmentMetadata(query_->array()->array_schema(), dense_, uri);
  read_state_ = nullptr;
  Status st = metadata_->init(subarray);
  if (!st.ok()) {
    delete metadata_;
    metadata_ = nullptr;
    write_state_ = nullptr;
    return st;
  }
  write_state_ = new WriteState(this, metadata_);

  // Success
  return Status::Ok();
}

Status Fragment::init(const uri::URI& uri, FragmentMetadata* metadata) {
  // Set member attributes
  fragment_uri_ = uri;
  metadata_ = metadata;
  dense_ = metadata_->dense();
  read_state_ = new ReadState(this, query_, metadata_);

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
