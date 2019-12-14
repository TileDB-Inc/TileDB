/**
 * @file   writer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file implements class Writer.
 */

#include "tiledb/sm/query/writer.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/tile_io.h"

#include <iostream>
#include <sstream>

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Writer::Writer() {
  array_ = nullptr;
  array_schema_ = nullptr;
  coords_buffer_ = nullptr;
  coords_buffer_size_ = nullptr;
  coords_num_ = 0;
  has_coords_ = false;
  coord_buffer_is_set_ = false;
  global_write_state_.reset(nullptr);
  initialized_ = false;
  layout_ = Layout::ROW_MAJOR;
  storage_manager_ = nullptr;
}

Writer::~Writer() {
  clear_coord_buffers();
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status Writer::add_range(unsigned dim_idx, const Range& range) {
  if (!array_schema_->dense())
    return LOG_STATUS(
        Status::WriterError("Adding a subarray range to a write query is not "
                            "supported in sparse arrays"));

  if (subarray_.is_set(dim_idx))
    return LOG_STATUS(
        Status::WriterError("Cannot add range; Multi-range dense writes "
                            "are not supported"));

  return subarray_.add_range(dim_idx, range);
}

Status Writer::get_range_num(unsigned dim_idx, uint64_t* range_num) const {
  if (!array_schema_->dense())
    return LOG_STATUS(
        Status::WriterError("Getting the number of ranges from a write query "
                            "is not applicable to sparse arrays"));

  return subarray_.get_range_num(dim_idx, range_num);
}

Status Writer::get_range(
    unsigned dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) const {
  if (!array_schema_->dense())
    return LOG_STATUS(
        Status::WriterError("Getting a range from a write query is not "
                            "applicable to sparse arrays"));

  *stride = nullptr;
  return subarray_.get_range(dim_idx, range_idx, start, end);
}

const ArraySchema* Writer::array_schema() const {
  return array_schema_;
}

std::vector<std::string> Writer::buffer_names() const {
  std::vector<std::string> ret;

  // Add to the buffer names the attributes, as well as the dimensions only if
  // coords_buffer_ has not been set
  for (const auto& it : buffers_) {
    if (!array_schema_->is_dim(it.first) || (!coords_buffer_))
      ret.push_back(it.first);
  }

  // Special zipped coordinates name
  if (coords_buffer_)
    ret.push_back(constants::coords);

  return ret;
}

QueryBuffer Writer::buffer(const std::string& name) const {
  // Special zipped coordinates
  if (name == constants::coords)
    return QueryBuffer(coords_buffer_, nullptr, coords_buffer_size_, nullptr);

  // Attribute or dimension
  auto buf = buffers_.find(name);
  if (buf != buffers_.end())
    return buf->second;

  // Named buffer does not exist
  return QueryBuffer{};
}

Status Writer::finalize() {
  if (global_write_state_ != nullptr)
    return finalize_global_write_state();
  return Status::Ok();
}

Status Writer::get_buffer(
    const std::string& name, void** buffer, uint64_t** buffer_size) const {
  // Special zipped coordinates
  if (name == constants::coords) {
    *buffer = coords_buffer_;
    *buffer_size = coords_buffer_size_;
    return Status::Ok();
  }

  // Attribute or dimension
  auto it = buffers_.find(name);
  if (it != buffers_.end()) {
    *buffer = it->second.buffer_;
    *buffer_size = it->second.buffer_size_;
    return Status::Ok();
  }

  // Named buffer does not exist
  *buffer = nullptr;
  *buffer_size = nullptr;

  return Status::Ok();
}

Status Writer::get_buffer(
    const std::string& name,
    uint64_t** buffer_off,
    uint64_t** buffer_off_size,
    void** buffer_val,
    uint64_t** buffer_val_size) const {
  // Attribute or dimension
  auto it = buffers_.find(name);
  if (it != buffers_.end()) {
    *buffer_off = (uint64_t*)it->second.buffer_;
    *buffer_off_size = it->second.buffer_size_;
    *buffer_val = it->second.buffer_var_;
    *buffer_val_size = it->second.buffer_var_size_;
    return Status::Ok();
  }

  // Named buffer does not exist
  *buffer_off = nullptr;
  *buffer_off_size = nullptr;
  *buffer_val = nullptr;
  *buffer_val_size = nullptr;

  return Status::Ok();
}

bool Writer::get_check_coord_dups() const {
  return check_coord_dups_;
}

bool Writer::get_check_coord_oob() const {
  return check_coord_oob_;
}

bool Writer::get_dedup_coords() const {
  return dedup_coords_;
}

void Writer::set_check_coord_dups(bool b) {
  check_coord_dups_ = b;
}

void Writer::set_check_coord_oob(bool b) {
  check_coord_oob_ = b;
}

void Writer::set_dedup_coords(bool b) {
  dedup_coords_ = b;
}

Status Writer::init(const Layout& layout) {
  // Sanity checks
  if (storage_manager_ == nullptr)
    return LOG_STATUS(Status::WriterError(
        "Cannot initialize query; Storage manager not set"));
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot initialize query; Array schema not set"));
  if (buffers_.empty())
    return LOG_STATUS(
        Status::WriterError("Cannot initialize query; Buffers not set"));

  // Set a default subarray
  if (!subarray_.is_set())
    subarray_ = Subarray(array_, layout);

  RETURN_NOT_OK(set_layout(layout));
  RETURN_NOT_OK(check_subarray());
  RETURN_NOT_OK(check_buffer_sizes());
  RETURN_NOT_OK(check_buffer_names());
  if (array_schema_->dense())
    RETURN_NOT_OK(subarray_.to_byte_vec(&subarray_flat_));

  optimize_layout_for_1D();

  // Get configuration parameters
  const char *check_coord_dups, *check_coord_oob, *check_global_order;
  const char* dedup_coords;
  auto config = storage_manager_->config();
  RETURN_NOT_OK(config.get("sm.check_coord_dups", &check_coord_dups));
  RETURN_NOT_OK(config.get("sm.check_coord_oob", &check_coord_oob));
  RETURN_NOT_OK(config.get("sm.check_global_order", &check_global_order));
  RETURN_NOT_OK(config.get("sm.dedup_coords", &dedup_coords));
  assert(check_coord_dups != nullptr && dedup_coords != nullptr);
  check_coord_dups_ = !strcmp(check_coord_dups, "true");
  check_coord_oob_ = !strcmp(check_coord_oob, "true");
  check_global_order_ = !strcmp(check_global_order, "true");
  dedup_coords_ = !strcmp(dedup_coords, "true");
  initialized_ = true;

  return Status::Ok();
}

Layout Writer::layout() const {
  return layout_;
}

void Writer::set_array(const Array* array) {
  array_ = array;
  subarray_ = Subarray(array);
}

void Writer::set_array_schema(const ArraySchema* array_schema) {
  array_schema_ = array_schema;
}

Status Writer::set_buffer(
    const std::string& name, void* buffer, uint64_t* buffer_size) {
  // Check buffer
  if (buffer == nullptr || buffer_size == nullptr)
    return LOG_STATUS(Status::WriterError(
        "Cannot set buffer; Buffer or buffer size is null"));

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot set buffer; Array schema not set"));

  // Set special function for zipped coordinates buffer
  if (name == constants::coords)
    return set_coords_buffer(buffer, buffer_size);

  // For easy reference
  bool is_dim = array_schema_->is_dim(name);
  bool is_attr = array_schema_->is_attr(name);

  // Neither a dimension nor an attribute
  if (!is_dim && !is_attr)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer; Invalid buffer name '") + name +
        "' (it should be an attribute or dimension)"));

  // Error if it is var-sized
  bool var_size = (array_schema_->var_size(name));
  if (var_size)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is var-sized"));

  // Error if setting a new attribute/dimension after initialization
  bool exists = buffers_.find(name) != buffers_.end();
  if (initialized_ && !exists)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer for new attribute/dimension '") + name +
        "' after initialization"));

  // Check if zipped coordinates coexist with separate coordinate buffers
  if (is_dim && coords_buffer_ != nullptr)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set separate coordinate buffers after "
                    "having set the zipped coordinates buffer")));

  if (is_dim) {
    // Check number of coordinates
    uint64_t coords_num = *buffer_size / array_schema_->cell_size(name);
    if (coord_buffer_is_set_ && coords_num != coords_num_)
      return LOG_STATUS(Status::WriterError(
          std::string("Cannot set buffer; Input buffer for dimension '") +
          name +
          "' has a different number of coordinates than previously "
          "set coordinate buffers"));

    coords_num_ = coords_num;
    coord_buffer_is_set_ = true;
    has_coords_ = true;
  }

  // Set attribute/dimension buffer
  buffers_[name] = QueryBuffer(buffer, nullptr, buffer_size, nullptr);

  return Status::Ok();
}

Status Writer::set_buffer(
    const std::string& name,
    uint64_t* buffer_off,
    uint64_t* buffer_off_size,
    void* buffer_val,
    uint64_t* buffer_val_size) {
  // Check buffer
  if (buffer_off == nullptr || buffer_off_size == nullptr ||
      buffer_val == nullptr || buffer_val_size == nullptr)
    return LOG_STATUS(Status::WriterError(
        "Cannot set buffer; Buffer or buffer size is null"));

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot set buffer; Array schema not set"));

  // For easy reference
  bool is_dim = array_schema_->is_dim(name);
  bool is_attr = array_schema_->is_attr(name);

  // Neither a dimension nor an attribute
  if (!is_dim && !is_attr)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer; Invalid buffer name '") + name +
        "' (it should be an attribute or dimension)"));

  // Error if it is fixed-sized
  bool var_size = (array_schema_->var_size(name));
  if (!var_size)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is fixed-sized"));

  // Error if setting a new attribute/dimension after initialization
  bool exists = buffers_.find(name) != buffers_.end();
  if (initialized_ && !exists)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer for new attribute/dimension '") + name +
        "' after initialization"));

  if (is_dim) {
    // Check number of coordinates
    uint64_t coords_num = *buffer_off_size / constants::cell_var_offset_size;
    if (coord_buffer_is_set_ && coords_num != coords_num_)
      return LOG_STATUS(Status::WriterError(
          std::string("Cannot set buffer; Input buffer for dimension '") +
          name +
          "' has a different number of coordinates than previously "
          "set coordinate buffers"));

    coords_num_ = coords_num;
    coord_buffer_is_set_ = true;
    has_coords_ = true;
  }

  // Set attribute/dimension buffer
  buffers_[name] =
      QueryBuffer(buffer_off, buffer_val, buffer_off_size, buffer_val_size);

  return Status::Ok();
}

void Writer::set_fragment_uri(const URI& fragment_uri) {
  fragment_uri_ = fragment_uri;
}

Status Writer::set_layout(Layout layout) {
  layout_ = layout;
  subarray_.set_layout(layout);

  return Status::Ok();
}

void Writer::set_storage_manager(StorageManager* storage_manager) {
  storage_manager_ = storage_manager;
}

Status Writer::set_subarray(const Subarray& subarray) {
  // Not applicable to sparse arrays
  if (!array_schema_->dense())
    return LOG_STATUS(Status::WriterError(
        "Setting a subarray is not supported in sparse writes"));

  // Subarray must be unary for dense writes
  if (subarray.range_num() != 1)
    return LOG_STATUS(
        Status::WriterError("Cannot set subarray; Multi-range dense writes "
                            "are not supported"));

  // Reset the writer (this will nuke the global write state)
  reset();

  subarray_ = subarray;

  // Set subarray_flat so calls to `subarray()` will reflect newly set value
  RETURN_NOT_OK(subarray_.to_byte_vec(&subarray_flat_));

  return Status::Ok();
}

const void* Writer::subarray() const {
  // Only access subarray_flat_ if it is not empty
  if (!subarray_flat_.empty())
    return &subarray_flat_[0];

  return nullptr;
}

Status Writer::write() {
  STATS_FUNC_IN(writer_write);

  // In case the user has provided a coordinates buffer
  RETURN_NOT_OK(split_coords_buffer());

  if (check_coord_oob_)
    RETURN_NOT_OK(check_coord_oob());

  if (layout_ == Layout::COL_MAJOR || layout_ == Layout::ROW_MAJOR) {
    RETURN_NOT_OK(ordered_write());
  } else if (layout_ == Layout::UNORDERED) {
    RETURN_NOT_OK(unordered_write());
  } else if (layout_ == Layout::GLOBAL_ORDER) {
    RETURN_NOT_OK(global_write());
  } else {
    assert(false);
  }

  return Status::Ok();
  STATS_FUNC_OUT(writer_write);
}

const std::vector<WrittenFragmentInfo>& Writer::written_fragment_info() const {
  return written_fragment_info_;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status Writer::add_written_fragment_info(const URI& uri) {
  std::pair<uint64_t, uint64_t> timestamp_range;
  RETURN_NOT_OK(utils::parse::get_timestamp_range(uri, &timestamp_range));
  written_fragment_info_.emplace_back(uri, timestamp_range);
  return Status::Ok();
}

Status Writer::check_buffer_names() {
  // If the array is sparse, the coordinates must be provided
  if (!array_schema_->dense() && !has_coords_)
    return LOG_STATUS(
        Status::WriterError("Sparse array writes expect the coordinates of the "
                            "cells to be written"));

  // If the layout is unordered, the coordinates must be provided
  if (layout_ == Layout::UNORDERED && !has_coords_)
    return LOG_STATUS(Status::WriterError(
        "Unordered writes expect the coordinates of the cells to be written"));

  // All attributes/dimensions must be provided
  auto expected_num = array_schema_->attribute_num();
  expected_num += (coord_buffer_is_set_) ? array_schema_->dim_num() : 0;
  if (buffers_.size() != expected_num)
    return LOG_STATUS(
        Status::WriterError("Writes expect all attributes (and coordinates in "
                            "the sparse/unordered case) to be set"));

  return Status::Ok();
}

Status Writer::check_buffer_sizes() const {
  // This is applicable only to dense arrays and ordered layout
  if (!array_schema_->dense() ||
      (layout_ != Layout::ROW_MAJOR && layout_ != Layout::COL_MAJOR))
    return Status::Ok();

  auto cell_num = array_schema_->domain()->cell_num(subarray_.ndrange(0));
  uint64_t expected_cell_num = 0;
  for (const auto& it : buffers_) {
    const auto& attr = it.first;
    bool is_var = array_schema_->var_size(attr);
    auto buffer_size = *it.second.buffer_size_;
    if (is_var) {
      expected_cell_num = buffer_size / constants::cell_var_offset_size;
    } else {
      expected_cell_num = buffer_size / array_schema_->cell_size(attr);
    }

    if (expected_cell_num != cell_num) {
      std::stringstream ss;
      ss << "Buffer sizes check failed; Invalid number of cells given for ";
      ss << "attribute '" << attr << "'";
      ss << " (" << expected_cell_num << " != " << cell_num << ")";
      return LOG_STATUS(Status::WriterError(ss.str()));
    }
  }
  return Status::Ok();
}

Status Writer::check_coord_dups(const std::vector<uint64_t>& cell_pos) const {
  STATS_FUNC_IN(writer_check_coord_dups);

  // Check if applicable
  if (array_schema_->allows_dups() || !check_coord_dups_ || dedup_coords_)
    return Status::Ok();

  if (!has_coords_) {
    return LOG_STATUS(
        Status::WriterError("Cannot check for coordinate duplicates; "
                            "Coordinates buffer not found"));
  }

  if (coords_num_ < 2)
    return Status::Ok();

  // Prepare auxiliary vectors for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<const unsigned char*> buffs(dim_num);
  std::vector<uint64_t> coord_sizes(dim_num);
  std::vector<const unsigned char*> buffs_var(dim_num);
  std::vector<uint64_t*> buffs_var_sizes(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = (const unsigned char*)buffers_.find(dim_name)->second.buffer_;
    coord_sizes[d] = array_schema_->cell_size(dim_name);
    buffs_var[d] =
        (const unsigned char*)buffers_.find(dim_name)->second.buffer_var_;
    buffs_var_sizes[d] = buffers_.find(dim_name)->second.buffer_var_size_;
  }

  auto statuses = parallel_for(1, coords_num_, [&](uint64_t i) {
    // Check for duplicate in adjacent cells
    bool found_dup = true;
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim = array_schema_->dimension(d);
      if (!dim->var_size()) {  // Fixed-sized dimensions
        if (memcmp(
                buffs[d] + cell_pos[i] * coord_sizes[d],
                buffs[d] + cell_pos[i - 1] * coord_sizes[d],
                coord_sizes[d]) != 0) {  // Not the same
          found_dup = false;
          break;
        }
      } else {
        auto offs = (uint64_t*)buffs[d];
        auto a = cell_pos[i];
        auto b = cell_pos[i - 1];
        auto off_a = offs[a];
        auto off_b = offs[b];
        auto off_a_plus_1 =
            (a == coords_num_ - 1) ? *(buffs_var_sizes[d]) : offs[a + 1];
        auto off_b_plus_1 =
            (b == coords_num_ - 1) ? *(buffs_var_sizes[d]) : offs[b + 1];
        auto size_a = off_a_plus_1 - off_a;
        auto size_b = off_b_plus_1 - off_b;

        // Compare sizes
        if (size_a != size_b) {  // Not same
          found_dup = false;
          break;
        }

        // Compare var values
        if (memcmp(
                buffs_var[d] + off_a,
                buffs_var[d] + off_b,
                size_a) != 0) {  // Not the same
          found_dup = false;
          break;
        }
      }
    }

    // Found duplicate
    if (found_dup) {
      std::stringstream ss;
      ss << "Duplicate coordinates " << coords_to_str(cell_pos[i]);
      ss << " are not allowed";
      return Status::WriterError(ss.str());
    }

    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK_ELSE(st, LOG_STATUS(st));

  return Status::Ok();
  STATS_FUNC_OUT(writer_check_coord_dups);
}

Status Writer::check_coord_dups() const {
  STATS_FUNC_IN(writer_check_coord_dups_global);

  // Check if applicable
  if (array_schema_->allows_dups() || !check_coord_dups_ || dedup_coords_)
    return Status::Ok();

  if (!has_coords_) {
    return LOG_STATUS(
        Status::WriterError("Cannot check for coordinate duplicates; "
                            "Coordinates buffer not found"));
  }

  if (coords_num_ < 2)
    return Status::Ok();

  // Prepare auxiliary vectors for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<const unsigned char*> buffs(dim_num);
  std::vector<uint64_t> coord_sizes(dim_num);
  std::vector<const unsigned char*> buffs_var(dim_num);
  std::vector<uint64_t*> buffs_var_sizes(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = (const unsigned char*)buffers_.find(dim_name)->second.buffer_;
    coord_sizes[d] = array_schema_->cell_size(dim_name);
    buffs_var[d] =
        (const unsigned char*)buffers_.find(dim_name)->second.buffer_var_;
    buffs_var_sizes[d] = buffers_.find(dim_name)->second.buffer_var_size_;
  }

  auto statuses = parallel_for(1, coords_num_, [&](uint64_t i) {
    // Check for duplicate in adjacent cells
    bool found_dup = true;
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim = array_schema_->dimension(d);
      if (!dim->var_size()) {  // Fixed-sized dimensions
        if (memcmp(
                buffs[d] + i * coord_sizes[d],
                buffs[d] + (i - 1) * coord_sizes[d],
                coord_sizes[d]) != 0) {  // Not the same
          found_dup = false;
          break;
        }
      } else {
        auto offs = (uint64_t*)buffs[d];
        auto off_i_plus_1 =
            (i == coords_num_ - 1) ? *(buffs_var_sizes[d]) : offs[i + 1];
        auto size_i_minus_1 = offs[i] - offs[i - 1];
        auto size_i = off_i_plus_1 - offs[i];

        // Compare sizes
        if (size_i != size_i_minus_1) {  // Not same
          found_dup = false;
          break;
        }

        // Compare var values
        if (memcmp(
                buffs_var[d] + offs[i - 1],
                buffs_var[d] + offs[i],
                size_i) != 0) {  // Not the same
          found_dup = false;
          break;
        }
      }
    }

    // Found duplicate
    if (found_dup) {
      std::stringstream ss;
      ss << "Duplicate coordinates " << coords_to_str(i);
      ss << " are not allowed";
      return Status::WriterError(ss.str());
    }

    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK_ELSE(st, LOG_STATUS(st));

  return Status::Ok();

  STATS_FUNC_OUT(writer_check_coord_dups_global);
}

Status Writer::check_coord_oob() const {
  // Applicable only to sparse writes - exit if coordinates do not exist
  if (!has_coords_)
    return Status::Ok();

  // Exit if there are no coordinates to write
  if (coords_num_ == 0)
    return Status::Ok();

  // Exit if all dimensions are strings
  if (array_schema_->domain()->all_dims_string())
    return Status::Ok();

  // Prepare auxiliary vectors for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<unsigned char*> buffs(dim_num);
  std::vector<uint64_t> coord_sizes(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = (unsigned char*)buffers_.find(dim_name)->second.buffer_;
    coord_sizes[d] = array_schema_->cell_size(dim_name);
  }

  // Check if all coordinates fall in the domain in parallel
  auto statuses =
      parallel_for_2d(0, coords_num_, 0, dim_num, [&](uint64_t c, unsigned d) {
        auto dim = array_schema_->dimension(d);
        if (datatype_is_string(dim->type()))
          return Status::Ok();
        return dim->oob(buffs[d] + c * coord_sizes[d]);
      });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK_ELSE(st, LOG_STATUS(st));

  // Success
  return Status::Ok();
}

Status Writer::check_global_order() const {
  // Check if applicable
  if (!check_global_order_)
    return Status::Ok();

  // Applicable only to sparse writes - exit if coordinates do not exist
  if (!has_coords_ || coords_num_ < 2)
    return Status::Ok();

  // Prepare auxiliary vector for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<const QueryBuffer*> buffs(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = &(buffers_.find(dim_name)->second);
  }

  // Check if all coordinates fall in the domain in parallel
  auto domain = array_schema_->domain();
  auto statuses = parallel_for(0, coords_num_ - 1, [&](uint64_t i) {
    auto tile_cmp = domain->tile_order_cmp(buffs, i, i + 1);
    auto fail = (tile_cmp > 0) || ((tile_cmp == 0) &&
                                   domain->cell_order_cmp(buffs, i, i + 1) > 0);

    if (fail) {
      std::stringstream ss;
      ss << "Write failed; Coordinates " << coords_to_str(i);
      ss << " succeed " << coords_to_str(i + 1);
      ss << " in the global order";
      if (tile_cmp > 0)
        ss << " due to writes across tiles";
      return Status::WriterError(ss.str());
    }
    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK_ELSE(st, LOG_STATUS(st));

  return Status::Ok();
}

Status Writer::check_subarray() const {
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot check subarray; Array schema not set"));

  if (array_schema_->dense()) {
    if (layout_ == Layout::GLOBAL_ORDER && !subarray_.coincides_with_tiles())
      return LOG_STATUS(
          Status::WriterError("Cannot initialize query; In global writes for "
                              "dense arrays, the subarray "
                              "must coincide with the tile bounds"));
    if (layout_ == Layout::UNORDERED && subarray_.is_set())
      return LOG_STATUS(Status::WriterError(
          "Cannot initialize query; Setting a subarray in unordered writes for "
          "dense arrays in inapplicable"));
  }

  return Status::Ok();
}

void Writer::clear_coord_buffers() {
  // Applicable only if the coordinate buffers have been allocated by
  // TileDB, which happens only when the zipped coordinates buffer is set
  for (auto b : to_clean_)
    std::free(b);
  to_clean_.clear();
  coord_buffer_sizes_.clear();
}

Status Writer::close_files(FragmentMetadata* meta) const {
  // Close attribute and dimension files
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    RETURN_NOT_OK(storage_manager_->close_file(meta->uri(name)));
    if (array_schema_->var_size(name))
      RETURN_NOT_OK(storage_manager_->close_file(meta->var_uri(name)));
  }

  return Status::Ok();
}

Status Writer::compute_coord_dups(
    const std::vector<uint64_t>& cell_pos,
    std::set<uint64_t>* coord_dups) const {
  STATS_FUNC_IN(writer_compute_coord_dups);
  if (!has_coords_) {
    return LOG_STATUS(
        Status::WriterError("Cannot check for coordinate duplicates; "
                            "Coordinates buffer not found"));
  }

  if (coords_num_ < 2)
    return Status::Ok();

  // Prepare auxiliary vectors for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<const unsigned char*> buffs(dim_num);
  std::vector<uint64_t> coord_sizes(dim_num);
  std::vector<const unsigned char*> buffs_var(dim_num);
  std::vector<uint64_t*> buffs_var_sizes(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = (const unsigned char*)buffers_.find(dim_name)->second.buffer_;
    coord_sizes[d] = array_schema_->cell_size(dim_name);
    buffs_var[d] =
        (const unsigned char*)buffers_.find(dim_name)->second.buffer_var_;
    buffs_var_sizes[d] = buffers_.find(dim_name)->second.buffer_var_size_;
  }

  std::mutex mtx;
  auto statuses = parallel_for(1, coords_num_, [&](uint64_t i) {
    // Check for duplicate in adjacent cells
    bool found_dup = true;
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim = array_schema_->dimension(d);
      if (!dim->var_size()) {  // Fixed-sized dimensions
        if (memcmp(
                buffs[d] + cell_pos[i] * coord_sizes[d],
                buffs[d] + cell_pos[i - 1] * coord_sizes[d],
                coord_sizes[d]) != 0) {  // Not the same
          found_dup = false;
          break;
        }
      } else {
        auto offs = (uint64_t*)buffs[d];
        auto a = cell_pos[i];
        auto b = cell_pos[i - 1];
        auto off_a = offs[a];
        auto off_b = offs[b];
        auto off_a_plus_1 =
            (a == coords_num_ - 1) ? *(buffs_var_sizes[d]) : offs[a + 1];
        auto off_b_plus_1 =
            (b == coords_num_ - 1) ? *(buffs_var_sizes[d]) : offs[b + 1];
        auto size_a = off_a_plus_1 - off_a;
        auto size_b = off_b_plus_1 - off_b;

        // Compare sizes
        if (size_a != size_b) {  // Not same
          found_dup = false;
          break;
        }

        // Compare var values
        if (memcmp(
                buffs_var[d] + off_a,
                buffs_var[d] + off_b,
                size_a) != 0) {  // Not the same
          found_dup = false;
          break;
        }
      }
    }

    // Found duplicate
    if (found_dup) {
      std::lock_guard<std::mutex> lock(mtx);
      coord_dups->insert(cell_pos[i]);
    }

    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();

  STATS_FUNC_OUT(writer_compute_coord_dups);
}

Status Writer::compute_coord_dups(std::set<uint64_t>* coord_dups) const {
  STATS_FUNC_IN(writer_compute_coord_dups_global);

  if (!has_coords_) {
    return LOG_STATUS(
        Status::WriterError("Cannot check for coordinate duplicates; "
                            "Coordinates buffer not found"));
  }

  if (coords_num_ < 2)
    return Status::Ok();

  // Prepare auxiliary vectors for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<const unsigned char*> buffs(dim_num);
  std::vector<uint64_t> coord_sizes(dim_num);
  std::vector<const unsigned char*> buffs_var(dim_num);
  std::vector<uint64_t*> buffs_var_sizes(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = (const unsigned char*)buffers_.find(dim_name)->second.buffer_;
    coord_sizes[d] = array_schema_->cell_size(dim_name);
    buffs_var[d] =
        (const unsigned char*)buffers_.find(dim_name)->second.buffer_var_;
    buffs_var_sizes[d] = buffers_.find(dim_name)->second.buffer_var_size_;
  }

  std::mutex mtx;
  auto statuses = parallel_for(1, coords_num_, [&](uint64_t i) {
    // Check for duplicate in adjacent cells
    bool found_dup = true;
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim = array_schema_->dimension(d);
      if (!dim->var_size()) {  // Fixed-sized dimensions
        if (memcmp(
                buffs[d] + i * coord_sizes[d],
                buffs[d] + (i - 1) * coord_sizes[d],
                coord_sizes[d]) != 0) {  // Not the same
          found_dup = false;
          break;
        }
      } else {
        auto offs = (uint64_t*)buffs[d];
        auto off_i_plus_1 =
            (i == coords_num_ - 1) ? *(buffs_var_sizes[d]) : offs[i + 1];
        auto size_i_minus_1 = offs[i] - offs[i - 1];
        auto size_i = off_i_plus_1 - offs[i];

        // Compare sizes
        if (size_i != size_i_minus_1) {  // Not same
          found_dup = false;
          break;
        }

        // Compare var values
        if (memcmp(
                buffs_var[d] + offs[i - 1],
                buffs_var[d] + offs[i],
                size_i) != 0) {  // Not the same
          found_dup = false;
          break;
        }
      }
    }

    // Found duplicate
    if (found_dup) {
      std::lock_guard<std::mutex> lock(mtx);
      coord_dups->insert(i);
    }

    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses) {
    if (!st.ok())
      return st;
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_compute_coord_dups_global);
}

Status Writer::compute_coords_metadata(
    const std::unordered_map<std::string, std::vector<Tile>>& tiles,
    FragmentMetadata* meta) const {
  // Applicable only if there are coordinates
  if (!has_coords_)
    return Status::Ok();

  // Check if tiles are empty
  if (tiles.empty() || tiles.begin()->second.empty())
    return Status::Ok();

  // Compute number of tiles. Assumes all attributes and
  // and dimensions have the same number of tiles
  auto it = tiles.begin();
  auto tile_num = array_schema_->var_size(it->first) ? it->second.size() / 2 :
                                                       it->second.size();
  auto dim_num = array_schema_->dim_num();

  // Compute MBRs
  auto statuses = parallel_for(0, tile_num, [&](uint64_t t) {
    NDRange mbr(dim_num);
    std::vector<const void*> data(dim_num);
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim = array_schema_->dimension(d);
      const auto& dim_name = dim->name();
      auto tiles_it = tiles.find(dim_name);
      assert(tiles_it != tiles.end());
      if (!dim->var_size())
        dim->compute_mbr(tiles_it->second[t], &mbr[d]);
      else
        dim->compute_mbr_var(
            tiles_it->second[2 * t], tiles_it->second[2 * t + 1], &mbr[d]);
    }

    meta->set_mbr(t, mbr);
    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  // Set last tile cell number
  auto dim_0 = array_schema_->dimension(0);
  const auto& dim_tiles = tiles.find(dim_0->name())->second;
  const auto& last_tile_pos =
      (!dim_0->var_size()) ? dim_tiles.size() - 1 : dim_tiles.size() - 2;
  meta->set_last_tile_cell_num(dim_tiles[last_tile_pos].cell_num());

  return Status::Ok();
}

template <class T>
Status Writer::compute_write_cell_ranges(
    WriteCellSlabIter<T>* iter, WriteCellRangeVec* write_cell_ranges) const {
  STATS_FUNC_IN(writer_compute_write_cell_ranges);

  auto domain = array_schema_->domain();
  auto dim_num = array_schema_->dim_num();
  assert(!subarray_flat_.empty());
  auto subarray = (const T*)&subarray_flat_[0];
  auto cell_order = array_schema_->cell_order();
  bool same_layout = (cell_order == layout_);
  uint64_t start, end, start_in_sub, end_in_sub;
  const T* coords_start;

  // Compute the offset needed in case there is a layout mismatch
  uint64_t offset = 1;
  if (!same_layout) {
    if (layout_ == Layout::COL_MAJOR) {  // Subarray layout is col-major
      for (unsigned i = 0; i < dim_num - 1; ++i)
        offset *= subarray[2 * i + 1] - subarray[2 * i] + 1;
    } else {  // Array layout is col-major, subarray layout is row-major
      if (dim_num > 1) {
        for (unsigned i = 1; i < dim_num; ++i)
          offset *= subarray[2 * i + 1] - subarray[2 * i] + 1;
      }
    }
  }

  RETURN_NOT_OK(iter->begin());
  while (!iter->end()) {
    start = iter->slab_start();
    end = iter->slab_end();
    coords_start = iter->coords_start();

    if (same_layout) {
      start_in_sub = (layout_ == Layout::ROW_MAJOR) ?
                         domain->get_cell_pos_row<T>(subarray, coords_start) :
                         domain->get_cell_pos_col<T>(subarray, coords_start);
      end_in_sub = start_in_sub + end - start;
      write_cell_ranges->emplace_back(start, start_in_sub, end_in_sub);
    } else {
      start_in_sub = (layout_ == Layout::ROW_MAJOR) ?
                         domain->get_cell_pos_row<T>(subarray, coords_start) :
                         domain->get_cell_pos_col<T>(subarray, coords_start);
      end_in_sub = start_in_sub;
      write_cell_ranges->emplace_back(start, start_in_sub, end_in_sub);
      for (++start; start <= end; ++start) {
        start_in_sub += offset;
        end_in_sub = start_in_sub;
        write_cell_ranges->emplace_back(start, start_in_sub, end_in_sub);
      }
    }
    ++(*iter);
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_compute_write_cell_ranges);
}

Status Writer::create_fragment(
    bool dense, std::shared_ptr<FragmentMetadata>* frag_meta) const {
  STATS_FUNC_IN(writer_create_fragment);

  URI uri;
  uint64_t timestamp = array_->timestamp();
  if (!fragment_uri_.to_string().empty()) {
    uri = fragment_uri_;
  } else {
    std::string new_fragment_str;
    RETURN_NOT_OK(new_fragment_name(timestamp, &new_fragment_str));
    uri = array_schema_->array_uri().join_path(new_fragment_str);
  }
  auto timestamp_range = std::pair<uint64_t, uint64_t>(timestamp, timestamp);
  *frag_meta = std::make_shared<FragmentMetadata>(
      storage_manager_, array_schema_, uri, timestamp_range, dense);

  RETURN_NOT_OK((*frag_meta)->init(subarray_.ndrange(0)));
  return storage_manager_->create_dir(uri);

  STATS_FUNC_OUT(writer_create_fragment);
}

Status Writer::filter_tiles(
    std::unordered_map<std::string, std::vector<Tile>>* tiles) const {
  // Coordinates
  auto num = buffers_.size();
  auto statuses = parallel_for(0, num, [&](uint64_t i) {
    auto buff_it = buffers_.begin();
    std::advance(buff_it, i);
    const auto& name = buff_it->first;
    RETURN_CANCEL_OR_ERROR(filter_tiles(name, &((*tiles)[name])));
    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();
}

Status Writer::filter_tiles(
    const std::string& name, std::vector<Tile>* tiles) const {
  STATS_FUNC_IN(writer_filter_tiles);

  bool var_size = array_schema_->var_size(name);
  // Filter all tiles
  auto tile_num = tiles->size();
  for (size_t i = 0; i < tile_num; ++i) {
    RETURN_NOT_OK(filter_tile(name, &(*tiles)[i], var_size));
    if (var_size) {
      ++i;
      RETURN_NOT_OK(filter_tile(name, &(*tiles)[i], false));
    }
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_filter_tiles);
}

Status Writer::filter_tile(
    const std::string& name, Tile* tile, bool offsets) const {
  const auto orig_size = tile->chunked_buffer()->size();

  // Get a copy of the appropriate filter pipeline.
  FilterPipeline filters =
      (offsets ? array_schema_->cell_var_offsets_filters() :
                 array_schema_->filters(name));

  // Append an encryption filter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  assert(!tile->filtered());
  RETURN_NOT_OK(filters.run_forward(tile));
  assert(tile->filtered());

  tile->set_pre_filtered_size(orig_size);
  STATS_COUNTER_ADD(writer_num_bytes_before_filtering, orig_size);

  return Status::Ok();
}

Status Writer::finalize_global_write_state() {
  assert(layout_ == Layout::GLOBAL_ORDER);
  auto meta = global_write_state_->frag_meta_.get();
  const auto& uri = meta->fragment_uri();

  // Handle last tile
  Status st = global_write_handle_last_tile();
  if (!st.ok()) {
    close_files(meta);
    clean_up(uri);
    return st;
  }

  // Close all files
  RETURN_NOT_OK_ELSE(close_files(meta), clean_up(uri));

  // Check that the same number of cells was written across attributes
  // and dimensions
  auto cell_num = global_write_state_->cells_written_[buffers_.begin()->first];
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    if (global_write_state_->cells_written_[name] != cell_num) {
      clean_up(uri);
      return LOG_STATUS(Status::WriterError(
          "Failed to finalize global write state; Different "
          "number of cells written across attributes and coordinates"));
    }
  }

  // Check if the total number of cells written is equal to the subarray size
  if (!has_coords_) {  // This implies a dense array
    auto expected_cell_num =
        array_schema_->domain()->cell_num(subarray_.ndrange(0));
    if (cell_num != expected_cell_num) {
      clean_up(uri);
      std::stringstream ss;
      ss << "Failed to finalize global write state; Number "
         << "of cells written (" << cell_num
         << ") is different from the number of cells expected ("
         << expected_cell_num << ") for the query subarray";
      return LOG_STATUS(Status::WriterError(ss.str()));
    }
  }

  // Flush fragment metadata to storage
  RETURN_NOT_OK_ELSE(meta->store(array_->get_encryption_key()), clean_up(uri));

  // Add written fragment info
  RETURN_NOT_OK_ELSE(add_written_fragment_info(uri), clean_up(uri));

  // The following will make the fragment visible
  auto ok_uri =
      URI(uri.remove_trailing_slash().to_string() + constants::ok_file_suffix);
  RETURN_NOT_OK_ELSE(storage_manager_->vfs()->touch(ok_uri), clean_up(uri));

  // Delete global write state
  global_write_state_.reset(nullptr);

  return st;
}

Status Writer::global_write() {
  // Applicable only to global write on dense/sparse arrays
  assert(layout_ == Layout::GLOBAL_ORDER);

  // Initialize the global write state if this is the first invocation
  if (!global_write_state_) {
    RETURN_CANCEL_OR_ERROR(init_global_write_state());
  }
  auto frag_meta = global_write_state_->frag_meta_.get();
  auto uri = frag_meta->fragment_uri();

  // Check for coordinate duplicates
  if (has_coords_) {
    RETURN_CANCEL_OR_ERROR(check_coord_dups());
    RETURN_CANCEL_OR_ERROR(check_global_order());
  }

  // Retrieve coordinate duplicates
  std::set<uint64_t> coord_dups;
  if (dedup_coords_)
    RETURN_CANCEL_OR_ERROR(compute_coord_dups(&coord_dups));

  std::unordered_map<std::string, std::vector<Tile>> tiles;
  RETURN_CANCEL_OR_ERROR_ELSE(
      prepare_full_tiles(coord_dups, &tiles), clean_up(uri));

  // Find number of tiles
  uint64_t tile_num = 0;
  if (!tiles.empty()) {
    auto it = tiles.begin();
    tile_num = array_schema_->var_size(it->first) ? it->second.size() / 2 :
                                                    it->second.size();
  }

  // No cells to be written
  if (tile_num == 0)
    return Status::Ok();

  // Set new number of tiles in the fragment metadata
  auto new_num_tiles = frag_meta->tile_index_base() + tile_num;
  frag_meta->set_num_tiles(new_num_tiles);

  // Compute coordinate metadata (if coordinates are present)
  RETURN_CANCEL_OR_ERROR_ELSE(
      compute_coords_metadata(tiles, frag_meta), clean_up(uri));

  // Filter all tiles
  RETURN_CANCEL_OR_ERROR_ELSE(filter_tiles(&tiles), clean_up(uri));

  // Write tiles for all attributes
  RETURN_CANCEL_OR_ERROR_ELSE(
      write_all_tiles(frag_meta, &tiles), clean_up(uri));

  // Increment the tile index base for the next global order write.
  frag_meta->set_tile_index_base(new_num_tiles);

  return Status::Ok();
}

Status Writer::global_write_handle_last_tile() {
  if (all_last_tiles_empty())
    return Status::Ok();

  // Reserve space for the last tile in the fragment metadata
  auto meta = global_write_state_->frag_meta_.get();
  meta->set_num_tiles(meta->tile_index_base() + 1);
  const auto& uri = global_write_state_->frag_meta_->fragment_uri();

  // Filter last tiles
  std::unordered_map<std::string, std::vector<Tile>> tiles;
  RETURN_CANCEL_OR_ERROR_ELSE(filter_last_tiles(&tiles), clean_up(uri));

  // Write the last tiles
  RETURN_CANCEL_OR_ERROR(write_all_tiles(meta, &tiles));

  // Increment the tile index base.
  meta->set_tile_index_base(meta->tile_index_base() + 1);

  return Status::Ok();
}

Status Writer::filter_last_tiles(
    std::unordered_map<std::string, std::vector<Tile>>* tiles) const {
  // Initialize attribute and coordinate tiles
  for (auto it : buffers_)
    (*tiles)[it.first] = std::vector<Tile>();

  // Prepare the tiles first
  uint64_t num = buffers_.size();
  auto statuses = parallel_for(0, num, [&](uint64_t i) {
    auto buff_it = buffers_.begin();
    std::advance(buff_it, i);
    const auto& name = &(buff_it->first);

    auto& last_tile = global_write_state_->last_tiles_[*name].first;
    auto& last_tile_var = global_write_state_->last_tiles_[*name].second;

    if (!last_tile.empty()) {
      std::vector<Tile>& tiles_ref = (*tiles)[*name];
      // Note making shallow clones here, as it's not necessary to copy the
      // underlying tile Buffers.
      tiles_ref.push_back(last_tile.clone(false));
      if (!last_tile_var.empty())
        tiles_ref.push_back(last_tile_var.clone(false));
    }
    return Status::Ok();
  });

  // Check statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  // Compute coordinates metadata
  auto meta = global_write_state_->frag_meta_.get();
  RETURN_NOT_OK(compute_coords_metadata(*tiles, meta));

  // Filter tiles
  RETURN_NOT_OK(filter_tiles(tiles));

  return Status::Ok();
}

bool Writer::all_last_tiles_empty() const {
  // See if any last attribute/coordinate tiles are nonempty
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    auto& last_tile = global_write_state_->last_tiles_[name].first;
    if (!last_tile.empty())
      return false;
  }

  return true;
}

Status Writer::init_global_write_state() {
  STATS_FUNC_IN(writer_init_global_write_state);

  // Create global array state object
  if (global_write_state_ != nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot initialize global write state; State not "
                            "properly finalized"));
  global_write_state_.reset(new GlobalWriteState);

  // Create fragment
  RETURN_NOT_OK(
      create_fragment(!has_coords_, &(global_write_state_->frag_meta_)));
  auto uri = global_write_state_->frag_meta_->fragment_uri();

  // Initialize global write state for attribute and coordinates
  for (const auto& it : buffers_) {
    // Initialize last tiles
    const auto& name = it.first;
    auto last_tile_pair = std::pair<std::string, std::pair<Tile, Tile>>(
        name, std::pair<Tile, Tile>(Tile(), Tile()));
    auto it_ret = global_write_state_->last_tiles_.emplace(last_tile_pair);

    if (!array_schema_->var_size(name)) {
      auto& last_tile = it_ret.first->second.first;
      RETURN_NOT_OK_ELSE(init_tile(name, &last_tile), clean_up(uri));
    } else {
      auto& last_tile = it_ret.first->second.first;
      auto& last_tile_var = it_ret.first->second.second;
      RETURN_NOT_OK_ELSE(
          init_tile(name, &last_tile, &last_tile_var), clean_up(uri));
    }

    // Initialize cells written
    global_write_state_->cells_written_[name] = 0;
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_init_global_write_state);
}

Status Writer::init_tile(const std::string& name, Tile* tile) const {
  // For easy reference
  auto cell_size = array_schema_->cell_size(name);
  auto type = array_schema_->type(name);
  auto domain = array_schema_->domain();
  auto capacity = array_schema_->capacity();
  auto cell_num_per_tile = has_coords_ ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * cell_size;

  // Initialize
  RETURN_NOT_OK(tile->init_unfiltered(
      constants::format_version, type, tile_size, cell_size, 0));

  return Status::Ok();
}

Status Writer::init_tile(
    const std::string& name, Tile* tile, Tile* tile_var) const {
  // For easy reference
  auto type = array_schema_->type(name);
  auto domain = array_schema_->domain();
  auto capacity = array_schema_->capacity();
  auto cell_num_per_tile = has_coords_ ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * constants::cell_var_offset_size;

  // Initialize
  RETURN_NOT_OK(tile->init_unfiltered(
      constants::format_version,
      constants::cell_var_offset_type,
      tile_size,
      constants::cell_var_offset_size,
      0));
  RETURN_NOT_OK(tile_var->init_unfiltered(
      constants::format_version, type, tile_size, datatype_size(type), 0));
  return Status::Ok();
}

template <class T>
Status Writer::init_tile_dense_cell_range_iters(
    std::vector<WriteCellSlabIter<T>>* iters) const {
  STATS_FUNC_IN(writer_init_tile_dense_cell_range_iters);

  // For easy reference
  auto domain = array_schema_->domain();
  auto dim_num = domain->dim_num();
  std::vector<T> subarray;
  subarray.resize(2 * dim_num);
  assert(!subarray_flat_.empty());
  for (unsigned i = 0; i < 2 * dim_num; ++i)
    subarray[i] = ((T*)&subarray_flat_[0])[i];
  auto cell_order = domain->cell_order();

  // Compute tile domain and current tile coords
  std::vector<T> tile_domain, tile_coords;
  tile_domain.resize(2 * dim_num);
  tile_coords.resize(dim_num);
  domain->get_tile_domain(&subarray[0], &tile_domain[0]);
  for (unsigned i = 0; i < dim_num; ++i)
    tile_coords[i] = tile_domain[2 * i];

  // TODO: fix
  NDRange sub(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    Range r(&subarray[2 * d], 2 * sizeof(T));
    sub[d] = std::move(r);
  }
  auto tile_num = domain->tile_num(sub);

  // Iterate over all tiles in the tile domain
  iters->clear();
  std::vector<T> tile_subarray, subarray_in_tile;
  std::vector<T> frag_subarray, frag_subarray_in_tile;
  tile_subarray.resize(2 * dim_num);
  subarray_in_tile.resize(2 * dim_num);
  bool tile_overlap, in;
  for (uint64_t i = 0; i < tile_num; ++i) {
    // Compute subarray overlap with tile
    domain->get_tile_subarray(&tile_coords[0], &tile_subarray[0]);
    utils::geometry::overlap(
        &subarray[0],
        &tile_subarray[0],
        dim_num,
        &subarray_in_tile[0],
        &tile_overlap);

    // Create a new iter
    iters->emplace_back(domain, subarray_in_tile, cell_order);

    // Get next tile coordinates
    domain->get_next_tile_coords(&tile_domain[0], &tile_coords[0], &in);
    assert((i != tile_num - 1 && in) || (i == tile_num - 1 && !in));
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_init_tile_dense_cell_range_iters);
}

Status Writer::init_tiles(
    const std::string& name,
    uint64_t tile_num,
    std::vector<Tile>* tiles) const {
  // Initialize tiles
  bool var_size = array_schema_->var_size(name);
  auto tiles_len = (var_size) ? 2 * tile_num : tile_num;
  tiles->resize(tiles_len);
  for (size_t i = 0; i < tiles_len; i += (1 + var_size)) {
    if (!var_size) {
      RETURN_NOT_OK(init_tile(name, &((*tiles)[i])));
    } else {
      RETURN_NOT_OK(init_tile(name, &((*tiles)[i]), &((*tiles)[i + 1])));
    }
  }

  return Status::Ok();
}

Status Writer::new_fragment_name(
    uint64_t timestamp, std::string* frag_uri) const {
  timestamp = (timestamp != 0) ? timestamp : utils::time::timestamp_now_ms();

  if (frag_uri == nullptr)
    return Status::WriterError("Null fragment uri argument.");
  std::string uuid;
  frag_uri->clear();
  RETURN_NOT_OK(uuid::generate_uuid(&uuid, false));
  std::stringstream ss;
  ss << "/__" << timestamp << "_" << timestamp << "_" << uuid << "_"
     << constants::format_version;

  *frag_uri = ss.str();
  return Status::Ok();
}

void Writer::nuke_global_write_state() {
  auto meta = global_write_state_->frag_meta_.get();
  close_files(meta);
  storage_manager_->vfs()->remove_dir(meta->fragment_uri());
  global_write_state_.reset(nullptr);
}

void Writer::optimize_layout_for_1D() {
  if (array_schema_->dim_num() == 1 && layout_ != Layout::GLOBAL_ORDER &&
      layout_ != Layout::UNORDERED)
    layout_ = array_schema_->cell_order();
}

Status Writer::ordered_write() {
  STATS_FUNC_IN(writer_ordered_write);

  // Applicable only to ordered write on dense arrays
  assert(layout_ == Layout::ROW_MAJOR || layout_ == Layout::COL_MAJOR);
  assert(array_schema_->dense());

  auto type = array_schema_->domain()->dimension(0)->type();
  switch (type) {
    case Datatype::INT8:
      return ordered_write<int8_t>();
    case Datatype::UINT8:
      return ordered_write<uint8_t>();
    case Datatype::INT16:
      return ordered_write<int16_t>();
    case Datatype::UINT16:
      return ordered_write<uint16_t>();
    case Datatype::INT32:
      return ordered_write<int32_t>();
    case Datatype::UINT32:
      return ordered_write<uint32_t>();
    case Datatype::INT64:
      return ordered_write<int64_t>();
    case Datatype::UINT64:
      return ordered_write<uint64_t>();
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      return ordered_write<int64_t>();
    default:
      return LOG_STATUS(Status::WriterError(
          "Cannot write in ordered layout; Unsupported domain type"));
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_ordered_write);
}

template <class T>
Status Writer::ordered_write() {
  // Create new fragment
  std::shared_ptr<FragmentMetadata> frag_meta;
  RETURN_CANCEL_OR_ERROR(create_fragment(true, &frag_meta));
  const auto& uri = frag_meta->fragment_uri();

  // Initialize dense cell range iterators for each tile in global order
  std::vector<WriteCellSlabIter<T>> dense_cell_range_its;
  RETURN_CANCEL_OR_ERROR_ELSE(
      init_tile_dense_cell_range_iters<T>(&dense_cell_range_its),
      clean_up(uri));
  auto tile_num = dense_cell_range_its.size();
  if (tile_num == 0)  // Nothing to write
    return Status::Ok();

  // Compute write cell ranges, one vector per overlapping tile
  std::vector<WriteCellRangeVec> write_cell_ranges;
  write_cell_ranges.resize(tile_num);
  for (size_t i = 0; i < tile_num; ++i)
    RETURN_CANCEL_OR_ERROR_ELSE(
        compute_write_cell_ranges<T>(
            &dense_cell_range_its[i], &write_cell_ranges[i]),
        clean_up(uri));
  dense_cell_range_its.clear();

  // Set number of tiles in the fragment metadata
  frag_meta->set_num_tiles(tile_num);

  // Prepare tiles and filter attribute tiles
  std::unordered_map<std::string, std::vector<Tile>> attr_tiles;
  RETURN_NOT_OK_ELSE(
      prepare_and_filter_attr_tiles(write_cell_ranges, &attr_tiles),
      clean_up(uri));

  // Write tiles for all attributes
  RETURN_NOT_OK_ELSE(
      write_all_tiles(frag_meta.get(), &attr_tiles),
      storage_manager_->vfs()->remove_dir(uri));

  // Write the fragment metadata
  RETURN_CANCEL_OR_ERROR_ELSE(
      frag_meta->store(array_->get_encryption_key()), clean_up(uri));

  // Add written fragment info
  RETURN_NOT_OK_ELSE(add_written_fragment_info(uri), clean_up(uri));

  // The following will make the fragment visible
  auto ok_uri =
      URI(uri.remove_trailing_slash().to_string() + constants::ok_file_suffix);
  RETURN_NOT_OK_ELSE(storage_manager_->vfs()->touch(ok_uri), clean_up(uri));

  return Status::Ok();
}

Status Writer::prepare_and_filter_attr_tiles(
    const std::vector<WriteCellRangeVec>& write_cell_ranges,
    std::unordered_map<std::string, std::vector<Tile>>* attr_tiles) const {
  // Initialize attribute tiles
  for (const auto& it : buffers_)
    (*attr_tiles)[it.first] = std::vector<Tile>();

  uint64_t attr_num = buffers_.size();
  auto statuses = parallel_for(0, attr_num, [&](uint64_t i) {
    auto buff_it = buffers_.begin();
    std::advance(buff_it, i);
    const auto& attr = buff_it->first;
    auto& tiles = (*attr_tiles)[attr];
    RETURN_CANCEL_OR_ERROR(prepare_tiles(attr, write_cell_ranges, &tiles));
    RETURN_CANCEL_OR_ERROR(filter_tiles(attr, &tiles));
    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();
}

Status Writer::prepare_full_tiles(
    const std::set<uint64_t>& coord_dups,
    std::unordered_map<std::string, std::vector<Tile>>* tiles) const {
  // Initialize attribute and coordinate tiles
  for (const auto& it : buffers_)
    (*tiles)[it.first] = std::vector<Tile>();

  auto num = buffers_.size();
  auto statuses = parallel_for(0, num, [&](uint64_t i) {
    auto buff_it = buffers_.begin();
    std::advance(buff_it, i);
    const auto& name = buff_it->first;
    RETURN_CANCEL_OR_ERROR(
        prepare_full_tiles(name, coord_dups, &(*tiles)[name]));
    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();
}

Status Writer::prepare_full_tiles(
    const std::string& name,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  return array_schema_->var_size(name) ?
             prepare_full_tiles_var(name, coord_dups, tiles) :
             prepare_full_tiles_fixed(name, coord_dups, tiles);
}

Status Writer::prepare_full_tiles_fixed(
    const std::string& name,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  STATS_FUNC_IN(writer_prepare_full_tiles_fixed);
  // For easy reference
  auto it = buffers_.find(name);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_size = it->second.buffer_size_;
  auto cell_size = array_schema_->cell_size(name);
  auto capacity = array_schema_->capacity();
  auto cell_num = *buffer_size / cell_size;
  auto domain = array_schema_->domain();
  auto cell_num_per_tile = has_coords_ ? capacity : domain->cell_num_per_tile();

  // Do nothing if there are no cells to write
  if (cell_num == 0)
    return Status::Ok();

  // First fill the last tile
  auto& last_tile = global_write_state_->last_tiles_[name].first;
  uint64_t cell_idx = 0;
  if (!last_tile.empty()) {
    if (coord_dups.empty()) {
      do {
        RETURN_NOT_OK(
            last_tile.write(buffer + cell_idx * cell_size, cell_size));
        ++cell_idx;
      } while (!last_tile.full() && cell_idx != cell_num);
    } else {
      do {
        if (coord_dups.find(cell_idx) == coord_dups.end()) {
          RETURN_NOT_OK(
              last_tile.write(buffer + cell_idx * cell_size, cell_size));
        }
        ++cell_idx;
      } while (!last_tile.full() && cell_idx != cell_num);
    }
  }

  // Initialize full tiles and set previous last tile as first tile
  auto full_tile_num =
      (cell_num - cell_idx) / cell_num_per_tile + (int)last_tile.full();
  auto cell_num_to_write =
      (full_tile_num - last_tile.full()) * cell_num_per_tile;

  if (full_tile_num > 0) {
    tiles->resize(full_tile_num);
    for (auto& tile : (*tiles))
      RETURN_NOT_OK(init_tile(name, &tile));

    // Handle last tile (it must be either full or empty)
    if (last_tile.full()) {
      (*tiles)[0] = last_tile;
      last_tile.reset();
    } else {
      assert(last_tile.empty());
    }

    // Write all remaining cells one by one
    if (coord_dups.empty()) {
      for (uint64_t tile_idx = 0, i = 0; i < cell_num_to_write;) {
        if ((*tiles)[tile_idx].full())
          ++tile_idx;

        RETURN_NOT_OK((*tiles)[tile_idx].write(
            buffer + cell_idx * cell_size, cell_size * cell_num_per_tile));
        cell_idx += cell_num_per_tile;
        i += cell_num_per_tile;
      }
    } else {
      for (uint64_t tile_idx = 0, i = 0; i < cell_num_to_write;
           ++cell_idx, ++i) {
        if (coord_dups.find(cell_idx) == coord_dups.end()) {
          if ((*tiles)[tile_idx].full())
            ++tile_idx;

          RETURN_NOT_OK((*tiles)[tile_idx].write(
              buffer + cell_idx * cell_size, cell_size));
        }
      }
    }
  }

  // Potentially fill the last tile
  assert(cell_num - cell_idx < cell_num_per_tile - last_tile.cell_num());
  if (coord_dups.empty()) {
    for (; cell_idx < cell_num; ++cell_idx) {
      RETURN_NOT_OK(last_tile.write(buffer + cell_idx * cell_size, cell_size));
    }
  } else {
    for (; cell_idx < cell_num; ++cell_idx) {
      if (coord_dups.find(cell_idx) == coord_dups.end())
        RETURN_NOT_OK(
            last_tile.write(buffer + cell_idx * cell_size, cell_size));
    }
  }

  global_write_state_->cells_written_[name] += cell_num;

  return Status::Ok();

  STATS_FUNC_OUT(writer_prepare_full_tiles_fixed);
}

Status Writer::prepare_full_tiles_var(
    const std::string& name,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  STATS_FUNC_IN(writer_prepare_full_tiles_var);

  // For easy reference
  auto it = buffers_.find(name);
  auto buffer = (uint64_t*)it->second.buffer_;
  auto buffer_var = (unsigned char*)it->second.buffer_var_;
  auto buffer_size = it->second.buffer_size_;
  auto buffer_var_size = it->second.buffer_var_size_;
  auto capacity = array_schema_->capacity();
  auto cell_num = *buffer_size / constants::cell_var_offset_size;
  auto domain = array_schema_->domain();
  auto cell_num_per_tile = has_coords_ ? capacity : domain->cell_num_per_tile();
  uint64_t offset, var_size;

  // Do nothing if there are no cells to write
  if (cell_num == 0)
    return Status::Ok();

  // First fill the last tile
  auto& last_tile_pair = global_write_state_->last_tiles_[name];
  auto& last_tile = last_tile_pair.first;
  auto& last_tile_var = last_tile_pair.second;

  uint64_t cell_idx = 0;
  if (!last_tile.empty()) {
    if (coord_dups.empty()) {
      do {
        // Write offset
        offset = last_tile_var.size();
        RETURN_NOT_OK(last_tile.write(&offset, sizeof(offset)));

        // Write var-sized value
        var_size = (cell_idx == cell_num - 1) ?
                       *buffer_var_size - buffer[cell_idx] :
                       buffer[cell_idx + 1] - buffer[cell_idx];
        RETURN_NOT_OK(
            last_tile_var.write(&buffer_var[buffer[cell_idx]], var_size));

        ++cell_idx;
      } while (!last_tile.full() && cell_idx != cell_num);
    } else {
      do {
        if (coord_dups.find(cell_idx) == coord_dups.end()) {
          // Write offset
          offset = last_tile_var.size();
          RETURN_NOT_OK(last_tile.write(&offset, sizeof(offset)));

          // Write var-sized value
          var_size = (cell_idx == cell_num - 1) ?
                         *buffer_var_size - buffer[cell_idx] :
                         buffer[cell_idx + 1] - buffer[cell_idx];
          RETURN_NOT_OK(
              last_tile_var.write(&buffer_var[buffer[cell_idx]], var_size));
        }

        ++cell_idx;
      } while (!last_tile.full() && cell_idx != cell_num);
    }
  }

  // Initialize full tiles and set previous last tile as first tile
  auto full_tile_num =
      (cell_num - cell_idx) / cell_num_per_tile + last_tile.full();
  auto cell_num_to_write =
      (full_tile_num - last_tile.full()) * cell_num_per_tile;

  if (full_tile_num > 0) {
    tiles->resize(2 * full_tile_num);
    auto tiles_len = tiles->size();
    for (uint64_t i = 0; i < tiles_len; i += 2)
      RETURN_NOT_OK(init_tile(name, &((*tiles)[i]), &((*tiles)[i + 1])));

    // Handle last tile (it must be either full or empty)
    if (last_tile.full()) {
      (*tiles)[0] = last_tile;
      last_tile.reset();
      (*tiles)[1] = last_tile_var;
      last_tile_var.reset();
    } else {
      assert(last_tile.empty());
      assert(last_tile_var.empty());
    }

    // Write all remaining cells one by one
    if (coord_dups.empty()) {
      for (uint64_t tile_idx = 0, i = 0; i < cell_num_to_write;
           ++cell_idx, ++i) {
        if ((*tiles)[tile_idx].full())
          tile_idx += 2;

        // Write offset
        offset = (*tiles)[tile_idx + 1].size();
        RETURN_NOT_OK((*tiles)[tile_idx].write(&offset, sizeof(offset)));

        // Write var-sized value
        var_size = (cell_idx == cell_num - 1) ?
                       *buffer_var_size - buffer[cell_idx] :
                       buffer[cell_idx + 1] - buffer[cell_idx];
        RETURN_NOT_OK((*tiles)[tile_idx + 1].write(
            &buffer_var[buffer[cell_idx]], var_size));
      }
    } else {
      for (uint64_t tile_idx = 0, i = 0; i < cell_num_to_write;
           ++cell_idx, ++i) {
        if (coord_dups.find(cell_idx) == coord_dups.end()) {
          if ((*tiles)[tile_idx].full())
            tile_idx += 2;

          // Write offset
          offset = (*tiles)[tile_idx + 1].size();
          RETURN_NOT_OK((*tiles)[tile_idx].write(&offset, sizeof(offset)));

          // Write var-sized value
          var_size = (cell_idx == cell_num - 1) ?
                         *buffer_var_size - buffer[cell_idx] :
                         buffer[cell_idx + 1] - buffer[cell_idx];
          RETURN_NOT_OK((*tiles)[tile_idx + 1].write(
              &buffer_var[buffer[cell_idx]], var_size));
        }
      }
    }
  }

  // Potentially fill the last tile
  assert(cell_num - cell_idx < cell_num_per_tile - last_tile.cell_num());
  if (coord_dups.empty()) {
    for (; cell_idx < cell_num; ++cell_idx) {
      // Write offset
      offset = last_tile_var.size();
      RETURN_NOT_OK(last_tile.write(&offset, sizeof(offset)));

      // Write var-sized value
      var_size = (cell_idx == cell_num - 1) ?
                     *buffer_var_size - buffer[cell_idx] :
                     buffer[cell_idx + 1] - buffer[cell_idx];
      RETURN_NOT_OK(
          last_tile_var.write(&buffer_var[buffer[cell_idx]], var_size));
    }
  } else {
    for (; cell_idx < cell_num; ++cell_idx) {
      if (coord_dups.find(cell_idx) == coord_dups.end()) {
        // Write offset
        offset = last_tile_var.size();
        RETURN_NOT_OK(last_tile.write(&offset, sizeof(offset)));

        // Write var-sized value
        var_size = (cell_idx == cell_num - 1) ?
                       *buffer_var_size - buffer[cell_idx] :
                       buffer[cell_idx + 1] - buffer[cell_idx];
        RETURN_NOT_OK(
            last_tile_var.write(&buffer_var[buffer[cell_idx]], var_size));
      }
    }
  }

  global_write_state_->cells_written_[name] += cell_num;

  return Status::Ok();

  STATS_FUNC_OUT(writer_prepare_full_tiles_var);
}

Status Writer::prepare_tiles(
    const std::string& attribute,
    const std::vector<WriteCellRangeVec>& write_cell_ranges,
    std::vector<Tile>* tiles) const {
  STATS_FUNC_IN(writer_prepare_tiles_ordered);

  // Trivial case
  auto tile_num = write_cell_ranges.size();
  if (tile_num == 0)
    return Status::Ok();

  // For easy reference
  auto var_size = array_schema_->var_size(attribute);
  auto it = buffers_.find(attribute);
  auto buffer = (uint64_t*)it->second.buffer_;
  auto buffer_var = (uint64_t*)it->second.buffer_var_;
  auto buffer_size = it->second.buffer_size_;
  auto buffer_var_size = it->second.buffer_var_size_;
  auto cell_val_num = array_schema_->cell_val_num(attribute);

  // Initialize tiles and buffer
  RETURN_NOT_OK(init_tiles(attribute, tile_num, tiles));
  auto buff = std::make_shared<ConstBuffer>(buffer, *buffer_size);
  auto buff_var =
      (!var_size) ? nullptr :
                    std::make_shared<ConstBuffer>(buffer_var, *buffer_var_size);

  // Populate each tile with the write cell ranges
  uint64_t end_pos = array_schema_->domain()->cell_num_per_tile() - 1;
  for (size_t i = 0, t = 0; i < tile_num; ++i, t += (var_size) ? 2 : 1) {
    uint64_t pos = 0;
    for (const auto& wcr : write_cell_ranges[i]) {
      // Write empty range
      if (wcr.pos_ > pos) {
        if (var_size)
          RETURN_NOT_OK(write_empty_cell_range_to_tile_var(
              wcr.pos_ - pos, &(*tiles)[t], &(*tiles)[t + 1]));
        else
          RETURN_NOT_OK(write_empty_cell_range_to_tile(
              (wcr.pos_ - pos) * cell_val_num, &(*tiles)[t]));
        pos = wcr.pos_;
      }

      // Write (non-empty) range
      if (var_size) {
        RETURN_NOT_OK(write_cell_range_to_tile_var(
            buff.get(),
            buff_var.get(),
            wcr.start_,
            wcr.end_,
            &(*tiles)[t],
            &(*tiles)[t + 1]));
      } else {
        RETURN_NOT_OK(write_cell_range_to_tile(
            buff.get(), wcr.start_, wcr.end_, &(*tiles)[t]));
      }
      pos += wcr.end_ - wcr.start_ + 1;
    }

    // Write empty range
    if (pos <= end_pos) {
      if (var_size) {
        RETURN_NOT_OK(write_empty_cell_range_to_tile_var(
            end_pos - pos + 1, &(*tiles)[t], &(*tiles)[t + 1]));
      } else {
        RETURN_NOT_OK(write_empty_cell_range_to_tile(
            (end_pos - pos + 1) * cell_val_num, &(*tiles)[t]));
      }
    }
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_prepare_tiles_ordered);
}

Status Writer::prepare_tiles(
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    std::unordered_map<std::string, std::vector<Tile>>* tiles) const {
  // Initialize attribute tiles
  tiles->clear();
  for (const auto& it : buffers_)
    (*tiles)[it.first] = std::vector<Tile>();

  // Prepare tiles for all attributes and coordinates
  auto buffer_num = buffers_.size();
  auto statuses = parallel_for(0, buffer_num, [&](uint64_t i) {
    auto buff_it = buffers_.begin();
    std::advance(buff_it, i);
    const auto& name = buff_it->first;
    RETURN_CANCEL_OR_ERROR(
        prepare_tiles(name, cell_pos, coord_dups, &((*tiles)[name])));
    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();
}

Status Writer::prepare_tiles(
    const std::string& name,
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  return array_schema_->var_size(name) ?
             prepare_tiles_var(name, cell_pos, coord_dups, tiles) :
             prepare_tiles_fixed(name, cell_pos, coord_dups, tiles);
}

Status Writer::prepare_tiles_fixed(
    const std::string& name,
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  STATS_FUNC_IN(writer_prepare_tiles_fixed);

  // Trivial case
  if (cell_pos.empty())
    return Status::Ok();

  // For easy reference
  auto buffer = (unsigned char*)buffers_.find(name)->second.buffer_;
  auto cell_size = array_schema_->cell_size(name);
  auto cell_num = (uint64_t)cell_pos.size();
  auto capacity = array_schema_->capacity();
  auto dups_num = coord_dups.size();
  auto tile_num = utils::math::ceil(cell_num - dups_num, capacity);

  // Initialize tiles
  tiles->resize(tile_num);
  for (auto& tile : (*tiles))
    RETURN_NOT_OK(init_tile(name, &tile));

  // Write all cells one by one
  if (dups_num == 0) {
    for (uint64_t i = 0, tile_idx = 0; i < cell_num; ++i) {
      if ((*tiles)[tile_idx].full())
        ++tile_idx;

      RETURN_NOT_OK((*tiles)[tile_idx].write(
          buffer + cell_pos[i] * cell_size, cell_size));
    }
  } else {
    for (uint64_t i = 0, tile_idx = 0; i < cell_num; ++i) {
      if (coord_dups.find(cell_pos[i]) != coord_dups.end())
        continue;

      if ((*tiles)[tile_idx].full())
        ++tile_idx;

      RETURN_NOT_OK((*tiles)[tile_idx].write(
          buffer + cell_pos[i] * cell_size, cell_size));
    }
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_prepare_tiles_fixed);
}

Status Writer::prepare_tiles_var(
    const std::string& name,
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  STATS_FUNC_IN(writer_prepare_tiles_var);

  // For easy reference
  auto it = buffers_.find(name);
  auto buffer = (uint64_t*)it->second.buffer_;
  auto buffer_var = (unsigned char*)it->second.buffer_var_;
  auto buffer_var_size = it->second.buffer_var_size_;
  auto cell_num = (uint64_t)cell_pos.size();
  auto capacity = array_schema_->capacity();
  auto dups_num = coord_dups.size();
  auto tile_num = utils::math::ceil(cell_num - dups_num, capacity);
  uint64_t offset;
  uint64_t var_size;

  // Initialize tiles
  tiles->resize(2 * tile_num);
  auto tiles_len = tiles->size();
  for (uint64_t i = 0; i < tiles_len; i += 2)
    RETURN_NOT_OK(init_tile(name, &((*tiles)[i]), &((*tiles)[i + 1])));

  // Write all cells one by one
  if (dups_num == 0) {
    for (uint64_t i = 0, tile_idx = 0; i < cell_num; ++i) {
      if ((*tiles)[tile_idx].full())
        tile_idx += 2;

      // Write offset
      offset = (*tiles)[tile_idx + 1].size();
      RETURN_NOT_OK((*tiles)[tile_idx].write(&offset, sizeof(offset)));

      // Write var-sized value
      var_size = (cell_pos[i] == cell_num - 1) ?
                     *buffer_var_size - buffer[cell_pos[i]] :
                     buffer[cell_pos[i] + 1] - buffer[cell_pos[i]];
      RETURN_NOT_OK((*tiles)[tile_idx + 1].write(
          &buffer_var[buffer[cell_pos[i]]], var_size));
    }
  } else {
    for (uint64_t i = 0, tile_idx = 0; i < cell_num; ++i) {
      if (coord_dups.find(cell_pos[i]) != coord_dups.end())
        continue;

      if ((*tiles)[tile_idx].full())
        tile_idx += 2;

      // Write offset
      offset = (*tiles)[tile_idx + 1].size();
      RETURN_NOT_OK((*tiles)[tile_idx].write(&offset, sizeof(offset)));

      // Write var-sized value
      var_size = (cell_pos[i] == cell_num - 1) ?
                     *buffer_var_size - buffer[cell_pos[i]] :
                     buffer[cell_pos[i] + 1] - buffer[cell_pos[i]];
      RETURN_NOT_OK((*tiles)[tile_idx + 1].write(
          &buffer_var[buffer[cell_pos[i]]], var_size));
    }
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_prepare_tiles_var);
}

void Writer::reset() {
  if (global_write_state_ != nullptr)
    nuke_global_write_state();
  initialized_ = false;
}

Status Writer::sort_coords(std::vector<uint64_t>* cell_pos) const {
  STATS_FUNC_IN(writer_sort_coords);

  // For easy reference
  auto domain = array_schema_->domain();

  // Prepare auxiliary vector for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<const QueryBuffer*> buffs(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = &(buffers_.find(dim_name)->second);
  }

  // Populate cell_pos
  cell_pos->resize(coords_num_);
  for (uint64_t i = 0; i < coords_num_; ++i)
    (*cell_pos)[i] = i;

  // Sort the coordinates in global order
  parallel_sort(cell_pos->begin(), cell_pos->end(), GlobalCmp(domain, &buffs));

  return Status::Ok();

  STATS_FUNC_OUT(writer_sort_coords);
}

Status Writer::split_coords_buffer() {
  // Do nothing if the coordinates buffer is not set
  if (coords_buffer_ == nullptr)
    return Status::Ok();

  // For easy reference
  auto dim_num = array_schema_->dim_num();
  auto coord_size = array_schema_->domain()->dimension(0)->coord_size();
  auto coords_size = dim_num * coord_size;
  coords_num_ = *coords_buffer_size_ / coords_size;

  clear_coord_buffers();

  // New coord buffer allocations
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = array_schema_->dimension(d);
    const auto& dim_name = dim->name();
    auto coord_buffer_size = coords_num_ * dim->coord_size();
    auto it = coord_buffer_sizes_.emplace(dim_name, coord_buffer_size);
    QueryBuffer buff;
    buff.buffer_size_ = &(it.first->second);
    buff.buffer_ = std::malloc(coord_buffer_size);
    to_clean_.push_back(buff.buffer_);
    if (buff.buffer_ == nullptr)
      RETURN_NOT_OK(Status::WriterError(
          "Cannot split coordinate buffers; memory allocation failed"));
    buffers_[dim_name] = buff;
  }

  // Split coordinates
  auto coord = (unsigned char*)nullptr;
  for (unsigned d = 0; d < dim_num; ++d) {
    auto coord_size = array_schema_->dimension(d)->coord_size();
    const auto& dim_name = array_schema_->dimension(d)->name();
    auto buff = (unsigned char*)(buffers_[dim_name].buffer_);
    for (uint64_t c = 0; c < coords_num_; ++c) {
      coord =
          &(((unsigned char*)coords_buffer_)[c * coords_size + d * coord_size]);
      std::memcpy(&(buff[c * coord_size]), coord, coord_size);
    }
  }

  return Status::Ok();
}

Status Writer::unordered_write() {
  // Applicable only to unordered write on dense/sparse arrays
  assert(layout_ == Layout::UNORDERED);

  // Sort coordinates first
  std::vector<uint64_t> cell_pos;
  RETURN_CANCEL_OR_ERROR(sort_coords(&cell_pos));

  // Check for coordinate duplicates
  RETURN_CANCEL_OR_ERROR(check_coord_dups(cell_pos));

  // Retrieve coordinate duplicates
  std::set<uint64_t> coord_dups;
  if (dedup_coords_)
    RETURN_CANCEL_OR_ERROR(compute_coord_dups(cell_pos, &coord_dups));

  // Create new fragment
  std::shared_ptr<FragmentMetadata> frag_meta;
  RETURN_CANCEL_OR_ERROR(create_fragment(false, &frag_meta));
  const auto& uri = frag_meta->fragment_uri();

  // Prepare tiles
  std::unordered_map<std::string, std::vector<Tile>> tiles;
  RETURN_CANCEL_OR_ERROR_ELSE(
      prepare_tiles(cell_pos, coord_dups, &tiles), clean_up(uri));

  // Clear the boolean vector for coordinate duplicates
  coord_dups.clear();

  // No tiles
  if (tiles.empty() || tiles.begin()->second.empty())
    return Status::Ok();

  // Set the number of tiles in the metadata
  auto it = tiles.begin();
  auto tile_num = array_schema_->var_size(it->first) ? it->second.size() / 2 :
                                                       it->second.size();
  frag_meta->set_num_tiles(tile_num);

  // Compute coordinates metadata
  RETURN_CANCEL_OR_ERROR_ELSE(
      compute_coords_metadata(tiles, frag_meta.get()), clean_up(uri));

  // Filter all tiles
  RETURN_CANCEL_OR_ERROR_ELSE(filter_tiles(&tiles), clean_up(uri));

  // Write tiles for all attributes and coordinates
  RETURN_CANCEL_OR_ERROR_ELSE(
      write_all_tiles(frag_meta.get(), &tiles), clean_up(uri));

  // Write the fragment metadata
  RETURN_CANCEL_OR_ERROR_ELSE(
      frag_meta->store(array_->get_encryption_key()), clean_up(uri));

  // Add written fragment info
  RETURN_NOT_OK_ELSE(add_written_fragment_info(uri), clean_up(uri));

  // The following will make the fragment visible
  auto ok_uri =
      URI(uri.remove_trailing_slash().to_string() + constants::ok_file_suffix);
  RETURN_NOT_OK_ELSE(storage_manager_->vfs()->touch(ok_uri), clean_up(uri));

  return Status::Ok();
}

Status Writer::write_empty_cell_range_to_tile(uint64_t num, Tile* tile) const {
  auto type = tile->type();
  auto fill_size = datatype_size(type);
  auto fill_value = constants::fill_value(type);
  assert(fill_value != nullptr);

  for (uint64_t i = 0; i < num; ++i)
    RETURN_NOT_OK(tile->write(fill_value, fill_size));

  return Status::Ok();
}

Status Writer::write_empty_cell_range_to_tile_var(
    uint64_t num, Tile* tile, Tile* tile_var) const {
  auto type = tile_var->type();
  auto fill_size = datatype_size(type);
  auto fill_value = constants::fill_value(type);
  assert(fill_value != nullptr);

  for (uint64_t i = 0; i < num; ++i) {
    // Write next offset
    uint64_t next_offset = tile_var->size();
    RETURN_NOT_OK(tile->write(&next_offset, sizeof(uint64_t)));

    // Write variable-sized empty value
    RETURN_NOT_OK(tile_var->write(fill_value, fill_size));
  }

  return Status::Ok();
}

Status Writer::write_cell_range_to_tile(
    ConstBuffer* buff, uint64_t start, uint64_t end, Tile* tile) const {
  auto cell_size = tile->cell_size();
  buff->set_offset(start * cell_size);
  return tile->write(buff, (end - start + 1) * cell_size);
}

Status Writer::write_cell_range_to_tile_var(
    ConstBuffer* buff,
    ConstBuffer* buff_var,
    uint64_t start,
    uint64_t end,
    Tile* tile,
    Tile* tile_var) const {
  auto buff_cell_num = buff->size() / sizeof(uint64_t);
  for (auto i = start; i <= end; ++i) {
    // Write next offset
    uint64_t next_offset = tile_var->size();
    RETURN_NOT_OK(tile->write(&next_offset, sizeof(uint64_t)));

    // Write variable-sized value
    auto last_cell = (i == buff_cell_num - 1);
    auto start_offset = buff->value<uint64_t>(i * sizeof(uint64_t));
    auto end_offset = last_cell ?
                          buff_var->size() :
                          buff->value<uint64_t>((i + 1) * sizeof(uint64_t));
    auto cell_var_size = end_offset - start_offset;
    buff_var->set_offset(start_offset);
    RETURN_NOT_OK(tile_var->write(buff_var, cell_var_size));
  }

  return Status::Ok();
}

Status Writer::write_all_tiles(
    FragmentMetadata* frag_meta,
    std::unordered_map<std::string, std::vector<Tile>>* const tiles) const {
  STATS_FUNC_IN(writer_write_all_tiles);

  assert(!tiles->empty());

  std::vector<std::future<Status>> tasks;
  for (auto& it : *tiles) {
    tasks.push_back(
        storage_manager_->writer_thread_pool()->enqueue([&, this]() {
          RETURN_CANCEL_OR_ERROR(write_tiles(it.first, frag_meta, &it.second));
          return Status::Ok();
        }));
  }

  // Wait for writes and check all statuses
  auto statuses =
      storage_manager_->writer_thread_pool()->wait_all_status(tasks);
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();

  STATS_FUNC_OUT(writer_write_all_tiles);
}

Status Writer::write_tiles(
    const std::string& name,
    FragmentMetadata* frag_meta,
    std::vector<Tile>* const tiles) const {
  // Handle zero tiles
  if (tiles->empty())
    return Status::Ok();

  // For easy reference
  bool var_size = array_schema_->var_size(name);
  const auto& uri = frag_meta->uri(name);
  const auto& var_uri = var_size ? frag_meta->var_uri(name) : URI("");

  // Write tiles
  auto tile_num = tiles->size();
  for (size_t i = 0, tile_id = 0; i < tile_num; ++i, ++tile_id) {
    Tile* tile = &(*tiles)[i];
    RETURN_NOT_OK(storage_manager_->write(uri, tile->filtered_buffer()));
    frag_meta->set_tile_offset(name, tile_id, tile->filtered_buffer()->size());

    STATS_COUNTER_ADD(
        writer_num_bytes_written, tile->filtered_buffer()->size());

    if (var_size) {
      ++i;

      tile = &(*tiles)[i];
      RETURN_NOT_OK(storage_manager_->write(var_uri, tile->filtered_buffer()));
      frag_meta->set_tile_var_offset(
          name, tile_id, tile->filtered_buffer()->size());
      frag_meta->set_tile_var_size(name, tile_id, tile->pre_filtered_size());

      STATS_COUNTER_ADD(
          writer_num_bytes_written, tile->filtered_buffer()->size());
    }
  }

  // Close files, except in the case of global order
  if (layout_ != Layout::GLOBAL_ORDER) {
    RETURN_NOT_OK(storage_manager_->close_file(frag_meta->uri(name)));
    if (var_size)
      RETURN_NOT_OK(storage_manager_->close_file(frag_meta->var_uri(name)));
  }

  STATS_COUNTER_ADD(writer_num_attr_tiles_written, tile_num);

  return Status::Ok();
}

std::string Writer::coords_to_str(uint64_t i) const {
  std::stringstream ss;
  auto dim_num = array_schema_->dim_num();

  ss << "(";
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = array_schema_->dimension(d);
    const auto& dim_name = dim->name();
    auto buff = buffers_.find(dim_name)->second;
    ss << dim->coord_to_str(buff, i);
    if (d < dim_num - 1)
      ss << ", ";
  }
  ss << ")";

  return ss.str();
}

void Writer::clean_up(const URI& uri) {
  storage_manager_->vfs()->remove_dir(uri);
  global_write_state_.reset(nullptr);
}

Status Writer::set_coords_buffer(void* buffer, uint64_t* buffer_size) {
  // Error if setting non-existing coordinates after initialization
  if (initialized_ && has_coords_)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set coordinates after initialization")));

  if (coord_buffer_is_set_)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set zipped coordinates buffer after having set "
                    "separate coordinate buffers")));

  // Set zipped coordinates buffer
  coords_buffer_ = buffer;
  coords_buffer_size_ = buffer_size;
  has_coords_ = true;

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
