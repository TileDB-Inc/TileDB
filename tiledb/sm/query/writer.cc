/**
 * @file   writer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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
  coord_buffers_alloced_ = false;
  coords_num_ = 0;
  global_write_state_.reset(nullptr);
  initialized_ = false;
  layout_ = Layout::ROW_MAJOR;
  storage_manager_ = nullptr;
  subarray_ = nullptr;
}

Writer::~Writer() {
  std::free(subarray_);
  clear_coord_buffers();
}

/* ****************************** */
/*               API              */
/* ****************************** */

const ArraySchema* Writer::array_schema() const {
  return array_schema_;
}

std::vector<std::string> Writer::buffer_names() const {
  std::vector<std::string> ret;
  const size_t ret_size =
      attr_buffers_.size() + (coords_buffer_ ? 1 : coord_buffers_.size());
  ret.reserve(ret_size);

  // Attributes
  for (const auto& it : attr_buffers_)
    ret.push_back(it.first);

  // Coordinates
  if (coords_buffer_ != nullptr) {
    ret.push_back(constants::coords);
  } else {
    for (const auto& it : coord_buffers_)
      ret.push_back(it.first);
  }

  return ret;
}

QueryBuffer Writer::buffer(const std::string& name) const {
  // Special zipped coordinates
  if (name == constants::coords)
    return QueryBuffer(coords_buffer_, nullptr, coords_buffer_size_, nullptr);

  // Attribute
  auto attr_buf = attr_buffers_.find(name);
  if (attr_buf != attr_buffers_.end())
    return attr_buf->second;

  // Dimension
  auto coord_buf = coord_buffers_.find(name);
  if (coord_buf != coord_buffers_.end())
    return coord_buf->second;

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

  // Attribute
  auto attr_it = attr_buffers_.find(name);
  if (attr_it != attr_buffers_.end()) {
    *buffer = attr_it->second.buffer_;
    *buffer_size = attr_it->second.buffer_size_;
    return Status::Ok();
  }

  // Dimension
  auto coord_it = coord_buffers_.find(name);
  if (coord_it != coord_buffers_.end()) {
    *buffer = coord_it->second.buffer_;
    *buffer_size = coord_it->second.buffer_size_;
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
  // Attribute
  auto attr_it = attr_buffers_.find(name);
  if (attr_it != attr_buffers_.end()) {
    *buffer_off = (uint64_t*)attr_it->second.buffer_;
    *buffer_off_size = attr_it->second.buffer_size_;
    *buffer_val = attr_it->second.buffer_var_;
    *buffer_val_size = attr_it->second.buffer_var_size_;
    return Status::Ok();
  }

  // Dimension
  auto coord_it = coord_buffers_.find(name);
  if (coord_it != coord_buffers_.end()) {
    *buffer_off = (uint64_t*)coord_it->second.buffer_;
    *buffer_off_size = coord_it->second.buffer_size_;
    *buffer_val = coord_it->second.buffer_var_;
    *buffer_val_size = coord_it->second.buffer_var_size_;
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

Status Writer::init() {
  // Sanity checks
  if (storage_manager_ == nullptr)
    return LOG_STATUS(Status::WriterError(
        "Cannot initialize query; Storage manager not set"));
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot initialize query; Array schema not set"));
  if (attr_buffers_.empty())
    return LOG_STATUS(
        Status::WriterError("Cannot initialize query; Buffers not set"));

  if (subarray_ == nullptr)
    RETURN_NOT_OK(set_subarray(nullptr));
  RETURN_NOT_OK(check_subarray());
  RETURN_NOT_OK(check_buffer_sizes());
  RETURN_NOT_OK(check_buffer_names());

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

  auto dim_num = array_schema_->dim_num();
  coord_sizes_.resize(dim_num);
  for (unsigned d = 0; d < dim_num; ++d)
    coord_sizes_[d] = array_schema_->dimension(d)->coord_size();

  return Status::Ok();
}

Layout Writer::layout() const {
  return layout_;
}

void Writer::set_array(const Array* array) {
  array_ = array;
}

void Writer::set_array_schema(const ArraySchema* array_schema) {
  array_schema_ = array_schema;
}

Status Writer::set_buffer(
    const std::string& name, void* buffer, uint64_t* buffer_size) {
  std::cerr << "JOE Writer::set_buffer 1" << std::endl;

  // Check buffer
  if (buffer == nullptr || buffer_size == nullptr)
    return LOG_STATUS(Status::WriterError(
        "Cannot set buffer; Buffer or buffer size is null"));

  std::cerr << "JOE Writer::set_buffer 2" << std::endl;

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot set buffer; Array schema not set"));

  // Invoke the appropriate fuinction based on the buffer name
  if (name == constants::coords) {
    std::cerr << "JOE Writer::set_buffer 3" << std::endl;
    return set_coords_buffer(buffer, buffer_size);
  } else if (array_schema_->attribute(name) != nullptr) {
    std::cerr << "JOE Writer::set_buffer 4" << std::endl;
    return set_attr_buffer(name, buffer, buffer_size);
  } else if (array_schema_->dimension(name) != nullptr) {
    std::cerr << "JOE Writer::set_buffer 5" << std::endl;
    return set_coord_buffer(name, buffer, buffer_size);
  } else {
    std::cerr << "JOE Writer::set_buffer 6" << std::endl;
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer; Invalid buffer name '") + name +
        "' (it should "
        "be an attribute or dimension)"));
  }

  std::cerr << "JOE Writer::set_buffer 7" << std::endl;

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

  // Check that attribute exists
  if (name != constants::coords && array_schema_->attribute(name) == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot set buffer; Invalid attribute"));

  // Check that attribute is var-sized
  bool var_size = (name != constants::coords && array_schema_->var_size(name));
  if (!var_size)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer; Input attribute '") + name +
        "' is fixed-sized"));

  // Error if setting a new attribute after initialization
  bool attr_exists = attr_buffers_.find(name) != attr_buffers_.end();
  if (initialized_ && !attr_exists)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer for new attribute '") + name +
        "' after initialization"));

  // Set attribute buffer
  attr_buffers_[name] =
      QueryBuffer(buffer_off, buffer_val, buffer_off_size, buffer_val_size);

  return Status::Ok();
}

void Writer::set_fragment_uri(const URI& fragment_uri) {
  fragment_uri_ = fragment_uri;
}

Status Writer::set_layout(Layout layout) {
  // Ordered layout for writes in sparse arrays is meaningless
  if (!array_schema_->dense() &&
      (layout == Layout::COL_MAJOR || layout == Layout::ROW_MAJOR))
    return LOG_STATUS(
        Status::WriterError("Cannot set layout; Ordered layouts cannot be used "
                            "when writing to sparse "
                            "arrays - use GLOBAL_ORDER or UNORDERED instead"));

  layout_ = layout;

  return Status::Ok();
}

void Writer::set_storage_manager(StorageManager* storage_manager) {
  storage_manager_ = storage_manager;
}

Status Writer::set_subarray(const void* subarray) {
  // Check
  if (subarray != nullptr) {
    if (!array_schema_->dense())  // Sparse arrays
      return LOG_STATUS(Status::WriterError(
          "Cannot set subarray when writing to sparse arrays"));
    else if (layout_ == Layout::UNORDERED)  // Dense arrays
      return LOG_STATUS(Status::WriterError(
          "Cannot set subarray when performing sparse writes to dense arrays "
          "(i.e., when writing in UNORDERED mode)"));
  }

  // Reset the writer (this will nuke the global write state)
  reset();

  uint64_t subarray_size = 2 * array_schema_->coords_size();
  if (subarray_ == nullptr)
    subarray_ = std::malloc(subarray_size);
  if (subarray_ == nullptr)
    return LOG_STATUS(
        Status::WriterError("Memory allocation for subarray failed"));

  if (subarray == nullptr)
    std::memcpy(subarray_, array_schema_->domain()->domain(), subarray_size);
  else
    std::memcpy(subarray_, subarray, subarray_size);

  return Status::Ok();
}

Status Writer::set_subarray(const Subarray& subarray) {
  if (!array_schema_->dense())  // Sparse arrays
    return LOG_STATUS(Status::WriterError(
        "Cannot set subarray when writing to sparse arrays"));

  // TODO
  // TODO: for the dense case, allow only single-range subarrays
  (void)subarray;

  return Status::Ok();
}

void* Writer::subarray() const {
  return subarray_;
}

Status Writer::write() {
  std::cerr << "JOE Writer::write 1" << std::endl;

  STATS_FUNC_IN(writer_write);

  // In case the user has provided a coordinates buffer
  RETURN_NOT_OK(split_coords_buffer());

  std::cerr << "JOE Writer::write 2" << std::endl;

  if (check_coord_oob_)
    RETURN_NOT_OK(check_coord_oob());

  std::cerr << "JOE Writer::write 3" << std::endl;

  if (layout_ == Layout::COL_MAJOR || layout_ == Layout::ROW_MAJOR) {
    std::cerr << "JOE Writer::write 4" << std::endl;
    RETURN_NOT_OK(ordered_write());
  } else if (layout_ == Layout::UNORDERED) {
    std::cerr << "JOE Writer::write 5" << std::endl;
    RETURN_NOT_OK(unordered_write());
  } else if (layout_ == Layout::GLOBAL_ORDER) {
    std::cerr << "JOE Writer::write 6" << std::endl;
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

void Writer::add_written_fragment_info(const URI& uri) {
  auto timestamp_range = utils::parse::get_timestamp_range(
      constants::format_version, uri.last_path_part());
  written_fragment_info_.emplace_back(uri, timestamp_range);
}

Status Writer::check_buffer_names() {
  bool has_coords = !coord_buffers_.empty() || coords_buffer_ != nullptr;

  // If the array is sparse, the coordinates must be provided
  if (!array_schema_->dense() && !has_coords)
    return LOG_STATUS(
        Status::WriterError("Sparse array writes expect the coordinates of the "
                            "cells to be written"));

  // If the layout is unordered, the coordinates must be provided
  if (layout_ == Layout::UNORDERED && !has_coords)
    return LOG_STATUS(Status::WriterError(
        "Unordered writes expect the coordinates of the cells to be written"));

  // All attributes must be provided
  if (attr_buffers_.size() != array_schema_->attribute_num())
    return LOG_STATUS(
        Status::WriterError("Writes expect all attributes to be set"));

  // If coordinates were given, they must be given for all dimensions
  if (!coord_buffers_.empty() &&
      coord_buffers_.size() != array_schema_->dim_num())
    return LOG_STATUS(Status::WriterError(
        "Writes expect coordinate buffers to be set for all dimensions"));

  return Status::Ok();
}

Status Writer::check_buffer_sizes() const {
  // This is applicable only to dense arrays and ordered layout
  if (!array_schema_->dense() ||
      (layout_ != Layout::ROW_MAJOR && layout_ != Layout::COL_MAJOR))
    return Status::Ok();

  auto cell_num = array_schema_->domain()->cell_num(subarray_);
  uint64_t expected_cell_num = 0;
  for (const auto& it : attr_buffers_) {
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

  if (coord_buffers_.empty()) {
    return LOG_STATUS(
        Status::WriterError("Cannot check for coordinate duplicates; "
                            "Coordinates buffer not found"));
  }

  if (coords_num_ < 2)
    return Status::Ok();

  // Prepare auxiliary vector for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<unsigned char*> buffs(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = (unsigned char*)coord_buffers_.find(dim_name)->second.buffer_;
  }

  auto statuses = parallel_for(1, coords_num_, [&](uint64_t i) {
    // Check for duplicate in adjacent cells
    bool found_dup = true;
    for (unsigned d = 0; d < dim_num; ++d) {
      if (memcmp(
              buffs[d] + cell_pos[i] * coord_sizes_[d],
              buffs[d] + cell_pos[i - 1] * coord_sizes_[d],
              coord_sizes_[d]) != 0) {  // Not the same
        found_dup = false;
        break;
      }
    }

    // Found duplicate
    if (found_dup) {
      return LOG_STATUS(
          Status::WriterError("Duplicate coordinates are not allowed"));
    }

    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();
  STATS_FUNC_OUT(writer_check_coord_dups);
}

Status Writer::check_coord_dups() const {
  STATS_FUNC_IN(writer_check_coord_dups_global);

  if (coord_buffers_.empty()) {
    return LOG_STATUS(
        Status::WriterError("Cannot check for coordinate duplicates; "
                            "Coordinates buffer not found"));
  }

  if (coords_num_ < 2)
    return Status::Ok();

  // Prepare auxiliary vector for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<unsigned char*> buffs(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = (unsigned char*)coord_buffers_.find(dim_name)->second.buffer_;
  }

  auto statuses = parallel_for(1, coords_num_, [&](uint64_t i) {
    // Check for duplicate in adjacent cells
    bool found_dup = true;
    for (unsigned d = 0; d < dim_num; ++d) {
      if (memcmp(
              buffs[d] + i * coord_sizes_[d],
              buffs[d] + (i - 1) * coord_sizes_[d],
              coord_sizes_[d]) != 0) {  // Not the same
        found_dup = false;
        break;
      }
    }

    // Found duplicate
    if (found_dup) {
      return LOG_STATUS(
          Status::WriterError("Duplicate coordinates are not allowed"));
    }

    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses) {
    if (!st.ok())
      return st;
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_check_coord_dups_global);
}

Status Writer::check_coord_oob() const {
  // Applicable only to sparse writes - exit if coordinates do not exist
  if (coord_buffers_.empty())
    return Status::Ok();

  // Exit if there are no coordinates to write
  if (coords_num_ == 0)
    return Status::Ok();

  // Prepare auxiliary vector for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<unsigned char*> buffs(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = (unsigned char*)coord_buffers_.find(dim_name)->second.buffer_;
  }

  // Check if all coordinates fall in the domain in parallel
  auto statuses =
      parallel_for_2d(0, coords_num_, 0, dim_num, [&](uint64_t c, unsigned d) {
        auto dim = array_schema_->dimension(d);
        std::string err_msg;
        if (dim->oob(buffs[d] + c * coord_sizes_[d], &err_msg))
          return Status::WriterError(err_msg);
        return Status::Ok();
      });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK_ELSE(st, LOG_STATUS(st));

  // Success
  return Status::Ok();
}

Status Writer::check_global_order() const {
  // Applicable only to sparse writes - exit if coordinates do not exist
  if (coord_buffers_.empty() || coords_num_ < 2)
    return Status::Ok();

  // Prepare auxiliary vector for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<const void*> buffs(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = coord_buffers_.find(dim_name)->second.buffer_;
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
      return LOG_STATUS(Status::WriterError(ss.str()));
    }
    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses) {
    if (!st.ok())
      return st;
  }

  return Status::Ok();
}

Status Writer::check_subarray() const {
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot check subarray; Array schema not set"));

  if (subarray_ == nullptr) {
    if (array_schema_->dense())
      return LOG_STATUS(Status::WriterError(
          "Cannot initialize query; Dense writes must specify a subarray"));
    else
      return Status::Ok();
  }

  switch (array_schema_->domain()->type()) {
    case Datatype::INT8:
      return check_subarray<int8_t>();
    case Datatype::UINT8:
      return check_subarray<uint8_t>();
    case Datatype::INT16:
      return check_subarray<int16_t>();
    case Datatype::UINT16:
      return check_subarray<uint16_t>();
    case Datatype::INT32:
      return check_subarray<int32_t>();
    case Datatype::UINT32:
      return check_subarray<uint32_t>();
    case Datatype::INT64:
      return check_subarray<int64_t>();
    case Datatype::UINT64:
      return check_subarray<uint64_t>();
    case Datatype::FLOAT32:
      return check_subarray<float>();
    case Datatype::FLOAT64:
      return check_subarray<double>();
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
      return check_subarray<int64_t>();
    case Datatype::CHAR:
    case Datatype::STRING_ASCII:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::ANY:
      // Not supported domain type
      assert(false);
      break;
  }

  return Status::Ok();
}

template <class T>
Status Writer::check_subarray() const {
  // Check subarray bounds
  auto domain = array_schema_->domain();
  auto dim_num = domain->dim_num();
  auto subarray = (T*)subarray_;

  // In global dense writes, the subarray must coincide with tile extents
  // Note that in the dense case, the domain type is integer
  if (array_schema_->dense() && layout() == Layout::GLOBAL_ORDER) {
    for (unsigned int i = 0; i < dim_num; ++i) {
      auto dim_domain = static_cast<const T*>(domain->dimension(i)->domain());
      auto tile_extent =
          static_cast<const T*>(domain->dimension(i)->tile_extent());
      assert(tile_extent != nullptr);
      auto norm_1 = uint64_t(subarray[2 * i] - dim_domain[0]);
      auto norm_2 = (uint64_t(subarray[2 * i + 1]) - dim_domain[0]) + 1;
      if ((norm_1 / (*tile_extent) * (*tile_extent) != norm_1) ||
          (norm_2 / (*tile_extent) * (*tile_extent) != norm_2)) {
        return LOG_STATUS(Status::WriterError(
            "Invalid subarray; In global writes for dense arrays, the subarray "
            "must coincide with the tile bounds"));
      }
    }
  }

  return Status::Ok();
}

void Writer::clear_coord_buffers() {
  // Applicable only if the coordinate buffers have been allocated by
  // TileDB
  if (coord_buffers_alloced_) {
    for (auto& buff : coord_buffers_) {
      std::free(buff.second.buffer_);
      std::free(buff.second.buffer_var_);
    }
    coord_buffer_sizes_.clear();
    coord_buffers_.clear();
    coord_buffers_alloced_ = false;
  }
}

Status Writer::close_files(FragmentMetadata* meta) const {
  // Close attribute files
  for (const auto& it : attr_buffers_) {
    const auto& attr = it.first;
    RETURN_NOT_OK(storage_manager_->close_file(meta->attr_uri(attr)));
    if (array_schema_->var_size(attr))
      RETURN_NOT_OK(storage_manager_->close_file(meta->attr_var_uri(attr)));
  }

  // Close coordinate files
  // TODO: close separate coordinate files
  if (!coord_buffers_.empty())
    RETURN_NOT_OK(
        storage_manager_->close_file(meta->attr_uri(constants::coords)));

  return Status::Ok();
}

Status Writer::compute_coord_dups(
    const std::vector<uint64_t>& cell_pos,
    std::set<uint64_t>* coord_dups) const {
  STATS_FUNC_IN(writer_compute_coord_dups);

  if (coord_buffers_.empty()) {
    return LOG_STATUS(
        Status::WriterError("Cannot check for coordinate duplicates; "
                            "Coordinates buffer not found"));
  }

  if (coords_num_ < 2)
    return Status::Ok();

  // Prepare auxiliary vector for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<unsigned char*> buffs(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = (unsigned char*)coord_buffers_.find(dim_name)->second.buffer_;
  }

  std::mutex mtx;
  auto statuses = parallel_for(1, coords_num_, [&](uint64_t i) {
    // Check for duplicate in adjacent cells
    bool found_dup = true;
    for (unsigned d = 0; d < dim_num; ++d) {
      if (memcmp(
              buffs[d] + cell_pos[i] * coord_sizes_[d],
              buffs[d] + cell_pos[i - 1] * coord_sizes_[d],
              coord_sizes_[d]) != 0) {  // Not the same
        found_dup = false;
        break;
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
  for (auto& st : statuses) {
    if (!st.ok())
      return st;
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_compute_coord_dups);
}

Status Writer::compute_coord_dups(std::set<uint64_t>* coord_dups) const {
  STATS_FUNC_IN(writer_compute_coord_dups_global);

  if (coord_buffers_.empty()) {
    return LOG_STATUS(
        Status::WriterError("Cannot check for coordinate duplicates; "
                            "Coordinates buffer not found"));
  }

  if (coords_num_ < 2)
    return Status::Ok();

  // Prepare auxiliary vector for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<unsigned char*> buffs(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = (unsigned char*)coord_buffers_.find(dim_name)->second.buffer_;
  }

  std::mutex mtx;
  auto statuses = parallel_for(1, coords_num_, [&](uint64_t i) {
    // Check for duplicate in adjacent cells
    bool found_dup = true;
    for (unsigned d = 0; d < dim_num; ++d) {
      if (memcmp(
              buffs[d] + i * coord_sizes_[d],
              buffs[d] + (i - 1) * coord_sizes_[d],
              coord_sizes_[d]) != 0) {  // Not the same
        found_dup = false;
        break;
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
  auto coords_type = array_schema_->coords_type();
  switch (coords_type) {
    case Datatype::INT8:
      return compute_coords_metadata<int8_t>(tiles, meta);
    case Datatype::UINT8:
      return compute_coords_metadata<uint8_t>(tiles, meta);
    case Datatype::INT16:
      return compute_coords_metadata<int16_t>(tiles, meta);
    case Datatype::UINT16:
      return compute_coords_metadata<uint16_t>(tiles, meta);
    case Datatype::INT32:
      return compute_coords_metadata<int32_t>(tiles, meta);
    case Datatype::UINT32:
      return compute_coords_metadata<uint32_t>(tiles, meta);
    case Datatype::INT64:
      return compute_coords_metadata<int64_t>(tiles, meta);
    case Datatype::UINT64:
      return compute_coords_metadata<uint64_t>(tiles, meta);
    case Datatype::FLOAT32:
      assert(!array_schema_->dense());
      return compute_coords_metadata<float>(tiles, meta);
    case Datatype::FLOAT64:
      assert(!array_schema_->dense());
      return compute_coords_metadata<double>(tiles, meta);
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
      return compute_coords_metadata<int64_t>(tiles, meta);
    default:
      return LOG_STATUS(Status::WriterError(
          "Cannot compute coordinates metadata; Unsupported domain type"));
  }

  return Status::Ok();
}

template <class T>
Status Writer::compute_coords_metadata(
    const std::unordered_map<std::string, std::vector<Tile>>& tiles,
    FragmentMetadata* meta) const {
  STATS_FUNC_IN(writer_compute_coords_metadata);

  // Check if tiles are empty
  if (tiles.empty() || tiles.begin()->second.empty())
    return Status::Ok();

  // For easy reference
  auto tile_num = tiles.begin()->second.size();
  auto dim_num = array_schema_->dim_num();

  // Compute MBRs
  auto statuses = parallel_for(0, tile_num, [&](uint64_t t) {
    std::vector<T> mbr(2 * dim_num);
    std::vector<T*> data(dim_num);
    for (unsigned d = 0; d < dim_num; ++d) {
      const auto& dim_name = array_schema_->dimension(d)->name();
      auto tiles_it = tiles.find(dim_name);
      data[d] = (T*)(tiles_it->second[t].internal_data());
    }

    // Initialize MBR with the first coords
    auto cell_num = tiles.begin()->second[t].cell_num();
    assert(cell_num > 0);
    for (unsigned d = 0; d < dim_num; ++d) {
      mbr[2 * d] = data[d][0];
      mbr[2 * d + 1] = data[d][0];
    }

    // Expand the MBR with the rest coords
    for (uint64_t c = 1; c < cell_num; ++c)
      utils::geometry::expand_mbr(data, c, &mbr[0]);

    meta->set_mbr(t, &mbr[0]);
    return Status::Ok();
  });

  // Set last tile cell number
  meta->set_last_tile_cell_num(tiles.begin()->second.back().cell_num());

  return Status::Ok();

  STATS_FUNC_OUT(writer_compute_coords_metadata);
}

template <class T>
Status Writer::compute_write_cell_ranges(
    WriteCellSlabIter<T>* iter, WriteCellRangeVec* write_cell_ranges) const {
  STATS_FUNC_IN(writer_compute_write_cell_ranges);

  auto domain = array_schema_->domain();
  auto dim_num = array_schema_->dim_num();
  auto subarray = (const T*)subarray_;
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

  std::cerr << "JOE create_fragment 1" << std::endl;

  URI uri;
  uint64_t timestamp = 0;
  if (!fragment_uri_.to_string().empty()) {
    uri = fragment_uri_;
  } else {
    std::string new_fragment_str;
    RETURN_NOT_OK(new_fragment_name(&new_fragment_str, &timestamp));
    uri = array_schema_->array_uri().join_path(new_fragment_str);
  }
  std::cerr << "JOE create_fragment 2" << std::endl;
  auto timestamp_range = std::pair<uint64_t, uint64_t>(timestamp, timestamp);
  std::cerr << "JOE create_fragment 3" << std::endl;
  *frag_meta = std::make_shared<FragmentMetadata>(
      storage_manager_, array_schema_, uri, timestamp_range, dense);
  std::cerr << "JOE create_fragment 3.1" << std::endl;

  RETURN_NOT_OK((*frag_meta)->init(subarray_));
  std::cerr << "JOE create_fragment 4" << std::endl;
  return storage_manager_->create_dir(uri);

  STATS_FUNC_OUT(writer_create_fragment);
}

Status Writer::filter_attr_tiles(
    std::unordered_map<std::string, std::vector<Tile>>* attr_tiles) const {
  auto attr_num = attr_buffers_.size();
  auto statuses = parallel_for(0, attr_num, [&](uint64_t i) {
    auto buff_it = attr_buffers_.begin();
    std::advance(buff_it, i);
    const auto& attr = buff_it->first;
    auto& tiles = (*attr_tiles)[attr];
    RETURN_CANCEL_OR_ERROR(filter_tiles(attr, &tiles));
    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses) {
    if (!st.ok())
      return st;
  }

  return Status::Ok();
}

Status Writer::filter_tiles(
    const std::string& attribute, std::vector<Tile>* tiles) const {
  STATS_FUNC_IN(writer_filter_tiles);

  bool var_size = array_schema_->var_size(attribute);
  // Filter all tiles
  auto tile_num = tiles->size();
  for (size_t i = 0; i < tile_num; ++i) {
    RETURN_NOT_OK(filter_tile(attribute, &(*tiles)[i], var_size));
    if (var_size) {
      ++i;
      RETURN_NOT_OK(filter_tile(attribute, &(*tiles)[i], false));
    }
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_filter_tiles);
}

Status Writer::filter_tile(
    const std::string& attribute, Tile* tile, bool offsets) const {
  auto orig_size = tile->buffer2()->size();

  // Get a copy of the appropriate filter pipeline.
  FilterPipeline filters;
  if (tile->stores_coords()) {
    filters = *array_schema_->coords_filters();
  } else if (offsets) {
    filters = *array_schema_->cell_var_offsets_filters();
  } else {
    filters = *array_schema_->filters(attribute);
  }

  // Append an encryption filter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  RETURN_NOT_OK(filters.run_forward(tile));

  tile->set_filtered(true);
  tile->set_pre_filtered_size(orig_size);
  STATS_COUNTER_ADD(writer_num_bytes_before_filtering, orig_size);

  return Status::Ok();
}

Status Writer::finalize_global_write_state() {
  assert(layout_ == Layout::GLOBAL_ORDER);
  auto meta = global_write_state_->frag_meta_.get();
  auto uri = meta->fragment_uri();

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
  uint64_t cell_num = 0;
  if (!coord_buffers_.empty()) {
    const auto& dim_name = coord_buffers_.begin()->first;
    cell_num = global_write_state_->coord_cells_written_[dim_name];
  } else if (!attr_buffers_.empty()) {
    const auto& attr = attr_buffers_.begin()->first;
    cell_num = global_write_state_->attr_cells_written_[attr];
  }

  for (const auto& it : attr_buffers_) {
    const auto& attr = it.first;
    if (global_write_state_->attr_cells_written_[attr] != cell_num) {
      clean_up(uri);
      return LOG_STATUS(Status::WriterError(
          "Failed to finalize global write state; Different "
          "number of cells written across attributes and coordinates"));
    }
  }
  for (const auto& it : coord_buffers_) {
    const auto& dim_name = it.first;
    if (global_write_state_->coord_cells_written_[dim_name] != cell_num) {
      clean_up(uri);
      return LOG_STATUS(Status::WriterError(
          "Failed to finalize global write state; Different "
          "number of cells written across attributes and coordinates"));
    }
  }

  // Check if the total number of cells written is equal to the subarray size
  if (coord_buffers_.empty()) {
    auto expected_cell_num = array_schema_->domain()->cell_num(subarray_);
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
  add_written_fragment_info(uri);

  // Delete global write state
  global_write_state_.reset(nullptr);

  return st;
}

Status Writer::global_write() {
  std::cerr << "JOE Writer::global_write 1" << std::endl;

  // Applicable only to global write on dense/sparse arrays
  assert(layout_ == Layout::GLOBAL_ORDER);

  std::cerr << "JOE Writer::global_write 1.1" << std::endl;

  // Initialize the global write state if this is the first invocation
  if (!global_write_state_) {
    std::cerr << "JOE Writer::global_write 1.2" << std::endl;
    RETURN_CANCEL_OR_ERROR(init_global_write_state());
  }
  std::cerr << "JOE Writer::global_write 1.3" << std::endl;
  auto frag_meta = global_write_state_->frag_meta_.get();
  std::cerr << "JOE Writer::global_write 1.4" << std::endl;
  auto uri = frag_meta->fragment_uri();

  std::cerr << "JOE Writer::global_write 2" << std::endl;

  // Check for coordinate duplicates
  if (!coord_buffers_.empty()) {
    if (check_coord_dups_ && !dedup_coords_)
      RETURN_CANCEL_OR_ERROR(check_coord_dups());
    if (check_global_order_)
      RETURN_CANCEL_OR_ERROR(check_global_order());
  }

  std::cerr << "JOE Writer::global_write 3" << std::endl;

  // Retrieve coordinate duplicates
  std::set<uint64_t> coord_dups;
  if (dedup_coords_)
    RETURN_CANCEL_OR_ERROR(compute_coord_dups(&coord_dups));


  std::cerr << "JOE Writer::global_write 4" << std::endl;

  std::unordered_map<std::string, std::vector<Tile>> coord_tiles;
  std::unordered_map<std::string, std::vector<Tile>> attr_tiles;
  auto statuses = parallel_for(0, 2, [&](uint64_t i) {
    if (i == 0) {
      // Prepare coordinate tiles
      RETURN_CANCEL_OR_ERROR_ELSE(
          prepare_full_coord_tiles(coord_dups, &coord_tiles), clean_up(uri));
    } else {
      // Prepare attribute tiles
      RETURN_CANCEL_OR_ERROR_ELSE(
          prepare_full_attr_tiles(coord_dups, &attr_tiles), clean_up(uri));
    }

    return Status::Ok();
  });

  std::cerr << "JOE Writer::global_write 6" << std::endl;

  // Check statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  // Find number of tiles
  uint64_t tile_num = 0;
  if (!attr_tiles.empty()) {
    auto it = attr_tiles.begin();
    tile_num = array_schema_->var_size(it->first) ? it->second.size() / 2 :
                                                    it->second.size();
  } else {
    assert(!coord_tiles.empty());
    tile_num = coord_tiles[0].size();
  }

  std::cerr << "JOE Writer::global_write 7" << std::endl;

  // No cells to be written
  if (tile_num == 0)
    return Status::Ok();

  std::cerr << "JOE Writer::global_write 8" << std::endl;

  // Set new number of tiles in the fragment metadata
  auto new_num_tiles = frag_meta->tile_index_base() + tile_num;
  frag_meta->set_num_tiles(new_num_tiles);

  std::cerr << "JOE Writer::global_write 9" << std::endl;

  std::vector<Tile> coords_tiles;
  statuses = parallel_for(0, 2, [&](uint64_t i) {
    if (i == 0) {
      // Filter coordinate tiles
      RETURN_CANCEL_OR_ERROR_ELSE(
          compute_coords_metadata(coord_tiles, frag_meta), clean_up(uri));
      // TODO: remove and filter coordinate tiles separately
      RETURN_CANCEL_OR_ERROR_ELSE(
          zip_coord_tiles(coord_tiles, &coords_tiles), clean_up(uri));
      RETURN_CANCEL_OR_ERROR_ELSE(
          filter_tiles(constants::coords, &coords_tiles), clean_up(uri));
    } else {
      // Filter attribute tiles
      RETURN_CANCEL_OR_ERROR_ELSE(
          filter_attr_tiles(&attr_tiles), clean_up(uri));
    }

    return Status::Ok();
  });

  std::cerr << "JOE Writer::global_write 10" << std::endl;

  // Check statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  std::cerr << "JOE Writer::global_write 11" << std::endl;

  // Write tiles for all attributes
  RETURN_NOT_OK_ELSE(
      write_all_tiles(frag_meta, attr_tiles, coords_tiles), clean_up(uri));

  std::cerr << "JOE Writer::global_write 12" << std::endl;

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
  std::vector<Tile> coords_tiles;
  std::unordered_map<std::string, std::vector<Tile>> attr_tiles;
  auto statuses = parallel_for(0, 2, [&](uint64_t i) {
    if (i == 0) {
      // Filter last coordinate tiles
      RETURN_NOT_OK_ELSE(filter_last_coord_tiles(&coords_tiles), clean_up(uri));
    } else {
      // Filter last attribute tiles
      RETURN_NOT_OK_ELSE(filter_last_attr_tiles(&attr_tiles), clean_up(uri));
    }

    return Status::Ok();
  });

  // Check statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  // Write the last tiles
  RETURN_NOT_OK(write_all_tiles(meta, attr_tiles, coords_tiles));

  // Increment the tile index base.
  meta->set_tile_index_base(meta->tile_index_base() + 1);

  return Status::Ok();
}

Status Writer::filter_last_attr_tiles(
    std::unordered_map<std::string, std::vector<Tile>>* attr_tiles) const {
  // Initialize attribute tiles
  for (auto it : attr_buffers_)
    (*attr_tiles)[it.first] = std::vector<Tile>();

  uint64_t attr_num = attr_buffers_.size();
  auto statuses = parallel_for(0, attr_num, [&](uint64_t i) {
    auto buff_it = attr_buffers_.begin();
    std::advance(buff_it, i);
    const auto& attr = buff_it->first;
    auto& last_tile = global_write_state_->last_attr_tiles_[attr].first;
    auto& last_tile_var = global_write_state_->last_attr_tiles_[attr].second;

    if (!last_tile.empty()) {
      std::vector<Tile>& tiles = (*attr_tiles)[attr];
      // Note making shallow clones here, as it's not necessary to copy the
      // underlying tile Buffers.
      tiles.push_back(last_tile.clone(false));
      if (!last_tile_var.empty())
        tiles.push_back(last_tile_var.clone(false));
      RETURN_NOT_OK(filter_tiles(attr, &tiles));
    }
    return Status::Ok();
  });

  // Check statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();
}

Status Writer::filter_last_coord_tiles(std::vector<Tile>* coords_tiles) const {
  // Prepare coord tiles map
  std::unordered_map<std::string, std::vector<Tile>> coord_tiles;
  auto dim_num = array_schema_->dim_num();
  auto meta = global_write_state_->frag_meta_.get();
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    coord_tiles[dim_name] = std::vector<Tile>();
  }

  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    auto& last_tile = global_write_state_->last_coord_tiles_[dim_name].first;
    auto& last_tile_var =
        global_write_state_->last_coord_tiles_[dim_name].second;

    if (!last_tile.empty()) {
      auto& tiles = coord_tiles[dim_name];
      // Note making shallow clones here, as it's not necessary to copy the
      // underlying tile Buffers.
      tiles.push_back(last_tile.clone(false));
      if (!last_tile_var.empty())
        tiles.push_back(last_tile_var.clone(false));
    }
  }

  RETURN_NOT_OK(compute_coords_metadata(coord_tiles, meta));
  // TODO: remove
  RETURN_NOT_OK(zip_coord_tiles(coord_tiles, coords_tiles));
  RETURN_NOT_OK(filter_tiles(constants::coords, coords_tiles));

  return Status::Ok();
}

bool Writer::all_last_tiles_empty() const {
  // See if any last coordinate tiles are nonempty
  auto dim_num = array_schema_->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    auto& last_tile = global_write_state_->last_coord_tiles_[dim_name].first;
    if (!last_tile.empty())
      return false;
  }

  // See if any last coordinate tiles are nonempty
  for (const auto& it : attr_buffers_) {
    const auto& attr = it.first;
    auto& last_tile = global_write_state_->last_attr_tiles_[attr].first;
    if (!last_tile.empty())
      return false;
  }

  return true;
}

Status Writer::init_global_write_state() {
  STATS_FUNC_IN(writer_init_global_write_state);

  std::cerr << "JOE Writer::init_global_write_state 1" << std::endl;

  // Create global array state object
  if (global_write_state_ != nullptr)
    return LOG_STATUS(Status::WriterError(
        "Cannot initialize global write state; State not properly finalized"));
  global_write_state_.reset(new GlobalWriteState);

  std::cerr << "JOE Writer::init_global_write_state 2" << std::endl;

  bool has_coords = !coord_buffers_.empty();

  std::cerr << "JOE Writer::init_global_write_state 3" << std::endl;

  // Create fragment
  RETURN_NOT_OK(
      create_fragment(!has_coords, &(global_write_state_->frag_meta_)));
  std::cerr << "JOE Writer::init_global_write_state 3.1" << std::endl;
  auto uri = global_write_state_->frag_meta_->fragment_uri();

  std::cerr << "JOE Writer::init_global_write_state 4" << std::endl;

  // Initialize global write state for coordinates
  if (has_coords) {
    auto dim_num = array_schema_->dim_num();
    for (unsigned d = 0; d < dim_num; ++d) {
      const auto& dim_name = array_schema_->dimension(d)->name();

      // Initialize last tiles
      auto last_tile_pair = std::pair<std::string, std::pair<Tile, Tile>>(
          dim_name, std::pair<Tile, Tile>(Tile(), Tile()));
      auto it_ret =
          global_write_state_->last_coord_tiles_.emplace(last_tile_pair);

      auto& last_tile = it_ret.first->second.first;
      RETURN_NOT_OK_ELSE(init_coord_tile(d, &last_tile), clean_up(uri));

      // Initialize cells written
      global_write_state_->coord_cells_written_[dim_name] = 0;
    }
  }

  std::cerr << "JOE Writer::init_global_write_state 5" << std::endl;

  // Initialize global write state for attributes
  for (const auto& it : attr_buffers_) {
    std::cerr << "JOE Writer::init_global_write_state 5.1" << std::endl;
    // Initialize last tiles
    const auto& attr = it.first;
    std::cerr << "JOE Writer::init_global_write_state 5.2" << std::endl;
    auto last_tile_pair = std::pair<std::string, std::pair<Tile, Tile>>(
        attr, std::pair<Tile, Tile>(Tile(), Tile()));
    std::cerr << "JOE Writer::init_global_write_state 5.2" << std::endl;
    auto it_ret = global_write_state_->last_attr_tiles_.emplace(last_tile_pair);
    std::cerr << "JOE Writer::init_global_write_state 5.3" << std::endl;

    if (!array_schema_->var_size(attr)) {
      std::cerr << "JOE Writer::init_global_write_state 5.4" << std::endl;
      auto& last_tile = it_ret.first->second.first;
      std::cerr << "JOE Writer::init_global_write_state 5.5" << std::endl;
      RETURN_NOT_OK_ELSE(init_tile(attr, &last_tile), clean_up(uri));
      std::cerr << "JOE Writer::init_global_write_state 5.6" << std::endl;
    } else {
      std::cerr << "JOE Writer::init_global_write_state 5.7" << std::endl;
      auto& last_tile = it_ret.first->second.first;
      auto& last_tile_var = it_ret.first->second.second;
      std::cerr << "JOE Writer::init_global_write_state 5.8" << std::endl;
      RETURN_NOT_OK_ELSE(
          init_tile(attr, &last_tile, &last_tile_var), clean_up(uri));
      std::cerr << "JOE Writer::init_global_write_state 5.9" << std::endl;
    }

    // Initialize cells written
    global_write_state_->attr_cells_written_[attr] = 0;
  }

  std::cerr << "JOE Writer::init_global_write_state 6" << std::endl;

  return Status::Ok();

  STATS_FUNC_OUT(writer_init_global_write_state);
}

Status Writer::init_tile(const std::string& attribute, Tile* tile) const {
  // For easy reference
  auto has_coords = !coord_buffers_.empty();
  auto domain = array_schema_->domain();
  auto cell_size = array_schema_->cell_size(attribute);
  auto capacity = array_schema_->capacity();
  auto type = array_schema_->type(attribute);
  auto cell_num_per_tile = has_coords ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * cell_size;

  // Initialize
  RETURN_NOT_OK(
      tile->init(constants::format_version, type, tile_size, cell_size, 0));

  return Status::Ok();
}

Status Writer::init_tile(
    const std::string& attribute, Tile* tile, Tile* tile_var) const {
  // For easy reference
  auto has_coords = !coord_buffers_.empty();
  auto domain = array_schema_->domain();
  auto capacity = array_schema_->capacity();
  auto type = array_schema_->type(attribute);
  auto cell_num_per_tile = has_coords ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * constants::cell_var_offset_size;

  // Initialize
  RETURN_NOT_OK(tile->init(
      constants::format_version,
      constants::cell_var_offset_type,
      tile_size,
      constants::cell_var_offset_size,
      0));
  RETURN_NOT_OK(tile_var->init(
      constants::format_version, type, tile_size, datatype_size(type), 0));
  return Status::Ok();
}

Status Writer::init_coord_tile(unsigned dim_idx, Tile* tile) const {
  // For easy reference
  auto dim = array_schema_->dimension(dim_idx);
  auto coord_size = dim->coord_size();
  auto capacity = array_schema_->capacity();
  auto type = dim->type();
  auto tile_size = capacity * coord_size;

  // Initialize
  RETURN_NOT_OK(
      tile->init(constants::format_version, type, tile_size, coord_size, 0));

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
  for (unsigned i = 0; i < 2 * dim_num; ++i)
    subarray[i] = ((T*)subarray_)[i];
  auto cell_order = domain->cell_order();

  // Compute tile domain and current tile coords
  std::vector<T> tile_domain, tile_coords;
  tile_domain.resize(2 * dim_num);
  tile_coords.resize(dim_num);
  domain->get_tile_domain(&subarray[0], &tile_domain[0]);
  for (unsigned i = 0; i < dim_num; ++i)
    tile_coords[i] = tile_domain[2 * i];
  auto tile_num = domain->tile_num<T>(&subarray[0]);

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
    const std::string& attribute,
    uint64_t tile_num,
    std::vector<Tile>* tiles) const {
  // Initialize tiles
  bool var_size = array_schema_->var_size(attribute);
  auto tiles_len = (var_size) ? 2 * tile_num : tile_num;
  tiles->resize(tiles_len);
  for (size_t i = 0; i < tiles_len; i += (1 + var_size)) {
    if (!var_size) {
      RETURN_NOT_OK(init_tile(attribute, &((*tiles)[i])));
    } else {
      RETURN_NOT_OK(init_tile(attribute, &((*tiles)[i]), &((*tiles)[i + 1])));
    }
  }

  return Status::Ok();
}

Status Writer::new_fragment_name(
    std::string* frag_uri, uint64_t* timestamp) const {
  if (frag_uri == nullptr)
    return Status::WriterError("Null fragment uri argument.");
  *timestamp = utils::time::timestamp_now_ms();
  std::string uuid;
  frag_uri->clear();
  RETURN_NOT_OK(uuid::generate_uuid(&uuid, false));
  std::stringstream ss;
  ss << "/__" << *timestamp << "_" << *timestamp << "_" << uuid;

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

  auto coords_type = array_schema_->coords_type();
  switch (coords_type) {
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
  auto uri = frag_meta->fragment_uri();

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
  std::vector<Tile> coords_tiles;  // Will be ignored
  RETURN_NOT_OK_ELSE(
      write_all_tiles(frag_meta.get(), attr_tiles, coords_tiles),
      storage_manager_->vfs()->remove_dir(uri));

  // Write the fragment metadata
  RETURN_CANCEL_OR_ERROR_ELSE(
      frag_meta->store(array_->get_encryption_key()), clean_up(uri));

  // Add written fragment info
  add_written_fragment_info(frag_meta->fragment_uri());

  return Status::Ok();
}

Status Writer::prepare_and_filter_attr_tiles(
    const std::vector<WriteCellRangeVec>& write_cell_ranges,
    std::unordered_map<std::string, std::vector<Tile>>* attr_tiles) const {
  // Initialize attribute tiles
  for (const auto& it : attr_buffers_)
    (*attr_tiles)[it.first] = std::vector<Tile>();

  uint64_t attr_num = attr_buffers_.size();
  auto statuses = parallel_for(0, attr_num, [&](uint64_t i) {
    auto buff_it = attr_buffers_.begin();
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

Status Writer::prepare_full_attr_tiles(
    const std::set<uint64_t>& coord_dups,
    std::unordered_map<std::string, std::vector<Tile>>* attr_tiles) const {
  // Initialize attribute tiles
  for (const auto& it : attr_buffers_)
    (*attr_tiles)[it.first] = std::vector<Tile>();

  auto attr_num = attr_buffers_.size();
  auto statuses = parallel_for(0, attr_num, [&](uint64_t i) {
    auto buff_it = attr_buffers_.begin();
    std::advance(buff_it, i);
    const auto& attr = buff_it->first;
    auto& full_tiles = (*attr_tiles)[attr];
    RETURN_CANCEL_OR_ERROR(prepare_full_tiles(attr, coord_dups, &full_tiles));
    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();
}

Status Writer::prepare_full_tiles(
    const std::string& attribute,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  return array_schema_->var_size(attribute) ?
             prepare_full_tiles_var(attribute, coord_dups, tiles) :
             prepare_full_tiles_fixed(attribute, coord_dups, tiles);
}

Status Writer::prepare_full_tiles_fixed(
    const std::string& attribute,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  STATS_FUNC_IN(writer_prepare_full_tiles_fixed);

  // For easy reference
  auto has_coords = !coord_buffers_.empty();
  auto it = attr_buffers_.find(attribute);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_size = it->second.buffer_size_;
  auto capacity = array_schema_->capacity();
  auto cell_size = array_schema_->cell_size(attribute);
  auto cell_num = *buffer_size / cell_size;
  auto domain = array_schema_->domain();
  auto cell_num_per_tile = has_coords ? capacity : domain->cell_num_per_tile();

  // Do nothing if there are no cells to write
  if (cell_num == 0)
    return Status::Ok();

  // First fill the last tile
  auto& last_tile = global_write_state_->last_attr_tiles_[attribute].first;
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
        if (coord_dups.find(cell_idx) == coord_dups.end())
          RETURN_NOT_OK(
              last_tile.write(buffer + cell_idx * cell_size, cell_size));
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
      RETURN_NOT_OK(init_tile(attribute, &tile));

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

  global_write_state_->attr_cells_written_[attribute] += cell_num;

  return Status::Ok();

  STATS_FUNC_OUT(writer_prepare_full_tiles_fixed);
}

Status Writer::prepare_full_tiles_var(
    const std::string& attribute,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  STATS_FUNC_IN(writer_prepare_full_tiles_var);

  // For easy reference
  auto has_coords = !coord_buffers_.empty();
  auto it = attr_buffers_.find(attribute);
  auto buffer = (uint64_t*)it->second.buffer_;
  auto buffer_var = (unsigned char*)it->second.buffer_var_;
  auto buffer_size = it->second.buffer_size_;
  auto buffer_var_size = it->second.buffer_var_size_;
  auto capacity = array_schema_->capacity();
  auto cell_num = *buffer_size / constants::cell_var_offset_size;
  auto domain = array_schema_->domain();
  auto cell_num_per_tile = has_coords ? capacity : domain->cell_num_per_tile();
  uint64_t offset, var_size;

  // Do nothing if there are no cells to write
  if (cell_num == 0)
    return Status::Ok();

  // First fill the last tile
  auto& last_tile_pair = global_write_state_->last_attr_tiles_[attribute];
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
      RETURN_NOT_OK(init_tile(attribute, &((*tiles)[i]), &((*tiles)[i + 1])));

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

  global_write_state_->attr_cells_written_[attribute] += cell_num;

  return Status::Ok();

  STATS_FUNC_OUT(writer_prepare_full_tiles_var);
}

Status Writer::prepare_full_coord_tiles(
    const std::set<uint64_t>& coord_dups,
    std::unordered_map<std::string, std::vector<Tile>>* tiles) const {
  // If there are no coordinates, exit
  if (coord_buffers_.empty())
    return Status::Ok();

  // Prepare tiles map
  auto dim_num = array_schema_->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    (*tiles)[dim_name] = std::vector<Tile>();
  }

  // Prepare full coordinate tiles
  auto statuses = parallel_for(0, dim_num, [&](uint64_t d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    auto& coord_tiles = (*tiles)[dim_name];
    return prepare_full_coord_tiles_fixed(d, coord_dups, &coord_tiles);
  });

  // Check all statuses
  for (auto& st : statuses) {
    if (!st.ok())
      return st;
  }

  return Status::Ok();
}

Status Writer::prepare_full_coord_tiles_fixed(
    unsigned dim_idx,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  // For easy reference
  auto dim = array_schema_->dimension(dim_idx);
  const auto& dim_name = dim->name();
  auto it = coord_buffers_.find(dim_name);
  assert(it != coord_buffers_.end());
  auto buffer = (unsigned char*)(it->second.buffer_);
  auto capacity = array_schema_->capacity();
  auto coord_size = dim->coord_size();

  // Do nothing if there are no cells to write
  if (coords_num_ == 0)
    return Status::Ok();

  // First fill the last tile
  auto& last_tile = global_write_state_->last_coord_tiles_[dim_name].first;
  uint64_t cell_idx = 0;
  if (!last_tile.empty()) {
    if (coord_dups.empty()) {
      do {
        RETURN_NOT_OK(
            last_tile.write(buffer + cell_idx * coord_size, coord_size));
        ++cell_idx;
      } while (!last_tile.full() && cell_idx != coords_num_);
    } else {
      do {
        if (coord_dups.find(cell_idx) == coord_dups.end())
          RETURN_NOT_OK(
              last_tile.write(buffer + cell_idx * coord_size, coord_size));
        ++cell_idx;
      } while (!last_tile.full() && cell_idx != coords_num_);
    }
  }

  // Initialize full tiles and set previous last tile as first tile
  auto full_tile_num =
      (coords_num_ - cell_idx) / capacity + (int)last_tile.full();
  auto cell_num_to_write = (full_tile_num - last_tile.full()) * capacity;

  if (full_tile_num > 0) {
    tiles->resize(full_tile_num);
    for (auto& tile : (*tiles))
      RETURN_NOT_OK(init_coord_tile(dim_idx, &tile));

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
            buffer + cell_idx * coord_size, coord_size * capacity));
        cell_idx += capacity;
        i += capacity;
      }
    } else {
      for (uint64_t tile_idx = 0, i = 0; i < cell_num_to_write;
           ++cell_idx, ++i) {
        if (coord_dups.find(cell_idx) == coord_dups.end()) {
          if ((*tiles)[tile_idx].full())
            ++tile_idx;

          RETURN_NOT_OK((*tiles)[tile_idx].write(
              buffer + cell_idx * coord_size, coord_size));
        }
      }
    }
  }

  // Potentially fill the last tile
  assert(coords_num_ - cell_idx < capacity - last_tile.cell_num());
  if (coord_dups.empty()) {
    for (; cell_idx < coords_num_; ++cell_idx) {
      RETURN_NOT_OK(
          last_tile.write(buffer + cell_idx * coord_size, coord_size));
    }
  } else {
    for (; cell_idx < coords_num_; ++cell_idx) {
      if (coord_dups.find(cell_idx) == coord_dups.end())
        RETURN_NOT_OK(
            last_tile.write(buffer + cell_idx * coord_size, coord_size));
    }
  }

  global_write_state_->coord_cells_written_[dim_name] += coords_num_;

  return Status::Ok();
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
  auto it = attr_buffers_.find(attribute);
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
          write_empty_cell_range_to_tile_var(
              wcr.pos_ - pos, &(*tiles)[t], &(*tiles)[t + 1]);
        else
          write_empty_cell_range_to_tile(
              (wcr.pos_ - pos) * cell_val_num, &(*tiles)[t]);
        pos = wcr.pos_;
      }

      // Write (non-empty) range
      if (var_size)
        write_cell_range_to_tile_var(
            buff.get(),
            buff_var.get(),
            wcr.start_,
            wcr.end_,
            &(*tiles)[t],
            &(*tiles)[t + 1]);
      else
        write_cell_range_to_tile(
            buff.get(), wcr.start_, wcr.end_, &(*tiles)[t]);
      pos += wcr.end_ - wcr.start_ + 1;
    }

    // Write empty range
    if (pos <= end_pos) {
      if (var_size)
        write_empty_cell_range_to_tile_var(
            end_pos - pos + 1, &(*tiles)[t], &(*tiles)[t + 1]);
      else
        write_empty_cell_range_to_tile(
            (end_pos - pos + 1) * cell_val_num, &(*tiles)[t]);
    }
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_prepare_tiles_ordered);
}

Status Writer::prepare_tiles(
    const std::string& attribute,
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  return array_schema_->var_size(attribute) ?
             prepare_tiles_var(attribute, cell_pos, coord_dups, tiles) :
             prepare_tiles_fixed(attribute, cell_pos, coord_dups, tiles);
}

Status Writer::prepare_tiles_fixed(
    const std::string& attribute,
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  STATS_FUNC_IN(writer_prepare_tiles_fixed);

  // Trivial case
  if (cell_pos.empty())
    return Status::Ok();

  // For easy reference
  auto it = attr_buffers_.find(attribute);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto cell_num = (uint64_t)cell_pos.size();
  auto capacity = array_schema_->capacity();
  auto dups_num = coord_dups.size();
  auto tile_num = utils::math::ceil(cell_num - dups_num, capacity);
  auto cell_size = array_schema_->cell_size(attribute);

  // Initialize tiles
  tiles->resize(tile_num);
  for (auto& tile : (*tiles))
    RETURN_NOT_OK(init_tile(attribute, &tile));

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

Status Writer::prepare_attr_tiles(
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    std::unordered_map<std::string, std::vector<Tile>>* attr_tiles) const {
  // Initialize attribute tiles
  attr_tiles->clear();
  for (const auto& it : attr_buffers_)
    (*attr_tiles)[it.first] = std::vector<Tile>();

  // Prepare tiles for all attributes
  auto attr_num = attr_buffers_.size();
  auto statuses = parallel_for(0, attr_num, [&](uint64_t i) {
    auto buff_it = attr_buffers_.begin();
    std::advance(buff_it, i);
    const auto& attr = buff_it->first;
    auto& tiles = (*attr_tiles)[attr];
    RETURN_CANCEL_OR_ERROR(prepare_tiles(attr, cell_pos, coord_dups, &tiles));
    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();
}

Status Writer::prepare_coord_tiles(
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    std::unordered_map<std::string, std::vector<Tile>>* tiles) const {
  // If coord buffers are empty, there is nothing to do
  if (coord_buffers_.empty())
    return Status::Ok();

  // Prepare coordinate tiles map
  auto dim_num = array_schema_->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    (*tiles)[dim_name] = std::vector<Tile>();
  }

  // Prepare coordinate tiles in parallel
  auto statuses = parallel_for(0, dim_num, [&](uint64_t d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    auto& coord_tiles = (*tiles)[dim_name];
    return prepare_coord_tiles_fixed(d, cell_pos, coord_dups, &coord_tiles);
  });

  // Check all statuses
  for (auto& st : statuses) {
    if (!st.ok())
      return st;
  }

  return Status::Ok();
}

Status Writer::prepare_coord_tiles_fixed(
    unsigned dim_idx,
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  STATS_FUNC_IN(writer_prepare_tiles_fixed);

  // Trivial case
  if (cell_pos.empty())
    return Status::Ok();

  // For easy reference
  const auto& dim_name = array_schema_->dimension(dim_idx)->name();
  auto buffer = (unsigned char*)coord_buffers_.find(dim_name)->second.buffer_;
  auto capacity = array_schema_->capacity();
  auto dups_num = coord_dups.size();
  auto tile_num = utils::math::ceil(coords_num_ - dups_num, capacity);
  auto coord_size = array_schema_->dimension(dim_idx)->coord_size();

  // Initialize tiles
  tiles->resize(tile_num);
  for (auto& tile : (*tiles))
    RETURN_NOT_OK(init_coord_tile(dim_idx, &tile));

  // Write all cells one by one
  if (dups_num == 0) {
    for (uint64_t i = 0, tile_idx = 0; i < coords_num_; ++i) {
      if ((*tiles)[tile_idx].full())
        ++tile_idx;

      RETURN_NOT_OK((*tiles)[tile_idx].write(
          buffer + cell_pos[i] * coord_size, coord_size));
    }
  } else {
    for (uint64_t i = 0, tile_idx = 0; i < coords_num_; ++i) {
      if (coord_dups.find(cell_pos[i]) != coord_dups.end())
        continue;

      if ((*tiles)[tile_idx].full())
        ++tile_idx;

      RETURN_NOT_OK((*tiles)[tile_idx].write(
          buffer + cell_pos[i] * coord_size, coord_size));
    }
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_prepare_tiles_fixed);
}

Status Writer::prepare_tiles_var(
    const std::string& attribute,
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  STATS_FUNC_IN(writer_prepare_tiles_var);

  // For easy reference
  auto it = attr_buffers_.find(attribute);
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
    RETURN_NOT_OK(init_tile(attribute, &((*tiles)[i]), &((*tiles)[i + 1])));

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
  std::vector<const void*> buffs(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = (const void*)coord_buffers_.find(dim_name)->second.buffer_;
  }

  // Populate cell_pos
  cell_pos->resize(coords_num_);
  for (uint64_t i = 0; i < coords_num_; ++i)
    (*cell_pos)[i] = i;

  // Sort the coordinates in global order
  parallel_sort(cell_pos->begin(), cell_pos->end(), GlobalCmp2(domain, buffs));

  return Status::Ok();

  STATS_FUNC_OUT(writer_sort_coords);
}

Status Writer::split_coords_buffer() {
  // Do nothing if the coordinates buffer is not set
  if (coords_buffer_ == nullptr)
    return Status::Ok();

  // For easy reference
  auto coords_size = array_schema_->coords_size();
  coords_num_ = *coords_buffer_size_ / coords_size;
  auto dim_num = array_schema_->dim_num();

  clear_coord_buffers();

  coord_buffers_alloced_ = true;

  // New coord buffer allocations
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = array_schema_->dimension(d);
    const auto& dim_name = dim->name();
    auto coord_buffer_size = coords_num_ * dim->coord_size();
    auto it = coord_buffer_sizes_.emplace(dim_name, coord_buffer_size);
    QueryBuffer buff;
    buff.buffer_size_ = &(it.first->second);
    buff.buffer_ = std::malloc(coord_buffer_size);
    if (buff.buffer_ == nullptr)
      RETURN_NOT_OK(Status::WriterError(
          "Cannot split coordinate buffers; memory allocation failed"));
    coord_buffers_.emplace(dim_name, buff);
  }

  // Split coordinates
  auto coord = (unsigned char*)nullptr;
  for (unsigned d = 0; d < dim_num; ++d) {
    auto coord_size = array_schema_->dimension(d)->coord_size();
    const auto& dim_name = array_schema_->dimension(d)->name();
    auto buff = (unsigned char*)(coord_buffers_[dim_name].buffer_);
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
  if (check_coord_dups_ && !dedup_coords_)
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
  std::unordered_map<std::string, std::vector<Tile>> coord_tiles;
  std::unordered_map<std::string, std::vector<Tile>> attr_tiles;
  auto statuses = parallel_for(0, 2, [&](uint64_t i) {
    if (i == 0) {
      // Prepare coordinate tiles
      RETURN_CANCEL_OR_ERROR_ELSE(
          prepare_coord_tiles(cell_pos, coord_dups, &coord_tiles),
          clean_up(uri));
    } else {
      // Prepare attribute tiles
      RETURN_CANCEL_OR_ERROR_ELSE(
          prepare_attr_tiles(cell_pos, coord_dups, &attr_tiles), clean_up(uri));
    }

    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  // Clear the boolean vector for coordinate duplicates
  coord_dups.clear();

  // No tiles
  if (coord_tiles.empty() || coord_tiles.begin()->second.empty())
    return Status::Ok();

  // Set the number of tiles in the metadata
  frag_meta->set_num_tiles(coord_tiles.begin()->second.size());

  // Filter tiles
  std::vector<Tile> coords_tiles;
  statuses = parallel_for(0, 2, [&](uint64_t i) {
    if (i == 0) {
      // Filter coordinate tiles
      RETURN_CANCEL_OR_ERROR_ELSE(
          compute_coords_metadata(coord_tiles, frag_meta.get()), clean_up(uri));
      // TODO: remove zipping and filter coordinate tiles separately
      RETURN_NOT_OK_ELSE(
          zip_coord_tiles(coord_tiles, &coords_tiles), clean_up(uri));
      RETURN_CANCEL_OR_ERROR_ELSE(
          filter_tiles(constants::coords, &coords_tiles), clean_up(uri));
    } else {
      // Filter attribute tiles
      RETURN_CANCEL_OR_ERROR_ELSE(
          filter_attr_tiles(&attr_tiles), clean_up(uri));
    }

    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  // Write tiles for all attributes and coordinates
  RETURN_NOT_OK_ELSE(
      write_all_tiles(frag_meta.get(), attr_tiles, coords_tiles),
      clean_up(uri));

  // Write the fragment metadata
  RETURN_CANCEL_OR_ERROR_ELSE(
      frag_meta->store(array_->get_encryption_key()), clean_up(uri));

  // Add written fragment info
  add_written_fragment_info(frag_meta->fragment_uri());

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
    const std::unordered_map<std::string, std::vector<Tile>>& attr_tiles,
    const std::vector<Tile>& coords_tiles) const {
  STATS_FUNC_IN(writer_write_all_tiles);

  assert(!attr_tiles.empty() || !coords_tiles.empty());

  std::vector<std::future<Status>> tasks;

  // Attribute tiles
  for (const auto& it : attr_buffers_) {
    const auto& attr = it.first;
    auto& tiles = attr_tiles.find(attr)->second;
    tasks.push_back(
        storage_manager_->writer_thread_pool()->enqueue([&, this]() {
          RETURN_CANCEL_OR_ERROR(write_tiles(attr, frag_meta, tiles));
          return Status::Ok();
        }));
  }

  // Coordinate tiles
  if (!coord_buffers_.empty()) {
    tasks.push_back(
        storage_manager_->writer_thread_pool()->enqueue([&, this]() {
          RETURN_CANCEL_OR_ERROR(
              write_tiles(constants::coords, frag_meta, coords_tiles));
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
    const std::string& attribute,
    FragmentMetadata* frag_meta,
    const std::vector<Tile>& tiles) const {
  // Handle zero tiles
  if (tiles.empty())
    return Status::Ok();

  // For easy reference
  bool var_size = array_schema_->var_size(attribute);
  const auto& attr_uri = frag_meta->attr_uri(attribute);
  const auto& attr_var_uri =
      var_size ? frag_meta->attr_var_uri(attribute) : URI("");

  // Write tiles
  auto tile_num = tiles.size();
  for (size_t i = 0, tile_id = 0; i < tile_num; ++i, ++tile_id) {
    RETURN_NOT_OK(storage_manager_->write(attr_uri, tiles[i].buffer2()));
    frag_meta->set_tile_offset(attribute, tile_id, tiles[i].buffer2()->size());

    STATS_COUNTER_ADD(writer_num_bytes_written, tiles[i].buffer2()->size());

    if (var_size) {
      ++i;

      RETURN_NOT_OK(storage_manager_->write(attr_var_uri, tiles[i].buffer2()));
      frag_meta->set_tile_var_offset(
          attribute, tile_id, tiles[i].buffer2()->size());
      frag_meta->set_tile_var_size(
          attribute, tile_id, tiles[i].pre_filtered_size());

      STATS_COUNTER_ADD(writer_num_bytes_written, tiles[i].buffer2()->size());
    }
  }

  // Close files, except in the case of global order
  if (layout_ != Layout::GLOBAL_ORDER) {
    RETURN_NOT_OK(storage_manager_->close_file(frag_meta->attr_uri(attribute)));
    if (var_size)
      RETURN_NOT_OK(
          storage_manager_->close_file(frag_meta->attr_var_uri(attribute)));
  }

  STATS_COUNTER_ADD(writer_num_attr_tiles_written, tile_num);

  return Status::Ok();
}

// TODO: remove
Status Writer::zip_coord_tiles(
    const std::unordered_map<std::string, std::vector<Tile>>& coord_tiles,
    std::vector<Tile>* coords_tiles) const {
  if (coord_tiles.empty() || coord_tiles.begin()->second.empty())
    return Status::Ok();

  auto tile_num = coord_tiles.begin()->second.size();
  coords_tiles->clear();
  coords_tiles->resize(tile_num);
  auto type = array_schema()->domain()->type();
  unsigned dim_num = array_schema()->dim_num();
  uint64_t coords_size = array_schema()->coords_size();
  for (size_t t = 0; t < tile_num; ++t) {
    auto& new_tile = (*coords_tiles)[t];
    auto cell_num = coord_tiles.begin()->second[t].cell_num();
    RETURN_NOT_OK(new_tile.init(
        constants::format_version,
        type,
        cell_num * coords_size,
        coords_size,
        dim_num));
    for (unsigned d = 0; d < dim_num; ++d) {
      const auto& dim_name = array_schema_->dimension(d)->name();
      const auto& coord_tile = coord_tiles.find(dim_name)->second[t];
      new_tile.write(coord_tile);
    }
  }

  return Status::Ok();
}

std::string Writer::coords_to_str(uint64_t i) const {
  std::stringstream ss;
  auto dim_num = array_schema_->dim_num();

  ss << "(";
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = array_schema_->dimension(d);
    const auto& dim_name = dim->name();
    auto buff = (unsigned char*)coord_buffers_.find(dim_name)->second.buffer_;
    auto coord = buff + i * coord_sizes_[d];
    ss << dim->coord_to_str(coord);
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
  bool has_coords = coords_buffer_ != nullptr || !coord_buffers_.empty();
  if (initialized_ && has_coords)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set coordinates after initialization")));

  if (!coord_buffers_.empty())
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set zipped coordinates buffer after having set "
                    "separate coordinate buffers")));

  // Set zipped coordinates buffer
  coords_buffer_ = buffer;
  coords_buffer_size_ = buffer_size;

  return Status::Ok();
}

Status Writer::set_coord_buffer(
    const std::string& name, void* buffer, uint64_t* buffer_size) {
  // Check that dimension is fixed-sized
  auto dim = array_schema_->dimension(name);
  if (dim->var_size())
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer; Input dimension '") + name +
        "' is var-sized"));

  // Check if zipped coordinates buffer is set
  if (coords_buffer_ != nullptr)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set separate coordinates buffer after having "
                    "set the zipped coordinates buffer")));

  // Check number of coordinates
  uint64_t coords_num = *buffer_size / dim->coord_size();
  if (!coord_buffers_.empty() && coords_num != coords_num_)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer; Input buffer for dimension '") + name +
        "' has a different number of coordinates than previously "
        "set coordinate buffers"));

  // Error if setting a new dimension after initialization
  bool dim_exists = coord_buffers_.find(name) != coord_buffers_.end();
  if (initialized_ && !dim_exists)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer for new dimension '") + name +
        "' after initialization"));

  // Set coordinate buffer
  coord_buffers_[name] = QueryBuffer(buffer, nullptr, buffer_size, nullptr);
  coords_num_ = coords_num;

  return Status::Ok();
}

Status Writer::set_attr_buffer(
    const std::string& name, void* buffer, uint64_t* buffer_size) {
  // Check that attribute is fixed-sized
  bool var_size = (array_schema_->var_size(name));
  if (var_size)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer; Input attribute '") + name +
        "' is var-sized"));

  // Error if setting a new attribute after initialization
  bool attr_exists = attr_buffers_.find(name) != attr_buffers_.end();
  if (initialized_ && !attr_exists)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer for new attribute '") + name +
        "' after initialization"));

  // Set attribute buffer
  attr_buffers_[name] = QueryBuffer(buffer, nullptr, buffer_size, nullptr);

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
