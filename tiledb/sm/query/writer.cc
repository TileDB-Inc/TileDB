/**
 * @file   writer.cc
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
  global_write_state_.reset(nullptr);
  initialized_ = false;
  layout_ = Layout::ROW_MAJOR;
  storage_manager_ = nullptr;
  subarray_ = nullptr;
}

Writer::~Writer() {
  std::free(subarray_);
}

/* ****************************** */
/*               API              */
/* ****************************** */

const ArraySchema* Writer::array_schema() const {
  return array_schema_;
}

std::vector<std::string> Writer::attributes() const {
  return attributes_;
}

AttributeBuffer Writer::buffer(const std::string& attribute) const {
  auto attrbuf = attr_buffers_.find(attribute);
  if (attrbuf == attr_buffers_.end())
    return AttributeBuffer{};
  return attrbuf->second;
}

Status Writer::finalize() {
  if (global_write_state_ != nullptr)
    return finalize_global_write_state();
  return Status::Ok();
}

Status Writer::get_buffer(
    const std::string& attribute, void** buffer, uint64_t** buffer_size) const {
  auto it = attr_buffers_.find(attribute);
  if (it == attr_buffers_.end()) {
    *buffer = nullptr;
    *buffer_size = nullptr;
  } else {
    *buffer = it->second.buffer_;
    *buffer_size = it->second.buffer_size_;
  }

  return Status::Ok();
}

Status Writer::get_buffer(
    const std::string& attribute,
    uint64_t** buffer_off,
    uint64_t** buffer_off_size,
    void** buffer_val,
    uint64_t** buffer_val_size) const {
  auto it = attr_buffers_.find(attribute);
  if (it == attr_buffers_.end()) {
    *buffer_off = nullptr;
    *buffer_off_size = nullptr;
    *buffer_val = nullptr;
    *buffer_val_size = nullptr;
  } else {
    *buffer_off = (uint64_t*)it->second.buffer_;
    *buffer_off_size = it->second.buffer_size_;
    *buffer_val = it->second.buffer_var_;
    *buffer_val_size = it->second.buffer_var_size_;
  }

  return Status::Ok();
}

Status Writer::init() {
  // Sanity checks
  if (storage_manager_ == nullptr)
    return LOG_STATUS(Status::WriterError(
        "Cannot initialize query; Storage manager not set"));
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot initialize query; Array metadata not set"));
  if (attr_buffers_.empty())
    return LOG_STATUS(
        Status::WriterError("Cannot initialize query; Buffers not set"));
  if (attributes_.empty())
    return LOG_STATUS(
        Status::WriterError("Cannot initialize query; Attributes not set"));

  if (subarray_ == nullptr)
    RETURN_NOT_OK(set_subarray(nullptr));
  RETURN_NOT_OK(check_subarray());
  RETURN_NOT_OK(check_buffer_sizes());
  RETURN_NOT_OK(check_attributes());

  optimize_layout_for_1D();

  // Get configuration parameters
  const char *check_coord_dups, *dedup_coords, *check_coord_oob;
  auto config = storage_manager_->config();
  RETURN_NOT_OK(config.get("sm.check_coord_dups", &check_coord_dups));
  RETURN_NOT_OK(config.get("sm.check_coord_oob", &check_coord_oob));
  RETURN_NOT_OK(config.get("sm.dedup_coords", &dedup_coords));
  assert(check_coord_dups != nullptr && dedup_coords != nullptr);
  check_coord_dups_ = !strcmp(check_coord_dups, "true");
  check_coord_oob_ = !strcmp(check_coord_oob, "true");
  dedup_coords_ = !strcmp(dedup_coords, "true");
  initialized_ = true;

  return Status::Ok();
}

Layout Writer::layout() const {
  return layout_;
}

void Writer::set_array(const Array* array) {
  array_ = array;
}

void Writer::reset_global_write_state() {
  global_write_state_.reset(nullptr);
}

void Writer::set_array_schema(const ArraySchema* array_schema) {
  array_schema_ = array_schema;
  if (array_schema->is_kv())
    layout_ = Layout::UNORDERED;
}

Status Writer::set_buffer(
    const std::string& attribute, void* buffer, uint64_t* buffer_size) {
  // Check buffer
  if (buffer == nullptr || buffer_size == nullptr)
    return LOG_STATUS(Status::WriterError(
        "Cannot set buffer; Buffer or buffer size is null"));

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot set buffer; Array schema not set"));

  // Check that attribute exists
  if (attribute != constants::coords &&
      array_schema_->attribute(attribute) == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot set buffer; Invalid attribute"));

  // Check that attribute is fixed-sized
  bool var_size =
      (attribute != constants::coords && array_schema_->var_size(attribute));
  if (var_size)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer; Input attribute '") + attribute +
        "' is var-sized"));

  // Error if setting a new attribute after initialization
  bool attr_exists = attr_buffers_.find(attribute) != attr_buffers_.end();
  if (initialized_ && !attr_exists)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer for new attribute '") + attribute +
        "' after initialization"));

  // Append to attributes only if buffer not set before
  if (!attr_exists)
    attributes_.push_back(std::string(attribute));

  // Set attribute buffer
  attr_buffers_[attribute] =
      AttributeBuffer(buffer, nullptr, buffer_size, nullptr);

  return Status::Ok();
}

Status Writer::set_buffer(
    const std::string& attribute,
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
  if (attribute != constants::coords &&
      array_schema_->attribute(attribute) == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot set buffer; Invalid attribute"));

  // Check that attribute is var-sized
  bool var_size =
      (attribute != constants::coords && array_schema_->var_size(attribute));
  if (!var_size)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer; Input attribute '") + attribute +
        "' is fixed-sized"));

  // Error if setting a new attribute after initialization
  bool attr_exists = attr_buffers_.find(attribute) != attr_buffers_.end();
  if (initialized_ && !attr_exists)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer for new attribute '") + attribute +
        "' after initialization"));

  // Append to attributes only if buffer not set before
  if (!attr_exists)
    attributes_.push_back(std::string(attribute));

  // Set attribute buffer
  attr_buffers_[attribute] =
      AttributeBuffer(buffer_off, buffer_val, buffer_off_size, buffer_val_size);

  return Status::Ok();
}

void Writer::set_fragment_uri(const URI& fragment_uri) {
  fragment_uri_ = fragment_uri;
}

Status Writer::set_layout(Layout layout) {
  // Check if the array is a key-value store
  if (array_schema_->is_kv())
    return LOG_STATUS(Status::WriterError(
        "Cannot set layout; The array is defined as a key-value store"));

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

void* Writer::subarray() const {
  return subarray_;
}

Status Writer::write() {
  STATS_FUNC_IN(writer_write);

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

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status Writer::check_attributes() {
  // There should be no duplicate attributes
  std::set<std::string> unique_attributes;
  int has_coords = 0;
  for (const auto& attr : attributes_) {
    unique_attributes.insert(attr);
    if (attr == constants::coords)
      has_coords = 1;
  }
  if (unique_attributes.size() != attributes_.size())
    return LOG_STATUS(
        Status::WriterError("Check attributes failed; Duplicate attributes"));

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
  if (attributes_.size() != array_schema_->attribute_num() + has_coords)
    return LOG_STATUS(Status::WriterError(
        "Check attributes failed; Writes expect "
        "all attributes (plus coordinates for unordered writes) to be set"));

  return Status::Ok();
}

Status Writer::check_buffer_sizes() const {
  // This is applicable only to dense arrays and ordered layout
  if (!array_schema_->dense() ||
      (layout_ != Layout::ROW_MAJOR && layout_ != Layout::COL_MAJOR))
    return Status::Ok();

  auto cell_num = array_schema_->domain()->cell_num(subarray_);
  uint64_t expected_cell_num = 0;
  for (const auto& attr : attributes_) {
    bool is_var = array_schema_->var_size(attr);
    auto it = attr_buffers_.find(attr);
    auto buffer_size = *it->second.buffer_size_;
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

  auto coords_buff_it = attr_buffers_.find(constants::coords);
  if (coords_buff_it == attr_buffers_.end())
    return LOG_STATUS(
        Status::WriterError("Cannot check for coordinate duplicates; "
                            "Coordinates buffer not found"));

  auto coords_buff = (unsigned char*)coords_buff_it->second.buffer_;
  auto coords_size = array_schema_->coords_size();
  auto coords_num = cell_pos.size();

  for (uint64_t i = 1; i < coords_num; ++i) {
    if (!memcmp(
            coords_buff + cell_pos[i] * coords_size,
            coords_buff + cell_pos[i - 1] * coords_size,
            coords_size)) {
      return LOG_STATUS(
          Status::WriterError("Duplicate coordinates are not allowed"));
    }
  }

  return Status::Ok();
  STATS_FUNC_OUT(writer_check_coord_dups);
}

Status Writer::check_coord_dups() const {
  STATS_FUNC_IN(writer_check_coord_dups_global);

  auto coords_buff_it = attr_buffers_.find(constants::coords);
  if (coords_buff_it == attr_buffers_.end())
    return LOG_STATUS(
        Status::WriterError("Cannot check for coordinate duplicates; "
                            "Coordinates buffer not found"));

  auto coords_buff = (unsigned char*)coords_buff_it->second.buffer_;
  auto coords_buff_size = *coords_buff_it->second.buffer_size_;
  auto coords_size = array_schema_->coords_size();
  auto coords_num = coords_buff_size / coords_size;

  for (uint64_t i = 1; i < coords_num; ++i) {
    if (!memcmp(
            coords_buff + i * coords_size,
            coords_buff + (i - 1) * coords_size,
            coords_size)) {
      return LOG_STATUS(
          Status::WriterError("Duplicate coordinates are not allowed"));
    }
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_check_coord_dups_global);
}

Status Writer::check_coord_oob() const {
  switch (array_schema_->domain()->type()) {
    case Datatype::INT8:
      return check_coord_oob<int8_t>();
    case Datatype::UINT8:
      return check_coord_oob<uint8_t>();
    case Datatype::INT16:
      return check_coord_oob<int16_t>();
    case Datatype::UINT16:
      return check_coord_oob<uint16_t>();
    case Datatype::INT32:
      return check_coord_oob<int32_t>();
    case Datatype::UINT32:
      return check_coord_oob<uint32_t>();
    case Datatype::INT64:
      return check_coord_oob<int64_t>();
    case Datatype::UINT64:
      return check_coord_oob<uint64_t>();
    case Datatype::FLOAT32:
      return check_coord_oob<float>();
    case Datatype::FLOAT64:
      return check_coord_oob<double>();
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
      return LOG_STATUS(
          Status::WriterError("Cannot perform out-of-bounds check on "
                              "coordinates; Domain type not supported"));
  }

  return Status::Ok();
}

template <class T>
Status Writer::check_coord_oob() const {
  auto coords_it = attr_buffers_.find(constants::coords);

  // Applicable only to sparse writes - exit if coordinates do not exist
  if (coords_it == attr_buffers_.end())
    return Status::Ok();

  // Get coordinates buffer
  auto coords_buff = (T*)coords_it->second.buffer_;
  auto coords_buff_size = *(coords_it->second.buffer_size_);
  auto coords_num = coords_buff_size / array_schema_->coords_size();
  auto dim_num = array_schema_->dim_num();
  auto domain = (T*)array_schema_->domain()->domain();

  // Check if all coordinates fall in the domain in parallel
  auto statuses = parallel_for(0, coords_num, [&](uint64_t i) {
    if (!utils::geometry::coords_in_rect<T>(
            &coords_buff[i * dim_num], domain, dim_num)) {
      std::stringstream ss;
      ss << "Write failed; Coordinates (" << coords_buff[i * dim_num];
      for (unsigned int j = 1; j < dim_num; ++j)
        ss << "," << coords_buff[i * dim_num + j];
      ss << ") are out of bounds";
      return LOG_STATUS(Status::WriterError(ss.str()));
    }
    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses) {
    if (!st.ok())
      return st;
  }

  // Success
  return Status::Ok();
}

Status Writer::check_subarray() const {
  if (subarray_ == nullptr)
    return Status::Ok();

  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot check subarray; Array schema not set"));

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

Status Writer::close_files(FragmentMetadata* meta) const {
  for (const auto& attr : attributes_) {
    RETURN_NOT_OK(storage_manager_->close_file(meta->attr_uri(attr)));
    if (array_schema_->var_size(attr))
      RETURN_NOT_OK(storage_manager_->close_file(meta->attr_var_uri(attr)));
  }

  return Status::Ok();
}

Status Writer::compute_coord_dups(
    const std::vector<uint64_t>& cell_pos,
    std::set<uint64_t>* coord_dups) const {
  STATS_FUNC_IN(writer_compute_coord_dups);

  auto coords_buff_it = attr_buffers_.find(constants::coords);
  if (coords_buff_it == attr_buffers_.end())
    return LOG_STATUS(
        Status::WriterError("Cannot check for coordinate duplicates; "
                            "Coordinates buffer not found"));

  auto coords_buff = (unsigned char*)coords_buff_it->second.buffer_;
  auto coords_size = array_schema_->coords_size();
  auto coords_num = cell_pos.size();

  for (uint64_t i = 1; i < coords_num; ++i) {
    if (!memcmp(
            coords_buff + cell_pos[i] * coords_size,
            coords_buff + cell_pos[i - 1] * coords_size,
            coords_size)) {
      coord_dups->insert(cell_pos[i]);
    }
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_compute_coord_dups);
}

Status Writer::compute_coord_dups(std::set<uint64_t>* coord_dups) const {
  STATS_FUNC_IN(writer_compute_coord_dups_global);

  auto coords_buff_it = attr_buffers_.find(constants::coords);
  if (coords_buff_it == attr_buffers_.end())
    return LOG_STATUS(
        Status::WriterError("Cannot check for coordinate duplicates; "
                            "Coordinates buffer not found"));

  auto coords_buff = (unsigned char*)coords_buff_it->second.buffer_;
  auto coords_buff_size = *coords_buff_it->second.buffer_size_;
  auto coords_size = array_schema_->coords_size();
  auto coords_num = coords_buff_size / coords_size;

  for (uint64_t i = 1; i < coords_num; ++i) {
    if (!memcmp(
            coords_buff + i * coords_size,
            coords_buff + (i - 1) * coords_size,
            coords_size)) {
      coord_dups->insert(i);
    }
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_compute_coord_dups_global);
}

template <class T>
Status Writer::compute_coords_metadata(
    const std::vector<Tile>& tiles, FragmentMetadata* meta) const {
  STATS_FUNC_IN(writer_compute_coords_metadata);

  // Check if tiles are empty
  if (tiles.empty())
    return Status::Ok();

  // For easy reference
  auto coords_size = array_schema_->coords_size();
  auto dim_num = array_schema_->dim_num();
  std::vector<T> mbr;
  mbr.resize(2 * dim_num);

  // Compute MBRs
  for (uint64_t tile_id = 0; tile_id < tiles.size(); tile_id++) {
    const auto& tile = tiles[tile_id];
    // Initialize MBR with the first coords
    auto data = (T*)tile.data();
    auto cell_num = tile.size() / coords_size;
    assert(cell_num > 0);
    for (unsigned i = 0; i < dim_num; ++i) {
      mbr[2 * i] = data[i];
      mbr[2 * i + 1] = data[i];
    }

    // Expand the MBR with the rest coords
    for (uint64_t i = 1; i < cell_num; ++i)
      utils::geometry::expand_mbr(&mbr[0], &data[i * dim_num], dim_num);

    meta->set_mbr(tile_id, &mbr[0]);
  }

  // Compute bounding coordinates
  std::vector<T> bcoords;
  bcoords.resize(2 * dim_num);
  for (uint64_t tile_id = 0; tile_id < tiles.size(); tile_id++) {
    const auto& tile = tiles[tile_id];
    auto data = (T*)tile.data();
    auto cell_num = tile.size() / coords_size;
    assert(cell_num > 0);

    std::memcpy(&bcoords[0], data, coords_size);
    std::memcpy(
        &bcoords[dim_num], &data[(cell_num - 1) * dim_num], coords_size);
    meta->set_bounding_coords(tile_id, &bcoords[0]);
  }

  // Set last tile cell number
  meta->set_last_tile_cell_num(tiles.back().size() / coords_size);

  return Status::Ok();

  STATS_FUNC_OUT(writer_compute_coords_metadata);
}

template <class T>
Status Writer::compute_write_cell_ranges(
    DenseCellRangeIter<T>* iter, WriteCellRangeVec* write_cell_ranges) const {
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
    start = iter->range_start();
    end = iter->range_end();
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
  uint64_t timestamp;
  if (!fragment_uri_.to_string().empty()) {
    uri = fragment_uri_;
  } else {
    std::string new_fragment_str;
    RETURN_NOT_OK(new_fragment_name(&new_fragment_str, &timestamp));
    uri = array_schema_->array_uri().join_path(new_fragment_str);
  }
  *frag_meta =
      std::make_shared<FragmentMetadata>(array_schema_, dense, uri, timestamp);
  RETURN_NOT_OK((*frag_meta)->init(subarray_));
  return storage_manager_->create_dir(uri);

  STATS_FUNC_OUT(writer_create_fragment);
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
  auto orig_size = tile->buffer()->size();

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
  STATS_COUNTER_ADD(writer_num_input_bytes, orig_size);

  return Status::Ok();
}

Status Writer::finalize_global_write_state() {
  auto coords_type = array_schema_->coords_type();
  switch (coords_type) {
    case Datatype::INT8:
      return finalize_global_write_state<int8_t>();
    case Datatype::UINT8:
      return finalize_global_write_state<uint8_t>();
    case Datatype::INT16:
      return finalize_global_write_state<int16_t>();
    case Datatype::UINT16:
      return finalize_global_write_state<uint16_t>();
    case Datatype::INT32:
      return finalize_global_write_state<int>();
    case Datatype::UINT32:
      return finalize_global_write_state<unsigned>();
    case Datatype::INT64:
      return finalize_global_write_state<int64_t>();
    case Datatype::UINT64:
      return finalize_global_write_state<uint64_t>();
    case Datatype::FLOAT32:
      return finalize_global_write_state<float>();
    case Datatype::FLOAT64:
      return finalize_global_write_state<double>();
    default:
      return LOG_STATUS(Status::WriterError(
          "Cannot finalize global write state; Unsupported domain type"));
  }

  return Status::Ok();
}

template <class T>
Status Writer::finalize_global_write_state() {
  assert(layout_ == Layout::GLOBAL_ORDER);
  auto meta = global_write_state_->frag_meta_.get();

  // Handle last tile
  Status st = global_write_handle_last_tile<T>();
  if (!st.ok()) {
    close_files(meta);
    storage_manager_->vfs()->remove_dir(meta->fragment_uri());
    global_write_state_.reset(nullptr);
    return st;
  }

  // Close all files
  st = close_files(meta);
  if (!st.ok()) {
    storage_manager_->vfs()->remove_dir(meta->fragment_uri());
    global_write_state_.reset(nullptr);
    return st;
  }

  // Check that the same number of cells was written across attributes
  for (size_t i = 1; i < attributes_.size(); ++i) {
    if (global_write_state_->cells_written_[attributes_[i]] !=
        global_write_state_->cells_written_[attributes_[i - 1]]) {
      storage_manager_->vfs()->remove_dir(meta->fragment_uri());
      global_write_state_.reset(nullptr);
      return LOG_STATUS(Status::WriterError(
          "Failed to finalize global write state; Different "
          "number of cells written across attributes"));
    }
  }

  // Check if the total number of cells written is equal to the subarray size
  if (!has_coords()) {
    auto cells_written = global_write_state_->cells_written_[attributes_[0]];
    if (cells_written != array_schema_->domain()->cell_num<T>((T*)subarray_)) {
      storage_manager_->vfs()->remove_dir(meta->fragment_uri());
      global_write_state_.reset(nullptr);
      return LOG_STATUS(Status::WriterError(
          "Failed to finalize global write state; Number "
          "of cells written is different from the number of "
          "cells expected for the query subarray"));
    }
  }

  // Flush fragment metadata to storage
  st = storage_manager_->store_fragment_metadata(
      meta, array_->get_encryption_key());
  if (!st.ok())
    storage_manager_->vfs()->remove_dir(meta->fragment_uri());

  // Delete global write state
  global_write_state_.reset(nullptr);

  return st;
}

Status Writer::global_write() {
  STATS_FUNC_IN(writer_global_write);

  // Applicable only to global write on dense/sparse arrays
  assert(layout_ == Layout::GLOBAL_ORDER);

  auto coords_type = array_schema_->coords_type();
  switch (coords_type) {
    case Datatype::INT8:
      return global_write<int8_t>();
    case Datatype::UINT8:
      return global_write<uint8_t>();
    case Datatype::INT16:
      return global_write<int16_t>();
    case Datatype::UINT16:
      return global_write<uint16_t>();
    case Datatype::INT32:
      return global_write<int>();
    case Datatype::UINT32:
      return global_write<unsigned>();
    case Datatype::INT64:
      return global_write<int64_t>();
    case Datatype::UINT64:
      return global_write<uint64_t>();
    case Datatype::FLOAT32:
      assert(!array_schema_->dense());
      return global_write<float>();
    case Datatype::FLOAT64:
      assert(!array_schema_->dense());
      return global_write<double>();
    default:
      return LOG_STATUS(Status::WriterError(
          "Cannot write in global layout; Unsupported domain type"));
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_global_write);
}

template <class T>
Status Writer::global_write() {
  // Initialize the global write state if this is the first invocation
  if (!global_write_state_)
    RETURN_CANCEL_OR_ERROR(init_global_write_state());
  auto frag_meta = global_write_state_->frag_meta_.get();
  auto uri = frag_meta->fragment_uri();
  auto num_attributes = attributes_.size();

  // Check for coordinate duplicates
  bool has_coords =
      attr_buffers_.find(constants::coords) != attr_buffers_.end();
  if (has_coords && check_coord_dups_ && !dedup_coords_)
    RETURN_CANCEL_OR_ERROR(check_coord_dups());

  // Retrieve coordinate duplicates
  std::set<uint64_t> coord_dups;
  if (dedup_coords_)
    RETURN_CANCEL_OR_ERROR(compute_coord_dups(&coord_dups));

  // Prepare tiles for all attributes
  std::vector<std::vector<Tile>> attribute_tiles(num_attributes);
  auto statuses = parallel_for(0, num_attributes, [&](uint64_t i) {
    const auto& attr = attributes_[i];
    auto& full_tiles = attribute_tiles[i];
    RETURN_CANCEL_OR_ERROR(prepare_full_tiles(attr, coord_dups, &full_tiles));
    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses) {
    if (!st.ok()) {
      storage_manager_->vfs()->remove_dir(uri);
      global_write_state_.reset(nullptr);
      return st;
    }
  }
  statuses.clear();

  // Increment number of tiles in the fragment metadata
  uint64_t num_tiles = array_schema_->var_size(attributes_[0]) ?
                           attribute_tiles[0].size() / 2 :
                           attribute_tiles[0].size();
  auto new_num_tiles = frag_meta->tile_index_base() + num_tiles;
  frag_meta->set_num_tiles(new_num_tiles);

  // Filter all tiles
  statuses = parallel_for(0, num_attributes, [&](uint64_t i) {
    const auto& attr = attributes_[i];
    auto& full_tiles = attribute_tiles[i];
    if (attr == constants::coords)
      RETURN_CANCEL_OR_ERROR(compute_coords_metadata<T>(full_tiles, frag_meta));
    RETURN_CANCEL_OR_ERROR(filter_tiles(attr, &full_tiles));
    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses) {
    if (!st.ok()) {
      storage_manager_->vfs()->remove_dir(uri);
      global_write_state_.reset(nullptr);
      return st;
    }
  }
  statuses.clear();

  // Write tiles for all attributes
  auto st = write_all_tiles(frag_meta, attribute_tiles);
  if (!st.ok()) {
    storage_manager_->vfs()->remove_dir(uri);
    global_write_state_.reset(nullptr);
    return st;
  }

  // Increment the tile index base for the next global order write.
  frag_meta->set_tile_index_base(new_num_tiles);

  return Status::Ok();
}

template <class T>
Status Writer::global_write_handle_last_tile() {
  // See if any last tiles are nonempty.
  bool all_empty = true;
  for (const auto& attr : attributes_) {
    auto& last_tile = global_write_state_->last_tiles_[attr].first;
    if (!last_tile.empty()) {
      all_empty = false;
      break;
    }
  }

  // Return early if there are no tiles to write.
  if (all_empty)
    return Status::Ok();

  // Reserve space for the last tile in the fragment metadata
  auto meta = global_write_state_->frag_meta_.get();
  meta->set_num_tiles(meta->tile_index_base() + 1);

  // Filter the last tiles
  uint64_t num_attributes = attributes_.size();
  std::vector<std::vector<Tile>> attribute_tiles(num_attributes);
  auto statuses = parallel_for(0, num_attributes, [&](uint64_t i) {
    const auto& attr = attributes_[i];
    auto& last_tile = global_write_state_->last_tiles_[attr].first;
    auto& last_tile_var = global_write_state_->last_tiles_[attr].second;

    if (!last_tile.empty()) {
      std::vector<Tile>& tiles = attribute_tiles[i];
      tiles.push_back(last_tile);
      if (!last_tile_var.empty())
        tiles.push_back(last_tile_var);
      if (attr == constants::coords)
        RETURN_NOT_OK(compute_coords_metadata<T>(tiles, meta));
      RETURN_NOT_OK(filter_tiles(attr, &tiles));
    }
    return Status::Ok();
  });

  // Check statuses
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  // Write the last tiles
  RETURN_NOT_OK(write_all_tiles(meta, attribute_tiles));

  // Increment the tile index base.
  meta->set_tile_index_base(meta->tile_index_base() + 1);

  return Status::Ok();
}

bool Writer::has_coords() const {
  return attr_buffers_.find(constants::coords) != attr_buffers_.end();
}

Status Writer::init_global_write_state() {
  STATS_FUNC_IN(writer_init_global_write_state);

  // Create global array state object
  if (global_write_state_ != nullptr)
    return LOG_STATUS(Status::WriterError(
        "Cannot initialize global write state; State not properly finalized"));
  global_write_state_.reset(new GlobalWriteState);

  // Create fragments
  RETURN_NOT_OK(
      create_fragment(!has_coords(), &(global_write_state_->frag_meta_)));

  Status st = Status::Ok();
  for (const auto& attr : attributes_) {
    // Initialize last tiles
    auto last_tile_pair = std::pair<std::string, std::pair<Tile, Tile>>(
        attr, std::pair<Tile, Tile>(Tile(), Tile()));
    auto it_ret = global_write_state_->last_tiles_.emplace(last_tile_pair);

    if (!array_schema_->var_size(attr)) {
      auto& last_tile = it_ret.first->second.first;
      st = init_tile(attr, &last_tile);
      if (!st.ok())
        break;
    } else {
      auto& last_tile = it_ret.first->second.first;
      auto& last_tile_var = it_ret.first->second.second;
      st = init_tile(attr, &last_tile, &last_tile_var);
      if (!st.ok())
        break;
    }

    // Initialize cells written
    global_write_state_->cells_written_[attr] = 0;
  }

  // Handle error
  if (!st.ok()) {
    storage_manager_->vfs()->remove_dir(
        global_write_state_->frag_meta_->fragment_uri());
    global_write_state_.reset(nullptr);
  }

  return st;

  STATS_FUNC_OUT(writer_init_global_write_state);
}

Status Writer::init_tile(const std::string& attribute, Tile* tile) const {
  // For easy reference
  auto domain = array_schema_->domain();
  auto cell_size = array_schema_->cell_size(attribute);
  auto capacity = array_schema_->capacity();
  auto type = array_schema_->type(attribute);
  auto is_coords = (attribute == constants::coords);
  auto dim_num = (is_coords) ? array_schema_->dim_num() : 0;
  auto cell_num_per_tile =
      (has_coords()) ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * cell_size;

  // Initialize
  RETURN_NOT_OK(tile->init(type, tile_size, cell_size, dim_num));

  return Status::Ok();
}

Status Writer::init_tile(
    const std::string& attribute, Tile* tile, Tile* tile_var) const {
  // For easy reference
  auto domain = array_schema_->domain();
  auto capacity = array_schema_->capacity();
  auto type = array_schema_->type(attribute);
  auto cell_num_per_tile =
      (has_coords()) ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * constants::cell_var_offset_size;

  // Initialize
  RETURN_NOT_OK(tile->init(
      constants::cell_var_offset_type,
      tile_size,
      constants::cell_var_offset_size,
      0));
  RETURN_NOT_OK(tile_var->init(type, tile_size, datatype_size(type), 0));
  return Status::Ok();
}

template <class T>
Status Writer::init_tile_dense_cell_range_iters(
    std::vector<DenseCellRangeIter<T>>* iters) const {
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
  ss << "/__" << uuid << "_" << *timestamp;
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
      return ordered_write<int>();
    case Datatype::UINT32:
      return ordered_write<unsigned>();
    case Datatype::INT64:
      return ordered_write<int64_t>();
    case Datatype::UINT64:
      return ordered_write<uint64_t>();
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
  std::vector<DenseCellRangeIter<T>> dense_cell_range_its;
  RETURN_CANCEL_OR_ERROR_ELSE(
      init_tile_dense_cell_range_iters<T>(&dense_cell_range_its),
      storage_manager_->vfs()->remove_dir(uri));
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
        storage_manager_->vfs()->remove_dir(uri));
  dense_cell_range_its.clear();

  // Set number of tiles in the fragment metadata
  frag_meta->set_num_tiles(tile_num);

  // Prepare tiles for all attributes and filter
  uint64_t num_attributes = attributes_.size();
  std::vector<std::vector<Tile>> attr_tiles(num_attributes);
  auto statuses = parallel_for(0, num_attributes, [&](uint64_t i) {
    const auto& attr = attributes_[i];
    std::vector<Tile>& tiles = attr_tiles[i];
    RETURN_CANCEL_OR_ERROR(prepare_tiles(attr, write_cell_ranges, &tiles));
    RETURN_CANCEL_OR_ERROR(filter_tiles(attr, &tiles));
    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK_ELSE(st, storage_manager_->vfs()->remove_dir(uri));

  // Write tiles for all attributes
  RETURN_NOT_OK_ELSE(
      write_all_tiles(frag_meta.get(), attr_tiles),
      storage_manager_->vfs()->remove_dir(uri));

  // Write the fragment metadata
  RETURN_CANCEL_OR_ERROR_ELSE(
      storage_manager_->store_fragment_metadata(
          frag_meta.get(), array_->get_encryption_key()),
      storage_manager_->vfs()->remove_dir(uri));

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
  auto it = attr_buffers_.find(attribute);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_size = it->second.buffer_size_;
  auto capacity = array_schema_->capacity();
  auto cell_size = array_schema_->cell_size(attribute);
  auto cell_num = *buffer_size / cell_size;
  auto domain = array_schema_->domain();
  auto cell_num_per_tile =
      (has_coords()) ? capacity : domain->cell_num_per_tile();

  // Do nothing if there are no cells to write
  if (cell_num == 0)
    return Status::Ok();

  // First fill the last tile
  auto& last_tile = global_write_state_->last_tiles_[attribute].first;
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

  global_write_state_->cells_written_[attribute] += cell_num;

  return Status::Ok();

  STATS_FUNC_OUT(writer_prepare_full_tiles_fixed);
}

Status Writer::prepare_full_tiles_var(
    const std::string& attribute,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  STATS_FUNC_IN(writer_prepare_full_tiles_var);

  // For easy reference
  auto it = attr_buffers_.find(attribute);
  auto buffer = (uint64_t*)it->second.buffer_;
  auto buffer_var = (unsigned char*)it->second.buffer_var_;
  auto buffer_size = it->second.buffer_size_;
  auto buffer_var_size = it->second.buffer_var_size_;
  auto capacity = array_schema_->capacity();
  auto cell_num = *buffer_size / constants::cell_var_offset_size;
  auto domain = array_schema_->domain();
  auto cell_num_per_tile =
      (has_coords()) ? capacity : domain->cell_num_per_tile();
  uint64_t offset, var_size;

  // Do nothing if there are no cells to write
  if (cell_num == 0)
    return Status::Ok();

  // First fill the last tile
  auto& last_tile_pair = global_write_state_->last_tiles_[attribute];
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

  global_write_state_->cells_written_[attribute] += cell_num;

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

template <class T>
Status Writer::sort_coords(std::vector<uint64_t>* cell_pos) const {
  STATS_FUNC_IN(writer_sort_coords);

  // For easy reference
  auto domain = array_schema_->domain();
  uint64_t coords_size = array_schema_->coords_size();
  auto it = attr_buffers_.find(constants::coords);
  auto buffer = (T*)it->second.buffer_;
  auto buffer_size = it->second.buffer_size_;
  uint64_t coords_num = *buffer_size / coords_size;

  // Populate cell_pos
  cell_pos->resize(coords_num);
  for (uint64_t i = 0; i < coords_num; ++i)
    (*cell_pos)[i] = i;

  // Sort the coordinates in global order
  parallel_sort(
      cell_pos->begin(), cell_pos->end(), GlobalCmp<T>(domain, buffer));

  return Status::Ok();

  STATS_FUNC_OUT(writer_sort_coords);
}

Status Writer::unordered_write() {
  STATS_FUNC_IN(writer_unordered_write);

  // Applicable only to unordered write on dense/sparse arrays
  assert(layout_ == Layout::UNORDERED);

  auto coords_type = array_schema_->coords_type();
  switch (coords_type) {
    case Datatype::INT8:
      return unordered_write<int8_t>();
    case Datatype::UINT8:
      return unordered_write<uint8_t>();
    case Datatype::INT16:
      return unordered_write<int16_t>();
    case Datatype::UINT16:
      return unordered_write<uint16_t>();
    case Datatype::INT32:
      return unordered_write<int>();
    case Datatype::UINT32:
      return unordered_write<unsigned>();
    case Datatype::INT64:
      return unordered_write<int64_t>();
    case Datatype::UINT64:
      return unordered_write<uint64_t>();
    case Datatype::FLOAT32:
      assert(!array_schema_->dense());
      return unordered_write<float>();
    case Datatype::FLOAT64:
      assert(!array_schema_->dense());
      return unordered_write<double>();
    default:
      return LOG_STATUS(Status::WriterError(
          "Cannot write in unordered layout; Unsupported domain type"));
  }

  return Status::Ok();

  STATS_FUNC_OUT(writer_unordered_write);
}

template <class T>
Status Writer::unordered_write() {
  // Sort coordinates first
  std::vector<uint64_t> cell_pos;
  RETURN_CANCEL_OR_ERROR(sort_coords<T>(&cell_pos));

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
  auto uri = frag_meta->fragment_uri();

  // Prepare tiles for all attributes
  auto num_attributes = attributes_.size();
  std::vector<std::vector<Tile>> attribute_tiles(num_attributes);
  auto statuses = parallel_for(0, num_attributes, [&](uint64_t i) {
    const auto& attr = attributes_[i];
    auto& tiles = attribute_tiles[i];
    RETURN_CANCEL_OR_ERROR(prepare_tiles(attr, cell_pos, coord_dups, &tiles));
    return Status::Ok();
  });

  // Clear the boolean vector for coordinate duplicates
  coord_dups.clear();

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK_ELSE(st, storage_manager_->vfs()->remove_dir(uri));
  statuses.clear();

  // Set the number of tiles in the metadata
  uint64_t num_tiles = array_schema_->var_size(attributes_[0]) ?
                           attribute_tiles[0].size() / 2 :
                           attribute_tiles[0].size();
  frag_meta->set_num_tiles(num_tiles);

  // Filter all tiles
  statuses = parallel_for(0, num_attributes, [&](uint64_t i) {
    const auto& attr = attributes_[i];
    auto& tiles = attribute_tiles[i];
    if (attr == constants::coords)
      RETURN_CANCEL_OR_ERROR(
          compute_coords_metadata<T>(tiles, frag_meta.get()));
    RETURN_CANCEL_OR_ERROR(filter_tiles(attr, &tiles));
    return Status::Ok();
  });

  // Check all statuses
  for (auto& st : statuses)
    RETURN_NOT_OK_ELSE(st, storage_manager_->vfs()->remove_dir(uri));
  statuses.clear();

  // Write tiles for all attributes
  RETURN_NOT_OK_ELSE(
      write_all_tiles(frag_meta.get(), attribute_tiles),
      storage_manager_->vfs()->remove_dir(uri));

  // Write the fragment metadata
  RETURN_CANCEL_OR_ERROR_ELSE(
      storage_manager_->store_fragment_metadata(
          frag_meta.get(), array_->get_encryption_key()),
      storage_manager_->vfs()->remove_dir(uri));

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
    const std::vector<std::vector<tiledb::sm::Tile>>& attribute_tiles) const {
  STATS_FUNC_IN(writer_write_all_tiles);

  std::vector<std::future<Status>> tasks;

  auto num_attributes = attributes_.size();
  for (uint64_t i = 0; i < num_attributes; i++) {
    const auto& attr = attributes_[i];
    auto& tiles = attribute_tiles[i];
    tasks.push_back(
        storage_manager_->writer_thread_pool()->enqueue([&, this]() {
          RETURN_CANCEL_OR_ERROR(write_tiles(attr, frag_meta, tiles));
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
  auto attr_uri = frag_meta->attr_uri(attribute);
  auto attr_var_uri = var_size ? frag_meta->attr_var_uri(attribute) : URI("");

  // Write tiles
  auto tile_num = tiles.size();
  for (size_t i = 0, tile_id = 0; i < tile_num; ++i, ++tile_id) {
    RETURN_NOT_OK(storage_manager_->write(attr_uri, tiles[i].buffer()));
    frag_meta->set_tile_offset(attribute, tile_id, tiles[i].buffer()->size());

    STATS_COUNTER_ADD(writer_num_bytes_written, tiles[i].buffer()->size());

    if (var_size) {
      ++i;

      RETURN_NOT_OK(storage_manager_->write(attr_var_uri, tiles[i].buffer()));
      frag_meta->set_tile_var_offset(
          attribute, tile_id, tiles[i].buffer()->size());
      frag_meta->set_tile_var_size(
          attribute, tile_id, tiles[i].pre_filtered_size());

      STATS_COUNTER_ADD(writer_num_bytes_written, tiles[i].buffer()->size());
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

}  // namespace sm
}  // namespace tiledb
