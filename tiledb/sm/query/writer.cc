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
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/tile_io.h"

#include <sstream>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Writer::Writer() {
  array_schema_ = nullptr;
  global_write_state_.reset(nullptr);
  layout_ = Layout::ROW_MAJOR;
  storage_manager_ = nullptr;
  subarray_ = nullptr;
}

Writer::~Writer() {
  if (subarray_ != nullptr)
    std::free(subarray_);
}

/* ****************************** */
/*               API              */
/* ****************************** */

const ArraySchema* Writer::array_schema() const {
  return array_schema_;
}

Status Writer::finalize() {
  if (global_write_state_ != nullptr)
    return finalize_global_write_state();
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
  RETURN_NOT_OK(check_subarray(subarray_));
  RETURN_NOT_OK(check_buffer_sizes());

  return Status::Ok();
}

Layout Writer::layout() const {
  return layout_;
}

void Writer::set_array_schema(const ArraySchema* array_schema) {
  array_schema_ = array_schema;
  if (array_schema->is_kv())
    layout_ = Layout::UNORDERED;
}

Status Writer::set_buffers(
    const char** attributes,
    unsigned int attribute_num,
    void** buffers,
    uint64_t* buffer_sizes) {
  if (buffers == nullptr || buffer_sizes == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot set buffers; Buffers not provided"));

  RETURN_NOT_OK(set_attributes(attributes, attribute_num));
  set_buffers(buffers, buffer_sizes);

  return Status::Ok();
}

void Writer::set_buffers(void** buffers, uint64_t* buffer_sizes) {
  attr_buffers_.clear();
  unsigned bid = 0;
  for (const auto& attr : attributes_) {
    if (!array_schema_->var_size(attr)) {
      attr_buffers_.emplace(
          attr,
          AttributeBuffer(buffers[bid], nullptr, &buffer_sizes[bid], nullptr));
      ++bid;
    } else {
      attr_buffers_.emplace(
          attr,
          AttributeBuffer(
              buffers[bid],
              buffers[bid + 1],
              &buffer_sizes[bid],
              &buffer_sizes[bid + 1]));
      bid += 2;
    }
  }
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
  RETURN_NOT_OK(check_subarray(subarray));

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

Status Writer::write() {
  RETURN_NOT_OK(check_attributes());

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
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status Writer::check_attributes() {
  // There should be no duplicate attributes
  std::set<std::string> unique_attributes;
  for (const auto& attr : attributes_)
    unique_attributes.insert(attr);
  if (unique_attributes.size() != attributes_.size())
    return LOG_STATUS(
        Status::WriterError("Check attributes failed; Duplicate attributes"));

  // If it is an unordered write query, all attributes must be provided
  if (layout_ == Layout::UNORDERED) {
    if (attributes_.size() != array_schema_->attribute_num() + 1)
      return LOG_STATUS(Status::WriterError(
          "Check attributes failed; Unordered writes expect "
          "all attributes (plus coordinates) to be set"));
  }

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

Status Writer::check_subarray(const void* subarray) const {
  if (subarray == nullptr)
    return Status::Ok();

  switch (array_schema_->domain()->type()) {
    case Datatype::INT8:
      return check_subarray<int8_t>(static_cast<const int8_t*>(subarray));
    case Datatype::UINT8:
      return check_subarray<uint8_t>(static_cast<const uint8_t*>(subarray));
    case Datatype::INT16:
      return check_subarray<int16_t>(static_cast<const int16_t*>(subarray));
    case Datatype::UINT16:
      return check_subarray<uint16_t>(static_cast<const uint16_t*>(subarray));
    case Datatype::INT32:
      return check_subarray<int32_t>(static_cast<const int32_t*>(subarray));
    case Datatype::UINT32:
      return check_subarray<uint32_t>(static_cast<const uint32_t*>(subarray));
    case Datatype::INT64:
      return check_subarray<int64_t>(static_cast<const int64_t*>(subarray));
    case Datatype::UINT64:
      return check_subarray<uint64_t>(static_cast<const uint64_t*>(subarray));
    case Datatype::FLOAT32:
      return check_subarray<float>(static_cast<const float*>(subarray));
    case Datatype::FLOAT64:
      return check_subarray<double>(static_cast<const double*>(subarray));
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
Status Writer::check_subarray(const T* subarray) const {
  // Check subarray bounds
  auto domain = array_schema_->domain();
  auto dim_num = domain->dim_num();
  for (unsigned int i = 0; i < dim_num; ++i) {
    auto dim_domain = static_cast<const T*>(domain->dimension(i)->domain());
    if (subarray[2 * i] < dim_domain[0] || subarray[2 * i + 1] > dim_domain[1])
      return LOG_STATUS(Status::WriterError("Subarray out of bounds"));
    if (subarray[2 * i] > subarray[2 * i + 1])
      return LOG_STATUS(Status::WriterError(
          "Subarray lower bound is larger than upper bound"));
  }

  // In global dense writes, the subarray must coincide with tile extents
  if (array_schema_->dense() && layout_ == Layout::GLOBAL_ORDER) {
    for (unsigned int i = 0; i < dim_num; ++i) {
      auto dim_domain = static_cast<const T*>(domain->dimension(i)->domain());
      auto tile_extent =
          static_cast<const T*>(domain->dimension(i)->tile_extent());
      assert(tile_extent != nullptr);
      auto norm_1 = (subarray[2 * i] - dim_domain[0]);
      auto norm_2 = (subarray[2 * i + 1] - dim_domain[0]) + 1;
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

template <class T>
Status Writer::compute_coords_metadata(
    const std::vector<Tile>& tiles, FragmentMetadata* meta) const {
  // Check if tiles are empty
  if (tiles.empty())
    return Status::Ok();

  // For easy reference
  auto coords_size = array_schema_->coords_size();
  auto dim_num = array_schema_->dim_num();
  std::vector<T> mbr;
  mbr.resize(2 * dim_num);

  // Compute MBRs
  for (const auto& tile : tiles) {
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
      utils::expand_mbr(&mbr[0], &data[i * dim_num], dim_num);

    meta->append_mbr(&mbr[0]);
  }

  // Compute bounding coordinates
  std::vector<T> bcoords;
  bcoords.resize(2 * dim_num);
  for (const auto& tile : tiles) {
    auto data = (T*)tile.data();
    auto cell_num = tile.size() / coords_size;
    assert(cell_num > 0);

    std::memcpy(&bcoords[0], data, coords_size);
    std::memcpy(
        &bcoords[dim_num], &data[(cell_num - 1) * dim_num], coords_size);
    meta->append_bounding_coords(&bcoords[0]);
  }

  // Set last tile cell number
  meta->set_last_tile_cell_num(tiles.back().size() / coords_size);

  return Status::Ok();
}

template <class T>
Status Writer::compute_write_cell_ranges(
    DenseCellRangeIter<T>* iter, WriteCellRangeVec* write_cell_ranges) const {
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
}

Status Writer::create_fragment(
    bool dense, std::shared_ptr<FragmentMetadata>* frag_meta) const {
  auto uri = URI(new_fragment_name());
  *frag_meta = std::make_shared<FragmentMetadata>(array_schema_, dense, uri);
  RETURN_NOT_OK((*frag_meta)->init(subarray_));
  return storage_manager_->create_dir(uri);
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
  st = storage_manager_->store_fragment_metadata(meta);
  if (!st.ok())
    storage_manager_->vfs()->remove_dir(meta->fragment_uri());

  // Delete global write state
  global_write_state_.reset(nullptr);

  return st;
}

Status Writer::global_write() {
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
}

template <class T>
Status Writer::global_write() {
  // Initialize the global write state if this is the first invocation
  if (!global_write_state_)
    RETURN_NOT_OK(init_global_write_state());
  auto frag_meta = global_write_state_->frag_meta_.get();
  auto uri = frag_meta->fragment_uri();

  // Prepare tiles for all attributes and write
  auto st = Status::Ok();
  for (const auto& attr : attributes_) {
    std::vector<Tile> full_tiles;
    st = prepare_full_tiles(attr, &full_tiles);
    if (!st.ok())
      break;

    if (attr == constants::coords) {
      st = compute_coords_metadata<T>(full_tiles, frag_meta);
      if (!st.ok())
        break;
    }

    st = write_tiles(attr, frag_meta, full_tiles);
    if (!st.ok())
      break;
  }

  if (!st.ok()) {
    storage_manager_->vfs()->remove_dir(uri);
    global_write_state_.reset(nullptr);
  }

  return st;
}

template <class T>
Status Writer::global_write_handle_last_tile() {
  for (const auto& attr : attributes_) {
    auto& last_tile = global_write_state_->last_tiles_[attr].first;
    auto& last_tile_var = global_write_state_->last_tiles_[attr].second;
    auto meta = global_write_state_->frag_meta_.get();
    if (!last_tile.empty()) {
      std::vector<Tile> tiles;
      tiles.push_back(last_tile);
      if (!last_tile_var.empty())
        tiles.push_back(last_tile_var);
      if (attr == constants::coords)
        RETURN_NOT_OK(compute_coords_metadata<T>(tiles, meta));
      RETURN_NOT_OK(write_tiles(attr, meta, tiles));
    }
  }

  return Status::Ok();
}

bool Writer::has_coords() const {
  return attr_buffers_.find(constants::coords) != attr_buffers_.end();
}

Status Writer::init_global_write_state() {
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
}

Status Writer::init_tile(const std::string& attribute, Tile* tile) const {
  // For easy reference
  auto domain = array_schema_->domain();
  auto cell_size = array_schema_->cell_size(attribute);
  auto capacity = array_schema_->capacity();
  auto type = array_schema_->type(attribute);
  auto is_coords = (attribute == constants::coords);
  auto dim_num = (is_coords) ? array_schema_->dim_num() : 0;
  auto compressor = array_schema_->compression(attribute);
  auto compression_level = array_schema_->compression_level(attribute);
  auto cell_num_per_tile =
      (has_coords()) ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * cell_size;

  // Initialize
  RETURN_NOT_OK(tile->init(
      type, compressor, compression_level, tile_size, cell_size, dim_num));

  return Status::Ok();
}

Status Writer::init_tile(
    const std::string& attribute, Tile* tile, Tile* tile_var) const {
  // For easy reference
  auto domain = array_schema_->domain();
  auto capacity = array_schema_->capacity();
  auto type = array_schema_->type(attribute);
  auto compressor = array_schema_->compression(attribute);
  auto compression_level = array_schema_->compression_level(attribute);
  auto cell_num_per_tile =
      (has_coords()) ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * constants::cell_var_offset_size;

  // Initialize
  RETURN_NOT_OK(tile->init(
      constants::cell_var_offset_type,
      array_schema_->cell_var_offsets_compression(),
      array_schema_->cell_var_offsets_compression_level(),
      tile_size,
      constants::cell_var_offset_size,
      0));
  RETURN_NOT_OK(tile_var->init(
      type, compressor, compression_level, tile_size, datatype_size(type), 0));
  return Status::Ok();
}

template <class T>
Status Writer::init_tile_dense_cell_range_iters(
    std::vector<DenseCellRangeIter<T>>* iters) const {
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
    domain->subarray_overlap(
        &subarray[0], &tile_subarray[0], &subarray_in_tile[0], &tile_overlap);

    // Create a new iter
    iters->emplace_back(domain, subarray_in_tile, cell_order);

    // Get next tile coordinates
    domain->get_next_tile_coords(&tile_domain[0], &tile_coords[0], &in);
    assert((i != tile_num - 1 && in) || (i == tile_num - 1 && !in));
  }

  return Status::Ok();
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

std::string Writer::new_fragment_name() const {
  uint64_t ms = utils::timestamp_ms();
  std::stringstream ss;
  ss << array_schema_->array_uri().to_string() << "/__"
     << std::this_thread::get_id() << "_" << ms;
  return ss.str();
}

Status Writer::ordered_write() {
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
}

template <class T>
Status Writer::ordered_write() {
  // Create new fragment
  std::shared_ptr<FragmentMetadata> frag_meta;
  RETURN_NOT_OK(create_fragment(true, &frag_meta));
  auto uri = frag_meta->fragment_uri();

  // Initialize dense cell range iterators for each tile in global order
  std::vector<DenseCellRangeIter<T>> dense_cell_range_its;
  RETURN_NOT_OK_ELSE(
      init_tile_dense_cell_range_iters<T>(&dense_cell_range_its),
      storage_manager_->vfs()->remove_dir(uri));
  auto tile_num = dense_cell_range_its.size();
  if (tile_num == 0)  // Nothing to write
    return Status::Ok();

  // Compute write cell ranges, one vector per overlapping tile
  std::vector<WriteCellRangeVec> write_cell_ranges;
  write_cell_ranges.resize(tile_num);
  for (size_t i = 0; i < tile_num; ++i)
    RETURN_NOT_OK_ELSE(
        compute_write_cell_ranges<T>(
            &dense_cell_range_its[i], &write_cell_ranges[i]),
        storage_manager_->vfs()->remove_dir(uri));
  dense_cell_range_its.clear();

  // Prepare tiles for all attributes and write
  for (const auto& attr : attributes_) {
    std::vector<Tile> tiles;
    RETURN_NOT_OK_ELSE(
        prepare_tiles(attr, write_cell_ranges, &tiles),
        storage_manager_->vfs()->remove_dir(uri));
    RETURN_NOT_OK_ELSE(
        write_tiles(attr, frag_meta.get(), tiles),
        storage_manager_->vfs()->remove_dir(uri));
  }

  // Write the fragment metadata
  RETURN_NOT_OK_ELSE(
      storage_manager_->store_fragment_metadata(frag_meta.get()),
      storage_manager_->vfs()->remove_dir(uri));

  return Status::Ok();
}

Status Writer::prepare_full_tiles(
    const std::string& attribute, std::vector<Tile>* tiles) const {
  return array_schema_->var_size(attribute) ?
             prepare_full_tiles_var(attribute, tiles) :
             prepare_full_tiles_fixed(attribute, tiles);
}

Status Writer::prepare_full_tiles_fixed(
    const std::string& attribute, std::vector<Tile>* tiles) const {
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
    do {
      RETURN_NOT_OK(last_tile.write(buffer + cell_idx * cell_size, cell_size));
      ++cell_idx;
    } while (!last_tile.full());
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
    for (uint64_t tile_idx = 0, i = 0; i < cell_num_to_write; ++cell_idx, ++i) {
      if ((*tiles)[tile_idx].full())
        ++tile_idx;

      RETURN_NOT_OK(
          (*tiles)[tile_idx].write(buffer + cell_idx * cell_size, cell_size));
    }
  }

  // Potentially fill the last tile
  assert(cell_num - cell_idx < cell_num_per_tile - last_tile.cell_num());
  for (; cell_idx < cell_num; ++cell_idx) {
    RETURN_NOT_OK(last_tile.write(buffer + cell_idx * cell_size, cell_size));
  }

  global_write_state_->cells_written_[attribute] += cell_num;

  return Status::Ok();
}

Status Writer::prepare_full_tiles_var(
    const std::string& attribute, std::vector<Tile>* tiles) const {
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
    } while (!last_tile.full());
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
    for (uint64_t tile_idx = 0, i = 0; i < cell_num_to_write; ++cell_idx, ++i) {
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

  // Potentially fill the last tile
  assert(cell_num - cell_idx < cell_num_per_tile - last_tile.cell_num());
  for (; cell_idx < cell_num; ++cell_idx) {
    // Write offset
    offset = last_tile_var.size();
    RETURN_NOT_OK(last_tile.write(&offset, sizeof(offset)));

    // Write var-sized value
    var_size = (cell_idx == cell_num - 1) ?
                   *buffer_var_size - buffer[cell_idx] :
                   buffer[cell_idx + 1] - buffer[cell_idx];
    RETURN_NOT_OK(last_tile_var.write(&buffer_var[buffer[cell_idx]], var_size));
  }

  global_write_state_->cells_written_[attribute] += cell_num;

  return Status::Ok();
}

Status Writer::prepare_tiles(
    const std::string& attribute,
    const std::vector<WriteCellRangeVec>& write_cell_ranges,
    std::vector<Tile>* tiles) const {
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
}

Status Writer::prepare_tiles(
    const std::string& attribute,
    const std::vector<uint64_t>& cell_pos,
    std::vector<Tile>* tiles) const {
  return array_schema_->var_size(attribute) ?
             prepare_tiles_var(attribute, cell_pos, tiles) :
             prepare_tiles_fixed(attribute, cell_pos, tiles);
}

Status Writer::prepare_tiles_fixed(
    const std::string& attribute,
    const std::vector<uint64_t>& cell_pos,
    std::vector<Tile>* tiles) const {
  // Trivial case
  if (cell_pos.empty())
    return Status::Ok();

  // For easy reference
  auto it = attr_buffers_.find(attribute);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto cell_num = (uint64_t)cell_pos.size();
  auto capacity = array_schema_->capacity();
  auto tile_num = utils::ceil(cell_num, capacity);
  auto cell_size = array_schema_->cell_size(attribute);

  // Initialize tiles
  tiles->resize(tile_num);
  for (auto& tile : (*tiles))
    RETURN_NOT_OK(init_tile(attribute, &tile));

  // Write all cells one by one
  for (uint64_t i = 0, tile_idx = 0; i < cell_num; ++i) {
    if ((*tiles)[tile_idx].full())
      ++tile_idx;

    RETURN_NOT_OK(
        (*tiles)[tile_idx].write(buffer + cell_pos[i] * cell_size, cell_size));
  }

  return Status::Ok();
}

Status Writer::prepare_tiles_var(
    const std::string& attribute,
    const std::vector<uint64_t>& cell_pos,
    std::vector<Tile>* tiles) const {
  // For easy reference
  auto it = attr_buffers_.find(attribute);
  auto buffer = (uint64_t*)it->second.buffer_;
  auto buffer_var = (unsigned char*)it->second.buffer_var_;
  auto buffer_var_size = it->second.buffer_var_size_;
  auto cell_num = (uint64_t)cell_pos.size();
  auto capacity = array_schema_->capacity();
  auto tile_num = utils::ceil(cell_num, capacity);
  uint64_t offset;
  uint64_t var_size;

  // Initialize tiles
  tiles->resize(2 * tile_num);
  auto tiles_len = tiles->size();
  for (uint64_t i = 0; i < tiles_len; i += 2)
    RETURN_NOT_OK(init_tile(attribute, &((*tiles)[i]), &((*tiles)[i + 1])));

  // Write all cells one by one
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

  return Status::Ok();
}

Status Writer::set_attributes(
    const char** attributes, unsigned int attribute_num) {
  // Get attributes
  std::vector<std::string> attributes_vec;
  if (attributes == nullptr) {  // Default: all attributes
    attributes_vec = array_schema_->attribute_names();
    if ((!array_schema_->dense() || layout_ == Layout::UNORDERED))
      attributes_vec.emplace_back(constants::coords);
  } else {  // Custom attributes
    // Get attributes
    unsigned uri_max_len = constants::uri_max_len;
    for (unsigned int i = 0; i < attribute_num; ++i) {
      // Check attribute name length
      if (attributes[i] == nullptr || strlen(attributes[i]) > uri_max_len)
        return LOG_STATUS(Status::WriterError("Invalid attribute name length"));
      attributes_vec.emplace_back(attributes[i]);
    }

    // Sanity check on duplicates
    if (utils::has_duplicates(attributes_vec))
      return LOG_STATUS(
          Status::WriterError("Cannot set attributes; Duplicate attributes"));
  }

  // Set attribute names
  for (const auto& attr : attributes_vec)
    attributes_.emplace_back(attr);

  return Status::Ok();
}

template <class T>
Status Writer::sort_coords(std::vector<uint64_t>* cell_pos) const {
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
  std::sort(cell_pos->begin(), cell_pos->end(), GlobalCmp<T>(domain, buffer));

  return Status::Ok();
}

Status Writer::unordered_write() {
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
}

template <class T>
Status Writer::unordered_write() {
  // Sort coordinates first
  std::vector<uint64_t> cell_pos;
  RETURN_NOT_OK(sort_coords<T>(&cell_pos));

  // Create new fragment
  std::shared_ptr<FragmentMetadata> frag_meta;
  RETURN_NOT_OK(create_fragment(false, &frag_meta));
  auto uri = frag_meta->fragment_uri();

  // Prepare tiles for all attributes and write
  for (const auto& attr : attributes_) {
    std::vector<Tile> tiles;
    RETURN_NOT_OK_ELSE(
        prepare_tiles(attr, cell_pos, &tiles),
        storage_manager_->vfs()->remove_dir(uri));
    if (attr == constants::coords)
      RETURN_NOT_OK_ELSE(
          compute_coords_metadata<T>(tiles, frag_meta.get()),
          storage_manager_->vfs()->remove_dir(uri));
    RETURN_NOT_OK_ELSE(
        write_tiles(attr, frag_meta.get(), tiles),
        storage_manager_->vfs()->remove_dir(uri));
  }

  // Write the fragment metadata
  RETURN_NOT_OK_ELSE(
      storage_manager_->store_fragment_metadata(frag_meta.get()),
      storage_manager_->vfs()->remove_dir(uri));

  return Status::Ok();
}

Status Writer::write_empty_cell_range_to_tile(uint64_t num, Tile* tile) const {
  auto type = tile->type();
  auto fill_size = datatype_size(type);
  auto fill_value = utils::fill_value(type);
  assert(fill_value != nullptr);

  for (uint64_t i = 0; i < num; ++i)
    RETURN_NOT_OK(tile->write(fill_value, fill_size));

  return Status::Ok();
}

Status Writer::write_empty_cell_range_to_tile_var(
    uint64_t num, Tile* tile, Tile* tile_var) const {
  auto type = tile_var->type();
  auto fill_size = datatype_size(type);
  auto fill_value = utils::fill_value(type);
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

Status Writer::write_tiles(
    const std::string& attribute,
    FragmentMetadata* frag_meta,
    std::vector<Tile>& tiles) const {
  // Handle zero tiles
  if (tiles.empty())
    return Status::Ok();

  // For easy reference
  auto var_size = array_schema_->var_size(attribute);

  // Prepare TileIO
  auto tile_io = std::make_shared<TileIO>(
      storage_manager_, frag_meta->attr_uri(attribute));
  auto tile_io_var =
      (!var_size) ? nullptr :
                    std::make_shared<TileIO>(
                        storage_manager_, frag_meta->attr_var_uri(attribute));

  // Write tiles
  auto tile_num = tiles.size();
  uint64_t bytes_written, bytes_written_var;
  for (size_t i = 0; i < tile_num; ++i) {
    RETURN_NOT_OK(tile_io->write(&(tiles[i]), &bytes_written));
    frag_meta->append_tile_offset(attribute, bytes_written);

    if (var_size) {
      ++i;
      RETURN_NOT_OK(tile_io_var->write(&(tiles[i]), &bytes_written_var));
      frag_meta->append_tile_var_offset(attribute, bytes_written_var);
      frag_meta->append_tile_var_size(attribute, tiles[i].size());
    }
  }

  // Close files, except in the case of global order
  if (layout_ != Layout::GLOBAL_ORDER) {
    RETURN_NOT_OK(storage_manager_->close_file(frag_meta->attr_uri(attribute)));
    if (var_size)
      RETURN_NOT_OK(
          storage_manager_->close_file(frag_meta->attr_var_uri(attribute)));
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
