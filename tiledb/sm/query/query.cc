/**
 * @file   query.cc
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
 * This file implements class Query.
 */

#include "tiledb/sm/query/query.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"

#include <cassert>
#include <set>
#include <sstream>

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Query::Query() {
  common_query_ = nullptr;
  subarray_ = nullptr;
  array_read_state_ = nullptr;
  array_ordered_read_state_ = nullptr;
  array_ordered_write_state_ = nullptr;
  array_schema_ = nullptr;
  buffers_ = nullptr;
  buffer_sizes_ = nullptr;
  callback_ = nullptr;
  callback_data_ = nullptr;
  fragments_init_ = false;
  storage_manager_ = nullptr;
  fragments_borrowed_ = false;
  consolidation_fragment_uri_ = URI();
  status_ = QueryStatus::INPROGRESS;
  layout_ = Layout::ROW_MAJOR;
  buffer_num_ = 0;
}

Query::Query(Query* common_query) {
  common_query_ = common_query;
  subarray_ = nullptr;
  array_read_state_ = nullptr;
  array_ordered_read_state_ = nullptr;
  array_ordered_write_state_ = nullptr;
  callback_ = nullptr;
  callback_data_ = nullptr;
  fragments_init_ = false;
  storage_manager_ = common_query->storage_manager();
  fragments_borrowed_ = false;
  array_schema_ = common_query->array_schema();
  type_ = common_query->type();
  layout_ = common_query->layout();
  status_ = QueryStatus::INPROGRESS;
  consolidation_fragment_uri_ = common_query->consolidation_fragment_uri_;
  buffer_num_ = common_query->buffer_num_;
}

Query::~Query() {
  if (subarray_ != nullptr)
    std::free(subarray_);

  delete array_read_state_;
  delete array_ordered_read_state_;
  delete array_ordered_write_state_;

  clear_fragments();
}

/* ****************************** */
/*               API              */
/* ****************************** */

const ArraySchema* Query::array_schema() const {
  return array_schema_;
}

Status Query::async_process() {
  // In case this query follows another one (the common query)
  if (common_query_ != nullptr) {
    fragment_metadata_ = common_query_->fragment_metadata();
    fragments_ = common_query_->fragments();
    fragments_init_ = true;
    fragments_borrowed_ = true;
  }

  // Initialize fragments
  if (!fragments_init_) {
    RETURN_NOT_OK(init_fragments(fragment_metadata_));
    RETURN_NOT_OK(init_states());
  }

  Status st;
  if (type_ == QueryType::READ)
    st = read();
  else  // WRITE MODE
    st = write();

  if (st.ok()) {  // Success
    // Check for overflow (applicable only to reads)
    if (type_ == QueryType::READ &&
        ((layout_ == Layout::GLOBAL_ORDER && array_read_state_->overflow()) ||
         ((layout_ == Layout::COL_MAJOR || layout_ == Layout::ROW_MAJOR) &&
          array_ordered_read_state_->overflow())))
      set_status(QueryStatus::INCOMPLETE);
    else  // Completion
      set_status(QueryStatus::COMPLETED);

    // Invoke the callback
    if (callback_ != nullptr)
      callback_(callback_data_);
  } else {  // Error
    set_status(QueryStatus::FAILED);
  }

  return st;
}

const std::vector<unsigned int>& Query::attribute_ids() const {
  return attribute_ids_;
}

Status Query::clear_fragments() {
  Status ret = Status::Ok();
  if (!fragments_borrowed_) {
    for (auto& fragment : fragments_) {
      auto st = fragment->finalize();
      if (!st.ok() && ret.ok())
        ret = st;
      delete fragment;
    }
  }
  fragments_.clear();

  return ret;
}

Status Query::coords_buffer_i(int* coords_buffer_i) const {
  int buffer_i = 0;
  auto attribute_id_num = attribute_ids_.size();
  auto attribute_num = array_schema_->attribute_num();
  for (size_t i = 0; i < attribute_id_num; ++i) {
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

Status Query::compute_subarrays(
    void* subarray, std::vector<void*>* subarrays) const {
  // Prepare subarray
  auto domain = array_schema_->domain();
  uint64_t subarray_size = 2 * array_schema_->coords_size();
  void* first_subarray = std::malloc(subarray_size);
  if (first_subarray == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot compute subarrays; Failed to allocate memory"));
  auto to_copy = (subarray != nullptr) ? subarray : domain->domain();
  std::memcpy(first_subarray, to_copy, subarray_size);

  // Prepare buffer sizes
  std::vector<uint64_t> max_buffer_sizes;
  max_buffer_sizes.resize(buffer_num_);

  // Compute subarrays
  std::list<void*> my_subarrays;
  my_subarrays.emplace_back(first_subarray);
  auto it = my_subarrays.begin();
  Status st = Status::Ok();
  do {
    // Compute max buffer sizes for the current subarray
    st = storage_manager_->array_compute_max_read_buffer_sizes(
        array_schema_,
        fragment_metadata_,
        *it,
        attribute_ids_,
        &max_buffer_sizes[0],
        buffer_num_);
    if (!st.ok())
      break;

    // Handle case of no results
    auto no_results = true;
    for (unsigned i = 0; i < buffer_num_; ++i) {
      if (max_buffer_sizes[i] != 0) {
        no_results = false;
        break;
      }
    }
    if (no_results) {
      std::free(*it);
      it = my_subarrays.erase(it);
      continue;
    }

    // Handle case of split
    auto must_split = false;
    for (unsigned i = 0; i < buffer_num_; ++i) {
      if (max_buffer_sizes[i] > buffer_sizes_[i]) {
        must_split = true;
        break;
      }
    }
    if (must_split) {
      void *subarray_1 = nullptr, *subarray_2 = nullptr;
      st = domain->split_subarray(*it, layout_, &subarray_1, &subarray_2);
      if (!st.ok())
        break;
      if (subarray_1 == nullptr || subarray_2 == nullptr) {
        st = LOG_STATUS(Status::QueryError(
            "Cannot compute subarrays; Subarray is not splittable"));
        break;
      }
      my_subarrays.insert(std::next(it), subarray_2);
      my_subarrays.insert(std::next(it), subarray_1);
      it = my_subarrays.erase(it);
      continue;
    }

    ++it;
  } while (it != my_subarrays.end());

  // Clean up upon error
  if (!st.ok()) {
    for (auto s : my_subarrays)
      std::free(s);
    return st;
  }

  // Prepare the result
  for (auto s : my_subarrays)
    subarrays->emplace_back(s);

  return Status::Ok();
}

Status Query::finalize() {
  // Clear sorted read state
  if (array_ordered_read_state_ != nullptr)
    RETURN_NOT_OK(array_ordered_read_state_->finalize());
  delete array_ordered_read_state_;
  array_ordered_read_state_ = nullptr;

  // Clear sorted write state
  if (array_ordered_write_state_ != nullptr)
    RETURN_NOT_OK(array_ordered_write_state_->finalize());
  delete array_ordered_write_state_;
  array_ordered_write_state_ = nullptr;

  // Clear fragments
  return clear_fragments();
}

const std::vector<Fragment*>& Query::fragments() const {
  return fragments_;
}

const std::vector<FragmentMetadata*>& Query::fragment_metadata() const {
  return fragment_metadata_;
}

std::vector<URI> Query::fragment_uris() const {
  std::vector<URI> uris;
  for (auto fragment : fragments_)
    uris.emplace_back(fragment->fragment_uri());

  return uris;
}

unsigned int Query::fragment_num() const {
  return (unsigned int)fragments_.size();
}

Status Query::init() {
  // Sanity checks
  if (storage_manager_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot initialize query; Storage manager not set"));
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot initialize query; Array metadata not set"));
  if (buffers_ == nullptr || buffer_sizes_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot initialize query; Buffers not set"));
  if (attribute_ids_.empty())
    return LOG_STATUS(
        Status::QueryError("Cannot initialize query; Attributes not set"));

  status_ = QueryStatus::INPROGRESS;

  if (subarray_ == nullptr)
    RETURN_NOT_OK(set_subarray(nullptr));

  RETURN_NOT_OK(check_buffer_sizes_ordered());

  RETURN_NOT_OK(init_fragments(fragment_metadata_));
  RETURN_NOT_OK(init_states());

  return Status::Ok();
}

Status Query::init(
    StorageManager* storage_manager,
    const ArraySchema* array_schema,
    const std::vector<FragmentMetadata*>& fragment_metadata,
    QueryType type,
    Layout layout,
    const void* subarray,
    const char** attributes,
    unsigned int attribute_num,
    void** buffers,
    uint64_t* buffer_sizes,
    const URI& consolidation_fragment_uri) {
  storage_manager_ = storage_manager;
  array_schema_ = array_schema;
  type_ = type;
  layout_ = layout;
  status_ = QueryStatus::INPROGRESS;
  buffers_ = buffers;
  buffer_sizes_ = buffer_sizes;
  fragment_metadata_ = fragment_metadata;
  consolidation_fragment_uri_ = consolidation_fragment_uri;

  RETURN_NOT_OK(set_attributes(attributes, attribute_num));
  RETURN_NOT_OK(array_schema->buffer_num(attribute_ids_, &buffer_num_));
  RETURN_NOT_OK(set_subarray(subarray));
  RETURN_NOT_OK(init_fragments(fragment_metadata_));

  return Status::Ok();
}

Status Query::init(
    StorageManager* storage_manager,
    const ArraySchema* array_schema,
    const std::vector<FragmentMetadata*>& fragment_metadata,
    QueryType type,
    Layout layout,
    const void* subarray,
    const std::vector<unsigned int>& attribute_ids,
    void** buffers,
    uint64_t* buffer_sizes,
    bool add_coords) {
  storage_manager_ = storage_manager;
  array_schema_ = array_schema;
  type_ = type;
  layout_ = layout;
  attribute_ids_ = attribute_ids;
  status_ = QueryStatus::INPROGRESS;
  buffers_ = buffers;
  buffer_sizes_ = buffer_sizes;
  fragment_metadata_ = fragment_metadata;

  RETURN_NOT_OK(array_schema->buffer_num(attribute_ids, &buffer_num_));

  if (add_coords)
    this->add_coords();

  RETURN_NOT_OK(set_subarray(subarray));

  return Status::Ok();
}

URI Query::last_fragment_uri() const {
  if (fragments_.empty())
    return URI();
  return fragments_.back()->fragment_uri();
}

Layout Query::layout() const {
  return layout_;
}

bool Query::overflow() const {
  // Not applicable to writes
  if (type_ != QueryType::READ)
    return false;

  // Check overflow
  if (array_ordered_read_state_ != nullptr)
    return array_ordered_read_state_->overflow();

  return array_read_state_->overflow();
}

bool Query::overflow(unsigned int attribute_id) const {
  assert(type_ == QueryType::READ);

  // Trivial case
  if (fragments_.empty())
    return false;

  // Check overflow
  if (array_ordered_read_state_ != nullptr)
    return array_ordered_read_state_->overflow(attribute_id);

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

Status Query::sparse_read() {
  auto coords_type = array_schema_->coords_type();
  switch (coords_type) {
    case Datatype::INT8:
      return sparse_read<int8_t>();
    case Datatype::UINT8:
      return sparse_read<uint8_t>();
    case Datatype::INT16:
      return sparse_read<int16_t>();
    case Datatype::UINT16:
      return sparse_read<uint16_t>();
    case Datatype::INT32:
      return sparse_read<int>();
    case Datatype::UINT32:
      return sparse_read<unsigned>();
    case Datatype::INT64:
      return sparse_read<int64_t>();
    case Datatype::UINT64:
      return sparse_read<uint64_t>();
    case Datatype::FLOAT32:
      return sparse_read<float>();
    case Datatype::FLOAT64:
      return sparse_read<double>();
    default:
      return LOG_STATUS(
          Status::SparseReaderError("Cannot read; Unsupported domain type"));
  }

  return Status::Ok();
}

template <class T>
Status Query::sparse_read() {
  status_ = QueryStatus::INPROGRESS;

  // Get overlapping tile indexes
  OverlappingTileVec tiles;
  RETURN_NOT_OK(compute_overlapping_tiles<T>(&tiles));

  // Read tiles
  RETURN_NOT_OK(read_tiles(constants::coords, &tiles));
  for (const auto& attr : attributes_) {
    if (attr != constants::coords)
      RETURN_NOT_OK(read_tiles(attr, &tiles));
  }

  // Compute the read coordinates for all fragments
  std::list<std::shared_ptr<OverlappingCoords<T>>> coords;
  RETURN_NOT_OK(compute_overlapping_coords<T>(tiles, &coords));

  // Sort and dedup the coordinates (not applicable to the global order
  // layout for a single fragment)
  if (!(fragment_metadata_.size() == 1 && layout_ == Layout::GLOBAL_ORDER)) {
    RETURN_NOT_OK(sort_coords<T>(&coords));
    RETURN_NOT_OK(dedup_coords<T>(&coords));
  }

  // Compute the maximal cell ranges
  OverlappingCellRangeList cell_ranges;
  RETURN_NOT_OK(compute_cell_ranges(coords, &cell_ranges));
  coords.clear();

  // Copy cells
  for (const auto& attr : attributes_)
    RETURN_NOT_OK(copy_cells(attr, cell_ranges));

  status_ = QueryStatus::COMPLETED;
  return Status::Ok();
}

template <class T>
Status Query::compute_overlapping_tiles(OverlappingTileVec* tiles) const {
  // For easy reference
  auto subarray = (T*)subarray_;
  auto dim_num = array_schema_->dim_num();
  auto fragment_num = fragment_metadata_.size();
  bool full_overlap;

  // Find overlapping tile indexes for each fragment
  tiles->clear();
  for (unsigned i = 0; i < fragment_num; ++i) {
    auto mbrs = fragment_metadata_[i]->mbrs();
    auto mbr_num = (uint64_t)mbrs.size();
    for (uint64_t j = 0; j < mbr_num; ++j) {
      if (overlap(&subarray[0], (const T*)(mbrs[j]), dim_num, &full_overlap)) {
        auto tile = std::make_shared<OverlappingTile>(i, j, full_overlap);
        tiles->emplace_back(tile);
      }
    }
  }

  return Status::Ok();
}

Status Query::read_tiles(
    const std::string& attr_name, OverlappingTileVec* tiles) const {
  // Prepare tile IO
  unsigned attr_id;
  RETURN_NOT_OK(array_schema_->attribute_id(attr_name, &attr_id));
  auto var_size = array_schema_->var_size(attr_id);
  std::vector<std::shared_ptr<TileIO>> tile_io;
  std::vector<std::shared_ptr<TileIO>> tile_io_var;
  for (const auto& f : fragment_metadata_) {
    tile_io.emplace_back(std::make_shared<TileIO>(
        storage_manager_, f->attr_uri(attr_id), f->file_sizes(attr_id)));
    if (var_size)
      tile_io_var.emplace_back(std::make_shared<TileIO>(
          storage_manager_,
          f->attr_var_uri(attr_id),
          f->file_var_sizes(attr_id)));
    else
      tile_io_var.emplace_back();
  }
  bool is_coords = (attr_id == array_schema_->attribute_num());

  // For each fragment, read the tiles
  for (auto& tile : *tiles) {
    auto& tile_pair = tile->attr_tiles_[attr_name];

    // Fixed-sized or offsets tile
    auto t = std::make_shared<Tile>();
    RETURN_NOT_OK(t->init(
        (var_size) ? constants::cell_var_offset_type :
                     array_schema_->type(attr_id),
        (var_size) ? array_schema_->cell_var_offsets_compression() :
                     array_schema_->compression(attr_id),
        (var_size) ? constants::cell_var_offset_size :
                     array_schema_->cell_size(attr_id),
        (is_coords) ? array_schema_->dim_num() : 0));
    RETURN_NOT_OK(tile_io[tile->fragment_idx_]->read(
        t.get(),
        fragment_metadata_[tile->fragment_idx_]->file_offset(
            attr_id, tile->tile_idx_),
        fragment_metadata_[tile->fragment_idx_]->compressed_tile_size(
            attr_id, tile->tile_idx_),
        fragment_metadata_[tile->fragment_idx_]->tile_size(
            attr_id, tile->tile_idx_)));
    tile_pair.first = t;

    // Var-sized tile
    if (var_size) {
      auto t_var = std::make_shared<Tile>();
      RETURN_NOT_OK(t_var->init(
          array_schema_->type(attr_id),
          array_schema_->compression(attr_id),
          datatype_size(array_schema_->type(attr_id)),
          0));
      RETURN_NOT_OK(tile_io_var[tile->fragment_idx_]->read(
          t_var.get(),
          fragment_metadata_[tile->fragment_idx_]->file_var_offset(
              attr_id, tile->tile_idx_),
          fragment_metadata_[tile->fragment_idx_]->compressed_tile_var_size(
              attr_id, tile->tile_idx_),
          fragment_metadata_[tile->fragment_idx_]->tile_var_size(
              attr_id, tile->tile_idx_)));
      tile_pair.second = t_var;
    }
  }

  return Status::Ok();
}

template <class T>
Status Query::compute_overlapping_coords(
    const OverlappingTileVec& tiles,
    std::list<std::shared_ptr<OverlappingCoords<T>>>* coords) const {
  for (const auto& tile : tiles) {
    std::list<std::shared_ptr<OverlappingCoords<T>>> tile_coords;
    if (tile.get()->full_overlap_) {
      RETURN_NOT_OK(get_all_coords<T>(tile, &tile_coords));
    } else {
      RETURN_NOT_OK(compute_overlapping_coords<T>(tile, &tile_coords));
    }
    coords->splice(coords->end(), tile_coords);
  }

  return Status::Ok();
}

template <class T>
Status Query::compute_overlapping_coords(
    const std::shared_ptr<OverlappingTile>& tile,
    std::list<std::shared_ptr<OverlappingCoords<T>>>* coords) const {
  auto dim_num = array_schema_->dim_num();
  const auto t = tile->attr_tiles_.find(constants::coords)->second.first;
  auto t_ptr = t.get();
  auto coords_num = t_ptr->cell_num();
  auto subarray = (T*)subarray_;
  auto c = (T*)t_ptr->data();

  for (uint64_t i = 0, pos = 0; i < coords_num; ++i, pos += dim_num) {
    if (utils::coords_in_rect<T>(&c[pos], &subarray[0], dim_num))
      coords->emplace_back(
          std::make_shared<OverlappingCoords<T>>(tile, &c[pos], i));
  }

  return Status::Ok();
}

template <class T>
Status Query::get_all_coords(
    const std::shared_ptr<OverlappingTile>& tile,
    std::list<std::shared_ptr<OverlappingCoords<T>>>* coords) const {
  auto dim_num = array_schema_->dim_num();
  const auto& t = tile->attr_tiles_.find(constants::coords)->second.first;
  auto t_ptr = t.get();
  auto coords_num = t_ptr->cell_num();
  auto c = (T*)t_ptr->data();

  for (uint64_t i = 0; i < coords_num; ++i)
    coords->emplace_back(
        std::make_shared<OverlappingCoords<T>>(tile, &c[i * dim_num], i));

  return Status::Ok();
}

template <class T>
Status Query::sort_coords(
    std::list<std::shared_ptr<OverlappingCoords<T>>>* coords) const {
  if (layout_ == Layout::GLOBAL_ORDER) {
    auto domain = array_schema_->domain();
    coords->sort(GlobalCmp<T>(domain));
  } else {
    auto dim_num = array_schema_->dim_num();
    if (layout_ == Layout::ROW_MAJOR)
      coords->sort(RowCmp<T>(dim_num));
    else if (layout_ == Layout::COL_MAJOR)
      coords->sort(ColCmp<T>(dim_num));
  }

  return Status::Ok();
}

template <class T>
Status Query::dedup_coords(
    std::list<std::shared_ptr<OverlappingCoords<T>>>* coords) const {
  auto coords_size = array_schema_->coords_size();
  auto it = coords->begin();
  while (it != coords->end()) {
    auto next_it = std::next(it);
    if (next_it != coords->end() &&
        !std::memcmp(
            it->get()->coords_, next_it->get()->coords_, coords_size)) {
      if (it->get()->tile_.get()->fragment_idx_ <
          next_it->get()->tile_.get()->fragment_idx_) {
        it = coords->erase(it);
      } else {
        coords->erase(next_it);
        continue;
      }
    }
    ++it;
  }
  return Status::Ok();
}

template <class T>
Status Query::compute_cell_ranges(
    const std::list<std::shared_ptr<OverlappingCoords<T>>>& coords,
    OverlappingCellRangeList* cell_ranges) const {
  // Trivial case
  auto coords_num = (uint64_t)coords.size();
  if (coords_num == 0)
    return Status::Ok();

  // Initialize the first range
  auto it = coords.begin();
  uint64_t start_pos = it->get()->pos_;
  uint64_t end_pos = start_pos;
  auto tile = it->get()->tile_;

  // Scan the coordinates and compute ranges
  for (++it; it != coords.end(); ++it) {
    if (it->get()->tile_.get() == tile.get() &&
        it->get()->pos_ == end_pos + 1) {
      // Same range - advance end position
      end_pos = it->get()->pos_;
    } else {
      // New range - append previous range
      cell_ranges->emplace_back(
          std::make_shared<OverlappingCellRange>(tile, start_pos, end_pos));
      start_pos = it->get()->pos_;
      end_pos = start_pos;
      tile = it->get()->tile_;
    }
  }

  // Append the last range
  cell_ranges->emplace_back(
      std::make_shared<OverlappingCellRange>(tile, start_pos, end_pos));

  return Status::Ok();
}

template <class T>
bool Query::overlap(
    const T* a, const T* b, unsigned dim_num, bool* a_contains_b) const {
  for (unsigned i = 0; i < dim_num; ++i) {
    if (a[2 * i] > b[2 * i + 1] || a[2 * i + 1] < b[2 * i])
      return false;
  }

  *a_contains_b = true;
  for (unsigned i = 0; i < dim_num; ++i) {
    if (a[2 * i] > b[2 * i] || a[2 * i + 1] < b[2 * i + 1]) {
      *a_contains_b = false;
      break;
    }
  }

  return true;
}

Status Query::copy_cells(
    const std::string& attribute,
    const OverlappingCellRangeList& cell_ranges) const {
  unsigned attr_id;
  RETURN_NOT_OK(array_schema_->attribute_id(attribute, &attr_id));
  if (array_schema_->var_size(attr_id))
    return copy_var_cells(attribute, cell_ranges);
  return copy_fixed_cells(attribute, cell_ranges);
}

Status Query::buffer_idx(const std::string& attribute, unsigned* bid) const {
  unsigned attr_id;
  *bid = 0;
  for (const auto& a : attributes_) {
    if (a == attribute)
      return Status::Ok();
    RETURN_NOT_OK(array_schema_->attribute_id(a, &attr_id));
    *bid += 1 + (unsigned)array_schema_->var_size(attr_id);
  }

  return Status::QueryError(
      std::string("Cannot retrieve buffer index; Invalid attribute '") +
      attribute + "'");
}

Status Query::copy_fixed_cells(
    const std::string& attribute,
    const OverlappingCellRangeList& cell_ranges) const {
  // For easy reference
  unsigned attr_id, bid;
  RETURN_NOT_OK(array_schema_->attribute_id(attribute, &attr_id));
  RETURN_NOT_OK(buffer_idx(attribute, &bid));
  auto buffer = (unsigned char*)buffers_[bid];
  uint64_t buffer_offset = 0;
  auto cell_size = array_schema_->cell_size(attr_id);

  // Copy cells
  for (const auto& cr : cell_ranges) {
    const auto& tile = cr->tile_->attr_tiles_.find(attribute)->second.first;
    auto data = (unsigned char*)tile->data();
    auto bytes_to_copy = (cr->end_ - cr->start_ + 1) * cell_size;
    if (buffer_offset + bytes_to_copy > buffer_sizes_[bid])
      return LOG_STATUS(Status::QueryError(
          std::string("Cannot copy cells for attribute '") + attribute +
          "'; Result buffer overflowed"));
    std::memcpy(
        buffer + buffer_offset,
        data + cr.get()->start_ * cell_size,
        bytes_to_copy);
    buffer_offset += bytes_to_copy;
  }

  // Update buffer sizes
  buffer_sizes_[bid] = buffer_offset;

  return Status::Ok();
}

Status Query::copy_var_cells(
    const std::string& attribute,
    const OverlappingCellRangeList& cell_ranges) const {
  // For easy reference
  unsigned attr_id, bid;
  RETURN_NOT_OK(array_schema_->attribute_id(attribute, &attr_id));
  RETURN_NOT_OK(buffer_idx(attribute, &bid));
  auto buffer = (unsigned char*)buffers_[bid];
  auto buffer_var = (unsigned char*)buffers_[bid + 1];
  uint64_t buffer_offset = 0;
  uint64_t buffer_var_offset = 0;
  uint64_t offset_size = constants::cell_var_offset_size;
  uint64_t cell_var_size;

  // Copy cells
  for (const auto& cr : cell_ranges) {
    const auto& tile_pair = cr->tile_->attr_tiles_.find(attribute)->second;
    const auto& tile = tile_pair.first;
    const auto& tile_var = tile_pair.second;
    const auto offsets = (uint64_t*)tile->data();
    auto data = (unsigned char*)tile_var->data();
    auto cell_num = tile->cell_num();
    auto tile_var_size = tile_var->size();

    for (uint64_t i = cr->start_; i <= cr->end_; ++i) {
      // Copy offsets
      if (buffer_offset + offset_size > buffer_sizes_[bid])
        return LOG_STATUS(Status::QueryError(
            std::string("Cannot copy cell offsets for var-sized attribute '") +
            attribute + "'; Result buffer overflowed"));
      std::memcpy(buffer + buffer_offset, &buffer_var_offset, offset_size);
      buffer_offset += offset_size;

      // Copy values
      cell_var_size = (i != cell_num - 1) ?
                          offsets[i + 1] - offsets[i] :
                          tile_var_size - (offsets[i] - offsets[0]);

      if (buffer_var_offset + cell_var_size > buffer_sizes_[bid + 1])
        return LOG_STATUS(Status::QueryError(
            std::string("Cannot copy cell data for var-sized attribute '") +
            attribute + "'; Result buffer overflowed"));

      std::memcpy(
          buffer_var + buffer_var_offset,
          &data[offsets[i] - offsets[0]],
          cell_var_size);
      buffer_var_offset += cell_var_size;
    }
  }

  // Update buffer sizes
  buffer_sizes_[bid] = buffer_offset;
  buffer_sizes_[bid + 1] = buffer_var_offset;

  return Status::Ok();
}

Status Query::read() {
  if (!array_schema_->dense())
    return sparse_read();

  // Check attributes
  RETURN_NOT_OK(check_attributes());

  // Zero-out all buffers
  zero_out_buffers();

  // Handle case of no fragments
  if (fragments_.empty()) {
    zero_out_buffer_sizes(buffer_sizes_);
    status_ = QueryStatus::COMPLETED;
    return Status::Ok();
  }

  status_ = QueryStatus::INPROGRESS;

  // Perform query
  Status st;
  if (layout_ == Layout::COL_MAJOR || layout_ == Layout::ROW_MAJOR)
    st = array_ordered_read_state_->read(buffers_, buffer_sizes_);
  else  // layout = Layout::GLOBAL_ORDER
    st = array_read_state_->read(buffers_, buffer_sizes_);

  // Set query status
  if (st.ok()) {
    if (overflow())
      status_ = QueryStatus::INCOMPLETE;
    else
      status_ = QueryStatus::COMPLETED;
  } else {
    status_ = QueryStatus::FAILED;
  }

  return st;
}

Status Query::read(void** buffers, uint64_t* buffer_sizes) {
  // Handle case of no fragments
  if (fragments_.empty()) {
    zero_out_buffer_sizes(buffer_sizes_);
    return Status::Ok();
  }

  return array_read_state_->read(buffers, buffer_sizes);
}

void Query::set_array_schema(const ArraySchema* array_schema) {
  array_schema_ = array_schema;
  if (array_schema->is_kv())
    layout_ =
        (type_ == QueryType::WRITE) ? Layout::UNORDERED : Layout::GLOBAL_ORDER;
}

Status Query::set_buffers(
    const char** attributes,
    unsigned int attribute_num,
    void** buffers,
    uint64_t* buffer_sizes) {
  // Sanity checks
  if (attributes == nullptr && attribute_num == 0)
    return LOG_STATUS(
        Status::QueryError("Cannot set buffers; no attributes provided"));

  if (buffers == nullptr || buffer_sizes == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set buffers; Buffers not provided"));

  // Get attributes
  std::vector<std::string> attributes_vec;
  for (unsigned int i = 0; i < attribute_num; ++i) {
    // Check attribute name length
    if (attributes[i] == nullptr)
      return LOG_STATUS(
          Status::QueryError("Cannot set buffers; Attributes cannot be null"));
    attributes_vec.emplace_back(attributes[i]);
  }

  // Sanity check on duplicates
  if (utils::has_duplicates(attributes_vec))
    return LOG_STATUS(
        Status::QueryError("Cannot set buffers; Duplicate attributes given"));

  // Set attribute ids
  RETURN_NOT_OK(
      array_schema_->get_attribute_ids(attributes_vec, attribute_ids_));

  // Set attribute names
  for (unsigned i = 0; i < attribute_num; ++i)
    attributes_.emplace_back(attributes[i]);

  // Set buffers and buffer sizes
  buffers_ = buffers;
  buffer_sizes_ = buffer_sizes;

  return Status::Ok();
}

void Query::set_buffers(void** buffers, uint64_t* buffer_sizes) {
  buffers_ = buffers;
  buffer_sizes_ = buffer_sizes;
}

void Query::set_callback(
    const std::function<void(void*)>& callback, void* callback_data) {
  callback_ = callback;
  callback_data_ = callback_data;
}

Status Query::set_fragment_metadata(
    const std::vector<FragmentMetadata*>& fragment_metadata) {
  fragment_metadata_ = fragment_metadata;
  return Status::Ok();
}

Status Query::set_layout(Layout layout) {
  // Check if the array is a key-value store
  if (array_schema_->is_kv())
    return LOG_STATUS(Status::QueryError(
        "Cannot set layout; The array is defined as a key-value store"));

  // Ordered layout for writes in sparse arrays is meaningless
  if (type_ == QueryType::WRITE && !array_schema_->dense() &&
      (layout == Layout::COL_MAJOR || layout == Layout::ROW_MAJOR))
    return LOG_STATUS(Status::QueryError(
        "Cannot set layout; Ordered layouts can be used when writing to sparse "
        "arrays - use UNORDERED instead"));

  // Layout for 1D vectors should not be col-major
  // Use the equivalent row-major
  if (array_schema_->dim_num() == 1 && layout == Layout::COL_MAJOR)
    layout_ = Layout::ROW_MAJOR;
  else
    layout_ = layout;

  return Status::Ok();
}

void Query::set_status(QueryStatus status) {
  status_ = status;
}

void Query::set_storage_manager(StorageManager* storage_manager) {
  storage_manager_ = storage_manager;
}

Status Query::set_subarray(const void* subarray) {
  RETURN_NOT_OK(check_subarray(subarray));

  uint64_t subarray_size = 2 * array_schema_->coords_size();

  if (subarray_ == nullptr)
    subarray_ = std::malloc(subarray_size);

  if (subarray_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Memory allocation for subarray failed"));

  if (subarray == nullptr)
    std::memcpy(subarray_, array_schema_->domain()->domain(), subarray_size);
  else
    std::memcpy(subarray_, subarray, subarray_size);

  return Status::Ok();
}

void Query::set_type(QueryType type) {
  type_ = type;
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
  // Check attributes
  RETURN_NOT_OK(check_attributes());

  // Set query status
  status_ = QueryStatus::INPROGRESS;

  // Write based on mode
  if (layout_ == Layout::COL_MAJOR || layout_ == Layout::ROW_MAJOR) {
    RETURN_NOT_OK(array_ordered_write_state_->write(buffers_, buffer_sizes_));
  } else if (layout_ == Layout::GLOBAL_ORDER || layout_ == Layout::UNORDERED) {
    RETURN_NOT_OK(write(buffers_, buffer_sizes_));
  } else {
    assert(0);
  }

  // In all types except WRITE with GLOBAL ORDER, the fragment must be finalized
  if (!(type_ == QueryType::WRITE && layout_ == Layout::GLOBAL_ORDER))
    clear_fragments();

  status_ = QueryStatus::COMPLETED;

  return Status::Ok();
}

Status Query::write(void** buffers, uint64_t* buffer_sizes) {
  // Sanity checks
  if (type_ != QueryType::WRITE) {
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

void Query::add_coords() {
  unsigned int attribute_num = array_schema_->attribute_num();
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

Status Query::check_attributes() {
  // If it is a write query, there should be no duplicate attributes
  if (type_ == QueryType::WRITE) {
    std::set<unsigned int> unique_attribute_ids;
    for (auto& id : attribute_ids_)
      unique_attribute_ids.insert(id);
    if (unique_attribute_ids.size() != attribute_ids_.size())
      return LOG_STATUS(
          Status::QueryError("Check attributes failed; Duplicate attributes "
                             "set for a write query"));
  }

  // If it is an unordered write query, all attributes must be provided
  if (type_ == QueryType::WRITE && layout_ == Layout::UNORDERED) {
    if (attribute_ids_.size() != array_schema_->attribute_num() + 1)
      return LOG_STATUS(
          Status::QueryError("Check attributes failed; Unordered writes expect "
                             "all attributes to be set"));
  }

  return Status::Ok();
}

Status Query::check_buffer_sizes_ordered() const {
  if (!array_schema_->dense() || type_ == QueryType::READ ||
      (layout_ != Layout::ROW_MAJOR && layout_ != Layout::COL_MAJOR))
    return Status::Ok();
  auto cell_num = array_schema_->domain()->cell_num(subarray_);
  unsigned bid = 0;
  uint64_t expected_cell_num = 0;
  for (auto& aid : attribute_ids_) {
    bool is_var_attr = array_schema_->var_size(aid);
    if (is_var_attr) {
      expected_cell_num = buffer_sizes_[bid] / constants::cell_var_offset_size;
    } else {
      expected_cell_num = buffer_sizes_[bid] / array_schema_->cell_size(aid);
    }
    if (expected_cell_num != cell_num) {
      std::stringstream ss;
      ss << "Buffer sizes check failed; Invalid number of cells given for ";
      ss << "attribute '" << array_schema_->attribute_name(aid) << "'";
      ss << " (" << expected_cell_num << " != " << cell_num << ")";
      return LOG_STATUS(Status::QueryError(ss.str()));
    }
    if (is_var_attr)
      bid += 2;
    else
      bid += 1;
  }
  return Status::Ok();
}

Status Query::check_subarray(const void* subarray) const {
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
      assert(0);
      break;
  }

  return Status::Ok();
}

template <class T>
Status Query::check_subarray(const T* subarray) const {
  auto domain = array_schema_->domain();
  auto dim_num = domain->dim_num();
  for (unsigned int i = 0; i < dim_num; ++i) {
    auto dim_domain = static_cast<const T*>(domain->dimension(i)->domain());
    if (subarray[2 * i] < dim_domain[0] || subarray[2 * i + 1] > dim_domain[1])
      return LOG_STATUS(Status::QueryError("Subarray out of bounds"));
  }

  return Status::Ok();
}

Status Query::init_fragments(
    const std::vector<FragmentMetadata*>& fragment_metadata) {
  // Do nothing if the fragments are already initialized
  if (fragments_init_)
    return Status::Ok();

  if (type_ == QueryType::WRITE) {
    RETURN_NOT_OK(new_fragment());
  } else if (type_ == QueryType::READ) {
    RETURN_NOT_OK(open_fragments(fragment_metadata));
  }

  fragments_init_ = true;

  return Status::Ok();
}

Status Query::init_states() {
  // Initialize new fragment if needed
  if (type_ == QueryType::WRITE &&
      (layout_ == Layout::COL_MAJOR || layout_ == Layout::ROW_MAJOR)) {
    array_ordered_write_state_ = new ArrayOrderedWriteState(this);
    Status st = array_ordered_write_state_->init();
    if (!st.ok()) {
      delete array_ordered_write_state_;
      array_ordered_write_state_ = nullptr;
      return st;
    }
  } else if (type_ == QueryType::READ && layout_ == Layout::GLOBAL_ORDER) {
    array_read_state_ = new ArrayReadState(this);
  } else if (
      type_ == QueryType::READ &&
      (layout_ == Layout::COL_MAJOR || layout_ == Layout::ROW_MAJOR)) {
    array_read_state_ = new ArrayReadState(this);
    array_ordered_read_state_ = new ArrayOrderedReadState(this);
    Status st = array_ordered_read_state_->init();
    if (!st.ok()) {
      delete array_ordered_read_state_;
      array_ordered_read_state_ = nullptr;
      return st;
    }
  }

  return Status::Ok();
}

Status Query::new_fragment() {
  // Get new fragment name
  auto consolidation = !consolidation_fragment_uri_.is_invalid();
  auto array_name = array_schema_->array_uri().to_string();
  std::string new_fragment_name =
      consolidation ?
          (array_name + "/" + consolidation_fragment_uri_.last_path_part()) :
          this->new_fragment_name();

  if (new_fragment_name.empty())
    return LOG_STATUS(Status::QueryError("Cannot produce new fragment name"));

  // Create new fragment
  auto fragment = new Fragment(this);
  RETURN_NOT_OK_ELSE(
      fragment->init(URI(new_fragment_name), subarray_, consolidation),
      delete fragment);
  fragments_.push_back(fragment);

  return Status::Ok();
}

std::string Query::new_fragment_name() const {
  uint64_t ms = utils::timestamp_ms();
  std::stringstream ss;
  ss << array_schema_->array_uri().to_string() << "/__"
     << std::this_thread::get_id() << "_" << ms;
  return ss.str();
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
    if ((!array_schema_->dense() ||
         (type_ == QueryType::WRITE && layout_ == Layout::UNORDERED)))
      attributes_vec.emplace_back(constants::coords);
  } else {  // Custom attributes
    // Get attributes
    unsigned uri_max_len = constants::uri_max_len;
    for (unsigned int i = 0; i < attribute_num; ++i) {
      // Check attribute name length
      if (attributes[i] == nullptr || strlen(attributes[i]) > uri_max_len)
        return LOG_STATUS(Status::QueryError("Invalid attribute name length"));
      attributes_vec.emplace_back(attributes[i]);
    }

    // Sanity check on duplicates
    if (utils::has_duplicates(attributes_vec))
      return LOG_STATUS(Status::QueryError(
          "Cannot initialize array schema; Duplicate attributes"));
  }

  // Set attribute names
  for (const auto& attr : attributes_vec)
    attributes_.emplace_back(attr);

  // Set attribute ids
  RETURN_NOT_OK(
      array_schema_->get_attribute_ids(attributes_vec, attribute_ids_));

  return Status::Ok();
}

void Query::zero_out_buffer_sizes(uint64_t* buffer_sizes) const {
  unsigned int buffer_i = 0;
  auto attribute_id_num = (unsigned int)attribute_ids_.size();
  for (unsigned int i = 0; i < attribute_id_num; ++i) {
    // Update all sizes to 0
    buffer_sizes[buffer_i] = 0;
    if (!array_schema_->var_size(attribute_ids_[i]))
      ++buffer_i;
    else
      buffer_i += 2;
  }
}

void Query::zero_out_buffers() {
  unsigned int buffer_i = 0;
  auto attribute_id_num = (unsigned int)attribute_ids_.size();
  for (unsigned int i = 0; i < attribute_id_num; ++i) {
    std::memset(buffers_[buffer_i], 0, buffer_sizes_[buffer_i]);
    ++buffer_i;
    if (array_schema_->var_size(attribute_ids_[i])) {
      std::memset(buffers_[buffer_i], 0, buffer_sizes_[buffer_i]);
      ++buffer_i;
    }
  }
}

}  // namespace sm
}  // namespace tiledb
