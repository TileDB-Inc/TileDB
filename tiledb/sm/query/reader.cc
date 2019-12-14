/**
 * @file   reader.cc
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
 * This file implements class Reader.
 */

#include "tiledb/sm/query/reader.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/read_cell_slab_iter.h"
#include "tiledb/sm/query/result_tile.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/subarray/cell_slab.h"
#include "tiledb/sm/tile/tile_io.h"

#include <iostream>

namespace tiledb {
namespace sm {

namespace {
/**
 * If the given iterator points to an "invalid" element, advance it until the
 * pointed-to element is valid, or `end`. Validity is determined by calling
 * `it->valid()`.
 *
 * Example:
 *
 * @code{.cpp}
 * std::vector<T> vec = ...;
 * // Get an iterator to the first valid vec element, or vec.end() if the
 * // vector is empty or only contains invalid elements.
 * auto it = skip_invalid_elements(vec.begin(), vec.end());
 * // If there was a valid element, now advance the iterator to the next
 * // valid element (or vec.end() if there are no more).
 * it = skip_invalid_elements(++it, vec.end());
 * @endcode
 *
 *
 * @tparam IterT The iterator type
 * @param it The iterator
 * @param end The end iterator value
 * @return Iterator pointing to a valid element, or `end`.
 */
template <typename IterT>
inline IterT skip_invalid_elements(IterT it, const IterT& end) {
  while (it != end && !it->valid()) {
    ++it;
  }
  return it;
}
}  // namespace

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Reader::Reader() {
  array_ = nullptr;
  array_schema_ = nullptr;
  storage_manager_ = nullptr;
  layout_ = Layout::ROW_MAJOR;
  sparse_mode_ = false;
  read_state_.initialized_ = false;
}

Reader::~Reader() = default;

/* ****************************** */
/*               API              */
/* ****************************** */

const Array* Reader::array() const {
  return array_;
}

Status Reader::add_range(
    unsigned dim_idx, const void* start, const void* end, const void* stride) {
  if (stride != nullptr)
    return LOG_STATUS(Status::ReaderError(
        "Cannot add range; Setting range stride is currently unsupported"));
  return subarray_.add_range(dim_idx, start, end);
}

Status Reader::get_range_num(unsigned dim_idx, uint64_t* range_num) const {
  return subarray_.get_range_num(dim_idx, range_num);
}

Status Reader::get_range(
    unsigned dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) const {
  *stride = nullptr;
  return subarray_.get_range(dim_idx, range_idx, start, end);
}

Status Reader::get_est_result_size(const char* attr_name, uint64_t* size) {
  return subarray_.get_est_result_size(attr_name, size);
}

Status Reader::get_est_result_size(
    const char* attr_name, uint64_t* size_off, uint64_t* size_val) {
  return subarray_.get_est_result_size(attr_name, size_off, size_val);
}

const ArraySchema* Reader::array_schema() const {
  return array_schema_;
}

std::vector<std::string> Reader::attributes() const {
  return attributes_;
}

QueryBuffer Reader::buffer(const std::string& name) const {
  // TODO: fetch separate coordinate buffers as well. To be addressed in
  // TODO: subsequent PR
  auto buf = attr_buffers_.find(name);
  if (buf == attr_buffers_.end())
    return QueryBuffer{};
  return buf->second;
}

bool Reader::incomplete() const {
  return read_state_.overflowed_ || !read_state_.done();
}

// TODO: handle both attributes and dimensions
Status Reader::get_buffer(
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

// TODO: handle both attributes and dimensions
Status Reader::get_buffer(
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

Status Reader::init() {
  // Sanity checks
  if (storage_manager_ == nullptr)
    return LOG_STATUS(Status::ReaderError(
        "Cannot initialize reader; Storage manager not set"));
  if (array_schema_ == nullptr)
    return LOG_STATUS(Status::ReaderError(
        "Cannot initialize reader; Array metadata not set"));
  if (attr_buffers_.empty())
    return LOG_STATUS(
        Status::ReaderError("Cannot initialize reader; Buffers not set"));
  if (attributes_.empty())
    return LOG_STATUS(
        Status::ReaderError("Cannot initialize reader; Attributes not set"));
  if (array_schema_->dense() && !sparse_mode_ && !subarray_.is_set())
    return LOG_STATUS(Status::ReaderError(
        "Cannot initialize reader; Dense reads must have a subarray set"));

  // Get configuration parameters
  const char *memory_budget, *memory_budget_var;
  auto config = storage_manager_->config();
  RETURN_NOT_OK(config.get("sm.memory_budget", &memory_budget));
  RETURN_NOT_OK(config.get("sm.memory_budget_var", &memory_budget_var));
  RETURN_NOT_OK(utils::parse::convert(memory_budget, &memory_budget_));
  RETURN_NOT_OK(utils::parse::convert(memory_budget_var, &memory_budget_var_));
  RETURN_NOT_OK(init_read_state());

  return Status::Ok();
}

URI Reader::first_fragment_uri() const {
  if (fragment_metadata_.empty())
    return URI();
  return fragment_metadata_.front()->fragment_uri();
}

URI Reader::last_fragment_uri() const {
  if (fragment_metadata_.empty())
    return URI();
  return fragment_metadata_.back()->fragment_uri();
}

Layout Reader::layout() const {
  return layout_;
}

bool Reader::no_results() const {
  for (const auto& it : attr_buffers_) {
    if (*(it.second.buffer_size_) != 0)
      return false;
  }
  return true;
}

const Reader::ReadState* Reader::read_state() const {
  return &read_state_;
}

Reader::ReadState* Reader::read_state() {
  return &read_state_;
}

Status Reader::read() {
  auto coords_type = array_schema_->coords_type();
  switch (coords_type) {
    case Datatype::INT8:
      return read<int8_t>();
    case Datatype::UINT8:
      return read<uint8_t>();
    case Datatype::INT16:
      return read<int16_t>();
    case Datatype::UINT16:
      return read<uint16_t>();
    case Datatype::INT32:
      return read<int>();
    case Datatype::UINT32:
      return read<unsigned>();
    case Datatype::INT64:
      return read<int64_t>();
    case Datatype::UINT64:
      return read<uint64_t>();
    case Datatype::FLOAT32:
      return read<float>();
    case Datatype::FLOAT64:
      return read<double>();
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
      return read<int64_t>();
    default:
      return LOG_STATUS(
          Status::ReaderError("Cannot read; Unsupported domain type"));
  }

  return Status::Ok();
}

template <class T>
Status Reader::read() {
  STATS_FUNC_IN(reader_read);

  // Get next partition
  if (!read_state_.unsplittable_)
    RETURN_NOT_OK(read_state_.next());

  // Handle empty array or empty/finished subarray
  if (fragment_metadata_.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // Loop until you find results, or unsplittable, or done
  do {
    read_state_.overflowed_ = false;
    reset_buffer_sizes();

    // Perform read
    if (array_schema_->dense() && !sparse_mode_) {
      RETURN_NOT_OK(dense_read<T>());
    } else {
      RETURN_NOT_OK(sparse_read<T>());
    }

    // In the case of overflow, we need to split the current partition
    // without advancing to the next partition
    if (read_state_.overflowed_) {
      zero_out_buffer_sizes();
      RETURN_NOT_OK(read_state_.split_current<T>());

      if (read_state_.unsplittable_)
        return Status::Ok();
    } else {
      bool no_results = this->no_results();

      // Need to reset unsplittable if the results fit after all
      if (!no_results)
        read_state_.unsplittable_ = false;

      if (!no_results || read_state_.done())
        return Status::Ok();

      RETURN_NOT_OK(read_state_.next());
    }
  } while (true);

  return Status::Ok();

  STATS_FUNC_OUT(reader_read);
}

void Reader::set_array(const Array* array) {
  array_ = array;
  subarray_ = Subarray(array, Layout::ROW_MAJOR);
}

void Reader::set_array_schema(const ArraySchema* array_schema) {
  array_schema_ = array_schema;
}

Status Reader::set_buffer(
    const std::string& attribute,
    void* buffer,
    uint64_t* buffer_size,
    bool check_null_buffers) {
  // Check buffer
  if (check_null_buffers && (buffer == nullptr || buffer_size == nullptr))
    return LOG_STATUS(Status::ReaderError(
        "Cannot set buffer; Buffer or buffer size is null"));

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::ReaderError("Cannot set buffer; Array schema not set"));

  // Check that attribute exists
  if (attribute != constants::coords &&
      array_schema_->attribute(attribute) == nullptr)
    return LOG_STATUS(
        Status::ReaderError("Cannot set buffer; Invalid attribute"));

  // Check that attribute is fixed-sized
  bool var_size =
      (attribute != constants::coords && array_schema_->var_size(attribute));
  if (var_size)
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot set buffer; Input attribute '") + attribute +
        "' is var-sized"));

  // Error if setting a new attribute after initialization
  bool attr_exists = attr_buffers_.find(attribute) != attr_buffers_.end();
  if (read_state_.initialized_ && !attr_exists)
    return LOG_STATUS(Status::ReaderError(
        std::string("Cannot set buffer for new attribute '") + attribute +
        "' after initialization"));

  // Append to attributes only if buffer not set before
  if (!attr_exists)
    attributes_.emplace_back(attribute);

  // Set attribute buffer
  attr_buffers_[attribute] = QueryBuffer(buffer, nullptr, buffer_size, nullptr);

  return Status::Ok();
}

Status Reader::set_buffer(
    const std::string& attribute,
    uint64_t* buffer_off,
    uint64_t* buffer_off_size,
    void* buffer_val,
    uint64_t* buffer_val_size,
    bool check_null_buffers) {
  // Check buffer
  if (check_null_buffers &&
      (buffer_off == nullptr || buffer_off_size == nullptr ||
       buffer_val == nullptr || buffer_val_size == nullptr))
    return LOG_STATUS(Status::ReaderError(
        "Cannot set buffer; Buffer or buffer size is null"));

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::ReaderError("Cannot set buffer; Array schema not set"));

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
  if (read_state_.initialized_ && !attr_exists)
    return LOG_STATUS(Status::ReaderError(
        std::string("Cannot set buffer for new attribute '") + attribute +
        "' after initialization"));

  // Append to attributes only if buffer not set before
  if (!attr_exists)
    attributes_.emplace_back(attribute);

  // Set attribute buffer
  attr_buffers_[attribute] =
      QueryBuffer(buffer_off, buffer_val, buffer_off_size, buffer_val_size);

  return Status::Ok();
}

void Reader::set_fragment_metadata(
    const std::vector<FragmentMetadata*>& fragment_metadata) {
  fragment_metadata_ = fragment_metadata;
}

Status Reader::set_layout(Layout layout) {
  layout_ = layout;
  subarray_.set_layout(layout);

  return Status::Ok();
}

Status Reader::set_sparse_mode(bool sparse_mode) {
  if (!array_schema_->dense())
    return LOG_STATUS(Status::ReaderError(
        "Cannot set sparse mode; Only applicable to dense arrays"));

  bool all_sparse = true;
  for (const auto& f : fragment_metadata_) {
    if (f->dense()) {
      all_sparse = false;
      break;
    }
  }

  if (!all_sparse)
    return LOG_STATUS(
        Status::ReaderError("Cannot set sparse mode; Only applicable to opened "
                            "dense arrays having only sparse fragments"));

  sparse_mode_ = sparse_mode;
  return Status::Ok();
}

void Reader::set_storage_manager(StorageManager* storage_manager) {
  storage_manager_ = storage_manager;
}

Status Reader::set_subarray(const void* subarray, bool check_expanded_domain) {
  Subarray new_subarray(array_, layout_);
  if (subarray != nullptr) {
    auto dim_num = array_schema_->dim_num();
    auto coord_size = datatype_size(array_schema_->coords_type());
    auto s = (unsigned char*)subarray;
    for (unsigned i = 0; i < dim_num; ++i)
      RETURN_NOT_OK(new_subarray.add_range(
          i, (void*)(s + 2 * i * coord_size), check_expanded_domain));
  }

  return set_subarray(new_subarray);
}

Status Reader::set_subarray(const Subarray& subarray) {
  // Check layout
  if (subarray.layout() == Layout::GLOBAL_ORDER && subarray.range_num() != 1)
    return LOG_STATUS(
        Status::ReaderError("Cannot set subarray; Multi-range subarrays with "
                            "global order layout are not supported"));

  subarray_ = subarray;
  layout_ = subarray.layout();

  return Status::Ok();
}

const Subarray* Reader::subarray() const {
  return &subarray_;
}

/* ********************************* */
/*          STATIC FUNCTIONS         */
/* ********************************* */

template <class T>
void Reader::compute_result_space_tiles(
    const std::vector<std::vector<uint8_t>>& tile_coords,
    const TileDomain<T>& array_tile_domain,
    const std::vector<TileDomain<T>>& frag_tile_domains,
    std::map<const T*, ResultSpaceTile<T>>* result_space_tiles) {
  auto fragment_num = (unsigned)frag_tile_domains.size();
  auto dim_num = array_tile_domain.dim_num();
  std::vector<T> start_coords;
  const T* coords;
  start_coords.resize(dim_num);

  // For all tile coordinates
  for (const auto& tc : tile_coords) {
    coords = (T*)(&(tc[0]));
    start_coords = array_tile_domain.start_coords(coords);

    // Create result space tile and insert into the map
    auto r = result_space_tiles->emplace(coords, ResultSpaceTile<T>());
    auto& result_space_tile = r.first->second;
    result_space_tile.start_coords_ = start_coords;

    // Add fragment info to the result space tile
    for (unsigned f = 0; f < fragment_num; ++f) {
      // Check if the fragment overlaps with the space tile
      if (!frag_tile_domains[f].in_tile_domain(coords))
        continue;

      // Check if any previous fragment covers this fragment
      // for the tile identified by `coords`
      bool covered = false;
      for (unsigned j = 0; j < f; ++j) {
        if (frag_tile_domains[j].covers(coords, frag_tile_domains[f])) {
          covered = true;
          break;
        }
      }

      // Exclude this fragment from the space tile
      if (covered)
        continue;

      // Include this fragment in the space tile
      auto frag_domain = frag_tile_domains[f].domain_slice();
      auto frag_idx = frag_tile_domains[f].id();
      result_space_tile.frag_domains_.emplace_back(frag_idx, frag_domain);
      auto tile_idx = frag_tile_domains[f].tile_pos(coords);
      ResultTile result_tile(frag_idx, tile_idx);
      result_space_tile.result_tiles_[frag_idx] = result_tile;
    }
  }
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void Reader::clear_tiles(
    const std::string& attr,
    const std::vector<ResultTile*>& result_tiles) const {
  for (auto& result_tile : result_tiles)
    result_tile->attr_tiles_.erase(attr);
}

template <class T>
Status Reader::compute_result_cell_slabs(
    const std::vector<ResultCoords<T>>& result_coords,
    std::vector<ResultCellSlab>* result_cell_slabs) const {
  STATS_FUNC_IN(reader_compute_cell_ranges);

  // Trivial case
  auto coords_num = (uint64_t)result_coords.size();
  if (coords_num == 0)
    return Status::Ok();

  // Initialize the first range
  auto coords_end = result_coords.end();
  auto it = skip_invalid_elements(result_coords.begin(), coords_end);
  if (it == coords_end) {
    return LOG_STATUS(Status::ReaderError("Unexpected empty cell range."));
  }
  uint64_t start_pos = it->pos_;
  uint64_t end_pos = start_pos;
  auto tile = it->tile_;

  // Scan the coordinates and compute ranges
  it = skip_invalid_elements(++it, coords_end);
  while (it != coords_end) {
    if (it->tile_ == tile && it->pos_ == end_pos + 1) {
      // Same range - advance end position
      end_pos = it->pos_;
    } else {
      // New range - append previous range
      result_cell_slabs->emplace_back(tile, start_pos, end_pos - start_pos + 1);
      start_pos = it->pos_;
      end_pos = start_pos;
      tile = it->tile_;
    }
    it = skip_invalid_elements(++it, coords_end);
  }

  // Append the last range
  result_cell_slabs->emplace_back(tile, start_pos, end_pos - start_pos + 1);

  return Status::Ok();

  STATS_FUNC_OUT(reader_compute_cell_ranges);
}

template <class T>
Status Reader::compute_range_result_coords(
    unsigned frag_idx,
    ResultTile* tile,
    const std::vector<const T*>& range,
    std::vector<ResultCoords<T>>* result_coords) const {
  auto dim_num = array_schema_->dim_num();
  assert(dim_num == range.size());
  const auto& t = tile->attr_tiles_.find(constants::coords)->second.first;
  auto coords_num = t.cell_num();
  auto c = (T*)t.internal_data();

  for (uint64_t i = 0, pos = 0; i < coords_num; ++i, pos += dim_num) {
    if (utils::geometry::coords_in_rect<T>(&c[pos], range, dim_num) &&
        !coords_overwritten(frag_idx, &c[pos]))
      result_coords->emplace_back(tile, &c[pos], i);
  }

  return Status::Ok();
}

template <class T>
Status Reader::compute_range_result_coords(
    const std::vector<bool>& single_fragment,
    const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
    std::vector<ResultTile>* result_tiles,
    std::vector<std::vector<ResultCoords<T>>>* range_result_coords) {
  auto range_num = read_state_.partitioner_.current().range_num();
  range_result_coords->resize(range_num);
  auto cell_order = array_schema_->cell_order();

  auto statuses = parallel_for(0, range_num, [&](uint64_t r) {
    // Compute overlapping coordinates per range
    RETURN_NOT_OK(compute_range_result_coords(
        r, result_tile_map, result_tiles, &((*range_result_coords)[r])));

    // Potentially sort for deduping purposes (for the case of updates)
    if (!single_fragment[r]) {
      Layout layout =
          (layout_ == Layout::GLOBAL_ORDER || layout_ == Layout ::UNORDERED) ?
              cell_order :
              layout_;
      RETURN_CANCEL_OR_ERROR(
          sort_result_coords<T>(&((*range_result_coords)[r]), layout));
      RETURN_CANCEL_OR_ERROR(
          dedup_result_coords<T>(&((*range_result_coords)[r])));
    }

    // Compute tile coordinate
    return Status::Ok();
  });
  for (auto st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();
}

template <class T>
Status Reader::compute_range_result_coords(
    uint64_t range_idx,
    const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
    std::vector<ResultTile>* result_tiles,
    std::vector<ResultCoords<T>>* range_result_coords) {
  const auto& subarray = read_state_.partitioner_.current();
  const auto& overlap = subarray.tile_overlap();
  auto range = subarray.range<T>(range_idx);
  auto fragment_num = fragment_metadata_.size();

  for (unsigned f = 0; f < fragment_num; ++f) {
    // Skip dense fragments
    if (fragment_metadata_[f]->dense())
      continue;

    auto tr = overlap[f][range_idx].tile_ranges_.begin();
    auto tr_end = overlap[f][range_idx].tile_ranges_.end();
    auto t = overlap[f][range_idx].tiles_.begin();
    auto t_end = overlap[f][range_idx].tiles_.end();

    while (tr != tr_end || t != t_end) {
      // Handle tile range
      if (tr != tr_end && (t == t_end || tr->first < t->first)) {
        for (uint64_t i = tr->first; i <= tr->second; ++i) {
          auto pair = std::pair<unsigned, uint64_t>(f, i);
          auto tile_it = result_tile_map.find(pair);
          assert(tile_it != result_tile_map.end());
          auto tile_idx = tile_it->second;
          auto& tile = (*result_tiles)[tile_idx];

          // Add results only if the sparse tile MBR is not fully
          // covered by a more recent fragment's non-empty domain
          if (!sparse_tile_overwritten<T>(f, i))
            RETURN_NOT_OK(get_all_result_coords<T>(&tile, range_result_coords));
        }
        ++tr;
      } else {
        // Handle single tile
        auto pair = std::pair<unsigned, uint64_t>(f, t->first);
        auto tile_it = result_tile_map.find(pair);
        assert(tile_it != result_tile_map.end());
        auto tile_idx = tile_it->second;
        auto& tile = (*result_tiles)[tile_idx];
        if (t->second == 1.0) {  // Full overlap
          // Add results only if the sparse tile MBR is not fully
          // covered by a more recent fragment's non-empty domain
          if (!sparse_tile_overwritten<T>(f, t->first))
            RETURN_NOT_OK(get_all_result_coords<T>(&tile, range_result_coords));
        } else {  // Partial overlap
          RETURN_NOT_OK(compute_range_result_coords<T>(
              f, &tile, range, range_result_coords));
        }
        ++t;
      }
    }
  }

  return Status::Ok();
}

template <class T>
Status Reader::compute_subarray_coords(
    std::vector<std::vector<ResultCoords<T>>>* range_result_coords,
    std::vector<std::vector<T>>* tile_coords,
    std::vector<ResultCoords<T>>* result_coords) {
  // Add all valid ``range_result_coords`` to ``result_coords``
  for (const auto& rv : *range_result_coords) {
    for (const auto& c : rv) {
      if (c.valid())
        result_coords->emplace_back(c.tile_, c.coords_, c.pos_);
    }
  }

  // No need to sort in UNORDERED layout
  if (layout_ == Layout::UNORDERED)
    return Status::Ok();

  // Compute tile coordinates only for the GLOBAL_ORDER layout
  if (layout_ == Layout::GLOBAL_ORDER)
    compute_sparse_tile_coords(result_coords, tile_coords);

  // Sort
  auto cell_order = array_schema_->cell_order();
  Layout layout = (layout_ == Layout ::UNORDERED) ? cell_order : layout_;
  RETURN_NOT_OK(sort_result_coords(result_coords, layout));

  return Status::Ok();
}

template <class T>
Status Reader::compute_sparse_tile_coords(
    std::vector<ResultCoords<T>>* result_coords,
    std::vector<std::vector<T>>* tile_coords) const {
  auto dim_num = array_schema_->dim_num();
  auto domain = (const T*)array_schema_->domain()->domain();
  auto tile_extents = (const T*)array_schema_->domain()->tile_extents();
  std::map<std::vector<T>, size_t> tile_coords_map;
  std::vector<T> tile_coords_tmp;
  tile_coords_tmp.resize(dim_num);

  for (auto& rc : *result_coords) {
    for (unsigned i = 0; i < dim_num; ++i)
      tile_coords_tmp[i] =
          (tile_extents == nullptr) ?
              0 :
              (rc.coords_[i] - domain[2 * i]) / tile_extents[i];
    auto it = tile_coords_map.find(tile_coords_tmp);
    if (it == tile_coords_map.end()) {  // New tile coordinates
      tile_coords->emplace_back(tile_coords_tmp);
      rc.tile_coords_ = &(tile_coords->back())[0];
    } else {  // Existing tile coordinates
      rc.tile_coords_ = &((*tile_coords)[it->second][0]);
    }
  }

  return Status::Ok();
}

template <class T>
Status Reader::compute_sparse_result_tiles(
    std::vector<ResultTile>* result_tiles,
    std::map<std::pair<unsigned, uint64_t>, size_t>* result_tile_map,
    std::vector<bool>* single_fragment) {
  STATS_FUNC_IN(reader_compute_overlapping_tiles);

  // For easy reference
  const auto& subarray = read_state_.partitioner_.current();
  const auto& overlap = subarray.tile_overlap();
  auto range_num = subarray.range_num();
  auto fragment_num = fragment_metadata_.size();
  std::vector<unsigned> first_fragment;
  first_fragment.resize(range_num);
  for (uint64_t r = 0; r < range_num; ++r)
    first_fragment[r] = UINT32_MAX;

  single_fragment->resize(range_num);
  for (uint64_t i = 0; i < range_num; ++i)
    (*single_fragment)[i] = true;

  result_tiles->clear();
  for (unsigned f = 0; f < fragment_num; ++f) {
    // Skip dense fragments
    if (fragment_metadata_[f]->dense())
      continue;

    for (uint64_t r = 0; r < range_num; ++r) {
      // Handle range of tiles (full overlap)
      const auto& tile_ranges = overlap[f][r].tile_ranges_;
      for (const auto& tr : tile_ranges) {
        for (uint64_t t = tr.first; t <= tr.second; ++t) {
          auto pair = std::pair<unsigned, uint64_t>(f, t);
          // Add tile only if it does not already exist
          if (result_tile_map->find(pair) == result_tile_map->end()) {
            result_tiles->emplace_back(f, t);
            (*result_tile_map)[pair] = result_tiles->size() - 1;
            if (f > first_fragment[r])
              (*single_fragment)[r] = false;
            else
              first_fragment[r] = f;
          }
        }
      }

      // Handle single tiles
      const auto& o_tiles = overlap[f][r].tiles_;
      for (const auto& o_tile : o_tiles) {
        auto t = o_tile.first;
        auto pair = std::pair<unsigned, uint64_t>(f, t);
        // Add tile only if it does not already exist
        if (result_tile_map->find(pair) == result_tile_map->end()) {
          result_tiles->emplace_back(f, t);
          (*result_tile_map)[pair] = result_tiles->size() - 1;
          if (f > first_fragment[r])
            (*single_fragment)[r] = false;
          else
            first_fragment[r] = f;
        }
      }
    }
  }

  return Status::Ok();

  STATS_FUNC_OUT(reader_compute_overlapping_tiles);
}

Status Reader::copy_cells(
    const std::string& attribute,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs) {
  if (result_cell_slabs.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  if (array_schema_->var_size(attribute))
    return copy_var_cells(attribute, stride, result_cell_slabs);
  return copy_fixed_cells(attribute, stride, result_cell_slabs);
}

Status Reader::copy_fixed_cells(
    const std::string& attribute,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs) {
  STATS_FUNC_IN(reader_copy_fixed_cells);

  // For easy reference
  auto it = attr_buffers_.find(attribute);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_size = it->second.buffer_size_;
  auto cell_size = array_schema_->cell_size(attribute);
  auto type = array_schema_->type(attribute);
  auto fill_size = datatype_size(type);
  auto fill_value = constants::fill_value(type);
  assert(fill_value != nullptr);

  // Precompute the cell range destination offsets in the buffer
  auto num_cs = result_cell_slabs.size();
  uint64_t buffer_offset = 0;
  std::vector<uint64_t> cs_offsets(num_cs);
  for (uint64_t i = 0; i < num_cs; i++) {
    const auto& cs = result_cell_slabs[i];
    auto bytes_to_copy = cs.length_ * cell_size;
    cs_offsets[i] = buffer_offset;
    buffer_offset += bytes_to_copy;
  }

  // Handle overflow
  if (buffer_offset > *buffer_size) {
    read_state_.overflowed_ = true;
    return Status::Ok();
  }

  // Copy cell ranges in parallel.
  auto statuses = parallel_for(0, num_cs, [&](uint64_t i) {
    const auto& cs = result_cell_slabs[i];
    uint64_t offset = cs_offsets[i];
    // Check for overflow
    auto bytes_to_copy = cs.length_ * cell_size;
    assert(offset + bytes_to_copy <= *buffer_size);

    // Copy
    if (cs.tile_ == nullptr) {  // Empty range
      auto fill_num = bytes_to_copy / fill_size;
      for (uint64_t j = 0; j < fill_num; ++j) {
        std::memcpy(buffer + offset, fill_value, fill_size);
        offset += fill_size;
      }
    } else {  // Non-empty range
      const auto& tile = cs.tile_->attr_tiles_.find(attribute)->second.first;
      if (stride == UINT64_MAX) {
        const uint64_t tile_offset = cs.start_ * cell_size;
        RETURN_NOT_OK(tile.read(buffer + offset, bytes_to_copy, tile_offset));
      } else {
        uint64_t byte_stride = stride * cell_size;
        uint64_t cell_offset = offset;
        uint64_t data_offset = cs.start_ * cell_size;
        for (uint64_t j = 0; j < cs.length_; ++j) {
          RETURN_NOT_OK(
              tile.read(buffer + cell_offset, cell_size, data_offset));
          cell_offset += cell_size;
          data_offset += byte_stride;
        }
      }
    }

    return Status::Ok();
  });

  for (auto st : statuses)
    RETURN_NOT_OK(st);

  // Update buffer offsets
  *(attr_buffers_[attribute].buffer_size_) = buffer_offset;
  STATS_COUNTER_ADD(reader_num_fixed_cell_bytes_copied, buffer_offset);

  return Status::Ok();

  STATS_FUNC_OUT(reader_copy_fixed_cells);
}

Status Reader::copy_var_cells(
    const std::string& attribute,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs) {
  STATS_FUNC_IN(reader_copy_var_cells);

  // For easy reference
  auto it = attr_buffers_.find(attribute);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_var = (unsigned char*)it->second.buffer_var_;
  auto buffer_size = it->second.buffer_size_;
  auto buffer_var_size = it->second.buffer_var_size_;
  uint64_t offset_size = constants::cell_var_offset_size;
  auto type = array_schema_->type(attribute);
  auto fill_size = datatype_size(type);
  auto fill_value = constants::fill_value(type);
  assert(fill_value != nullptr);

  // Compute the destinations of offsets and var-len data in the buffers.
  std::vector<std::vector<uint64_t>> offset_offsets_per_cs;
  std::vector<std::vector<uint64_t>> var_offsets_per_cs;
  uint64_t total_offset_size, total_var_size;
  RETURN_NOT_OK(compute_var_cell_destinations(
      attribute,
      stride,
      result_cell_slabs,
      &offset_offsets_per_cs,
      &var_offsets_per_cs,
      &total_offset_size,
      &total_var_size));

  // Check for overflow and return early (without copying) in that case.
  if (total_offset_size > *buffer_size || total_var_size > *buffer_var_size) {
    read_state_.overflowed_ = true;
    return Status::Ok();
  }

  // Copy result cell slabs in parallel
  const auto num_cs = result_cell_slabs.size();
  auto statuses = parallel_for(0, num_cs, [&](uint64_t cs_idx) {
    const auto& cs = result_cell_slabs[cs_idx];
    const auto& offset_offsets = offset_offsets_per_cs[cs_idx];
    const auto& var_offsets = var_offsets_per_cs[cs_idx];

    // Get tile information, if the range is nonempty.
    uint64_t* tile_offsets = nullptr;
    Tile* tile_var = nullptr;
    uint64_t tile_cell_num = 0;
    if (cs.tile_ != nullptr) {
      std::pair<tiledb::sm::Tile, tiledb::sm::Tile>* const tile_pair =
          &cs.tile_->attr_tiles_.find(attribute)->second;
      Tile* const tile = &tile_pair->first;
      tile_var = &tile_pair->second;
      tile_offsets = (uint64_t*)tile->internal_data();
      tile_cell_num = tile->cell_num();
    }

    // Copy each cell in the range
    uint64_t dest_vec_idx = 0;
    stride = (stride == UINT64_MAX) ? 1 : stride;
    for (auto cell_idx = cs.start_; dest_vec_idx < cs.length_;
         cell_idx += stride, dest_vec_idx++) {
      auto offset_dest = buffer + offset_offsets[dest_vec_idx];
      auto var_offset = var_offsets[dest_vec_idx];
      auto var_dest = buffer_var + var_offset;

      // Copy offset
      std::memcpy(offset_dest, &var_offset, offset_size);

      // Copy variable-sized value
      if (cs.tile_ == nullptr) {
        std::memcpy(var_dest, &fill_value, fill_size);
      } else {
        const uint64_t cell_var_size =
            (cell_idx != tile_cell_num - 1) ?
                tile_offsets[cell_idx + 1] - tile_offsets[cell_idx] :
                tile_var->size() - (tile_offsets[cell_idx] - tile_offsets[0]);
        const uint64_t tile_var_offset =
            tile_offsets[cell_idx] - tile_offsets[0];
        RETURN_NOT_OK(tile_var->read(var_dest, cell_var_size, tile_var_offset));
      }
    }

    return Status::Ok();
  });

  // Check all statuses
  for (auto st : statuses)
    RETURN_NOT_OK(st);

  // Update buffer offsets
  *(attr_buffers_[attribute].buffer_size_) = total_offset_size;
  *(attr_buffers_[attribute].buffer_var_size_) = total_var_size;
  STATS_COUNTER_ADD(
      reader_num_var_cell_bytes_copied, total_offset_size + total_var_size);

  return Status::Ok();

  STATS_FUNC_OUT(reader_copy_var_cells);
}

Status Reader::compute_var_cell_destinations(
    const std::string& attribute,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<std::vector<uint64_t>>* offset_offsets_per_cs,
    std::vector<std::vector<uint64_t>>* var_offsets_per_cs,
    uint64_t* total_offset_size,
    uint64_t* total_var_size) const {
  // For easy reference
  auto num_cs = result_cell_slabs.size();
  auto offset_size = constants::cell_var_offset_size;
  auto type = array_schema_->type(attribute);
  auto fill_size = datatype_size(type);

  // Resize the output vectors
  offset_offsets_per_cs->resize(num_cs);
  var_offsets_per_cs->resize(num_cs);

  // Compute the destinations for all result cell slabs
  *total_offset_size = 0;
  *total_var_size = 0;
  for (uint64_t cs_idx = 0; cs_idx < num_cs; cs_idx++) {
    const auto& cs = result_cell_slabs[cs_idx];
    (*offset_offsets_per_cs)[cs_idx].resize(cs.length_);
    (*var_offsets_per_cs)[cs_idx].resize(cs.length_);

    // Get tile information, if the range is nonempty.
    uint64_t* tile_offsets = nullptr;
    uint64_t tile_cell_num = 0;
    uint64_t tile_var_size = 0;
    if (cs.tile_ != nullptr) {
      const auto& tile_pair = cs.tile_->attr_tiles_.find(attribute)->second;
      const auto& tile = tile_pair.first;
      const auto& tile_var = tile_pair.second;
      tile_offsets = (uint64_t*)tile.internal_data();
      tile_cell_num = tile.cell_num();
      tile_var_size = tile_var.size();
    }

    // Compute the destinations for each cell in the range.
    uint64_t dest_vec_idx = 0;
    stride = (stride == UINT64_MAX) ? 1 : stride;
    for (auto cell_idx = cs.start_; dest_vec_idx < cs.length_;
         cell_idx += stride, dest_vec_idx++) {
      // Get size of variable-sized cell
      uint64_t cell_var_size = 0;
      if (cs.tile_ == nullptr) {
        cell_var_size = fill_size;
      } else {
        cell_var_size =
            (cell_idx != tile_cell_num - 1) ?
                tile_offsets[cell_idx + 1] - tile_offsets[cell_idx] :
                tile_var_size - (tile_offsets[cell_idx] - tile_offsets[0]);
      }

      // Record destination offsets.
      (*offset_offsets_per_cs)[cs_idx][dest_vec_idx] = *total_offset_size;
      (*var_offsets_per_cs)[cs_idx][dest_vec_idx] = *total_var_size;
      *total_offset_size += offset_size;
      *total_var_size += cell_var_size;
    }
  }

  return Status::Ok();
}

template <class T>
void Reader::compute_result_space_tiles(
    const Subarray& subarray,
    std::map<const T*, ResultSpaceTile<T>>* result_space_tiles) const {
  // For easy reference
  auto dim_num = array_schema_->dim_num();
  auto domain = (const T*)array_schema_->domain()->domain();
  auto tile_extents = (const T*)array_schema_->domain()->tile_extents();
  auto tile_order = array_schema_->tile_order();

  // Compute fragment tile domains
  std::vector<TileDomain<T>> frag_tile_domains;
  auto fragment_num = (int)fragment_metadata_.size();
  if (fragment_num > 0) {
    for (int i = fragment_num - 1; i >= 0; --i) {
      if (fragment_metadata_[i]->dense()) {
        auto non_empty_domain =
            (const T*)fragment_metadata_[i]->non_empty_domain();
        frag_tile_domains.emplace_back(
            i, dim_num, domain, non_empty_domain, tile_extents, tile_order);
      }
    }
  }

  // Get tile coords and array domain
  const auto& tile_coords = subarray.tile_coords();
  TileDomain<T> array_tile_domain(
      UINT32_MAX, dim_num, domain, domain, tile_extents, tile_order);

  // Compute result space tiles
  compute_result_space_tiles<T>(
      tile_coords, array_tile_domain, frag_tile_domains, result_space_tiles);
}

template <class T>
void Reader::compute_result_cell_slabs(
    const Subarray& subarray,
    std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
    std::vector<ResultCoords<T>>* result_coords,
    std::vector<ResultTile*>* result_tiles,
    std::vector<ResultCellSlab>* result_cell_slabs) const {
  auto layout = subarray.layout();
  if (layout == Layout::ROW_MAJOR || layout == Layout::COL_MAJOR) {
    uint64_t result_coords_pos = 0;
    std::set<std::pair<unsigned, uint64_t>> frag_tile_set;
    compute_result_cell_slabs_row_col(
        subarray,
        result_space_tiles,
        result_coords,
        &result_coords_pos,
        result_tiles,
        &frag_tile_set,
        result_cell_slabs);
  } else if (layout == Layout::GLOBAL_ORDER) {
    compute_result_cell_slabs_global(
        subarray,
        result_space_tiles,
        result_coords,
        result_tiles,
        result_cell_slabs);
  } else {  // UNORDERED
    assert(false);
  }
}

template <class T>
void Reader::compute_result_cell_slabs_row_col(
    const Subarray& subarray,
    std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
    std::vector<ResultCoords<T>>* result_coords,
    uint64_t* result_coords_pos,
    std::vector<ResultTile*>* result_tiles,
    std::set<std::pair<unsigned, uint64_t>>* frag_tile_set,
    std::vector<ResultCellSlab>* result_cell_slabs) const {
  // Compute result space tiles. The result space tiles hold all the
  // relevant result tiles of the dense fragments
  compute_result_space_tiles<T>(subarray, result_space_tiles);

  // Gather result cell slabs and pointers to result tiles
  // `result_tiles` holds pointers to tiles that store actual results,
  // which can be stored either in `sparse_result_tiles` (sparse)
  // or in `result_space_tiles` (dense).
  auto rcs_it = ReadCellSlabIter<T>(
      &subarray, result_space_tiles, result_coords, *result_coords_pos);
  for (rcs_it.begin(); !rcs_it.end(); ++rcs_it) {
    // Add result cell slab
    auto result_cell_slab = rcs_it.result_cell_slab();
    result_cell_slabs->push_back(result_cell_slab);
    // Add result tile
    if (result_cell_slab.tile_ != nullptr) {
      auto frag_idx = result_cell_slab.tile_->frag_idx_;
      auto tile_idx = result_cell_slab.tile_->tile_idx_;
      auto frag_tile_pair = std::pair<unsigned, uint64_t>(frag_idx, tile_idx);
      auto it = frag_tile_set->find(frag_tile_pair);
      if (it == frag_tile_set->end()) {
        frag_tile_set->insert(frag_tile_pair);
        result_tiles->push_back(result_cell_slab.tile_);
      }
    }
  }
  *result_coords_pos = rcs_it.result_coords_pos();
}

template <class T>
void Reader::compute_result_cell_slabs_global(
    const Subarray& subarray,
    std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
    std::vector<ResultCoords<T>>* result_coords,
    std::vector<ResultTile*>* result_tiles,
    std::vector<ResultCellSlab>* result_cell_slabs) const {
  const auto& tile_coords = subarray.tile_coords();
  auto cell_order = array_schema_->cell_order();
  std::vector<Subarray> tile_subarrays;
  tile_subarrays.reserve(tile_coords.size());
  uint64_t result_coords_pos = 0;
  std::set<std::pair<unsigned, uint64_t>> frag_tile_set;

  for (const auto& tc : tile_coords) {
    tile_subarrays.emplace_back(
        subarray.crop_to_tile((const T*)&tc[0], cell_order));
    auto& tile_subarray = tile_subarrays.back();
    tile_subarray.template compute_tile_coords<T>();

    compute_result_cell_slabs_row_col(
        tile_subarray,
        result_space_tiles,
        result_coords,
        &result_coords_pos,
        result_tiles,
        &frag_tile_set,
        result_cell_slabs);
  }
}

template <class T>
Status Reader::compute_result_coords(
    std::vector<ResultTile>* result_tiles,
    std::vector<std::vector<T>>* tile_coords,
    std::vector<ResultCoords<T>>* result_coords) {
  // Get overlapping tile indexes
  typedef std::pair<unsigned, uint64_t> FragTilePair;
  std::map<FragTilePair, size_t> result_tile_map;
  std::vector<bool> single_fragment;

  RETURN_CANCEL_OR_ERROR(compute_sparse_result_tiles<T>(
      result_tiles, &result_tile_map, &single_fragment));

  // Create temporary vector with pointers to result tiles, so that
  // `read_tiles`, `filter_tiles` and `clear_tiles` below can work without
  // changes
  std::vector<ResultTile*> tmp_result_tiles;
  for (auto& result_tile : *result_tiles)
    tmp_result_tiles.push_back(&result_tile);

  // Read and filter coordinate tiles
  RETURN_CANCEL_OR_ERROR(read_tiles(constants::coords, tmp_result_tiles));
  RETURN_CANCEL_OR_ERROR(filter_tiles(constants::coords, tmp_result_tiles));

  // Compute the read coordinates for all fragments for each subarray range
  std::vector<std::vector<ResultCoords<T>>> range_result_coords;
  RETURN_CANCEL_OR_ERROR(compute_range_result_coords<T>(
      single_fragment, result_tile_map, result_tiles, &range_result_coords));
  result_tile_map.clear();

  // Compute final coords (sorted in the result layout) of the whole subarray.
  RETURN_CANCEL_OR_ERROR(compute_subarray_coords<T>(
      &range_result_coords, tile_coords, result_coords));
  range_result_coords.clear();

  return Status::Ok();
}

template <class T>
Status Reader::dedup_result_coords(
    std::vector<ResultCoords<T>>* result_coords) const {
  STATS_FUNC_IN(reader_dedup_coords);

  auto coords_size = array_schema_->coords_size();
  auto coords_end = result_coords->end();
  auto it = skip_invalid_elements(result_coords->begin(), coords_end);
  while (it != coords_end) {
    auto next_it = skip_invalid_elements(std::next(it), coords_end);
    if (next_it != coords_end &&
        !std::memcmp(it->coords_, next_it->coords_, coords_size)) {
      if (it->tile_->frag_idx_ < next_it->tile_->frag_idx_) {
        it->invalidate();
        it = skip_invalid_elements(++it, coords_end);
      } else {
        next_it->invalidate();
      }
    } else {
      it = skip_invalid_elements(++it, coords_end);
    }
  }
  return Status::Ok();

  STATS_FUNC_OUT(reader_dedup_coords);
}

template <class T>
Status Reader::dense_read() {
  STATS_FUNC_IN(reader_dense_read);

  // Sanity checks
  assert(std::is_integral<T>::value);
  assert(!fragment_metadata_.empty());

  // Compute result coordinates from the sparse fragments
  // `sparse_result_tiles` will hold all the relevant result tiles of
  // sparse fragments
  std::vector<ResultCoords<T>> result_coords;
  std::vector<ResultTile> sparse_result_tiles;
  std::vector<std::vector<T>> tile_coords;
  RETURN_NOT_OK(compute_result_coords(
      &sparse_result_tiles, &tile_coords, &result_coords));

  // Compute result cell slabs.
  // `result_space_tiles` will hold all the relevant result tiles of
  // dense fragments. `result` tiles will hold pointers to the
  // final result tiles for both sparse and dense fragments.
  std::map<const T*, ResultSpaceTile<T>> result_space_tiles;
  std::vector<ResultCellSlab> result_cell_slabs;
  std::vector<ResultTile*> result_tiles;
  auto& subarray = read_state_.partitioner_.current();
  subarray.compute_tile_coords<T>();
  compute_result_cell_slabs(
      subarray,
      &result_space_tiles,
      &result_coords,
      &result_tiles,
      &result_cell_slabs);

  // Clear sparse coordinate tiles (not needed any more)
  for (auto& tile : sparse_result_tiles)
    tile.attr_tiles_.erase(constants::coords);
  tile_coords.clear();

  // Needed when copying the cells
  auto stride = array_schema_->domain()->stride<T>(subarray.layout());

  // Copy cells
  for (const auto& attr : attributes_) {
    if (read_state_.overflowed_)
      break;
    if (attr == constants::coords)
      continue;

    RETURN_CANCEL_OR_ERROR(read_tiles(attr, result_tiles));
    RETURN_CANCEL_OR_ERROR(filter_tiles(attr, result_tiles));
    RETURN_CANCEL_OR_ERROR(copy_cells(attr, stride, result_cell_slabs));
    clear_tiles(attr, result_tiles);
  }

  // Fill coordinates if the user requested them
  if (!read_state_.overflowed_ && has_coords())
    RETURN_CANCEL_OR_ERROR(fill_dense_coords<T>(subarray));

  return Status::Ok();

  STATS_FUNC_OUT(reader_dense_read);
}

template <class T>
Status Reader::fill_dense_coords(const Subarray& subarray) {
  // For easy reference
  uint64_t coords_buff_offset = 0;
  auto it = attr_buffers_.find(constants::coords);
  assert(it != attr_buffers_.end());
  auto coords_buff = it->second.buffer_;
  auto coords_buff_size = *(it->second.buffer_size_);

  if (layout_ == Layout::GLOBAL_ORDER) {
    RETURN_NOT_OK(fill_dense_coords_global<T>(
        subarray, coords_buff, coords_buff_size, &coords_buff_offset));
  } else {
    assert(layout_ == Layout::ROW_MAJOR || layout_ == Layout::COL_MAJOR);
    RETURN_NOT_OK(fill_dense_coords_row_col<T>(
        subarray, coords_buff, coords_buff_size, &coords_buff_offset));
  }

  // Update buffer size
  *(it->second.buffer_size_) = coords_buff_offset;

  return Status::Ok();
}

template <class T>
Status Reader::fill_dense_coords_global(
    const Subarray& subarray,
    void* coords_buff,
    uint64_t coords_buff_size,
    uint64_t* coords_buff_offset) {
  auto tile_coords = subarray.tile_coords();
  auto cell_order = array_schema_->cell_order();

  for (const auto& tc : tile_coords) {
    auto tile_subarray = subarray.crop_to_tile((const T*)&tc[0], cell_order);
    RETURN_NOT_OK(fill_dense_coords_row_col<T>(
        tile_subarray, coords_buff, coords_buff_size, coords_buff_offset));
  }

  return Status::Ok();
}

template <class T>
Status Reader::fill_dense_coords_row_col(
    const Subarray& subarray,
    void* coords_buff,
    uint64_t coords_buff_size,
    uint64_t* coords_buff_offset) {
  STATS_FUNC_IN(reader_fill_coords);

  auto cell_order = array_schema_->cell_order();
  auto coords_size = array_schema_->coords_size();

  // Iterate over all coordinates, retrieved in cell slabs
  CellSlabIter<T> iter(&subarray);
  RETURN_CANCEL_OR_ERROR(iter.begin());
  while (!iter.end()) {
    auto cell_slab = iter.cell_slab();
    auto coords_num = cell_slab.length_;

    // Check for overflow
    if (coords_num * coords_size + (*coords_buff_offset) > coords_buff_size) {
      read_state_.overflowed_ = true;
      return Status::Ok();
    }

    if (layout_ == Layout::ROW_MAJOR ||
        (layout_ == Layout::GLOBAL_ORDER && cell_order == Layout::ROW_MAJOR))
      fill_dense_coords_row_slab(
          &cell_slab.coords_[0], coords_num, coords_buff, coords_buff_offset);
    else
      fill_dense_coords_col_slab(
          &cell_slab.coords_[0], coords_num, coords_buff, coords_buff_offset);
    ++iter;
  }

  return Status::Ok();

  STATS_FUNC_OUT(reader_fill_coords);
}

template <class T>
void Reader::fill_dense_coords_row_slab(
    const T* start, uint64_t num, void* buff, uint64_t* offset) const {
  // For easy reference
  auto dim_num = array_schema_->dim_num();
  assert(dim_num > 0);
  auto c_buff = (char*)buff;

  // Fill coordinates
  for (uint64_t i = 0; i < num; ++i) {
    // First dim-1 dimensions are copied as they are
    if (dim_num > 1) {
      auto bytes_to_copy = (dim_num - 1) * sizeof(T);
      std::memcpy(c_buff + *offset, start, bytes_to_copy);
      *offset += bytes_to_copy;
    }

    // Last dimension is incremented by `i`
    auto new_coord = start[dim_num - 1] + i;
    std::memcpy(c_buff + *offset, &new_coord, sizeof(T));
    *offset += sizeof(T);
  }
}

template <class T>
void Reader::fill_dense_coords_col_slab(
    const T* start, uint64_t num, void* buff, uint64_t* offset) const {
  // For easy reference
  auto dim_num = array_schema_->dim_num();
  assert(dim_num > 0);
  auto c_buff = (char*)buff;

  // Fill coordinates
  for (uint64_t i = 0; i < num; ++i) {
    // First dimension is incremented by `i`
    auto new_coord = start[0] + i;
    std::memcpy(c_buff + *offset, &new_coord, sizeof(T));
    *offset += sizeof(T);

    // Last dim-1 dimensions are copied as they are
    if (dim_num > 1) {
      auto bytes_to_copy = (dim_num - 1) * sizeof(T);
      std::memcpy(c_buff + *offset, &start[1], bytes_to_copy);
      *offset += bytes_to_copy;
    }
  }
}

Status Reader::filter_tiles(
    const std::string& attribute,
    const std::vector<ResultTile*>& result_tiles) const {
  STATS_FUNC_IN(reader_filter_tiles);

  auto var_size = array_schema_->var_size(attribute);
  auto num_tiles = static_cast<uint64_t>(result_tiles.size());
  auto encryption_key = array_->encryption_key();

  auto statuses = parallel_for(0, num_tiles, [&, this](uint64_t i) {
    auto& tile = result_tiles[i];
    auto it = tile->attr_tiles_.find(attribute);
    // Skip non-existent attributes (e.g. coords in the dense case).
    if (it == tile->attr_tiles_.end())
      return Status::Ok();

    // Get information about the tile in its fragment
    auto& fragment = fragment_metadata_[tile->frag_idx_];
    auto tile_attr_uri = fragment->attr_uri(attribute);
    uint64_t tile_attr_offset;
    RETURN_NOT_OK(fragment->file_offset(
        *encryption_key, attribute, tile->tile_idx_, &tile_attr_offset));

    auto& tile_pair = it->second;
    auto& t = tile_pair.first;
    auto& t_var = tile_pair.second;

    if (!t.filtered()) {
      // Decompress, etc.
      RETURN_NOT_OK(filter_tile(attribute, &t, var_size));
      RETURN_NOT_OK(storage_manager_->write_to_cache(
          tile_attr_uri, tile_attr_offset, t.buffer2()));
    }

    if (var_size && !t_var.filtered()) {
      auto tile_attr_var_uri = fragment->attr_var_uri(attribute);
      uint64_t tile_attr_var_offset;
      RETURN_NOT_OK(fragment->file_var_offset(
          *encryption_key, attribute, tile->tile_idx_, &tile_attr_var_offset));

      // Decompress, etc.
      RETURN_NOT_OK(filter_tile(attribute, &t_var, false));
      RETURN_NOT_OK(storage_manager_->write_to_cache(
          tile_attr_var_uri, tile_attr_var_offset, t_var.buffer2()));
    }

    return Status::Ok();
  });

  for (const auto& st : statuses)
    RETURN_CANCEL_OR_ERROR(st);

  return Status::Ok();

  STATS_FUNC_OUT(reader_filter_tiles);
}

Status Reader::filter_tile(
    const std::string& attribute, Tile* tile, bool offsets) const {
  uint64_t orig_size = tile->buffer2()->size();

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

  RETURN_NOT_OK(filters.run_reverse(tile));

  tile->set_filtered(true);
  tile->set_pre_filtered_size(orig_size);

  STATS_COUNTER_ADD(reader_num_bytes_after_filtering, tile->size());

  return Status::Ok();
}

template <class T>
Status Reader::get_all_result_coords(
    ResultTile* tile, std::vector<ResultCoords<T>>* result_coords) const {
  auto dim_num = array_schema_->dim_num();
  const auto& t = tile->attr_tiles_.find(constants::coords)->second.first;
  auto coords_num = t.cell_num();
  auto c = (T*)t.internal_data();

  for (uint64_t i = 0; i < coords_num; ++i)
    result_coords->emplace_back(tile, &c[i * dim_num], i);

  return Status::Ok();
}

bool Reader::has_coords() const {
  return attr_buffers_.find(constants::coords) != attr_buffers_.end();
}

Status Reader::init_read_state() {
  // Check subarray
  if (subarray_.layout() == Layout::GLOBAL_ORDER && subarray_.range_num() != 1)
    return LOG_STATUS(
        Status::ReaderError("Cannot initialize read state; Multi-range "
                            "subarrays do not support global order"));

  // Get config
  bool found = false;
  auto config = storage_manager_->config();
  uint64_t memory_budget = 0;
  RETURN_NOT_OK(
      config.get<uint64_t>("sm.memory_budget", &memory_budget, &found));
  assert(found);
  uint64_t memory_budget_var = 0;
  RETURN_NOT_OK(
      config.get<uint64_t>("sm.memory_budget_var", &memory_budget_var, &found));
  assert(found);

  // Create read state
  read_state_.partitioner_ =
      SubarrayPartitioner(subarray_, memory_budget, memory_budget_var);
  read_state_.overflowed_ = false;
  read_state_.unsplittable_ = false;

  // Set result size budget
  for (const auto& a : attr_buffers_) {
    auto attr_name = a.first;
    auto buffer_size = a.second.buffer_size_;
    auto buffer_var_size = a.second.buffer_var_size_;
    if (!array_schema_->var_size(a.first)) {
      RETURN_NOT_OK(read_state_.partitioner_.set_result_budget(
          attr_name.c_str(), *buffer_size));
    } else {
      RETURN_NOT_OK(read_state_.partitioner_.set_result_budget(
          attr_name.c_str(), *buffer_size, *buffer_var_size));
    }
  }

  // Set memory budget
  RETURN_NOT_OK(read_state_.partitioner_.set_memory_budget(
      memory_budget_, memory_budget_var_));

  read_state_.unsplittable_ = false;
  read_state_.overflowed_ = false;
  read_state_.initialized_ = true;

  return Status::Ok();
}

Status Reader::init_tile(
    uint32_t format_version, const std::string& attribute, Tile* tile) const {
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
  RETURN_NOT_OK(
      tile->init(format_version, type, tile_size, cell_size, dim_num));

  return Status::Ok();
}

Status Reader::init_tile(
    uint32_t format_version,
    const std::string& attribute,
    Tile* tile,
    Tile* tile_var) const {
  // For easy reference
  auto domain = array_schema_->domain();
  auto capacity = array_schema_->capacity();
  auto type = array_schema_->type(attribute);
  auto cell_num_per_tile =
      (has_coords()) ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * constants::cell_var_offset_size;

  // Initialize
  RETURN_NOT_OK(tile->init(
      format_version,
      constants::cell_var_offset_type,
      tile_size,
      constants::cell_var_offset_size,
      0));
  RETURN_NOT_OK(
      tile_var->init(format_version, type, tile_size, datatype_size(type), 0));
  return Status::Ok();
}

Status Reader::read_tiles(
    const std::string& attr,
    const std::vector<ResultTile*>& result_tiles) const {
  // Shortcut for empty tile vec
  if (result_tiles.empty())
    return Status::Ok();

  // Read the tiles asynchronously
  std::vector<std::future<Status>> tasks;
  RETURN_CANCEL_OR_ERROR(read_tiles(attr, result_tiles, &tasks));

  // Wait for the reads to finish and check statuses.
  auto statuses =
      storage_manager_->reader_thread_pool()->wait_all_status(tasks);
  for (const auto& st : statuses)
    RETURN_CANCEL_OR_ERROR(st);

  return Status::Ok();
}

Status Reader::read_tiles(
    const std::string& attribute,
    const std::vector<ResultTile*>& result_tiles,
    std::vector<std::future<Status>>* tasks) const {
  // For each tile, read from its fragment.
  bool var_size = array_schema_->var_size(attribute);
  auto num_tiles = static_cast<uint64_t>(result_tiles.size());
  auto encryption_key = array_->encryption_key();

  // Populate the list of regions per file to be read.
  std::map<URI, std::vector<std::tuple<uint64_t, void*, uint64_t>>> all_regions;
  for (uint64_t i = 0; i < num_tiles; i++) {
    auto& tile = result_tiles[i];
    auto it = tile->attr_tiles_.find(attribute);
    if (it == tile->attr_tiles_.end())
      it = tile->attr_tiles_
               .insert(std::pair<std::string, ResultTile::TilePair>(
                   attribute, ResultTile::TilePair(Tile(), Tile())))
               .first;

    // Initialize the tile(s)
    auto& tile_pair = it->second;
    auto& t = tile_pair.first;
    auto& t_var = tile_pair.second;
    auto& fragment = fragment_metadata_[tile->frag_idx_];
    auto format_version = fragment->format_version();
    if (!var_size) {
      RETURN_NOT_OK(init_tile(format_version, attribute, &t));
    } else {
      RETURN_NOT_OK(init_tile(format_version, attribute, &t, &t_var));
    }

    // Get information about the tile in its fragment
    auto tile_attr_uri = fragment->attr_uri(attribute);
    uint64_t tile_attr_offset;
    RETURN_NOT_OK(fragment->file_offset(
        *encryption_key, attribute, tile->tile_idx_, &tile_attr_offset));
    auto tile_size = fragment->tile_size(attribute, tile->tile_idx_);
    uint64_t tile_persisted_size;
    RETURN_NOT_OK(fragment->persisted_tile_size(
        *encryption_key, attribute, tile->tile_idx_, &tile_persisted_size));

    // Try the cache first.
    bool cache_hit;
    RETURN_NOT_OK(storage_manager_->read_from_cache(
        tile_attr_uri, tile_attr_offset, t.buffer2(), tile_size, &cache_hit));
    if (cache_hit) {
      t.set_filtered(true);
      STATS_COUNTER_ADD(reader_attr_tile_cache_hits, 1);
    } else {
      // Add the region of the fragment to be read.
      RETURN_NOT_OK(t.buffer2()->realloc(tile_persisted_size));
      t.buffer2()->set_size(tile_persisted_size);
      t.buffer2()->reset_offset();
      all_regions[tile_attr_uri].emplace_back(
          tile_attr_offset, t.buffer2()->data(), tile_persisted_size);

      STATS_COUNTER_ADD(reader_num_tile_bytes_read, tile_persisted_size);
    }

    if (var_size) {
      auto tile_attr_var_uri = fragment->attr_var_uri(attribute);
      uint64_t tile_attr_var_offset;
      RETURN_NOT_OK(fragment->file_var_offset(
          *encryption_key, attribute, tile->tile_idx_, &tile_attr_var_offset));
      uint64_t tile_var_size;
      RETURN_NOT_OK(fragment->tile_var_size(
          *encryption_key, attribute, tile->tile_idx_, &tile_var_size));
      uint64_t tile_var_persisted_size;
      RETURN_NOT_OK(fragment->persisted_tile_var_size(
          *encryption_key,
          attribute,
          tile->tile_idx_,
          &tile_var_persisted_size));

      RETURN_NOT_OK(storage_manager_->read_from_cache(
          tile_attr_var_uri,
          tile_attr_var_offset,
          t_var.buffer2(),
          tile_var_size,
          &cache_hit));

      if (cache_hit) {
        t_var.set_filtered(true);
        STATS_COUNTER_ADD(reader_attr_tile_cache_hits, 1);
      } else {
        // Add the region of the fragment to be read.
        RETURN_NOT_OK(t_var.buffer2()->realloc(tile_var_persisted_size));
        t_var.buffer2()->set_size(tile_var_persisted_size);
        t_var.buffer2()->reset_offset();
        all_regions[tile_attr_var_uri].emplace_back(
            tile_attr_var_offset,
            t_var.buffer2()->data(),
            tile_var_persisted_size);

        STATS_COUNTER_ADD(reader_num_tile_bytes_read, tile_var_persisted_size);
        STATS_COUNTER_ADD(reader_num_var_cell_bytes_read, tile_persisted_size);
        STATS_COUNTER_ADD(
            reader_num_var_cell_bytes_read, tile_var_persisted_size);
      }
    } else {
      STATS_COUNTER_ADD_IF(
          !cache_hit, reader_num_fixed_cell_bytes_read, tile_persisted_size);
    }
  }

  // Enqueue all regions to be read.
  for (const auto& item : all_regions) {
    RETURN_NOT_OK(storage_manager_->vfs()->read_all(
        item.first,
        item.second,
        storage_manager_->reader_thread_pool(),
        tasks));
  }

  STATS_COUNTER_ADD(
      reader_num_attr_tiles_touched, ((var_size ? 2 : 1) * num_tiles));

  return Status::Ok();
}

void Reader::reset_buffer_sizes() {
  for (auto& it : attr_buffers_) {
    *(it.second.buffer_size_) = it.second.original_buffer_size_;
    if (it.second.buffer_var_size_ != nullptr)
      *(it.second.buffer_var_size_) = it.second.original_buffer_var_size_;
  }
}

template <class T>
Status Reader::sort_result_coords(
    std::vector<ResultCoords<T>>* result_coords, Layout layout) const {
  STATS_FUNC_IN(reader_sort_coords);

  // TODO: do not sort if it is single fragment and
  // (i) it is single dimension, or (ii) it is global order

  auto dim_num = array_schema_->dim_num();
  auto domain = array_schema_->domain();

  if (layout == Layout::ROW_MAJOR) {
    parallel_sort(
        result_coords->begin(), result_coords->end(), RowCmp<T>(dim_num));
  } else if (layout == Layout::COL_MAJOR) {
    parallel_sort(
        result_coords->begin(), result_coords->end(), ColCmp<T>(dim_num));
  } else if (layout == Layout::GLOBAL_ORDER) {
    parallel_sort(
        result_coords->begin(), result_coords->end(), GlobalCmp<T>(domain));
  } else {
    assert(false);
  }

  return Status::Ok();

  STATS_FUNC_OUT(reader_sort_coords);
}

template <class T>
Status Reader::sparse_read() {
  STATS_FUNC_IN(reader_sparse_read);

  // Compute result coordinates from the sparse fragments
  // `sparse_result_tiles` will hold all the relevant result tiles of
  // sparse fragments
  std::vector<ResultCoords<T>> result_coords;
  std::vector<ResultTile> sparse_result_tiles;
  std::vector<std::vector<T>> tile_coords;
  RETURN_NOT_OK(compute_result_coords(
      &sparse_result_tiles, &tile_coords, &result_coords));
  std::vector<ResultTile*> result_tiles;
  for (auto& srt : sparse_result_tiles)
    result_tiles.push_back(&srt);

  // Compute result cell slabs
  std::vector<ResultCellSlab> result_cell_slabs;
  RETURN_CANCEL_OR_ERROR(
      compute_result_cell_slabs(result_coords, &result_cell_slabs));
  result_coords.clear();

  uint64_t stride = UINT64_MAX;

  // Copy coordinates
  if (std::find(attributes_.begin(), attributes_.end(), constants::coords) !=
      attributes_.end())
    RETURN_CANCEL_OR_ERROR(
        copy_cells(constants::coords, stride, result_cell_slabs));

  // Clear sparse coordinate tiles (not needed any more)
  for (auto& tile : sparse_result_tiles)
    tile.attr_tiles_.erase(constants::coords);
  tile_coords.clear();

  // Copy cells
  for (const auto& attr : attributes_) {
    if (read_state_.overflowed_)
      break;
    if (attr == constants::coords)
      continue;

    RETURN_CANCEL_OR_ERROR(read_tiles(attr, result_tiles));
    RETURN_CANCEL_OR_ERROR(filter_tiles(attr, result_tiles));
    RETURN_CANCEL_OR_ERROR(copy_cells(attr, stride, result_cell_slabs));
    clear_tiles(attr, result_tiles);
  }

  return Status::Ok();

  STATS_FUNC_OUT(reader_sparse_read);
}

void Reader::zero_out_buffer_sizes() {
  for (auto& attr_buffer : attr_buffers_) {
    if (attr_buffer.second.buffer_size_ != nullptr)
      *(attr_buffer.second.buffer_size_) = 0;
    if (attr_buffer.second.buffer_var_size_ != nullptr)
      *(attr_buffer.second.buffer_var_size_) = 0;
  }
}

template <class T>
bool Reader::sparse_tile_overwritten(
    unsigned frag_idx, uint64_t tile_idx) const {
  auto mbr = (const T*)fragment_metadata_[frag_idx]->mbr(tile_idx);
  assert(mbr != nullptr);
  auto fragment_num = fragment_metadata_.size();
  auto dim_num = array_schema_->dim_num();

  for (unsigned f = frag_idx + 1; f < fragment_num; ++f) {
    if (fragment_metadata_[f]->dense() &&
        utils::geometry::rect_in_rect<T>(
            mbr, (const T*)fragment_metadata_[f]->non_empty_domain(), dim_num))
      return true;
  }

  return false;
}

template <class T>
bool Reader::coords_overwritten(unsigned frag_idx, const T* coords) const {
  auto fragment_num = fragment_metadata_.size();
  auto dim_num = array_schema_->dim_num();

  for (unsigned f = frag_idx + 1; f < fragment_num; ++f) {
    if (fragment_metadata_[f]->dense() &&
        utils::geometry::coords_in_rect<T>(
            coords,
            (const T*)fragment_metadata_[f]->non_empty_domain(),
            dim_num))
      return true;
  }

  return false;
}

// Explicit template instantiations
template void Reader::compute_result_space_tiles<int8_t>(
    const std::vector<std::vector<uint8_t>>& tile_coords,
    const TileDomain<int8_t>& array_tile_domain,
    const std::vector<TileDomain<int8_t>>& frag_tile_domains,
    std::map<const int8_t*, ResultSpaceTile<int8_t>>* result_space_tiles);
template void Reader::compute_result_space_tiles<uint8_t>(
    const std::vector<std::vector<uint8_t>>& tile_coords,
    const TileDomain<uint8_t>& array_tile_domain,
    const std::vector<TileDomain<uint8_t>>& frag_tile_domains,
    std::map<const uint8_t*, ResultSpaceTile<uint8_t>>* result_space_tiles);
template void Reader::compute_result_space_tiles<int16_t>(
    const std::vector<std::vector<uint8_t>>& tile_coords,
    const TileDomain<int16_t>& array_tile_domain,
    const std::vector<TileDomain<int16_t>>& frag_tile_domains,
    std::map<const int16_t*, ResultSpaceTile<int16_t>>* result_space_tiles);
template void Reader::compute_result_space_tiles<uint16_t>(
    const std::vector<std::vector<uint8_t>>& tile_coords,
    const TileDomain<uint16_t>& array_tile_domain,
    const std::vector<TileDomain<uint16_t>>& frag_tile_domains,
    std::map<const uint16_t*, ResultSpaceTile<uint16_t>>* result_space_tiles);
template void Reader::compute_result_space_tiles<int32_t>(
    const std::vector<std::vector<uint8_t>>& tile_coords,
    const TileDomain<int32_t>& array_tile_domain,
    const std::vector<TileDomain<int32_t>>& frag_tile_domains,
    std::map<const int32_t*, ResultSpaceTile<int32_t>>* result_space_tiles);
template void Reader::compute_result_space_tiles<uint32_t>(
    const std::vector<std::vector<uint8_t>>& tile_coords,
    const TileDomain<uint32_t>& array_tile_domain,
    const std::vector<TileDomain<uint32_t>>& frag_tile_domains,
    std::map<const uint32_t*, ResultSpaceTile<uint32_t>>* result_space_tiles);
template void Reader::compute_result_space_tiles<int64_t>(
    const std::vector<std::vector<uint8_t>>& tile_coords,
    const TileDomain<int64_t>& array_tile_domain,
    const std::vector<TileDomain<int64_t>>& frag_tile_domains,
    std::map<const int64_t*, ResultSpaceTile<int64_t>>* result_space_tiles);
template void Reader::compute_result_space_tiles<uint64_t>(
    const std::vector<std::vector<uint8_t>>& tile_coords,
    const TileDomain<uint64_t>& array_tile_domain,
    const std::vector<TileDomain<uint64_t>>& frag_tile_domains,
    std::map<const uint64_t*, ResultSpaceTile<uint64_t>>* result_space_tiles);

}  // namespace sm
}  // namespace tiledb
