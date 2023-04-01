/**
 * @file   reader_base.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file implements class ReaderBase.
 */

#include "tiledb/sm/query/readers/reader_base.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/hilbert.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/query/hilbert_order.h"
#include "tiledb/sm/query/legacy/cell_slab_iter.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/readers/attribute_order_validator.h"
#include "tiledb/sm/query/readers/filtered_data.h"
#include "tiledb/sm/query/strategy_base.h"
#include "tiledb/sm/query/writers/domain_buffer.h"
#include "tiledb/sm/subarray/subarray.h"

namespace tiledb {
namespace sm {

using dimension_size_type = uint32_t;

class ReaderBaseStatusException : public StatusException {
 public:
  explicit ReaderBaseStatusException(const std::string& message)
      : StatusException("ReaderBase", message) {
  }
};

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

ReaderBase::ReaderBase(
    stats::Stats* stats,
    shared_ptr<Logger> logger,
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout,
    std::optional<QueryCondition>& condition)
    : StrategyBase(
          stats,
          logger,
          storage_manager,
          array,
          config,
          buffers,
          subarray,
          layout)
    , condition_(condition)
    , user_requested_timestamps_(false)
    , use_timestamps_(false)
    , initial_data_loaded_(false)
    , max_batch_size_(config.get<uint64_t>("vfs.max_batch_size").value())
    , min_batch_gap_(config.get<uint64_t>("vfs.min_batch_gap").value())
    , min_batch_size_(config.get<uint64_t>("vfs.min_batch_size").value()) {
  if (array != nullptr)
    fragment_metadata_ = array->fragment_metadata();
  timestamps_needed_for_deletes_and_updates_.resize(fragment_metadata_.size());

  if (layout_ == Layout::GLOBAL_ORDER && subarray.range_num() > 1) {
    throw ReaderBaseStatusException(
        "Cannot initialize reader; Multi-range reads are not supported on a "
        "global order query.");
  }
}

/* ********************************* */
/*          STATIC FUNCTIONS         */
/* ********************************* */

template <class T>
void ReaderBase::compute_result_space_tiles(
    const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
    const std::vector<std::vector<uint8_t>>& tile_coords,
    const TileDomain<T>& array_tile_domain,
    const std::vector<TileDomain<T>>& frag_tile_domains,
    std::map<const T*, ResultSpaceTile<T>>& result_space_tiles) {
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
    auto r = result_space_tiles.emplace(coords, ResultSpaceTile<T>());
    auto& result_space_tile = r.first->second;
    result_space_tile.set_start_coords(start_coords);

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
      result_space_tile.append_frag_domain(frag_idx, frag_domain);
      auto tile_idx = frag_tile_domains[f].tile_pos(coords);
      ResultTile result_tile(
          frag_idx,
          tile_idx,
          *(fragment_metadata[frag_idx]->array_schema()).get());
      result_space_tile.set_result_tile(frag_idx, result_tile);
    }
  }
}

/* ****************************** */
/*         PUBLIC METHODS         */
/* ****************************** */

bool ReaderBase::skip_field(
    const unsigned frag_idx, const std::string& name) const {
  auto& fragment{fragment_metadata_[frag_idx]};
  const auto format_version{fragment->format_version()};
  const auto& schema{fragment->array_schema()};

  // Applicable for zipped coordinates only to versions < 5
  if (name == constants::coords && format_version >= 5) {
    return true;
  }

  // Applicable to separate coordinates only to versions >= 5
  const auto is_dim{schema->is_dim(name)};
  if (is_dim && format_version < 5) {
    return true;
  }

  // Not a member of array schema, this field was added in array
  // schema evolution, ignore for this fragment's tile offsets
  if (!schema->is_field(name)) {
    return true;
  }

  // If the fragment doesn't include timestamps
  if (timestamps_not_present(name, frag_idx)) {
    return true;
  }

  // Continue if the fragment doesn't have delete metadata.
  if (delete_meta_not_present(name, frag_idx)) {
    return true;
  }

  return false;
}

/* ****************************** */
/*        PROTECTED METHODS       */
/* ****************************** */

bool ReaderBase::process_partial_timestamps(FragmentMetadata& frag_meta) const {
  return frag_meta.has_timestamps() &&
         frag_meta.partial_time_overlap(
             array_->timestamp_start(), array_->timestamp_end_opened_at());
}

void ReaderBase::clear_tiles(
    const std::string& name,
    const std::vector<ResultTile*>& result_tiles,
    const uint64_t min_result_tile) const {
  for (uint64_t i = min_result_tile; i < result_tiles.size(); i++) {
    result_tiles[i]->erase_tile(name);
  }
}

bool ReaderBase::need_timestamped_conditions() {
  // If we have any delete condition that falls between the timestamps of a
  // fragment with timestamps, generate timestamped query conditions.
  bool make_timestamped_conditions = false;
  for (uint64_t i = 0; i < fragment_metadata_.size(); i++) {
    if (fragment_metadata_[i]->has_timestamps()) {
      for (auto& delete_and_update_condition : delete_and_update_conditions_) {
        auto delete_timestamp =
            delete_and_update_condition.condition_timestamp();
        auto& frag_timestamps = fragment_metadata_[i]->timestamp_range();
        if (delete_timestamp >= frag_timestamps.first &&
            delete_timestamp <= frag_timestamps.second) {
          make_timestamped_conditions = true;
          timestamps_needed_for_deletes_and_updates_[i] = true;
        }
      }
    }
  }

  return make_timestamped_conditions;
}

Status ReaderBase::generate_timestamped_conditions() {
  // Generate timestamped conditions.
  timestamped_delete_and_update_conditions_.reserve(
      delete_and_update_conditions_.size());
  for (auto& delete_and_update_condition : delete_and_update_conditions_) {
    // We want the condition to be:
    // DELETE WHERE (cond) AND cell timestamp <= condition timestamp.
    // For apply, this condition needs to be be negated and become:
    // (!cond) OR cell timestamp > condition timestamp.

    // Make the timestamp condition, cell timestamp > condition timestamp.
    QueryCondition timestamp_condition;
    auto condition_timestamp =
        delete_and_update_condition.condition_timestamp();
    std::string attr = constants::timestamps;
    RETURN_NOT_OK(timestamp_condition.init(
        std::move(attr),
        &condition_timestamp,
        constants::timestamp_size,
        QueryConditionOp::GT));

    // Combine the timestamp condition and delete condition. The condition is
    // already negated.
    QueryCondition timestamped_condition(
        delete_and_update_condition.condition_marker());
    RETURN_NOT_OK(timestamp_condition.combine(
        delete_and_update_condition,
        QueryConditionCombinationOp::OR,
        &timestamped_condition));
    timestamped_delete_and_update_conditions_.push_back(timestamped_condition);
  }

  return Status::Ok();
}

void ReaderBase::reset_buffer_sizes() {
  for (auto& it : buffers_) {
    *(it.second.buffer_size_) = it.second.original_buffer_size_;
    if (it.second.buffer_var_size_ != nullptr)
      *(it.second.buffer_var_size_) = it.second.original_buffer_var_size_;
    if (it.second.validity_vector_.buffer_size() != nullptr)
      *(it.second.validity_vector_.buffer_size()) =
          it.second.original_validity_vector_size_;
  }
}

void ReaderBase::zero_out_buffer_sizes() {
  for (auto& buffer : buffers_) {
    if (buffer.second.buffer_size_ != nullptr)
      *(buffer.second.buffer_size_) = 0;
    if (buffer.second.buffer_var_size_ != nullptr)
      *(buffer.second.buffer_var_size_) = 0;
    if (buffer.second.validity_vector_.buffer_size() != nullptr)
      *(buffer.second.validity_vector_.buffer_size()) = 0;
  }
}

void ReaderBase::check_subarray() const {
  if (subarray_.layout() == Layout::GLOBAL_ORDER &&
      subarray_.range_num() != 1) {
    throw ReaderBaseStatusException(
        "Cannot initialize reader; Multi-range subarrays with "
        "global order layout are not supported");
  }
}

void ReaderBase::check_validity_buffer_sizes() const {
  // Verify that the validity buffer size for each
  // nullable attribute is large enough to contain
  // a validity value for each cell.
  for (const auto& it : buffers_) {
    const std::string& name = it.first;
    if (array_schema_.is_nullable(name)) {
      const uint64_t buffer_size = *it.second.buffer_size_;

      uint64_t min_cell_num = 0;
      if (array_schema_.var_size(name)) {
        min_cell_num = buffer_size / constants::cell_var_offset_size;

        // If the offsets buffer contains an extra element to mark
        // the offset to the end of the data buffer, we do not need
        // a validity value for that extra offset.
        if (offsets_extra_element_)
          min_cell_num = std::min<uint64_t>(0, min_cell_num - 1);
      } else {
        min_cell_num = buffer_size / array_schema_.cell_size(name);
      }

      const uint64_t buffer_validity_size =
          *it.second.validity_vector_.buffer_size();
      const uint64_t cell_validity_num =
          buffer_validity_size / constants::cell_validity_size;

      if (cell_validity_num < min_cell_num) {
        std::stringstream ss;
        ss << "Buffer sizes check failed; Invalid number of validity cells "
              "given for ";
        ss << "attribute '" << name << "'";
        ss << " (" << cell_validity_num << " < " << min_cell_num << ")";
        throw ReaderBaseStatusException(ss.str());
      }
    }
  }
}

bool ReaderBase::partial_consolidated_fragment_overlap(
    Subarray& subarray) const {
  // Fetch relevant fragments so we check only intersecting fragments
  for (const auto frag_idx : subarray.relevant_fragments()) {
    auto& fragment = fragment_metadata_[frag_idx];
    if (fragment->has_timestamps() &&
        fragment->partial_time_overlap(
            array_->timestamp_start(), array_->timestamp_end_opened_at())) {
      return true;
    }
  }

  return false;
}

Status ReaderBase::add_partial_overlap_condition() {
  // add one query condition for start time, one for end time and combine them
  QueryCondition timestamps_qc_start;
  auto ts_start = array_->timestamp_start();
  RETURN_NOT_OK(timestamps_qc_start.init(
      std::string(constants::timestamps),
      &ts_start,
      sizeof(uint64_t),
      QueryConditionOp::GE));
  QueryCondition timestamps_qc_end;
  auto ts_end = array_->timestamp_end_opened_at();
  RETURN_NOT_OK(timestamps_qc_end.init(
      std::string(constants::timestamps),
      &ts_end,
      sizeof(uint64_t),
      QueryConditionOp::LE));
  RETURN_NOT_OK(timestamps_qc_start.combine(
      timestamps_qc_end,
      QueryConditionCombinationOp::AND,
      &partial_overlap_condition_));

  return Status::Ok();
}

Status ReaderBase::add_delete_timestamps_condition() {
  // Add the delete timestamp condition if any fragments have delete metadata.
  bool add_delete_timestamps_condition = false;
  for (auto& frag_meta : fragment_metadata_) {
    if (frag_meta->has_delete_meta()) {
      add_delete_timestamps_condition = true;
      break;
    }
  }

  // The delete timestamp condition uses the open timestamp to filter cells.
  if (add_delete_timestamps_condition) {
    uint64_t open_ts = array_->timestamp_end_opened_at();
    RETURN_NOT_OK(delete_timestamps_condition_.init(
        std::string(constants::delete_timestamps),
        &open_ts,
        sizeof(uint64_t),
        open_ts == std::numeric_limits<uint64_t>::max() ?
            QueryConditionOp::GE :
            QueryConditionOp::GT));
  }

  return Status::Ok();
}

bool ReaderBase::include_timestamps(const unsigned f) const {
  auto frag_has_ts = fragment_metadata_[f]->has_timestamps();
  auto partial_overlap = fragment_metadata_[f]->partial_time_overlap(
      array_->timestamp_start(), array_->timestamp_end_opened_at());
  auto dups = array_schema_.allows_dups();
  auto timestamps_needed = timestamps_needed_for_deletes_and_updates_[f];

  return frag_has_ts && (user_requested_timestamps_ || partial_overlap ||
                         !dups || timestamps_needed);
}

Status ReaderBase::load_tile_offsets(
    const RelevantFragments& relevant_fragments,
    const std::vector<std::string>& names) {
  auto timer_se = stats_->start_timer("load_tile_offsets");
  const auto encryption_key = array_->encryption_key();

  const auto status = parallel_for(
      storage_manager_->compute_tp(),
      0,
      relevant_fragments.size(),
      [&](const uint64_t i) {
        auto frag_idx = relevant_fragments[i];
        auto& fragment = fragment_metadata_[frag_idx];

        // Filter the 'names' for format-specific names.
        std::vector<std::string> filtered_names;
        filtered_names.reserve(names.size());
        for (const auto& name : names) {
          if (skip_field(frag_idx, name)) {
            continue;
          }

          filtered_names.emplace_back(name);
        }

        RETURN_NOT_OK(fragment->load_tile_offsets(
            *encryption_key, std::move(filtered_names)));
        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status ReaderBase::load_tile_var_sizes(
    const RelevantFragments& relevant_fragments,
    const std::vector<std::string>& names) {
  auto timer_se = stats_->start_timer("load_tile_var_sizes");
  const auto encryption_key = array_->encryption_key();

  const auto status = parallel_for(
      storage_manager_->compute_tp(),
      0,
      relevant_fragments.size(),
      [&](const uint64_t i) {
        auto frag_idx = relevant_fragments[i];
        auto& fragment = fragment_metadata_[frag_idx];

        const auto& schema = fragment->array_schema();
        for (const auto& name : names) {
          // Not a member of array schema, this field was added in array
          // schema evolution, ignore for this fragment's tile var sizes.
          if (!schema->is_field(name)) {
            continue;
          }

          // Not a var size attribute.
          if (!schema->var_size(name)) {
            continue;
          }

          throw_if_not_ok(fragment->load_tile_var_sizes(*encryption_key, name));
        }

        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status ReaderBase::load_processed_conditions() {
  auto timer_se = stats_->start_timer("load_processed_conditions");
  const auto encryption_key = array_->encryption_key();

  // Load all fragments in parallel.
  const auto status = parallel_for(
      storage_manager_->compute_tp(),
      0,
      fragment_metadata_.size(),
      [&](const uint64_t i) {
        auto& fragment = fragment_metadata_[i];

        if (fragment->has_delete_meta()) {
          RETURN_NOT_OK(fragment->load_processed_conditions(*encryption_key));
        }

        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status ReaderBase::read_and_unfilter_attribute_tiles(
    const std::vector<std::string>& names,
    const std::vector<ResultTile*>& result_tiles) const {
  // The filtered data here contains the memory allocations for all of the
  // filtered data that is read by `read_attribute_tiles`. To prevent
  // modifications to the filter pipeline at the moment, the `result_tiles`
  // vector will be mutated in the following way: the `filtered_data_` and
  // `filtered_size_` of each tiles will be stored in the `Tile` objects inside
  // of `ResultTile`. `filtered_data_` will point to a memory location inside of
  // a data block of 'filtered_data'. The filtered pipeline uses
  // `filtered_data()` and `filtered_size()` to access the tile data. It also
  // uses 'clear_filtered_buffer()' to clear those values once the tile is
  // unfiltered to prevent access to memory that went away. Another refactor
  // will store those two values in another class and pass it down to the filter
  // pipeline to remove more and more data from the 'ResultTile' object and
  // eventually get rid of it altogether so that we can clarify the data flow.
  // At the end of this function call, all memory inside of 'filtered_data' has
  // been used and the tiles are unfiltered so the data can be deleted.
  auto filtered_data{read_attribute_tiles(names, result_tiles)};
  for (auto& name : names) {
    RETURN_NOT_OK(unfilter_tiles(name, result_tiles));
  }

  return Status::Ok();
}

Status ReaderBase::read_and_unfilter_coordinate_tiles(
    const std::vector<std::string>& names,
    const std::vector<ResultTile*>& result_tiles) const {
  // See the comment in 'read_and_unfilter_attribute_tiles' to get more
  // information about the lifetime of this object.
  auto filtered_data{read_coordinate_tiles(names, result_tiles)};
  for (auto& name : names) {
    RETURN_NOT_OK(unfilter_tiles(name, result_tiles));
  }

  return Status::Ok();
}

std::vector<FilteredData> ReaderBase::read_attribute_tiles(
    const std::vector<std::string>& names,
    const std::vector<ResultTile*>& result_tiles) const {
  auto timer_se = stats_->start_timer("read_attribute_tiles");
  return read_tiles(names, result_tiles);
}

std::vector<FilteredData> ReaderBase::read_coordinate_tiles(
    const std::vector<std::string>& names,
    const std::vector<ResultTile*>& result_tiles) const {
  auto timer_se = stats_->start_timer("read_coordinate_tiles");
  return read_tiles(names, result_tiles);
}

std::vector<FilteredData> ReaderBase::read_tiles(
    const std::vector<std::string>& names,
    const std::vector<ResultTile*>& result_tiles) const {
  auto timer_se = stats_->start_timer("read_tiles");
  std::vector<FilteredData> filtered_data;

  // Shortcut for empty tile vec.
  if (result_tiles.empty() || names.empty()) {
    return filtered_data;
  }

  uint64_t num_tiles_read{0};
  std::vector<ThreadPool::Task> read_tasks;
  filtered_data.reserve(names.size());

  // Run all attributes independently.
  for (auto name : names) {
    // Create the filtered data blocks. This will also kick off the read for the
    // data blocks right after the memory is allocated so that we can optimize
    // read and memory allocations.
    const bool var_sized{array_schema_.var_size(name)};
    const bool nullable{array_schema_.is_nullable(name)};
    filtered_data.emplace_back(
        *this,
        min_batch_size_,
        max_batch_size_,
        min_batch_gap_,
        fragment_metadata_,
        result_tiles,
        name,
        var_sized,
        nullable,
        storage_manager_,
        read_tasks);

    // Go through each tiles and create the attribute tiles.
    for (auto tile : result_tiles) {
      auto const fragment{fragment_metadata_[tile->frag_idx()]};
      const auto& array_schema{fragment->array_schema()};

      if (skip_field(tile->frag_idx(), name)) {
        continue;
      }

      num_tiles_read++;
      const auto tile_idx{tile->tile_idx()};

      // Construct a TileSizes class.
      ResultTile::TileSizes tile_sizes{
          fragment, name, var_sized, nullable, tile_idx};

      // Construct a tile data class.
      // See the explanation in 'read_and_unfilter_attribute_tiles' for more
      // lifetime details. The tile data class is used to transmit the location
      // of the fixed/var/nullable filtered data to the created 'TileTuple'
      // object inside of each 'ResultTile'. The filter pipeline currently uses
      // the 'ResultTile' object to access the data. Eventually, these
      // 'TileData' objects should be returned by this function and passed into
      // 'unfilter_tiles' so that the filter pipeline can stop using the
      // 'ResultTile' object to get access to the filtered data.
      ResultTile::TileData tile_data{
          filtered_data.back().fixed_filtered_data(fragment.get(), tile),
          filtered_data.back().var_filtered_data(fragment.get(), tile),
          filtered_data.back().nullable_filtered_data(fragment.get(), tile)};

      // Initialize the tile(s)
      const format_version_t format_version{fragment->format_version()};
      const auto is_dim{array_schema->is_dim(name)};
      if (is_dim) {
        const uint64_t dim_num{array_schema->dim_num()};
        for (uint64_t d = 0; d < dim_num; ++d) {
          if (array_schema->dimension_ptr(d)->name() == name) {
            tile->init_coord_tile(
                format_version, array_schema_, name, tile_sizes, tile_data, d);
            break;
          }
        }
      } else {
        tile->init_attr_tile(
            format_version, array_schema_, name, tile_sizes, tile_data);
      }
    }
  }

  stats_->add_counter("num_tiles_read", num_tiles_read);

  // Wait for the read tasks to finish.
  auto statuses{storage_manager_->io_tp()->wait_all_status(read_tasks)};
  for (const auto& st : statuses) {
    throw_if_not_ok(st);
  }

  return filtered_data;
}

tuple<Status, optional<uint64_t>, optional<uint64_t>, optional<uint64_t>>
ReaderBase::load_tile_chunk_data(
    const std::string& name,
    ResultTile* const tile,
    const bool var_size,
    const bool nullable,
    ChunkData& tile_chunk_data,
    ChunkData& tile_chunk_var_data,
    ChunkData& tile_chunk_validity_data) const {
  assert(tile);

  if (skip_field(tile->frag_idx(), name)) {
    return {Status::Ok(), 0, 0, 0};
  }

  auto tile_tuple = tile->tile_tuple(name);

  // Skip non-existent attributes/dimensions (e.g. coords in the
  // dense case).
  if (tile_tuple == nullptr || tile_tuple->fixed_tile().filtered_size() == 0) {
    return {Status::Ok(), 0, 0, 0};
  }

  const auto t = &tile_tuple->fixed_tile();
  const auto t_var = var_size ? &tile_tuple->var_tile() : nullptr;
  const auto t_validity = nullable ? &tile_tuple->validity_tile() : nullptr;

  uint64_t unfiltered_tile_size = 0, unfiltered_tile_var_size = 0,
           unfiltered_tile_validity_size = 0;

  const FilterPipeline& filters = array_schema_.filters(name);
  if (!var_size ||
      !filters.skip_offsets_filtering(t_var->type(), array_schema_.version())) {
    unfiltered_tile_size = t->load_chunk_data(tile_chunk_data);
  }

  if (var_size) {
    unfiltered_tile_var_size = t_var->load_chunk_data(tile_chunk_var_data);
  }
  if (nullable) {
    unfiltered_tile_validity_size =
        t_validity->load_chunk_data(tile_chunk_validity_data);
  }
  return {
      Status::Ok(),
      unfiltered_tile_size,
      unfiltered_tile_var_size,
      unfiltered_tile_validity_size};
}

Status ReaderBase::zip_tile_coordinates(
    const std::string& name, Tile* tile) const {
  if (tile->stores_coords()) {
    bool using_compression =
        array_schema_.filters(name).get_filter<CompressionFilter>() != nullptr;
    auto version = tile->format_version();
    if (version > 1 || using_compression) {
      RETURN_NOT_OK(tile->zip_coordinates());
    }
  }
  return Status::Ok();
}

Status ReaderBase::post_process_unfiltered_tile(
    const std::string& name,
    ResultTile* const tile,
    const bool var_size,
    const bool nullable) const {
  assert(tile);

  if (skip_field(tile->frag_idx(), name)) {
    return Status::Ok();
  }

  auto tile_tuple = tile->tile_tuple(name);

  // Skip non-existent attributes/dimensions (e.g. coords in the
  // dense case).
  if (tile_tuple == nullptr || tile_tuple->fixed_tile().filtered_size() == 0) {
    return Status::Ok();
  }

  auto& t = tile_tuple->fixed_tile();
  t.clear_filtered_buffer();

  throw_if_not_ok(zip_tile_coordinates(name, &t));

  if (var_size) {
    auto& t_var = tile_tuple->var_tile();
    t_var.clear_filtered_buffer();
    throw_if_not_ok(zip_tile_coordinates(name, &t_var));
  }

  if (nullable) {
    auto& t_validity = tile_tuple->validity_tile();
    t_validity.clear_filtered_buffer();
    throw_if_not_ok(zip_tile_coordinates(name, &t_validity));
  }

  return Status::Ok();
}

Status ReaderBase::unfilter_tiles(
    const std::string& name,
    const std::vector<ResultTile*>& result_tiles) const {
  const auto stat_type = (array_schema_.is_attr(name)) ? "unfilter_attr_tiles" :
                                                         "unfilter_coord_tiles";

  const auto timer_se = stats_->start_timer(stat_type);
  auto var_size = array_schema_.var_size(name);
  auto nullable = array_schema_.is_nullable(name);
  auto num_tiles = static_cast<uint64_t>(result_tiles.size());

  auto chunking = true;
  if (var_size) {
    auto filters = array_schema_.filters(name);
    chunking = filters.use_tile_chunking(
        var_size, array_schema_.version(), array_schema_.type(name));
  }

  if (num_tiles == 0) {
    return Status::Ok();
  }

  // Compute parallelization parameters.
  uint64_t num_range_threads = 1;
  const auto num_threads = storage_manager_->compute_tp()->concurrency_level();
  if (chunking && num_tiles < num_threads) {
    // Ceil the division between thread_num and num_tiles.
    num_range_threads = 1 + ((num_threads - 1) / num_tiles);
  }

  // Vectors with all the necessary chunk data for unfiltering
  std::vector<ChunkData> tiles_chunk_data(num_tiles);
  std::vector<ChunkData> tiles_chunk_var_data(num_tiles);
  std::vector<ChunkData> tiles_chunk_validity_data(num_tiles);
  // Vectors with the sizes of all unfiltered tile buffers
  std::vector<uint64_t> unfiltered_tile_size(num_tiles);
  std::vector<uint64_t> unfiltered_tile_var_size(num_tiles);
  std::vector<uint64_t> unfiltered_tile_validity_size(num_tiles);

  // Pre-compute chunk offsets.
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, num_tiles, [&, this](uint64_t i) {
        auto&& [st, tile_size, tile_var_size, tile_validity_size] =
            load_tile_chunk_data(
                name,
                result_tiles[i],
                var_size,
                nullable,
                tiles_chunk_data[i],
                tiles_chunk_var_data[i],
                tiles_chunk_validity_data[i]);
        RETURN_NOT_OK(st);
        unfiltered_tile_size[i] = tile_size.value();
        unfiltered_tile_var_size[i] = tile_var_size.value();
        unfiltered_tile_validity_size[i] = tile_validity_size.value();
        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, throw_if_not_ok(logger_->status(status)));

  if (tiles_chunk_data.empty())
    return Status::Ok();

  // Unfilter all tiles/chunks in parallel using the precomputed offsets.
  status = parallel_for_2d(
      storage_manager_->compute_tp(),
      0,
      num_tiles,
      0,
      num_range_threads,
      [&](uint64_t i, uint64_t range_thread_idx) {
        return unfilter_tile(
            name,
            result_tiles[i],
            var_size,
            nullable,
            range_thread_idx,
            num_range_threads,
            tiles_chunk_data[i],
            tiles_chunk_var_data[i],
            tiles_chunk_validity_data[i]);
      });
  RETURN_CANCEL_OR_ERROR(status);

  // Perform required post-processing of unfiltered tiles
  for (size_t i = 0; i < num_tiles; i++) {
    RETURN_NOT_OK(post_process_unfiltered_tile(
        name, result_tiles[i], var_size, nullable));
  }

  return Status::Ok();
}

Status ReaderBase::unfilter_tile(
    const std::string& name,
    ResultTile* const tile,
    const bool var_size,
    const bool nullable,
    const uint64_t thread_idx,
    const uint64_t num_threads,
    const ChunkData& tile_chunk_fixed_data,
    const ChunkData& tile_chunk_var_data,
    const ChunkData& tile_chunk_validity_data) const {
  assert(tile);

  if (skip_field(tile->frag_idx(), name)) {
    return Status::Ok();
  }

  auto tile_tuple = tile->tile_tuple(name);

  // Skip non-existent attributes/dimensions (e.g. coords in the
  // dense case).
  if (tile_tuple == nullptr || tile_tuple->fixed_tile().filtered_size() == 0) {
    return Status::Ok();
  }

  auto t = &tile_tuple->fixed_tile();
  auto t_var = var_size ? &tile_tuple->var_tile() : nullptr;
  auto t_validity = nullable ? &tile_tuple->validity_tile() : nullptr;

  FilterPipeline fixed_filters;
  FilterPipeline var_filters;
  FilterPipeline validity_filters;

  // Create our filter pipelines
  if (!var_size) {
    fixed_filters = array_schema_.filters(name);
    RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
        &fixed_filters, array_->get_encryption_key()));
  } else {
    fixed_filters = array_schema_.cell_var_offsets_filters();
    RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
        &fixed_filters, array_->get_encryption_key()));

    var_filters = array_schema_.filters(name);
    RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
        &var_filters, array_->get_encryption_key()));
  }

  if (nullable) {
    validity_filters = array_schema_.cell_validity_filters();
    RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
        &validity_filters, array_->get_encryption_key()));
  }

  bool skip_offsets_filtering = false;
  if (var_size) {
    skip_offsets_filtering = var_filters.skip_offsets_filtering(
        t_var->type(), array_schema_.version());
  }

  auto concurrency_level = storage_manager_->compute_tp()->concurrency_level();

  // Unfiltered fixed data
  if (!skip_offsets_filtering &&
      thread_idx <= tile_chunk_fixed_data.filtered_chunks_.size() - 1) {
    // Compute chunk boundaries
    auto&& [t_min, t_max] = compute_chunk_min_max(
        tile_chunk_fixed_data.chunk_offsets_.size(), num_threads, thread_idx);

    // Reverse the tile filters.
    RETURN_NOT_OK(fixed_filters.run_reverse(
        stats_,
        t,
        nullptr,
        tile_chunk_fixed_data,
        t_min,
        t_max,
        concurrency_level,
        storage_manager_->config()));
  }

  // Prevent processing past the end of chunks in case there are more
  // threads than chunks.
  if (var_size &&
      thread_idx <= tile_chunk_var_data.filtered_chunks_.size() - 1) {
    auto&& [tvar_min, tvar_max] = compute_chunk_min_max(
        tile_chunk_var_data.chunk_offsets_.size(), num_threads, thread_idx);
    // Reverse the filters of tile var data
    RETURN_NOT_OK(var_filters.run_reverse(
        stats_,
        t_var,
        skip_offsets_filtering ? t : nullptr,
        tile_chunk_var_data,
        tvar_min,
        tvar_max,
        concurrency_level,
        storage_manager_->config()));
  }

  // Prevent processing past the end of chunks in case there are more
  // threads than chunks.
  if (nullable &&
      thread_idx <= tile_chunk_validity_data.filtered_chunks_.size() - 1) {
    // Compute chunk boundaries
    auto&& [tval_min, tval_max] = compute_chunk_min_max(
        tile_chunk_validity_data.chunk_offsets_.size(),
        num_threads,
        thread_idx);

    // Reverse the tile validity filters.
    RETURN_NOT_OK(validity_filters.run_reverse(
        stats_,
        t_validity,
        nullptr,
        tile_chunk_validity_data,
        tval_min,
        tval_max,
        concurrency_level,
        storage_manager_->config()));
  }

  return Status::Ok();
}

tuple<uint64_t, uint64_t> ReaderBase::compute_chunk_min_max(
    const uint64_t num_chunks,
    const uint64_t num_range_threads,
    const uint64_t thread_idx) const {
  auto t_part_num = std::min(num_chunks, num_range_threads);
  auto t_min = (thread_idx * num_chunks + t_part_num - 1) / t_part_num;
  auto t_max = std::min(
      ((thread_idx + 1) * num_chunks + t_part_num - 1) / t_part_num,
      num_chunks);

  return {t_min, t_max};
}

uint64_t ReaderBase::offsets_bytesize() const {
  return offsets_bitsize_ == 32 ? sizeof(uint32_t) :
                                  constants::cell_var_offset_size;
}

uint64_t ReaderBase::get_attribute_tile_size(
    const std::string& name, unsigned f, uint64_t t) {
  uint64_t tile_size = 0;
  if (!fragment_metadata_[f]->array_schema()->is_field(name)) {
    return tile_size;
  }

  tile_size += fragment_metadata_[f]->tile_size(name, t);

  if (array_schema_.var_size(name)) {
    tile_size += fragment_metadata_[f]->tile_var_size(name, t);
  }

  if (array_schema_.is_nullable(name)) {
    tile_size +=
        fragment_metadata_[f]->cell_num(t) * constants::cell_validity_size;
  }

  return tile_size;
}

template <class T>
void ReaderBase::compute_result_space_tiles(
    const Subarray& subarray,
    const Subarray& partitioner_subarray,
    std::map<const T*, ResultSpaceTile<T>>& result_space_tiles) const {
  // For easy reference
  auto domain = array_schema_.domain().domain();
  auto tile_extents = array_schema_.domain().tile_extents();
  auto tile_order = array_schema_.tile_order();

  // Compute fragment tile domains
  std::vector<TileDomain<T>> frag_tile_domains;
  auto relevant_frags = partitioner_subarray.relevant_fragments();
  for (size_t i = relevant_frags.size(); i > 0; --i) {
    auto f = relevant_frags[i - 1];
    if (fragment_metadata_[f]->dense()) {
      frag_tile_domains.emplace_back(
          f,
          domain,
          fragment_metadata_[f]->non_empty_domain(),
          tile_extents,
          tile_order);
    }
  }

  // Get tile coords and array domain
  const auto& tile_coords = subarray.tile_coords();
  TileDomain<T> array_tile_domain(
      UINT32_MAX, domain, domain, tile_extents, tile_order);

  // Compute result space tiles
  compute_result_space_tiles<T>(
      fragment_metadata_,
      tile_coords,
      array_tile_domain,
      frag_tile_domains,
      result_space_tiles);
}

bool ReaderBase::has_coords() const {
  for (const auto& it : buffers_) {
    if (it.first == constants::coords || array_schema_.is_dim(it.first)) {
      return true;
    }
  }

  return false;
}

template <typename IndexType>
tuple<Range, std::vector<const void*>, std::vector<uint64_t>>
ReaderBase::cache_dimension_label_data() {
  // Cache the non empty domains and tile index for the first tile of each
  // fragment.
  auto index_dim{array_schema_.domain().dimension_ptr(0)};
  const IndexType* dim_dom = index_dim->domain().typed_data<IndexType>();
  auto tile_extent{index_dim->tile_extent().rvalue_as<IndexType>()};
  std::vector<const void*> non_empty_domains(fragment_metadata_.size());
  std::vector<uint64_t> frag_first_array_tile_idx(fragment_metadata_.size());
  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(),
      0,
      fragment_metadata_.size(),
      [&](unsigned f) {
        non_empty_domains[f] =
            fragment_metadata_[f]->non_empty_domain()[0].data();
        auto ned = static_cast<const IndexType*>(non_empty_domains[f]);
        frag_first_array_tile_idx[f] =
            index_dim->tile_idx<IndexType>(ned[0], dim_dom[0], tile_extent);

        return Status::Ok();
      }));

  // Compute the array non empty domain.
  IndexType min = std::numeric_limits<IndexType>::max();
  IndexType max = std::numeric_limits<IndexType>::min();
  for (uint64_t f = 0; f < fragment_metadata_.size(); f++) {
    auto ned = static_cast<const IndexType*>(non_empty_domains[f]);
    min = std::min(min, ned[0]);
    max = std::max(max, ned[1]);
  }

  return {
      Range(&min, &max, sizeof(IndexType)),
      std::move(non_empty_domains),
      std::move(frag_first_array_tile_idx)};
}

template <typename IndexType, typename AttributeType>
void ReaderBase::validate_attribute_order(
    std::string& attribute_name,
    bool increasing_data,
    Range& array_non_empty_domain,
    std::vector<const void*>& non_empty_domains,
    std::vector<uint64_t>& frag_first_array_tile_idx) {
  // For only one fragment, no work to do.
  if (fragment_metadata_.size() == 1) {
    return;
  }

  // For easy reference.
  auto array_min_idx = array_non_empty_domain.typed_data<IndexType>()[0];
  auto array_max_idx = array_non_empty_domain.typed_data<IndexType>()[1];
  auto index_dim{array_schema_.domain().dimension_ptr(0)};
  auto index_name = index_dim->name();

  // See if some values will already be processed by previous fragments.
  AttributeOrderValidator validator(attribute_name, fragment_metadata_.size());
  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(),
      0,
      fragment_metadata_.size(),
      [&](uint64_t f) {
        validator.find_fragments_to_check(
            array_min_idx, array_max_idx, f, non_empty_domains);

        return Status::Ok();
      }));

  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(),
      0,
      fragment_metadata_.size(),
      [&](int64_t f) {
        validator.validate_without_loading_tiles<IndexType, AttributeType>(
            array_schema_,
            index_dim,
            increasing_data,
            f,
            non_empty_domains,
            fragment_metadata_,
            frag_first_array_tile_idx);
        return Status::Ok();
      }));

  // If we need tiles to finish order validation, load them, then finish the
  // validation.
  if (validator.need_to_load_tiles()) {
    auto tiles_to_load = validator.tiles_to_load();

    throw_if_not_ok(
        read_and_unfilter_attribute_tiles({attribute_name}, tiles_to_load));

    // Validate bounds not validated using tile data.
    throw_if_not_ok(parallel_for(
        storage_manager_->compute_tp(),
        0,
        fragment_metadata_.size(),
        [&](unsigned f) {
          validator.validate_with_loaded_tiles<IndexType, AttributeType>(
              index_dim,
              increasing_data,
              f,
              non_empty_domains,
              fragment_metadata_,
              frag_first_array_tile_idx);
          return Status::Ok();
        }));
  }
}

template <typename IndexType>
void ReaderBase::validate_attribute_order(
    Datatype attribute_type,
    std::string& attribute_name,
    bool increasing_data,
    Range& array_non_empty_domain,
    std::vector<const void*>& non_empty_domains,
    std::vector<uint64_t>& frag_first_array_tile_idx) {
  auto timer_se = stats_->start_timer("validate_attribute_order");

  switch (attribute_type) {
    case Datatype::INT8:
      validate_attribute_order<IndexType, int8_t>(
          attribute_name,
          increasing_data,
          array_non_empty_domain,
          non_empty_domains,
          frag_first_array_tile_idx);
      break;
    case Datatype::UINT8:
      validate_attribute_order<IndexType, uint8_t>(
          attribute_name,
          increasing_data,
          array_non_empty_domain,
          non_empty_domains,
          frag_first_array_tile_idx);
      break;
    case Datatype::INT16:
      validate_attribute_order<IndexType, int16_t>(
          attribute_name,
          increasing_data,
          array_non_empty_domain,
          non_empty_domains,
          frag_first_array_tile_idx);
      break;
    case Datatype::UINT16:
      validate_attribute_order<IndexType, uint16_t>(
          attribute_name,
          increasing_data,
          array_non_empty_domain,
          non_empty_domains,
          frag_first_array_tile_idx);
      break;
    case Datatype::INT32:
      validate_attribute_order<IndexType, int32_t>(
          attribute_name,
          increasing_data,
          array_non_empty_domain,
          non_empty_domains,
          frag_first_array_tile_idx);
      break;
    case Datatype::UINT32:
      validate_attribute_order<IndexType, uint32_t>(
          attribute_name,
          increasing_data,
          array_non_empty_domain,
          non_empty_domains,
          frag_first_array_tile_idx);
      break;
    case Datatype::INT64:
      validate_attribute_order<IndexType, int64_t>(
          attribute_name,
          increasing_data,
          array_non_empty_domain,
          non_empty_domains,
          frag_first_array_tile_idx);
      break;
    case Datatype::UINT64:
      validate_attribute_order<IndexType, uint64_t>(
          attribute_name,
          increasing_data,
          array_non_empty_domain,
          non_empty_domains,
          frag_first_array_tile_idx);
      break;
    case Datatype::FLOAT32:
      validate_attribute_order<IndexType, float>(
          attribute_name,
          increasing_data,
          array_non_empty_domain,
          non_empty_domains,
          frag_first_array_tile_idx);
      break;
    case Datatype::FLOAT64:
      validate_attribute_order<IndexType, double>(
          attribute_name,
          increasing_data,
          array_non_empty_domain,
          non_empty_domains,
          frag_first_array_tile_idx);
      break;
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      validate_attribute_order<IndexType, int64_t>(
          attribute_name,
          increasing_data,
          array_non_empty_domain,
          non_empty_domains,
          frag_first_array_tile_idx);
      break;
    case Datatype::STRING_ASCII:
      validate_attribute_order<IndexType, std::string_view>(
          attribute_name,
          increasing_data,
          array_non_empty_domain,
          non_empty_domains,
          frag_first_array_tile_idx);
      break;
    default:
      throw ReaderBaseStatusException("Invalid attribute type");
  }
}

// Explicit template instantiations
template void ReaderBase::compute_result_space_tiles<int8_t>(
    const Subarray&,
    const Subarray&,
    std::map<const int8_t*, ResultSpaceTile<int8_t>>&) const;
template void ReaderBase::compute_result_space_tiles<uint8_t>(
    const Subarray&,
    const Subarray&,
    std::map<const uint8_t*, ResultSpaceTile<uint8_t>>&) const;
template void ReaderBase::compute_result_space_tiles<int16_t>(
    const Subarray&,
    const Subarray&,
    std::map<const int16_t*, ResultSpaceTile<int16_t>>&) const;
template void ReaderBase::compute_result_space_tiles<uint16_t>(
    const Subarray&,
    const Subarray&,
    std::map<const uint16_t*, ResultSpaceTile<uint16_t>>&) const;
template void ReaderBase::compute_result_space_tiles<int32_t>(
    const Subarray&,
    const Subarray&,
    std::map<const int32_t*, ResultSpaceTile<int32_t>>&) const;
template void ReaderBase::compute_result_space_tiles<uint32_t>(
    const Subarray&,
    const Subarray&,
    std::map<const uint32_t*, ResultSpaceTile<uint32_t>>&) const;
template void ReaderBase::compute_result_space_tiles<int64_t>(
    const Subarray&,
    const Subarray&,
    std::map<const int64_t*, ResultSpaceTile<int64_t>>&) const;
template void ReaderBase::compute_result_space_tiles<uint64_t>(
    const Subarray&,
    const Subarray&,
    std::map<const uint64_t*, ResultSpaceTile<uint64_t>>&) const;
template tuple<Range, std::vector<const void*>, std::vector<uint64_t>>
ReaderBase::cache_dimension_label_data<int8_t>();
template tuple<Range, std::vector<const void*>, std::vector<uint64_t>>
ReaderBase::cache_dimension_label_data<uint8_t>();
template tuple<Range, std::vector<const void*>, std::vector<uint64_t>>
ReaderBase::cache_dimension_label_data<int16_t>();
template tuple<Range, std::vector<const void*>, std::vector<uint64_t>>
ReaderBase::cache_dimension_label_data<uint16_t>();
template tuple<Range, std::vector<const void*>, std::vector<uint64_t>>
ReaderBase::cache_dimension_label_data<int32_t>();
template tuple<Range, std::vector<const void*>, std::vector<uint64_t>>
ReaderBase::cache_dimension_label_data<uint32_t>();
template tuple<Range, std::vector<const void*>, std::vector<uint64_t>>
ReaderBase::cache_dimension_label_data<int64_t>();
template tuple<Range, std::vector<const void*>, std::vector<uint64_t>>
ReaderBase::cache_dimension_label_data<uint64_t>();
template void ReaderBase::validate_attribute_order<int8_t>(
    Datatype,
    std::string&,
    bool,
    Range&,
    std::vector<const void*>&,
    std::vector<uint64_t>&);
template void ReaderBase::validate_attribute_order<uint8_t>(
    Datatype,
    std::string&,
    bool,
    Range&,
    std::vector<const void*>&,
    std::vector<uint64_t>&);
template void ReaderBase::validate_attribute_order<int16_t>(
    Datatype,
    std::string&,
    bool,
    Range&,
    std::vector<const void*>&,
    std::vector<uint64_t>&);
template void ReaderBase::validate_attribute_order<uint16_t>(
    Datatype,
    std::string&,
    bool,
    Range&,
    std::vector<const void*>&,
    std::vector<uint64_t>&);
template void ReaderBase::validate_attribute_order<int32_t>(
    Datatype,
    std::string&,
    bool,
    Range&,
    std::vector<const void*>&,
    std::vector<uint64_t>&);
template void ReaderBase::validate_attribute_order<uint32_t>(
    Datatype,
    std::string&,
    bool,
    Range&,
    std::vector<const void*>&,
    std::vector<uint64_t>&);
template void ReaderBase::validate_attribute_order<int64_t>(
    Datatype,
    std::string&,
    bool,
    Range&,
    std::vector<const void*>&,
    std::vector<uint64_t>&);
template void ReaderBase::validate_attribute_order<uint64_t>(
    Datatype,
    std::string&,
    bool,
    Range&,
    std::vector<const void*>&,
    std::vector<uint64_t>&);

}  // namespace sm
}  // namespace tiledb
