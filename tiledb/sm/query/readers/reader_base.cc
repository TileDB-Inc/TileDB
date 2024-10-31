/**
 * @file   reader_base.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
#include "tiledb/common/indexed_list.h"
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
#include "tiledb/type/apply_with_type.h"

namespace tiledb::sm {

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
    stats::Stats* stats, shared_ptr<Logger> logger, StrategyParams& params)
    : StrategyBase(stats, logger, params)
    , memory_tracker_(params.query_memory_tracker())
    , condition_(params.condition())
    , user_requested_timestamps_(false)
    , deletes_consolidation_no_purge_(
          buffers_.count(constants::delete_timestamps) != 0)
    , use_timestamps_(false)
    , initial_data_loaded_(false)
    , max_batch_size_(config_.get<uint64_t>("vfs.max_batch_size").value())
    , min_batch_gap_(config_.get<uint64_t>("vfs.min_batch_gap").value())
    , min_batch_size_(config_.get<uint64_t>("vfs.min_batch_size").value())
    , aggregate_buffers_(params.aggregate_buffers()) {
  if (params.array() != nullptr)
    fragment_metadata_ = params.array()->fragment_metadata();
  timestamps_needed_for_deletes_and_updates_.resize(fragment_metadata_.size());

  if (layout_ == Layout::GLOBAL_ORDER && subarray_.range_num() > 1) {
    throw ReaderBaseStatusException(
        "Cannot initialize reader; Multi-range reads are not supported on a "
        "global order query.");
  }

  // Validate the aggregates and store the requested aggregates by field name.
  for (auto& aggregate : params.default_channel_aggregates()) {
    aggregate.second->validate_output_buffer(
        aggregate.first, aggregate_buffers_);
    aggregates_[aggregate.second->field_name()].emplace_back(aggregate.second);
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

std::vector<uint64_t> ReaderBase::tile_offset_sizes() {
  auto timer_se = stats_->start_timer("tile_offset_sizes");

  // For easy reference.
  std::vector<uint64_t> ret(fragment_metadata_.size());
  const auto dim_num = array_schema_.dim_num();

  // Compute the size of tile offsets per fragments.
  const auto relevant_fragments = subarray_.relevant_fragments();
  throw_if_not_ok(parallel_for(
      &resources_.compute_tp(), 0, relevant_fragments.size(), [&](uint64_t i) {
        // For easy reference.
        auto frag_idx = relevant_fragments[i];
        auto& fragment = fragment_metadata_[frag_idx];
        const auto& schema = fragment->array_schema();
        const auto tile_num = fragment->tile_num();
        const auto dense = schema->dense();

        // Compute the number of dimensions/attributes requiring offsets.
        uint64_t num = 0;

        // For fragments with version smaller than 5 we have zipped coords.
        // Otherwise we load each dimensions independently.
        if (!dense) {
          if (fragment->version() < 5) {
            num = 1;
          } else {
            for (unsigned d = 0; d < dim_num; ++d) {
              // Fixed tile (offsets or fixed data).
              num++;

              // If var size, we load var offsets and var tile sizes.
              if (is_dim_var_size_[d]) {
                num += 2;
              }
            }
          }
        }

        // Process everything loaded for query condition.
        for (auto& name : qc_loaded_attr_names_) {
          // Not a member of array schema, this field was added in array
          // schema evolution, ignore for this fragment's tile offsets.
          // Also skip dimensions.
          if (!schema->is_field(name) || schema->is_dim(name)) {
            continue;
          }

          // Fixed tile (offsets or fixed data).
          num++;

          // If var size, we load var offsets and var tile sizes.
          const auto attr = schema->attribute(name);
          num += 2 * attr->var_size();

          // If nullable, we load nullable offsets.
          num += attr->nullable();
        }

        // Process everything loaded for user requested data.
        for (auto& it : buffers_) {
          const auto& name = it.first;

          // Skip dimensions and attributes loaded by query condition as they
          // are processed above. Special attributes (timestamps, delete
          // timestamps, etc.) are processed below.
          if (array_schema_.is_dim(name) || !schema->is_field(name) ||
              qc_loaded_attr_names_set_.count(name) != 0 ||
              schema->is_special_attribute(name)) {
            continue;
          }

          // Fixed tile (offsets or fixed data).
          num++;

          // If var size, we load var offsets and var tile sizes.
          const auto attr = schema->attribute(name);
          num += 2 * attr->var_size();

          // If nullable, we load nullable offsets.
          num += attr->nullable();
        }

        if (!dense) {
          // Add timestamps if required.
          if (!timestamps_not_present(constants::timestamps, frag_idx)) {
            num++;
          }

          // Add delete metadata if required.
          if (!delete_meta_not_present(
                  constants::delete_timestamps, frag_idx)) {
            num++;
            num += deletes_consolidation_no_purge_;
          }
        }

        // Finally set the size of the loaded data.

        // The expected size of the tile offsets
        unsigned offsets_size = num * tile_num * sizeof(uint64_t);

        // Other than the offsets themselves, there is also memory used for the
        // initialization of the vectors that hold them. This initialization
        // takes place in LoadedFragmentMetadata::resize_offsets()

        // Calculate the number of fields
        unsigned num_fields = schema->attribute_num() + 1 +
                              fragment->has_timestamps() +
                              fragment->has_delete_meta() * 2;

        // If version < 5 we use zipped coordinates, otherwise separate
        num_fields += (fragment->version() >= 5) ? schema->dim_num() : 0;

        // The additional memory required for the vectors to
        // store the tile offsets. The number of fields is calculated above.
        // Each vector requires 32 bytes. Each field requires 4 vectors. These
        // are: tile_offsets_, tile_var_offsets_, tile_var_sizes_,
        // tile_validity_offsets_ and are located in loaded_fragment_metadata.h
        unsigned offsets_init_size = num_fields * 4 * 32;

        ret[frag_idx] = offsets_size + offsets_init_size;
        return Status::Ok();
      }));

  return ret;
}

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

void ReaderBase::check_subarray(bool check_ranges_oob) const {
  if (subarray_.layout() == Layout::GLOBAL_ORDER &&
      subarray_.range_num() != 1) {
    throw ReaderBaseStatusException(
        "Cannot initialize reader; Multi-range subarrays with "
        "global order layout are not supported");
  }
  if (check_ranges_oob) {
    subarray_.check_oob();
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

void ReaderBase::load_tile_offsets(
    const RelevantFragments& relevant_fragments,
    const std::vector<std::string>& names) {
  auto timer_se = stats_->start_timer("load_tile_offsets");
  const auto encryption_key = array_->encryption_key();

  throw_if_not_ok(parallel_for(
      &resources_.compute_tp(),
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

        fragment->loaded_metadata()->load_tile_offsets(
            *encryption_key, filtered_names);
        return Status::Ok();
      }));
}

void ReaderBase::load_tile_var_sizes(
    const RelevantFragments& relevant_fragments,
    const std::vector<std::string>& names) {
  auto timer_se = stats_->start_timer("load_tile_var_sizes");
  const auto encryption_key = array_->encryption_key();

  throw_if_not_ok(parallel_for(
      &resources_.compute_tp(),
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

          fragment->loaded_metadata()->load_tile_var_sizes(
              *encryption_key, name);
        }

        return Status::Ok();
      }));
}

void ReaderBase::load_tile_metadata(
    const RelevantFragments& relevant_fragments,
    const std::vector<std::string>& names) {
  auto timer_se = stats_->start_timer("load_tile_metadata");
  const auto encryption_key = array_->encryption_key();

  throw_if_not_ok(parallel_for(
      &resources_.compute_tp(),
      0,
      relevant_fragments.size(),
      [&](const uint64_t i) {
        auto frag_idx = relevant_fragments[i];
        auto& fragment = fragment_metadata_[frag_idx];

        // Generate the list of name with aggregates.
        const auto& schema = fragment->array_schema();
        std::vector<std::string> to_load;
        for (auto& n : names) {
          // Not a member of array schema, this field was added in array
          // schema evolution, ignore for this fragment's tile metadata.
          if (!schema->is_field(n)) {
            continue;
          }

          if (aggregates_.count(n) != 0) {
            to_load.emplace_back(n);
          }
        }

        fragment->loaded_metadata()->load_tile_max_values(
            *encryption_key, to_load);
        fragment->loaded_metadata()->load_tile_min_values(
            *encryption_key, to_load);
        fragment->loaded_metadata()->load_tile_sum_values(
            *encryption_key, to_load);
        fragment->loaded_metadata()->load_tile_null_count_values(
            *encryption_key, to_load);

        return Status::Ok();
      }));
}

void ReaderBase::load_processed_conditions() {
  auto timer_se = stats_->start_timer("load_processed_conditions");
  const auto encryption_key = array_->encryption_key();

  // Load all fragments in parallel.
  throw_if_not_ok(parallel_for(
      &resources_.compute_tp(),
      0,
      fragment_metadata_.size(),
      [&](const uint64_t i) {
        auto& fragment = fragment_metadata_[i];

        if (fragment->has_delete_meta()) {
          fragment->loaded_metadata()->load_processed_conditions(
              *encryption_key);
        }

        return Status::Ok();
      }));
}

Status ReaderBase::read_and_unfilter_attribute_tiles(
    const std::vector<NameToLoad>& names,
    const std::vector<ResultTile*>& result_tiles) {
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
  read_attribute_tiles(names, result_tiles);
  for (auto& name : names) {
    RETURN_NOT_OK(
        unfilter_tiles(name.name(), name.validity_only(), result_tiles));
  }

  return Status::Ok();
}

Status ReaderBase::read_and_unfilter_coordinate_tiles(
    const std::vector<std::string>& names,
    const std::vector<ResultTile*>& result_tiles) {
  // See the comment in 'read_and_unfilter_attribute_tiles' to get more
  // information about the lifetime of this object.
  read_coordinate_tiles(names, result_tiles);
  for (auto& name : names) {
    RETURN_NOT_OK(unfilter_tiles(name, false, result_tiles));
  }

  return Status::Ok();
}

void ReaderBase::read_attribute_tiles(
    const std::vector<NameToLoad>& names,
    const std::vector<ResultTile*>& result_tiles) const {
  auto timer_se = stats_->start_timer("read_attribute_tiles");
  return read_tiles(names, result_tiles);
}

void ReaderBase::read_coordinate_tiles(
    const std::vector<std::string>& names,
    const std::vector<ResultTile*>& result_tiles) const {
  auto timer_se = stats_->start_timer("read_coordinate_tiles");
  return read_tiles(NameToLoad::from_string_vec(names), result_tiles);
}

void ReaderBase::read_tiles(
    const std::vector<NameToLoad>& names,
    const std::vector<ResultTile*>& result_tiles) const {
  auto timer_se = stats_->start_timer("read_tiles");

  // Shortcut for empty tile vec.
  if (result_tiles.empty() || names.empty()) {
    return;
  }

  uint64_t num_tiles_read{0};

  // Run all attributes independently.
  for (auto n : names) {
    auto& name = n.name();
    auto val_only = n.validity_only();

    // Create the filtered data blocks. This will also kick off the read for the
    // data blocks right after the memory is allocated so that we can optimize
    // read and memory allocations.
    const bool var_sized{array_schema_.var_size(name)};
    const bool nullable{array_schema_.is_nullable(name)};
    shared_ptr<FilteredData> filtered_data = make_shared<FilteredData>(
        HERE(),
        resources_,
        *this,
        min_batch_size_,
        max_batch_size_,
        min_batch_gap_,
        fragment_metadata_,
        result_tiles,
        name,
        var_sized,
        nullable,
        val_only,
        memory_tracker_);

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
          fragment, name, var_sized, nullable, val_only, tile_idx};

      // Construct a tile data class.
      // See the explanation in 'read_and_unfilter_attribute_tiles' for more
      // lifetime details. The tile data class is used to transmit the location
      // of the fixed/var/nullable filtered data to the created 'TileTuple'
      // object inside of each 'ResultTile'. The filter pipeline currently uses
      // the 'ResultTile' object to access the data. Eventually, these
      // 'TileData' objects should be returned by this function and passed into
      // 'unfilter_tiles' so that the filter pipeline can stop using the
      // 'ResultTile' object to get access to the filtered data.
      std::tuple<void*, ThreadPool::SharedTask> n = {
          nullptr, ThreadPool::SharedTask()};
      ResultTile::TileData tile_data{
          val_only ? n :
                     filtered_data->fixed_filtered_data(fragment.get(), tile),
          val_only ? n : filtered_data->var_filtered_data(fragment.get(), tile),
          filtered_data->nullable_filtered_data(fragment.get(), tile),
          filtered_data};

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

  return;
}

tuple<Status, optional<uint64_t>, optional<uint64_t>, optional<uint64_t>>
ReaderBase::load_tile_chunk_data(
    const std::string& name,
    const bool validity_only,
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
  const auto t_var =
      var_size && !validity_only ? &tile_tuple->var_tile() : nullptr;
  const auto t_validity = nullable ? &tile_tuple->validity_tile() : nullptr;

  uint64_t unfiltered_tile_size = 0, unfiltered_tile_var_size = 0,
           unfiltered_tile_validity_size = 0;

  const FilterPipeline& filters = array_schema_.filters(name);
  if (!validity_only) {
    if (!var_size || !filters.skip_offsets_filtering(
                         t_var->type(), array_schema_.version())) {
      if (var_size) {
        unfiltered_tile_size = t->load_offsets_chunk_data(tile_chunk_data);
      } else {
        unfiltered_tile_size = t->load_chunk_data(tile_chunk_data);
      }
    }

    if (var_size) {
      unfiltered_tile_var_size = t_var->load_chunk_data(tile_chunk_var_data);
    }
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
      tile->zip_coordinates();
    }
  }
  return Status::Ok();
}

Status ReaderBase::post_process_unfiltered_tile(
    const std::string& name,
    const bool validity_only,
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

  if (var_size && !validity_only) {
    auto& t_var = tile_tuple->var_tile();
    t_var.clear_filtered_buffer();
    throw_if_not_ok(zip_tile_coordinates(name, &t_var));
    t.add_extra_offset(t_var);
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
    const bool validity_only,
    const std::vector<ResultTile*>& result_tiles) {
  const auto stat_type = (array_schema_.is_attr(name)) ?
                             "unfilter_attr_tiles_builder" :
                             "unfilter_coord_tiles_builder";

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
  const auto num_threads = resources_.compute_tp().concurrency_level();
  if (chunking && num_tiles < num_threads) {
    // Ceil the division between thread_num and num_tiles.
    num_range_threads = 1 + ((num_threads - 1) / num_tiles);
  }

  for (size_t i = 0; i < num_tiles; i++) {
    auto result_tile = result_tiles[i];
    if (skip_field(result_tile->frag_idx(), name)) {
      continue;
    }
    ThreadPool::SharedTask task =
        resources_.compute_tp().execute([name,
                                         validity_only,
                                         var_size,
                                         nullable,
                                         num_range_threads,
                                         result_tile,
                                         this]() {
          const auto stat_type =
              (array_schema_.is_attr(name)) ?
                  "unfilter_attr_tiles_builder.unfilter_attr_tiles" :
                  "unfilter_coord_tiles_builder.unfilter_coord_tiles";

          const auto timer_se = stats_->start_timer(stat_type);
          // Chunks for unfiltering
          ChunkData tiles_chunk_data;
          ChunkData tiles_chunk_var_data;
          ChunkData tiles_chunk_validity_data;
          auto&& [st, tile_size, tile_var_size, tile_validity_size] =
              load_tile_chunk_data(
                  name,
                  validity_only,
                  result_tile,
                  var_size,
                  nullable,
                  tiles_chunk_data,
                  tiles_chunk_var_data,
                  tiles_chunk_validity_data);
          if (!st.ok())
            return st;

          if (tile_size.value() == 0)
            return Status::Ok();

          for (uint64_t range_thread_idx = 0;
               range_thread_idx < num_range_threads;
               range_thread_idx++) {
            st = unfilter_tile(
                name,
                validity_only,
                result_tile,
                var_size,
                nullable,
                range_thread_idx,
                num_range_threads,
                tiles_chunk_data,
                tiles_chunk_var_data,
                tiles_chunk_validity_data);
            if (!st.ok()) {
              return st;
            }
          }

          // Perform required post-processing of unfiltered tiles
          return post_process_unfiltered_tile(
              name, validity_only, result_tile, var_size, nullable);
        });

    if (skip_field(result_tile->frag_idx(), name)) {
      task.wait();
      continue;
    }
    // Store as a shared_ptr so we can move lifetimes around
    // This should be changes once we use taskgraphs for modeling the data flow
    auto tile_tuple = result_tile->tile_tuple(name);
    tile_tuple->fixed_tile().set_unfilter_data_compute_task(task);

    if (var_size) {
      tile_tuple->var_tile().set_unfilter_data_compute_task(task);
    }

    if (nullable) {
      tile_tuple->validity_tile().set_unfilter_data_compute_task(task);
    }
  }

  return Status::Ok();
}

Status ReaderBase::unfilter_tile(
    const std::string& name,
    const bool validity_only,
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
  auto t_var = var_size && !validity_only ? &tile_tuple->var_tile() : nullptr;
  auto t_validity = nullable ? &tile_tuple->validity_tile() : nullptr;

  FilterPipeline fixed_filters;
  FilterPipeline var_filters;
  FilterPipeline validity_filters;

  // Create our filter pipelines
  if (!validity_only) {
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
  }

  if (nullable) {
    validity_filters = array_schema_.cell_validity_filters();
    RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
        &validity_filters, array_->get_encryption_key()));
  }

  bool skip_offsets_filtering = false;
  if (var_size && !validity_only) {
    skip_offsets_filtering = var_filters.skip_offsets_filtering(
        t_var->type(), array_schema_.version());
  }

  auto concurrency_level = resources_.compute_tp().concurrency_level();

  if (!validity_only) {
    // Unfiltered fixed data
    if (!skip_offsets_filtering &&
        thread_idx <= tile_chunk_fixed_data.filtered_chunks_.size() - 1) {
      // Compute chunk boundaries
      auto&& [t_min, t_max] = compute_chunk_min_max(
          tile_chunk_fixed_data.chunk_offsets_.size(), num_threads, thread_idx);

      // Reverse the tile filters.
      stats_->add_counter("tiles_unfiltered", 1);
      RETURN_NOT_OK(fixed_filters.run_reverse(
          stats_,
          t,
          nullptr,
          tile_chunk_fixed_data,
          t_min,
          t_max,
          concurrency_level,
          resources_.config()));
    }

    // Prevent processing past the end of chunks in case there are more
    // threads than chunks.
    if (var_size &&
        thread_idx <= tile_chunk_var_data.filtered_chunks_.size() - 1) {
      auto&& [tvar_min, tvar_max] = compute_chunk_min_max(
          tile_chunk_var_data.chunk_offsets_.size(), num_threads, thread_idx);
      // Reverse the filters of tile var data
      stats_->add_counter("tiles_unfiltered", 1);
      RETURN_NOT_OK(var_filters.run_reverse(
          stats_,
          t_var,
          skip_offsets_filtering ? t : nullptr,
          tile_chunk_var_data,
          tvar_min,
          tvar_max,
          concurrency_level,
          resources_.config()));
    }
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
    stats_->add_counter("tiles_unfiltered", 1);
    RETURN_NOT_OK(validity_filters.run_reverse(
        stats_,
        t_validity,
        nullptr,
        tile_chunk_validity_data,
        tval_min,
        tval_max,
        concurrency_level,
        resources_.config()));
  }

  return Status::Ok();
}

uint64_t ReaderBase::offsets_bytesize() const {
  return offsets_bitsize_ == 32 ? sizeof(uint32_t) :
                                  constants::cell_var_offset_size;
}

uint64_t ReaderBase::get_attribute_tile_size(
    const std::string& name, unsigned f, uint64_t t) const {
  uint64_t tile_size = 0;
  if (!fragment_metadata_[f]->array_schema()->is_field(name)) {
    return tile_size;
  }

  tile_size += fragment_metadata_[f]->tile_size(name, t);

  if (array_schema_.var_size(name)) {
    tile_size +=
        fragment_metadata_[f]->loaded_metadata()->tile_var_size(name, t);
  }

  if (array_schema_.is_nullable(name)) {
    tile_size +=
        fragment_metadata_[f]->cell_num(t) * constants::cell_validity_size;
  }

  return tile_size;
}

uint64_t ReaderBase::get_attribute_persisted_tile_size(
    const std::string& name, unsigned f, uint64_t t) const {
  uint64_t tile_size = 0;
  if (!fragment_metadata_[f]->array_schema()->is_field(name)) {
    return tile_size;
  }

  tile_size +=
      fragment_metadata_[f]->loaded_metadata()->persisted_tile_size(name, t);

  if (array_schema_.var_size(name)) {
    tile_size +=
        fragment_metadata_[f]->loaded_metadata()->persisted_tile_var_size(
            name, t);
  }

  if (array_schema_.is_nullable(name)) {
    tile_size +=
        fragment_metadata_[f]->loaded_metadata()->persisted_tile_validity_size(
            name, t);
  }

  return tile_size;
}

template <class T>
std::pair<TileDomain<T>, std::vector<TileDomain<T>>>
ReaderBase::compute_tile_domains(const Subarray& partitioner_subarray) const {
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

  return {
      TileDomain<T>(UINT32_MAX, domain, domain, tile_extents, tile_order),
      frag_tile_domains};
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
      &resources_.compute_tp(), 0, fragment_metadata_.size(), [&](unsigned f) {
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
  AttributeOrderValidator validator(
      attribute_name, fragment_metadata_.size(), query_memory_tracker_);
  throw_if_not_ok(parallel_for(
      &resources_.compute_tp(), 0, fragment_metadata_.size(), [&](uint64_t f) {
        validator.find_fragments_to_check(
            array_min_idx, array_max_idx, f, non_empty_domains);

        return Status::Ok();
      }));

  throw_if_not_ok(parallel_for(
      &resources_.compute_tp(), 0, fragment_metadata_.size(), [&](int64_t f) {
        validator.validate_without_loading_tiles<IndexType, AttributeType>(
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
        &resources_.compute_tp(),
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

  auto g = [&](auto T) {
    if constexpr (std::is_same_v<decltype(T), char>) {
      validate_attribute_order<IndexType, std::string_view>(
          attribute_name,
          increasing_data,
          array_non_empty_domain,
          non_empty_domains,
          frag_first_array_tile_idx);
    } else if constexpr (tiledb::type::TileDBFundamental<decltype(T)>) {
      validate_attribute_order<IndexType, decltype(T)>(
          attribute_name,
          increasing_data,
          array_non_empty_domain,
          non_empty_domains,
          frag_first_array_tile_idx);
    } else {
      throw ReaderBaseStatusException("Invalid attribute type");
    }
  };
  apply_with_type(g, attribute_type);
}

// Explicit template instantiations
template std::pair<TileDomain<int8_t>, std::vector<TileDomain<int8_t>>>
ReaderBase::compute_tile_domains<int8_t>(const Subarray&) const;
template std::pair<TileDomain<uint8_t>, std::vector<TileDomain<uint8_t>>>
ReaderBase::compute_tile_domains<uint8_t>(const Subarray&) const;
template std::pair<TileDomain<int16_t>, std::vector<TileDomain<int16_t>>>
ReaderBase::compute_tile_domains<int16_t>(const Subarray&) const;
template std::pair<TileDomain<uint16_t>, std::vector<TileDomain<uint16_t>>>
ReaderBase::compute_tile_domains<uint16_t>(const Subarray&) const;
template std::pair<TileDomain<int32_t>, std::vector<TileDomain<int32_t>>>
ReaderBase::compute_tile_domains<int32_t>(const Subarray&) const;
template std::pair<TileDomain<uint32_t>, std::vector<TileDomain<uint32_t>>>
ReaderBase::compute_tile_domains<uint32_t>(const Subarray&) const;
template std::pair<TileDomain<int64_t>, std::vector<TileDomain<int64_t>>>
ReaderBase::compute_tile_domains<int64_t>(const Subarray&) const;
template std::pair<TileDomain<uint64_t>, std::vector<TileDomain<uint64_t>>>
ReaderBase::compute_tile_domains<uint64_t>(const Subarray&) const;
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

}  // namespace tiledb::sm

template <>
IndexedList<tiledb::sm::ResultTile>::IndexedList(
    shared_ptr<tiledb::sm::MemoryTracker> memory_tracker)
    : memory_tracker_(memory_tracker)
    , list_(memory_tracker->get_resource(sm::MemoryType::RESULT_TILE)) {
}
