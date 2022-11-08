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

template <
    typename CmpObject,
    typename std::enable_if_t<
        std::is_same_v<CmpObject, HilbertCmpQB> ||
        std::is_same_v<CmpObject, GlobalCmpQB>>*>
struct ReaderBase::BitSortFilterMetadataStorage {
  std::vector<Tile*> dim_tiles_;
  std::vector<QueryBuffer> query_buffers_;
  std::vector<uint64_t> hilbert_values_;
  std::optional<DomainBuffersView> db_;
  std::optional<CmpObject> cmp_obj_;
  std::function<bool(const uint64_t&, const uint64_t&)> comparator_;

  BitSortFilterMetadataType get_bitsort_filter_metadata_type() {
    return BitSortFilterMetadataType(&dim_tiles_, &comparator_);
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
    QueryCondition& condition)
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
    , disable_cache_(false)
    , disable_batching_(false)
    , user_requested_timestamps_(false)
    , use_timestamps_(false)
    , initial_data_loaded_(false) {
  if (array != nullptr)
    fragment_metadata_ = array->fragment_metadata();
  timestamps_needed_for_deletes_and_updates_.resize(fragment_metadata_.size());

  bool found = false;
  if (!config_.get<bool>("vfs.disable_batching", &disable_batching_, &found)
           .ok()) {
    throw ReaderBaseStatusException("Cannot get disable batching setting");
  }
  assert(found);
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

bool ReaderBase::partial_consolidated_fragment_overlap() const {
  // Fetch relevant fragments so we check only intersecting fragments
  const auto relevant_fragments = subarray_.relevant_fragments();
  bool all_frag = !subarray_.is_set();
  for (size_t i = 0;
       i < (all_frag ? fragment_metadata_.size() : relevant_fragments->size());
       i++) {
    auto frag_idx = all_frag ? i : relevant_fragments->at(i);
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
    Subarray& subarray, const std::vector<std::string>& names) {
  auto timer_se = stats_->start_timer("load_tile_offsets");
  const auto encryption_key = array_->encryption_key();

  // Fetch relevant fragments so we load tile offsets only from intersecting
  // fragments
  const auto relevant_fragments = subarray.relevant_fragments();

  bool all_frag = !subarray.is_set();

  const auto status = parallel_for(
      storage_manager_->compute_tp(),
      0,
      all_frag ? fragment_metadata_.size() : relevant_fragments->size(),
      [&](const uint64_t i) {
        auto frag_idx = all_frag ? i : relevant_fragments->at(i);
        auto& fragment = fragment_metadata_[frag_idx];
        const auto format_version = fragment->format_version();

        // Filter the 'names' for format-specific names.
        std::vector<std::string> filtered_names;
        filtered_names.reserve(names.size());
        const auto& schema = fragment->array_schema();
        for (const auto& name : names) {
          // Applicable for zipped coordinates only to versions < 5
          if (name == constants::coords && format_version >= 5) {
            continue;
          }

          // Applicable to separate coordinates only to versions >= 5
          const auto is_dim = schema->is_dim(name);
          if (is_dim && format_version < 5) {
            continue;
          }

          // Not a member of array schema, this field was added in array schema
          // evolution, ignore for this fragment's tile offsets
          if (!schema->is_field(name)) {
            continue;
          }

          // If the fragment doesn't include timestamps
          if (timestamps_not_present(name, frag_idx)) {
            continue;
          }

          // Continue if the fragment doesn't have delete metadata.
          if (delete_meta_not_present(name, frag_idx)) {
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
    Subarray& subarray, const std::vector<std::string>& names) {
  auto timer_se = stats_->start_timer("load_tile_var_sizes");
  const auto encryption_key = array_->encryption_key();

  // Fetch relevant fragments so we load tile var sizes only from intersecting
  // fragments
  const auto relevant_fragments = subarray.relevant_fragments();

  bool all_frag = !subarray.is_set();

  const auto status = parallel_for(
      storage_manager_->compute_tp(),
      0,
      all_frag ? fragment_metadata_.size() : relevant_fragments->size(),
      [&](const uint64_t i) {
        auto frag_idx = all_frag ? i : relevant_fragments->at(i);
        auto& fragment = fragment_metadata_[frag_idx];

        const auto& schema = fragment->array_schema();
        for (const auto& name : names) {
          // Not a member of array schema, this field was added in array schema
          // evolution, ignore for this fragment's tile var sizes.
          if (!schema->is_field(name))
            continue;

          // Not a var size attribute.
          if (!schema->var_size(name))
            continue;

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

Status ReaderBase::read_attribute_tiles(
    const std::vector<std::string>& names,
    const std::vector<ResultTile*>& result_tiles) const {
  auto timer_se = stats_->start_timer("read_attribute_tiles");
  return read_tiles(names, result_tiles);
}

Status ReaderBase::read_coordinate_tiles(
    const std::vector<std::string>& names,
    const std::vector<ResultTile*>& result_tiles) const {
  auto timer_se = stats_->start_timer("read_coordinate_tiles");
  return read_tiles(names, result_tiles);
}

Status ReaderBase::read_tiles(
    const std::vector<std::string>& names,
    const std::vector<ResultTile*>& result_tiles) const {
  auto timer_se = stats_->start_timer("read_tiles");

  // Shortcut for empty tile vec
  if (result_tiles.empty() || names.empty()) {
    return Status::Ok();
  }

  // Populate the list of regions per file to be read.
  std::unordered_map<
      URI,
      std::vector<tuple<uint64_t, Tile*, uint64_t>>,
      URIHasher>
      all_regions;

  uint64_t num_tiles_read = 0;

  // Run all tiles and attributes.
  for (auto name : names) {
    for (auto tile : result_tiles) {
      // For each tile, read from its fragment.
      auto const fragment = fragment_metadata_[tile->frag_idx()];

      const format_version_t format_version = fragment->format_version();

      // Applicable for zipped coordinates only to versions < 5
      if (name == constants::coords && format_version >= 5) {
        continue;
      }

      // Applicable to separate coordinates only to versions >= 5
      const auto& array_schema = fragment->array_schema();
      const bool is_dim = array_schema->is_dim(name);
      if (is_dim && format_version < 5) {
        continue;
      }

      // If the fragment doesn't have the attribute, this is a schema
      // evolution field and will be treated with fill-in value instead of
      // reading from disk
      if (!array_schema->is_field(name)) {
        continue;
      }

      // If the fragment doesn't include timestamps
      if (timestamps_not_present(name, tile->frag_idx())) {
        continue;
      }

      // Continue if the fragment doesn't have delete metadata.
      if (delete_meta_not_present(name, tile->frag_idx())) {
        continue;
      }

      num_tiles_read++;

      const bool var_size = array_schema->var_size(name);
      const bool nullable = array_schema->is_nullable(name);
      const auto tile_idx = tile->tile_idx();

      // Construct a TileSizes class.
      ResultTile::TileSizes tile_sizes(
          fragment, name, var_size, nullable, tile_idx);

      // Initialize the tile(s)
      ResultTile::TileTuple* tile_tuple = nullptr;
      if (is_dim) {
        const uint64_t dim_num = array_schema->dim_num();
        for (uint64_t d = 0; d < dim_num; ++d) {
          if (array_schema->dimension_ptr(d)->name() == name) {
            tile->init_coord_tile(
                format_version, array_schema_, name, tile_sizes, d);
            break;
          }
        }
        tile_tuple = tile->tile_tuple(name);
      } else {
        tile->init_attr_tile(format_version, array_schema_, name, tile_sizes);
        tile_tuple = tile->tile_tuple(name);
      }

      assert(tile_tuple != nullptr);
      Tile* const t = &tile_tuple->fixed_tile();
      Tile* const t_var = var_size ? &tile_tuple->var_tile() : nullptr;
      Tile* const t_validity =
          nullable ? &tile_tuple->validity_tile() : nullptr;
      auto&& [status, tile_attr_uri] = fragment->uri(name);
      RETURN_NOT_OK(status);

      // Try the cache first.
      bool cache_hit = false;
      uint64_t tile_attr_offset;
      RETURN_NOT_OK(fragment->file_offset(name, tile_idx, &tile_attr_offset));
      if (!disable_cache_) {
        RETURN_NOT_OK(storage_manager_->read_from_cache(
            *tile_attr_uri,
            tile_attr_offset,
            t->filtered_buffer(),
            tile_sizes.tile_persisted_size(),
            &cache_hit));
      }

      if (!cache_hit) {
        // Add the region of the fragment to be read.
        all_regions[*tile_attr_uri].emplace_back(
            tile_attr_offset, t, tile_sizes.tile_persisted_size());
      }

      if (var_size) {
        auto&& [status, tile_attr_var_uri] = fragment->var_uri(name);
        RETURN_NOT_OK(status);

        uint64_t tile_attr_var_offset;
        RETURN_NOT_OK(
            fragment->file_var_offset(name, tile_idx, &tile_attr_var_offset));
        if (!disable_cache_) {
          RETURN_NOT_OK(storage_manager_->read_from_cache(
              *tile_attr_var_uri,
              tile_attr_var_offset,
              t_var->filtered_buffer(),
              tile_sizes.tile_var_persisted_size(),
              &cache_hit));
        }

        if (!cache_hit) {
          // Add the region of the fragment to be read.
          all_regions[*tile_attr_var_uri].emplace_back(
              tile_attr_var_offset,
              t_var,
              tile_sizes.tile_var_persisted_size());
        }
      }

      if (nullable) {
        auto&& [status, tile_validity_attr_uri] = fragment->validity_uri(name);
        RETURN_NOT_OK(status);

        uint64_t tile_attr_validity_offset;
        RETURN_NOT_OK(fragment->file_validity_offset(
            name, tile_idx, &tile_attr_validity_offset));
        if (!disable_cache_) {
          RETURN_NOT_OK(storage_manager_->read_from_cache(
              *tile_validity_attr_uri,
              tile_attr_validity_offset,
              t_validity->filtered_buffer(),
              tile_sizes.tile_validity_persisted_size(),
              &cache_hit));
        }

        if (!cache_hit) {
          // Add the region of the fragment to be read.
          all_regions[*tile_validity_attr_uri].emplace_back(
              tile_attr_validity_offset,
              t_validity,
              tile_sizes.tile_validity_persisted_size());
        }
      }
    }
  }

  stats_->add_counter("num_tiles_read", num_tiles_read);

  // Do not use the read-ahead cache because tiles will be
  // cached in the tile cache.
  const bool use_read_ahead = false;

  // Read the tiles asynchronously
  std::vector<ThreadPool::Task> tasks;

  // Enqueue all regions to be read.
  for (const auto& item : all_regions) {
    if (disable_batching_) {
      RETURN_NOT_OK(storage_manager_->vfs()->read_all_no_batching(
          item.first,
          item.second,
          storage_manager_->io_tp(),
          &tasks,
          use_read_ahead));
    } else {
      RETURN_NOT_OK(storage_manager_->vfs()->read_all(
          item.first,
          item.second,
          storage_manager_->io_tp(),
          &tasks,
          use_read_ahead));
    }
  }

  // Wait for the reads to finish and check statuses.
  auto statuses = storage_manager_->io_tp()->wait_all_status(tasks);
  for (const auto& st : statuses)
    RETURN_CANCEL_OR_ERROR(st);

  return Status::Ok();
}

tuple<Status, optional<uint64_t>> ReaderBase::load_chunk_data(
    Tile* const tile, ChunkData* unfiltered_tile) const {
  assert(tile);
  assert(unfiltered_tile);
  assert(tile->filtered());

  Status st = Status::Ok();
  auto filtered_buffer_data = tile->filtered_buffer().data();
  if (filtered_buffer_data == nullptr) {
    st = logger_->status(Status_ReaderError("Tile has null buffer."));
    return {st, nullopt};
  }

  // Make a pass over the tile to get the chunk information.
  uint64_t num_chunks;
  memcpy(&num_chunks, filtered_buffer_data, sizeof(uint64_t));
  filtered_buffer_data += sizeof(uint64_t);

  auto& filtered_chunks = unfiltered_tile->filtered_chunks_;
  auto& chunk_offsets = unfiltered_tile->chunk_offsets_;
  filtered_chunks.resize(num_chunks);
  chunk_offsets.resize(num_chunks);
  uint64_t total_orig_size = 0;
  for (uint64_t i = 0; i < num_chunks; i++) {
    auto& chunk = filtered_chunks[i];
    memcpy(
        &(chunk.unfiltered_data_size_), filtered_buffer_data, sizeof(uint32_t));
    filtered_buffer_data += sizeof(uint32_t);

    memcpy(
        &(chunk.filtered_data_size_), filtered_buffer_data, sizeof(uint32_t));
    filtered_buffer_data += sizeof(uint32_t);

    memcpy(
        &(chunk.filtered_metadata_size_),
        filtered_buffer_data,
        sizeof(uint32_t));
    filtered_buffer_data += sizeof(uint32_t);

    chunk.filtered_metadata_ = filtered_buffer_data;
    chunk.filtered_data_ = static_cast<char*>(chunk.filtered_metadata_) +
                           chunk.filtered_metadata_size_;

    chunk_offsets[i] = total_orig_size;
    total_orig_size += chunk.unfiltered_data_size_;

    filtered_buffer_data +=
        chunk.filtered_metadata_size_ + chunk.filtered_data_size_;
  }

  if (total_orig_size != tile->size()) {
    return {LOG_STATUS(Status_ReaderError(
                "Error incorrect unfiltered tile size allocated.")),
            nullopt};
  }

  return {Status::Ok(), total_orig_size};
}

tuple<Status, optional<uint64_t>, optional<uint64_t>, optional<uint64_t>>
ReaderBase::load_tile_chunk_data(
    const std::string& name,
    ResultTile* const tile,
    const bool var_size,
    const bool nullable,
    ChunkData* const tile_chunk_data,
    ChunkData* const tile_chunk_var_data,
    ChunkData* const tile_chunk_validity_data) const {
  assert(tile);
  assert(tile_chunk_data);
  assert(tile_chunk_var_data);
  assert(tile_chunk_validity_data);

  auto& fragment = fragment_metadata_[tile->frag_idx()];
  const auto format_version = fragment->format_version();
  uint64_t unfiltered_tile_size = 0, unfiltered_tile_var_size = 0,
           unfiltered_tile_validity_size = 0;

  // Applicable for zipped coordinates only to versions < 5
  // Applicable for separate coordinates only to version >= 5
  if (name != constants::coords ||
      (name == constants::coords && format_version < 5) ||
      (array_schema_.is_dim(name) && format_version >= 5)) {
    auto tile_tuple = tile->tile_tuple(name);

    // Skip non-existent attributes/dimensions (e.g. coords in the
    // dense case).
    if (tile_tuple == nullptr ||
        tile_tuple->fixed_tile().filtered_buffer().size() == 0) {
      return {Status::Ok(), 0, 0, 0};
    }

    // If the fragment doesn't include timestamps
    if (timestamps_not_present(name, tile->frag_idx())) {
      return {Status::Ok(), 0, 0, 0};
    }

    // If the fragment doesn't have delete metadata.
    if (delete_meta_not_present(name, tile->frag_idx())) {
      return {Status::Ok(), 0, 0, 0};
    }

    const auto t = &tile_tuple->fixed_tile();
    const auto t_var = var_size ? &tile_tuple->var_tile() : nullptr;
    const auto t_validity = nullable ? &tile_tuple->validity_tile() : nullptr;

    auto&& [st, tile_size] = load_chunk_data(t, tile_chunk_data);
    RETURN_NOT_OK_TUPLE(st, nullopt, nullopt, nullopt);
    unfiltered_tile_size = tile_size.value();
    if (var_size) {
      auto&& [st, tile_var_size] = load_chunk_data(t_var, tile_chunk_var_data);
      RETURN_NOT_OK_TUPLE(st, nullopt, nullopt, nullopt);
      unfiltered_tile_var_size = tile_var_size.value();
    }
    if (nullable) {
      auto&& [st, tile_validity_size] =
          load_chunk_data(t_validity, tile_chunk_validity_data);
      RETURN_NOT_OK_TUPLE(st, nullopt, nullopt, nullopt);
      unfiltered_tile_validity_size = tile_validity_size.value();
    }
  }
  return {Status::Ok(),
          unfiltered_tile_size,
          unfiltered_tile_var_size,
          unfiltered_tile_validity_size};
}

Status ReaderBase::unfilter_tile_chunk_range(
    const std::string& name,
    ResultTile* const tile,
    const bool var_size,
    const bool nullable,
    const uint64_t range_thread_idx,
    const uint64_t num_range_threads,
    const ChunkData& tile_chunk_data,
    const ChunkData& tile_chunk_var_data,
    const ChunkData& tile_chunk_validity_data) const {
  assert(tile);
  auto& fragment = fragment_metadata_[tile->frag_idx()];
  auto format_version = fragment->format_version();

  // Applicable for zipped coordinates only to versions < 5
  // Applicable for separate coordinates only to version >= 5
  if (name != constants::coords ||
      (name == constants::coords && format_version < 5) ||
      (array_schema_.is_dim(name) && format_version >= 5)) {
    auto tile_tuple = tile->tile_tuple(name);

    // Skip non-existent attributes/dimensions (e.g. coords in the
    // dense case).
    if (tile_tuple == nullptr ||
        tile_tuple->fixed_tile().filtered_buffer().size() == 0) {
      return Status::Ok();
    }

    // If the fragment doesn't include timestamps
    if (timestamps_not_present(name, tile->frag_idx())) {
      return Status::Ok();
    }

    // If the fragment doesn't have delete metadata.
    if (delete_meta_not_present(name, tile->frag_idx())) {
      return Status::Ok();
    }

    auto t = &tile_tuple->fixed_tile();
    auto t_var = var_size ? &tile_tuple->var_tile() : nullptr;
    auto t_validity = nullable ? &tile_tuple->validity_tile() : nullptr;

    // Unfilter 't' for fixed-sized tiles, otherwise unfilter both 't' and
    // 't_var' for var-sized tiles.
    if (!var_size) {
      if (!nullable) {
        if (array_schema_.bitsort_filter_attr().has_value()) {
          BitSortFilterMetadataType bitsort_metadata;
          if (array_schema_.cell_order() == Layout::HILBERT) {
            BitSortFilterMetadataStorage<HilbertCmpQB> bitsort_storage;
            bitsort_metadata = construct_bitsort_filter_argument<HilbertCmpQB>(
                tile, bitsort_storage);
            RETURN_NOT_OK(unfilter_tile_chunk_range(
                num_range_threads,
                range_thread_idx,
                name,
                t,
                tile_chunk_data,
                &bitsort_metadata));
          } else {
            BitSortFilterMetadataStorage<GlobalCmpQB> bitsort_storage;
            bitsort_metadata = construct_bitsort_filter_argument<GlobalCmpQB>(
                tile, bitsort_storage);
            RETURN_NOT_OK(unfilter_tile_chunk_range(
                num_range_threads,
                range_thread_idx,
                name,
                t,
                tile_chunk_data,
                &bitsort_metadata));
          }
        } else {
          RETURN_NOT_OK(unfilter_tile_chunk_range(
              num_range_threads, range_thread_idx, name, t, tile_chunk_data));
        }
      } else {
        RETURN_NOT_OK(unfilter_tile_chunk_range_nullable(
            num_range_threads,
            range_thread_idx,
            name,
            t,
            tile_chunk_data,
            t_validity,
            tile_chunk_validity_data));
      }
    } else {
      if (!nullable)
        RETURN_NOT_OK(unfilter_tile_chunk_range(
            num_range_threads,
            range_thread_idx,
            name,
            t,
            tile_chunk_data,
            t_var,
            tile_chunk_var_data));
      else {
        RETURN_NOT_OK(unfilter_tile_chunk_range_nullable(
            num_range_threads,
            range_thread_idx,
            name,
            t,
            tile_chunk_data,
            t_var,
            tile_chunk_var_data,
            t_validity,
            tile_chunk_validity_data));
      }
    }
  }
  return Status::Ok();
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

  auto& fragment = fragment_metadata_[tile->frag_idx()];
  auto format_version = fragment->format_version();

  // Applicable for zipped coordinates only to versions < 5
  // Applicable for separate coordinates only to version >= 5
  if (name != constants::coords ||
      (name == constants::coords && format_version < 5) ||
      (array_schema_.is_dim(name) && format_version >= 5)) {
    auto tile_tuple = tile->tile_tuple(name);

    // Skip non-existent attributes/dimensions (e.g. coords in the
    // dense case).
    if (tile_tuple == nullptr ||
        tile_tuple->fixed_tile().filtered_buffer().size() == 0) {
      return Status::Ok();
    }

    // If the fragment doesn't include timestamps
    if (timestamps_not_present(name, tile->frag_idx())) {
      return Status::Ok();
    }

    // If the fragment doesn't have delete metadata.
    if (delete_meta_not_present(name, tile->frag_idx())) {
      return Status::Ok();
    }

    auto& t = tile_tuple->fixed_tile();
    t.filtered_buffer().clear();

    throw_if_not_ok(zip_tile_coordinates(name, &t));

    if (var_size) {
      auto& t_var = tile_tuple->var_tile();
      t_var.filtered_buffer().clear();
      throw_if_not_ok(zip_tile_coordinates(name, &t_var));
    }

    if (nullable) {
      auto& t_validity = tile_tuple->validity_tile();
      t_validity.filtered_buffer().clear();
      throw_if_not_ok(zip_tile_coordinates(name, &t_validity));
    }
  }

  return Status::Ok();
}

Status ReaderBase::unfilter_tiles_chunk_range(
    const std::string& name,
    const std::vector<ResultTile*>& result_tiles) const {
  const auto num_tiles = static_cast<uint64_t>(result_tiles.size());
  if (num_tiles == 0) {
    return Status::Ok();
  }

  // Compute parallelization parameters.
  uint64_t num_range_threads = 1;
  const auto num_threads = storage_manager_->compute_tp()->concurrency_level();
  if (num_tiles < num_threads) {
    // Ceil the division between thread_num and num_tiles.
    num_range_threads = 1 + ((num_threads - 1) / num_tiles);
  }

  const auto var_size = array_schema_.var_size(name);
  const auto nullable = array_schema_.is_nullable(name);

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
                &tiles_chunk_data[i],
                &tiles_chunk_var_data[i],
                &tiles_chunk_validity_data[i]);
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
        return unfilter_tile_chunk_range(
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

Status ReaderBase::unfilter_tile_chunk_range(
    const uint64_t num_range_threads,
    const uint64_t thread_idx,
    const std::string& name,
    Tile* tile,
    const ChunkData& tile_chunk_data,
    void* support_data) const {
  assert(tile);
  // Prevent processing past the end of chunks in case there are more
  // threads than chunks.
  if (thread_idx > tile_chunk_data.filtered_chunks_.size() - 1) {
    return Status::Ok();
  }

  FilterPipeline filters = array_schema_.filters(name);

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  // Compute chunk boundaries
  auto&& [t_min, t_max] = compute_chunk_min_max(
      tile_chunk_data.chunk_offsets_.size(), num_range_threads, thread_idx);

  // Reverse the tile filters.
  RETURN_NOT_OK(filters.run_reverse_chunk_range(
      stats_,
      tile,
      support_data,
      tile_chunk_data,
      t_min,
      t_max,
      storage_manager_->compute_tp()->concurrency_level(),
      storage_manager_->config()));

  return Status::Ok();
}

Status ReaderBase::unfilter_tile_chunk_range(
    const uint64_t num_range_threads,
    const uint64_t thread_idx,
    const std::string& name,
    Tile* tile,
    const ChunkData& tile_chunk_data,
    Tile* tile_var,
    const ChunkData& tile_var_chunk_data) const {
  assert(tile);
  assert(tile_var);

  FilterPipeline offset_filters = array_schema_.cell_var_offsets_filters();
  FilterPipeline filters = array_schema_.filters(name);
  auto concurrency_level = storage_manager_->compute_tp()->concurrency_level();

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &offset_filters, array_->get_encryption_key()));
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  // Compute chunk boundaries
  auto&& [t_min, t_max] = compute_chunk_min_max(
      tile_chunk_data.chunk_offsets_.size(), num_range_threads, thread_idx);

  // Reverse the filters of tile offsets
  RETURN_NOT_OK(offset_filters.run_reverse_chunk_range(
      stats_,
      tile,
      nullptr,
      tile_chunk_data,
      t_min,
      t_max,
      concurrency_level,
      storage_manager_->config()));

  if (tile_var_chunk_data.chunk_offsets_.size() > 0) {
    auto&& [tvar_min, tvar_max] = compute_chunk_min_max(
        tile_var_chunk_data.chunk_offsets_.size(),
        num_range_threads,
        thread_idx);
    // Reverse the filters of tile var data
    RETURN_NOT_OK(filters.run_reverse_chunk_range(
        stats_,
        tile_var,
        nullptr,
        tile_var_chunk_data,
        tvar_min,
        tvar_max,
        concurrency_level,
        storage_manager_->config()));
  }

  return Status::Ok();
}

Status ReaderBase::unfilter_tile_chunk_range_nullable(
    const uint64_t num_range_threads,
    const uint64_t thread_idx,
    const std::string& name,
    Tile* tile,
    const ChunkData& tile_chunk_data,
    Tile* tile_validity,
    const ChunkData& tile_validity_chunk_data) const {
  assert(tile);
  assert(tile_validity);

  FilterPipeline filters = array_schema_.filters(name);
  FilterPipeline validity_filters = array_schema_.cell_validity_filters();
  auto concurrency_level = storage_manager_->compute_tp()->concurrency_level();

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &validity_filters, array_->get_encryption_key()));

  // Prevent processing past the end of chunks in case there are more
  // threads than chunks.
  if (thread_idx <= tile_chunk_data.filtered_chunks_.size() - 1) {
    // Compute chunk boundaries
    auto&& [t_min, t_max] = compute_chunk_min_max(
        tile_chunk_data.chunk_offsets_.size(), num_range_threads, thread_idx);

    // Reverse the tile filters.
    RETURN_NOT_OK(filters.run_reverse_chunk_range(
        stats_,
        tile,
        nullptr,
        tile_chunk_data,
        t_min,
        t_max,
        concurrency_level,
        storage_manager_->config()));
  }

  // Prevent processing past the end of chunks in case there are more
  // threads than chunks.
  if (thread_idx <= tile_validity_chunk_data.filtered_chunks_.size() - 1) {
    // Compute chunk boundaries
    auto&& [tval_min, tval_max] = compute_chunk_min_max(
        tile_validity_chunk_data.chunk_offsets_.size(),
        num_range_threads,
        thread_idx);

    // Reverse the tile validity filters.
    RETURN_NOT_OK(validity_filters.run_reverse_chunk_range(
        stats_,
        tile_validity,
        nullptr,
        tile_validity_chunk_data,
        tval_min,
        tval_max,
        concurrency_level,
        storage_manager_->config()));
  }

  return Status::Ok();
}

Status ReaderBase::unfilter_tile_chunk_range_nullable(
    const uint64_t num_range_threads,
    const uint64_t thread_idx,
    const std::string& name,
    Tile* tile,
    const ChunkData& tile_chunk_data,
    Tile* tile_var,
    const ChunkData& tile_var_chunk_data,
    Tile* tile_validity,
    const ChunkData& tile_validity_chunk_data) const {
  assert(tile);
  assert(tile_var);
  assert(tile_validity);

  FilterPipeline offset_filters = array_schema_.cell_var_offsets_filters();
  FilterPipeline filters = array_schema_.filters(name);
  FilterPipeline validity_filters = array_schema_.cell_validity_filters();
  auto concurrency_level = storage_manager_->compute_tp()->concurrency_level();

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &offset_filters, array_->get_encryption_key()));
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &validity_filters, array_->get_encryption_key()));

  // Prevent processing past the end of chunks in case there are more
  // threads than chunks.
  if (thread_idx <= tile_chunk_data.filtered_chunks_.size() - 1) {
    // Compute chunk boundaries
    auto&& [t_min, t_max] = compute_chunk_min_max(
        tile_chunk_data.chunk_offsets_.size(), num_range_threads, thread_idx);

    // Reverse the filters of tile offsets
    RETURN_NOT_OK(offset_filters.run_reverse_chunk_range(
        stats_,
        tile,
        nullptr,
        tile_chunk_data,
        t_min,
        t_max,
        concurrency_level,
        storage_manager_->config()));
  }

  // Prevent processing past the end of chunks in case there are more
  // threads than chunks.
  if (thread_idx <= tile_var_chunk_data.filtered_chunks_.size() - 1) {
    // Compute chunk boundaries
    auto&& [tvar_min, tvar_max] = compute_chunk_min_max(
        tile_var_chunk_data.chunk_offsets_.size(),
        num_range_threads,
        thread_idx);

    // Reverse the filters of tile var data
    RETURN_NOT_OK(filters.run_reverse_chunk_range(
        stats_,
        tile_var,
        nullptr,
        tile_var_chunk_data,
        tvar_min,
        tvar_max,
        concurrency_level,
        storage_manager_->config()));
  }

  // Prevent processing past the end of chunks in case there are more
  // threads than chunks.
  if (thread_idx <= tile_validity_chunk_data.filtered_chunks_.size() - 1) {
    // Compute chunk boundaries
    auto&& [tval_min, tval_max] = compute_chunk_min_max(
        tile_validity_chunk_data.chunk_offsets_.size(),
        num_range_threads,
        thread_idx);

    // Reverse the filters of tile validity
    RETURN_NOT_OK(validity_filters.run_reverse_chunk_range(
        stats_,
        tile_validity,
        nullptr,
        tile_validity_chunk_data,
        tval_min,
        tval_max,
        concurrency_level,
        storage_manager_->config()));
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

  if (array_schema_.is_dim(name) &&
      array_schema_.bitsort_filter_attr().has_value()) {
    // We omit running unfilter_tiles when there is a dimension, since we
    // process the dimension tiles in the bitsort filter.
    return Status::Ok();
  }

  auto chunking = true;
  if (var_size) {
    auto filters = array_schema_.filters(name);
    chunking = filters.use_tile_chunking(
        var_size, array_schema_.version(), array_schema_.type(name));
  }

  // The per tile cache is only used in readers where unfiltering
  // was done in parallel on tiles. The new readers parallelize both on
  // tiles and chunk ranges and don't benefit from using a tile cache.
  if (disable_cache_ == true && chunking) {
    return unfilter_tiles_chunk_range(name, result_tiles);
  }

  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, num_tiles, [&, this](uint64_t i) {
        ResultTile* const tile = result_tiles[i];

        auto& fragment = fragment_metadata_[tile->frag_idx()];
        auto format_version = fragment->format_version();

        // Applicable for zipped coordinates only to versions < 5
        // Applicable for separate coordinates only to version >= 5
        if (name != constants::coords ||
            (name == constants::coords && format_version < 5) ||
            (array_schema_.is_dim(name) && format_version >= 5)) {
          auto tile_tuple = tile->tile_tuple(name);

          // Skip non-existent attributes/dimensions (e.g. coords in the
          // dense case).
          if (tile_tuple == nullptr ||
              tile_tuple->fixed_tile().filtered_buffer().size() == 0) {
            return Status::Ok();
          }

          // If the fragment doesn't include timestamps
          if (timestamps_not_present(name, tile->frag_idx())) {
            return Status::Ok();
          }

          // If the fragment doesn't have delete metadata.
          if (delete_meta_not_present(name, tile->frag_idx())) {
            return Status::Ok();
          }

          Tile* const t = &tile_tuple->fixed_tile();
          Tile* const t_var = var_size ? &tile_tuple->var_tile() : nullptr;
          Tile* const t_validity =
              nullable ? &tile_tuple->validity_tile() : nullptr;

          if (disable_cache_ == false) {
            logger_->info("using cache");
            // Get information about the tile in its fragment.
            auto&& [status, tile_attr_uri] = fragment->uri(name);
            RETURN_NOT_OK(status);

            auto tile_idx = tile->tile_idx();
            uint64_t tile_attr_offset;
            RETURN_NOT_OK(
                fragment->file_offset(name, tile_idx, &tile_attr_offset));

            // Cache 't'.
            if (t->filtered() && !disable_cache_) {
              // Store the filtered buffer in the tile cache.
              RETURN_NOT_OK(storage_manager_->write_to_cache(
                  *tile_attr_uri, tile_attr_offset, t->filtered_buffer()));
            }

            // Cache 't_var'.
            if (var_size && t_var->filtered() && !disable_cache_) {
              auto&& [status, tile_attr_var_uri] = fragment->var_uri(name);
              RETURN_NOT_OK(status);

              uint64_t tile_attr_var_offset;
              RETURN_NOT_OK(fragment->file_var_offset(
                  name, tile_idx, &tile_attr_var_offset));

              // Store the filtered buffer in the tile cache.
              RETURN_NOT_OK(storage_manager_->write_to_cache(
                  *tile_attr_var_uri,
                  tile_attr_var_offset,
                  t_var->filtered_buffer()));
            }

            // Cache 't_validity'.
            if (nullable && t_validity->filtered() && !disable_cache_) {
              auto&& [status, tile_attr_validity_uri] =
                  fragment->validity_uri(name);
              RETURN_NOT_OK(status);

              uint64_t tile_attr_validity_offset;
              RETURN_NOT_OK(fragment->file_validity_offset(
                  name, tile_idx, &tile_attr_validity_offset));

              // Store the filtered buffer in the tile cache.
              RETURN_NOT_OK(storage_manager_->write_to_cache(
                  *tile_attr_validity_uri,
                  tile_attr_validity_offset,
                  t_validity->filtered_buffer()));
            }
          }

          // Unfilter 't' for fixed-sized tiles, otherwise unfilter both 't' and
          // 't_var' for var-sized tiles.
          if (!var_size) {
            if (!nullable) {
              if (array_schema_.bitsort_filter_attr().has_value()) {
                BitSortFilterMetadataType bitsort_metadata;
                if (array_schema_.cell_order() == Layout::HILBERT) {
                  BitSortFilterMetadataStorage<HilbertCmpQB> bitsort_storage;
                  bitsort_metadata =
                      construct_bitsort_filter_argument<HilbertCmpQB>(
                          tile, bitsort_storage);
                  RETURN_NOT_OK(unfilter_tile(name, t, &bitsort_metadata));
                } else {
                  BitSortFilterMetadataStorage<GlobalCmpQB> bitsort_storage;
                  bitsort_metadata =
                      construct_bitsort_filter_argument<GlobalCmpQB>(
                          tile, bitsort_storage);
                  RETURN_NOT_OK(unfilter_tile(name, t, &bitsort_metadata));
                }
              } else {
                RETURN_NOT_OK(unfilter_tile(name, t));
              }

            } else {
              RETURN_NOT_OK(unfilter_tile_nullable(name, t, t_validity));
            }
          } else {
            if (!nullable)
              RETURN_NOT_OK(unfilter_tile(name, t, t_var));
            else
              RETURN_NOT_OK(unfilter_tile_nullable(name, t, t_var, t_validity));
          }
        }

        return Status::Ok();
      });

  RETURN_CANCEL_OR_ERROR(status);

  return Status::Ok();
}

Status ReaderBase::unfilter_tile(
    const std::string& name, Tile* tile, void* support_data) const {
  FilterPipeline filters = array_schema_.filters(name);

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  // Reverse the tile filters.
  RETURN_NOT_OK(filters.run_reverse(
      stats_,
      tile,
      nullptr,
      storage_manager_->compute_tp(),
      storage_manager_->config(),
      support_data));

  return Status::Ok();
}

Status ReaderBase::unfilter_tile(
    const std::string& name, Tile* tile, Tile* tile_var) const {
  FilterPipeline offset_filters = array_schema_.cell_var_offsets_filters();
  FilterPipeline filters = array_schema_.filters(name);

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &offset_filters, array_->get_encryption_key()));
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  // Reverse the tile filters.
  // If offsets don't need to be unfiltered separately, it means they
  // will be created on the fly from filtered data
  if (filters.skip_offsets_filtering(
          tile_var->type(), array_schema_.version())) {
    RETURN_NOT_OK(filters.run_reverse(
        stats_, tile_var, tile, storage_manager_->compute_tp(), config_, tile));
  } else {
    RETURN_NOT_OK(offset_filters.run_reverse(
        stats_,
        tile,
        nullptr,
        storage_manager_->compute_tp(),
        config_,
        nullptr));
    RETURN_NOT_OK(filters.run_reverse(
        stats_,
        tile_var,
        nullptr,
        storage_manager_->compute_tp(),
        config_,
        nullptr));
  }

  return Status::Ok();
}

Status ReaderBase::unfilter_tile_nullable(
    const std::string& name, Tile* tile, Tile* tile_validity) const {
  FilterPipeline filters = array_schema_.filters(name);
  FilterPipeline validity_filters = array_schema_.cell_validity_filters();

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &validity_filters, array_->get_encryption_key()));

  // Reverse the tile filters.
  RETURN_NOT_OK(filters.run_reverse(
      stats_,
      tile,
      nullptr,
      storage_manager_->compute_tp(),
      storage_manager_->config(),
      nullptr));
  // Reverse the validity tile filters.
  RETURN_NOT_OK(validity_filters.run_reverse(
      stats_,
      tile_validity,
      nullptr,
      storage_manager_->compute_tp(),
      storage_manager_->config(),
      nullptr));

  return Status::Ok();
}

Status ReaderBase::unfilter_tile_nullable(
    const std::string& name,
    Tile* tile,
    Tile* tile_var,
    Tile* tile_validity) const {
  FilterPipeline offset_filters = array_schema_.cell_var_offsets_filters();
  FilterPipeline filters = array_schema_.filters(name);
  FilterPipeline validity_filters = array_schema_.cell_validity_filters();

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &offset_filters, array_->get_encryption_key()));
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &validity_filters, array_->get_encryption_key()));

  // Reverse the tile filters.
  // If offsets don't need to be unfiltered separately, it means they
  // will be created on the fly from filtered var-length data
  if (filters.skip_offsets_filtering(tile->type(), array_schema_.version())) {
    RETURN_NOT_OK(filters.run_reverse(
        stats_,
        tile_var,
        tile,
        storage_manager_->compute_tp(),
        storage_manager_->config(),
        tile));
  } else {
    RETURN_NOT_OK(offset_filters.run_reverse(
        stats_,
        tile,
        nullptr,
        storage_manager_->compute_tp(),
        storage_manager_->config(),
        nullptr));
    RETURN_NOT_OK(filters.run_reverse(
        stats_,
        tile_var,
        nullptr,
        storage_manager_->compute_tp(),
        storage_manager_->config(),
        nullptr));
  }

  // Reverse the validity tile filters.
  RETURN_NOT_OK(validity_filters.run_reverse(
      stats_,
      tile_validity,
      nullptr,
      storage_manager_->compute_tp(),
      storage_manager_->config(),
      nullptr));

  return Status::Ok();
}

uint64_t ReaderBase::offsets_bytesize() const {
  return offsets_bitsize_ == 32 ? sizeof(uint32_t) :
                                  constants::cell_var_offset_size;
}

uint64_t ReaderBase::get_attribute_tile_size(
    const std::string& name, unsigned f, uint64_t t) {
  uint64_t tile_size = 0;
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

  if (partitioner_subarray.is_set()) {
    auto relevant_frags = partitioner_subarray.relevant_fragments();
    for (auto it = relevant_frags->rbegin(); it != relevant_frags->rend();
         it++) {
      if (fragment_metadata_[*it]->dense()) {
        frag_tile_domains.emplace_back(
            *it,
            domain,
            fragment_metadata_[*it]->non_empty_domain(),
            tile_extents,
            tile_order);
      }
    }
  } else {
    auto fragment_num = (int)fragment_metadata_.size();
    if (fragment_num > 0) {
      for (int i = fragment_num - 1; i >= 0; --i) {
        if (fragment_metadata_[i]->dense()) {
          frag_tile_domains.emplace_back(
              i,
              domain,
              fragment_metadata_[i]->non_empty_domain(),
              tile_extents,
              tile_order);
        }
      }
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

  return {Range(&min, &max, sizeof(IndexType)),
          std::move(non_empty_domains),
          std::move(frag_first_array_tile_idx)};
}

template <typename IndexType>
void ReaderBase::ensure_continuous_domain_written(
    std::vector<const void*>& non_empty_domains) {
  // Store the non empty domains in a vector and sort them by min.
  std::vector<std::pair<IndexType, IndexType>> sorted_non_empty_domains(
      fragment_metadata_.size());
  for (uint64_t f = 0; f < fragment_metadata_.size(); f++) {
    auto ned = static_cast<const IndexType*>(non_empty_domains[f]);
    sorted_non_empty_domains[f] = std::make_pair(ned[0], ned[1]);
  }
  std::sort(sorted_non_empty_domains.begin(), sorted_non_empty_domains.end());

  // Go through the sorted non empty domains and make sure there is no holes.
  IndexType max_value = sorted_non_empty_domains[0].second;
  for (auto& non_empty_domain : sorted_non_empty_domains) {
    // If the start is greater than the current max, there is a discountinuity.
    if (non_empty_domain.first > max_value + 1) {
      throw ReaderBaseStatusException("Discontiuity found in array domain");
    }

    // Adjust the max. Since the non empty domains are sorted, the min never
    // changes.
    max_value = std::max(max_value, non_empty_domain.second);
  }
}

/**
 * Utilitary function that returns if a value is contained in a non empty
 * domain.
 *
 * @tparam Index type
 * @param v Value to check.
 * @param domain Pointer to the domain values.
 * @return Is the value in the given domain or not.
 */
template <typename IndexType>
bool in_domain(IndexType v, const IndexType* domain) {
  return v >= domain[0] && v <= domain[1];
}

template <typename IndexType>
std::pair<bool, bool> ReaderBase::attribute_order_ned_bounds_already_validated(
    IndexType array_min_idx,
    IndexType array_max_idx,
    uint64_t f,
    std::vector<const void*>& non_empty_domains) {
  std::pair<bool, bool> ret{false, false};
  const IndexType* non_empty_domain =
      static_cast<const IndexType*>(non_empty_domains[f]);

  // Value is the array minimum, no need to validate.
  auto min = non_empty_domain[0];
  if (min == array_min_idx) {
    ret.first = true;
  }

  // Value is the array maximum, no need to validate.
  auto max = non_empty_domain[1];
  if (max == array_max_idx) {
    ret.second = true;
  }

  // Look at all the more recent fragments.
  for (uint64_t f2 = f + 1; f2 < fragment_metadata_.size(); f2++) {
    const IndexType* non_empty_domain2 =
        static_cast<const IndexType*>(non_empty_domains[f2]);
    if (!ret.first) {
      // See if the min is covered.
      ret.first |= in_domain(min, non_empty_domain2);

      // If the min if next to the max of a previous fragment, it will already
      // be validated when processing that fragment.
      ret.first |= min - 1 == non_empty_domain2[1];
    }

    if (!ret.second) {
      // See if the max is covered.
      ret.second |= in_domain(max, non_empty_domain2);

      // If the max if next to the min of a previous fragment, it will already
      // be validated when processing that fragment.
      ret.second |= max + 1 == non_empty_domain2[0];
    }
  }

  return ret;
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
  const IndexType* dim_dom = index_dim->domain().typed_data<IndexType>();
  auto tile_extent{index_dim->tile_extent().rvalue_as<IndexType>()};

  // See if some values will already be processed by previous fragments.
  AttributeOrderValidationData order_validation_data(fragment_metadata_.size());
  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(),
      0,
      fragment_metadata_.size(),
      [&](uint64_t f) {
        auto bounds_validated = attribute_order_ned_bounds_already_validated(
            array_min_idx, array_max_idx, f, non_empty_domains);
        order_validation_data.min_validated(f) = bounds_validated.first;
        order_validation_data.max_validated(f) = bounds_validated.second;

        return Status::Ok();
      }));

  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(),
      0,
      fragment_metadata_.size(),
      [&](int64_t f) {
        const IndexType* non_empty_domain =
            static_cast<const IndexType*>(non_empty_domains[f]);

        if (!order_validation_data.min_validated(f)) {
          // See if the min is tile aligned.
          auto min = non_empty_domain[0];
          bool min_tile_aligned = min == index_dim->round_to_tile<IndexType>(
                                             min, dim_dom[0], tile_extent);

          // Find a fragment that contains min - 1.
          for (int64_t f2 = f - 1; f2 >= 0; f2--) {
            const IndexType* non_empty_domain2 =
                static_cast<const IndexType*>(non_empty_domains[f2]);
            if (in_domain<IndexType>(min - 1, non_empty_domain2)) {
              // Get the min of the current fragment.
              auto value =
                  increasing_data ?
                      fragment_metadata_[f]->get_tile_min_as<AttributeType>(
                          attribute_name, 0) :
                      fragment_metadata_[f]->get_tile_max_as<AttributeType>(
                          attribute_name, 0);

              // Get the max from the intersecting fragment. If the min
              // is tile aligned, get the tile before.
              uint64_t f2_tile_idx = frag_first_array_tile_idx[f] -
                                     frag_first_array_tile_idx[f2] -
                                     min_tile_aligned;
              auto value_previous =
                  increasing_data ?
                      fragment_metadata_[f2]->get_tile_max_as<AttributeType>(
                          attribute_name, f2_tile_idx) :
                      fragment_metadata_[f2]->get_tile_min_as<AttributeType>(
                          attribute_name, f2_tile_idx);

              // If we are tile aligned or the min is right next to the other
              // fragment's max, we can validate. Otherwise we'll need to load
              // the tile.
              if (min_tile_aligned || min - 1 == non_empty_domain2[1]) {
                order_validation_data.min_validated(f) = true;

                // Check the order.
                if (increasing_data) {
                  if (value_previous > value) {
                    throw ReaderBaseStatusException("Attribute out of order");
                  }
                } else {
                  if (value_previous < value) {
                    throw ReaderBaseStatusException("Attribute out of order");
                  }
                }
              } else {
                // Add the tile to the list of tiles to load.
                order_validation_data.add_tile_to_load(
                    f, true, f2, f2_tile_idx, array_schema_);
              }

              break;
            }

            if (f2 == 0) {
              throw std::logic_error(
                  "Shouldn't reach the last fragment without finding overlap.");
            }
          }
        }

        if (!order_validation_data.max_validated(f)) {
          // See if the max is tile aligned.
          auto max = non_empty_domain[1];
          bool max_tile_aligned =
              max + 1 == index_dim->round_to_tile<IndexType>(
                             max + 1, dim_dom[0], tile_extent);

          // Find a fragment that contains max + 1.
          for (int64_t f2 = f - 1; f2 >= 0; f2--) {
            const IndexType* non_empty_domain2 =
                static_cast<const IndexType*>(non_empty_domains[f2]);
            if (in_domain<IndexType>(max + 1, non_empty_domain2)) {
              // Get the max of the current fragment.
              auto max_tile_idx = fragment_metadata_[f]->tile_num() - 1;
              auto value =
                  increasing_data ?
                      fragment_metadata_[f]->get_tile_max_as<AttributeType>(
                          attribute_name, max_tile_idx) :
                      fragment_metadata_[f]->get_tile_min_as<AttributeType>(
                          attribute_name, max_tile_idx);

              // Get the min from the intersecting fragment. If the max
              // is tile aligned, get the tile after.
              uint64_t f2_tile_idx =
                  max_tile_idx + frag_first_array_tile_idx[f] -
                  frag_first_array_tile_idx[f2] + max_tile_aligned;
              auto value_next =
                  increasing_data ?
                      fragment_metadata_[f2]->get_tile_min_as<AttributeType>(
                          attribute_name, f2_tile_idx) :
                      fragment_metadata_[f2]->get_tile_max_as<AttributeType>(
                          attribute_name, f2_tile_idx);

              // If we are tile aligned or the max is right next to the
              // other fragment's min, we can validate. Otherwise we'll
              // need to load the tile.
              if (max_tile_aligned || max + 1 == non_empty_domain2[0]) {
                order_validation_data.max_validated(f) = true;

                // Check the order.
                if (increasing_data) {
                  if (value_next < value) {
                    throw ReaderBaseStatusException("Attribute out of order");
                  }
                } else {
                  if (value_next > value) {
                    throw ReaderBaseStatusException("Attribute out of order");
                  }
                }
              } else {
                // Add the tile to the list of tiles to load.
                order_validation_data.add_tile_to_load(
                    f, false, f2, f2_tile_idx, array_schema_);
              }

              break;
            }

            if (f2 == 0) {
              throw std::logic_error(
                  "Shouldn't reach the last fragment without finding overlap.");
            }
          }
        }

        return Status::Ok();
      }));

  // If we need tiles to finish order validation, load them, then finish the
  // validation.
  if (order_validation_data.need_to_load_tiles()) {
    auto tiles_to_load = order_validation_data.tiles_to_load();
    throw_if_not_ok(read_attribute_tiles({attribute_name}, tiles_to_load));
    throw_if_not_ok(unfilter_tiles(attribute_name, tiles_to_load));
    validate_attribute_order_with_tile_data<IndexType, AttributeType>(
        attribute_name,
        increasing_data,
        non_empty_domains,
        frag_first_array_tile_idx,
        order_validation_data);
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

template <typename IndexType, typename AttributeType>
void ReaderBase::validate_attribute_order_with_tile_data(
    std::string& attribute_name,
    bool increasing_data,
    std::vector<const void*>& non_empty_domains,
    std::vector<uint64_t>& frag_first_array_tile_idx,
    AttributeOrderValidationData& order_validation_data) {
  // For easy reference.
  auto index_dim{array_schema_.domain().dimension_ptr(0)};
  auto tile_extent = index_dim->tile_extent().rvalue_as<IndexType>();
  const IndexType* dim_dom = index_dim->domain().typed_data<IndexType>();

  // Validate bounds not validated using tile data.
  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(),
      0,
      fragment_metadata_.size(),
      [&](unsigned f) {
        const IndexType* non_empty_domain =
            static_cast<const IndexType*>(non_empty_domains[f]);
        if (!order_validation_data.min_validated(f)) {
          // Get the min of the current fragment.
          auto value = fragment_metadata_[f]->get_tile_min_as<AttributeType>(
              attribute_name, 0);

          // Get the previous value from the loaded tile.
          auto rt = order_validation_data.min_tile_to_compare_against(f);
          const auto cell_idx =
              non_empty_domain[0] -
              index_dim->tile_coord_low(
                  rt->tile_idx() + frag_first_array_tile_idx[rt->frag_idx()],
                  dim_dom[0],
                  tile_extent) -
              1;
          AttributeType value_previous =
              rt->attribute_value<AttributeType>(attribute_name, cell_idx);

          // Validate the order.
          if (increasing_data) {
            if (value_previous > value) {
              throw ReaderBaseStatusException("Attribute out of order");
            }
          } else {
            if (value_previous < value) {
              throw ReaderBaseStatusException("Attribute out of order");
            }
          }
        }

        if (!order_validation_data.max_validated(f)) {
          // Get the min of the current fragment.
          auto max_tile_idx = fragment_metadata_[f]->tile_num() - 1;
          auto value = fragment_metadata_[f]->get_tile_max_as<AttributeType>(
              attribute_name, max_tile_idx);

          // Get the previous value from the loaded tile.
          auto rt = order_validation_data.max_tile_to_compare_against(f);
          const auto cell_idx =
              non_empty_domain[1] -
              index_dim->tile_coord_low(
                  rt->tile_idx() + frag_first_array_tile_idx[rt->frag_idx()],
                  dim_dom[0],
                  tile_extent) +
              1;
          AttributeType value_next =
              rt->attribute_value<AttributeType>(attribute_name, cell_idx);

          // Validate the order.
          if (increasing_data) {
            if (value > value_next) {
              throw ReaderBaseStatusException("Attribute out of order");
            }
          } else {
            if (value < value_next) {
              throw ReaderBaseStatusException("Attribute out of order");
            }
          }
        }

        return Status::Ok();
      }));
}

void ReaderBase::calculate_hilbert_values(
    const DomainBuffersView& domain_buffers,
    std::vector<uint64_t>& hilbert_values) const {
  auto dim_num = array_schema_.dim_num();
  Hilbert h(dim_num);
  auto bits = h.bits();
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;

  // Calculate Hilbert values in parallel
  uint64_t cell_num = hilbert_values.size();
  throw_if_not_ok(parallel_for(
      storage_manager_->compute_tp(), 0, cell_num, [&](uint64_t c) {
        std::vector<uint64_t> coords(dim_num);
        for (uint32_t d = 0; d < dim_num; ++d) {
          auto dim{array_schema_.dimension_ptr(d)};
          coords[d] = hilbert_order::map_to_uint64(
              *dim, domain_buffers[d], c, bits, max_bucket_val);
        }
        hilbert_values[c] = h.coords_to_hilbert(&coords[0]);

        return Status::Ok();
      }));
}

template <
    typename CmpObject,
    typename std::enable_if_t<
        std::is_same_v<CmpObject, HilbertCmpQB> ||
        std::is_same_v<CmpObject, GlobalCmpQB>>*>
BitSortFilterMetadataType ReaderBase::construct_bitsort_filter_argument(
    ResultTile* const tile,
    BitSortFilterMetadataStorage<CmpObject>& bitsort_storage) const {
  // Collect the storage vectors.
  std::vector<Tile*>& dim_tiles = bitsort_storage.dim_tiles_;
  std::vector<QueryBuffer>& query_buffers = bitsort_storage.query_buffers_;
  dimension_size_type num_dims = array_schema_.dim_num();
  std::vector<uint64_t> dim_data_sizes;

  dim_tiles.reserve(num_dims);
  query_buffers.reserve(num_dims);
  dim_data_sizes.reserve(num_dims);

  // Loop over the dimensions, adding the dimension tiles and constructed
  // QueryBuffer objects that represent the dimension tile data.
  for (dimension_size_type i = 0; i < num_dims; ++i) {
    const Dimension* dimension = array_schema_.domain().dimension_ptr(i);
    auto dim_tile_tuple = tile->tile_tuple(dimension->name());
    dim_tiles.emplace_back(&dim_tile_tuple->fixed_tile());

    auto& filtered_buffer = dim_tiles[i]->filtered_buffer();
    dim_data_sizes.emplace_back(filtered_buffer.size());
    query_buffers.emplace_back(
        filtered_buffer.data(), nullptr, &dim_data_sizes[i], nullptr);
  }

  bitsort_storage.db_.emplace(
      DomainBuffersView(array_schema_.domain(), query_buffers));
  if constexpr (std::is_same<CmpObject, HilbertCmpQB>::value) {
    std::vector<uint64_t>& hilbert_values = bitsort_storage.hilbert_values_;
    assert(dim_tiles.size() > 0);
    uint64_t cell_num = dim_tiles[0]->cell_num();
    hilbert_values.resize(cell_num);
    calculate_hilbert_values(bitsort_storage.db_.value(), hilbert_values);
    bitsort_storage.cmp_obj_.emplace(HilbertCmpQB(
        array_schema_.domain(), bitsort_storage.db_.value(), hilbert_values));
  } else if constexpr (std::is_same<CmpObject, GlobalCmpQB>::value) {
    bitsort_storage.cmp_obj_.emplace(
        GlobalCmpQB(array_schema_.domain(), bitsort_storage.db_.value()));
  }

  std::function<bool(const uint64_t&, const uint64_t&)>& comparator =
      bitsort_storage.comparator_;
  CmpObject& cmp_obj = bitsort_storage.cmp_obj_.value();
  comparator = [&cmp_obj](const uint64_t& left_idx, const uint64_t& right_idx) {
    return cmp_obj(left_idx, right_idx);
  };

  return bitsort_storage.get_bitsort_filter_metadata_type();
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
template void ReaderBase::ensure_continuous_domain_written<int8_t>(
    std::vector<const void*>&);
template void ReaderBase::ensure_continuous_domain_written<uint8_t>(
    std::vector<const void*>&);
template void ReaderBase::ensure_continuous_domain_written<int16_t>(
    std::vector<const void*>&);
template void ReaderBase::ensure_continuous_domain_written<uint16_t>(
    std::vector<const void*>&);
template void ReaderBase::ensure_continuous_domain_written<int32_t>(
    std::vector<const void*>&);
template void ReaderBase::ensure_continuous_domain_written<uint32_t>(
    std::vector<const void*>&);
template void ReaderBase::ensure_continuous_domain_written<int64_t>(
    std::vector<const void*>&);
template void ReaderBase::ensure_continuous_domain_written<uint64_t>(
    std::vector<const void*>&);
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