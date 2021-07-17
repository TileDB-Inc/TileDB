/**
 * @file   reader_base.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#include "tiledb/sm/query/reader_base.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/strategy_base.h"
#include "tiledb/sm/subarray/subarray.h"

namespace tiledb {
namespace sm {

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

ReaderBase::ReaderBase(
    stats::Stats* stats,
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout,
    QueryCondition& condition)
    : StrategyBase(
          stats, storage_manager, array, config, buffers, subarray, layout)
    , condition_(condition)
    , fix_var_sized_overflows_(false)
    , clear_coords_tiles_on_copy_(true)
    , copy_overflowed_(false) {
  if (array != nullptr)
    fragment_metadata_ = array->fragment_metadata();
}

/* ****************************** */
/*        PROTECTED METHODS       */
/* ****************************** */

void ReaderBase::clear_tiles(
    const std::string& name,
    const std::vector<ResultTile*>& result_tiles) const {
  for (auto& result_tile : result_tiles)
    result_tile->erase_tile(name);
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

Status ReaderBase::check_subarray() const {
  if (subarray_.layout() == Layout::GLOBAL_ORDER && subarray_.range_num() != 1)
    return LOG_STATUS(Status::ReaderError(
        "Cannot initialize reader; Multi-range subarrays with "
        "global order layout are not supported"));

  return Status::Ok();
}

Status ReaderBase::check_validity_buffer_sizes() const {
  // Verify that the validity buffer size for each
  // nullable attribute is large enough to contain
  // a validity value for each cell.
  for (const auto& it : buffers_) {
    const std::string& name = it.first;
    if (array_schema_->is_nullable(name)) {
      const uint64_t buffer_size = *it.second.buffer_size_;

      uint64_t min_cell_num = 0;
      if (array_schema_->var_size(name)) {
        min_cell_num = buffer_size / constants::cell_var_offset_size;

        // If the offsets buffer contains an extra element to mark
        // the offset to the end of the data buffer, we do not need
        // a validity value for that extra offset.
        if (offsets_extra_element_)
          min_cell_num = std::min<uint64_t>(0, min_cell_num - 1);
      } else {
        min_cell_num = buffer_size / array_schema_->cell_size(name);
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
        return LOG_STATUS(Status::ReaderError(ss.str()));
      }
    }
  }

  return Status::Ok();
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
      all_frag ? fragment_metadata_.size() : relevant_fragments.size(),
      [&](const uint64_t i) {
        auto frag_idx = all_frag ? i : relevant_fragments[i];
        auto& fragment = fragment_metadata_[frag_idx];
        const auto format_version = fragment->format_version();

        // Filter the 'names' for format-specific names.
        std::vector<std::string> filtered_names;
        filtered_names.reserve(names.size());
        for (const auto& name : names) {
          // Applicable for zipped coordinates only to versions < 5
          if (name == constants::coords && format_version >= 5)
            continue;

          // Applicable to separate coordinates only to versions >= 5
          const auto is_dim = array_schema_->is_dim(name);
          if (is_dim && format_version < 5)
            continue;

          filtered_names.emplace_back(name);
        }

        fragment->load_tile_offsets(*encryption_key, std::move(filtered_names));
        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status ReaderBase::init_tile(
    uint32_t format_version, const std::string& name, Tile* tile) const {
  // For easy reference
  auto cell_size = array_schema_->cell_size(name);
  auto type = array_schema_->type(name);
  auto is_coords = (name == constants::coords);
  auto dim_num = (is_coords) ? array_schema_->dim_num() : 0;

  // Initialize
  RETURN_NOT_OK(tile->init_filtered(format_version, type, cell_size, dim_num));

  return Status::Ok();
}

Status ReaderBase::init_tile(
    uint32_t format_version,
    const std::string& name,
    Tile* tile,
    Tile* tile_var) const {
  // For easy reference
  auto type = array_schema_->type(name);

  // Initialize
  RETURN_NOT_OK(tile->init_filtered(
      format_version,
      constants::cell_var_offset_type,
      constants::cell_var_offset_size,
      0));
  RETURN_NOT_OK(
      tile_var->init_filtered(format_version, type, datatype_size(type), 0));
  return Status::Ok();
}

Status ReaderBase::init_tile_nullable(
    uint32_t format_version,
    const std::string& name,
    Tile* tile,
    Tile* tile_validity) const {
  // For easy reference
  auto cell_size = array_schema_->cell_size(name);
  auto type = array_schema_->type(name);
  auto is_coords = (name == constants::coords);
  auto dim_num = (is_coords) ? array_schema_->dim_num() : 0;

  // Initialize
  RETURN_NOT_OK(tile->init_filtered(format_version, type, cell_size, dim_num));
  RETURN_NOT_OK(tile_validity->init_filtered(
      format_version,
      constants::cell_validity_type,
      constants::cell_validity_size,
      0));

  return Status::Ok();
}

Status ReaderBase::init_tile_nullable(
    uint32_t format_version,
    const std::string& name,
    Tile* tile,
    Tile* tile_var,
    Tile* tile_validity) const {
  // For easy reference
  auto type = array_schema_->type(name);

  // Initialize
  RETURN_NOT_OK(tile->init_filtered(
      format_version,
      constants::cell_var_offset_type,
      constants::cell_var_offset_size,
      0));
  RETURN_NOT_OK(
      tile_var->init_filtered(format_version, type, datatype_size(type), 0));
  RETURN_NOT_OK(tile_validity->init_filtered(
      format_version,
      constants::cell_validity_type,
      constants::cell_validity_size,
      0));
  return Status::Ok();
}

Status ReaderBase::read_attribute_tiles(
    const std::vector<std::string>& names,
    const std::vector<ResultTile*>& result_tiles) const {
  auto timer_se = stats_->start_timer("attr_tiles");
  return read_tiles(names, result_tiles);
}

Status ReaderBase::read_coordinate_tiles(
    const std::vector<std::string>& names,
    const std::vector<ResultTile*>& result_tiles) const {
  auto timer_se = stats_->start_timer("coord_tiles");
  return read_tiles(names, result_tiles);
}

Status ReaderBase::read_tiles(
    const std::vector<std::string>& names,
    const std::vector<ResultTile*>& result_tiles) const {
  // Shortcut for empty tile vec
  if (result_tiles.empty())
    return Status::Ok();

  // Reading tiles are thread safe. However, we will perform
  // them on this thread if there is only one read to perform.
  if (names.size() == 1) {
    RETURN_NOT_OK(read_tiles(names[0], result_tiles));
  } else {
    const auto status = parallel_for(
        storage_manager_->compute_tp(), 0, names.size(), [&](const uint64_t i) {
          RETURN_NOT_OK(read_tiles(names[i], result_tiles));
          return Status::Ok();
        });

    RETURN_NOT_OK(status);
  }

  return Status::Ok();
}

Status ReaderBase::read_tiles(
    const std::string& name,
    const std::vector<ResultTile*>& result_tiles) const {
  // Shortcut for empty tile vec
  if (result_tiles.empty())
    return Status::Ok();

  // Read the tiles asynchronously
  std::vector<ThreadPool::Task> tasks;
  RETURN_CANCEL_OR_ERROR(read_tiles(name, result_tiles, &tasks));

  // Wait for the reads to finish and check statuses.
  auto statuses = storage_manager_->io_tp()->wait_all_status(tasks);
  for (const auto& st : statuses)
    RETURN_CANCEL_OR_ERROR(st);

  return Status::Ok();
}

Status ReaderBase::read_tiles(
    const std::string& name,
    const std::vector<ResultTile*>& result_tiles,
    std::vector<ThreadPool::Task>* const tasks) const {
  // Shortcut for empty tile vec
  if (result_tiles.empty())
    return Status::Ok();

  // For each tile, read from its fragment.
  const bool var_size = array_schema_->var_size(name);
  const bool nullable = array_schema_->is_nullable(name);
  const auto encryption_key = array_->encryption_key();

  // Gather the unique fragments indexes for which there are tiles
  std::unordered_set<uint32_t> fragment_idxs_set;
  for (const auto& tile : result_tiles)
    fragment_idxs_set.emplace(tile->frag_idx());

  // Put fragment indexes in a vector
  std::vector<uint32_t> fragment_idxs_vec;
  fragment_idxs_vec.reserve(fragment_idxs_set.size());
  for (const auto& idx : fragment_idxs_set)
    fragment_idxs_vec.emplace_back(idx);

  // Protect all elements within `result_tiles`.
  std::unique_lock<std::mutex> ul(result_tiles_mutex_);

  // Populate the list of regions per file to be read.
  std::map<URI, std::vector<std::tuple<uint64_t, void*, uint64_t>>> all_regions;
  for (const auto& tile : result_tiles) {
    FragmentMetadata* const fragment = fragment_metadata_[tile->frag_idx()];
    const uint32_t format_version = fragment->format_version();

    // Applicable for zipped coordinates only to versions < 5
    if (name == constants::coords && format_version >= 5)
      continue;

    // Applicable to separate coordinates only to versions >= 5
    const bool is_dim = array_schema_->is_dim(name);
    if (is_dim && format_version < 5)
      continue;

    // Initialize the tile(s)
    if (is_dim) {
      const uint64_t dim_num = array_schema_->dim_num();
      for (uint64_t d = 0; d < dim_num; ++d) {
        if (array_schema_->dimension(d)->name() == name) {
          tile->init_coord_tile(name, d);
          break;
        }
      }
    } else {
      tile->init_attr_tile(name);
    }

    ResultTile::TileTuple* const tile_tuple = tile->tile_tuple(name);
    assert(tile_tuple != nullptr);
    Tile* const t = &std::get<0>(*tile_tuple);
    Tile* const t_var = &std::get<1>(*tile_tuple);
    Tile* const t_validity = &std::get<2>(*tile_tuple);
    if (!var_size) {
      if (nullable)
        RETURN_NOT_OK(init_tile_nullable(format_version, name, t, t_validity));
      else
        RETURN_NOT_OK(init_tile(format_version, name, t));
    } else {
      if (nullable)
        RETURN_NOT_OK(
            init_tile_nullable(format_version, name, t, t_var, t_validity));
      else
        RETURN_NOT_OK(init_tile(format_version, name, t, t_var));
    }

    // Get information about the tile in its fragment
    auto tile_attr_uri = fragment->uri(name);
    auto tile_idx = tile->tile_idx();
    uint64_t tile_attr_offset;
    RETURN_NOT_OK(fragment->file_offset(
        *encryption_key, name, tile_idx, &tile_attr_offset));
    uint64_t tile_persisted_size;
    RETURN_NOT_OK(fragment->persisted_tile_size(
        *encryption_key, name, tile_idx, &tile_persisted_size));

    // Try the cache first.
    // TODO Parallelize.
    bool cache_hit;
    RETURN_NOT_OK(storage_manager_->read_from_cache(
        tile_attr_uri,
        tile_attr_offset,
        t->filtered_buffer(),
        tile_persisted_size,
        &cache_hit));

    if (!cache_hit) {
      // Add the region of the fragment to be read.
      RETURN_NOT_OK(t->filtered_buffer()->realloc(tile_persisted_size));
      t->filtered_buffer()->set_size(tile_persisted_size);
      t->filtered_buffer()->reset_offset();
      all_regions[tile_attr_uri].emplace_back(
          tile_attr_offset, t->filtered_buffer()->data(), tile_persisted_size);
    }

    if (var_size) {
      auto tile_attr_var_uri = fragment->var_uri(name);
      uint64_t tile_attr_var_offset;
      RETURN_NOT_OK(fragment->file_var_offset(
          *encryption_key, name, tile_idx, &tile_attr_var_offset));
      uint64_t tile_var_persisted_size;
      RETURN_NOT_OK(fragment->persisted_tile_var_size(
          *encryption_key, name, tile_idx, &tile_var_persisted_size));

      Buffer cached_var_buffer;
      RETURN_NOT_OK(storage_manager_->read_from_cache(
          tile_attr_var_uri,
          tile_attr_var_offset,
          t_var->filtered_buffer(),
          tile_var_persisted_size,
          &cache_hit));

      if (!cache_hit) {
        // Add the region of the fragment to be read.
        RETURN_NOT_OK(
            t_var->filtered_buffer()->realloc(tile_var_persisted_size));
        t_var->filtered_buffer()->set_size(tile_var_persisted_size);
        t_var->filtered_buffer()->reset_offset();
        all_regions[tile_attr_var_uri].emplace_back(
            tile_attr_var_offset,
            t_var->filtered_buffer()->data(),
            tile_var_persisted_size);
      }
    }

    if (nullable) {
      auto tile_validity_attr_uri = fragment->validity_uri(name);
      uint64_t tile_attr_validity_offset;
      RETURN_NOT_OK(fragment->file_validity_offset(
          *encryption_key, name, tile_idx, &tile_attr_validity_offset));
      uint64_t tile_validity_persisted_size;
      RETURN_NOT_OK(fragment->persisted_tile_validity_size(
          *encryption_key, name, tile_idx, &tile_validity_persisted_size));

      Buffer cached_valdity_buffer;
      RETURN_NOT_OK(storage_manager_->read_from_cache(
          tile_validity_attr_uri,
          tile_attr_validity_offset,
          t_validity->filtered_buffer(),
          tile_validity_persisted_size,
          &cache_hit));

      if (!cache_hit) {
        // Add the region of the fragment to be read.
        RETURN_NOT_OK(t_validity->filtered_buffer()->realloc(
            tile_validity_persisted_size));
        t_validity->filtered_buffer()->set_size(tile_validity_persisted_size);
        t_validity->filtered_buffer()->reset_offset();
        all_regions[tile_validity_attr_uri].emplace_back(
            tile_attr_validity_offset,
            t_validity->filtered_buffer()->data(),
            tile_validity_persisted_size);
      }
    }
  }

  // We're done accessing elements within `result_tiles`.
  ul.unlock();

  // Do not use the read-ahead cache because tiles will be
  // cached in the tile cache.
  const bool use_read_ahead = false;

  // Enqueue all regions to be read.
  for (const auto& item : all_regions) {
    RETURN_NOT_OK(storage_manager_->vfs()->read_all(
        item.first,
        item.second,
        storage_manager_->io_tp(),
        tasks,
        use_read_ahead));
  }

  return Status::Ok();
}

Status ReaderBase::unfilter_tiles(
    const std::string& name,
    const std::vector<ResultTile*>& result_tiles,
    const ResultCellSlabsIndex* const rcs_index) const {
  auto stat_type = (array_schema_->is_attr(name)) ? "unfilter_attr_tiles" :
                                                    "unfilter_coord_tiles";
  auto timer_se = stats_->start_timer(stat_type);

  auto var_size = array_schema_->var_size(name);
  auto nullable = array_schema_->is_nullable(name);
  auto num_tiles = static_cast<uint64_t>(result_tiles.size());
  auto encryption_key = array_->encryption_key();

  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, num_tiles, [&, this](uint64_t i) {
        ResultTile* const tile = result_tiles[i];

        auto& fragment = fragment_metadata_[tile->frag_idx()];
        auto format_version = fragment->format_version();

        // Applicable for zipped coordinates only to versions < 5
        // Applicable for separate coordinates only to version >= 5
        if (name != constants::coords ||
            (name == constants::coords && format_version < 5) ||
            (array_schema_->is_dim(name) && format_version >= 5)) {
          auto tile_tuple = tile->tile_tuple(name);

          // Skip non-existent attributes/dimensions (e.g. coords in the
          // dense case).
          if (tile_tuple == nullptr ||
              std::get<0>(*tile_tuple).filtered_buffer()->size() == 0)
            return Status::Ok();

          // Get information about the tile in its fragment.
          auto tile_attr_uri = fragment->uri(name);
          auto tile_idx = tile->tile_idx();
          uint64_t tile_attr_offset;
          RETURN_NOT_OK(fragment->file_offset(
              *encryption_key, name, tile_idx, &tile_attr_offset));

          auto& t = std::get<0>(*tile_tuple);
          auto& t_var = std::get<1>(*tile_tuple);
          auto& t_validity = std::get<2>(*tile_tuple);

          // If we're performing selective unfiltering, lookup the result
          // cell slab ranges associated with this tile. If we do not have
          // any ranges, use an empty list to indicate that this tile doesn't
          // contain any results.
          const std::vector<std::pair<uint64_t, uint64_t>>*
              result_cell_slab_ranges = nullptr;
          static const std::vector<std::pair<uint64_t, uint64_t>> empty_ranges;
          if (rcs_index) {
            result_cell_slab_ranges =
                rcs_index->find(tile) != rcs_index->end() ?
                    &rcs_index->at(tile) :
                    &empty_ranges;
          }

          // Cache 't'.
          if (t.filtered()) {
            // Store the filtered buffer in the tile cache.
            RETURN_NOT_OK(storage_manager_->write_to_cache(
                tile_attr_uri, tile_attr_offset, t.filtered_buffer()));
          }

          // Cache 't_var'.
          if (var_size && t_var.filtered()) {
            auto tile_attr_var_uri = fragment->var_uri(name);
            uint64_t tile_attr_var_offset;
            RETURN_NOT_OK(fragment->file_var_offset(
                *encryption_key, name, tile_idx, &tile_attr_var_offset));

            // Store the filtered buffer in the tile cache.
            RETURN_NOT_OK(storage_manager_->write_to_cache(
                tile_attr_var_uri,
                tile_attr_var_offset,
                t_var.filtered_buffer()));
          }

          // Cache 't_validity'.
          if (nullable && t_validity.filtered()) {
            auto tile_attr_validity_uri = fragment->validity_uri(name);
            uint64_t tile_attr_validity_offset;
            RETURN_NOT_OK(fragment->file_validity_offset(
                *encryption_key, name, tile_idx, &tile_attr_validity_offset));

            // Store the filtered buffer in the tile cache.
            RETURN_NOT_OK(storage_manager_->write_to_cache(
                tile_attr_validity_uri,
                tile_attr_validity_offset,
                t_validity.filtered_buffer()));
          }

          // Unfilter 't' for fixed-sized tiles, otherwise unfilter both 't' and
          // 't_var' for var-sized tiles.
          if (!var_size) {
            if (!nullable)
              RETURN_NOT_OK(unfilter_tile(name, &t, result_cell_slab_ranges));
            else
              RETURN_NOT_OK(unfilter_tile_nullable(
                  name, &t, &t_validity, result_cell_slab_ranges));
          } else {
            if (!nullable)
              RETURN_NOT_OK(
                  unfilter_tile(name, &t, &t_var, result_cell_slab_ranges));
            else
              RETURN_NOT_OK(unfilter_tile_nullable(
                  name, &t, &t_var, &t_validity, result_cell_slab_ranges));
          }
        }

        return Status::Ok();
      });

  RETURN_CANCEL_OR_ERROR(status);

  return Status::Ok();
}

Status ReaderBase::unfilter_tile(
    const std::string& name,
    Tile* tile,
    const std::vector<std::pair<uint64_t, uint64_t>>* result_cell_slab_ranges)
    const {
  FilterPipeline filters = array_schema_->filters(name);

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  // Skip selective unfiltering on coordinate tiles.
  if (name == constants::coords || tile->stores_coords()) {
    result_cell_slab_ranges = nullptr;
  }

  // Reverse the tile filters.
  RETURN_NOT_OK(filters.run_reverse(
      stats_,
      tile,
      storage_manager_->compute_tp(),
      storage_manager_->config(),
      result_cell_slab_ranges));

  return Status::Ok();
}

Status ReaderBase::unfilter_tile(
    const std::string& name,
    Tile* tile,
    Tile* tile_var,
    const std::vector<std::pair<uint64_t, uint64_t>>* result_cell_slab_ranges)
    const {
  FilterPipeline offset_filters = array_schema_->cell_var_offsets_filters();
  FilterPipeline filters = array_schema_->filters(name);

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &offset_filters, array_->get_encryption_key()));
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  // Skip selective unfiltering on coordinate tiles.
  if (name == constants::coords || tile->stores_coords()) {
    result_cell_slab_ranges = nullptr;
  }

  // Reverse the tile filters, but do not use selective
  // unfiltering for offset tiles.
  RETURN_NOT_OK(offset_filters.run_reverse(
      stats_, tile, storage_manager_->compute_tp(), config_, nullptr));
  RETURN_NOT_OK(filters.run_reverse(
      stats_,
      tile,
      tile_var,
      storage_manager_->compute_tp(),
      config_,
      result_cell_slab_ranges));

  return Status::Ok();
}

Status ReaderBase::unfilter_tile_nullable(
    const std::string& name,
    Tile* tile,
    Tile* tile_validity,
    const std::vector<std::pair<uint64_t, uint64_t>>* result_cell_slab_ranges)
    const {
  FilterPipeline filters = array_schema_->filters(name);
  FilterPipeline validity_filters = array_schema_->cell_validity_filters();

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &validity_filters, array_->get_encryption_key()));

  // Skip selective unfiltering on coordinate tiles.
  if (name == constants::coords || tile->stores_coords()) {
    result_cell_slab_ranges = nullptr;
  }

  // Reverse the tile filters.
  RETURN_NOT_OK(filters.run_reverse(
      stats_,
      tile,
      storage_manager_->compute_tp(),
      storage_manager_->config(),
      result_cell_slab_ranges));

  // Reverse the validity tile filters, without
  // selective decompression.
  RETURN_NOT_OK(validity_filters.run_reverse(
      stats_,
      tile_validity,
      storage_manager_->compute_tp(),
      storage_manager_->config(),
      nullptr));

  return Status::Ok();
}

Status ReaderBase::unfilter_tile_nullable(
    const std::string& name,
    Tile* tile,
    Tile* tile_var,
    Tile* tile_validity,
    const std::vector<std::pair<uint64_t, uint64_t>>* result_cell_slab_ranges)
    const {
  FilterPipeline offset_filters = array_schema_->cell_var_offsets_filters();
  FilterPipeline filters = array_schema_->filters(name);
  FilterPipeline validity_filters = array_schema_->cell_validity_filters();

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &offset_filters, array_->get_encryption_key()));
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &validity_filters, array_->get_encryption_key()));

  // Skip selective unfiltering on coordinate tiles.
  if (name == constants::coords || tile->stores_coords()) {
    result_cell_slab_ranges = nullptr;
  }

  // Reverse the tile filters, but do not use selective
  // unfiltering for offset tiles.
  RETURN_NOT_OK(offset_filters.run_reverse(
      stats_,
      tile,
      storage_manager_->compute_tp(),
      storage_manager_->config(),
      nullptr));
  RETURN_NOT_OK(filters.run_reverse(
      stats_,
      tile,
      tile_var,
      storage_manager_->compute_tp(),
      storage_manager_->config(),
      result_cell_slab_ranges));

  // Reverse the validity tile filters, without
  // selective decompression.
  RETURN_NOT_OK(validity_filters.run_reverse(
      stats_,
      tile_validity,
      storage_manager_->compute_tp(),
      storage_manager_->config(),
      nullptr));

  return Status::Ok();
}

Status ReaderBase::copy_coordinates(
    const std::vector<ResultTile*>& result_tiles,
    std::vector<ResultCellSlab>& result_cell_slabs) {
  auto timer_se = stats_->start_timer("copy_coordinates");

  if (result_cell_slabs.empty() && result_tiles.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  const uint64_t stride = UINT64_MAX;

  // Build a list of coordinate names to copy, separating them by
  // whether they are of fixed or variable length. The motivation
  // is that copying fixed and variable cells require two different
  // context caches. Processing them separately allows us to maintain
  // a single context cache at the same time to reduce memory use.
  std::vector<std::string> fixed_names;
  std::vector<std::string> var_names;

  for (const auto& it : buffers_) {
    const auto& name = it.first;
    if (copy_overflowed_)
      break;
    if (!(name == constants::coords || array_schema_->is_dim(name)))
      continue;

    if (array_schema_->var_size(name))
      var_names.emplace_back(name);
    else
      fixed_names.emplace_back(name);
  }

  // Copy result cells for fixed-sized coordinates.
  if (!fixed_names.empty()) {
    CopyFixedCellsContextCache ctx_cache;
    for (const auto& name : fixed_names) {
      RETURN_CANCEL_OR_ERROR(
          copy_fixed_cells(name, stride, result_cell_slabs, &ctx_cache));
      if (clear_coords_tiles_on_copy_)
        clear_tiles(name, result_tiles);
    }
  }

  // Copy result cells for var-sized coordinates.
  if (!var_names.empty()) {
    CopyVarCellsContextCache ctx_cache;
    for (const auto& name : var_names) {
      RETURN_CANCEL_OR_ERROR(
          copy_var_cells(name, stride, result_cell_slabs, &ctx_cache));
      if (clear_coords_tiles_on_copy_)
        clear_tiles(name, result_tiles);
    }
  }

  return Status::Ok();
}

Status ReaderBase::copy_attribute_values(
    const uint64_t stride,
    const std::vector<ResultTile*>& result_tiles,
    std::vector<ResultCellSlab>& result_cell_slabs,
    Subarray& subarray) {
  auto timer_se = stats_->start_timer("copy_attr_values");

  if (result_cell_slabs.empty() && result_tiles.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  const std::unordered_set<std::string>& condition_names =
      condition_.field_names();

  // Build a set of attribute names to process.
  std::unordered_map<std::string, ProcessTileFlags> names;
  for (const auto& it : buffers_) {
    const auto& name = it.first;

    if (copy_overflowed_) {
      break;
    }

    if (name == constants::coords || array_schema_->is_dim(name)) {
      continue;
    }

    // If the query condition has a clause for `name`, we will only
    // flag it to copy because we have already preloaded the offsets
    // and read the tiles in `apply_query_condition`.
    ProcessTileFlags flags = ProcessTileFlag::COPY;
    if (condition_names.count(name) == 0) {
      flags |= ProcessTileFlag::READ;
      flags |= ProcessTileFlag::SELECTIVE_UNFILTERING;
    }

    names[name] = flags;
  }

  RETURN_NOT_OK(
      process_tiles(names, result_tiles, result_cell_slabs, subarray, stride));

  return Status::Ok();
}

Status ReaderBase::copy_fixed_cells(
    const std::string& name,
    uint64_t stride,
    const std::vector<ResultCellSlab>& result_cell_slabs,
    CopyFixedCellsContextCache* const ctx_cache) {
  assert(ctx_cache);

  auto stat_type = (array_schema_->is_attr(name)) ? "copy_fixed_attr_values" :
                                                    "copy_fixed_coords";
  auto timer_se = stats_->start_timer(stat_type);

  if (result_cell_slabs.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // Perform a lazy initialization of the context cache for copying
  // fixed cells.
  populate_cfc_ctx_cache(result_cell_slabs, ctx_cache);

  auto it = buffers_.find(name);
  auto buffer_size = it->second.buffer_size_;
  auto cell_size = array_schema_->cell_size(name);

  // Precompute the cell range destination offsets in the buffer.
  uint64_t buffer_offset = 0;
  tdb_unique_ptr<std::vector<uint64_t>> cs_offsets =
      ctx_cache->get_cs_offsets();
  for (uint64_t i = 0; i < cs_offsets->size(); i++) {
    const auto& cs = result_cell_slabs[i];
    auto bytes_to_copy = cs.length_ * cell_size;
    (*cs_offsets)[i] = buffer_offset;
    buffer_offset += bytes_to_copy;
  }

  // Handle overflow.
  if (buffer_offset > *buffer_size) {
    copy_overflowed_ = true;
    return Status::Ok();
  }

  // Copy result cell slabs in parallel.
  std::function<Status(size_t)> copy_fn = std::bind(
      &ReaderBase::copy_partitioned_fixed_cells,
      this,
      std::placeholders::_1,
      &name,
      stride,
      &result_cell_slabs,
      *cs_offsets,
      *ctx_cache->cs_partitions());
  auto status = parallel_for(
      storage_manager_->compute_tp(),
      0,
      ctx_cache->cs_partitions()->size(),
      std::move(copy_fn));

  RETURN_NOT_OK(status);

  // Update buffer offsets
  *(buffers_[name].buffer_size_) = buffer_offset;
  if (array_schema_->is_nullable(name)) {
    *(buffers_[name].validity_vector_.buffer_size()) =
        (buffer_offset / cell_size) * constants::cell_validity_size;
  }

  return Status::Ok();
}

void ReaderBase::populate_cfc_ctx_cache(
    const std::vector<ResultCellSlab>& result_cell_slabs,
    CopyFixedCellsContextCache* const ctx_cache) {
  const int num_copy_threads =
      storage_manager_->compute_tp()->concurrency_level();

  // Initialize the context cache. This is a no-op if already initialized.
  ctx_cache->initialize(result_cell_slabs, num_copy_threads);
}

uint64_t ReaderBase::offsets_bytesize() const {
  assert(offsets_bitsize_ == 32 || offsets_bitsize_ == 64);
  return offsets_bitsize_ == 32 ? sizeof(uint32_t) :
                                  constants::cell_var_offset_size;
}

Status ReaderBase::copy_partitioned_fixed_cells(
    const size_t partition_idx,
    const std::string* const name,
    const uint64_t stride,
    const std::vector<ResultCellSlab>* const result_cell_slabs,
    const std::vector<uint64_t>& cs_offsets,
    const std::vector<size_t>& cs_partitions) {
  assert(name);
  assert(result_cell_slabs);

  // For easy reference.
  auto nullable = array_schema_->is_nullable(*name);
  auto it = buffers_.find(*name);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_validity = (unsigned char*)it->second.validity_vector_.buffer();
  auto cell_size = array_schema_->cell_size(*name);
  ByteVecValue fill_value;
  uint8_t fill_value_validity = 0;
  if (array_schema_->is_attr(*name)) {
    fill_value = array_schema_->attribute(*name)->fill_value();
    fill_value_validity =
        array_schema_->attribute(*name)->fill_value_validity();
  }
  uint64_t fill_value_size = (uint64_t)fill_value.size();

  // Calculate the partition to operate on.
  const uint64_t cs_idx_start =
      partition_idx == 0 ? 0 : cs_partitions[partition_idx - 1];
  const uint64_t cs_idx_end = cs_partitions[partition_idx];

  // Copy the cells.
  for (uint64_t cs_idx = cs_idx_start; cs_idx < cs_idx_end; ++cs_idx) {
    const auto& cs = (*result_cell_slabs)[cs_idx];
    uint64_t offset = cs_offsets[cs_idx];

    // Copy
    if (cs.tile_ == nullptr) {  // Empty range
      auto bytes_to_copy = cs.length_ * cell_size;
      auto fill_num = bytes_to_copy / fill_value_size;
      for (uint64_t j = 0; j < fill_num; ++j) {
        std::memcpy(buffer + offset, fill_value.data(), fill_value_size);
        if (nullable) {
          std::memset(
              buffer_validity +
                  (offset / cell_size * constants::cell_validity_size),
              fill_value_validity,
              constants::cell_validity_size);
        }
        offset += fill_value_size;
      }
    } else {  // Non-empty range
      if (stride == UINT64_MAX) {
        if (!nullable)
          RETURN_NOT_OK(
              cs.tile_->read(*name, buffer, offset, cs.start_, cs.length_));
        else
          RETURN_NOT_OK(cs.tile_->read_nullable(
              *name, buffer, offset, cs.start_, cs.length_, buffer_validity));
      } else {
        auto cell_offset = offset;
        auto start = cs.start_;
        for (uint64_t j = 0; j < cs.length_; ++j) {
          if (!nullable)
            RETURN_NOT_OK(cs.tile_->read(*name, buffer, cell_offset, start, 1));
          else
            RETURN_NOT_OK(cs.tile_->read_nullable(
                *name, buffer, cell_offset, start, 1, buffer_validity));
          cell_offset += cell_size;
          start += stride;
        }
      }
    }
  }

  return Status::Ok();
}

Status ReaderBase::copy_var_cells(
    const std::string& name,
    const uint64_t stride,
    std::vector<ResultCellSlab>& result_cell_slabs,
    CopyVarCellsContextCache* const ctx_cache) {
  assert(ctx_cache);

  auto stat_type = (array_schema_->is_attr(name)) ? "copy_var_attr_values" :
                                                    "copy_var_coords";
  auto timer_se = stats_->start_timer(stat_type);

  if (result_cell_slabs.empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // Perform a lazy initialization of the context cache for copying
  // fixed cells.
  populate_cvc_ctx_cache(result_cell_slabs, ctx_cache);

  // Compute the destinations of offsets and var-len data in the buffers.
  uint64_t total_offset_size, total_var_size, total_validity_size;
  tdb_unique_ptr<std::vector<uint64_t>> offset_offsets_per_cs =
      ctx_cache->get_offset_offsets_per_cs();
  tdb_unique_ptr<std::vector<uint64_t>> var_offsets_per_cs =
      ctx_cache->get_var_offsets_per_cs();

  RETURN_NOT_OK(compute_var_cell_destinations(
      name,
      stride,
      result_cell_slabs,
      offset_offsets_per_cs.get(),
      var_offsets_per_cs.get(),
      &total_offset_size,
      &total_var_size,
      &total_validity_size));

  // Check for overflow and return early (without copying) in that case.
  if (copy_overflowed_) {
    return Status::Ok();
  }

  // Copy result cell slabs in parallel
  std::function<Status(size_t)> copy_fn = std::bind(
      &ReaderBase::copy_partitioned_var_cells,
      this,
      std::placeholders::_1,
      &name,
      stride,
      &result_cell_slabs,
      offset_offsets_per_cs.get(),
      var_offsets_per_cs.get(),
      ctx_cache->cs_partitions());
  auto status = parallel_for(
      storage_manager_->compute_tp(),
      0,
      ctx_cache->cs_partitions()->size(),
      copy_fn);

  RETURN_NOT_OK(status);

  // Update buffer offsets
  *(buffers_[name].buffer_size_) = total_offset_size;
  *(buffers_[name].buffer_var_size_) = total_var_size;
  if (array_schema_->is_nullable(name))
    *(buffers_[name].validity_vector_.buffer_size()) = total_validity_size;

  return Status::Ok();
}

void ReaderBase::populate_cvc_ctx_cache(
    const std::vector<ResultCellSlab>& result_cell_slabs,
    CopyVarCellsContextCache* const ctx_cache) {
  const int num_copy_threads =
      storage_manager_->compute_tp()->concurrency_level();

  // Initialize the context cache. This is a no-op if already initialized.
  ctx_cache->initialize(result_cell_slabs, num_copy_threads);
}

Status ReaderBase::compute_var_cell_destinations(
    const std::string& name,
    uint64_t stride,
    std::vector<ResultCellSlab>& result_cell_slabs,
    std::vector<uint64_t>* offset_offsets_per_cs,
    std::vector<uint64_t>* var_offsets_per_cs,
    uint64_t* total_offset_size,
    uint64_t* total_var_size,
    uint64_t* total_validity_size) {
  // For easy reference
  auto nullable = array_schema_->is_nullable(name);
  auto num_cs = result_cell_slabs.size();
  auto offset_size = offsets_bytesize();
  ByteVecValue fill_value;
  if (array_schema_->is_attr(name))
    fill_value = array_schema_->attribute(name)->fill_value();
  auto fill_value_size = (uint64_t)fill_value.size();

  auto it = buffers_.find(name);
  auto buffer_size = *it->second.buffer_size_;
  auto buffer_var_size = *it->second.buffer_var_size_;
  auto buffer_validity_size = it->second.validity_vector_.buffer_size();

  if (offsets_extra_element_)
    buffer_size -= offset_size;

  // Compute the destinations for all result cell slabs
  *total_offset_size = 0;
  *total_var_size = 0;
  *total_validity_size = 0;
  size_t total_cs_length = 0;
  for (uint64_t cs_idx = 0; cs_idx < num_cs; cs_idx++) {
    const auto& cs = result_cell_slabs[cs_idx];

    // Get tile information, if the range is nonempty.
    uint64_t* tile_offsets = nullptr;
    uint64_t tile_cell_num = 0;
    uint64_t tile_var_size = 0;
    if (cs.tile_ != nullptr) {
      const auto tile_tuple = cs.tile_->tile_tuple(name);
      const auto& tile = std::get<0>(*tile_tuple);
      const auto& tile_var = std::get<1>(*tile_tuple);

      // Get the internal buffer to the offset values.
      ChunkedBuffer* const chunked_buffer = tile.chunked_buffer();

      // Offset tiles are always contiguously allocated.
      assert(
          chunked_buffer->buffer_addressing() ==
          ChunkedBuffer::BufferAddressing::CONTIGUOUS);

      tile_offsets = (uint64_t*)chunked_buffer->get_contiguous_unsafe();
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
        cell_var_size = fill_value_size;
      } else {
        cell_var_size =
            (cell_idx != tile_cell_num - 1) ?
                tile_offsets[cell_idx + 1] - tile_offsets[cell_idx] :
                tile_var_size - (tile_offsets[cell_idx] - tile_offsets[0]);
      }

      if (*total_offset_size + offset_size > buffer_size ||
          *total_var_size + cell_var_size > buffer_var_size ||
          (buffer_validity_size &&
           *total_validity_size + constants::cell_validity_size >
               *buffer_validity_size)) {
        // Try to fix the overflow by reducing the result cell slabs.
        if (fix_var_sized_overflows_) {
          // Remove slabs until we reach the one that caused the overflow.
          while (result_cell_slabs.size() != cs_idx + 1) {
            result_cell_slabs.pop_back();
          }

          // Adjust the length of the result cell slab.
          auto length = cell_idx - result_cell_slabs.back().start_;
          result_cell_slabs.back().length_ = length;

          // If the overflow occured on the first cell, remove the slab.
          if (length == 0) {
            result_cell_slabs.pop_back();
          }

          // Cannot even copy one cell, return overflow.
          if (result_cell_slabs.size() == 0) {
            copy_overflowed_ = true;
          }
        } else {
          copy_overflowed_ = true;
        }

        // In case an extra offset is configured, we need to account memory for
        // it on each read
        *total_offset_size += offsets_extra_element_ ? offset_size : 0;

        return Status::Ok();
      }

      // Record destination offsets.
      (*offset_offsets_per_cs)[total_cs_length + dest_vec_idx] =
          *total_offset_size;
      (*var_offsets_per_cs)[total_cs_length + dest_vec_idx] = *total_var_size;
      *total_offset_size += offset_size;
      *total_var_size += cell_var_size;
      if (nullable)
        *total_validity_size += constants::cell_validity_size;
    }

    total_cs_length += cs.length_;
  }

  // In case an extra offset is configured, we need to account memory for it on
  // each read
  *total_offset_size += offsets_extra_element_ ? offset_size : 0;

  return Status::Ok();
}

Status ReaderBase::copy_partitioned_var_cells(
    const size_t partition_idx,
    const std::string* const name,
    uint64_t stride,
    const std::vector<ResultCellSlab>* const result_cell_slabs,
    const std::vector<uint64_t>* const offset_offsets_per_cs,
    const std::vector<uint64_t>* const var_offsets_per_cs,
    const std::vector<std::pair<size_t, size_t>>* const cs_partitions) {
  assert(name);
  assert(result_cell_slabs);

  auto it = buffers_.find(*name);
  auto nullable = array_schema_->is_nullable(*name);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_var = (unsigned char*)it->second.buffer_var_;
  auto buffer_validity = (unsigned char*)it->second.validity_vector_.buffer();
  auto offset_size = offsets_bytesize();
  ByteVecValue fill_value;
  uint8_t fill_value_validity = 0;
  if (array_schema_->is_attr(*name)) {
    fill_value = array_schema_->attribute(*name)->fill_value();
    fill_value_validity =
        array_schema_->attribute(*name)->fill_value_validity();
  }
  auto fill_value_size = (uint64_t)fill_value.size();
  auto attr_datatype_size = datatype_size(array_schema_->type(*name));

  // Fetch the starting array offset into both `offset_offsets_per_cs`
  // and `var_offsets_per_cs`.
  size_t arr_offset =
      partition_idx == 0 ? 0 : (*cs_partitions)[partition_idx - 1].first;

  // Fetch the inclusive starting cell slab index and the exclusive ending
  // cell slab index.
  const size_t start_cs_idx =
      partition_idx == 0 ? 0 : (*cs_partitions)[partition_idx - 1].second;
  const size_t end_cs_idx = (*cs_partitions)[partition_idx].second;

  // Copy all cells within the range of cell slabs.
  for (uint64_t cs_idx = start_cs_idx; cs_idx < end_cs_idx; ++cs_idx) {
    // If there was an overflow and we fixed it, don't copy past the new
    // result cell slabs.
    if (cs_idx >= result_cell_slabs->size()) {
      break;
    }
    const auto& cs = (*result_cell_slabs)[cs_idx];

    // Get tile information, if the range is nonempty.
    uint64_t* tile_offsets = nullptr;
    Tile* tile_var = nullptr;
    Tile* tile_validity = nullptr;
    uint64_t tile_cell_num = 0;
    if (cs.tile_ != nullptr) {
      const auto tile_tuple = cs.tile_->tile_tuple(*name);
      Tile* const tile = &std::get<0>(*tile_tuple);
      tile_var = &std::get<1>(*tile_tuple);
      tile_validity = &std::get<2>(*tile_tuple);

      // Get the internal buffer to the offset values.
      ChunkedBuffer* const chunked_buffer = tile->chunked_buffer();

      // Offset tiles are always contiguously allocated.
      assert(
          chunked_buffer->buffer_addressing() ==
          ChunkedBuffer::BufferAddressing::CONTIGUOUS);

      tile_offsets = (uint64_t*)chunked_buffer->get_contiguous_unsafe();
      tile_cell_num = tile->cell_num();
    }

    // Copy each cell in the range
    uint64_t dest_vec_idx = 0;
    stride = (stride == UINT64_MAX) ? 1 : stride;
    for (auto cell_idx = cs.start_; dest_vec_idx < cs.length_;
         cell_idx += stride, dest_vec_idx++) {
      auto offset_offsets = (*offset_offsets_per_cs)[arr_offset + dest_vec_idx];
      auto offset_dest = buffer + offset_offsets;
      auto var_offset = (*var_offsets_per_cs)[arr_offset + dest_vec_idx];
      auto var_dest = buffer_var + var_offset;
      auto validity_dest = buffer_validity + (offset_offsets / offset_size);

      if (offsets_format_mode_ == "elements") {
        var_offset = var_offset / attr_datatype_size;
      }

      // Copy offset
      std::memcpy(offset_dest, &var_offset, offset_size);

      // Copy variable-sized value
      if (cs.tile_ == nullptr) {
        std::memcpy(var_dest, fill_value.data(), fill_value_size);
        if (nullable)
          std::memset(
              validity_dest,
              fill_value_validity,
              constants::cell_validity_size);
      } else {
        const uint64_t cell_var_size =
            (cell_idx != tile_cell_num - 1) ?
                tile_offsets[cell_idx + 1] - tile_offsets[cell_idx] :
                tile_var->size() - (tile_offsets[cell_idx] - tile_offsets[0]);
        const uint64_t tile_var_offset =
            tile_offsets[cell_idx] - tile_offsets[0];

        RETURN_NOT_OK(tile_var->read(var_dest, cell_var_size, tile_var_offset));

        if (nullable)
          RETURN_NOT_OK(tile_validity->read(
              validity_dest, constants::cell_validity_size, cell_idx));
      }
    }

    arr_offset += cs.length_;
  }

  return Status::Ok();
}

Status ReaderBase::process_tiles(
    const std::unordered_map<std::string, ProcessTileFlags>& names,
    const std::vector<ResultTile*>& result_tiles,
    std::vector<ResultCellSlab>& result_cell_slabs,
    Subarray& subarray,
    const uint64_t stride) {
  // If a name needs to be read, we put it on `read_names` vector (it may
  // contain other flags). Otherwise, we put the name on the `copy_names`
  // vector if it needs to be copied back to the user buffer.
  // We can benefit from concurrent reads by processing `read_names`
  // separately from `copy_names`.
  std::vector<std::string> read_names;
  std::vector<std::string> copy_names;
  read_names.reserve(names.size());
  for (const auto& name_pair : names) {
    const std::string name = name_pair.first;
    const ProcessTileFlags flags = name_pair.second;
    if (flags & ProcessTileFlag::READ) {
      read_names.push_back(name);
    } else if (flags & ProcessTileFlag::COPY) {
      copy_names.push_back(name);
    }
  }

  // Pre-load all attribute offsets into memory for attributes
  // to be read.
  load_tile_offsets(subarray, read_names);

  // Get the maximum number of attributes to read and unfilter in parallel.
  // Each attribute requires additional memory to buffer reads into
  // before copying them back into `buffers_`. Cells must be copied
  // before moving onto the next set of concurrent reads to prevent
  // bloating memory. Additionally, the copy cells paths are performed
  // in serial, which will bottleneck the read concurrency. Increasing
  // this number will have diminishing returns on performance.
  const uint64_t concurrent_reads = constants::concurrent_attr_reads;

  // Instantiate context caches for copying fixed and variable
  // cells.
  CopyFixedCellsContextCache fixed_ctx_cache;
  CopyVarCellsContextCache var_ctx_cache;

  // Handle attribute/dimensions that need to be copied but do
  // not need to be read.
  for (const auto& copy_name : copy_names) {
    if (!array_schema_->var_size(copy_name))
      RETURN_CANCEL_OR_ERROR(copy_fixed_cells(
          copy_name, stride, result_cell_slabs, &fixed_ctx_cache));
    else
      RETURN_CANCEL_OR_ERROR(
          copy_var_cells(copy_name, stride, result_cell_slabs, &var_ctx_cache));
    clear_tiles(copy_name, result_tiles);
  }

  // Iterate through all of the attribute names. This loop
  // will read, unfilter, and copy tiles back into the `buffers_`.
  uint64_t idx = 0;
  tdb_unique_ptr<ResultCellSlabsIndex> rcs_index = nullptr;
  while (idx < read_names.size()) {
    // We will perform `concurrent_reads` unless we have a smaller
    // number of remaining attributes to process.
    const uint64_t num_reads =
        std::min(concurrent_reads, read_names.size() - idx);

    // Build a vector of the attribute names to process.
    std::vector<std::string> inner_names(
        read_names.begin() + idx, read_names.begin() + idx + num_reads);

    // Read the tiles for the names in `inner_names`. Each attribute
    // name will be read concurrently.
    RETURN_CANCEL_OR_ERROR(read_attribute_tiles(inner_names, result_tiles));

    // Copy the cells into the associated `buffers_`, and then clear the cells
    // from the tiles. The cell copies are not thread safe. Clearing tiles are
    // thread safe, but quick enough that they do not justify scheduling on
    // separate threads.
    for (const auto& inner_name : inner_names) {
      const ProcessTileFlags flags = names.at(inner_name);

      if (flags & ProcessTileFlag::SELECTIVE_UNFILTERING) {
        // Lazily compute the result cell slabs index.
        if (!rcs_index)
          rcs_index = compute_rcs_index(result_cell_slabs, subarray);

        RETURN_CANCEL_OR_ERROR(
            unfilter_tiles(inner_name, result_tiles, rcs_index.get()));
      } else {
        RETURN_CANCEL_OR_ERROR(
            unfilter_tiles(inner_name, result_tiles, nullptr));
      }

      if (flags & ProcessTileFlag::COPY) {
        if (!array_schema_->var_size(inner_name)) {
          RETURN_CANCEL_OR_ERROR(copy_fixed_cells(
              inner_name, stride, result_cell_slabs, &fixed_ctx_cache));
        } else {
          RETURN_CANCEL_OR_ERROR(copy_var_cells(
              inner_name, stride, result_cell_slabs, &var_ctx_cache));
        }
        clear_tiles(inner_name, result_tiles);
      }
    }

    idx += inner_names.size();
  }

  return Status::Ok();
}

tdb_unique_ptr<ReaderBase::ResultCellSlabsIndex> ReaderBase::compute_rcs_index(
    const std::vector<ResultCellSlab>& result_cell_slabs,
    Subarray& subarray) const {
  // Build an association from the result tile to the cell slab ranges
  // that it contains.
  tdb_unique_ptr<ReaderBase::ResultCellSlabsIndex> rcs_index =
      tdb_unique_ptr<ReaderBase::ResultCellSlabsIndex>(
          tdb_new(ResultCellSlabsIndex));

  std::vector<ResultCellSlab>::const_iterator it = result_cell_slabs.cbegin();
  while (it != result_cell_slabs.cend()) {
    std::pair<uint64_t, uint64_t> range =
        std::make_pair(it->start_, it->start_ + it->length_);
    if (rcs_index->find(it->tile_) == rcs_index->end()) {
      std::vector<std::pair<uint64_t, uint64_t>> ranges(1, std::move(range));
      rcs_index->insert(std::make_pair(it->tile_, std::move(ranges)));
    } else {
      (*rcs_index)[it->tile_].emplace_back(std::move(range));
    }
    ++it;
  }

  // TODO this need to move to a better location.
  // The result cell slab ranges must be sorted in ascending order for the
  // selective decompression intersection algorithm. For 1-dimensional arrays,
  // the result cell slab ranges are guaranteed to be sorted in ascending
  // order. For arrays with more than one dimension or multi range queries,
  // we must sort them.
  auto range_num = subarray.range_num();
  if (array_schema_->dim_num() > 1 || range_num > 1) {
    struct RangeCompare {
      inline bool operator()(
          const std::pair<uint64_t, uint64_t>& a,
          const std::pair<uint64_t, uint64_t>& b) const {
        return a.first < b.first;
      }
    };

    // Sort each range, per tile.
    for (auto& kv : *rcs_index) {
      parallel_sort(
          storage_manager_->compute_tp(),
          kv.second.begin(),
          kv.second.end(),
          RangeCompare());
    }
  }

  return rcs_index;
}

Status ReaderBase::apply_query_condition(
    std::vector<ResultCellSlab>* const result_cell_slabs,
    const std::vector<ResultTile*>& result_tiles,
    Subarray& subarray,
    uint64_t stride) {
  if (condition_.empty() || result_cell_slabs->empty())
    return Status::Ok();

  // To evaluate the query condition, we need to read tiles for the
  // attributes used in the query condition. Build a map of attribute
  // names to read.
  const std::unordered_set<std::string>& condition_names =
      condition_.field_names();
  std::unordered_map<std::string, ProcessTileFlags> names;
  for (const auto& condition_name : condition_names) {
    names[condition_name] = ProcessTileFlag::READ;
  }

  // Each element in `names` has been flagged with `ProcessTileFlag::READ`.
  // This will read the tiles, but will not copy them into the user buffers.
  process_tiles(names, result_tiles, *result_cell_slabs, subarray, stride);

  // The `UINT64_MAX` is a sentinel value to indicate that we do not
  // use a stride in the cell index calculation. To simplify our logic,
  // assign this to `1`.
  if (stride == UINT64_MAX)
    stride = 1;

  RETURN_NOT_OK(condition_.apply(array_schema_, result_cell_slabs, stride));

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
