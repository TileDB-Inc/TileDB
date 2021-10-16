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
#include "tiledb/sm/subarray/cell_slab_iter.h"
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

  auto uint64_t_max = std::numeric_limits<uint64_t>::max();
  copy_end_ = std::pair<uint64_t, uint64_t>(uint64_t_max, uint64_t_max);
}

/* ********************************* */
/*          STATIC FUNCTIONS         */
/* ********************************* */

template <class T>
void ReaderBase::compute_result_space_tiles(
    const Domain* domain,
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
      ResultTile result_tile(frag_idx, tile_idx, domain);
      result_space_tile.set_result_tile(frag_idx, result_tile);
    }
  }
}

/* ****************************** */
/*        PROTECTED METHODS       */
/* ****************************** */

void ReaderBase::clear_tiles(
    const std::string& name,
    const std::vector<ResultTile*>* result_tiles) const {
  for (auto& result_tile : *result_tiles)
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
    Subarray* subarray, const std::vector<std::string>* names) {
  auto timer_se = stats_->start_timer("load_tile_offsets");
  const auto encryption_key = array_->encryption_key();

  // Fetch relevant fragments so we load tile offsets only from intersecting
  // fragments
  const auto relevant_fragments = subarray->relevant_fragments();

  bool all_frag = !subarray->is_set();

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
        filtered_names.reserve(names->size());
        auto schema = fragment->array_schema();
        for (const auto& name : *names) {
          // Applicable for zipped coordinates only to versions < 5
          if (name == constants::coords && format_version >= 5)
            continue;

          // Applicable to separate coordinates only to versions >= 5
          const auto is_dim = schema->is_dim(name);
          if (is_dim && format_version < 5)
            continue;

          // Not a member of array schema, this field was added in array schema
          // evolution, ignore for this fragment's tile offsets
          if (!schema->is_field(name))
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
    const std::vector<std::string>* names,
    const std::vector<ResultTile*>* result_tiles) const {
  auto timer_se = stats_->start_timer("attr_tiles");
  return read_tiles(names, result_tiles);
}

Status ReaderBase::read_coordinate_tiles(
    const std::vector<std::string>* names,
    const std::vector<ResultTile*>* result_tiles) const {
  auto timer_se = stats_->start_timer("coord_tiles");
  return read_tiles(names, result_tiles);
}

Status ReaderBase::read_tiles(
    const std::vector<std::string>* names,
    const std::vector<ResultTile*>* result_tiles) const {
  // Shortcut for empty tile vec
  if (result_tiles->empty())
    return Status::Ok();

  // Reading tiles are thread safe. However, we will perform
  // them on this thread if there is only one read to perform.
  if (names->size() == 1) {
    RETURN_NOT_OK(read_tiles(names->at(0), result_tiles));
  } else {
    const auto status = parallel_for(
        storage_manager_->compute_tp(),
        0,
        names->size(),
        [&](const uint64_t i) {
          RETURN_NOT_OK(read_tiles(names->at(i), result_tiles));
          return Status::Ok();
        });

    RETURN_NOT_OK(status);
  }

  return Status::Ok();
}

Status ReaderBase::read_tiles(
    const std::string& name,
    const std::vector<ResultTile*>* result_tiles) const {
  // Shortcut for empty tile vec
  if (result_tiles->empty())
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
    const std::vector<ResultTile*>* result_tiles,
    std::vector<ThreadPool::Task>* const tasks) const {
  // Shortcut for empty tile vec
  if (result_tiles->empty())
    return Status::Ok();

  // For each tile, read from its fragment.
  const bool var_size = array_schema_->var_size(name);
  const bool nullable = array_schema_->is_nullable(name);
  const auto encryption_key = array_->encryption_key();

  // Gather the unique fragments indexes for which there are tiles
  std::unordered_set<uint32_t> fragment_idxs_set;
  for (const auto& tile : *result_tiles)
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
  for (const auto& tile : *result_tiles) {
    auto const fragment = fragment_metadata_[tile->frag_idx()];
    const uint32_t format_version = fragment->format_version();

    // Applicable for zipped coordinates only to versions < 5
    if (name == constants::coords && format_version >= 5)
      continue;

    // Applicable to separate coordinates only to versions >= 5
    const bool is_dim = array_schema_->is_dim(name);
    if (is_dim && format_version < 5)
      continue;

    // If the fragment doesn't have the attribute, this is a schema evolution
    // field and will be treated with fill-in value instead of reading from disk
    if (!fragment->array_schema()->is_field(name))
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
    const std::vector<ResultTile*>* result_tiles) const {
  auto stat_type = (array_schema_->is_attr(name)) ? "unfilter_attr_tiles" :
                                                    "unfilter_coord_tiles";
  auto timer_se = stats_->start_timer(stat_type);

  auto var_size = array_schema_->var_size(name);
  auto nullable = array_schema_->is_nullable(name);
  auto num_tiles = static_cast<uint64_t>(result_tiles->size());
  auto encryption_key = array_->encryption_key();

  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, num_tiles, [&, this](uint64_t i) {
        ResultTile* const tile = result_tiles->at(i);

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
              RETURN_NOT_OK(unfilter_tile(name, &t));
            else
              RETURN_NOT_OK(unfilter_tile_nullable(name, &t, &t_validity));
          } else {
            if (!nullable)
              RETURN_NOT_OK(unfilter_tile(name, &t, &t_var));
            else
              RETURN_NOT_OK(
                  unfilter_tile_nullable(name, &t, &t_var, &t_validity));
          }
        }

        return Status::Ok();
      });

  RETURN_CANCEL_OR_ERROR(status);

  return Status::Ok();
}

Status ReaderBase::unfilter_tile(const std::string& name, Tile* tile) const {
  FilterPipeline filters = array_schema_->filters(name);

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  // Reverse the tile filters.
  RETURN_NOT_OK(filters.run_reverse(
      stats_,
      tile,
      storage_manager_->compute_tp(),
      storage_manager_->config()));

  return Status::Ok();
}

Status ReaderBase::unfilter_tile(
    const std::string& name, Tile* tile, Tile* tile_var) const {
  FilterPipeline offset_filters = array_schema_->cell_var_offsets_filters();
  FilterPipeline filters = array_schema_->filters(name);

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &offset_filters, array_->get_encryption_key()));
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  // Reverse the tile filters.
  RETURN_NOT_OK(offset_filters.run_reverse(
      stats_, tile, storage_manager_->compute_tp(), config_));
  RETURN_NOT_OK(filters.run_reverse(
      stats_, tile_var, storage_manager_->compute_tp(), config_));

  return Status::Ok();
}

Status ReaderBase::unfilter_tile_nullable(
    const std::string& name, Tile* tile, Tile* tile_validity) const {
  FilterPipeline filters = array_schema_->filters(name);
  FilterPipeline validity_filters = array_schema_->cell_validity_filters();

  // Append an encryption unfilter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &validity_filters, array_->get_encryption_key()));

  // Reverse the tile filters.
  RETURN_NOT_OK(filters.run_reverse(
      stats_,
      tile,
      storage_manager_->compute_tp(),
      storage_manager_->config()));

  // Reverse the validity tile filters.
  RETURN_NOT_OK(validity_filters.run_reverse(
      stats_,
      tile_validity,
      storage_manager_->compute_tp(),
      storage_manager_->config()));

  return Status::Ok();
}

Status ReaderBase::unfilter_tile_nullable(
    const std::string& name,
    Tile* tile,
    Tile* tile_var,
    Tile* tile_validity) const {
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

  // Reverse the tile filters.
  RETURN_NOT_OK(offset_filters.run_reverse(
      stats_,
      tile,
      storage_manager_->compute_tp(),
      storage_manager_->config()));
  RETURN_NOT_OK(filters.run_reverse(
      stats_,
      tile_var,
      storage_manager_->compute_tp(),
      storage_manager_->config()));

  // Reverse the validity tile filters.
  RETURN_NOT_OK(validity_filters.run_reverse(
      stats_,
      tile_validity,
      storage_manager_->compute_tp(),
      storage_manager_->config()));

  return Status::Ok();
}

Status ReaderBase::copy_coordinates(
    const std::vector<ResultTile*>* result_tiles,
    std::vector<ResultCellSlab>* result_cell_slabs) {
  auto timer_se = stats_->start_timer("copy_coordinates");

  if (result_cell_slabs->empty() && result_tiles->empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  const uint64_t stride = UINT64_MAX;

  // Build a list of coordinate names to copy, separating them by
  // whether they are of fixed or variable length. The motivation
  // is that copying fixed and variable cells require two different
  // cell slab partitions. Processing them separately allows us to
  // reduce memory use.
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
    std::vector<size_t> fixed_cs_partitions;
    compute_fixed_cs_partitions(result_cell_slabs, &fixed_cs_partitions);

    for (const auto& name : fixed_names) {
      RETURN_CANCEL_OR_ERROR(copy_fixed_cells(
          name, stride, result_cell_slabs, &fixed_cs_partitions));
      if (clear_coords_tiles_on_copy_)
        clear_tiles(name, result_tiles);
    }
  }

  // Copy result cells for var-sized coordinates.
  if (!var_names.empty()) {
    std::vector<std::pair<size_t, size_t>> var_cs_partitions;
    size_t total_var_cs_length;
    compute_var_cs_partitions(
        result_cell_slabs, &var_cs_partitions, &total_var_cs_length);

    for (const auto& name : var_names) {
      RETURN_CANCEL_OR_ERROR(copy_var_cells(
          name,
          stride,
          result_cell_slabs,
          &var_cs_partitions,
          total_var_cs_length));
      if (clear_coords_tiles_on_copy_)
        clear_tiles(name, result_tiles);
    }
  }

  return Status::Ok();
}

Status ReaderBase::copy_attribute_values(
    const uint64_t stride,
    std::vector<ResultTile*>* result_tiles,
    std::vector<ResultCellSlab>* result_cell_slabs,
    Subarray& subarray,
    uint64_t memory_budget,
    bool include_dim) {
  auto timer_se = stats_->start_timer("copy_attr_values");

  if (result_cell_slabs->empty() && result_tiles->empty()) {
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

    if (!include_dim &&
        (name == constants::coords || array_schema_->is_dim(name))) {
      continue;
    }

    // If the query condition has a clause for `name`, we will only
    // flag it to copy because we have already preloaded the offsets
    // and read the tiles in `apply_query_condition`.
    ProcessTileFlags flags = ProcessTileFlag::COPY;
    if (condition_names.count(name) == 0) {
      flags |= ProcessTileFlag::READ;
    }

    names[name] = flags;
  }

  if (!names.empty()) {
    RETURN_NOT_OK(process_tiles(
        &names,
        result_tiles,
        result_cell_slabs,
        &subarray,
        stride,
        memory_budget,
        nullptr));
  }

  return Status::Ok();
}

Status ReaderBase::copy_fixed_cells(
    const std::string& name,
    uint64_t stride,
    const std::vector<ResultCellSlab>* result_cell_slabs,
    std::vector<size_t>* fixed_cs_partitions) {
  auto stat_type = (array_schema_->is_attr(name)) ? "copy_fixed_attr_values" :
                                                    "copy_fixed_coords";
  auto timer_se = stats_->start_timer(stat_type);

  if (result_cell_slabs->empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  auto it = buffers_.find(name);
  auto buffer_size = it->second.buffer_size_;
  auto cell_size = array_schema_->cell_size(name);

  // Precompute the cell range destination offsets in the buffer.
  uint64_t buffer_offset = 0;
  auto size = std::min<uint64_t>(result_cell_slabs->size(), copy_end_.first);
  std::vector<uint64_t> cs_offsets(size);
  for (uint64_t i = 0; i < cs_offsets.size(); i++) {
    const auto& cs = result_cell_slabs->at(i);

    auto cs_length = cs.length_;
    if (i == copy_end_.first - 1) {
      cs_length = copy_end_.second;
    }

    auto bytes_to_copy = cs_length * cell_size;
    cs_offsets[i] = buffer_offset;
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
      result_cell_slabs,
      &cs_offsets,
      fixed_cs_partitions);
  auto status = parallel_for(
      storage_manager_->compute_tp(),
      0,
      fixed_cs_partitions->size(),
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

void ReaderBase::compute_fixed_cs_partitions(
    const std::vector<ResultCellSlab>* result_cell_slabs,
    std::vector<size_t>* fixed_cs_partitions) {
  if (result_cell_slabs->empty()) {
    return;
  }

  const int num_copy_threads =
      storage_manager_->compute_tp()->concurrency_level();

  // Calculate the partition sizes.
  auto num_cs = std::min<uint64_t>(result_cell_slabs->size(), copy_end_.first);
  const uint64_t num_cs_partitions =
      std::min<uint64_t>(num_copy_threads, num_cs);
  const uint64_t cs_per_partition = num_cs / num_cs_partitions;
  const uint64_t cs_per_partition_carry = num_cs % num_cs_partitions;

  // Calculate the partition offsets.
  uint64_t num_cs_partitioned = 0;
  fixed_cs_partitions->reserve(num_cs_partitions);
  for (uint64_t i = 0; i < num_cs_partitions; ++i) {
    const uint64_t num_cs_in_partition =
        cs_per_partition + ((i < cs_per_partition_carry) ? 1 : 0);
    num_cs_partitioned += num_cs_in_partition;
    fixed_cs_partitions->emplace_back(num_cs_partitioned);
  }
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
    const std::vector<uint64_t>* cs_offsets,
    const std::vector<size_t>* cs_partitions) {
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
      partition_idx == 0 ? 0 : cs_partitions->at(partition_idx - 1);
  const uint64_t cs_idx_end = cs_partitions->at(partition_idx);

  // Copy the cells.
  for (uint64_t cs_idx = cs_idx_start; cs_idx < cs_idx_end; ++cs_idx) {
    // If there was an overflow and we fixed it, don't copy past the new
    // result cell slabs.
    if (cs_idx >= copy_end_.first) {
      break;
    }

    const auto& cs = (*result_cell_slabs)[cs_idx];
    uint64_t offset = cs_offsets->at(cs_idx);

    auto cs_length = cs.length_;
    if (cs_idx == copy_end_.first - 1) {
      cs_length = copy_end_.second;
    }

    // Copy

    // First we check if this is an older (pre TileDB 2.0) array with zipped
    // coordinates and the user has requested split buffer if so we should
    // proceed to copying the tile If not, and there is no tile or the tile is
    // empty for the field then this is a read of an older fragment in schema
    // evolution. In that case we want to set the field to fill values for this
    // for this tile.
    const bool split_buffer_for_zipped_coords =
        array_schema_->is_dim(*name) && cs.tile_->stores_zipped_coords();
    if ((cs.tile_ == nullptr || cs.tile_->tile_tuple(*name) == nullptr) &&
        !split_buffer_for_zipped_coords) {  // Empty range or attributed added
                                            // in schema evolution
      auto bytes_to_copy = cs_length * cell_size;
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
              cs.tile_->read(*name, buffer, offset, cs.start_, cs_length));
        else
          RETURN_NOT_OK(cs.tile_->read_nullable(
              *name, buffer, offset, cs.start_, cs_length, buffer_validity));
      } else {
        auto cell_offset = offset;
        auto start = cs.start_;
        for (uint64_t j = 0; j < cs_length; ++j) {
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
    std::vector<ResultCellSlab>* result_cell_slabs,
    std::vector<std::pair<size_t, size_t>>* var_cs_partitions,
    size_t total_cs_length) {
  auto stat_type = (array_schema_->is_attr(name)) ? "copy_var_attr_values" :
                                                    "copy_var_coords";
  auto timer_se = stats_->start_timer(stat_type);

  if (result_cell_slabs->empty()) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  std::vector<uint64_t> offset_offsets_per_cs(total_cs_length);
  std::vector<uint64_t> var_offsets_per_cs(total_cs_length);

  // Compute the destinations of offsets and var-len data in the buffers.
  uint64_t total_offset_size, total_var_size, total_validity_size;
  RETURN_NOT_OK(compute_var_cell_destinations(
      name,
      stride,
      result_cell_slabs,
      &offset_offsets_per_cs,
      &var_offsets_per_cs,
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
      result_cell_slabs,
      &offset_offsets_per_cs,
      &var_offsets_per_cs,
      var_cs_partitions);
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, var_cs_partitions->size(), copy_fn);

  RETURN_NOT_OK(status);

  // Update buffer offsets
  *(buffers_[name].buffer_size_) = total_offset_size;
  *(buffers_[name].buffer_var_size_) = total_var_size;
  if (array_schema_->is_nullable(name))
    *(buffers_[name].validity_vector_.buffer_size()) = total_validity_size;

  return Status::Ok();
}

void ReaderBase::compute_var_cs_partitions(
    const std::vector<ResultCellSlab>* result_cell_slabs,
    std::vector<std::pair<size_t, size_t>>* var_cs_partitions,
    size_t* total_var_cs_length) {
  if (result_cell_slabs->empty()) {
    return;
  }

  const int num_copy_threads =
      storage_manager_->compute_tp()->concurrency_level();

  // Calculate the partition range.
  const uint64_t num_cs =
      std::min<uint64_t>(result_cell_slabs->size(), copy_end_.first);
  const uint64_t num_cs_partitions =
      std::min<uint64_t>(num_copy_threads, num_cs);
  const uint64_t cs_per_partition = num_cs / num_cs_partitions;
  const uint64_t cs_per_partition_carry = num_cs % num_cs_partitions;

  // Compute the boundary between each partition. Each boundary
  // is represented by an `std::pair` that contains the total
  // length of each cell slab in the leading partition and an
  // exclusive cell slab index that ends the partition.
  uint64_t next_partition_idx = cs_per_partition;
  if (cs_per_partition_carry > 0)
    ++next_partition_idx;

  *total_var_cs_length = 0;
  var_cs_partitions->reserve(num_cs_partitions);
  for (uint64_t cs_idx = 0; cs_idx < num_cs; cs_idx++) {
    if (cs_idx == next_partition_idx) {
      var_cs_partitions->emplace_back(*total_var_cs_length, cs_idx);

      // The final partition may contain extra cell slabs that did
      // not evenly divide into the partition range. Set the
      // `next_partition_idx` to zero and build the last boundary
      // after this for-loop.
      if (var_cs_partitions->size() == num_cs_partitions) {
        next_partition_idx = 0;
      } else {
        next_partition_idx += cs_per_partition;
        if (cs_idx < (cs_per_partition_carry - 1))
          ++next_partition_idx;
      }
    }

    auto cs_length = result_cell_slabs->at(cs_idx).length_;
    if (cs_idx == copy_end_.first - 1) {
      cs_length = copy_end_.second;
    }
    *total_var_cs_length += cs_length;
  }

  // Store the final boundary.
  var_cs_partitions->emplace_back(*total_var_cs_length, num_cs);
}

Status ReaderBase::compute_var_cell_destinations(
    const std::string& name,
    uint64_t stride,
    std::vector<ResultCellSlab>* result_cell_slabs,
    std::vector<uint64_t>* offset_offsets_per_cs,
    std::vector<uint64_t>* var_offsets_per_cs,
    uint64_t* total_offset_size,
    uint64_t* total_var_size,
    uint64_t* total_validity_size) {
  // For easy reference
  auto nullable = array_schema_->is_nullable(name);
  auto num_cs = std::min<uint64_t>(result_cell_slabs->size(), copy_end_.first);
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
    const auto& cs = result_cell_slabs->at(cs_idx);

    auto cs_length = cs.length_;
    if (cs_idx == copy_end_.first - 1) {
      cs_length = copy_end_.second;
    }

    // Get tile information, if the range is nonempty.
    uint64_t* tile_offsets = nullptr;
    uint64_t tile_cell_num = 0;
    uint64_t tile_var_size = 0;
    if (cs.tile_ != nullptr && cs.tile_->tile_tuple(name) != nullptr) {
      const auto tile_tuple = cs.tile_->tile_tuple(name);
      const auto& tile = std::get<0>(*tile_tuple);
      const auto& tile_var = std::get<1>(*tile_tuple);

      // Get the internal buffer to the offset values.
      Buffer* const buffer = tile.buffer();

      tile_offsets = (uint64_t*)buffer->data();
      tile_cell_num = tile.cell_num();
      tile_var_size = tile_var.size();
    }

    // Compute the destinations for each cell in the range.
    uint64_t dest_vec_idx = 0;
    stride = (stride == UINT64_MAX) ? 1 : stride;

    for (auto cell_idx = cs.start_; dest_vec_idx < cs_length;
         cell_idx += stride, dest_vec_idx++) {
      // Get size of variable-sized cell
      uint64_t cell_var_size = 0;
      if (cs.tile_ == nullptr || cs.tile_->tile_tuple(name) == nullptr) {
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
        // Try to fix the overflow by reducing the result cell slabs to copy.
        if (fix_var_sized_overflows_) {
          copy_end_ = std::pair<uint64_t, uint64_t>(cs_idx + 1, cell_idx);

          // Cannot even copy one cell, return overflow.
          if (cs_idx == 0 && cell_idx == result_cell_slabs->front().start_) {
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

    total_cs_length += cs_length;
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
    if (cs_idx >= copy_end_.first) {
      break;
    }
    const auto& cs = (*result_cell_slabs)[cs_idx];

    auto cs_length = cs.length_;
    if (cs_idx == copy_end_.first - 1) {
      cs_length = copy_end_.second;
    }

    // Get tile information, if the range is nonempty.
    uint64_t* tile_offsets = nullptr;
    Tile* tile_var = nullptr;
    Tile* tile_validity = nullptr;
    uint64_t tile_cell_num = 0;
    if (cs.tile_ != nullptr && cs.tile_->tile_tuple(*name) != nullptr) {
      const auto tile_tuple = cs.tile_->tile_tuple(*name);
      Tile* const tile = &std::get<0>(*tile_tuple);
      tile_var = &std::get<1>(*tile_tuple);
      tile_validity = &std::get<2>(*tile_tuple);

      // Get the internal buffer to the offset values.
      Buffer* const buffer = tile->buffer();

      tile_offsets = (uint64_t*)buffer->data();
      tile_cell_num = tile->cell_num();
    }

    // Copy each cell in the range
    uint64_t dest_vec_idx = 0;
    stride = (stride == UINT64_MAX) ? 1 : stride;
    for (auto cell_idx = cs.start_; dest_vec_idx < cs_length;
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
      if (cs.tile_ == nullptr || cs.tile_->tile_tuple(*name) == nullptr) {
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

    arr_offset += cs_length;
  }

  return Status::Ok();
}

Status ReaderBase::process_tiles(
    const std::unordered_map<std::string, ProcessTileFlags>* names,
    std::vector<ResultTile*>* result_tiles,
    std::vector<ResultCellSlab>* result_cell_slabs,
    Subarray* subarray,
    const uint64_t stride,
    uint64_t memory_budget,
    uint64_t* memory_used_for_tiles) {
  // If a name needs to be read, we put it on `read_names` vector (it may
  // contain other flags). Otherwise, we put the name on the `copy_names`
  // vector if it needs to be copied back to the user buffer.
  // We can benefit from concurrent reads by processing `read_names`
  // separately from `copy_names`.
  std::vector<std::string> read_names;
  std::vector<std::string> copy_names;
  bool is_apply_query_condition = true;
  read_names.reserve(names->size());
  for (const auto& name_pair : *names) {
    const std::string name = name_pair.first;
    const ProcessTileFlags flags = name_pair.second;
    if (flags & ProcessTileFlag::READ) {
      if (flags & ProcessTileFlag::COPY)
        is_apply_query_condition = false;
      read_names.push_back(name);
    } else if (flags & ProcessTileFlag::COPY) {
      copy_names.push_back(name);
      is_apply_query_condition = false;
    }
  }

  // Pre-load all attribute offsets into memory for attributes
  // to be read.
  load_tile_offsets(subarray, &read_names);

  // Respect the memory budget if it is set.
  if (memory_budget != std::numeric_limits<uint64_t>::max()) {
    // For query condition, make sure all tiles fit in memory budget.
    if (is_apply_query_condition) {
      for (const auto& name_pair : *names) {
        const std::string name = name_pair.first;
        assert(name_pair.second == ProcessTileFlag::READ);

        for (const auto& rt : *result_tiles) {
          uint64_t tile_size = 0;
          RETURN_NOT_OK(get_attribute_tile_size(name, rt, &tile_size));
          *memory_used_for_tiles += tile_size;
        }
      }

      if (*memory_used_for_tiles > memory_budget) {
        return Status::ReaderError(
            "Exceeded tile memory budget applying query condition");
      }
    } else {
      // Make sure that for each attribute, all tiles can fit in the budget.
      uint64_t new_result_tile_size = result_tiles->size();
      for (const auto& name_pair : *names) {
        uint64_t mem_usage = 0;
        for (uint64_t i = 0;
             i < result_tiles->size() && i < new_result_tile_size;
             i++) {
          const auto& rt = result_tiles->at(i);
          uint64_t tile_size = 0;
          RETURN_NOT_OK(
              get_attribute_tile_size(name_pair.first, rt, &tile_size));
          if (mem_usage + tile_size > memory_budget) {
            new_result_tile_size = i;
            break;
          }
          mem_usage += tile_size;
        }
      }

      if (new_result_tile_size != result_tiles->size()) {
        // Erase tiles from result tiles.
        result_tiles->erase(
            result_tiles->begin() + new_result_tile_size - 1,
            result_tiles->end());

        // Find the result cell slab index to end the copy opeation.
        auto& last_tile = result_tiles->back();
        uint64_t last_idx = 0;
        for (uint64_t i = 0; i < result_cell_slabs->size(); i++) {
          if (result_cell_slabs->at(i).tile_ == last_tile) {
            if (i == 0) {
              return Status::ReaderError(
                  "Unable to copy one tile with current budget");
            }
            last_idx = i;
          }
        }

        // Adjust copy_end_
        if (copy_end_.first > last_idx + 1) {
          copy_end_.first = last_idx + 1;
          copy_end_.second = result_cell_slabs->at(last_idx).length_;
        }
      }
    }
  }

  // Get the maximum number of attributes to read and unfilter in parallel.
  // Each attribute requires additional memory to buffer reads into
  // before copying them back into `buffers_`. Cells must be copied
  // before moving onto the next set of concurrent reads to prevent
  // bloating memory. Additionally, the copy cells paths are performed
  // in serial, which will bottleneck the read concurrency. Increasing
  // this number will have diminishing returns on performance.
  const uint64_t concurrent_reads = constants::concurrent_attr_reads;

  // Instantiate partitions for copying fixed and variable cells.
  std::vector<size_t> fixed_cs_partitions;
  compute_fixed_cs_partitions(result_cell_slabs, &fixed_cs_partitions);

  std::vector<std::pair<size_t, size_t>> var_cs_partitions;
  size_t total_var_cs_length;
  compute_var_cs_partitions(
      result_cell_slabs, &var_cs_partitions, &total_var_cs_length);

  // Handle attribute/dimensions that need to be copied but do
  // not need to be read.
  for (const auto& copy_name : copy_names) {
    if (!array_schema_->var_size(copy_name))
      RETURN_CANCEL_OR_ERROR(copy_fixed_cells(
          copy_name, stride, result_cell_slabs, &fixed_cs_partitions));
    else
      RETURN_CANCEL_OR_ERROR(copy_var_cells(
          copy_name,
          stride,
          result_cell_slabs,
          &var_cs_partitions,
          total_var_cs_length));

    // Copy only here should be attributes in the query condition. They should
    // be treated as coordinates.
    if (clear_coords_tiles_on_copy_)
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
    RETURN_CANCEL_OR_ERROR(read_attribute_tiles(&inner_names, result_tiles));

    // Copy the cells into the associated `buffers_`, and then clear the cells
    // from the tiles. The cell copies are not thread safe. Clearing tiles are
    // thread safe, but quick enough that they do not justify scheduling on
    // separate threads.
    for (const auto& inner_name : inner_names) {
      const ProcessTileFlags flags = names->at(inner_name);

      RETURN_CANCEL_OR_ERROR(unfilter_tiles(inner_name, result_tiles));

      if (flags & ProcessTileFlag::COPY) {
        if (!array_schema_->var_size(inner_name)) {
          RETURN_CANCEL_OR_ERROR(copy_fixed_cells(
              inner_name, stride, result_cell_slabs, &fixed_cs_partitions));
        } else {
          RETURN_CANCEL_OR_ERROR(copy_var_cells(
              inner_name,
              stride,
              result_cell_slabs,
              &var_cs_partitions,
              total_var_cs_length));
        }
        clear_tiles(inner_name, result_tiles);
      }
    }

    idx += inner_names.size();
  }

  return Status::Ok();
}

tdb_unique_ptr<ReaderBase::ResultCellSlabsIndex> ReaderBase::compute_rcs_index(
    const std::vector<ResultCellSlab>* result_cell_slabs) const {
  // Build an association from the result tile to the cell slab ranges
  // that it contains.
  tdb_unique_ptr<ReaderBase::ResultCellSlabsIndex> rcs_index =
      tdb_unique_ptr<ReaderBase::ResultCellSlabsIndex>(
          tdb_new(ResultCellSlabsIndex));

  std::vector<ResultCellSlab>::const_iterator it = result_cell_slabs->cbegin();
  while (it != result_cell_slabs->cend()) {
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

  return rcs_index;
}

Status ReaderBase::apply_query_condition(
    std::vector<ResultCellSlab>* const result_cell_slabs,
    std::vector<ResultTile*>* result_tiles,
    Subarray* subarray,
    uint64_t stride,
    uint64_t memory_budget_rcs,
    uint64_t memory_budget_tiles,
    uint64_t* memory_used_for_tiles) {
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
  process_tiles(
      &names,
      result_tiles,
      result_cell_slabs,
      subarray,
      stride,
      memory_budget_tiles,
      memory_used_for_tiles);

  // The `UINT64_MAX` is a sentinel value to indicate that we do not
  // use a stride in the cell index calculation. To simplify our logic,
  // assign this to `1`.
  if (stride == UINT64_MAX)
    stride = 1;

  RETURN_NOT_OK(condition_.apply(
      array_schema_, result_cell_slabs, stride, memory_budget_rcs));

  return Status::Ok();
}

Status ReaderBase::get_attribute_tile_size(
    const std::string& name, ResultTile* result_tile, uint64_t* tile_size) {
  *tile_size = 0;
  auto f = result_tile->frag_idx();
  auto t = result_tile->tile_idx();
  *tile_size += fragment_metadata_[f]->tile_size(name, t);

  if (array_schema_->var_size(name)) {
    uint64_t temp = 0;
    RETURN_NOT_OK(fragment_metadata_[f]->tile_var_size(
        *array_->encryption_key(), name, t, &temp));
    *tile_size += temp;
  }

  if (array_schema_->is_nullable(name)) {
    *tile_size += result_tile->cell_num() * constants::cell_validity_size;
  }

  return Status::Ok();
}

template <class T>
void ReaderBase::compute_result_space_tiles(
    const Subarray& subarray,
    std::map<const T*, ResultSpaceTile<T>>* result_space_tiles) const {
  // For easy reference
  auto domain = array_schema_->domain()->domain();
  auto tile_extents = array_schema_->domain()->tile_extents();
  auto tile_order = array_schema_->tile_order();

  // Compute fragment tile domains
  std::vector<TileDomain<T>> frag_tile_domains;
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

  // Get tile coords and array domain
  const auto& tile_coords = subarray.tile_coords();
  TileDomain<T> array_tile_domain(
      UINT32_MAX, domain, domain, tile_extents, tile_order);

  // Compute result space tiles
  compute_result_space_tiles<T>(
      array_schema_->domain(),
      tile_coords,
      array_tile_domain,
      frag_tile_domains,
      result_space_tiles);
}

bool ReaderBase::has_coords() const {
  for (const auto& it : buffers_) {
    if (it.first == constants::coords || array_schema_->is_dim(it.first))
      return true;
  }

  return false;
}

template <class T>
Status ReaderBase::fill_dense_coords(const Subarray& subarray) {
  auto timer_se = stats_->start_timer("fill_dense_coords");

  // Reading coordinates with a query condition is currently unsupported.
  // Query conditions mutate the result cell slabs to filter attributes.
  // This path does not use result cell slabs, which will fill coordinates
  // for cells that should be filtered out.
  if (!condition_.empty()) {
    return LOG_STATUS(
        Status::ReaderError("Cannot read dense coordinates; dense coordinate "
                            "reads are unsupported with a query condition"));
  }

  // Prepare buffers
  std::vector<unsigned> dim_idx;
  std::vector<QueryBuffer*> buffers;
  auto coords_it = buffers_.find(constants::coords);
  auto dim_num = array_schema_->dim_num();
  if (coords_it != buffers_.end()) {
    buffers.emplace_back(&(coords_it->second));
    dim_idx.emplace_back(dim_num);
  } else {
    for (unsigned d = 0; d < dim_num; ++d) {
      const auto& dim = array_schema_->dimension(d);
      auto it = buffers_.find(dim->name());
      if (it != buffers_.end()) {
        buffers.emplace_back(&(it->second));
        dim_idx.emplace_back(d);
      }
    }
  }
  std::vector<uint64_t> offsets(buffers.size(), 0);

  if (layout_ == Layout::GLOBAL_ORDER) {
    RETURN_NOT_OK(
        fill_dense_coords_global<T>(subarray, dim_idx, buffers, &offsets));
  } else {
    assert(layout_ == Layout::ROW_MAJOR || layout_ == Layout::COL_MAJOR);
    RETURN_NOT_OK(
        fill_dense_coords_row_col<T>(subarray, dim_idx, buffers, &offsets));
  }

  // Update buffer sizes
  for (size_t i = 0; i < buffers.size(); ++i)
    *(buffers[i]->buffer_size_) = offsets[i];

  return Status::Ok();
}

template <class T>
Status ReaderBase::fill_dense_coords_global(
    const Subarray& subarray,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>* offsets) {
  auto tile_coords = subarray.tile_coords();
  auto cell_order = array_schema_->cell_order();

  for (const auto& tc : tile_coords) {
    auto tile_subarray = subarray.crop_to_tile((const T*)&tc[0], cell_order);
    RETURN_NOT_OK(
        fill_dense_coords_row_col<T>(tile_subarray, dim_idx, buffers, offsets));
  }

  return Status::Ok();
}

template <class T>
Status ReaderBase::fill_dense_coords_row_col(
    const Subarray& subarray,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>* offsets) {
  auto cell_order = array_schema_->cell_order();
  auto dim_num = array_schema_->dim_num();

  // Iterate over all coordinates, retrieved in cell slabs
  CellSlabIter<T> iter(&subarray);
  RETURN_CANCEL_OR_ERROR(iter.begin());
  while (!iter.end()) {
    auto cell_slab = iter.cell_slab();
    auto coords_num = cell_slab.length_;

    // Check for overflow
    for (size_t i = 0; i < buffers.size(); ++i) {
      auto idx = (dim_idx[i] == dim_num) ? 0 : dim_idx[i];
      auto dim = array_schema_->domain()->dimension(idx);
      auto coord_size = dim->coord_size();
      coord_size = (dim_idx[i] == dim_num) ? coord_size * dim_num : coord_size;
      auto buff_size = *(buffers[i]->buffer_size_);
      auto offset = (*offsets)[i];
      if (coords_num * coord_size + offset > buff_size) {
        copy_overflowed_ = true;
        return Status::Ok();
      }
    }

    // Copy slab
    if (layout_ == Layout::ROW_MAJOR ||
        (layout_ == Layout::GLOBAL_ORDER && cell_order == Layout::ROW_MAJOR))
      fill_dense_coords_row_slab(
          &cell_slab.coords_[0], coords_num, dim_idx, buffers, offsets);
    else
      fill_dense_coords_col_slab(
          &cell_slab.coords_[0], coords_num, dim_idx, buffers, offsets);

    ++iter;
  }

  return Status::Ok();
}

template <class T>
void ReaderBase::fill_dense_coords_row_slab(
    const T* start,
    uint64_t num,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>* offsets) const {
  // For easy reference
  auto dim_num = array_schema_->dim_num();

  // Special zipped coordinates
  if (dim_idx.size() == 1 && dim_idx[0] == dim_num) {
    auto c_buff = (char*)buffers[0]->buffer_;
    auto offset = &(*offsets)[0];

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
  } else {  // Set of separate coordinate buffers
    for (uint64_t i = 0; i < num; ++i) {
      for (size_t b = 0; b < buffers.size(); ++b) {
        auto c_buff = (char*)buffers[b]->buffer_;
        auto offset = &(*offsets)[b];

        // First dim-1 dimensions are copied as they are
        if (dim_num > 1 && dim_idx[b] < dim_num - 1) {
          std::memcpy(c_buff + *offset, &start[dim_idx[b]], sizeof(T));
          *offset += sizeof(T);
        } else {
          // Last dimension is incremented by `i`
          auto new_coord = start[dim_num - 1] + i;
          std::memcpy(c_buff + *offset, &new_coord, sizeof(T));
          *offset += sizeof(T);
        }
      }
    }
  }
}

template <class T>
void ReaderBase::fill_dense_coords_col_slab(
    const T* start,
    uint64_t num,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>* offsets) const {
  // For easy reference
  auto dim_num = array_schema_->dim_num();

  // Special zipped coordinates
  if (dim_idx.size() == 1 && dim_idx[0] == dim_num) {
    auto c_buff = (char*)buffers[0]->buffer_;
    auto offset = &(*offsets)[0];

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
  } else {  // Separate coordinate buffers
    for (uint64_t i = 0; i < num; ++i) {
      for (size_t b = 0; b < buffers.size(); ++b) {
        auto c_buff = (char*)buffers[b]->buffer_;
        auto offset = &(*offsets)[b];

        // First dimension is incremented by `i`
        if (dim_idx[b] == 0) {
          auto new_coord = start[0] + i;
          std::memcpy(c_buff + *offset, &new_coord, sizeof(T));
          *offset += sizeof(T);
        } else {  // Last dim-1 dimensions are copied as they are
          std::memcpy(c_buff + *offset, &start[dim_idx[b]], sizeof(T));
          *offset += sizeof(T);
        }
      }
    }
  }
}

// Explicit template instantiations
template void ReaderBase::compute_result_space_tiles<int8_t>(
    const Subarray& subarray,
    std::map<const int8_t*, ResultSpaceTile<int8_t>>* result_space_tiles) const;
template void ReaderBase::compute_result_space_tiles<uint8_t>(
    const Subarray& subarray,
    std::map<const uint8_t*, ResultSpaceTile<uint8_t>>* result_space_tiles)
    const;
template void ReaderBase::compute_result_space_tiles<int16_t>(
    const Subarray& subarray,
    std::map<const int16_t*, ResultSpaceTile<int16_t>>* result_space_tiles)
    const;
template void ReaderBase::compute_result_space_tiles<uint16_t>(
    const Subarray& subarray,
    std::map<const uint16_t*, ResultSpaceTile<uint16_t>>* result_space_tiles)
    const;
template void ReaderBase::compute_result_space_tiles<int32_t>(
    const Subarray& subarray,
    std::map<const int32_t*, ResultSpaceTile<int32_t>>* result_space_tiles)
    const;
template void ReaderBase::compute_result_space_tiles<uint32_t>(
    const Subarray& subarray,
    std::map<const uint32_t*, ResultSpaceTile<uint32_t>>* result_space_tiles)
    const;
template void ReaderBase::compute_result_space_tiles<int64_t>(
    const Subarray& subarray,
    std::map<const int64_t*, ResultSpaceTile<int64_t>>* result_space_tiles)
    const;
template void ReaderBase::compute_result_space_tiles<uint64_t>(
    const Subarray& subarray,
    std::map<const uint64_t*, ResultSpaceTile<uint64_t>>* result_space_tiles)
    const;

template Status ReaderBase::fill_dense_coords<int8_t>(const Subarray&);
template Status ReaderBase::fill_dense_coords<uint8_t>(const Subarray&);
template Status ReaderBase::fill_dense_coords<int16_t>(const Subarray&);
template Status ReaderBase::fill_dense_coords<uint16_t>(const Subarray&);
template Status ReaderBase::fill_dense_coords<int32_t>(const Subarray&);
template Status ReaderBase::fill_dense_coords<uint32_t>(const Subarray&);
template Status ReaderBase::fill_dense_coords<int64_t>(const Subarray&);
template Status ReaderBase::fill_dense_coords<uint64_t>(const Subarray&);

}  // namespace sm
}  // namespace tiledb
