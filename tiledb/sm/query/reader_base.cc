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
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filter_type.h"
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
    tdb_shared_ptr<Logger> logger,
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
    , condition_(condition) {
  if (array != nullptr)
    fragment_metadata_ = array->fragment_metadata();
}

/* ********************************* */
/*          STATIC FUNCTIONS         */
/* ********************************* */

template <class T>
void ReaderBase::compute_result_space_tiles(
    const std::vector<tdb_shared_ptr<FragmentMetadata>>& fragment_metadata,
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
          frag_idx, tile_idx, fragment_metadata[frag_idx]->array_schema());
      result_space_tile.set_result_tile(frag_idx, result_tile);
    }
  }
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
    return logger_->status(Status_ReaderError(
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
        return logger_->status(Status_ReaderError(ss.str()));
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
      all_frag ? fragment_metadata_.size() : relevant_fragments->size(),
      [&](const uint64_t i) {
        auto frag_idx = all_frag ? i : relevant_fragments->at(i);
        auto& fragment = fragment_metadata_[frag_idx];
        const auto format_version = fragment->format_version();

        // Filter the 'names' for format-specific names.
        std::vector<std::string> filtered_names;
        filtered_names.reserve(names.size());
        auto schema = fragment->array_schema();
        for (const auto& name : names) {
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

        auto schema = fragment->array_schema();
        for (const auto& name : names) {
          // Not a member of array schema, this field was added in array schema
          // evolution, ignore for this fragment's tile var sizes.
          if (!schema->is_field(name))
            continue;

          // Not a var size attribute.
          if (!schema->var_size(name))
            continue;

          fragment->load_tile_var_sizes(*encryption_key, name);
        }

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
    const std::vector<ResultTile*>& result_tiles,
    const bool disable_cache) const {
  auto timer_se = stats_->start_timer("read_attribute_tiles");
  return read_tiles(names, result_tiles, disable_cache);
}

Status ReaderBase::read_coordinate_tiles(
    const std::vector<std::string>& names,
    const std::vector<ResultTile*>& result_tiles,
    const bool disable_cache) const {
  auto timer_se = stats_->start_timer("read_coordinate_tiles");
  return read_tiles(names, result_tiles, disable_cache);
}

Status ReaderBase::read_tiles(
    const std::vector<std::string>& names,
    const std::vector<ResultTile*>& result_tiles,
    const bool disable_cache) const {
  auto timer_se = stats_->start_timer("read_tiles");
  auto enc_type = array_->get_encryption_key().encryption_type();

  // Shortcut for empty tile vec
  if (result_tiles.empty())
    return Status::Ok();

  // Populate the list of regions per file to be read.
  std::unordered_map<
      URI,
      std::vector<std::tuple<uint64_t, Tile*, uint64_t>>,
      URIHasher>
      all_regions;

  // Run all tiles and attributes.
  for (auto name : names) {
    for (auto tile : result_tiles) {
      // For each tile, read from its fragment.
      auto const fragment = fragment_metadata_[tile->frag_idx()];

      const uint32_t format_version = fragment->format_version();

      // Applicable for zipped coordinates only to versions < 5
      if (name == constants::coords && format_version >= 5)
        continue;

      // Applicable to separate coordinates only to versions >= 5
      const bool is_dim = fragment->array_schema()->is_dim(name);
      if (is_dim && format_version < 5)
        continue;

      // If the fragment doesn't have the attribute, this is a schema
      // evolution field and will be treated with fill-in value instead of
      // reading from disk
      if (!fragment->array_schema()->is_field(name))
        continue;

      const bool var_size = fragment->array_schema()->var_size(name);
      const bool nullable = fragment->array_schema()->is_nullable(name);

      // Initialize the tile(s)
      ResultTile::TileTuple* tile_tuple = nullptr;
      if (is_dim) {
        const uint64_t dim_num = fragment->array_schema()->dim_num();
        for (uint64_t d = 0; d < dim_num; ++d) {
          if (fragment->array_schema()->dimension(d)->name() == name) {
            tile->init_coord_tile(name, d);
            break;
          }
        }
        tile_tuple = tile->tile_tuple(name);
      } else {
        tile->init_attr_tile(name);
        tile_tuple = tile->tile_tuple(name);
      }

      assert(tile_tuple != nullptr);
      Tile* const t = &std::get<0>(*tile_tuple);
      Tile* const t_var = &std::get<1>(*tile_tuple);
      Tile* const t_validity = &std::get<2>(*tile_tuple);
      if (!var_size) {
        if (nullable)
          RETURN_NOT_OK(
              init_tile_nullable(format_version, name, t, t_validity));
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
      RETURN_NOT_OK(fragment->file_offset(name, tile_idx, &tile_attr_offset));
      uint64_t tile_persisted_size;
      RETURN_NOT_OK(
          fragment->persisted_tile_size(name, tile_idx, &tile_persisted_size));
      uint64_t tile_size = fragment->tile_size(name, tile_idx);

      // Try the cache first.
      bool cache_hit = false;
      if (!disable_cache) {
        RETURN_NOT_OK(storage_manager_->read_from_cache(
            tile_attr_uri,
            tile_attr_offset,
            t->filtered_buffer(),
            tile_persisted_size,
            &cache_hit));
      }

      if (!cache_hit) {
        // Add the region of the fragment to be read.
        all_regions[tile_attr_uri].emplace_back(
            tile_attr_offset, t, tile_persisted_size);

        RETURN_NOT_OK(t->filtered_buffer()->realloc(tile_persisted_size));
        t->filtered_buffer()->set_size(tile_persisted_size);
        t->filtered_buffer()->reset_offset();

        // Pre-allocate the unfiltered buffer unless we are using the no-op
        // filter with no encryption as the no-op filter will reuse the
        // filtered buffer.
        const auto& filters =
            var_size ? fragment->array_schema()->cell_var_offsets_filters() :
                       fragment->array_schema()->filters(name);
        if (filters.size() != 1 ||
            filters.get_filter(0)->type() != FilterType::FILTER_NONE ||
            enc_type != EncryptionType::NO_ENCRYPTION) {
          RETURN_NOT_OK(t->buffer()->realloc(tile_size));
        }
      }

      if (var_size) {
        auto tile_attr_var_uri = fragment->var_uri(name);
        uint64_t tile_attr_var_offset;
        RETURN_NOT_OK(
            fragment->file_var_offset(name, tile_idx, &tile_attr_var_offset));
        uint64_t tile_var_persisted_size;
        RETURN_NOT_OK(fragment->persisted_tile_var_size(
            name, tile_idx, &tile_var_persisted_size));
        uint64_t tile_var_size;
        RETURN_NOT_OK(fragment->tile_var_size(name, tile_idx, &tile_var_size));

        if (!disable_cache) {
          RETURN_NOT_OK(storage_manager_->read_from_cache(
              tile_attr_var_uri,
              tile_attr_var_offset,
              t_var->filtered_buffer(),
              tile_var_persisted_size,
              &cache_hit));
        }

        if (!cache_hit) {
          // Add the region of the fragment to be read.
          all_regions[tile_attr_var_uri].emplace_back(
              tile_attr_var_offset, t_var, tile_var_persisted_size);

          RETURN_NOT_OK(
              t_var->filtered_buffer()->realloc(tile_var_persisted_size));
          t_var->filtered_buffer()->set_size(tile_var_persisted_size);
          t_var->filtered_buffer()->reset_offset();

          // Pre-allocate the unfiltered buffer unless we are using the no-op
          // filter with no encryption as the no-op filter will reuse the
          // filtered buffer.
          const auto& filters = fragment->array_schema()->filters(name);
          if (filters.size() != 1 ||
              filters.get_filter(0)->type() != FilterType::FILTER_NONE ||
              enc_type != EncryptionType::NO_ENCRYPTION) {
            RETURN_NOT_OK(t_var->buffer()->realloc(tile_var_size));
          }
        }
      }

      if (nullable) {
        auto tile_validity_attr_uri = fragment->validity_uri(name);
        uint64_t tile_attr_validity_offset;
        RETURN_NOT_OK(fragment->file_validity_offset(
            name, tile_idx, &tile_attr_validity_offset));
        uint64_t tile_validity_persisted_size;
        RETURN_NOT_OK(fragment->persisted_tile_validity_size(
            name, tile_idx, &tile_validity_persisted_size));
        uint64_t tile_validity_size =
            fragment->cell_num(tile_idx) * constants::cell_validity_size;

        if (!disable_cache) {
          RETURN_NOT_OK(storage_manager_->read_from_cache(
              tile_validity_attr_uri,
              tile_attr_validity_offset,
              t_validity->filtered_buffer(),
              tile_validity_persisted_size,
              &cache_hit));
        }

        if (!cache_hit) {
          // Add the region of the fragment to be read.
          all_regions[tile_validity_attr_uri].emplace_back(
              tile_attr_validity_offset,
              t_validity,
              tile_validity_persisted_size);

          RETURN_NOT_OK(t_validity->filtered_buffer()->realloc(
              tile_validity_persisted_size));
          t_validity->filtered_buffer()->set_size(tile_validity_persisted_size);
          t_validity->filtered_buffer()->reset_offset();

          // Pre-allocate the unfiltered buffer unless we are using the no-op
          // filter with no encryption as the no-op filter will reuse the
          // filtered buffer.
          const auto& filters =
              fragment->array_schema()->cell_validity_filters();
          if (filters.size() != 1 ||
              filters.get_filter(0)->type() != FilterType::FILTER_NONE ||
              enc_type != EncryptionType::NO_ENCRYPTION) {
            RETURN_NOT_OK(t_validity->buffer()->realloc(tile_validity_size));
          }
        }
      }
    }
  }

  // Do not use the read-ahead cache because tiles will be
  // cached in the tile cache.
  const bool use_read_ahead = false;

  // Read the tiles asynchronously
  std::vector<ThreadPool::Task> tasks;

  // Enqueue all regions to be read.
  for (const auto& item : all_regions) {
    RETURN_NOT_OK(storage_manager_->vfs()->read_all(
        item.first,
        item.second,
        storage_manager_->io_tp(),
        &tasks,
        use_read_ahead));
  }

  // Wait for the reads to finish and check statuses.
  auto statuses = storage_manager_->io_tp()->wait_all_status(tasks);
  for (const auto& st : statuses)
    RETURN_CANCEL_OR_ERROR(st);

  return Status::Ok();
}

Status ReaderBase::unfilter_tiles(
    const std::string& name,
    const std::vector<ResultTile*>& result_tiles,
    const bool disable_cache) const {
  auto stat_type = (array_schema_->is_attr(name)) ? "unfilter_attr_tiles" :
                                                    "unfilter_coord_tiles";
  auto timer_se = stats_->start_timer(stat_type);

  auto var_size = array_schema_->var_size(name);
  auto nullable = array_schema_->is_nullable(name);
  auto num_tiles = static_cast<uint64_t>(result_tiles.size());

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

          auto& t = std::get<0>(*tile_tuple);
          auto& t_var = std::get<1>(*tile_tuple);
          auto& t_validity = std::get<2>(*tile_tuple);

          if (!disable_cache) {
            logger_->info("using cache");
            // Get information about the tile in its fragment.
            auto tile_attr_uri = fragment->uri(name);
            auto tile_idx = tile->tile_idx();
            uint64_t tile_attr_offset;
            RETURN_NOT_OK(
                fragment->file_offset(name, tile_idx, &tile_attr_offset));

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
                  name, tile_idx, &tile_attr_var_offset));

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
                  name, tile_idx, &tile_attr_validity_offset));

              // Store the filtered buffer in the tile cache.
              RETURN_NOT_OK(storage_manager_->write_to_cache(
                  tile_attr_validity_uri,
                  tile_attr_validity_offset,
                  t_validity.filtered_buffer()));
            }
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

std::tuple<Status, std::optional<uint64_t>> ReaderBase::get_attribute_tile_size(
    const std::string& name, unsigned f, uint64_t t) {
  uint64_t tile_size = 0;
  tile_size += fragment_metadata_[f]->tile_size(name, t);

  if (array_schema_->var_size(name)) {
    uint64_t temp = 0;
    RETURN_NOT_OK_TUPLE(
        fragment_metadata_[f]->tile_var_size(name, t, &temp), std::nullopt);
    tile_size += temp;
  }

  if (array_schema_->is_nullable(name)) {
    tile_size +=
        fragment_metadata_[f]->cell_num(t) * constants::cell_validity_size;
  }

  return {Status::Ok(), tile_size};
}

template <class T>
void ReaderBase::compute_result_space_tiles(
    const Subarray& subarray,
    const Subarray& partitioner_subarray,
    std::map<const T*, ResultSpaceTile<T>>& result_space_tiles) const {
  // For easy reference
  auto domain = array_schema_->domain()->domain();
  auto tile_extents = array_schema_->domain()->tile_extents();
  auto tile_order = array_schema_->tile_order();

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
    if (it.first == constants::coords || array_schema_->is_dim(it.first))
      return true;
  }

  return false;
}

template <class T>
std::tuple<Status, std::optional<bool>> ReaderBase::fill_dense_coords(
    const Subarray& subarray) {
  auto timer_se = stats_->start_timer("fill_dense_coords");

  // Reading coordinates with a query condition is currently unsupported.
  // Query conditions mutate the result cell slabs to filter attributes.
  // This path does not use result cell slabs, which will fill coordinates
  // for cells that should be filtered out.
  if (!condition_.empty()) {
    return {logger_->status(Status_ReaderError(
                "Cannot read dense coordinates; dense coordinate "
                "reads are unsupported with a query condition")),
            std::nullopt};
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

  bool overflowed = false;
  if (layout_ == Layout::GLOBAL_ORDER) {
    auto&& [st, of] =
        fill_dense_coords_global<T>(subarray, dim_idx, buffers, offsets);
    RETURN_NOT_OK_TUPLE(st, std::nullopt);
    overflowed = *of;
  } else {
    assert(layout_ == Layout::ROW_MAJOR || layout_ == Layout::COL_MAJOR);
    auto&& [st, of] =
        fill_dense_coords_row_col<T>(subarray, dim_idx, buffers, offsets);
    RETURN_NOT_OK_TUPLE(st, std::nullopt);
    overflowed = *of;
  }

  // Update buffer sizes
  for (size_t i = 0; i < buffers.size(); ++i)
    *(buffers[i]->buffer_size_) = offsets[i];

  return {Status::Ok(), overflowed};
}

template <class T>
std::tuple<Status, std::optional<bool>> ReaderBase::fill_dense_coords_global(
    const Subarray& subarray,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>& offsets) {
  auto tile_coords = subarray.tile_coords();
  auto cell_order = array_schema_->cell_order();

  bool overflowed = false;
  for (const auto& tc : tile_coords) {
    auto tile_subarray = subarray.crop_to_tile((const T*)&tc[0], cell_order);
    auto&& [st, of] =
        fill_dense_coords_row_col<T>(tile_subarray, dim_idx, buffers, offsets);
    RETURN_NOT_OK_TUPLE(st, std::nullopt);
    overflowed |= *of;
  }

  return {Status::Ok(), overflowed};
}

template <class T>
std::tuple<Status, std::optional<bool>> ReaderBase::fill_dense_coords_row_col(
    const Subarray& subarray,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>& offsets) {
  auto cell_order = array_schema_->cell_order();
  auto dim_num = array_schema_->dim_num();

  // Iterate over all coordinates, retrieved in cell slabs
  CellSlabIter<T> iter(&subarray);
  RETURN_CANCEL_OR_ERROR_TUPLE(iter.begin());
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
      auto offset = offsets[i];
      if (coords_num * coord_size + offset > buff_size) {
        return {Status::Ok(), true};
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

  return {Status::Ok(), false};
}

template <class T>
void ReaderBase::fill_dense_coords_row_slab(
    const T* start,
    uint64_t num,
    const std::vector<unsigned>& dim_idx,
    const std::vector<QueryBuffer*>& buffers,
    std::vector<uint64_t>& offsets) const {
  // For easy reference
  auto dim_num = array_schema_->dim_num();

  // Special zipped coordinates
  if (dim_idx.size() == 1 && dim_idx[0] == dim_num) {
    auto c_buff = (char*)buffers[0]->buffer_;
    auto offset = &offsets[0];

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
        auto offset = &offsets[b];

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
    std::vector<uint64_t>& offsets) const {
  // For easy reference
  auto dim_num = array_schema_->dim_num();

  // Special zipped coordinates
  if (dim_idx.size() == 1 && dim_idx[0] == dim_num) {
    auto c_buff = (char*)buffers[0]->buffer_;
    auto offset = &offsets[0];

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
        auto offset = &offsets[b];

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

template std::tuple<Status, std::optional<bool>>
ReaderBase::fill_dense_coords<int8_t>(const Subarray&);
template std::tuple<Status, std::optional<bool>>
ReaderBase::fill_dense_coords<uint8_t>(const Subarray&);
template std::tuple<Status, std::optional<bool>>
ReaderBase::fill_dense_coords<int16_t>(const Subarray&);
template std::tuple<Status, std::optional<bool>>
ReaderBase::fill_dense_coords<uint16_t>(const Subarray&);
template std::tuple<Status, std::optional<bool>>
ReaderBase::fill_dense_coords<int32_t>(const Subarray&);
template std::tuple<Status, std::optional<bool>>
ReaderBase::fill_dense_coords<uint32_t>(const Subarray&);
template std::tuple<Status, std::optional<bool>>
ReaderBase::fill_dense_coords<int64_t>(const Subarray&);
template std::tuple<Status, std::optional<bool>>
ReaderBase::fill_dense_coords<uint64_t>(const Subarray&);

}  // namespace sm
}  // namespace tiledb
