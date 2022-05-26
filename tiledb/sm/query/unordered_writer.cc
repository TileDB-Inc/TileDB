/**
 * @file   unordered_writer.cc
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
 * This file implements class UnorderedWriter.
 */

#include "tiledb/sm/query/unordered_writer.h"
#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/hilbert.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/sm/query/hilbert_order.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile_metadata_generator.h"
#include "tiledb/sm/tile/writer_tile.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

UnorderedWriter::UnorderedWriter(
    stats::Stats* stats,
    shared_ptr<Logger> logger,
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout,
    std::vector<WrittenFragmentInfo>& written_fragment_info,
    Query::CoordsInfo& coords_info,
    URI fragment_uri)
    : WriterBase(
          stats,
          logger,
          storage_manager,
          array,
          config,
          buffers,
          subarray,
          layout,
          written_fragment_info,
          false,
          coords_info,
          fragment_uri) {
}

UnorderedWriter::~UnorderedWriter() {
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status UnorderedWriter::dowork() {
  get_dim_attr_stats();

  auto timer_se = stats_->start_timer("write");

  // In case the user has provided a coordinates buffer
  RETURN_NOT_OK(split_coords_buffer());

  if (check_coord_oob_)
    RETURN_NOT_OK(check_coord_oob());

  RETURN_NOT_OK(unordered_write());

  return Status::Ok();
}

Status UnorderedWriter::finalize() {
  auto timer_se = stats_->start_timer("finalize");

  return Status::Ok();
}

void UnorderedWriter::reset() {
  initialized_ = false;
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

Status UnorderedWriter::check_coord_dups(
    const std::vector<uint64_t>& cell_pos) const {
  auto timer_se = stats_->start_timer("check_coord_dups");

  // Check if applicable
  if (array_schema_.allows_dups() || !check_coord_dups_ || dedup_coords_)
    return Status::Ok();

  if (!coords_info_.has_coords_) {
    return logger_->status(
        Status_WriterError("Cannot check for coordinate duplicates; "
                           "Coordinates buffer not found"));
  }

  if (coords_info_.coords_num_ < 2)
    return Status::Ok();

  // Prepare auxiliary vectors for better performance
  auto dim_num = array_schema_.dim_num();
  std::vector<const unsigned char*> buffs(dim_num);
  std::vector<uint64_t> coord_sizes(dim_num);
  std::vector<const unsigned char*> buffs_var(dim_num);
  std::vector<uint64_t*> buffs_var_sizes(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name{array_schema_.dimension_ptr(d)->name()};
    buffs[d] = (const unsigned char*)buffers_.find(dim_name)->second.buffer_;
    coord_sizes[d] = array_schema_.cell_size(dim_name);
    buffs_var[d] =
        (const unsigned char*)buffers_.find(dim_name)->second.buffer_var_;
    buffs_var_sizes[d] = buffers_.find(dim_name)->second.buffer_var_size_;
  }

  auto status = parallel_for(
      storage_manager_->compute_tp(),
      1,
      coords_info_.coords_num_,
      [&](uint64_t i) {
        // Check for duplicate in adjacent cells
        bool found_dup = true;
        for (unsigned d = 0; d < dim_num; ++d) {
          auto dim{array_schema_.dimension_ptr(d)};
          if (!dim->var_size()) {  // Fixed-sized dimensions
            if (memcmp(
                    buffs[d] + cell_pos[i] * coord_sizes[d],
                    buffs[d] + cell_pos[i - 1] * coord_sizes[d],
                    coord_sizes[d]) != 0) {  // Not the same
              found_dup = false;
              break;
            }
          } else {
            auto offs = (uint64_t*)buffs[d];
            auto a = cell_pos[i];
            auto b = cell_pos[i - 1];
            auto off_a = offs[a];
            auto off_b = offs[b];
            auto off_a_plus_1 = (a == coords_info_.coords_num_ - 1) ?
                                    *(buffs_var_sizes[d]) :
                                    offs[a + 1];
            auto off_b_plus_1 = (b == coords_info_.coords_num_ - 1) ?
                                    *(buffs_var_sizes[d]) :
                                    offs[b + 1];
            auto size_a = off_a_plus_1 - off_a;
            auto size_b = off_b_plus_1 - off_b;

            // Compare sizes
            if (size_a != size_b) {  // Not same
              found_dup = false;
              break;
            }

            // Compare var values
            if (memcmp(
                    buffs_var[d] + off_a,
                    buffs_var[d] + off_b,
                    size_a) != 0) {  // Not the same
              found_dup = false;
              break;
            }
          }
        }

        // Found duplicate
        if (found_dup) {
          std::stringstream ss;
          ss << "Duplicate coordinates " << coords_to_str(cell_pos[i]);
          ss << " are not allowed";
          return Status_WriterError(ss.str());
        }

        return Status::Ok();
      });

  RETURN_NOT_OK_ELSE(status, logger_->status(status));

  return Status::Ok();
}

void UnorderedWriter::clean_up(const URI& uri) {
  storage_manager_->vfs()->remove_dir(uri);
}

Status UnorderedWriter::compute_coord_dups(
    const std::vector<uint64_t>& cell_pos,
    std::set<uint64_t>* coord_dups) const {
  auto timer_se = stats_->start_timer("compute_coord_dups");

  if (!coords_info_.has_coords_) {
    return logger_->status(
        Status_WriterError("Cannot check for coordinate duplicates; "
                           "Coordinates buffer not found"));
  }

  if (coords_info_.coords_num_ < 2)
    return Status::Ok();

  // Prepare auxiliary vectors for better performance
  auto dim_num = array_schema_.dim_num();
  std::vector<const unsigned char*> buffs(dim_num);
  std::vector<uint64_t> coord_sizes(dim_num);
  std::vector<const unsigned char*> buffs_var(dim_num);
  std::vector<uint64_t*> buffs_var_sizes(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name{array_schema_.dimension_ptr(d)->name()};
    buffs[d] = (const unsigned char*)buffers_.find(dim_name)->second.buffer_;
    coord_sizes[d] = array_schema_.cell_size(dim_name);
    buffs_var[d] =
        (const unsigned char*)buffers_.find(dim_name)->second.buffer_var_;
    buffs_var_sizes[d] = buffers_.find(dim_name)->second.buffer_var_size_;
  }

  std::mutex mtx;
  auto status = parallel_for(
      storage_manager_->compute_tp(),
      1,
      coords_info_.coords_num_,
      [&](uint64_t i) {
        // Check for duplicate in adjacent cells
        bool found_dup = true;
        for (unsigned d = 0; d < dim_num; ++d) {
          auto dim{array_schema_.dimension_ptr(d)};
          if (!dim->var_size()) {  // Fixed-sized dimensions
            if (memcmp(
                    buffs[d] + cell_pos[i] * coord_sizes[d],
                    buffs[d] + cell_pos[i - 1] * coord_sizes[d],
                    coord_sizes[d]) != 0) {  // Not the same
              found_dup = false;
              break;
            }
          } else {
            auto offs = (uint64_t*)buffs[d];
            auto a = cell_pos[i];
            auto b = cell_pos[i - 1];
            auto off_a = offs[a];
            auto off_b = offs[b];
            auto off_a_plus_1 = (a == coords_info_.coords_num_ - 1) ?
                                    *(buffs_var_sizes[d]) :
                                    offs[a + 1];
            auto off_b_plus_1 = (b == coords_info_.coords_num_ - 1) ?
                                    *(buffs_var_sizes[d]) :
                                    offs[b + 1];
            auto size_a = off_a_plus_1 - off_a;
            auto size_b = off_b_plus_1 - off_b;

            // Compare sizes
            if (size_a != size_b) {  // Not same
              found_dup = false;
              break;
            }

            // Compare var values
            if (memcmp(
                    buffs_var[d] + off_a,
                    buffs_var[d] + off_b,
                    size_a) != 0) {  // Not the same
              found_dup = false;
              break;
            }
          }
        }

        // Found duplicate
        if (found_dup) {
          std::lock_guard<std::mutex> lock(mtx);
          coord_dups->insert(cell_pos[i]);
        }

        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status UnorderedWriter::prepare_tiles(
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    std::unordered_map<std::string, WriterTileVector>* tiles) const {
  auto timer_se = stats_->start_timer("prepare_tiles");

  // Initialize attribute tiles
  tiles->clear();
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    (*tiles).emplace(name, WriterTileVector(array_schema_, name));
  }

  // Prepare tiles for all attributes and coordinates
  auto buffer_num = buffers_.size();
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, buffer_num, [&](uint64_t i) {
        auto buff_it = buffers_.begin();
        std::advance(buff_it, i);
        const auto& name = buff_it->first;
        RETURN_CANCEL_OR_ERROR(
            prepare_tiles(name, cell_pos, coord_dups, &((*tiles)[name])));
        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status UnorderedWriter::prepare_tiles(
    const std::string& name,
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    WriterTileVector* tiles) const {
  return array_schema_.var_size(name) ?
             prepare_tiles_var(name, cell_pos, coord_dups, tiles) :
             prepare_tiles_fixed(name, cell_pos, coord_dups, tiles);
}

Status UnorderedWriter::prepare_tiles_fixed(
    const std::string& name,
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    WriterTileVector* tiles) const {
  // Trivial case
  if (cell_pos.empty())
    return Status::Ok();

  // For easy reference
  auto nullable = array_schema_.is_nullable(name);
  auto buffer = (unsigned char*)buffers_.find(name)->second.buffer_;
  auto buffer_validity =
      (unsigned char*)buffers_.find(name)->second.validity_vector_.buffer();
  auto cell_size = array_schema_.cell_size(name);
  auto cell_num = (uint64_t)cell_pos.size();
  auto capacity = array_schema_.capacity();
  auto dups_num = coord_dups.size();
  auto tile_num = utils::math::ceil(cell_num - dups_num, capacity);
  auto& domain{array_schema_.domain()};
  auto cell_num_per_tile =
      coords_info_.has_coords_ ? capacity : domain.cell_num_per_tile();

  // Initialize tiles
  tiles->resize(tile_num);
  for (auto& tile : *tiles) {
    if (!nullable)
      RETURN_NOT_OK(init_tile(name, &tile.fixed_tile()));
    else
      RETURN_NOT_OK(
          init_tile_nullable(name, &tile.fixed_tile(), &tile.validity_tile()));
  }

  // Write all cells one by one
  uint64_t cell_idx = 0;
  auto tile_it = tiles->begin();
  if (dups_num == 0) {
    for (uint64_t i = 0; i < cell_num; ++i, ++cell_idx) {
      if (cell_idx == cell_num_per_tile) {
        tile_it++;
        cell_idx = 0;
      }

      RETURN_NOT_OK(tile_it->fixed_tile().write(
          buffer + cell_pos[i] * cell_size, cell_idx * cell_size, cell_size));
      if (nullable)
        RETURN_NOT_OK(tile_it->validity_tile().write(
            buffer_validity + cell_pos[i] * constants::cell_validity_size,
            cell_idx * constants::cell_validity_size,
            constants::cell_validity_size));
    }
  } else {
    for (uint64_t i = 0; i < cell_num; ++i) {
      if (coord_dups.find(cell_pos[i]) != coord_dups.end())
        continue;

      if (cell_idx == cell_num_per_tile) {
        tile_it++;
        cell_idx = 0;
      }

      RETURN_NOT_OK(tile_it->fixed_tile().write(
          buffer + cell_pos[i] * cell_size, cell_idx * cell_size, cell_size));
      if (nullable)
        RETURN_NOT_OK(tile_it->validity_tile().write(
            buffer_validity + cell_pos[i] * constants::cell_validity_size,
            cell_idx * constants::cell_validity_size,
            constants::cell_validity_size));
      ++cell_idx;
    }
  }

  uint64_t last_tile_cell_num = (cell_num - dups_num) % capacity;
  if (last_tile_cell_num != 0) {
    tile_it->fixed_tile().final_size(last_tile_cell_num * cell_size);

    if (nullable) {
      tile_it->validity_tile().final_size(
          last_tile_cell_num * constants::cell_validity_size);
    }
  }

  return Status::Ok();
}

Status UnorderedWriter::prepare_tiles_var(
    const std::string& name,
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    WriterTileVector* tiles) const {
  // For easy reference
  auto it = buffers_.find(name);
  auto nullable = array_schema_.is_nullable(name);
  auto buffer = it->second.buffer_;
  auto buffer_var = (unsigned char*)it->second.buffer_var_;
  auto buffer_validity = (uint8_t*)it->second.validity_vector_.buffer();
  auto buffer_var_size = it->second.buffer_var_size_;
  auto cell_num = (uint64_t)cell_pos.size();
  auto capacity = array_schema_.capacity();
  auto dups_num = coord_dups.size();
  auto tile_num = utils::math::ceil(cell_num - dups_num, capacity);
  auto attr_datatype_size = datatype_size(array_schema_.type(name));
  auto cell_num_per_tile = coords_info_.has_coords_ ?
                               capacity :
                               array_schema_.domain().cell_num_per_tile();

  // Initialize tiles
  tiles->resize(tile_num);
  for (auto& tile : *tiles) {
    if (!nullable)
      RETURN_NOT_OK(init_tile(name, &tile.offset_tile(), &tile.var_tile()));
    else
      RETURN_NOT_OK(init_tile_nullable(
          name, &tile.offset_tile(), &tile.var_tile(), &tile.validity_tile()));
  }

  // Write all cells one by one
  uint64_t cell_idx = 0;
  auto tile_it = tiles->begin();
  uint64_t offset = 0;
  if (dups_num == 0) {
    for (uint64_t i = 0; i < cell_num; ++i, ++cell_idx) {
      if (cell_idx == cell_num_per_tile) {
        tile_it->var_tile().final_size(offset);
        cell_idx = 0;
        offset = 0;
        tile_it++;
      }

      // Write offset.
      RETURN_NOT_OK(tile_it->offset_tile().write(
          &offset, cell_idx * sizeof(offset), sizeof(offset)));

      // Write var-sized value(s).
      auto buff_offset =
          prepare_buffer_offset(buffer, cell_pos[i], attr_datatype_size);
      uint64_t var_size = (cell_pos[i] == cell_num - 1) ?
                              *buffer_var_size - buff_offset :
                              prepare_buffer_offset(
                                  buffer, cell_pos[i] + 1, attr_datatype_size) -
                                  buff_offset;
      RETURN_NOT_OK(tile_it->var_tile().write_var(
          buffer_var + buff_offset, offset, var_size));
      offset += var_size;

      // Write validity value(s).
      if (nullable) {
        RETURN_NOT_OK(tile_it->validity_tile().write(
            buffer_validity + cell_pos[i],
            cell_idx * constants::cell_validity_size,
            constants::cell_validity_size));
      }
    }
  } else {
    for (uint64_t i = 0; i < cell_num; ++i) {
      if (coord_dups.find(cell_pos[i]) != coord_dups.end())
        continue;

      if (cell_idx == cell_num_per_tile) {
        tile_it->var_tile().final_size(offset);
        cell_idx = 0;
        offset = 0;
        tile_it++;
      }

      // Write offset.
      RETURN_NOT_OK(tile_it->offset_tile().write(
          &offset, cell_idx * sizeof(offset), sizeof(offset)));

      // Write var-sized value(s).
      auto buff_offset =
          prepare_buffer_offset(buffer, cell_pos[i], attr_datatype_size);
      uint64_t var_size = (cell_pos[i] == cell_num - 1) ?
                              *buffer_var_size - buff_offset :
                              prepare_buffer_offset(
                                  buffer, cell_pos[i] + 1, attr_datatype_size) -
                                  buff_offset;
      RETURN_NOT_OK(tile_it->var_tile().write_var(
          buffer_var + buff_offset, offset, var_size));
      offset += var_size;

      // Write validity value(s).
      if (nullable) {
        RETURN_NOT_OK(tile_it->validity_tile().write(
            buffer_validity + cell_pos[i],
            cell_idx * constants::cell_validity_size,
            constants::cell_validity_size));
      }

      ++cell_idx;
    }
  }

  if (cell_num > 0) {
    tile_it->var_tile().final_size(offset);
  }

  uint64_t last_tile_cell_num = (cell_num - dups_num) % capacity;
  if (last_tile_cell_num != 0) {
    tile_it->offset_tile().final_size(
        last_tile_cell_num * constants::cell_var_offset_size);

    if (nullable) {
      tile_it->validity_tile().final_size(
          last_tile_cell_num * constants::cell_validity_size);
    }
  }

  return Status::Ok();
}

Status UnorderedWriter::sort_coords(std::vector<uint64_t>& cell_pos) const {
  auto timer_se = stats_->start_timer("sort_coords");

  // Populate cell_pos
  cell_pos.resize(coords_info_.coords_num_);
  for (uint64_t i = 0; i < coords_info_.coords_num_; ++i)
    cell_pos[i] = i;

  // Sort the coordinates in global order
  auto cell_order = array_schema_.cell_order();
  const Domain& domain = array_schema_.domain();
  DomainBuffersView domain_buffs{array_schema_, buffers_};
  if (cell_order != Layout::HILBERT) {  // Row- or col-major
    parallel_sort(
        storage_manager_->compute_tp(),
        cell_pos.begin(),
        cell_pos.end(),
        GlobalCmpQB(domain, domain_buffs));
  } else {  // Hilbert order
    std::vector<uint64_t> hilbert_values(coords_info_.coords_num_);
    RETURN_NOT_OK(calculate_hilbert_values(domain_buffs, hilbert_values));
    parallel_sort(
        storage_manager_->compute_tp(),
        cell_pos.begin(),
        cell_pos.end(),
        HilbertCmpQB(domain, domain_buffs, hilbert_values));
  }

  return Status::Ok();
}

Status UnorderedWriter::unordered_write() {
  // Applicable only to unordered write on sparse arrays
  assert(layout_ == Layout::UNORDERED);
  assert(!array_schema_.dense());

  // Sort coordinates first
  std::vector<uint64_t> cell_pos;
  RETURN_CANCEL_OR_ERROR(sort_coords(cell_pos));

  // Check for coordinate duplicates
  RETURN_CANCEL_OR_ERROR(check_coord_dups(cell_pos));

  // Retrieve coordinate duplicates
  std::set<uint64_t> coord_dups;
  if (dedup_coords_)
    RETURN_CANCEL_OR_ERROR(compute_coord_dups(cell_pos, &coord_dups));

  // Create new fragment
  auto frag_meta = make_shared<FragmentMetadata>(HERE());
  RETURN_CANCEL_OR_ERROR(create_fragment(false, frag_meta));
  const auto& uri = frag_meta->fragment_uri();

  // Prepare tiles
  std::unordered_map<std::string, WriterTileVector> tiles;
  RETURN_CANCEL_OR_ERROR_ELSE(
      prepare_tiles(cell_pos, coord_dups, &tiles), clean_up(uri));

  // Clear the boolean vector for coordinate duplicates
  coord_dups.clear();

  // No tiles
  if (tiles.empty() || tiles.begin()->second.empty()) {
    clean_up(uri);
    return Status::Ok();
  }

  // Set the number of tiles in the metadata
  auto it = tiles.begin();
  auto tile_num = it->second.size();
  frag_meta->set_num_tiles(tile_num);

  stats_->add_counter("tile_num", tile_num);
  stats_->add_counter("cell_num", cell_pos.size());

  // Compute coordinates metadata
  RETURN_CANCEL_OR_ERROR_ELSE(
      compute_coords_metadata(tiles, frag_meta), clean_up(uri));

  // Compute tile metadata.
  RETURN_CANCEL_OR_ERROR_ELSE(
      compute_tiles_metadata(tile_num, tiles), clean_up(uri));

  // Filter all tiles
  RETURN_CANCEL_OR_ERROR_ELSE(filter_tiles(&tiles), clean_up(uri));

  // Write tiles for all attributes and coordinates
  RETURN_CANCEL_OR_ERROR_ELSE(
      write_all_tiles(frag_meta, &tiles), clean_up(uri));

  // Compute fragment min/max/sum/null count
  RETURN_NOT_OK_ELSE(
      frag_meta->compute_fragment_min_max_sum_null_count(), clean_up(uri));

  // Write the fragment metadata
  RETURN_CANCEL_OR_ERROR_ELSE(
      frag_meta->store(array_->get_encryption_key()), clean_up(uri));

  // Add written fragment info
  RETURN_NOT_OK_ELSE(add_written_fragment_info(uri), clean_up(uri));

  // The following will make the fragment visible
  auto&& [st, commit_uri] = array_->array_directory().get_commit_uri(uri);
  RETURN_NOT_OK_ELSE(st, storage_manager_->vfs()->remove_dir(uri));
  RETURN_NOT_OK_ELSE(
      storage_manager_->vfs()->touch(commit_uri.value()), clean_up(uri));

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
