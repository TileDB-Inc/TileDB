/**
 * @file   unordered_writer.cc
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
 * This file implements class UnorderedWriter.
 */

#include "tiledb/sm/query/writers/unordered_writer.h"
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
#include "tiledb/sm/query/hilbert_order.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile_metadata_generator.h"
#include "tiledb/sm/tile/writer_tile_tuple.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb::sm {

class UnorderWriterException : public StatusException {
 public:
  explicit UnorderWriterException(const std::string& message)
      : StatusException("UnorderWriter", message) {
  }
};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

UnorderedWriter::UnorderedWriter(
    stats::Stats* stats,
    shared_ptr<Logger> logger,
    StrategyParams& params,
    std::vector<WrittenFragmentInfo>& written_fragment_info,
    Query::CoordsInfo& coords_info,
    std::unordered_set<std::string>& written_buffers,
    bool remote_query,
    optional<std::string> fragment_name)
    : WriterBase(
          stats,
          logger,
          params,
          written_fragment_info,
          false,
          coords_info,
          remote_query,
          fragment_name)
    , frag_uri_(std::nullopt)
    , cell_pos_(query_memory_tracker_->get_resource(MemoryType::WRITER_DATA))
    , written_buffers_(written_buffers)
    , is_coords_pass_(true) {
  // Check the layout is unordered.
  if (layout_ != Layout::UNORDERED) {
    throw UnorderWriterException(
        "Failed to initialize UnorderedWriter; The unordered writer does not "
        "support layout " +
        layout_str(layout_));
  }

  // Check the array is sparse.
  if (array_schema_.dense()) {
    throw UnorderWriterException(
        "Failed to initialize UnorderedWriter; The unordered "
        "writer does not support dense arrays.");
  }

  // Check no ordered attributes.
  if (array_schema_.has_ordered_attributes()) {
    throw UnorderWriterException(
        "Failed to initialize UnorderedWriter; The unordered writer does not "
        "support ordered attributes.");
  }
}

UnorderedWriter::~UnorderedWriter() {
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status UnorderedWriter::dowork() {
  get_dim_attr_stats();

  auto timer_se = stats_->start_timer("dowork");

  // In case the user has provided a coordinates buffer
  RETURN_NOT_OK(split_coords_buffer());

  if (check_coord_oob_ && is_coords_pass_) {
    RETURN_NOT_OK(check_coord_oob());
  }

  try {
    auto status = unordered_write();
    if (!status.ok()) {
      clean_up();
      return status;
    }
  } catch (...) {
    clean_up();
    std::throw_with_nested(std::runtime_error("['UnorderedWriter::dowork] "));
  }

  return Status::Ok();
}

Status UnorderedWriter::finalize() {
  auto timer_se = stats_->start_timer("finalize");

  if (written_buffers_.size() <
      array_schema_.dim_num() + array_schema_.attribute_num()) {
    throw UnorderWriterException("Not all buffers already written");
  }

  return Status::Ok();
}

void UnorderedWriter::reset() {
}

std::string UnorderedWriter::name() {
  return "UnorderedWriter";
}

Status UnorderedWriter::alloc_frag_meta() {
  // Alloc FragmentMetadata object.
  frag_meta_ = this->create_fragment_metadata();
  // Used in serialization when FragmentMetadata is built from ground up.
  frag_meta_->set_context_resources(&resources_);

  return Status::Ok();
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

void UnorderedWriter::clean_up() {
  if (frag_uri_.has_value()) {
    throw_if_not_ok(resources_.vfs().remove_dir(frag_uri_.value()));
  }
}

Status UnorderedWriter::check_coord_dups() const {
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
      &resources_.compute_tp(), 1, coords_info_.coords_num_, [&](uint64_t i) {
        // Check for duplicate in adjacent cells
        bool found_dup = true;
        for (unsigned d = 0; d < dim_num; ++d) {
          auto dim{array_schema_.dimension_ptr(d)};
          if (!dim->var_size()) {  // Fixed-sized dimensions
            if (memcmp(
                    buffs[d] + cell_pos_[i] * coord_sizes[d],
                    buffs[d] + cell_pos_[i - 1] * coord_sizes[d],
                    coord_sizes[d]) != 0) {  // Not the same
              found_dup = false;
              break;
            }
          } else {
            auto offs = (uint64_t*)buffs[d];
            auto a = cell_pos_[i];
            auto b = cell_pos_[i - 1];
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
          ss << "Duplicate coordinates " << coords_to_str(cell_pos_[i]);
          ss << " are not allowed";
          return Status_WriterError(ss.str());
        }

        return Status::Ok();
      });

  RETURN_NOT_OK_ELSE(status, logger_->error(status.message()));

  return Status::Ok();
}

Status UnorderedWriter::compute_coord_dups() {
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
      &resources_.compute_tp(), 1, coords_info_.coords_num_, [&](uint64_t i) {
        // Check for duplicate in adjacent cells
        bool found_dup = true;
        for (unsigned d = 0; d < dim_num; ++d) {
          auto dim{array_schema_.dimension_ptr(d)};
          if (!dim->var_size()) {  // Fixed-sized dimensions
            if (memcmp(
                    buffs[d] + cell_pos_[i] * coord_sizes[d],
                    buffs[d] + cell_pos_[i - 1] * coord_sizes[d],
                    coord_sizes[d]) != 0) {  // Not the same
              found_dup = false;
              break;
            }
          } else {
            auto offs = (uint64_t*)buffs[d];
            auto a = cell_pos_[i];
            auto b = cell_pos_[i - 1];
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
          coord_dups_.insert(cell_pos_[i]);
        }

        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status UnorderedWriter::prepare_tiles(
    tdb::pmr::unordered_map<std::string, WriterTileTupleVector>* tiles) const {
  auto timer_se = stats_->start_timer("prepare_tiles");

  // Initialize attribute tiles
  tiles->clear();
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    if (written_buffers_.count(name) == 0) {
      tiles->emplace(
          std::piecewise_construct,
          std::forward_as_tuple(name),
          std::forward_as_tuple(query_memory_tracker_));
    }
  }

  // Prepare tiles for all attributes and coordinates
  auto status =
      parallel_for(&resources_.compute_tp(), 0, tiles->size(), [&](uint64_t i) {
        auto tiles_it = tiles->begin();
        std::advance(tiles_it, i);
        throw_if_not_ok(prepare_tiles(tiles_it->first, &(tiles_it->second)));
        throw_if_cancelled();
        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status UnorderedWriter::prepare_tiles(
    const std::string& name, WriterTileTupleVector* tiles) const {
  return array_schema_.var_size(name) ? prepare_tiles_var(name, tiles) :
                                        prepare_tiles_fixed(name, tiles);
}

Status UnorderedWriter::prepare_tiles_fixed(
    const std::string& name, WriterTileTupleVector* tiles) const {
  // Trivial case
  if (cell_pos_.empty()) {
    return Status::Ok();
  }

  // For easy reference
  auto nullable = array_schema_.is_nullable(name);
  auto type = array_schema_.type(name);
  auto buffer = (unsigned char*)buffers_.find(name)->second.buffer_;
  auto buffer_validity =
      (unsigned char*)buffers_.find(name)->second.validity_vector_.buffer();
  auto cell_size = array_schema_.cell_size(name);
  auto cell_num = (uint64_t)cell_pos_.size();
  auto cell_num_per_tile = array_schema_.capacity();
  auto dups_num = coord_dups_.size();
  auto tile_num = utils::math::ceil(cell_num - dups_num, cell_num_per_tile);

  // Initialize tiles
  tiles->reserve(tile_num);
  for (uint64_t i = 0; i < tile_num; i++) {
    tiles->emplace_back(
        array_schema_,
        cell_num_per_tile,
        false,
        nullable,
        cell_size,
        type,
        query_memory_tracker_);
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

      tile_it->fixed_tile().write(
          buffer + cell_pos_[i] * cell_size, cell_idx * cell_size, cell_size);
      if (nullable)
        tile_it->validity_tile().write(
            buffer_validity + cell_pos_[i] * constants::cell_validity_size,
            cell_idx * constants::cell_validity_size,
            constants::cell_validity_size);
    }
  } else {
    for (uint64_t i = 0; i < cell_num; ++i) {
      if (coord_dups_.find(cell_pos_[i]) != coord_dups_.end())
        continue;

      if (cell_idx == cell_num_per_tile) {
        tile_it++;
        cell_idx = 0;
      }

      tile_it->fixed_tile().write(
          buffer + cell_pos_[i] * cell_size, cell_idx * cell_size, cell_size);
      if (nullable)
        tile_it->validity_tile().write(
            buffer_validity + cell_pos_[i] * constants::cell_validity_size,
            cell_idx * constants::cell_validity_size,
            constants::cell_validity_size);
      ++cell_idx;
    }
  }

  uint64_t last_tile_cell_num = (cell_num - dups_num) % cell_num_per_tile;
  if (last_tile_cell_num != 0) {
    tile_it->set_final_size(last_tile_cell_num);
  }

  return Status::Ok();
}

Status UnorderedWriter::prepare_tiles_var(
    const std::string& name, WriterTileTupleVector* tiles) const {
  // For easy reference
  auto it = buffers_.find(name);
  auto nullable = array_schema_.is_nullable(name);
  auto cell_size = array_schema_.cell_size(name);
  auto type = array_schema_.type(name);
  auto buffer = it->second.buffer_;
  auto buffer_var = (unsigned char*)it->second.buffer_var_;
  auto buffer_validity = (uint8_t*)it->second.validity_vector_.buffer();
  auto buffer_var_size = it->second.buffer_var_size_;
  auto cell_num = (uint64_t)cell_pos_.size();
  auto cell_num_per_tile = array_schema_.capacity();
  auto dups_num = coord_dups_.size();
  auto tile_num = utils::math::ceil(cell_num - dups_num, cell_num_per_tile);
  auto attr_datatype_size = datatype_size(array_schema_.type(name));

  // Initialize tiles
  tiles->reserve(tile_num);
  for (uint64_t i = 0; i < tile_num; i++) {
    tiles->emplace_back(
        array_schema_,
        cell_num_per_tile,
        true,
        nullable,
        cell_size,
        type,
        query_memory_tracker_);
  }

  // Write all cells one by one
  uint64_t cell_idx = 0;
  auto tile_it = tiles->begin();
  uint64_t offset = 0;
  if (dups_num == 0) {
    for (uint64_t i = 0; i < cell_num; ++i, ++cell_idx) {
      if (cell_idx == cell_num_per_tile) {
        tile_it->var_tile().set_size(offset);
        cell_idx = 0;
        offset = 0;
        tile_it++;
      }

      // Write offset.
      tile_it->offset_tile().write(
          &offset, cell_idx * sizeof(offset), sizeof(offset));

      // Write var-sized value(s).
      auto buff_offset =
          prepare_buffer_offset(buffer, cell_pos_[i], attr_datatype_size);
      uint64_t var_size =
          (cell_pos_[i] == cell_num - 1) ?
              *buffer_var_size - buff_offset :
              prepare_buffer_offset(
                  buffer, cell_pos_[i] + 1, attr_datatype_size) -
                  buff_offset;
      tile_it->var_tile().write_var(buffer_var + buff_offset, offset, var_size);
      offset += var_size;

      // Write validity value(s).
      if (nullable) {
        tile_it->validity_tile().write(
            buffer_validity + cell_pos_[i],
            cell_idx * constants::cell_validity_size,
            constants::cell_validity_size);
      }
    }
  } else {
    for (uint64_t i = 0; i < cell_num; ++i) {
      if (coord_dups_.find(cell_pos_[i]) != coord_dups_.end())
        continue;

      if (cell_idx == cell_num_per_tile) {
        tile_it->var_tile().set_size(offset);
        cell_idx = 0;
        offset = 0;
        tile_it++;
      }

      // Write offset.
      tile_it->offset_tile().write(
          &offset, cell_idx * sizeof(offset), sizeof(offset));

      // Write var-sized value(s).
      auto buff_offset =
          prepare_buffer_offset(buffer, cell_pos_[i], attr_datatype_size);
      uint64_t var_size =
          (cell_pos_[i] == cell_num - 1) ?
              *buffer_var_size - buff_offset :
              prepare_buffer_offset(
                  buffer, cell_pos_[i] + 1, attr_datatype_size) -
                  buff_offset;
      tile_it->var_tile().write_var(buffer_var + buff_offset, offset, var_size);
      offset += var_size;

      // Write validity value(s).
      if (nullable) {
        tile_it->validity_tile().write(
            buffer_validity + cell_pos_[i],
            cell_idx * constants::cell_validity_size,
            constants::cell_validity_size);
      }

      ++cell_idx;
    }
  }

  if (cell_num > 0) {
    tile_it->var_tile().set_size(offset);
  }

  uint64_t last_tile_cell_num = (cell_num - dups_num) % cell_num_per_tile;
  if (last_tile_cell_num != 0) {
    tile_it->set_final_size(last_tile_cell_num);
  }

  return Status::Ok();
}

Status UnorderedWriter::sort_coords() {
  auto timer_se = stats_->start_timer("sort_coords");

  // Populate cell_pos_
  cell_pos_.resize(coords_info_.coords_num_);
  for (uint64_t i = 0; i < coords_info_.coords_num_; ++i)
    cell_pos_[i] = i;

  // Sort the coordinates in global order
  auto cell_order = array_schema_.cell_order();
  const Domain& domain = array_schema_.domain();
  DomainBuffersView domain_buffs{array_schema_, buffers_};
  if (cell_order != Layout::HILBERT) {  // Row- or col-major
    parallel_sort(
        &resources_.compute_tp(),
        cell_pos_.begin(),
        cell_pos_.end(),
        GlobalCmpQB(domain, domain_buffs));
  } else {  // Hilbert order
    std::vector<uint64_t> hilbert_values(coords_info_.coords_num_);
    RETURN_NOT_OK(calculate_hilbert_values(domain_buffs, hilbert_values));
    parallel_sort(
        &resources_.compute_tp(),
        cell_pos_.begin(),
        cell_pos_.end(),
        HilbertCmpQB(domain, domain_buffs, hilbert_values));
  }

  return Status::Ok();
}

Status UnorderedWriter::unordered_write() {
  // Applicable only to unordered write on sparse arrays
  iassert(layout_ == Layout::UNORDERED, "layout = {}", layout_str(layout_));
  iassert(!array_schema_.dense());

  if (written_buffers_.size() >=
      array_schema_.dim_num() + array_schema_.attribute_num()) {
    throw UnorderWriterException("All buffers already written");
  }

  if (is_coords_pass_) {
    for (ArraySchema::dimension_size_type d = 0; d < array_schema_.dim_num();
         d++) {
      if (buffers_.count(array_schema_.dimension_ptr(d)->name()) == 0) {
        throw UnorderWriterException("All dimension buffers should be set");
      }
    }

    // Sort coordinates first
    RETURN_CANCEL_OR_ERROR(sort_coords());

    // Check for coordinate duplicates
    RETURN_CANCEL_OR_ERROR(check_coord_dups());

    // Retrieve coordinate duplicates
    std::set<uint64_t> coord_dups;
    if (dedup_coords_) {
      RETURN_CANCEL_OR_ERROR(compute_coord_dups());
    }

    // Create new fragment
    frag_meta_ = this->create_fragment_metadata();
    RETURN_CANCEL_OR_ERROR(create_fragment(false, frag_meta_));
  }

  frag_uri_ = frag_meta_->fragment_uri();

  // Prepare tiles
  tdb::pmr::unordered_map<std::string, WriterTileTupleVector> tiles(
      query_memory_tracker_->get_resource(MemoryType::WRITER_TILE_DATA));
  RETURN_CANCEL_OR_ERROR(prepare_tiles(&tiles));

  // No tiles
  if (tiles.empty() || tiles.begin()->second.empty()) {
    // Add the written buffers to the list.
    for (const auto& it : buffers_) {
      const auto& name = it.first;
      written_buffers_.emplace(name);
    }

    return Status::Ok();
  }

  auto it = tiles.begin();
  auto tile_num = it->second.size();
  if (is_coords_pass_) {
    // Set the number of tiles in the metadata
    frag_meta_->set_num_tiles(tile_num);

    stats_->add_counter("tile_num", tile_num);
    stats_->add_counter("cell_num", cell_pos_.size());

    // Compute coordinates metadata
    auto mbrs = compute_mbrs(tiles);
    set_coords_metadata(0, tile_num, tiles, mbrs, frag_meta_);
  }

  // Compute tile metadata.
  RETURN_CANCEL_OR_ERROR(compute_tiles_metadata(tile_num, tiles));

  // Filter all tiles
  RETURN_CANCEL_OR_ERROR(filter_tiles(&tiles));

  // Write tiles for all attributes and coordinates
  RETURN_CANCEL_OR_ERROR(write_tiles(0, tile_num, frag_meta_, &tiles));

  // Add the written buffers to the list.
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    written_buffers_.emplace(name);
  }

  if (written_buffers_.size() >=
      array_schema_.dim_num() + array_schema_.attribute_num()) {
    // Compute fragment min/max/sum/null count and write the fragment metadata
    frag_meta_->compute_fragment_min_max_sum_null_count();
    frag_meta_->store(array_->get_encryption_key());

    // Add written fragment info
    RETURN_NOT_OK(add_written_fragment_info(frag_uri_.value()));

    // The following will make the fragment visible
    URI commit_uri =
        array_->array_directory().get_commit_uri(frag_uri_.value());
    throw_if_not_ok(resources_.vfs().touch(commit_uri));

    // Clear some data to prevent it from being serialized.
    cell_pos_.clear();
    coord_dups_.clear();
    frag_meta_ = nullptr;
  }

  is_coords_pass_ = false;
  return Status::Ok();
}

}  // namespace tiledb::sm
