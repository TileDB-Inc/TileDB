/**
 * @file   writer_base.cc
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
 * This file implements class WriterBase.
 */

#include "tiledb/sm/query/writer_base.h"
#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/hilbert.h"
#include "tiledb/sm/misc/math.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/time.h"
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

WriterBase::WriterBase(
    stats::Stats* stats,
    tdb_shared_ptr<Logger> logger,
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout,
    std::vector<WrittenFragmentInfo>& written_fragment_info,
    bool disable_check_global_order,
    Query::CoordsInfo& coords_info,
    URI fragment_uri)
    : StrategyBase(
          stats,
          logger->clone("Writer", ++logger_id_),
          storage_manager,
          array,
          config,
          buffers,
          subarray,
          layout)
    , disable_check_global_order_(disable_check_global_order)
    , coords_info_(coords_info)
    , check_coord_dups_(false)
    , check_coord_oob_(false)
    , check_global_order_(false)
    , dedup_coords_(false)
    , initialized_(false)
    , written_fragment_info_(written_fragment_info) {
  fragment_uri_ = fragment_uri;
}

WriterBase::~WriterBase() {
  clear_coord_buffers();
}

/* ****************************** */
/*               API              */
/* ****************************** */

bool WriterBase::get_check_coord_dups() const {
  return check_coord_dups_;
}

bool WriterBase::get_check_coord_oob() const {
  return check_coord_oob_;
}

bool WriterBase::get_dedup_coords() const {
  return dedup_coords_;
}

void WriterBase::set_check_coord_dups(bool b) {
  check_coord_dups_ = b;
}

void WriterBase::set_check_coord_oob(bool b) {
  check_coord_oob_ = b;
}

void WriterBase::set_dedup_coords(bool b) {
  dedup_coords_ = b;
}

Status WriterBase::check_var_attr_offsets() const {
  for (const auto& it : buffers_) {
    const auto& attr = it.first;
    if (!array_schema_.var_size(attr)) {
      continue;
    }

    const void* buffer_off = it.second.buffer_;
    const uint64_t buffer_off_size =
        get_offset_buffer_size(*it.second.buffer_size_);
    const uint64_t* buffer_val_size = it.second.buffer_var_size_;
    auto num_offsets = buffer_off_size / constants::cell_var_offset_size;
    if (num_offsets == 0)
      return Status::Ok();

    uint64_t prev_offset = get_offset_buffer_element(buffer_off, 0);
    // Allow the initial offset to be equal to the size, this indicates
    // the first and only value in the buffer is to be empty
    if (prev_offset > *buffer_val_size)
      return logger_->status(Status_WriterError(
          "Invalid offsets for attribute " + attr + "; offset " +
          std::to_string(prev_offset) + " specified for buffer of size " +
          std::to_string(*buffer_val_size)));

    for (uint64_t i = 1; i < num_offsets; i++) {
      uint64_t cur_offset = get_offset_buffer_element(buffer_off, i);
      if (cur_offset < prev_offset)
        return logger_->status(Status_WriterError(
            "Invalid offsets for attribute " + attr +
            "; offsets must be given in "
            "strictly ascending order."));

      // Allow the last offset(s) to be equal to the size, this indicates the
      // last value(s) are to be empty
      if (cur_offset > *buffer_val_size ||
          (cur_offset == *buffer_val_size &&
           get_offset_buffer_element(
               buffer_off, (i < num_offsets - 1 ? i + 1 : i)) !=
               *buffer_val_size))
        return logger_->status(Status_WriterError(
            "Invalid offsets for attribute " + attr + "; offset " +
            std::to_string(cur_offset) + " specified at index " +
            std::to_string(i) + " for buffer of size " +
            std::to_string(*buffer_val_size)));

      prev_offset = cur_offset;
    }
  }

  return Status::Ok();
}

Status WriterBase::init() {
  // Sanity checks
  if (storage_manager_ == nullptr)
    return logger_->status(
        Status_WriterError("Cannot initialize query; Storage manager not set"));
  if (buffers_.empty())
    return logger_->status(
        Status_WriterError("Cannot initialize writer; Buffers not set"));
  if (array_schema_.dense() &&
      (layout_ == Layout::ROW_MAJOR || layout_ == Layout::COL_MAJOR)) {
    for (const auto& b : buffers_) {
      if (array_schema_.is_dim(b.first)) {
        return logger_->status(Status_WriterError(
            "Cannot initialize writer; Sparse coordinates "
            "for dense arrays cannot be provided "
            "if the query layout is ROW_MAJOR or COL_MAJOR"));
      }
    }
  }

  // Get configuration parameters
  const char *check_coord_dups, *check_coord_oob, *check_global_order;
  const char* dedup_coords;
  RETURN_NOT_OK(config_.get("sm.check_coord_dups", &check_coord_dups));
  RETURN_NOT_OK(config_.get("sm.check_coord_oob", &check_coord_oob));
  RETURN_NOT_OK(config_.get("sm.check_global_order", &check_global_order));
  RETURN_NOT_OK(config_.get("sm.dedup_coords", &dedup_coords));
  assert(check_coord_dups != nullptr && dedup_coords != nullptr);
  check_coord_dups_ = !strcmp(check_coord_dups, "true");
  check_coord_oob_ = !strcmp(check_coord_oob, "true");
  check_global_order_ =
      disable_check_global_order_ ? false : !strcmp(check_global_order, "true");
  dedup_coords_ = !strcmp(dedup_coords, "true");
  bool found = false;
  offsets_format_mode_ = config_.get("sm.var_offsets.mode", &found);
  assert(found);
  if (offsets_format_mode_ != "bytes" && offsets_format_mode_ != "elements") {
    return logger_->status(
        Status_WriterError("Cannot initialize writer; Unsupported offsets "
                           "format in configuration"));
  }
  RETURN_NOT_OK(config_.get<bool>(
      "sm.var_offsets.extra_element", &offsets_extra_element_, &found));
  assert(found);
  RETURN_NOT_OK(config_.get<uint32_t>(
      "sm.var_offsets.bitsize", &offsets_bitsize_, &found));
  if (offsets_bitsize_ != 32 && offsets_bitsize_ != 64) {
    return logger_->status(
        Status_WriterError("Cannot initialize writer; Unsupported offsets "
                           "bitsize in configuration"));
  }
  assert(found);

  // Set a default subarray
  if (!subarray_.is_set())
    subarray_ = Subarray(array_, layout_, stats_, logger_);

  if (offsets_extra_element_)
    RETURN_NOT_OK(check_extra_element());

  RETURN_NOT_OK(check_subarray());
  RETURN_NOT_OK(check_buffer_sizes());

  optimize_layout_for_1D();
  RETURN_NOT_OK(check_var_attr_offsets());
  initialized_ = true;

  return Status::Ok();
}

Status WriterBase::initialize_memory_budget() {
  return Status::Ok();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status WriterBase::add_written_fragment_info(const URI& uri) {
  std::pair<uint64_t, uint64_t> timestamp_range;
  RETURN_NOT_OK(utils::parse::get_timestamp_range(uri, &timestamp_range));
  written_fragment_info_.emplace_back(uri, timestamp_range);
  return Status::Ok();
}

Status WriterBase::calculate_hilbert_values(
    const std::vector<const QueryBuffer*>& buffs,
    std::vector<uint64_t>* hilbert_values) const {
  auto dim_num = array_schema_.dim_num();
  Hilbert h(dim_num);
  auto bits = h.bits();
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;

  // Calculate Hilbert values in parallel
  assert(hilbert_values->size() >= coords_info_.coords_num_);
  auto status = parallel_for(
      storage_manager_->compute_tp(),
      0,
      coords_info_.coords_num_,
      [&](uint64_t c) {
        std::vector<uint64_t> coords(dim_num);
        for (uint32_t d = 0; d < dim_num; ++d) {
          auto dim = array_schema_.dimension(d);
          coords[d] = hilbert_order::map_to_uint64(
              *dim, buffs[d], c, bits, max_bucket_val);
        }
        (*hilbert_values)[c] = h.coords_to_hilbert(&coords[0]);

        return Status::Ok();
      });

  RETURN_NOT_OK_ELSE(status, logger_->status(status));

  return Status::Ok();
}

Status WriterBase::check_buffer_sizes() const {
  // This is applicable only to dense arrays and ordered layout
  if (!array_schema_.dense() ||
      (layout_ != Layout::ROW_MAJOR && layout_ != Layout::COL_MAJOR))
    return Status::Ok();

  auto cell_num = array_schema_.domain()->cell_num(subarray_.ndrange(0));
  uint64_t expected_cell_num = 0;
  for (const auto& it : buffers_) {
    const auto& attr = it.first;
    const bool is_var = array_schema_.var_size(attr);
    const uint64_t buffer_size =
        is_var ? get_offset_buffer_size(*it.second.buffer_size_) :
                 *it.second.buffer_size_;
    if (is_var) {
      expected_cell_num = buffer_size / constants::cell_var_offset_size;
    } else {
      expected_cell_num = buffer_size / array_schema_.cell_size(attr);
    }

    if (array_schema_.is_nullable(attr)) {
      const uint64_t buffer_validity_size =
          *it.second.validity_vector_.buffer_size();
      const uint64_t expected_validity_num =
          buffer_validity_size / constants::cell_validity_size;

      if (expected_validity_num != cell_num) {
        std::stringstream ss;
        ss << "Buffer sizes check failed; Invalid number of validity cells "
              "given for ";
        ss << "attribute '" << attr << "'";
        ss << " (" << expected_validity_num << " != " << cell_num << ")";
        return logger_->status(Status_WriterError(ss.str()));
      }
    } else {
      if (expected_cell_num != cell_num) {
        std::stringstream ss;
        ss << "Buffer sizes check failed; Invalid number of cells given for ";
        ss << "attribute '" << attr << "'";
        ss << " (" << expected_cell_num << " != " << cell_num << ")";
        return logger_->status(Status_WriterError(ss.str()));
      }
    }
  }

  return Status::Ok();
}

Status WriterBase::check_coord_oob() const {
  auto timer_se = stats_->start_timer("check_coord_oob");

  // Applicable only to sparse writes - exit if coordinates do not exist
  if (!coords_info_.has_coords_)
    return Status::Ok();

  // Exit if there are no coordinates to write
  if (coords_info_.coords_num_ == 0)
    return Status::Ok();

  // Exit if all dimensions are strings
  if (array_schema_.domain()->all_dims_string())
    return Status::Ok();

  // Prepare auxiliary vectors for better performance
  auto dim_num = array_schema_.dim_num();
  std::vector<unsigned char*> buffs(dim_num);
  std::vector<uint64_t> coord_sizes(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_.dimension(d)->name();
    buffs[d] = (unsigned char*)buffers_.find(dim_name)->second.buffer_;
    coord_sizes[d] = array_schema_.cell_size(dim_name);
  }

  // Check if all coordinates fall in the domain in parallel
  auto status = parallel_for_2d(
      storage_manager_->compute_tp(),
      0,
      coords_info_.coords_num_,
      0,
      dim_num,
      [&](uint64_t c, unsigned d) {
        auto dim = array_schema_.dimension(d);
        if (datatype_is_string(dim->type()))
          return Status::Ok();
        return dim->oob(buffs[d] + c * coord_sizes[d]);
      });

  RETURN_NOT_OK(status);

  // Success
  return Status::Ok();
}

Status WriterBase::check_subarray() const {
  if (array_schema_.dense()) {
    if (subarray_.range_num() != 1)
      return LOG_STATUS(
          Status_WriterError("Multi-range dense writes "
                             "are not supported"));

    if (layout_ == Layout::GLOBAL_ORDER && !subarray_.coincides_with_tiles())
      return logger_->status(
          Status_WriterError("Cannot initialize query; In global writes for "
                             "dense arrays, the subarray "
                             "must coincide with the tile bounds"));
  }
  return Status::Ok();
}

void WriterBase::clear_coord_buffers() {
  // Applicable only if the coordinate buffers have been allocated by
  // TileDB, which happens only when the zipped coordinates buffer is set
  for (auto b : to_clean_)
    tdb_free(b);
  to_clean_.clear();
  coord_buffer_sizes_.clear();
}

std::vector<std::string> WriterBase::buffer_names() const {
  std::vector<std::string> ret;

  // Add to the buffer names the attributes, as well as the dimensions only if
  // coords_buffer_ has not been set
  for (const auto& it : buffers_) {
    if (!array_schema_.is_dim(it.first) || (!coords_info_.coords_buffer_))
      ret.push_back(it.first);
  }

  // Special zipped coordinates name
  if (coords_info_.coords_buffer_)
    ret.push_back(constants::coords);

  return ret;
}

Status WriterBase::close_files(tdb_shared_ptr<FragmentMetadata> meta) const {
  // Close attribute and dimension files
  const auto buffer_name = buffer_names();

  std::vector<URI> file_uris;
  file_uris.reserve(buffer_name.size() * 3);

  for (const auto& name : buffer_name) {
    auto&& [status, uri] = meta->uri(name);
    RETURN_NOT_OK(status);

    file_uris.emplace_back(*uri);
    if (array_schema_.var_size(name)) {
      auto&& [status, var_uri] = meta->var_uri(name);
      RETURN_NOT_OK(status);

      file_uris.emplace_back(*var_uri);
    }
    if (array_schema_.is_nullable(name)) {
      auto&& [status, validity_uri] = meta->validity_uri(name);
      RETURN_NOT_OK(status);

      file_uris.emplace_back(*validity_uri);
    }
  }

  auto status = parallel_for(
      storage_manager_->io_tp(), 0, file_uris.size(), [&](uint64_t i) {
        const auto& file_ur = file_uris[i];
        RETURN_NOT_OK(storage_manager_->close_file(file_ur));
        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status WriterBase::compute_coords_metadata(
    const std::unordered_map<std::string, std::vector<WriterTile>>& tiles,
    tdb_shared_ptr<FragmentMetadata> meta) const {
  auto timer_se = stats_->start_timer("compute_coord_meta");

  // Applicable only if there are coordinates
  if (!coords_info_.has_coords_)
    return Status::Ok();

  // Check if tiles are empty
  if (tiles.empty() || tiles.begin()->second.empty())
    return Status::Ok();

  // Compute number of tiles. Assumes all attributes and
  // and dimensions have the same number of tiles
  auto it = tiles.begin();
  const uint64_t t = 1 + (array_schema_.var_size(it->first) ? 1 : 0) +
                     (array_schema_.is_nullable(it->first) ? 1 : 0);
  auto tile_num = it->second.size() / t;
  auto dim_num = array_schema_.dim_num();

  // Compute MBRs
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, tile_num, [&](uint64_t i) {
        NDRange mbr(dim_num);
        std::vector<const void*> data(dim_num);
        for (unsigned d = 0; d < dim_num; ++d) {
          auto dim = array_schema_.dimension(d);
          const auto& dim_name = dim->name();
          auto tiles_it = tiles.find(dim_name);
          assert(tiles_it != tiles.end());
          if (!dim->var_size())
            dim->compute_mbr(tiles_it->second[i], &mbr[d]);
          else
            dim->compute_mbr_var(
                tiles_it->second[2 * i], tiles_it->second[2 * i + 1], &mbr[d]);
        }

        meta->set_mbr(i, mbr);
        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  // Set last tile cell number
  auto dim_0 = array_schema_.dimension(0);
  const auto& dim_tiles = tiles.find(dim_0->name())->second;
  const auto& last_tile_pos =
      (!dim_0->var_size()) ? dim_tiles.size() - 1 : dim_tiles.size() - 2;
  meta->set_last_tile_cell_num(dim_tiles[last_tile_pos].cell_num());

  return Status::Ok();
}

Status WriterBase::compute_tiles_metadata(
    uint64_t tile_num,
    std::unordered_map<std::string, std::vector<WriterTile>>& tiles) const {
  auto attr_num = buffers_.size();
  auto compute_tp = storage_manager_->compute_tp();

  // Parallelize over attributes?
  if (attr_num > tile_num) {
    auto st = parallel_for(compute_tp, 0, attr_num, [&](uint64_t i) {
      auto buff_it = buffers_.begin();
      std::advance(buff_it, i);
      const auto& attr = buff_it->first;
      auto& attr_tiles = tiles[attr];
      const auto type = array_schema_.type(attr);
      const auto is_dim = array_schema_.is_dim(attr);
      const auto var_size = array_schema_.var_size(attr);
      const auto nullable = array_schema_.is_nullable(attr);
      const auto cell_size = array_schema_.cell_size(attr);
      const auto cell_val_num = array_schema_.cell_val_num(attr);
      const uint64_t tile_num_mult = 1 + var_size + nullable;
      TileMetadataGenerator md_generator(
          type, is_dim, var_size, cell_size, cell_val_num);
      for (uint64_t t = 0; t < tile_num; t++) {
        auto tile = &attr_tiles[t * tile_num_mult];
        md_generator.process_tile(
            tile,
            var_size ? &attr_tiles[t * tile_num_mult + 1] : nullptr,
            nullable ? &attr_tiles[t * tile_num_mult + var_size + 1] : nullptr);
        tile->set_metadata(md_generator.metadata());
      }

      return Status::Ok();
    });
    RETURN_NOT_OK(st);
  } else {  // Parallelize over tiles
    for (const auto& buff : buffers_) {
      const auto& attr = buff.first;
      auto& attr_tiles = tiles[attr];
      const auto type = array_schema_.type(attr);
      const auto is_dim = array_schema_.is_dim(attr);
      const auto var_size = array_schema_.var_size(attr);
      const auto nullable = array_schema_.is_nullable(attr);
      const auto cell_size = array_schema_.cell_size(attr);
      const auto cell_val_num = array_schema_.cell_val_num(attr);
      const uint64_t tile_num_mult = 1 + var_size + nullable;
      auto st = parallel_for(compute_tp, 0, tile_num, [&](uint64_t t) {
        TileMetadataGenerator md_generator(
            type, is_dim, var_size, cell_size, cell_val_num);
        auto tile = &attr_tiles[t * tile_num_mult];
        md_generator.process_tile(
            tile,
            var_size ? &attr_tiles[t * tile_num_mult + 1] : nullptr,
            nullable ? &attr_tiles[t * tile_num_mult + var_size + 1] : nullptr);
        tile->set_metadata(md_generator.metadata());

        return Status::Ok();
      });
      RETURN_NOT_OK(st);
    }
  }

  return Status::Ok();
}

std::string WriterBase::coords_to_str(uint64_t i) const {
  std::stringstream ss;
  auto dim_num = array_schema_.dim_num();

  ss << "(";
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = array_schema_.dimension(d);
    const auto& dim_name = dim->name();
    ss << buffers_.find(dim_name)->second.dimension_datum_at(*dim, i);
    if (d < dim_num - 1)
      ss << ", ";
  }
  ss << ")";

  return ss.str();
}

Status WriterBase::create_fragment(
    bool dense, tdb_shared_ptr<FragmentMetadata>& frag_meta) const {
  URI uri;
  uint64_t timestamp = array_->timestamp_end_opened_at();
  if (!fragment_uri_.to_string().empty()) {
    uri = fragment_uri_;
  } else {
    std::string new_fragment_str;
    auto write_version = array_->array_schema_latest().write_version();
    RETURN_NOT_OK(
        new_fragment_name(timestamp, write_version, &new_fragment_str));

    auto& array_dir = array_->array_directory();
    auto frag_uri = array_dir.get_fragments_dir(write_version);
    RETURN_NOT_OK(storage_manager_->vfs()->create_dir(frag_uri));
    auto commit_uri = array_dir.get_commits_dir(write_version);
    RETURN_NOT_OK(storage_manager_->vfs()->create_dir(commit_uri));

    uri = frag_uri.join_path(new_fragment_str);
  }
  auto timestamp_range = std::pair<uint64_t, uint64_t>(timestamp, timestamp);
  frag_meta = tdb::make_shared<FragmentMetadata>(
      HERE(),
      storage_manager_,
      nullptr,
      array_->array_schema_latest_ptr(),
      uri,
      timestamp_range,
      dense);

  RETURN_NOT_OK((frag_meta)->init(subarray_.ndrange(0)));
  return storage_manager_->create_dir(uri);
}

Status WriterBase::filter_tiles(
    std::unordered_map<std::string, std::vector<WriterTile>>* tiles) {
  auto timer_se = stats_->start_timer("filter_tiles");

  // Coordinates
  auto num = buffers_.size();
  auto status =
      parallel_for(storage_manager_->compute_tp(), 0, num, [&](uint64_t i) {
        auto buff_it = buffers_.begin();
        std::advance(buff_it, i);
        const auto& name = buff_it->first;
        RETURN_CANCEL_OR_ERROR(filter_tiles(name, &((*tiles)[name])));
        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status WriterBase::filter_tiles(
    const std::string& name, std::vector<WriterTile>* tiles) {
  const bool var_size = array_schema_.var_size(name);
  const bool nullable = array_schema_.is_nullable(name);
  const size_t tile_step = 1 + nullable + var_size;

  // Filter all tiles
  auto tile_num = tiles->size();

  // Make sure we have the correct number of tiles.
  if (tile_num % tile_step != 0) {
    return logger_->status(
        Status_WriterError("Incorrect number of tiles in filter_tiles"));
  }

  // Reserve a vector for offsets tiles, they need to be processed after var
  // data tiles as the processing of var data tiles depends on offset tiles.
  std::vector<std::tuple<WriterTile*, WriterTile*, bool, bool>> args;
  std::vector<std::tuple<WriterTile*, WriterTile*, bool, bool>> args_offsets;
  if (var_size) {
    args_offsets.reserve(tile_num / tile_step);
    args.reserve(tile_num - tile_num / tile_step);
  } else {
    args.reserve(tile_num);
  }

  for (size_t tile_idx = 0; tile_idx < tile_num; tile_idx += tile_step) {
    if (var_size) {
      args_offsets.emplace_back(&(*tiles)[tile_idx], nullptr, true, false);
      args.emplace_back(
          &(*tiles)[tile_idx + 1], &(*tiles)[tile_idx], false, false);
    } else {
      args.emplace_back(&(*tiles)[tile_idx], nullptr, false, false);
    }

    if (nullable) {
      args.emplace_back(
          &(*tiles)[tile_idx + var_size + 1], nullptr, false, true);
    }
  }

  // For fixed size, process everything, for var size, everything minus offsets.
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, args.size(), [&](uint64_t i) {
        const auto& [tile, offset_tile, contains_offsets, is_nullable] =
            args[i];
        RETURN_NOT_OK(filter_tile(
            name, tile, offset_tile, contains_offsets, is_nullable));
        return Status::Ok();
      });
  RETURN_NOT_OK(status);

  // Process offsets for var size.
  if (var_size) {
    auto status = parallel_for(
        storage_manager_->compute_tp(),
        0,
        args_offsets.size(),
        [&](uint64_t i) {
          const auto& [tile, offset_tile, contains_offsets, is_nullable] =
              args_offsets[i];
          RETURN_NOT_OK(filter_tile(
              name, tile, offset_tile, contains_offsets, is_nullable));
          return Status::Ok();
        });
    RETURN_NOT_OK(status);
  }

  return Status::Ok();
}

Status WriterBase::filter_tile(
    const std::string& name,
    WriterTile* const tile,
    WriterTile* const offsets_tile,
    const bool offsets,
    const bool nullable) {
  auto timer_se = stats_->start_timer("filter_tile");

  const auto orig_size = tile->size();

  // Get a copy of the appropriate filter pipeline.
  FilterPipeline filters;
  if (offsets) {
    assert(!nullable);
    filters = array_schema_.cell_var_offsets_filters();
  } else if (nullable) {
    filters = array_schema_.cell_validity_filters();
  } else {
    filters = array_schema_.filters(name);
  }

  // If those offsets belong to a var-sized string dimension/attribute then
  // don't filter the offsets as the information will be included in, and can be
  // reconstructed from, the filtered data tile.
  if (offsets && array_schema_.filters(name).skip_offsets_filtering(
                     array_schema_.type(name))) {
    tile->filtered_buffer().expand(sizeof(uint64_t));
    uint64_t nchunks = 0;
    memcpy(tile->filtered_buffer().data(), &nchunks, sizeof(uint64_t));
    tile->clear_data();
    tile->set_pre_filtered_size(orig_size);
    return Status::Ok();
  }

  // Append an encryption filter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  // Check if chunk or tile level filtering/unfiltering is appropriate
  bool use_chunking =
      filters.use_tile_chunking(array_schema_.var_size(name), tile->type());

  assert(!tile->filtered());
  RETURN_NOT_OK(filters.run_forward(
      stats_,
      tile,
      offsets_tile,
      storage_manager_->compute_tp(),
      use_chunking));
  assert(tile->filtered());

  tile->set_pre_filtered_size(orig_size);

  return Status::Ok();
}

bool WriterBase::has_min_max_metadata(
    const std::string& name, const bool var_size) {
  const auto type = array_schema_.type(name);
  const auto is_dim = array_schema_.is_dim(name);
  const auto cell_val_num = array_schema_.cell_val_num(name);
  return TileMetadataGenerator::has_min_max_metadata(
      type, is_dim, var_size, cell_val_num);
}

bool WriterBase::has_sum_metadata(
    const std::string& name, const bool var_size) {
  const auto type = array_schema_.type(name);
  const auto cell_val_num = array_schema_.cell_val_num(name);
  return TileMetadataGenerator::has_sum_metadata(type, var_size, cell_val_num);
}

Status WriterBase::init_tile(const std::string& name, WriterTile* tile) const {
  // For easy reference
  auto cell_size = array_schema_.cell_size(name);
  auto type = array_schema_.type(name);
  auto domain = array_schema_.domain();
  auto capacity = array_schema_.capacity();
  auto cell_num_per_tile =
      coords_info_.has_coords_ ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * cell_size;

  // Initialize
  RETURN_NOT_OK(tile->init_unfiltered(
      array_schema_.write_version(), type, tile_size, cell_size, 0));

  return Status::Ok();
}

Status WriterBase::init_tile(
    const std::string& name, WriterTile* tile, WriterTile* tile_var) const {
  // For easy reference
  auto type = array_schema_.type(name);
  auto domain = array_schema_.domain();
  auto capacity = array_schema_.capacity();
  auto cell_num_per_tile =
      coords_info_.has_coords_ ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * constants::cell_var_offset_size;

  // Initialize
  RETURN_NOT_OK(tile->init_unfiltered(
      array_schema_.write_version(),
      constants::cell_var_offset_type,
      tile_size,
      constants::cell_var_offset_size,
      0));
  RETURN_NOT_OK(tile_var->init_unfiltered(
      array_schema_.write_version(), type, tile_size, datatype_size(type), 0));
  return Status::Ok();
}

Status WriterBase::init_tile_nullable(
    const std::string& name,
    WriterTile* tile,
    WriterTile* tile_validity) const {
  // For easy reference
  auto cell_size = array_schema_.cell_size(name);
  auto type = array_schema_.type(name);
  auto domain = array_schema_.domain();
  auto capacity = array_schema_.capacity();
  auto cell_num_per_tile =
      coords_info_.has_coords_ ? capacity : domain->cell_num_per_tile();

  // Initialize
  RETURN_NOT_OK(tile->init_unfiltered(
      array_schema_.write_version(),
      type,
      cell_num_per_tile * cell_size,
      cell_size,
      0));
  RETURN_NOT_OK(tile_validity->init_unfiltered(
      array_schema_.write_version(),
      constants::cell_validity_type,
      cell_num_per_tile * constants::cell_validity_size,
      constants::cell_validity_size,
      0));

  return Status::Ok();
}

Status WriterBase::init_tile_nullable(
    const std::string& name,
    WriterTile* tile,
    WriterTile* tile_var,
    WriterTile* tile_validity) const {
  // For easy reference
  auto type = array_schema_.type(name);
  auto domain = array_schema_.domain();
  auto capacity = array_schema_.capacity();
  auto cell_num_per_tile =
      coords_info_.has_coords_ ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * constants::cell_var_offset_size;

  // Initialize
  RETURN_NOT_OK(tile->init_unfiltered(
      array_schema_.write_version(),
      constants::cell_var_offset_type,
      tile_size,
      constants::cell_var_offset_size,
      0));
  RETURN_NOT_OK(tile_var->init_unfiltered(
      array_schema_.write_version(), type, tile_size, datatype_size(type), 0));
  RETURN_NOT_OK(tile_validity->init_unfiltered(
      array_schema_.write_version(),
      constants::cell_validity_type,
      cell_num_per_tile * constants::cell_validity_size,
      constants::cell_validity_size,
      0));

  return Status::Ok();
}

Status WriterBase::init_tiles(
    const std::string& name,
    uint64_t tile_num,
    std::vector<WriterTile>* tiles) const {
  // Initialize tiles
  const bool var_size = array_schema_.var_size(name);
  const bool nullable = array_schema_.is_nullable(name);
  const size_t t =
      1 + static_cast<size_t>(var_size) + static_cast<size_t>(nullable);
  const size_t tiles_len = t * tile_num;
  tiles->resize(tiles_len);
  for (size_t i = 0; i < tiles_len; i += t) {
    if (!var_size) {
      if (nullable)
        RETURN_NOT_OK(
            init_tile_nullable(name, &((*tiles)[i]), &((*tiles)[i + 1])));
      else
        RETURN_NOT_OK(init_tile(name, &((*tiles)[i])));
    } else {
      if (nullable)
        RETURN_NOT_OK(init_tile_nullable(
            name, &((*tiles)[i]), &((*tiles)[i + 1]), &((*tiles)[i + 2])));
      else
        RETURN_NOT_OK(init_tile(name, &((*tiles)[i]), &((*tiles)[i + 1])));
    }
  }

  return Status::Ok();
}

Status WriterBase::new_fragment_name(
    uint64_t timestamp, uint32_t format_version, std::string* frag_uri) const {
  timestamp = (timestamp != 0) ? timestamp : utils::time::timestamp_now_ms();

  if (frag_uri == nullptr)
    return Status_WriterError("Null fragment uri argument.");
  std::string uuid;
  frag_uri->clear();
  RETURN_NOT_OK(uuid::generate_uuid(&uuid, false));
  std::stringstream ss;
  ss << "/__" << timestamp << "_" << timestamp << "_" << uuid << "_"
     << format_version;

  *frag_uri = ss.str();
  return Status::Ok();
}

void WriterBase::optimize_layout_for_1D() {
  if (array_schema_.dim_num() == 1 && layout_ != Layout::GLOBAL_ORDER &&
      layout_ != Layout::UNORDERED)
    layout_ = array_schema_.cell_order();
}

Status WriterBase::check_extra_element() {
  for (const auto& it : buffers_) {
    const auto& attr = it.first;
    if (!array_schema_.var_size(attr) || array_schema_.is_dim(attr))
      continue;

    const void* buffer_off = it.second.buffer_;
    uint64_t* buffer_off_size = it.second.buffer_size_;
    const auto num_offsets = *buffer_off_size / constants::cell_var_offset_size;
    const uint64_t* buffer_val_size = it.second.buffer_var_size_;
    const uint64_t attr_datatype_size = datatype_size(array_schema_.type(attr));
    const uint64_t max_offset = offsets_format_mode_ == "bytes" ?
                                    *buffer_val_size :
                                    *buffer_val_size / attr_datatype_size;
    const uint64_t last_offset =
        get_offset_buffer_element(buffer_off, num_offsets - 1);

    if (last_offset != max_offset)
      return logger_->status(Status_WriterError(
          "Invalid offsets for attribute " + attr +
          "; the last offset: " + std::to_string(last_offset) +
          " is not equal to the size of the data buffer: " +
          std::to_string(max_offset)));
  }

  return Status::Ok();
}

Status WriterBase::split_coords_buffer() {
  auto timer_se = stats_->start_timer("split_coords_buff");

  // Do nothing if the coordinates buffer is not set
  if (coords_info_.coords_buffer_ == nullptr)
    return Status::Ok();

  // For easy reference
  auto dim_num = array_schema_.dim_num();
  auto coord_size = array_schema_.domain()->dimension(0)->coord_size();
  auto coords_size = dim_num * coord_size;
  coords_info_.coords_num_ = *coords_info_.coords_buffer_size_ / coords_size;

  clear_coord_buffers();

  // New coord buffer allocations
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = array_schema_.dimension(d);
    const auto& dim_name = dim->name();
    auto coord_buffer_size = coords_info_.coords_num_ * dim->coord_size();
    auto it = coord_buffer_sizes_.emplace(dim_name, coord_buffer_size);
    QueryBuffer buff;
    buff.buffer_size_ = &(it.first->second);
    buff.buffer_ = tdb_malloc(coord_buffer_size);
    to_clean_.push_back(buff.buffer_);
    if (buff.buffer_ == nullptr)
      RETURN_NOT_OK(Status_WriterError(
          "Cannot split coordinate buffers; memory allocation failed"));
    buffers_[dim_name] = std::move(buff);
  }

  // Split coordinates
  auto coord = (unsigned char*)nullptr;
  for (unsigned d = 0; d < dim_num; ++d) {
    auto coord_size = array_schema_.dimension(d)->coord_size();
    const auto& dim_name = array_schema_.dimension(d)->name();
    auto buff = (unsigned char*)(buffers_[dim_name].buffer_);
    for (uint64_t c = 0; c < coords_info_.coords_num_; ++c) {
      coord = &(((unsigned char*)coords_info_
                     .coords_buffer_)[c * coords_size + d * coord_size]);
      std::memcpy(&(buff[c * coord_size]), coord, coord_size);
    }
  }

  return Status::Ok();
}

Status WriterBase::write_all_tiles(
    tdb_shared_ptr<FragmentMetadata> frag_meta,
    std::unordered_map<std::string, std::vector<WriterTile>>* const tiles) {
  auto timer_se = stats_->start_timer("tiles");

  assert(!tiles->empty());

  std::vector<ThreadPool::Task> tasks;
  for (auto& it : *tiles) {
    tasks.push_back(storage_manager_->io_tp()->execute([&, this]() {
      auto& attr = it.first;
      auto& tiles = it.second;
      RETURN_CANCEL_OR_ERROR(write_tiles(attr, frag_meta, 0, &tiles));

      // Fix var size attributes metadata.
      const auto var_size = array_schema_.var_size(attr);
      if (has_min_max_metadata(attr, var_size) &&
          array_schema_.var_size(attr)) {
        frag_meta->convert_tile_min_max_var_sizes_to_offsets(attr);

        const auto nullable = array_schema_.is_nullable(attr);
        const uint64_t tile_num_mult = 1 + var_size + nullable;
        for (uint64_t i = 0; i < tiles.size(); i += tile_num_mult) {
          auto tile_idx = i / tile_num_mult;
          frag_meta->set_tile_min_var(attr, tile_idx, tiles[i].min());
          frag_meta->set_tile_max_var(attr, tile_idx, tiles[i].max());
        }
      }
      return Status::Ok();
    }));
  }

  // Wait for writes and check all statuses
  auto statuses = storage_manager_->io_tp()->wait_all_status(tasks);
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();
}

Status WriterBase::write_tiles(
    const std::string& name,
    tdb_shared_ptr<FragmentMetadata> frag_meta,
    uint64_t start_tile_id,
    std::vector<WriterTile>* const tiles,
    bool close_files) {
  auto timer_se = stats_->start_timer("tiles");

  // Handle zero tiles
  if (tiles->empty())
    return Status::Ok();

  // For easy reference
  const bool var_size = array_schema_.var_size(name);
  const bool nullable = array_schema_.is_nullable(name);
  auto&& [status, uri] = frag_meta->uri(name);
  RETURN_NOT_OK(status);

  Status st;
  optional<URI> var_uri;
  if (!var_size)
    var_uri = URI("");
  else {
    tie(st, var_uri) = frag_meta->var_uri(name);
    RETURN_NOT_OK(st);
  }

  optional<URI> validity_uri;
  if (!nullable)
    validity_uri = URI("");
  else {
    tie(st, validity_uri) = frag_meta->validity_uri(name);
    RETURN_NOT_OK(st);
  }

  // Compute and set var buffer sizes for the min/max metadata
  const auto has_min_max_md = has_min_max_metadata(name, var_size);
  const auto has_sum_md = has_sum_metadata(name, var_size);
  auto tile_num = tiles->size();

  // Write tiles
  for (size_t i = 0, tile_id = start_tile_id; i < tile_num; ++i, ++tile_id) {
    WriterTile* tile = &(*tiles)[i];
    RETURN_NOT_OK(storage_manager_->write(
        *uri, tile->filtered_buffer().data(), tile->filtered_buffer().size()));
    frag_meta->set_tile_offset(name, tile_id, tile->filtered_buffer().size());

    auto&& [min, min_size, max, max_size, sum, null_count] = tile->metadata();
    if (var_size) {
      ++i;

      tile = &(*tiles)[i];
      RETURN_NOT_OK(storage_manager_->write(
          *var_uri,
          tile->filtered_buffer().data(),
          tile->filtered_buffer().size()));
      frag_meta->set_tile_var_offset(
          name, tile_id, tile->filtered_buffer().size());
      frag_meta->set_tile_var_size(name, tile_id, tile->pre_filtered_size());
      if (has_min_max_md && null_count != frag_meta->cell_num(tile_id)) {
        frag_meta->set_tile_min_var_size(name, tile_id, min_size);
        frag_meta->set_tile_max_var_size(name, tile_id, max_size);
      }
    } else {
      if (has_min_max_md && null_count != frag_meta->cell_num(tile_id)) {
        frag_meta->set_tile_min(name, tile_id, min, min_size);
        frag_meta->set_tile_max(name, tile_id, max, max_size);
      }

      if (has_sum_md) {
        frag_meta->set_tile_sum(name, tile_id, sum);
      }
    }

    if (nullable) {
      ++i;

      tile = &(*tiles)[i];
      RETURN_NOT_OK(storage_manager_->write(
          *validity_uri,
          tile->filtered_buffer().data(),
          tile->filtered_buffer().size()));
      frag_meta->set_tile_validity_offset(
          name, tile_id, tile->filtered_buffer().size());
      frag_meta->set_tile_null_count(name, tile_id, null_count);
    }
  }

  // Close files, except in the case of global order
  if (close_files && layout_ != Layout::GLOBAL_ORDER) {
    auto&& [st1, uri] = frag_meta->uri(name);
    RETURN_NOT_OK(st1);

    RETURN_NOT_OK(storage_manager_->close_file(*uri));
    if (var_size) {
      auto&& [st2, var_uri] = frag_meta->var_uri(name);
      RETURN_NOT_OK(st2);
      RETURN_NOT_OK(storage_manager_->close_file(*var_uri));
    }
    if (nullable) {
      auto&& [st2, validity_uri] = frag_meta->validity_uri(name);
      RETURN_NOT_OK(st2);
      RETURN_NOT_OK(storage_manager_->close_file(*validity_uri));
    }
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
