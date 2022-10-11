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

#include "tiledb/sm/query/writers/writer_base.h"
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
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/sm/query/hilbert_order.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile_metadata_generator.h"
#include "tiledb/sm/tile/writer_tile.h"
#include "tiledb/storage_format/uri/generate_uri.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {

class WriterBaseStatusException : public StatusException {
 public:
  explicit WriterBaseStatusException(const std::string& message)
      : StatusException("WriterBase", message) {
  }
};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

WriterBase::WriterBase(
    stats::Stats* stats,
    shared_ptr<Logger> logger,
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout,
    std::vector<WrittenFragmentInfo>& written_fragment_info,
    bool disable_checks_consolidation,
    Query::CoordsInfo& coords_info,
    bool remote_query,
    optional<std::string> fragment_name,
    bool skip_checks_serialization)
    : StrategyBase(
          stats,
          logger->clone("Writer", ++logger_id_),
          storage_manager,
          array,
          config,
          buffers,
          subarray,
          layout)
    , disable_checks_consolidation_(disable_checks_consolidation)
    , coords_info_(coords_info)
    , check_coord_dups_(false)
    , check_coord_oob_(false)
    , check_global_order_(false)
    , dedup_coords_(false)
    , written_fragment_info_(written_fragment_info)
    , remote_query_(remote_query) {
  // Sanity checks
  if (storage_manager_ == nullptr) {
    throw WriterBaseStatusException(
        "Cannot initialize query; Storage manager not set");
  }

  if (!skip_checks_serialization && buffers_.empty()) {
    throw WriterBaseStatusException(
        "Cannot initialize writer; Buffers not set");
  }

  if (array_schema_.dense() &&
      (layout_ == Layout::ROW_MAJOR || layout_ == Layout::COL_MAJOR)) {
    for (const auto& b : buffers_) {
      if (array_schema_.is_dim(b.first)) {
        throw WriterBaseStatusException(
            "Cannot initialize writer; Sparse coordinates for dense arrays "
            "cannot be provided if the query layout is ROW_MAJOR or COL_MAJOR");
      }
    }
  }

  // Get configuration parameters
  const char *check_coord_dups, *check_coord_oob, *check_global_order;
  const char* dedup_coords;
  if (!config_.get("sm.check_coord_dups", &check_coord_dups).ok()) {
    throw WriterBaseStatusException("Cannot get setting");
  }

  if (!config_.get("sm.check_coord_oob", &check_coord_oob).ok()) {
    throw WriterBaseStatusException("Cannot get setting");
  }

  if (!config_.get("sm.check_global_order", &check_global_order).ok()) {
    throw WriterBaseStatusException("Cannot get setting");
  }

  if (!config_.get("sm.dedup_coords", &dedup_coords).ok()) {
    throw WriterBaseStatusException("Cannot get setting");
  }

  assert(check_coord_dups != nullptr && dedup_coords != nullptr);
  check_coord_dups_ =
      disable_checks_consolidation_ ? false : !strcmp(check_coord_dups, "true");
  check_coord_oob_ = !strcmp(check_coord_oob, "true");
  check_global_order_ = disable_checks_consolidation_ ?
                            false :
                            !strcmp(check_global_order, "true");
  dedup_coords_ = !strcmp(dedup_coords, "true");
  bool found = false;
  offsets_format_mode_ = config_.get("sm.var_offsets.mode", &found);
  assert(found);
  if (offsets_format_mode_ != "bytes" && offsets_format_mode_ != "elements") {
    throw WriterBaseStatusException(
        "Cannot initialize writer; Unsupported offsets format in "
        "configuration");
  }
  if (!config_
           .get<bool>(
               "sm.var_offsets.extra_element", &offsets_extra_element_, &found)
           .ok()) {
    throw WriterBaseStatusException("Cannot get setting");
  }
  assert(found);

  if (!config_
           .get<uint32_t>("sm.var_offsets.bitsize", &offsets_bitsize_, &found)
           .ok()) {
    throw WriterBaseStatusException("Cannot get setting");
  }
  assert(found);

  if (offsets_bitsize_ != 32 && offsets_bitsize_ != 64) {
    throw WriterBaseStatusException(
        "Cannot initialize writer; Unsupported offsets bitsize in "
        "configuration");
  }

  // Set a default subarray
  if (!subarray_.is_set()) {
    subarray_ = Subarray(array_, layout_, stats_, logger_);
  }

  if (offsets_extra_element_) {
    check_extra_element();
  }

  if (!skip_checks_serialization) {
    check_subarray();
    check_buffer_sizes();
  }

  optimize_layout_for_1D();
  check_var_attr_offsets();

  // Get the timestamp the array was opened and the array write version.
  uint64_t timestamp = array_->timestamp_end_opened_at();
  auto write_version = array_->array_schema_latest().write_version();

  // Set the fragment URI using either the provided fragment name or a generated
  // fragment name.
  auto new_fragment_str =
      fragment_name.has_value() ?
          fragment_name.value() :
          storage_format::generate_fragment_name(timestamp, write_version);
  auto frag_dir_uri =
      array_->array_directory().get_fragments_dir(write_version);
  fragment_uri_ = frag_dir_uri.join_path(new_fragment_str);
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

void WriterBase::check_var_attr_offsets() const {
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
    if (num_offsets == 0) {
      return;
    }

    uint64_t prev_offset = get_offset_buffer_element(buffer_off, 0);
    // Allow the initial offset to be equal to the size, this indicates
    // the first and only value in the buffer is to be empty
    if (prev_offset > *buffer_val_size) {
      throw WriterBaseStatusException(
          "Invalid offsets for attribute " + attr + "; offset " +
          std::to_string(prev_offset) + " specified for buffer of size " +
          std::to_string(*buffer_val_size));
    }

    for (uint64_t i = 1; i < num_offsets; i++) {
      uint64_t cur_offset = get_offset_buffer_element(buffer_off, i);
      if (cur_offset < prev_offset)
        throw WriterBaseStatusException(
            "Invalid offsets for attribute " + attr +
            "; offsets must be given in "
            "strictly ascending order.");

      // Allow the last offset(s) to be equal to the size, this indicates the
      // last value(s) are to be empty
      if (cur_offset > *buffer_val_size ||
          (cur_offset == *buffer_val_size &&
           get_offset_buffer_element(
               buffer_off, (i < num_offsets - 1 ? i + 1 : i)) !=
               *buffer_val_size))
        throw WriterBaseStatusException(
            "Invalid offsets for attribute " + attr + "; offset " +
            std::to_string(cur_offset) + " specified at index " +
            std::to_string(i) + " for buffer of size " +
            std::to_string(*buffer_val_size));

      prev_offset = cur_offset;
    }
  }
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
    const DomainBuffersView& domain_buffers,
    std::vector<uint64_t>& hilbert_values) const {
  auto dim_num = array_schema_.dim_num();
  Hilbert h(dim_num);
  auto bits = h.bits();
  auto max_bucket_val = ((uint64_t)1 << bits) - 1;

  // Calculate Hilbert values in parallel
  assert(hilbert_values.size() >= coords_info_.coords_num_);
  auto status = parallel_for(
      storage_manager_->compute_tp(),
      0,
      coords_info_.coords_num_,
      [&](uint64_t c) {
        std::vector<uint64_t> coords(dim_num);
        for (uint32_t d = 0; d < dim_num; ++d) {
          auto dim{array_schema_.dimension_ptr(d)};
          coords[d] = hilbert_order::map_to_uint64(
              *dim, domain_buffers[d], c, bits, max_bucket_val);
        }
        hilbert_values[c] = h.coords_to_hilbert(&coords[0]);

        return Status::Ok();
      });

  RETURN_NOT_OK_ELSE(status, logger_->status_no_return_value(status));

  return Status::Ok();
}

void WriterBase::check_buffer_sizes() const {
  // This is applicable only to dense arrays and ordered layout
  if (!array_schema_.dense() ||
      (layout_ != Layout::ROW_MAJOR && layout_ != Layout::COL_MAJOR)) {
    return;
  }

  auto cell_num = array_schema_.domain().cell_num(subarray_.ndrange(0));
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
        throw WriterBaseStatusException(ss.str());
      }
    } else {
      if (expected_cell_num != cell_num) {
        std::stringstream ss;
        ss << "Buffer sizes check failed; Invalid number of cells given for ";
        ss << "attribute '" << attr << "'";
        ss << " (" << expected_cell_num << " != " << cell_num << ")";
        throw WriterBaseStatusException(ss.str());
      }
    }
  }
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
  if (array_schema_.domain().all_dims_string())
    return Status::Ok();

  // Prepare auxiliary vectors for better performance
  auto dim_num = array_schema_.dim_num();
  std::vector<unsigned char*> buffs(dim_num);
  std::vector<uint64_t> coord_sizes(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name{array_schema_.dimension_ptr(d)->name()};
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
        auto dim{array_schema_.dimension_ptr(d)};
        if (datatype_is_string(dim->type()))
          return Status::Ok();
        return dim->oob(buffs[d] + c * coord_sizes[d]);
      });

  RETURN_NOT_OK(status);

  // Success
  return Status::Ok();
}

void WriterBase::check_subarray() const {
  if (array_schema_.dense()) {
    if (subarray_.range_num() != 1) {
      throw WriterBaseStatusException(
          "Multi-range dense writes are not supported");
    }

    if (layout_ == Layout::GLOBAL_ORDER && !subarray_.coincides_with_tiles()) {
      throw WriterBaseStatusException(
          "Cannot initialize query; In global writes for dense arrays, the "
          "subarray must coincide with the tile bounds");
    }
  }
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

Status WriterBase::close_files(shared_ptr<FragmentMetadata> meta) const {
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
    const std::unordered_map<std::string, WriterTileVector>& tiles,
    shared_ptr<FragmentMetadata> meta) const {
  auto timer_se = stats_->start_timer("compute_coord_meta");

  // Applicable only if there are coordinates
  if (!coords_info_.has_coords_)
    return Status::Ok();

  // Check if tiles are empty
  if (tiles.empty() || tiles.begin()->second.empty())
    return Status::Ok();

  // Compute number of tiles. Assumes all attributes and
  // and dimensions have the same number of tiles
  auto tile_num = tiles.begin()->second.size();
  auto dim_num = array_schema_.dim_num();

  // Compute MBRs
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, tile_num, [&](uint64_t i) {
        NDRange mbr(dim_num);
        std::vector<const void*> data(dim_num);
        for (unsigned d = 0; d < dim_num; ++d) {
          auto dim{array_schema_.dimension_ptr(d)};
          const auto& dim_name = dim->name();
          auto tiles_it = tiles.find(dim_name);
          assert(tiles_it != tiles.end());
          if (!dim->var_size())
            RETURN_NOT_OK(
                dim->compute_mbr(tiles_it->second[i].fixed_tile(), &mbr[d]));
          else
            RETURN_NOT_OK(dim->compute_mbr_var(
                tiles_it->second[i].offset_tile(),
                tiles_it->second[i].var_tile(),
                &mbr[d]));
        }

        RETURN_NOT_OK(meta->set_mbr(i, mbr));
        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  // Set last tile cell number
  auto dim_0{array_schema_.dimension_ptr(0)};
  const auto& dim_tiles = tiles.find(dim_0->name())->second;
  const auto& last_tile_pos = dim_tiles.size() - 1;
  auto cell_num = dim_tiles[last_tile_pos].cell_num();
  meta->set_last_tile_cell_num(cell_num);

  return Status::Ok();
}

Status WriterBase::compute_tiles_metadata(
    uint64_t tile_num,
    std::unordered_map<std::string, WriterTileVector>& tiles) const {
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
      const auto cell_size = array_schema_.cell_size(attr);
      const auto cell_val_num = array_schema_.cell_val_num(attr);
      for (auto& tile : attr_tiles) {
        TileMetadataGenerator md_generator(
            type, is_dim, var_size, cell_size, cell_val_num);
        md_generator.process_full_tile(tile);
        md_generator.set_tile_metadata(tile);
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
      const auto cell_size = array_schema_.cell_size(attr);
      const auto cell_val_num = array_schema_.cell_val_num(attr);
      auto st = parallel_for(compute_tp, 0, tile_num, [&](uint64_t t) {
        TileMetadataGenerator md_generator(
            type, is_dim, var_size, cell_size, cell_val_num);
        md_generator.process_full_tile(attr_tiles[t]);
        md_generator.set_tile_metadata(attr_tiles[t]);

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
    auto dim{array_schema_.dimension_ptr(d)};
    const auto& dim_name = dim->name();
    ss << buffers_.find(dim_name)->second.dimension_datum_at(*dim, i);
    if (d < dim_num - 1)
      ss << ", ";
  }
  ss << ")";

  return ss.str();
}

Status WriterBase::create_fragment(
    bool dense, shared_ptr<FragmentMetadata>& frag_meta) {
  // Get write version, timestamp array was opened,  and a reference to the
  // array directory.
  auto write_version = array_->array_schema_latest().write_version();
  uint64_t timestamp = array_->timestamp_end_opened_at();
  auto& array_dir = array_->array_directory();

  // Create the directories.
  // Create the fragment directory, the directory for the new fragment URI, and
  // the commit directory.
  throw_if_not_ok(storage_manager_->vfs()->create_dir(
      array_dir.get_fragments_dir(write_version)));
  throw_if_not_ok(storage_manager_->create_dir(fragment_uri_));
  throw_if_not_ok(storage_manager_->vfs()->create_dir(
      array_dir.get_commits_dir(write_version)));

  // Create fragment metadata.
  auto timestamp_range = std::pair<uint64_t, uint64_t>(timestamp, timestamp);
  const bool has_timestamps = buffers_.count(constants::timestamps) != 0;
  const bool has_delete_metadata =
      buffers_.count(constants::delete_timestamps) != 0;
  frag_meta = make_shared<FragmentMetadata>(
      HERE(),
      storage_manager_,
      nullptr,
      array_->array_schema_latest_ptr(),
      fragment_uri_,
      timestamp_range,
      dense,
      has_timestamps,
      has_delete_metadata);

  RETURN_NOT_OK((frag_meta)->init(subarray_.ndrange(0)));
  return Status::Ok();
}

Status WriterBase::filter_tiles(
    std::unordered_map<std::string, WriterTileVector>* tiles) {
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
    const std::string& name, WriterTileVector* tiles) {
  const bool var_size = array_schema_.var_size(name);
  const bool nullable = array_schema_.is_nullable(name);

  // Filter all tiles
  auto tile_num = tiles->size();

  // Process all tiles, minus offsets, they get processed separately.
  std::vector<std::tuple<Tile*, Tile*, bool, bool>> args;
  args.reserve(tile_num * (1 + nullable));
  for (auto& tile : *tiles) {
    if (var_size) {
      args.emplace_back(&tile.var_tile(), &tile.offset_tile(), false, false);
    } else {
      args.emplace_back(&tile.fixed_tile(), nullptr, false, false);
    }

    if (nullable) {
      args.emplace_back(&tile.validity_tile(), nullptr, false, true);
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
        storage_manager_->compute_tp(), 0, tiles->size(), [&](uint64_t i) {
          auto& tile = (*tiles)[i];
          RETURN_NOT_OK(
              filter_tile(name, &tile.offset_tile(), nullptr, true, false));
          return Status::Ok();
        });
    RETURN_NOT_OK(status);
  }

  return Status::Ok();
}

Status WriterBase::filter_tile(
    const std::string& name,
    Tile* const tile,
    Tile* const offsets_tile,
    const bool offsets,
    const bool nullable) {
  auto timer_se = stats_->start_timer("filter_tile");

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
    return Status::Ok();
  }

  // Append an encryption filter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  // Check if chunk or tile level filtering/unfiltering is appropriate
  bool use_chunking = filters.use_tile_chunking(
      array_schema_.var_size(name), array_schema_.version(), tile->type());

  assert(!tile->filtered());
  RETURN_NOT_OK(filters.run_forward(
      stats_,
      tile,
      offsets_tile,
      storage_manager_->compute_tp(),
      use_chunking));
  assert(tile->filtered());

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

Status WriterBase::init_tiles(
    const std::string& name, uint64_t tile_num, WriterTileVector* tiles) const {
  // Initialize tiles
  const bool var_size = array_schema_.var_size(name);
  const bool nullable = array_schema_.is_nullable(name);
  const uint64_t cell_size = array_schema_.cell_size(name);
  const auto type = array_schema_.type(name);
  tiles->reserve(tile_num);
  for (uint64_t i = 0; i < tile_num; i++) {
    tiles->emplace_back(WriterTile(
        array_schema_,
        coords_info_.has_coords_,
        var_size,
        nullable,
        cell_size,
        type));
  }

  return Status::Ok();
}

void WriterBase::optimize_layout_for_1D() {
  if (array_schema_.dim_num() == 1 && layout_ != Layout::GLOBAL_ORDER &&
      layout_ != Layout::UNORDERED)
    layout_ = array_schema_.cell_order();
}

void WriterBase::check_extra_element() {
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

    if (last_offset != max_offset) {
      throw WriterBaseStatusException(
          "Invalid offsets for attribute " + attr +
          "; the last offset: " + std::to_string(last_offset) +
          " is not equal to the size of the data buffer: " +
          std::to_string(max_offset));
    }
  }
}

Status WriterBase::split_coords_buffer() {
  auto timer_se = stats_->start_timer("split_coords_buff");

  // Do nothing if the coordinates buffer is not set
  if (coords_info_.coords_buffer_ == nullptr)
    return Status::Ok();

  // For easy reference
  auto dim_num = array_schema_.dim_num();
  auto coords_size{dim_num *
                   array_schema_.domain().dimension_ptr(0)->coord_size()};
  coords_info_.coords_num_ = *coords_info_.coords_buffer_size_ / coords_size;

  clear_coord_buffers();

  // New coord buffer allocations
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim{array_schema_.dimension_ptr(d)};
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
    auto coord_size{array_schema_.dimension_ptr(d)->coord_size()};
    const auto& dim_name{array_schema_.dimension_ptr(d)->name()};
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
    shared_ptr<FragmentMetadata> frag_meta,
    std::unordered_map<std::string, WriterTileVector>* const tiles) {
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

        uint64_t idx = 0;
        for (auto& tile : tiles) {
          frag_meta->set_tile_min_var(attr, idx, tile.min());
          frag_meta->set_tile_max_var(attr, idx, tile.max());
          idx++;
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
    shared_ptr<FragmentMetadata> frag_meta,
    uint64_t start_tile_id,
    WriterTileVector* const tiles,
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
    auto& tile = (*tiles)[i];
    auto& t = var_size ? tile.offset_tile() : tile.fixed_tile();
    RETURN_NOT_OK(storage_manager_->write(
        *uri, t.filtered_buffer().data(), t.filtered_buffer().size()));
    frag_meta->set_tile_offset(name, tile_id, t.filtered_buffer().size());
    auto null_count = tile.null_count();

    if (var_size) {
      auto& t_var = tile.var_tile();
      RETURN_NOT_OK(storage_manager_->write(
          *var_uri,
          t_var.filtered_buffer().data(),
          t_var.filtered_buffer().size()));
      frag_meta->set_tile_var_offset(
          name, tile_id, t_var.filtered_buffer().size());
      frag_meta->set_tile_var_size(name, tile_id, tile.var_pre_filtered_size());
      if (has_min_max_md && null_count != frag_meta->cell_num(tile_id)) {
        frag_meta->set_tile_min_var_size(name, tile_id, tile.min().size());
        frag_meta->set_tile_max_var_size(name, tile_id, tile.max().size());
      }
    } else {
      if (has_min_max_md && null_count != frag_meta->cell_num(tile_id)) {
        frag_meta->set_tile_min(name, tile_id, tile.min());
        frag_meta->set_tile_max(name, tile_id, tile.max());
      }

      if (has_sum_md) {
        frag_meta->set_tile_sum(name, tile_id, tile.sum());
      }
    }

    if (nullable) {
      auto& t_val = tile.validity_tile();
      RETURN_NOT_OK(storage_manager_->write(
          *validity_uri,
          t_val.filtered_buffer().data(),
          t_val.filtered_buffer().size()));
      frag_meta->set_tile_validity_offset(
          name, tile_id, t_val.filtered_buffer().size());
      frag_meta->set_tile_null_count(name, tile_id, null_count);
    }
  }

  // Close files or flush multipart upload buffers in case of global order
  // writes
  if (close_files) {
    std::vector<URI> closing_uris;
    auto&& [st1, uri] = frag_meta->uri(name);
    RETURN_NOT_OK(st1);
    closing_uris.push_back(*uri);

    if (var_size) {
      auto&& [st2, var_uri] = frag_meta->var_uri(name);
      RETURN_NOT_OK(st2);
      closing_uris.push_back(*var_uri);
    }
    if (nullable) {
      auto&& [st2, validity_uri] = frag_meta->validity_uri(name);
      RETURN_NOT_OK(st2);
      closing_uris.push_back(*validity_uri);
    }
    for (auto& u : closing_uris) {
      if (layout_ == Layout::GLOBAL_ORDER) {
        // Flushing the multipart buffers after each write stage is a
        // requirement of remote global order writes, it should only be
        // done if this code is executed as a result of a remote query
        if (remote_query()) {
          RETURN_NOT_OK(
              storage_manager_->vfs()->flush_multipart_file_buffer(u));
        }
      } else {
        RETURN_NOT_OK(storage_manager_->close_file(u));
      }
    }
  }

  return Status::Ok();
}

bool WriterBase::remote_query() const {
  return remote_query_;
}

}  // namespace sm
}  // namespace tiledb
