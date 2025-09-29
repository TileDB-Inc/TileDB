/**
 * @file   writer_base.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB, Inc.
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
#include "tiledb/common/assert.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/webp_filter.h"
#include "tiledb/sm/fragment/fragment_identifier.h"
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
#include "tiledb/storage_format/uri/generate_uri.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb::sm {

class WriterBaseException : public StatusException {
 public:
  explicit WriterBaseException(const std::string& message)
      : StatusException("WriterBase", message) {
  }
};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

WriterBase::WriterBase(
    stats::Stats* stats,
    shared_ptr<Logger> logger,
    StrategyParams& params,
    std::vector<WrittenFragmentInfo>& written_fragment_info,
    bool disable_checks_consolidation,
    Query::CoordsInfo& coords_info,
    bool remote_query,
    optional<std::string> fragment_name)
    : StrategyBase(stats, logger->clone("Writer", ++logger_id_), params)
    , disable_checks_consolidation_(disable_checks_consolidation)
    , coords_info_(coords_info)
    , check_coord_dups_(false)
    , check_coord_oob_(false)
    , check_global_order_(false)
    , dedup_coords_(false)
    , written_fragment_info_(written_fragment_info)
    , remote_query_(remote_query) {
  // Sanity checks
  if (!params.skip_checks_serialization() && buffers_.empty()) {
    throw WriterBaseException("Cannot initialize writer; Buffers not set");
  }

  if (array_schema_.dense() &&
      (layout_ == Layout::ROW_MAJOR || layout_ == Layout::COL_MAJOR)) {
    for (const auto& b : buffers_) {
      if (array_schema_.is_dim(b.first)) {
        throw WriterBaseException(
            "Cannot initialize writer; Sparse coordinates for dense arrays "
            "cannot be provided if the query layout is ROW_MAJOR or COL_MAJOR");
      }
    }
  }

  // Get configuration parameters
  const char *check_coord_dups, *check_coord_oob, *check_global_order;
  const char* dedup_coords;
  if (!config_.get("sm.check_coord_dups", &check_coord_dups).ok()) {
    throw WriterBaseException("Cannot get setting");
  }

  if (!config_.get("sm.check_coord_oob", &check_coord_oob).ok()) {
    throw WriterBaseException("Cannot get setting");
  }

  if (!config_.get("sm.check_global_order", &check_global_order).ok()) {
    throw WriterBaseException("Cannot get setting");
  }

  if (!config_.get("sm.dedup_coords", &dedup_coords).ok()) {
    throw WriterBaseException("Cannot get setting");
  }

  iassert(check_coord_dups != nullptr && dedup_coords != nullptr);
  check_coord_dups_ =
      disable_checks_consolidation_ ? false : !strcmp(check_coord_dups, "true");
  check_coord_oob_ = !strcmp(check_coord_oob, "true");
  check_global_order_ = disable_checks_consolidation_ ?
                            false :
                            !strcmp(check_global_order, "true");
  dedup_coords_ = !strcmp(dedup_coords, "true");
  offsets_format_mode_ =
      config_.get<std::string>("sm.var_offsets.mode", Config::must_find);
  if (offsets_format_mode_ != "bytes" && offsets_format_mode_ != "elements") {
    throw WriterBaseException(
        "Cannot initialize writer; Unsupported offsets format in "
        "configuration");
  }
  offsets_extra_element_ =
      config_.get<bool>("sm.var_offsets.extra_element", Config::must_find);

  offsets_bitsize_ =
      config_.get<uint32_t>("sm.var_offsets.bitsize", Config::must_find);

  if (offsets_bitsize_ != 32 && offsets_bitsize_ != 64) {
    throw WriterBaseException(
        "Cannot initialize writer; Unsupported offsets bitsize in "
        "configuration");
  }

  // Check subarray is valid for strategy is set or set it to default if unset.
  if (subarray_.is_set()) {
    if (!array_schema_.dense()) {
      throw WriterBaseException(
          "Cannot initialize write; Non-default subarray are not supported in "
          "sparse writes");
    }
    if (subarray_.range_num() > 1) {
      throw WriterBaseException(
          "Cannot initialize writer; Multi-range dense writes are not "
          "supported");
    }
  } else {
    subarray_ = Subarray(array_, layout_, stats_, logger_);
  }

  if (offsets_extra_element_) {
    check_extra_element();
  }

  if (!params.skip_checks_serialization()) {
    // Consolidation might set a subarray that is not tile aligned.
    if (!disable_checks_consolidation) {
      check_subarray();
    }
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
          storage_format::generate_timestamped_name(timestamp, write_version);
  auto frag_dir_uri =
      array_->array_directory().get_fragments_dir(write_version);
  fragment_uri_ = frag_dir_uri.join_path(new_fragment_str);
  FragmentID fragment_id{fragment_uri_};
  fragment_timestamp_range_ = fragment_id.timestamp_range();
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
      throw WriterBaseException(
          "Invalid offsets for attribute " + attr + "; offset " +
          std::to_string(prev_offset) + " specified for buffer of size " +
          std::to_string(*buffer_val_size));
    }

    for (uint64_t i = 1; i < num_offsets; i++) {
      uint64_t cur_offset = get_offset_buffer_element(buffer_off, i);
      if (cur_offset < prev_offset)
        throw WriterBaseException(
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
        throw WriterBaseException(
            "Invalid offsets for attribute " + attr + "; offset " +
            std::to_string(cur_offset) + " specified at index " +
            std::to_string(i) + " for buffer of size " +
            std::to_string(*buffer_val_size));

      prev_offset = cur_offset;
    }
  }
}

void WriterBase::refresh_config() {
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

shared_ptr<FragmentMetadata> WriterBase::create_fragment_metadata() {
  return make_shared<FragmentMetadata>(
      HERE(),
      &resources_,
      query_memory_tracker_,
      array_->array_schema_latest().write_version());
}

Status WriterBase::add_written_fragment_info(const URI& uri) {
  written_fragment_info_.emplace_back(uri, fragment_timestamp_range_);
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
  iassert(hilbert_values.size() >= coords_info_.coords_num_);
  auto status = parallel_for(
      &resources_.compute_tp(), 0, coords_info_.coords_num_, [&](uint64_t c) {
        std::vector<uint64_t> coords(dim_num);
        for (uint32_t d = 0; d < dim_num; ++d) {
          auto dim{array_schema_.dimension_ptr(d)};
          coords[d] = hilbert_order::map_to_uint64(
              *dim, domain_buffers[d], c, bits, max_bucket_val);
        }
        hilbert_values[c] = h.coords_to_hilbert(&coords[0]);

        return Status::Ok();
      });

  RETURN_NOT_OK_ELSE(status, logger_->error(status.message()));

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
        throw WriterBaseException(ss.str());
      }
    } else {
      if (expected_cell_num != cell_num) {
        std::stringstream ss;
        ss << "Buffer sizes check failed; Invalid number of cells given for ";
        ss << "attribute '" << attr << "'";
        ss << " (" << expected_cell_num << " != " << cell_num << ")";
        throw WriterBaseException(ss.str());
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
      &resources_.compute_tp(),
      0,
      coords_info_.coords_num_,
      0,
      dim_num,
      [&](uint64_t c, unsigned d) {
        auto dim{array_schema_.dimension_ptr(d)};
        if (datatype_is_string(dim->type())) {
          return Status::Ok();
        } else {
          try {
            dim->oob(buffs[d] + c * coord_sizes[d]);
            return Status::Ok();
          } catch (const StatusException& e) {
            return Status_DimensionError(e.what());
          }
        }
      });

  RETURN_NOT_OK(status);

  // Success
  return Status::Ok();
}

void WriterBase::check_subarray() const {
  if (array_schema_.dense()) {
    if (subarray_.range_num() != 1) {
      throw WriterBaseException("Multi-range dense writes are not supported");
    }

    if (layout_ == Layout::GLOBAL_ORDER && !subarray_.coincides_with_tiles()) {
      throw WriterBaseException(
          "Cannot initialize query; In global writes for dense arrays, the "
          "subarray must coincide with the tile bounds");
    }
  }
}

bool is_sorted_buffer(
    const QueryBuffer& buffer, Datatype type, bool increasing) {
  switch (type) {
    case Datatype::INT8:
      return increasing ?
                 buffer.is_sorted<int8_t, std::less_equal<int8_t>>() :
                 buffer.is_sorted<int8_t, std::greater_equal<int8_t>>();
    case Datatype::UINT8:
      return increasing ?
                 buffer.is_sorted<uint8_t, std::less_equal<uint8_t>>() :
                 buffer.is_sorted<uint8_t, std::greater_equal<uint8_t>>();
    case Datatype::INT16:
      return increasing ?
                 buffer.is_sorted<int16_t, std::less_equal<int16_t>>() :
                 buffer.is_sorted<int16_t, std::greater_equal<int16_t>>();
    case Datatype::UINT16:
      return increasing ?
                 buffer.is_sorted<uint16_t, std::less_equal<uint16_t>>() :
                 buffer.is_sorted<uint16_t, std::greater_equal<uint16_t>>();
    case Datatype::INT32:
      return increasing ?
                 buffer.is_sorted<int32_t, std::less_equal<int32_t>>() :
                 buffer.is_sorted<int32_t, std::greater_equal<int32_t>>();
    case Datatype::UINT32:
      return increasing ?
                 buffer.is_sorted<uint32_t, std::less_equal<uint32_t>>() :
                 buffer.is_sorted<uint32_t, std::greater_equal<uint32_t>>();
    case Datatype::INT64:
      return increasing ?
                 buffer.is_sorted<int64_t, std::less_equal<int64_t>>() :
                 buffer.is_sorted<int64_t, std::greater_equal<int64_t>>();
    case Datatype::UINT64:
      return increasing ?
                 buffer.is_sorted<uint64_t, std::less_equal<uint64_t>>() :
                 buffer.is_sorted<uint64_t, std::greater_equal<uint64_t>>();
    case Datatype::FLOAT32:
      return increasing ? buffer.is_sorted<float, std::less_equal<float>>() :
                          buffer.is_sorted<float, std::greater_equal<float>>();
    case Datatype::FLOAT64:
      return increasing ?
                 buffer.is_sorted<double, std::less_equal<double>>() :
                 buffer.is_sorted<double, std::greater_equal<double>>();
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
      return increasing ?
                 buffer.is_sorted<int64_t, std::less_equal<int64_t>>() :
                 buffer.is_sorted<int64_t, std::greater_equal<int64_t>>();
    case Datatype::STRING_ASCII:
      return increasing ?
                 buffer.is_sorted_str<std::less_equal<std::string_view>>() :
                 buffer.is_sorted_str<std::greater_equal<std::string_view>>();
    default:
      throw WriterBaseException(
          "Unexpected datatype '" + datatype_str(type) +
          "' for an ordered attribute.");
  }

  return true;
}

void WriterBase::check_attr_order() const {
  auto timer_se = stats_->start_timer("check_attr_order");
  for (const auto& [name, buffer] : buffers_) {
    // Skip non-attribute buffers.
    if (!array_schema_.is_attr(name)) {
      continue;
    }

    // Get the attribute data order. If the data type is unordered, no futher
    // checks are needed.
    const auto* attr{array_schema_.attribute(name)};
    if (attr->order() == DataOrder::UNORDERED_DATA) {
      continue;
    }
    bool increasing{attr->order() == DataOrder::INCREASING_DATA};

    // Check the attribute sort. This assumes all ordered attributes are fixed
    // except STRING_ASCII which is assumed to always be variable.
    if (!is_sorted_buffer(buffer, attr->type(), increasing)) {
      throw WriterBaseException(
          "The data for attribute '" + name +
          "' is not in the expected order.");
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
    file_uris.emplace_back(meta->uri(name));
    if (array_schema_.var_size(name)) {
      file_uris.emplace_back(meta->var_uri(name));
    }
    if (array_schema_.is_nullable(name)) {
      file_uris.emplace_back(meta->validity_uri(name));
    }
  }

  auto status =
      parallel_for(&resources_.io_tp(), 0, file_uris.size(), [&](uint64_t i) {
        const auto& file_uri = file_uris[i];
        if (layout_ == Layout::GLOBAL_ORDER && remote_query()) {
          // flush with finalize == true
          resources_.vfs().flush(file_uri, true);
        } else {
          throw_if_not_ok(resources_.vfs().close_file(file_uri));
        }
        return Status::Ok();
      });

  throw_if_not_ok(status);

  return Status::Ok();
}

std::vector<NDRange> WriterBase::compute_mbrs(
    const tdb::pmr::unordered_map<std::string, WriterTileTupleVector>& tiles)
    const {
  auto timer_se = stats_->start_timer("compute_coord_meta");

  // Applicable only if there are coordinates
  if (!coords_info_.has_coords_) {
    return std::vector<NDRange>();
  }

  // Check if tiles are empty
  if (tiles.empty() || tiles.begin()->second.empty()) {
    return std::vector<NDRange>();
  }

  // Compute number of tiles. Assumes all attributes and
  // and dimensions have the same number of tiles
  auto tile_num = tiles.begin()->second.size();
  auto dim_num = array_schema_.dim_num();

  // Compute MBRs
  std::vector<NDRange> mbrs(tile_num);
  auto status =
      parallel_for(&resources_.compute_tp(), 0, tile_num, [&](uint64_t i) {
        mbrs[i].resize(dim_num);
        std::vector<const void*> data(dim_num);
        for (unsigned d = 0; d < dim_num; ++d) {
          auto dim{array_schema_.dimension_ptr(d)};
          const auto& dim_name = dim->name();
          auto tiles_it = tiles.find(dim_name);
          iassert(tiles_it != tiles.end());
          mbrs[i][d] = dim->var_size() ?
                           dim->compute_mbr_var(
                               tiles_it->second[i].offset_tile(),
                               tiles_it->second[i].var_tile()) :
                           dim->compute_mbr(tiles_it->second[i].fixed_tile());
        }

        return Status::Ok();
      });
  throw_if_not_ok(status);

  return mbrs;
}

void WriterBase::set_coords_metadata(
    const uint64_t start_tile_idx,
    const uint64_t end_tile_idx,
    const tdb::pmr::unordered_map<std::string, WriterTileTupleVector>& tiles,
    const std::vector<NDRange>& mbrs,
    shared_ptr<FragmentMetadata> meta) const {
  // Applicable only if there are coordinates
  if (!coords_info_.has_coords_) {
    return;
  }

  // Check if tiles are empty
  if (tiles.empty() || tiles.begin()->second.empty()) {
    return;
  }

  auto status = parallel_for(
      &resources_.compute_tp(), start_tile_idx, end_tile_idx, [&](uint64_t i) {
        meta->set_mbr(i - start_tile_idx, mbrs[i]);
        return Status::Ok();
      });
  throw_if_not_ok(status);

  // Set last tile cell number
  auto dim_0{array_schema_.dimension_ptr(0)};
  const auto& dim_tiles = tiles.find(dim_0->name())->second;
  auto cell_num = dim_tiles.at(end_tile_idx - 1).cell_num();
  meta->set_last_tile_cell_num(cell_num);
}

Status WriterBase::compute_tiles_metadata(
    uint64_t tile_num,
    tdb::pmr::unordered_map<std::string, WriterTileTupleVector>& tiles) const {
  auto* compute_tp = &resources_.compute_tp();

  // Parallelize over attributes?
  if (tiles.size() > tile_num) {
    auto st = parallel_for(compute_tp, 0, tiles.size(), [&](uint64_t i) {
      auto tiles_it = tiles.begin();
      std::advance(tiles_it, i);
      const auto& attr = tiles_it->first;
      auto& attr_tiles = tiles.at(attr);
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
    for (auto& tile_vec : tiles) {
      const auto& attr = tile_vec.first;
      auto& attr_tiles = tile_vec.second;
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
    bool dense,
    shared_ptr<FragmentMetadata>& frag_meta,
    const NDRange* domain) {
  // Get write version, timestamp array was opened,  and a reference to the
  // array directory.
  auto write_version = array_->array_schema_latest().write_version();
  uint64_t timestamp = array_->timestamp_end_opened_at();
  auto& array_dir = array_->array_directory();

  // Create the directories.
  // Create the fragment directory, the directory for the new fragment
  // URI, and the commit directory.
  resources_.vfs().create_dir(array_dir.get_fragments_dir(write_version));
  resources_.vfs().create_dir(fragment_uri_);
  resources_.vfs().create_dir(array_dir.get_commits_dir(write_version));

  // Create fragment metadata.
  auto timestamp_range = std::pair<uint64_t, uint64_t>(timestamp, timestamp);
  const bool has_timestamps = buffers_.count(constants::timestamps) != 0;
  const bool has_delete_metadata =
      buffers_.count(constants::delete_timestamps) != 0;
  frag_meta = make_shared<FragmentMetadata>(
      HERE(),
      &resources_,
      array_->array_schema_latest_ptr(),
      fragment_uri_,
      timestamp_range,
      query_memory_tracker_,
      dense,
      has_timestamps,
      has_delete_metadata);

  frag_meta->init(domain ? *domain : subarray_.ndrange(0));
  return Status::Ok();
}

Status WriterBase::filter_tiles(
    tdb::pmr::unordered_map<std::string, WriterTileTupleVector>* tiles) {
  auto timer_se = stats_->start_timer("filter_tiles");
  auto status =
      parallel_for(&resources_.compute_tp(), 0, tiles->size(), [&](uint64_t i) {
        auto tiles_it = tiles->begin();
        std::advance(tiles_it, i);
        throw_if_not_ok(filter_tiles(tiles_it->first, &tiles_it->second));
        throw_if_cancelled();
        return Status::Ok();
      });

  RETURN_NOT_OK(status);
  return Status::Ok();
}

Status WriterBase::filter_tiles(
    const std::string& name, WriterTileTupleVector* tiles) {
  const bool var_size = array_schema_.var_size(name);
  const bool nullable = array_schema_.is_nullable(name);

  // Filter all tiles
  auto tile_num = tiles->size();

  // Process all tiles, minus offsets, they get processed separately.
  std::vector<std::tuple<WriterTile*, WriterTile*, bool, bool>> args;
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

  // For fixed size, process everything, for var size, everything minus
  // offsets.
  auto status =
      parallel_for(&resources_.compute_tp(), 0, args.size(), [&](uint64_t i) {
        const auto& [tile, offset_tile, contains_offsets, is_nullable] =
            args[i];
        throw_if_not_ok(filter_tile(
            name, tile, offset_tile, contains_offsets, is_nullable));
        return Status::Ok();
      });
  RETURN_NOT_OK(status);

  // Process offsets for var size.
  if (var_size) {
    auto status = parallel_for(
        &resources_.compute_tp(), 0, tiles->size(), [&](uint64_t i) {
          auto& tile = (*tiles)[i];
          throw_if_not_ok(
              filter_tile(name, &tile.offset_tile(), nullptr, true, false));
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

  // Get a copy of the appropriate filter pipeline.
  FilterPipeline filters;
  if (offsets) {
    iassert(!nullable);
    filters = array_schema_.cell_var_offsets_filters();
  } else if (nullable) {
    filters = array_schema_.cell_validity_filters();
  } else {
    filters = array_schema_.filters(name);
  }

  // If those offsets belong to a var-sized string dimension/attribute then
  // don't filter the offsets as the information will be included in, and can
  // be reconstructed from, the filtered data tile.
  if (offsets && array_schema_.filters(name).skip_offsets_filtering(
                     array_schema_.type(name))) {
    tile->filtered_buffer().expand(sizeof(uint64_t));
    uint64_t nchunks = 0;
    memcpy(tile->filtered_buffer().data(), &nchunks, sizeof(uint64_t));
    tile->clear_data();
    return Status::Ok();
  }

  // Append an encryption filter when necessary.
  throw_if_not_ok(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  // Check if chunk or tile level filtering/unfiltering is appropriate
  bool use_chunking = filters.use_tile_chunking(
      array_schema_.var_size(name), array_schema_.version(), tile->type());

  iassert(!tile->filtered());
  filters.run_forward(
      stats_, tile, offsets_tile, &resources_.compute_tp(), use_chunking);
  iassert(tile->filtered());

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
    const std::string& name,
    uint64_t tile_num,
    WriterTileTupleVector* tiles) const {
  // Initialize tiles
  const bool var_size = array_schema_.var_size(name);
  const bool nullable = array_schema_.is_nullable(name);
  const uint64_t cell_size = array_schema_.cell_size(name);
  const auto type = array_schema_.type(name);
  const auto& domain{array_schema_.domain()};
  const auto capacity = array_schema_.capacity();
  const auto cell_num_per_tile =
      coords_info_.has_coords_ ? capacity : domain.cell_num_per_tile();
  tiles->reserve(tile_num);
  for (uint64_t i = 0; i < tile_num; i++) {
    tiles->emplace_back(
        array_schema_,
        cell_num_per_tile,
        var_size,
        nullable,
        cell_size,
        type,
        query_memory_tracker_);
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
      throw WriterBaseException(
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
  auto coords_size{
      dim_num * array_schema_.domain().dimension_ptr(0)->coord_size()};
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

Status WriterBase::write_tiles(
    const uint64_t start_tile_idx,
    const uint64_t end_tile_idx,
    shared_ptr<FragmentMetadata> frag_meta,
    tdb::pmr::unordered_map<std::string, WriterTileTupleVector>* const tiles) {
  auto timer_se = stats_->start_timer("write_num_tiles");

  iassert(!tiles->empty());

  std::vector<ThreadPool::Task> tasks;
  for (auto& it : *tiles) {
    tasks.push_back(resources_.io_tp().execute([&, this]() {
      auto& attr = it.first;
      auto& tiles = it.second;
      RETURN_CANCEL_OR_ERROR(write_tiles(
          start_tile_idx, end_tile_idx, attr, frag_meta, 0, &tiles));

      // Fix var size attributes metadata.
      const auto var_size = array_schema_.var_size(attr);
      if (has_min_max_metadata(attr, var_size) &&
          array_schema_.var_size(attr)) {
        frag_meta->convert_tile_min_max_var_sizes_to_offsets(attr);

        for (uint64_t idx = start_tile_idx; idx < end_tile_idx; idx++) {
          frag_meta->set_tile_min_var(
              attr, idx - start_tile_idx, tiles[idx].min());
          frag_meta->set_tile_max_var(
              attr, idx - start_tile_idx, tiles[idx].max());
        }
      }
      return Status::Ok();
    }));
  }

  // Wait for writes and check all statuses
  auto statuses = resources_.io_tp().wait_all_status(tasks);
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();
}

Status WriterBase::write_tiles(
    const uint64_t start_tile_idx,
    const uint64_t end_tile_idx,
    const std::string& name,
    shared_ptr<FragmentMetadata> frag_meta,
    uint64_t start_tile_id,
    WriterTileTupleVector* const tiles,
    bool close_files) {
  auto timer_se = stats_->start_timer("write_tiles");

  // Handle zero tiles
  if (tiles->empty()) {
    return Status::Ok();
  }

  // For easy reference
  const bool var_size = array_schema_.var_size(name);
  const bool nullable = array_schema_.is_nullable(name);
  auto uri = frag_meta->uri(name);

  URI var_uri = var_size ? frag_meta->var_uri(name) : URI("");
  URI validity_uri = nullable ? frag_meta->validity_uri(name) : URI("");

  // Compute and set var buffer sizes for the min/max metadata
  const auto has_min_max_md = has_min_max_metadata(name, var_size);
  const auto has_sum_md = has_sum_metadata(name, var_size);

  bool remote_global_order_write =
      (layout_ == Layout::GLOBAL_ORDER && remote_query());

  // Write tiles
  for (size_t i = start_tile_idx, tile_id = start_tile_id; i < end_tile_idx;
       ++i, ++tile_id) {
    auto& tile = (*tiles)[i];
    auto& t = var_size ? tile.offset_tile() : tile.fixed_tile();
    resources_.vfs().write(
        uri,
        t.filtered_buffer().data(),
        t.filtered_buffer().size(),
        remote_global_order_write);
    frag_meta->set_tile_offset(name, tile_id, t.filtered_buffer().size());
    auto null_count = tile.null_count();

    if (var_size) {
      auto& t_var = tile.var_tile();
      resources_.vfs().write(
          var_uri,
          t_var.filtered_buffer().data(),
          t_var.filtered_buffer().size(),
          remote_global_order_write);
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
      resources_.vfs().write(
          validity_uri,
          t_val.filtered_buffer().data(),
          t_val.filtered_buffer().size(),
          remote_global_order_write);
      frag_meta->set_tile_validity_offset(
          name, tile_id, t_val.filtered_buffer().size());
      frag_meta->set_tile_null_count(name, tile_id, null_count);
    }
  }

  // Close files or flush multipart upload buffers in case of global order
  // writes
  if (close_files) {
    std::vector<URI> closing_uris;
    closing_uris.push_back(frag_meta->uri(name));

    if (var_size) {
      closing_uris.push_back(frag_meta->var_uri(name));
    }
    if (nullable) {
      closing_uris.push_back(frag_meta->validity_uri(name));
    }
    for (auto& u : closing_uris) {
      if (layout_ == Layout::GLOBAL_ORDER) {
        // Flushing the multipart buffers after each write stage is a
        // requirement of remote global order writes, it should only be
        // done if this code is executed as a result of a remote query
        if (remote_query()) {
          throw_if_not_ok(resources_.vfs().flush_multipart_file_buffer(u));
        }
      } else {
        throw_if_not_ok(resources_.vfs().close_file(u));
      }
    }
  }

  return Status::Ok();
}

bool WriterBase::remote_query() const {
  return remote_query_;
}

}  // namespace tiledb::sm

template <>
IndexedList<tiledb::sm::WriterTileTuple>::IndexedList(
    shared_ptr<tiledb::sm::MemoryTracker> memory_tracker)
    : memory_tracker_(memory_tracker)
    , list_(memory_tracker->get_resource(sm::MemoryType::WRITER_TILE_DATA)) {
}

template <>
IndexedList<tiledb::common::IndexedList<tiledb::sm::WriterTileTuple>>::
    IndexedList(shared_ptr<tiledb::sm::MemoryTracker> memory_tracker)
    : memory_tracker_(memory_tracker)
    , list_(memory_tracker->get_resource(sm::MemoryType::WRITER_TILE_DATA)) {
}
