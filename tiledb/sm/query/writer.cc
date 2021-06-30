/**
 * @file   writer.cc
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
 * This file implements class Writer.
 */

#include "tiledb/sm/query/writer.h"
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
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/generic_tile_io.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Writer::Writer(
    stats::Stats* stats,
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
          stats, storage_manager, array, config, buffers, subarray, layout)
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

Writer::~Writer() {
  clear_coord_buffers();
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status Writer::finalize() {
  auto timer_se = stats_->start_timer("finalize");

  if (global_write_state_ != nullptr)
    return finalize_global_write_state();
  return Status::Ok();
}

bool Writer::get_check_coord_dups() const {
  return check_coord_dups_;
}

bool Writer::get_check_coord_oob() const {
  return check_coord_oob_;
}

bool Writer::get_dedup_coords() const {
  return dedup_coords_;
}

void Writer::set_check_coord_dups(bool b) {
  check_coord_dups_ = b;
}

void Writer::set_check_coord_oob(bool b) {
  check_coord_oob_ = b;
}

void Writer::set_dedup_coords(bool b) {
  dedup_coords_ = b;
}

inline uint64_t Writer::get_offset_buffer_element(
    const void* buffer, const uint64_t pos) const {
  if (offsets_bitsize_ == 32) {
    const uint32_t* buffer_32bit = reinterpret_cast<const uint32_t*>(buffer);
    return static_cast<uint64_t>(buffer_32bit[pos]);
  } else {
    return reinterpret_cast<const uint64_t*>(buffer)[pos];
  }
}

inline uint64_t Writer::prepare_buffer_offset(
    const void* buffer, const uint64_t pos, const uint64_t datasize) const {
  uint64_t offset = get_offset_buffer_element(buffer, pos);
  return offsets_format_mode_ == "elements" ? offset * datasize : offset;
}

inline uint64_t Writer::get_offset_buffer_size(
    const uint64_t buffer_size) const {
  return offsets_extra_element_ ?
             buffer_size - constants::cell_var_offset_size :
             buffer_size;
}

Status Writer::check_var_attr_offsets() const {
  for (const auto& it : buffers_) {
    const auto& attr = it.first;
    if (!array_schema_->var_size(attr)) {
      continue;
    }

    const void* buffer_off = it.second.buffer_;
    const uint64_t buffer_off_size =
        get_offset_buffer_size(*it.second.buffer_size_);
    const uint64_t* buffer_val_size = it.second.buffer_var_size_;
    auto num_offsets = buffer_off_size / sizeof(uint64_t);
    if (num_offsets == 0)
      return Status::Ok();

    uint64_t prev_offset = get_offset_buffer_element(buffer_off, 0);
    // Allow the initial offset to be equal to the size, this indicates
    // the first and only value in the buffer is to be empty
    if (prev_offset > *buffer_val_size)
      return LOG_STATUS(Status::WriterError(
          "Invalid offsets for attribute " + attr + "; offset " +
          std::to_string(prev_offset) + " specified for buffer of size " +
          std::to_string(*buffer_val_size)));
    else if (prev_offset == *buffer_val_size)
      return LOG_STATUS(Status::WriterError(
          "Invalid offsets for attribute " + attr +
          "; zero length single cell writes are not supported"));

    for (uint64_t i = 1; i < num_offsets; i++) {
      uint64_t cur_offset = get_offset_buffer_element(buffer_off, i);
      if (cur_offset < prev_offset)
        return LOG_STATUS(Status::WriterError(
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
        return LOG_STATUS(Status::WriterError(
            "Invalid offsets for attribute " + attr + "; offset " +
            std::to_string(cur_offset) + " specified for buffer of size " +
            std::to_string(*buffer_val_size)));

      prev_offset = cur_offset;
    }
  }

  return Status::Ok();
}

Status Writer::init() {
  // Sanity checks
  if (storage_manager_ == nullptr)
    return LOG_STATUS(Status::WriterError(
        "Cannot initialize query; Storage manager not set"));
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot initialize writer; Array schema not set"));
  if (buffers_.empty())
    return LOG_STATUS(
        Status::WriterError("Cannot initialize writer; Buffers not set"));
  if (array_schema_->dense() &&
      (layout_ == Layout::ROW_MAJOR || layout_ == Layout::COL_MAJOR)) {
    for (const auto& b : buffers_) {
      if (array_schema_->is_dim(b.first)) {
        return LOG_STATUS(Status::WriterError(
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
    return LOG_STATUS(
        Status::WriterError("Cannot initialize writer; Unsupported offsets "
                            "format in configuration"));
  }
  RETURN_NOT_OK(config_.get<bool>(
      "sm.var_offsets.extra_element", &offsets_extra_element_, &found));
  assert(found);
  RETURN_NOT_OK(config_.get<uint32_t>(
      "sm.var_offsets.bitsize", &offsets_bitsize_, &found));
  if (offsets_bitsize_ != 32 && offsets_bitsize_ != 64) {
    return LOG_STATUS(
        Status::WriterError("Cannot initialize writer; Unsupported offsets "
                            "bitsize in configuration"));
  }
  assert(found);

  // Set a default subarray
  if (!subarray_.is_set())
    subarray_ = Subarray(array_, layout_, stats_);

  if (offsets_extra_element_)
    RETURN_NOT_OK(check_extra_element());

  RETURN_NOT_OK(check_subarray());
  RETURN_NOT_OK(check_buffer_sizes());

  optimize_layout_for_1D();
  RETURN_NOT_OK(check_var_attr_offsets());
  initialized_ = true;

  return Status::Ok();
}

Status Writer::dowork() {
  get_dim_attr_stats();

  auto timer_se = stats_->start_timer("write");

  // In case the user has provided a coordinates buffer
  RETURN_NOT_OK(split_coords_buffer());

  if (check_coord_oob_)
    RETURN_NOT_OK(check_coord_oob());

  if (layout_ == Layout::COL_MAJOR || layout_ == Layout::ROW_MAJOR) {
    RETURN_NOT_OK(ordered_write());
  } else if (layout_ == Layout::UNORDERED) {
    RETURN_NOT_OK(unordered_write());
  } else if (layout_ == Layout::GLOBAL_ORDER) {
    RETURN_NOT_OK(global_write());
  } else {
    assert(false);
  }

  return Status::Ok();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status Writer::add_written_fragment_info(const URI& uri) {
  std::pair<uint64_t, uint64_t> timestamp_range;
  RETURN_NOT_OK(utils::parse::get_timestamp_range(uri, &timestamp_range));
  written_fragment_info_.emplace_back(uri, timestamp_range);
  return Status::Ok();
}

Status Writer::check_buffer_sizes() const {
  // This is applicable only to dense arrays and ordered layout
  if (!array_schema_->dense() ||
      (layout_ != Layout::ROW_MAJOR && layout_ != Layout::COL_MAJOR))
    return Status::Ok();

  auto cell_num = array_schema_->domain()->cell_num(subarray_.ndrange(0));
  uint64_t expected_cell_num = 0;
  for (const auto& it : buffers_) {
    const auto& attr = it.first;
    const bool is_var = array_schema_->var_size(attr);
    const uint64_t buffer_size =
        is_var ? get_offset_buffer_size(*it.second.buffer_size_) :
                 *it.second.buffer_size_;
    if (is_var) {
      expected_cell_num = buffer_size / constants::cell_var_offset_size;
    } else {
      expected_cell_num = buffer_size / array_schema_->cell_size(attr);
    }

    if (expected_cell_num != cell_num) {
      std::stringstream ss;
      ss << "Buffer sizes check failed; Invalid number of cells given for ";
      ss << "attribute '" << attr << "'";
      ss << " (" << expected_cell_num << " != " << cell_num << ")";
      return LOG_STATUS(Status::WriterError(ss.str()));
    }

    if (array_schema_->is_nullable(attr)) {
      const uint64_t buffer_validity_size =
          *it.second.validity_vector_.buffer_size();
      const uint64_t cell_validity_num =
          buffer_validity_size / constants::cell_validity_size;

      if (expected_cell_num != cell_validity_num) {
        std::stringstream ss;
        ss << "Buffer sizes check failed; Invalid number of validity cells "
              "given for ";
        ss << "attribute '" << attr << "'";
        ss << " (" << expected_cell_num << " != " << cell_validity_num << ")";
        return LOG_STATUS(Status::WriterError(ss.str()));
      }
    }
  }

  return Status::Ok();
}

Status Writer::check_coord_dups(const std::vector<uint64_t>& cell_pos) const {
  auto timer_se = stats_->start_timer("check_coord_dups");

  // Check if applicable
  if (array_schema_->allows_dups() || !check_coord_dups_ || dedup_coords_)
    return Status::Ok();

  if (!coords_info_.has_coords_) {
    return LOG_STATUS(
        Status::WriterError("Cannot check for coordinate duplicates; "
                            "Coordinates buffer not found"));
  }

  if (coords_info_.coords_num_ < 2)
    return Status::Ok();

  // Prepare auxiliary vectors for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<const unsigned char*> buffs(dim_num);
  std::vector<uint64_t> coord_sizes(dim_num);
  std::vector<const unsigned char*> buffs_var(dim_num);
  std::vector<uint64_t*> buffs_var_sizes(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = (const unsigned char*)buffers_.find(dim_name)->second.buffer_;
    coord_sizes[d] = array_schema_->cell_size(dim_name);
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
          auto dim = array_schema_->dimension(d);
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
          return Status::WriterError(ss.str());
        }

        return Status::Ok();
      });

  RETURN_NOT_OK_ELSE(status, LOG_STATUS(status));

  return Status::Ok();
}

Status Writer::check_coord_dups() const {
  auto timer_se = stats_->start_timer("check_coord_dups");

  // Check if applicable
  if (array_schema_->allows_dups() || !check_coord_dups_ || dedup_coords_)
    return Status::Ok();

  if (!coords_info_.has_coords_) {
    return LOG_STATUS(
        Status::WriterError("Cannot check for coordinate duplicates; "
                            "Coordinates buffer not found"));
  }

  if (coords_info_.coords_num_ < 2)
    return Status::Ok();

  // Prepare auxiliary vectors for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<const unsigned char*> buffs(dim_num);
  std::vector<uint64_t> coord_sizes(dim_num);
  std::vector<const unsigned char*> buffs_var(dim_num);
  std::vector<uint64_t*> buffs_var_sizes(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = (const unsigned char*)buffers_.find(dim_name)->second.buffer_;
    coord_sizes[d] = array_schema_->cell_size(dim_name);
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
          auto dim = array_schema_->dimension(d);
          if (!dim->var_size()) {  // Fixed-sized dimensions
            if (memcmp(
                    buffs[d] + i * coord_sizes[d],
                    buffs[d] + (i - 1) * coord_sizes[d],
                    coord_sizes[d]) != 0) {  // Not the same
              found_dup = false;
              break;
            }
          } else {
            auto offs = (uint64_t*)buffs[d];
            auto off_i_plus_1 = (i == coords_info_.coords_num_ - 1) ?
                                    *(buffs_var_sizes[d]) :
                                    offs[i + 1];
            auto size_i_minus_1 = offs[i] - offs[i - 1];
            auto size_i = off_i_plus_1 - offs[i];

            // Compare sizes
            if (size_i != size_i_minus_1) {  // Not same
              found_dup = false;
              break;
            }

            // Compare var values
            if (memcmp(
                    buffs_var[d] + offs[i - 1],
                    buffs_var[d] + offs[i],
                    size_i) != 0) {  // Not the same
              found_dup = false;
              break;
            }
          }
        }

        // Found duplicate
        if (found_dup) {
          std::stringstream ss;
          ss << "Duplicate coordinates " << coords_to_str(i);
          ss << " are not allowed";
          return Status::WriterError(ss.str());
        }

        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status Writer::check_coord_oob() const {
  auto timer_se = stats_->start_timer("check_coord_oob");

  // Applicable only to sparse writes - exit if coordinates do not exist
  if (!coords_info_.has_coords_)
    return Status::Ok();

  // Exit if there are no coordinates to write
  if (coords_info_.coords_num_ == 0)
    return Status::Ok();

  // Exit if all dimensions are strings
  if (array_schema_->domain()->all_dims_string())
    return Status::Ok();

  // Prepare auxiliary vectors for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<unsigned char*> buffs(dim_num);
  std::vector<uint64_t> coord_sizes(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = (unsigned char*)buffers_.find(dim_name)->second.buffer_;
    coord_sizes[d] = array_schema_->cell_size(dim_name);
  }

  // Check if all coordinates fall in the domain in parallel
  auto status = parallel_for_2d(
      storage_manager_->compute_tp(),
      0,
      coords_info_.coords_num_,
      0,
      dim_num,
      [&](uint64_t c, unsigned d) {
        auto dim = array_schema_->dimension(d);
        if (datatype_is_string(dim->type()))
          return Status::Ok();
        return dim->oob(buffs[d] + c * coord_sizes[d]);
      });

  RETURN_NOT_OK(status);

  // Success
  return Status::Ok();
}

Status Writer::check_global_order() const {
  auto timer_se = stats_->start_timer("check_global_order");

  // Check if applicable
  if (!check_global_order_)
    return Status::Ok();

  // Applicable only to sparse writes - exit if coordinates do not exist
  if (!coords_info_.has_coords_ || coords_info_.coords_num_ < 2)
    return Status::Ok();

  // Special case for Hilbert
  if (array_schema_->cell_order() == Layout::HILBERT)
    return check_global_order_hilbert();

  // Prepare auxiliary vector for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<const QueryBuffer*> buffs(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = &(buffers_.find(dim_name)->second);
  }

  // Check if all coordinates fall in the domain in parallel
  auto domain = array_schema_->domain();
  auto status = parallel_for(
      storage_manager_->compute_tp(),
      0,
      coords_info_.coords_num_ - 1,
      [&](uint64_t i) {
        auto tile_cmp = domain->tile_order_cmp(buffs, i, i + 1);
        auto fail =
            (tile_cmp > 0) ||
            ((tile_cmp == 0) && domain->cell_order_cmp(buffs, i, i + 1) > 0);

        if (fail) {
          std::stringstream ss;
          ss << "Write failed; Coordinates " << coords_to_str(i);
          ss << " succeed " << coords_to_str(i + 1);
          ss << " in the global order";
          if (tile_cmp > 0)
            ss << " due to writes across tiles";
          return Status::WriterError(ss.str());
        }
        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status Writer::check_global_order_hilbert() const {
  // Prepare auxiliary vector for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<const QueryBuffer*> buffs(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = &buffers_.at(dim_name);
  }

  // Compute hilbert values
  std::vector<uint64_t> hilbert_values(coords_info_.coords_num_);
  RETURN_NOT_OK(calculate_hilbert_values(buffs, &hilbert_values));

  // Check if all coordinates fall in the domain in parallel
  auto status = parallel_for(
      storage_manager_->compute_tp(),
      0,
      coords_info_.coords_num_ - 1,
      [&](uint64_t i) {
        if (hilbert_values[i] > hilbert_values[i + 1]) {
          std::stringstream ss;
          ss << "Write failed; Coordinates " << coords_to_str(i);
          ss << " succeed " << coords_to_str(i + 1);
          ss << " in the global order";
          return Status::WriterError(ss.str());
        }
        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status Writer::check_subarray() const {
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot check subarray; Array schema not set"));

  if (array_schema_->dense()) {
    if (layout_ == Layout::GLOBAL_ORDER && !subarray_.coincides_with_tiles())
      return LOG_STATUS(
          Status::WriterError("Cannot initialize query; In global writes for "
                              "dense arrays, the subarray "
                              "must coincide with the tile bounds"));
    if (layout_ == Layout::UNORDERED && subarray_.is_set())
      return LOG_STATUS(Status::WriterError(
          "Cannot initialize query; Setting a subarray in unordered writes for "
          "dense arrays in inapplicable"));
  }

  return Status::Ok();
}

void Writer::clear_coord_buffers() {
  // Applicable only if the coordinate buffers have been allocated by
  // TileDB, which happens only when the zipped coordinates buffer is set
  for (auto b : to_clean_)
    tdb_free(b);
  to_clean_.clear();
  coord_buffer_sizes_.clear();
}

std::vector<std::string> Writer::buffer_names() const {
  std::vector<std::string> ret;

  // Add to the buffer names the attributes, as well as the dimensions only if
  // coords_buffer_ has not been set
  for (const auto& it : buffers_) {
    if (!array_schema_->is_dim(it.first) || (!coords_info_.coords_buffer_))
      ret.push_back(it.first);
  }

  // Special zipped coordinates name
  if (coords_info_.coords_buffer_)
    ret.push_back(constants::coords);

  return ret;
}

Status Writer::close_files(FragmentMetadata* meta) const {
  // Close attribute and dimension files
  const auto buffer_name = buffer_names();

  std::vector<URI> file_uris;
  file_uris.reserve(buffer_name.size() * 3);

  for (const auto& name : buffer_name) {
    file_uris.emplace_back(meta->uri(name));
    if (array_schema_->var_size(name))
      file_uris.emplace_back(meta->var_uri(name));
    if (array_schema_->is_nullable(name))
      file_uris.emplace_back(meta->validity_uri(name));
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

Status Writer::compute_coord_dups(
    const std::vector<uint64_t>& cell_pos,
    std::set<uint64_t>* coord_dups) const {
  auto timer_se = stats_->start_timer("compute_coord_dups");

  if (!coords_info_.has_coords_) {
    return LOG_STATUS(
        Status::WriterError("Cannot check for coordinate duplicates; "
                            "Coordinates buffer not found"));
  }

  if (coords_info_.coords_num_ < 2)
    return Status::Ok();

  // Prepare auxiliary vectors for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<const unsigned char*> buffs(dim_num);
  std::vector<uint64_t> coord_sizes(dim_num);
  std::vector<const unsigned char*> buffs_var(dim_num);
  std::vector<uint64_t*> buffs_var_sizes(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = (const unsigned char*)buffers_.find(dim_name)->second.buffer_;
    coord_sizes[d] = array_schema_->cell_size(dim_name);
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
          auto dim = array_schema_->dimension(d);
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

Status Writer::compute_coord_dups(std::set<uint64_t>* coord_dups) const {
  auto timer_se = stats_->start_timer("compute_coord_dups");

  if (!coords_info_.has_coords_) {
    return LOG_STATUS(
        Status::WriterError("Cannot check for coordinate duplicates; "
                            "Coordinates buffer not found"));
  }

  if (coords_info_.coords_num_ < 2)
    return Status::Ok();

  // Prepare auxiliary vectors for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<const unsigned char*> buffs(dim_num);
  std::vector<uint64_t> coord_sizes(dim_num);
  std::vector<const unsigned char*> buffs_var(dim_num);
  std::vector<uint64_t*> buffs_var_sizes(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = (const unsigned char*)buffers_.find(dim_name)->second.buffer_;
    coord_sizes[d] = array_schema_->cell_size(dim_name);
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
          auto dim = array_schema_->dimension(d);
          if (!dim->var_size()) {  // Fixed-sized dimensions
            if (memcmp(
                    buffs[d] + i * coord_sizes[d],
                    buffs[d] + (i - 1) * coord_sizes[d],
                    coord_sizes[d]) != 0) {  // Not the same
              found_dup = false;
              break;
            }
          } else {
            auto offs = (uint64_t*)buffs[d];
            auto off_i_plus_1 = (i == coords_info_.coords_num_ - 1) ?
                                    *(buffs_var_sizes[d]) :
                                    offs[i + 1];
            auto size_i_minus_1 = offs[i] - offs[i - 1];
            auto size_i = off_i_plus_1 - offs[i];

            // Compare sizes
            if (size_i != size_i_minus_1) {  // Not same
              found_dup = false;
              break;
            }

            // Compare var values
            if (memcmp(
                    buffs_var[d] + offs[i - 1],
                    buffs_var[d] + offs[i],
                    size_i) != 0) {  // Not the same
              found_dup = false;
              break;
            }
          }
        }

        // Found duplicate
        if (found_dup) {
          std::lock_guard<std::mutex> lock(mtx);
          coord_dups->insert(i);
        }

        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status Writer::compute_coords_metadata(
    const std::unordered_map<std::string, std::vector<Tile>>& tiles,
    FragmentMetadata* meta) const {
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
  const uint64_t t = 1 + (array_schema_->var_size(it->first) ? 1 : 0) +
                     (array_schema_->is_nullable(it->first) ? 1 : 0);
  auto tile_num = it->second.size() / t;
  auto dim_num = array_schema_->dim_num();

  // Compute MBRs
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, tile_num, [&](uint64_t i) {
        NDRange mbr(dim_num);
        std::vector<const void*> data(dim_num);
        for (unsigned d = 0; d < dim_num; ++d) {
          auto dim = array_schema_->dimension(d);
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
  auto dim_0 = array_schema_->dimension(0);
  const auto& dim_tiles = tiles.find(dim_0->name())->second;
  const auto& last_tile_pos =
      (!dim_0->var_size()) ? dim_tiles.size() - 1 : dim_tiles.size() - 2;
  meta->set_last_tile_cell_num(dim_tiles[last_tile_pos].cell_num());

  return Status::Ok();
}

Status Writer::create_fragment(
    bool dense, tdb_shared_ptr<FragmentMetadata>* frag_meta) const {
  URI uri;
  uint64_t timestamp = array_->timestamp_end_opened_at();
  if (!fragment_uri_.to_string().empty()) {
    uri = fragment_uri_;
  } else {
    std::string new_fragment_str;
    RETURN_NOT_OK(new_fragment_name(
        timestamp, array_->array_schema()->write_version(), &new_fragment_str));
    uri = array_schema_->array_uri().join_path(new_fragment_str);
  }
  auto timestamp_range = std::pair<uint64_t, uint64_t>(timestamp, timestamp);
  *frag_meta = tdb_make_shared(
      FragmentMetadata,
      storage_manager_,
      array_schema_,
      uri,
      timestamp_range,
      dense);

  RETURN_NOT_OK((*frag_meta)->init(subarray_.ndrange(0)));
  return storage_manager_->create_dir(uri);
}

Status Writer::filter_tiles(
    std::unordered_map<std::string, std::vector<Tile>>* tiles) {
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

Status Writer::filter_tiles(const std::string& name, std::vector<Tile>* tiles) {
  const bool var_size = array_schema_->var_size(name);
  const bool nullable = array_schema_->is_nullable(name);

  // Filter all tiles
  auto tile_num = tiles->size();

  std::vector<std::tuple<Tile*, bool, bool>> args;
  args.reserve(tile_num);

  size_t i = 0;
  while (i < tile_num) {
    args.emplace_back(&(*tiles)[i++], var_size, false);
    if (var_size)
      args.emplace_back(&(*tiles)[i++], false, false);
    if (nullable)
      args.emplace_back(&(*tiles)[i++], false, true);
  }

  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, tile_num, [&](uint64_t i) {
        RETURN_NOT_OK(filter_tile(
            name,
            std::get<0>(args[i]),
            std::get<1>(args[i]),
            std::get<2>(args[i])));
        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status Writer::filter_tile(
    const std::string& name,
    Tile* const tile,
    const bool offsets,
    const bool nullable) {
  auto timer_se = stats_->start_timer("filter_tile");

  const auto orig_size = tile->chunked_buffer()->size();

  // Get a copy of the appropriate filter pipeline.
  FilterPipeline filters;
  if (offsets) {
    assert(!nullable);
    filters = array_schema_->cell_var_offsets_filters();
  } else if (nullable) {
    filters = array_schema_->cell_validity_filters();
  } else {
    filters = array_schema_->filters(name);
  }

  // Append an encryption filter when necessary.
  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &filters, array_->get_encryption_key()));

  assert(!tile->filtered());
  RETURN_NOT_OK(
      filters.run_forward(stats_, tile, storage_manager_->compute_tp()));
  assert(tile->filtered());

  tile->set_pre_filtered_size(orig_size);

  return Status::Ok();
}

Status Writer::finalize_global_write_state() {
  assert(layout_ == Layout::GLOBAL_ORDER);
  auto meta = global_write_state_->frag_meta_.get();
  const auto& uri = meta->fragment_uri();

  // Handle last tile
  Status st = global_write_handle_last_tile();
  if (!st.ok()) {
    close_files(meta);
    clean_up(uri);
    return st;
  }

  // Close all files
  RETURN_NOT_OK_ELSE(close_files(meta), clean_up(uri));

  // Check that the same number of cells was written across attributes
  // and dimensions
  auto cell_num = global_write_state_->cells_written_[buffers_.begin()->first];
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    if (global_write_state_->cells_written_[name] != cell_num) {
      clean_up(uri);
      return LOG_STATUS(Status::WriterError(
          "Failed to finalize global write state; Different "
          "number of cells written across attributes and coordinates"));
    }
  }

  // Check if the total number of cells written is equal to the subarray size
  if (!coords_info_.has_coords_) {  // This implies a dense array
    auto expected_cell_num =
        array_schema_->domain()->cell_num(subarray_.ndrange(0));
    if (cell_num != expected_cell_num) {
      clean_up(uri);
      std::stringstream ss;
      ss << "Failed to finalize global write state; Number "
         << "of cells written (" << cell_num
         << ") is different from the number of cells expected ("
         << expected_cell_num << ") for the query subarray";
      return LOG_STATUS(Status::WriterError(ss.str()));
    }
  }

  // Flush fragment metadata to storage
  RETURN_NOT_OK_ELSE(meta->store(array_->get_encryption_key()), clean_up(uri));

  // Add written fragment info
  RETURN_NOT_OK_ELSE(add_written_fragment_info(uri), clean_up(uri));

  // The following will make the fragment visible
  auto ok_uri =
      URI(uri.remove_trailing_slash().to_string() + constants::ok_file_suffix);
  RETURN_NOT_OK_ELSE(storage_manager_->vfs()->touch(ok_uri), clean_up(uri));

  // Delete global write state
  global_write_state_.reset(nullptr);

  return st;
}

Status Writer::global_write() {
  // Applicable only to global write on dense/sparse arrays
  assert(layout_ == Layout::GLOBAL_ORDER);

  // Initialize the global write state if this is the first invocation
  if (!global_write_state_) {
    RETURN_CANCEL_OR_ERROR(init_global_write_state());
  }
  auto frag_meta = global_write_state_->frag_meta_.get();
  auto uri = frag_meta->fragment_uri();

  // Check for coordinate duplicates
  if (coords_info_.has_coords_) {
    RETURN_CANCEL_OR_ERROR(check_coord_dups());
    RETURN_CANCEL_OR_ERROR(check_global_order());
  }

  // Retrieve coordinate duplicates
  std::set<uint64_t> coord_dups;
  if (dedup_coords_)
    RETURN_CANCEL_OR_ERROR(compute_coord_dups(&coord_dups));

  std::unordered_map<std::string, std::vector<Tile>> tiles;
  RETURN_CANCEL_OR_ERROR_ELSE(
      prepare_full_tiles(coord_dups, &tiles), clean_up(uri));

  // Find number of tiles and gather stats
  uint64_t tile_num = 0;
  if (!tiles.empty()) {
    auto it = tiles.begin();
    bool var_size = array_schema_->var_size(it->first);
    const uint64_t t = 1 + (array_schema_->var_size(it->first) ? 1 : 0) +
                       (array_schema_->is_nullable(it->first) ? 1 : 0);
    tile_num = it->second.size() / t;

    uint64_t cell_num = 0;
    for (size_t t = 0; t < tile_num; ++t) {
      cell_num +=
          var_size ? it->second[2 * t].cell_num() : it->second[t].cell_num();
    }
    stats_->add_counter("cell_num", cell_num);
    stats_->add_counter("tile_num", tile_num);
  }

  // No cells to be written
  if (tile_num == 0) {
    return Status::Ok();
  }

  // Set new number of tiles in the fragment metadata
  auto new_num_tiles = frag_meta->tile_index_base() + tile_num;
  frag_meta->set_num_tiles(new_num_tiles);

  // Compute coordinate metadata (if coordinates are present)
  RETURN_CANCEL_OR_ERROR_ELSE(
      compute_coords_metadata(tiles, frag_meta), clean_up(uri));

  // Filter all tiles
  RETURN_CANCEL_OR_ERROR_ELSE(filter_tiles(&tiles), clean_up(uri));

  // Write tiles for all attributes
  RETURN_CANCEL_OR_ERROR_ELSE(
      write_all_tiles(frag_meta, &tiles), clean_up(uri));

  // Increment the tile index base for the next global order write.
  frag_meta->set_tile_index_base(new_num_tiles);

  return Status::Ok();
}

Status Writer::global_write_handle_last_tile() {
  if (all_last_tiles_empty())
    return Status::Ok();

  // Reserve space for the last tile in the fragment metadata
  auto meta = global_write_state_->frag_meta_.get();
  meta->set_num_tiles(meta->tile_index_base() + 1);
  const auto& uri = global_write_state_->frag_meta_->fragment_uri();

  // Filter last tiles
  std::unordered_map<std::string, std::vector<Tile>> tiles;
  RETURN_CANCEL_OR_ERROR_ELSE(filter_last_tiles(&tiles), clean_up(uri));

  // Write the last tiles
  RETURN_CANCEL_OR_ERROR(write_all_tiles(meta, &tiles));

  // Increment the tile index base.
  meta->set_tile_index_base(meta->tile_index_base() + 1);

  return Status::Ok();
}

Status Writer::filter_last_tiles(
    std::unordered_map<std::string, std::vector<Tile>>* tiles) {
  // Initialize attribute and coordinate tiles
  for (const auto& it : buffers_)
    (*tiles)[it.first] = std::vector<Tile>();

  // Prepare the tiles first
  uint64_t num = buffers_.size();
  auto status =
      parallel_for(storage_manager_->compute_tp(), 0, num, [&](uint64_t i) {
        auto buff_it = buffers_.begin();
        std::advance(buff_it, i);
        const auto& name = &(buff_it->first);

        auto& last_tile = std::get<0>(global_write_state_->last_tiles_[*name]);
        auto& last_tile_var =
            std::get<1>(global_write_state_->last_tiles_[*name]);
        auto& last_tile_validity =
            std::get<2>(global_write_state_->last_tiles_[*name]);

        if (!last_tile.empty()) {
          std::vector<Tile>& tiles_ref = (*tiles)[*name];
          // Note making shallow clones here, as it's not necessary to copy the
          // underlying tile Buffers.
          tiles_ref.push_back(last_tile.clone(false));
          if (!last_tile_var.empty())
            tiles_ref.push_back(last_tile_var.clone(false));
          if (!last_tile_validity.empty())
            tiles_ref.push_back(last_tile_validity.clone(false));
        }
        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  // Compute coordinates metadata
  auto meta = global_write_state_->frag_meta_.get();
  RETURN_NOT_OK(compute_coords_metadata(*tiles, meta));

  // Gather stats
  stats_->add_counter("cell_num", tiles->begin()->second[0].cell_num());
  stats_->add_counter("tile_num", 1);

  // Filter tiles
  RETURN_NOT_OK(filter_tiles(tiles));

  return Status::Ok();
}

bool Writer::all_last_tiles_empty() const {
  // See if any last attribute/coordinate tiles are nonempty
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    auto& last_tile = std::get<0>(global_write_state_->last_tiles_[name]);
    if (!last_tile.empty())
      return false;
  }

  return true;
}

Status Writer::init_global_write_state() {
  // Create global array state object
  if (global_write_state_ != nullptr)
    return LOG_STATUS(
        Status::WriterError("Cannot initialize global write state; State not "
                            "properly finalized"));
  global_write_state_.reset(new GlobalWriteState);

  // Create fragment
  RETURN_NOT_OK(create_fragment(
      !coords_info_.has_coords_, &(global_write_state_->frag_meta_)));
  auto uri = global_write_state_->frag_meta_->fragment_uri();

  // Initialize global write state for attribute and coordinates
  for (const auto& it : buffers_) {
    // Initialize last tiles
    const auto& name = it.first;
    auto last_tile_tuple = std::pair<std::string, std::tuple<Tile, Tile, Tile>>(
        name, std::tuple<Tile, Tile, Tile>(Tile(), Tile(), Tile()));
    auto it_ret = global_write_state_->last_tiles_.emplace(last_tile_tuple);

    if (!array_schema_->var_size(name)) {
      auto& last_tile = std::get<0>(it_ret.first->second);
      if (!array_schema_->is_nullable(name)) {
        RETURN_NOT_OK_ELSE(init_tile(name, &last_tile), clean_up(uri));
      } else {
        auto& last_tile_validity = std::get<2>(it_ret.first->second);
        RETURN_NOT_OK_ELSE(
            init_tile_nullable(name, &last_tile, &last_tile_validity),
            clean_up(uri));
      }
    } else {
      auto& last_tile = std::get<0>(it_ret.first->second);
      auto& last_tile_var = std::get<1>(it_ret.first->second);
      if (!array_schema_->is_nullable(name)) {
        RETURN_NOT_OK_ELSE(
            init_tile(name, &last_tile, &last_tile_var), clean_up(uri));
      } else {
        auto& last_tile_validity = std::get<2>(it_ret.first->second);
        RETURN_NOT_OK_ELSE(
            init_tile_nullable(
                name, &last_tile, &last_tile_var, &last_tile_validity),
            clean_up(uri));
      }
    }

    // Initialize cells written
    global_write_state_->cells_written_[name] = 0;
  }

  return Status::Ok();
}

Status Writer::init_tile(const std::string& name, Tile* tile) const {
  // For easy reference
  auto cell_size = array_schema_->cell_size(name);
  auto type = array_schema_->type(name);
  auto domain = array_schema_->domain();
  auto capacity = array_schema_->capacity();
  auto cell_num_per_tile =
      coords_info_.has_coords_ ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * cell_size;

  // Initialize
  RETURN_NOT_OK(tile->init_unfiltered(
      array_schema_->write_version(), type, tile_size, cell_size, 0));

  return Status::Ok();
}

Status Writer::init_tile(
    const std::string& name, Tile* tile, Tile* tile_var) const {
  // For easy reference
  auto type = array_schema_->type(name);
  auto domain = array_schema_->domain();
  auto capacity = array_schema_->capacity();
  auto cell_num_per_tile =
      coords_info_.has_coords_ ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * constants::cell_var_offset_size;

  // Initialize
  RETURN_NOT_OK(tile->init_unfiltered(
      array_schema_->write_version(),
      constants::cell_var_offset_type,
      tile_size,
      constants::cell_var_offset_size,
      0));
  RETURN_NOT_OK(tile_var->init_unfiltered(
      array_schema_->write_version(), type, tile_size, datatype_size(type), 0));
  return Status::Ok();
}

Status Writer::init_tile_nullable(
    const std::string& name, Tile* tile, Tile* tile_validity) const {
  // For easy reference
  auto cell_size = array_schema_->cell_size(name);
  auto type = array_schema_->type(name);
  auto domain = array_schema_->domain();
  auto capacity = array_schema_->capacity();
  auto cell_num_per_tile =
      coords_info_.has_coords_ ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * cell_size;

  // Initialize
  RETURN_NOT_OK(tile->init_unfiltered(
      array_schema_->write_version(), type, tile_size, cell_size, 0));
  RETURN_NOT_OK(tile_validity->init_unfiltered(
      array_schema_->write_version(),
      constants::cell_validity_type,
      tile_size,
      constants::cell_validity_size,
      0));

  return Status::Ok();
}

Status Writer::init_tile_nullable(
    const std::string& name,
    Tile* tile,
    Tile* tile_var,
    Tile* tile_validity) const {
  // For easy reference
  auto type = array_schema_->type(name);
  auto domain = array_schema_->domain();
  auto capacity = array_schema_->capacity();
  auto cell_num_per_tile =
      coords_info_.has_coords_ ? capacity : domain->cell_num_per_tile();
  auto tile_size = cell_num_per_tile * constants::cell_var_offset_size;

  // Initialize
  RETURN_NOT_OK(tile->init_unfiltered(
      array_schema_->write_version(),
      constants::cell_var_offset_type,
      tile_size,
      constants::cell_var_offset_size,
      0));
  RETURN_NOT_OK(tile_var->init_unfiltered(
      array_schema_->write_version(), type, tile_size, datatype_size(type), 0));
  RETURN_NOT_OK(tile_validity->init_unfiltered(
      array_schema_->write_version(),
      constants::cell_validity_type,
      tile_size,
      constants::cell_validity_size,
      0));

  return Status::Ok();
}

Status Writer::init_tiles(
    const std::string& name,
    uint64_t tile_num,
    std::vector<Tile>* tiles) const {
  // Initialize tiles
  const bool var_size = array_schema_->var_size(name);
  const bool nullable = array_schema_->is_nullable(name);
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

Status Writer::new_fragment_name(
    uint64_t timestamp, uint32_t format_version, std::string* frag_uri) const {
  timestamp = (timestamp != 0) ? timestamp : utils::time::timestamp_now_ms();

  if (frag_uri == nullptr)
    return Status::WriterError("Null fragment uri argument.");
  std::string uuid;
  frag_uri->clear();
  RETURN_NOT_OK(uuid::generate_uuid(&uuid, false));
  std::stringstream ss;
  ss << "/__" << timestamp << "_" << timestamp << "_" << uuid << "_"
     << format_version;

  *frag_uri = ss.str();
  return Status::Ok();
}

void Writer::nuke_global_write_state() {
  auto meta = global_write_state_->frag_meta_.get();
  close_files(meta);
  storage_manager_->vfs()->remove_dir(meta->fragment_uri());
  global_write_state_.reset(nullptr);
}

void Writer::optimize_layout_for_1D() {
  if (array_schema_->dim_num() == 1 && layout_ != Layout::GLOBAL_ORDER &&
      layout_ != Layout::UNORDERED)
    layout_ = array_schema_->cell_order();
}

Status Writer::check_extra_element() {
  for (const auto& it : buffers_) {
    const auto& attr = it.first;
    if (!array_schema_->var_size(attr) || array_schema_->is_dim(attr))
      continue;

    const void* buffer_off = it.second.buffer_;
    uint64_t* buffer_off_size = it.second.buffer_size_;
    const auto num_offsets = *buffer_off_size / constants::cell_var_offset_size;
    const uint64_t* buffer_val_size = it.second.buffer_var_size_;
    const uint64_t attr_datatype_size =
        datatype_size(array_schema_->type(attr));
    const uint64_t max_offset = offsets_format_mode_ == "bytes" ?
                                    *buffer_val_size :
                                    *buffer_val_size / attr_datatype_size;
    const uint64_t last_offset =
        get_offset_buffer_element(buffer_off, num_offsets - 1);

    if (last_offset != max_offset)
      return LOG_STATUS(Status::WriterError(
          "Invalid offsets for attribute " + attr +
          "; the last offset: " + std::to_string(last_offset) +
          " is not equal to the size of the data buffer: " +
          std::to_string(max_offset)));
  }

  return Status::Ok();
}

Status Writer::ordered_write() {
  // Applicable only to ordered write on dense arrays
  assert(layout_ == Layout::ROW_MAJOR || layout_ == Layout::COL_MAJOR);
  assert(array_schema_->dense());

  auto type = array_schema_->domain()->dimension(0)->type();
  switch (type) {
    case Datatype::INT8:
      return ordered_write<int8_t>();
    case Datatype::UINT8:
      return ordered_write<uint8_t>();
    case Datatype::INT16:
      return ordered_write<int16_t>();
    case Datatype::UINT16:
      return ordered_write<uint16_t>();
    case Datatype::INT32:
      return ordered_write<int32_t>();
    case Datatype::UINT32:
      return ordered_write<uint32_t>();
    case Datatype::INT64:
      return ordered_write<int64_t>();
    case Datatype::UINT64:
      return ordered_write<uint64_t>();
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
      return ordered_write<int64_t>();
    default:
      return LOG_STATUS(Status::WriterError(
          "Cannot write in ordered layout; Unsupported domain type"));
  }

  return Status::Ok();
}

template <class T>
Status Writer::ordered_write() {
  auto timer_se = stats_->start_timer("filter_tile");

  // Create new fragment
  tdb_shared_ptr<FragmentMetadata> frag_meta;
  RETURN_CANCEL_OR_ERROR(create_fragment(true, &frag_meta));
  const auto& uri = frag_meta->fragment_uri();

  // Create a dense tiler
  DenseTiler<T> dense_tiler(
      &buffers_,
      &subarray_,
      stats_,
      offsets_format_mode_,
      offsets_bitsize_,
      offsets_extra_element_);
  auto tile_num = dense_tiler.tile_num();

  // Set number of tiles in the fragment metadata
  frag_meta->set_num_tiles(tile_num);

  // Prepare, filter and write tiles for all attributes
  auto attr_num = buffers_.size();
  auto compute_tp = storage_manager_->compute_tp();
  auto thread_num = compute_tp->concurrency_level();
  if (attr_num > tile_num) {  // Parallelize over attributes
    auto st = parallel_for(compute_tp, 0, attr_num, [&](uint64_t i) {
      auto buff_it = buffers_.begin();
      std::advance(buff_it, i);
      const auto& attr = buff_it->first;
      return prepare_filter_and_write_tiles<T>(
          attr, frag_meta.get(), &dense_tiler, 1);
    });
    RETURN_NOT_OK_ELSE(st, storage_manager_->vfs()->remove_dir(uri));
  } else {  // Parallelize over tiles
    size_t i = 0;
    for (const auto& buff : buffers_) {
      const auto& attr = buff.first;
      RETURN_NOT_OK_ELSE(
          prepare_filter_and_write_tiles<T>(
              attr, frag_meta.get(), &dense_tiler, thread_num),
          storage_manager_->vfs()->remove_dir(uri));
      ++i;
    }
  }

  // Write the fragment metadata
  RETURN_CANCEL_OR_ERROR_ELSE(
      frag_meta->store(array_->get_encryption_key()),
      storage_manager_->vfs()->remove_dir(uri));

  // Add written fragment info
  RETURN_NOT_OK_ELSE(
      add_written_fragment_info(uri), storage_manager_->vfs()->remove_dir(uri));

  // The following will make the fragment visible
  auto ok_uri =
      URI(uri.remove_trailing_slash().to_string() + constants::ok_file_suffix);
  RETURN_NOT_OK_ELSE(
      storage_manager_->vfs()->touch(ok_uri),
      storage_manager_->vfs()->remove_dir(uri));

  return Status::Ok();
}

Status Writer::prepare_full_tiles(
    const std::set<uint64_t>& coord_dups,
    std::unordered_map<std::string, std::vector<Tile>>* tiles) const {
  auto timer_se = stats_->start_timer("prepare_tiles");

  // Initialize attribute and coordinate tiles
  for (const auto& it : buffers_)
    (*tiles)[it.first] = std::vector<Tile>();

  auto num = buffers_.size();
  auto status =
      parallel_for(storage_manager_->compute_tp(), 0, num, [&](uint64_t i) {
        auto buff_it = buffers_.begin();
        std::advance(buff_it, i);
        const auto& name = buff_it->first;
        RETURN_CANCEL_OR_ERROR(
            prepare_full_tiles(name, coord_dups, &(*tiles)[name]));
        return Status::Ok();
      });

  RETURN_NOT_OK(status);

  return Status::Ok();
}

Status Writer::prepare_full_tiles(
    const std::string& name,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  return array_schema_->var_size(name) ?
             prepare_full_tiles_var(name, coord_dups, tiles) :
             prepare_full_tiles_fixed(name, coord_dups, tiles);
}

Status Writer::prepare_full_tiles_fixed(
    const std::string& name,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  // For easy reference
  auto nullable = array_schema_->is_nullable(name);
  auto it = buffers_.find(name);
  auto buffer = (unsigned char*)it->second.buffer_;
  auto buffer_validity = (unsigned char*)it->second.validity_vector_.buffer();
  auto buffer_size = it->second.buffer_size_;
  auto cell_size = array_schema_->cell_size(name);
  auto capacity = array_schema_->capacity();
  auto cell_num = *buffer_size / cell_size;
  auto domain = array_schema_->domain();
  auto cell_num_per_tile =
      coords_info_.has_coords_ ? capacity : domain->cell_num_per_tile();

  // Do nothing if there are no cells to write
  if (cell_num == 0)
    return Status::Ok();

  // First fill the last tile
  auto& last_tile = std::get<0>(global_write_state_->last_tiles_[name]);
  auto& last_tile_validity =
      std::get<2>(global_write_state_->last_tiles_[name]);
  uint64_t cell_idx = 0;
  if (!last_tile.empty()) {
    if (coord_dups.empty()) {
      do {
        RETURN_NOT_OK(
            last_tile.write(buffer + cell_idx * cell_size, cell_size));
        if (nullable) {
          RETURN_NOT_OK(last_tile_validity.write(
              buffer_validity + cell_idx * constants::cell_validity_size,
              constants::cell_validity_size));
        }
        ++cell_idx;
      } while (!last_tile.full() && cell_idx != cell_num);
    } else {
      do {
        if (coord_dups.find(cell_idx) == coord_dups.end()) {
          RETURN_NOT_OK(
              last_tile.write(buffer + cell_idx * cell_size, cell_size));
          if (nullable) {
            RETURN_NOT_OK(last_tile_validity.write(
                buffer_validity + cell_idx * constants::cell_validity_size,
                constants::cell_validity_size));
          }
        }
        ++cell_idx;
      } while (!last_tile.full() && cell_idx != cell_num);
    }
  }

  // Initialize full tiles and set previous last tile as first tile
  auto full_tile_num =
      (cell_num - cell_idx) / cell_num_per_tile + (int)last_tile.full();
  auto cell_num_to_write =
      (full_tile_num - last_tile.full()) * cell_num_per_tile;

  if (full_tile_num > 0) {
    const uint64_t t = 1 + (nullable ? 1 : 0);
    tiles->resize(t * full_tile_num);

    for (uint64_t i = 0; i < tiles->size(); i += t)
      if (!nullable)
        RETURN_NOT_OK(init_tile(name, &((*tiles)[i])));
      else
        RETURN_NOT_OK(
            init_tile_nullable(name, &((*tiles)[i]), &((*tiles)[i + 1])));

    // Handle last tile (it must be either full or empty)
    if (last_tile.full()) {
      (*tiles)[0] = last_tile;
      last_tile.reset();
      if (nullable) {
        (*tiles)[1] = last_tile_validity;
        last_tile_validity.reset();
      }
    } else {
      assert(last_tile.empty());
      if (nullable) {
        assert(last_tile_validity.empty());
      }
    }

    // Write all remaining cells one by one
    if (coord_dups.empty()) {
      for (uint64_t tile_idx = 0, i = 0; i < cell_num_to_write;) {
        if ((*tiles)[tile_idx].full())
          tile_idx += t;

        RETURN_NOT_OK((*tiles)[tile_idx].write(
            buffer + cell_idx * cell_size, cell_size * cell_num_per_tile));

        if (nullable) {
          RETURN_NOT_OK((*tiles)[tile_idx + 1].write(
              buffer_validity + cell_idx * constants::cell_validity_size,
              constants::cell_validity_size * cell_num_per_tile));
        }

        cell_idx += cell_num_per_tile;
        i += cell_num_per_tile;
      }
    } else {
      for (uint64_t tile_idx = 0, i = 0; i < cell_num_to_write;
           ++cell_idx, ++i) {
        if (coord_dups.find(cell_idx) == coord_dups.end()) {
          if ((*tiles)[tile_idx].full())
            tile_idx += t;

          RETURN_NOT_OK((*tiles)[tile_idx].write(
              buffer + cell_idx * cell_size, cell_size));

          if (nullable) {
            RETURN_NOT_OK((*tiles)[tile_idx + 1].write(
                buffer_validity + cell_idx * constants::cell_validity_size,
                constants::cell_validity_size));
          }
        }
      }
    }
  }

  // Potentially fill the last tile
  assert(cell_num - cell_idx < cell_num_per_tile - last_tile.cell_num());
  if (coord_dups.empty()) {
    for (; cell_idx < cell_num; ++cell_idx) {
      RETURN_NOT_OK(last_tile.write(buffer + cell_idx * cell_size, cell_size));
      if (nullable) {
        RETURN_NOT_OK(last_tile_validity.write(
            buffer_validity + cell_idx * constants::cell_validity_size,
            constants::cell_validity_size));
      }
    }
  } else {
    for (; cell_idx < cell_num; ++cell_idx) {
      if (coord_dups.find(cell_idx) == coord_dups.end())
        RETURN_NOT_OK(
            last_tile.write(buffer + cell_idx * cell_size, cell_size));
      if (nullable) {
        RETURN_NOT_OK(last_tile_validity.write(
            buffer_validity + cell_idx * constants::cell_validity_size,
            constants::cell_validity_size));
      }
    }
  }

  global_write_state_->cells_written_[name] += cell_num;

  return Status::Ok();
}

Status Writer::prepare_full_tiles_var(
    const std::string& name,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  // For easy reference
  auto it = buffers_.find(name);
  auto nullable = array_schema_->is_nullable(name);
  auto buffer = (uint64_t*)it->second.buffer_;
  auto buffer_var = (unsigned char*)it->second.buffer_var_;
  auto buffer_validity = (uint8_t*)it->second.validity_vector_.buffer();
  auto buffer_size = get_offset_buffer_size(*it->second.buffer_size_);
  auto buffer_var_size = it->second.buffer_var_size_;
  auto capacity = array_schema_->capacity();
  auto cell_num = buffer_size / constants::cell_var_offset_size;
  auto domain = array_schema_->domain();
  auto cell_num_per_tile =
      coords_info_.has_coords_ ? capacity : domain->cell_num_per_tile();
  auto attr_datatype_size = datatype_size(array_schema_->type(name));
  uint64_t offset, var_size;

  // Do nothing if there are no cells to write
  if (cell_num == 0)
    return Status::Ok();

  // First fill the last tile
  auto& last_tile_tuple = global_write_state_->last_tiles_[name];
  auto& last_tile = std::get<0>(last_tile_tuple);
  auto& last_tile_var = std::get<1>(last_tile_tuple);
  auto& last_tile_validity = std::get<2>(last_tile_tuple);

  uint64_t cell_idx = 0;
  if (!last_tile.empty()) {
    if (coord_dups.empty()) {
      do {
        // Write offset.
        offset = last_tile_var.size();
        RETURN_NOT_OK(last_tile.write(&offset, sizeof(offset)));

        // Write var-sized value(s).
        auto buff_offset =
            prepare_buffer_offset(buffer, cell_idx, attr_datatype_size);
        var_size = (cell_idx == cell_num - 1) ?
                       *buffer_var_size - buff_offset :
                       prepare_buffer_offset(
                           buffer, cell_idx + 1, attr_datatype_size) -
                           buff_offset;
        RETURN_NOT_OK(last_tile_var.write(buffer_var + buff_offset, var_size));

        // Write validity value(s).
        if (nullable)
          RETURN_NOT_OK(last_tile_validity.write(
              buffer_validity + cell_idx, constants::cell_validity_size));

        ++cell_idx;
      } while (!last_tile.full() && cell_idx != cell_num);
    } else {
      do {
        if (coord_dups.find(cell_idx) == coord_dups.end()) {
          // Write offset.
          offset = last_tile_var.size();
          RETURN_NOT_OK(last_tile.write(&offset, sizeof(offset)));

          // Write var-sized value(s).
          auto buff_offset =
              prepare_buffer_offset(buffer, cell_idx, attr_datatype_size);
          var_size = (cell_idx == cell_num - 1) ?
                         *buffer_var_size - buff_offset :
                         prepare_buffer_offset(
                             buffer, cell_idx + 1, attr_datatype_size) -
                             buff_offset;
          RETURN_NOT_OK(
              last_tile_var.write(buffer_var + buff_offset, var_size));

          // Write validity value(s).
          if (nullable)
            RETURN_NOT_OK(last_tile_validity.write(
                buffer_validity + cell_idx, constants::cell_validity_size));
        }

        ++cell_idx;
      } while (!last_tile.full() && cell_idx != cell_num);
    }
  }

  // Initialize full tiles and set previous last tile as first tile
  auto full_tile_num =
      (cell_num - cell_idx) / cell_num_per_tile + last_tile.full();
  auto cell_num_to_write =
      (full_tile_num - last_tile.full()) * cell_num_per_tile;

  if (full_tile_num > 0) {
    const uint64_t t = 2 + (nullable ? 1 : 0);
    tiles->resize(t * full_tile_num);
    auto tiles_len = tiles->size();
    for (uint64_t i = 0; i < tiles_len; i += t)
      if (!nullable)
        RETURN_NOT_OK(init_tile(name, &((*tiles)[i]), &((*tiles)[i + 1])));
      else
        RETURN_NOT_OK(init_tile_nullable(
            name, &((*tiles)[i]), &((*tiles)[i + 1]), &((*tiles)[i + 2])));

    // Handle last tile (it must be either full or empty)
    if (last_tile.full()) {
      (*tiles)[0] = last_tile;
      last_tile.reset();
      (*tiles)[1] = last_tile_var;
      last_tile_var.reset();
      if (nullable) {
        (*tiles)[2] = last_tile_validity;
        last_tile_validity.reset();
      }
    } else {
      assert(last_tile.empty());
      assert(last_tile_var.empty());
      if (nullable)
        assert(last_tile_validity.empty());
    }

    // Write all remaining cells one by one
    if (coord_dups.empty()) {
      for (uint64_t tile_idx = 0, i = 0; i < cell_num_to_write;
           ++cell_idx, ++i) {
        if ((*tiles)[tile_idx].full())
          tile_idx += t;

        // Write offset.
        offset = (*tiles)[tile_idx + 1].size();
        RETURN_NOT_OK((*tiles)[tile_idx].write(&offset, sizeof(offset)));

        // Write var-sized value(s).
        auto buff_offset =
            prepare_buffer_offset(buffer, cell_idx, attr_datatype_size);
        var_size = (cell_idx == cell_num - 1) ?
                       *buffer_var_size - buff_offset :
                       prepare_buffer_offset(
                           buffer, cell_idx + 1, attr_datatype_size) -
                           buff_offset;
        RETURN_NOT_OK(
            (*tiles)[tile_idx + 1].write(buffer_var + buff_offset, var_size));

        // Write validity value(s).
        if (nullable)
          RETURN_NOT_OK((*tiles)[tile_idx + 2].write(
              buffer_validity + cell_idx, constants::cell_validity_size));
      }
    } else {
      for (uint64_t tile_idx = 0, i = 0; i < cell_num_to_write;
           ++cell_idx, ++i) {
        if (coord_dups.find(cell_idx) == coord_dups.end()) {
          if ((*tiles)[tile_idx].full())
            tile_idx += t;

          // Write offset.
          offset = (*tiles)[tile_idx + 1].size();
          RETURN_NOT_OK((*tiles)[tile_idx].write(&offset, sizeof(offset)));

          // Write var-sized value(s).
          auto buff_offset =
              prepare_buffer_offset(buffer, cell_idx, attr_datatype_size);
          var_size = (cell_idx == cell_num - 1) ?
                         *buffer_var_size - buff_offset :
                         prepare_buffer_offset(
                             buffer, cell_idx + 1, attr_datatype_size) -
                             buff_offset;
          RETURN_NOT_OK(
              (*tiles)[tile_idx + 1].write(buffer_var + buff_offset, var_size));

          // Write validity value(s).
          if (nullable)
            RETURN_NOT_OK((*tiles)[tile_idx + 2].write(
                buffer_validity + cell_idx, constants::cell_validity_size));
        }
      }
    }
  }

  // Potentially fill the last tile
  assert(cell_num - cell_idx < cell_num_per_tile - last_tile.cell_num());
  if (coord_dups.empty()) {
    for (; cell_idx < cell_num; ++cell_idx) {
      // Write offset.
      offset = last_tile_var.size();
      RETURN_NOT_OK(last_tile.write(&offset, sizeof(offset)));

      // Write var-sized value(s).
      auto buff_offset =
          prepare_buffer_offset(buffer, cell_idx, attr_datatype_size);
      var_size =
          (cell_idx == cell_num - 1) ?
              *buffer_var_size - buff_offset :
              prepare_buffer_offset(buffer, cell_idx + 1, attr_datatype_size) -
                  buff_offset;
      RETURN_NOT_OK(last_tile_var.write(buffer_var + buff_offset, var_size));

      // Write validity value(s).
      if (nullable)
        RETURN_NOT_OK(last_tile_validity.write(
            buffer_validity + cell_idx, constants::cell_validity_size));
    }
  } else {
    for (; cell_idx < cell_num; ++cell_idx) {
      if (coord_dups.find(cell_idx) == coord_dups.end()) {
        // Write offset.
        offset = last_tile_var.size();
        RETURN_NOT_OK(last_tile.write(&offset, sizeof(offset)));

        // Write var-sized value(s).
        auto buff_offset =
            prepare_buffer_offset(buffer, cell_idx, attr_datatype_size);
        var_size = (cell_idx == cell_num - 1) ?
                       *buffer_var_size - buff_offset :
                       prepare_buffer_offset(
                           buffer, cell_idx + 1, attr_datatype_size) -
                           buff_offset;
        RETURN_NOT_OK(last_tile_var.write(buffer_var + buff_offset, var_size));

        // Write validity value(s).
        if (nullable)
          RETURN_NOT_OK(last_tile_validity.write(
              buffer_validity + cell_idx, constants::cell_validity_size));
      }
    }
  }

  global_write_state_->cells_written_[name] += cell_num;

  return Status::Ok();
}

Status Writer::prepare_tiles(
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    std::unordered_map<std::string, std::vector<Tile>>* tiles) const {
  auto timer_se = stats_->start_timer("prepare_tiles");

  // Initialize attribute tiles
  tiles->clear();
  for (const auto& it : buffers_)
    (*tiles)[it.first] = std::vector<Tile>();

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

Status Writer::prepare_tiles(
    const std::string& name,
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  return array_schema_->var_size(name) ?
             prepare_tiles_var(name, cell_pos, coord_dups, tiles) :
             prepare_tiles_fixed(name, cell_pos, coord_dups, tiles);
}

Status Writer::prepare_tiles_fixed(
    const std::string& name,
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  // Trivial case
  if (cell_pos.empty())
    return Status::Ok();

  // For easy reference
  auto nullable = array_schema_->is_nullable(name);
  auto buffer = (unsigned char*)buffers_.find(name)->second.buffer_;
  auto buffer_validity =
      (unsigned char*)buffers_.find(name)->second.validity_vector_.buffer();
  auto cell_size = array_schema_->cell_size(name);
  auto cell_num = (uint64_t)cell_pos.size();
  auto capacity = array_schema_->capacity();
  auto dups_num = coord_dups.size();
  auto tile_num = utils::math::ceil(cell_num - dups_num, capacity);

  // Initialize tiles
  const uint64_t t = 1 + (nullable ? 1 : 0);
  tiles->resize(t * tile_num);
  for (uint64_t i = 0; i < tiles->size(); i += t) {
    if (!nullable)
      RETURN_NOT_OK(init_tile(name, &((*tiles)[i])));
    else
      RETURN_NOT_OK(
          init_tile_nullable(name, &((*tiles)[i]), &((*tiles)[i + 1])));
  }

  // Write all cells one by one
  if (dups_num == 0) {
    for (uint64_t i = 0, tile_idx = 0; i < cell_num; ++i) {
      if ((*tiles)[tile_idx].full())
        tile_idx += t;

      RETURN_NOT_OK((*tiles)[tile_idx].write(
          buffer + cell_pos[i] * cell_size, cell_size));
      if (nullable)
        RETURN_NOT_OK((*tiles)[tile_idx + 1].write(
            buffer_validity + cell_pos[i] * constants::cell_validity_size,
            constants::cell_validity_size));
    }
  } else {
    for (uint64_t i = 0, tile_idx = 0; i < cell_num; ++i) {
      if (coord_dups.find(cell_pos[i]) != coord_dups.end())
        continue;

      if ((*tiles)[tile_idx].full())
        tile_idx += t;

      RETURN_NOT_OK((*tiles)[tile_idx].write(
          buffer + cell_pos[i] * cell_size, cell_size));
      if (nullable)
        RETURN_NOT_OK((*tiles)[tile_idx + 1].write(
            buffer_validity + cell_pos[i] * constants::cell_validity_size,
            constants::cell_validity_size));
    }
  }

  return Status::Ok();
}

Status Writer::prepare_tiles_var(
    const std::string& name,
    const std::vector<uint64_t>& cell_pos,
    const std::set<uint64_t>& coord_dups,
    std::vector<Tile>* tiles) const {
  // For easy reference
  auto it = buffers_.find(name);
  auto nullable = array_schema_->is_nullable(name);
  auto buffer = it->second.buffer_;
  auto buffer_var = (unsigned char*)it->second.buffer_var_;
  auto buffer_validity = (uint8_t*)it->second.validity_vector_.buffer();
  auto buffer_var_size = it->second.buffer_var_size_;
  auto cell_num = (uint64_t)cell_pos.size();
  auto capacity = array_schema_->capacity();
  auto dups_num = coord_dups.size();
  auto tile_num = utils::math::ceil(cell_num - dups_num, capacity);
  auto attr_datatype_size = datatype_size(array_schema_->type(name));
  uint64_t offset;
  uint64_t var_size;

  // Initialize tiles
  const uint64_t t = 2 + (nullable ? 1 : 0);
  tiles->resize(t * tile_num);
  auto tiles_len = tiles->size();
  for (uint64_t i = 0; i < tiles_len; i += t) {
    if (!nullable)
      RETURN_NOT_OK(init_tile(name, &((*tiles)[i]), &((*tiles)[i + 1])));
    else
      RETURN_NOT_OK(init_tile_nullable(
          name, &((*tiles)[i]), &((*tiles)[i + 1]), &((*tiles)[i + 2])));
  }

  // Write all cells one by one
  if (dups_num == 0) {
    for (uint64_t i = 0, tile_idx = 0; i < cell_num; ++i) {
      if ((*tiles)[tile_idx].full())
        tile_idx += t;

      // Write offset.
      offset = (*tiles)[tile_idx + 1].size();
      RETURN_NOT_OK((*tiles)[tile_idx].write(&offset, sizeof(offset)));

      // Write var-sized value(s).
      auto buff_offset =
          prepare_buffer_offset(buffer, cell_pos[i], attr_datatype_size);
      var_size = (cell_pos[i] == cell_num - 1) ?
                     *buffer_var_size - buff_offset :
                     prepare_buffer_offset(
                         buffer, cell_pos[i] + 1, attr_datatype_size) -
                         buff_offset;
      RETURN_NOT_OK(
          (*tiles)[tile_idx + 1].write(buffer_var + buff_offset, var_size));

      // Write validity value(s).
      if (nullable) {
        RETURN_NOT_OK((*tiles)[tile_idx + 2].write(
            buffer_validity + cell_pos[i], constants::cell_validity_size));
      }
    }
  } else {
    for (uint64_t i = 0, tile_idx = 0; i < cell_num; ++i) {
      if (coord_dups.find(cell_pos[i]) != coord_dups.end())
        continue;

      if ((*tiles)[tile_idx].full())
        tile_idx += t;

      // Write offset.
      offset = (*tiles)[tile_idx + 1].size();
      RETURN_NOT_OK((*tiles)[tile_idx].write(&offset, sizeof(offset)));

      // Write var-sized value(s).
      auto buff_offset =
          prepare_buffer_offset(buffer, cell_pos[i], attr_datatype_size);
      var_size = (cell_pos[i] == cell_num - 1) ?
                     *buffer_var_size - buff_offset :
                     prepare_buffer_offset(
                         buffer, cell_pos[i] + 1, attr_datatype_size) -
                         buff_offset;
      RETURN_NOT_OK(
          (*tiles)[tile_idx + 1].write(buffer_var + buff_offset, var_size));

      // Write validity value(s).
      if (nullable) {
        RETURN_NOT_OK((*tiles)[tile_idx + 2].write(
            buffer_validity + cell_pos[i], constants::cell_validity_size));
      }
    }
  }

  return Status::Ok();
}

void Writer::reset() {
  if (global_write_state_ != nullptr)
    nuke_global_write_state();
  initialized_ = false;
}

Status Writer::sort_coords(std::vector<uint64_t>* cell_pos) const {
  auto timer_se = stats_->start_timer("sort_coords");

  // For easy reference
  auto domain = array_schema_->domain();
  auto cell_order = array_schema_->cell_order();

  // Prepare auxiliary vector for better performance
  auto dim_num = array_schema_->dim_num();
  std::vector<const QueryBuffer*> buffs(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    const auto& dim_name = array_schema_->dimension(d)->name();
    buffs[d] = &(buffers_.find(dim_name)->second);
  }

  // Populate cell_pos
  cell_pos->resize(coords_info_.coords_num_);
  for (uint64_t i = 0; i < coords_info_.coords_num_; ++i)
    (*cell_pos)[i] = i;

  // Sort the coordinates in global order
  if (cell_order != Layout::HILBERT) {  // Row- or col-major
    parallel_sort(
        storage_manager_->compute_tp(),
        cell_pos->begin(),
        cell_pos->end(),
        GlobalCmp(domain, &buffs));
  } else {  // Hilbert order
    std::vector<uint64_t> hilbert_values(coords_info_.coords_num_);
    RETURN_NOT_OK(calculate_hilbert_values(buffs, &hilbert_values));
    parallel_sort(
        storage_manager_->compute_tp(),
        cell_pos->begin(),
        cell_pos->end(),
        HilbertCmp(domain, &buffs, &hilbert_values));
  }

  return Status::Ok();
}

Status Writer::split_coords_buffer() {
  auto timer_se = stats_->start_timer("split_coords_buff");

  // Do nothing if the coordinates buffer is not set
  if (coords_info_.coords_buffer_ == nullptr)
    return Status::Ok();

  // For easy reference
  auto dim_num = array_schema_->dim_num();
  auto coord_size = array_schema_->domain()->dimension(0)->coord_size();
  auto coords_size = dim_num * coord_size;
  coords_info_.coords_num_ = *coords_info_.coords_buffer_size_ / coords_size;

  clear_coord_buffers();

  // New coord buffer allocations
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = array_schema_->dimension(d);
    const auto& dim_name = dim->name();
    auto coord_buffer_size = coords_info_.coords_num_ * dim->coord_size();
    auto it = coord_buffer_sizes_.emplace(dim_name, coord_buffer_size);
    QueryBuffer buff;
    buff.buffer_size_ = &(it.first->second);
    buff.buffer_ = tdb_malloc(coord_buffer_size);
    to_clean_.push_back(buff.buffer_);
    if (buff.buffer_ == nullptr)
      RETURN_NOT_OK(Status::WriterError(
          "Cannot split coordinate buffers; memory allocation failed"));
    buffers_[dim_name] = std::move(buff);
  }

  // Split coordinates
  auto coord = (unsigned char*)nullptr;
  for (unsigned d = 0; d < dim_num; ++d) {
    auto coord_size = array_schema_->dimension(d)->coord_size();
    const auto& dim_name = array_schema_->dimension(d)->name();
    auto buff = (unsigned char*)(buffers_[dim_name].buffer_);
    for (uint64_t c = 0; c < coords_info_.coords_num_; ++c) {
      coord = &(((unsigned char*)coords_info_
                     .coords_buffer_)[c * coords_size + d * coord_size]);
      std::memcpy(&(buff[c * coord_size]), coord, coord_size);
    }
  }

  return Status::Ok();
}

Status Writer::unordered_write() {
  // Applicable only to unordered write on dense/sparse arrays
  assert(layout_ == Layout::UNORDERED);

  // Sort coordinates first
  std::vector<uint64_t> cell_pos;
  RETURN_CANCEL_OR_ERROR(sort_coords(&cell_pos));

  // Check for coordinate duplicates
  RETURN_CANCEL_OR_ERROR(check_coord_dups(cell_pos));

  // Retrieve coordinate duplicates
  std::set<uint64_t> coord_dups;
  if (dedup_coords_)
    RETURN_CANCEL_OR_ERROR(compute_coord_dups(cell_pos, &coord_dups));

  // Create new fragment
  tdb_shared_ptr<FragmentMetadata> frag_meta;
  RETURN_CANCEL_OR_ERROR(create_fragment(false, &frag_meta));
  const auto& uri = frag_meta->fragment_uri();

  // Prepare tiles
  std::unordered_map<std::string, std::vector<Tile>> tiles;
  RETURN_CANCEL_OR_ERROR_ELSE(
      prepare_tiles(cell_pos, coord_dups, &tiles), clean_up(uri));

  // Clear the boolean vector for coordinate duplicates
  coord_dups.clear();

  // No tiles
  if (tiles.empty() || tiles.begin()->second.empty())
    return Status::Ok();

  // Set the number of tiles in the metadata
  auto it = tiles.begin();
  const uint64_t tile_num_divisor =
      1 + (array_schema_->var_size(it->first) ? 1 : 0) +
      (array_schema_->is_nullable(it->first) ? 1 : 0);
  auto tile_num = it->second.size() / tile_num_divisor;
  frag_meta->set_num_tiles(tile_num);

  stats_->add_counter("tile_num", tile_num);
  stats_->add_counter("cell_num", cell_pos.size());

  // Compute coordinates metadata
  RETURN_CANCEL_OR_ERROR_ELSE(
      compute_coords_metadata(tiles, frag_meta.get()), clean_up(uri));

  // Filter all tiles
  RETURN_CANCEL_OR_ERROR_ELSE(filter_tiles(&tiles), clean_up(uri));

  // Write tiles for all attributes and coordinates
  RETURN_CANCEL_OR_ERROR_ELSE(
      write_all_tiles(frag_meta.get(), &tiles), clean_up(uri));

  // Write the fragment metadata
  RETURN_CANCEL_OR_ERROR_ELSE(
      frag_meta->store(array_->get_encryption_key()), clean_up(uri));

  // Add written fragment info
  RETURN_NOT_OK_ELSE(add_written_fragment_info(uri), clean_up(uri));

  // The following will make the fragment visible
  auto ok_uri =
      URI(uri.remove_trailing_slash().to_string() + constants::ok_file_suffix);
  RETURN_NOT_OK_ELSE(storage_manager_->vfs()->touch(ok_uri), clean_up(uri));

  return Status::Ok();
}

Status Writer::write_empty_cell_range_to_tile(
    const uint64_t cell_num,
    const uint32_t cell_val_num,
    Tile* const tile) const {
  auto type = tile->type();
  auto fill_size = datatype_size(type);
  auto fill_value = constants::fill_value(type);
  assert(fill_value != nullptr);

  for (uint64_t i = 0; i < cell_num; ++i) {
    for (uint64_t j = 0; j < cell_val_num; ++j) {
      RETURN_NOT_OK(tile->write(fill_value, fill_size));
    }
  }

  return Status::Ok();
}

Status Writer::write_empty_cell_range_to_tile_nullable(
    const uint64_t cell_num,
    const uint32_t cell_val_num,
    Tile* const tile,
    Tile* const tile_validity) const {
  auto type = tile->type();
  auto fill_size = datatype_size(type);
  auto fill_value = constants::fill_value(type);
  assert(fill_value != nullptr);

  for (uint64_t i = 0; i < cell_num; ++i) {
    for (uint64_t j = 0; j < cell_val_num; ++j) {
      RETURN_NOT_OK(tile->write(fill_value, fill_size));
    }

    // Write validity empty value, one per cell.
    uint8_t empty_validity_value = 0;
    RETURN_NOT_OK(tile_validity->write(
        &empty_validity_value, constants::cell_validity_size));
  }

  return Status::Ok();
}

Status Writer::write_empty_cell_range_to_tile_var(
    uint64_t num, Tile* tile, Tile* tile_var) const {
  auto type = tile_var->type();
  auto fill_size = datatype_size(type);
  auto fill_value = constants::fill_value(type);
  assert(fill_value != nullptr);

  for (uint64_t i = 0; i < num; ++i) {
    // Write next offset
    uint64_t next_offset = tile_var->size();
    RETURN_NOT_OK(tile->write(&next_offset, sizeof(uint64_t)));

    // Write variable-sized empty value
    RETURN_NOT_OK(tile_var->write(fill_value, fill_size));
  }

  return Status::Ok();
}

Status Writer::write_empty_cell_range_to_tile_var_nullable(
    uint64_t num, Tile* tile, Tile* tile_var, Tile* tile_validity) const {
  auto type = tile_var->type();
  auto fill_size = datatype_size(type);
  auto fill_value = constants::fill_value(type);
  assert(fill_value != nullptr);

  for (uint64_t i = 0; i < num; ++i) {
    // Write next offset
    const uint64_t next_offset = tile_var->size();
    RETURN_NOT_OK(tile->write(&next_offset, sizeof(uint64_t)));

    // Write variable-sized empty value
    RETURN_NOT_OK(tile_var->write(fill_value, fill_size));

    // Write validity empty value
    uint8_t empty_validity_value = 0;
    RETURN_NOT_OK(tile_validity->write(
        &empty_validity_value, constants::cell_validity_size));
  }

  return Status::Ok();
}

Status Writer::write_cell_range_to_tile(
    ConstBuffer* buff, uint64_t start, uint64_t end, Tile* tile) const {
  auto fixed_cell_size = tile->cell_size();
  buff->set_offset(start * fixed_cell_size);
  return tile->write(buff, (end - start + 1) * fixed_cell_size);
}

Status Writer::write_cell_range_to_tile_nullable(
    ConstBuffer* buff,
    ConstBuffer* buff_validity,
    uint64_t start,
    uint64_t end,
    Tile* tile,
    Tile* tile_validity) const {
  auto fixed_cell_size = tile->cell_size();
  buff->set_offset(start * fixed_cell_size);
  RETURN_NOT_OK(tile->write(buff, (end - start + 1) * fixed_cell_size));

  auto validity_cell_size = constants::cell_validity_size;
  buff_validity->set_offset(start * validity_cell_size);
  RETURN_NOT_OK(tile_validity->write(
      buff_validity, (end - start + 1) * validity_cell_size));

  return Status::Ok();
}

Status Writer::write_cell_range_to_tile_var(
    ConstBuffer* buff,
    ConstBuffer* buff_var,
    uint64_t start,
    uint64_t end,
    uint64_t attr_datatype_size,
    Tile* tile,
    Tile* tile_var) const {
  auto buff_cell_num = buff->size() / sizeof(uint64_t);

  for (auto i = start; i <= end; ++i) {
    // Write next offset
    uint64_t next_offset = tile_var->size();
    RETURN_NOT_OK(tile->write(&next_offset, sizeof(uint64_t)));

    // Write variable-sized value
    auto last_cell = (i == buff_cell_num - 1);
    auto start_offset =
        prepare_buffer_offset(buff->data(), i, attr_datatype_size);
    auto end_offset = last_cell ? buff_var->size() :
                                  prepare_buffer_offset(
                                      buff->data(), i + 1, attr_datatype_size);
    auto cell_var_size = end_offset - start_offset;
    buff_var->set_offset(start_offset);
    RETURN_NOT_OK(tile_var->write(buff_var, cell_var_size));
  }

  return Status::Ok();
}

Status Writer::write_cell_range_to_tile_var_nullable(
    ConstBuffer* buff,
    ConstBuffer* buff_var,
    ConstBuffer* buff_validity,
    uint64_t start,
    uint64_t end,
    uint64_t attr_datatype_size,
    Tile* tile,
    Tile* tile_var,
    Tile* tile_validity) const {
  auto buff_cell_num = buff->size() / sizeof(uint64_t);

  for (auto i = start; i <= end; ++i) {
    // Write next offset.
    uint64_t next_offset = tile_var->size();
    RETURN_NOT_OK(tile->write(&next_offset, sizeof(uint64_t)));

    // Write variable-sized value(s).
    auto last_cell = (i == buff_cell_num - 1);
    auto start_offset =
        prepare_buffer_offset(buff->data(), i, attr_datatype_size);
    auto end_offset = last_cell ? buff_var->size() :
                                  prepare_buffer_offset(
                                      buff->data(), i + 1, attr_datatype_size);
    auto cell_var_size = end_offset - start_offset;
    buff_var->set_offset(start_offset);
    RETURN_NOT_OK(tile_var->write(buff_var, cell_var_size));

    // Write the validity value(s).
    buff_validity->set_offset(i * constants::cell_validity_size);
    RETURN_NOT_OK(
        tile_validity->write(buff_validity, constants::cell_validity_size));
  }

  return Status::Ok();
}

Status Writer::write_all_tiles(
    FragmentMetadata* frag_meta,
    std::unordered_map<std::string, std::vector<Tile>>* const tiles) {
  auto timer_se = stats_->start_timer("tiles");

  assert(!tiles->empty());

  std::vector<ThreadPool::Task> tasks;
  for (auto& it : *tiles) {
    tasks.push_back(storage_manager_->io_tp()->execute([&, this]() {
      RETURN_CANCEL_OR_ERROR(write_tiles(it.first, frag_meta, 0, &it.second));
      return Status::Ok();
    }));
  }

  // Wait for writes and check all statuses
  auto statuses = storage_manager_->io_tp()->wait_all_status(tasks);
  for (auto& st : statuses)
    RETURN_NOT_OK(st);

  return Status::Ok();
}

Status Writer::write_tiles(
    const std::string& name,
    FragmentMetadata* frag_meta,
    uint64_t start_tile_id,
    std::vector<Tile>* const tiles,
    bool close_files) {
  auto timer_se = stats_->start_timer("tiles");

  // Handle zero tiles
  if (tiles->empty())
    return Status::Ok();

  // For easy reference
  const bool var_size = array_schema_->var_size(name);
  const bool nullable = array_schema_->is_nullable(name);
  const auto& uri = frag_meta->uri(name);
  const auto& var_uri = var_size ? frag_meta->var_uri(name) : URI("");
  const auto& validity_uri = nullable ? frag_meta->validity_uri(name) : URI("");

  // Write tiles
  auto tile_num = tiles->size();
  for (size_t i = 0, tile_id = start_tile_id; i < tile_num; ++i, ++tile_id) {
    Tile* tile = &(*tiles)[i];
    RETURN_NOT_OK(storage_manager_->write(uri, tile->filtered_buffer()));
    frag_meta->set_tile_offset(name, tile_id, tile->filtered_buffer()->size());

    if (var_size) {
      ++i;

      tile = &(*tiles)[i];
      RETURN_NOT_OK(storage_manager_->write(var_uri, tile->filtered_buffer()));
      frag_meta->set_tile_var_offset(
          name, tile_id, tile->filtered_buffer()->size());
      frag_meta->set_tile_var_size(name, tile_id, tile->pre_filtered_size());
    }

    if (nullable) {
      ++i;

      tile = &(*tiles)[i];
      RETURN_NOT_OK(
          storage_manager_->write(validity_uri, tile->filtered_buffer()));
      frag_meta->set_tile_validity_offset(
          name, tile_id, tile->filtered_buffer()->size());
    }
  }

  // Close files, except in the case of global order
  if (close_files && layout_ != Layout::GLOBAL_ORDER) {
    RETURN_NOT_OK(storage_manager_->close_file(frag_meta->uri(name)));
    if (var_size)
      RETURN_NOT_OK(storage_manager_->close_file(frag_meta->var_uri(name)));
    if (nullable)
      RETURN_NOT_OK(
          storage_manager_->close_file(frag_meta->validity_uri(name)));
  }

  return Status::Ok();
}

std::string Writer::coords_to_str(uint64_t i) const {
  std::stringstream ss;
  auto dim_num = array_schema_->dim_num();

  ss << "(";
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = array_schema_->dimension(d);
    const auto& dim_name = dim->name();
    ss << dim->coord_to_str(buffers_.find(dim_name)->second, i);
    if (d < dim_num - 1)
      ss << ", ";
  }
  ss << ")";

  return ss.str();
}

void Writer::clean_up(const URI& uri) {
  storage_manager_->vfs()->remove_dir(uri);
  global_write_state_.reset(nullptr);
}

Status Writer::calculate_hilbert_values(
    const std::vector<const QueryBuffer*>& buffs,
    std::vector<uint64_t>* hilbert_values) const {
  auto dim_num = array_schema_->dim_num();
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
          auto dim = array_schema_->dimension(d);
          coords[d] = dim->map_to_uint64(
              buffs[d], c, coords_info_.coords_num_, bits, max_bucket_val);
        }
        (*hilbert_values)[c] = h.coords_to_hilbert(&coords[0]);

        return Status::Ok();
      });

  RETURN_NOT_OK_ELSE(status, LOG_STATUS(status));

  return Status::Ok();
}

template <class T>
Status Writer::prepare_filter_and_write_tiles(
    const std::string& name,
    FragmentMetadata* frag_meta,
    DenseTiler<T>* dense_tiler,
    uint64_t thread_num) {
  auto timer_se = stats_->start_timer("prepare_filter_and_write_tiles");

  // For easy reference
  const bool var = array_schema_->var_size(name);
  const bool nullable = array_schema_->is_nullable(name);

  // Initialization
  auto tile_num = dense_tiler->tile_num();
  assert(tile_num > 0);
  uint64_t batch_num = tile_num / thread_num;
  uint64_t last_batch_size = tile_num % thread_num;
  batch_num += (last_batch_size > 0);
  last_batch_size = (last_batch_size == 0) ? thread_num : last_batch_size;

  // Process batches
  uint64_t frag_tile_id = 0;
  bool close_files = false;
  for (uint64_t b = 0; b < batch_num; ++b) {
    auto batch_size = (b == batch_num - 1) ? last_batch_size : thread_num;
    assert(batch_size > 0);
    std::vector<Tile> tiles(batch_size * (1 + var + nullable));
    auto st = parallel_for(
        storage_manager_->compute_tp(), 0, batch_size, [&](uint64_t i) {
          // Prepare and filter tiles
          auto tiles_id = i * (1 + var + nullable);
          if (!var) {
            RETURN_NOT_OK(dense_tiler->get_tile(
                frag_tile_id + i, name, &tiles[tiles_id]));
            RETURN_NOT_OK(filter_tile(name, &tiles[tiles_id], false, false));
          } else {
            RETURN_NOT_OK(dense_tiler->get_tile_var(
                frag_tile_id + i,
                name,
                &tiles[tiles_id],
                &tiles[tiles_id + 1]));
            RETURN_NOT_OK(filter_tile(name, &tiles[tiles_id], true, false));
            RETURN_NOT_OK(
                filter_tile(name, &tiles[tiles_id + 1], false, false));
          }
          if (nullable) {
            RETURN_NOT_OK(dense_tiler->get_tile_null(
                frag_tile_id + i, name, &tiles[tiles_id + 1 + var]));
            RETURN_NOT_OK(
                filter_tile(name, &tiles[tiles_id + 1 + var], false, true));
          }
          return Status::Ok();
        });
    RETURN_NOT_OK(st);

    // Write tiles
    close_files = (b == batch_num - 1);
    RETURN_NOT_OK(
        write_tiles(name, frag_meta, frag_tile_id, &tiles, close_files));

    frag_tile_id += batch_size;
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
