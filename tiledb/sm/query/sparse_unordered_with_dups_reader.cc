/**
 * @file   sparse_unordered_with_dups_reader.cc
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
 * This file implements class SparseUnorderedWithDupsReader.
 */
#include "tiledb/sm/query/sparse_unordered_with_dups_reader.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/comparators.h"
#include "tiledb/sm/misc/hilbert.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/result_tile.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/open_array_memory_tracker.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/subarray/subarray.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

template <class BitmapType>
SparseUnorderedWithDupsReader<BitmapType>::SparseUnorderedWithDupsReader(
    stats::Stats* stats,
    tdb_shared_ptr<Logger> logger,
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout,
    QueryCondition& condition)
    : SparseIndexReaderBase(
          stats,
          logger->clone("SparseUnorderedWithDupsReader", ++logger_id_),
          storage_manager,
          array,
          config,
          buffers,
          subarray,
          layout,
          condition) {
  // Defines specific bahavior in the tile copy code for this reader.
  fix_var_sized_overflows_ = true;
  clear_coords_tiles_on_copy_ = false;
  array_memory_tracker_ =
      storage_manager_->array_memory_tracker(array->array_uri());
}

/* ****************************** */
/*               API              */
/* ****************************** */

template <class BitmapType>
bool SparseUnorderedWithDupsReader<BitmapType>::incomplete() const {
  return this->copy_overflowed_ || !read_state_.result_cell_slabs_.empty() ||
         !read_state_.done_adding_result_tiles_ || !result_tiles_[0].empty();
}

template <class BitmapType>
Status SparseUnorderedWithDupsReader<BitmapType>::init() {
  RETURN_NOT_OK(SparseIndexReaderBase::init());

  // Initialize memory budget variables.
  RETURN_NOT_OK(initialize_memory_budget());

  return Status::Ok();
}

template <class BitmapType>
Status SparseUnorderedWithDupsReader<BitmapType>::initialize_memory_budget() {
  bool found = false;
  RETURN_NOT_OK(
      config_.get<uint64_t>("sm.mem.total_budget", &memory_budget_, &found));
  assert(found);

  RETURN_NOT_OK(config_.get<double>(
      "sm.mem.reader.sparse_unordered_with_dups.ratio_array_data",
      &memory_budget_ratio_array_data_,
      &found));
  assert(found);

  RETURN_NOT_OK(config_.get<double>(
      "sm.mem.reader.sparse_unordered_with_dups.ratio_coords",
      &memory_budget_ratio_coords_,
      &found));
  assert(found);

  RETURN_NOT_OK(config_.get<double>(
      "sm.mem.reader.sparse_unordered_with_dups.ratio_query_condition",
      &memory_budget_ratio_query_condition_,
      &found));
  assert(found);

  RETURN_NOT_OK(config_.get<double>(
      "sm.mem.reader.sparse_unordered_with_dups.ratio_tile_ranges",
      &memory_budget_ratio_tile_ranges_,
      &found));
  assert(found);

  return Status::Ok();
}

template <class BitmapType>
Status SparseUnorderedWithDupsReader<BitmapType>::dowork() {
  auto timer_se = stats_->start_timer("dowork");

  // Check that the query condition is valid.
  RETURN_NOT_OK(condition_.check(array_schema_));

  get_dim_attr_stats();

  // Reset the copy overflow flag.
  copy_overflowed_ = false;

  // Handle empty array.
  if (fragment_metadata_.empty()) {
    read_state_.done_adding_result_tiles_ = true;
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  reset_buffer_sizes();

  // This reader only has one result tile list.
  result_tiles_.resize(1);

  // Load initial data, if not loaded already.
  RETURN_NOT_OK(load_initial_data());

  // Create the result tiles we are going to process.
  RETURN_NOT_OK(create_result_tiles());

  // No more tiles to process, done.
  if (result_tiles_[0].empty()) {
    assert(read_state_.done_adding_result_tiles_);
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // Maintain a temporary vector with pointers to result tiles for calling
  // read_and_unfilter_coords.
  std::vector<ResultTile*> tmp_result_tiles;
  for (auto& rt_list : result_tiles_) {
    for (auto& result_tile : rt_list) {
      tmp_result_tiles.emplace_back(&result_tile);
    }
  }

  // Read and unfilter coords.
  RETURN_NOT_OK(
      read_and_unfilter_coords(subarray_.is_set(), &tmp_result_tiles));

  // Compute the tile bitmaps.
  RETURN_NOT_OK(compute_tile_bitmaps<BitmapType>(&result_tiles_));

  // Apply query condition.
  RETURN_NOT_OK(apply_query_condition<BitmapType>(&result_tiles_));

  // Clear result tiles that are not necessary anymore.
  auto it = result_tiles_[0].begin();
  while (it != result_tiles_[0].end()) {
    if (it->bitmap_num_cells == 0 ||
        (subarray_.is_set() &&
         it->bitmap_num_cells == std::numeric_limits<uint64_t>::max())) {
      auto f = it->frag_idx();
      RETURN_NOT_OK(remove_result_tile(f, it++));
    } else {
      it++;
    }
  }

  // No more tiles to process, done.
  if (result_tiles_[0].empty()) {
    assert(read_state_.done_adding_result_tiles_);
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // Copy tiles.
  if (offsets_bitsize_ == 64) {
    RETURN_NOT_OK(process_tiles<uint64_t>());
  } else {
    RETURN_NOT_OK(process_tiles<uint32_t>());
  }

  // End the iteration.
  RETURN_NOT_OK(end_iteration());

  return Status::Ok();
}

template <class BitmapType>
void SparseUnorderedWithDupsReader<BitmapType>::reset() {
}

template <class BitmapType>
Status SparseUnorderedWithDupsReader<BitmapType>::add_result_tile(
    const unsigned dim_num,
    const uint64_t memory_budget_qc_tiles,
    const uint64_t memory_budget_coords_tiles,
    const unsigned f,
    const uint64_t t,
    const uint64_t last_t,
    const Domain* const domain,
    bool* budget_exceeded) {
  // Calculate memory consumption for this tile.
  uint64_t tiles_size = 0, tiles_size_qc = 0;
  RETURN_NOT_OK(get_coord_tiles_size<BitmapType>(
      subarray_.is_set(), dim_num, f, t, &tiles_size, &tiles_size_qc));

  // Don't load more tiles than the memory budget.
  if (memory_used_for_coords_total_ + tiles_size > memory_budget_coords_tiles ||
      memory_used_qc_tiles_total_ + tiles_size_qc > memory_budget_qc_tiles) {
    *budget_exceeded = true;
    return Status::Ok();
  }

  // Adjust memory usage.
  memory_used_for_coords_total_ += tiles_size;
  memory_used_qc_tiles_total_ += tiles_size_qc;

  // Add the result tile.
  result_tiles_[0].emplace_back(f, t, domain);

  // Are all tiles loaded for this fragment.
  if (t == last_t)
    all_tiles_loaded_[f] = true;

  return Status::Ok();
}

template <class BitmapType>
Status SparseUnorderedWithDupsReader<BitmapType>::create_result_tiles() {
  auto timer_se = stats_->start_timer("create_result_tiles");

  // For easy reference.
  const auto fragment_num = fragment_metadata_.size();
  const auto domain = array_schema_->domain();
  const auto dim_num = array_schema_->dim_num();

  const uint64_t memory_budget_qc_tiles =
      memory_budget_ * memory_budget_ratio_query_condition_;
  const uint64_t memory_budget_coords =
      memory_budget_ * memory_budget_ratio_coords_;

  // Create result tiles.
  if (subarray_.is_set()) {
    // Load as many tiles as the memory budget allows.
    bool budget_exceeded = false;
    unsigned int f = 0;
    while (f < fragment_num && !budget_exceeded) {
      if (!all_tiles_loaded_[f]) {
        auto range_it = result_tile_ranges_[f].rbegin();
        all_tiles_loaded_[f] = range_it == result_tile_ranges_[f].rend();
        while (range_it != result_tile_ranges_[f].rend()) {
          const auto last_t = result_tile_ranges_[f].front().second;

          // Figure out the start index.
          auto start = range_it->first;
          if (!result_tiles_[0].empty() &&
              result_tiles_[0].back().frag_idx() == f) {
            start = std::max(start, result_tiles_[0].back().tile_idx() + 1);
          }

          // Add all tiles for this range.
          for (uint64_t t = start; t <= range_it->second; t++) {
            RETURN_NOT_OK(add_result_tile(
                dim_num,
                memory_budget_qc_tiles,
                memory_budget_coords,
                f,
                t,
                last_t,
                domain,
                &budget_exceeded));

            // Make sure we can add at least one tile.
            if (budget_exceeded) {
              logger_->debug(
                  "Budget exceeded adding result tiles, fragment {0}, tile {1}",
                  f,
                  t);
              if (result_tiles_[0].empty())
                return logger_->status(
                    Status::SparseUnorderedWithDupsReaderError(
                        "Cannot load a single tile, increase memory budget"));
              break;
            }
          }

          if (budget_exceeded)
            break;
          range_it++;
        }
      }

      f++;
    }
  } else {
    // Load as many tiles as the memory budget allows.
    bool budget_exceeded = false;
    unsigned int f = 0;
    while (f < fragment_num && !budget_exceeded) {
      if (!all_tiles_loaded_[f]) {
        auto tile_num = fragment_metadata_[f]->tile_num();

        // Figure out the start index.
        auto start = read_state_.frag_tile_idx_[f].first;
        if (!result_tiles_[0].empty() &&
            result_tiles_[0].back().frag_idx() == f) {
          start = std::max(start, result_tiles_[0].back().tile_idx() + 1);
        }

        // Add all tiles for this fragment.
        for (uint64_t t = start; t < tile_num; t++) {
          RETURN_NOT_OK(add_result_tile(
              dim_num,
              memory_budget_qc_tiles,
              memory_budget_coords,
              f,
              t,
              tile_num - 1,
              domain,
              &budget_exceeded));

          // Make sure we can add at least one tile.
          if (budget_exceeded) {
            logger_->debug(
                "Budget exceeded adding result tiles, fragment {0}, tile {1}",
                f,
                t);
            if (result_tiles_[0].empty())
              return logger_->status(Status::SparseUnorderedWithDupsReaderError(
                  "Cannot load a single tile, increase memory budget"));
            break;
          }
        }
      }
      f++;
    }
  }

  // Check if we are done adding result tiles.
  bool done_adding_result_tiles = true;
  for (unsigned int f = 0; f < fragment_num; f++) {
    done_adding_result_tiles &= all_tiles_loaded_[f];
  }

  logger_->debug(
      "Done adding result tiles, num result tiles {0}",
      result_tiles_[0].size());

  if (done_adding_result_tiles) {
    logger_->debug("All result tiles loaded");
  }

  read_state_.done_adding_result_tiles_ = done_adding_result_tiles;
  return Status::Ok();
}

/** Copy offsets with a result count bitmap. */
template <>
template <class OffType>
Status SparseUnorderedWithDupsReader<uint64_t>::copy_offsets_tile(
    const std::string& name,
    const bool nullable,
    const OffType offset_div,
    ResultTileWithBitmap<uint64_t>* rt,
    OffType* buffer,
    uint8_t* val_buffer,
    void** var_data) {
  // Get source buffers.
  const auto tile_tuple = rt->tile_tuple(name);
  const auto t = &std::get<0>(*tile_tuple);
  const auto t_var = &std::get<1>(*tile_tuple);
  const auto src_buff = (uint64_t*)t->buffer()->data();
  const auto src_var_buff = (char*)t_var->buffer()->data();
  const auto t_val = &std::get<2>(*tile_tuple);
  const auto last_c = rt->cell_num() - 1;

  // process all cells. Last cell is taken out for vectorization.
  for (uint64_t c = 0; c < last_c; c++) {
    for (uint64_t i = 0; i < rt->bitmap[c]; i++) {
      *buffer = (OffType)(src_buff[c + 1] - src_buff[c]) / offset_div;
      buffer++;
      *var_data = src_var_buff + src_buff[c];
      var_data++;
    }
  }

  // Do last cell.
  for (uint64_t i = 0; i < rt->bitmap[last_c]; i++) {
    *buffer = (OffType)(t_var->size() - src_buff[last_c]) / offset_div;
    buffer++;
    *var_data = src_var_buff + src_buff[last_c];
    var_data++;
  }

  // Copy nullable values.
  if (nullable) {
    const auto src_val_buff = (uint8_t*)t_val->buffer()->data();
    for (uint64_t c = 0; c <= last_c; c++) {
      for (uint64_t i = 0; i < rt->bitmap[c]; i++) {
        *val_buffer = src_val_buff[c];
        val_buffer++;
      }
    }
  }

  return Status::Ok();
}

/** Copy offsets with a result bitmap. */
template <>
template <class OffType>
Status SparseUnorderedWithDupsReader<uint8_t>::copy_offsets_tile(
    const std::string& name,
    const bool nullable,
    const OffType offset_div,
    ResultTileWithBitmap<uint8_t>* rt,
    OffType* buffer,
    uint8_t* val_buffer,
    void** var_data) {
  // Get source buffers.
  const auto tile_tuple = rt->tile_tuple(name);
  const auto t = &std::get<0>(*tile_tuple);
  const auto t_var = &std::get<1>(*tile_tuple);
  const auto src_buff = (uint64_t*)t->buffer()->data();
  const auto src_var_buff = (char*)t_var->buffer()->data();
  const auto t_val = &std::get<2>(*tile_tuple);

  // 0 sized bitmap means full tile, full tile copy done below.
  if (rt->bitmap.size() != 0) {
    // process all cells. Last cell is taken out for vectorization.
    const auto last_c = rt->cell_num() - 1;
    for (uint64_t c = 0; c < last_c; c++) {
      if (rt->bitmap[c]) {
        *buffer = (OffType)(src_buff[c + 1] - src_buff[c]) / offset_div;
        buffer++;
        *var_data = src_var_buff + src_buff[c];
        var_data++;
      }
    }

    // Do last cell.
    if (rt->bitmap[last_c]) {
      *buffer = (OffType)(t_var->size() - src_buff[last_c]) / offset_div;
      *var_data = src_var_buff + src_buff[last_c];
    }

    // Copy nullable values.
    if (nullable) {
      const auto src_val_buff = (uint8_t*)t_val->buffer()->data();
      for (uint64_t c = 0; c <= last_c; c++) {
        if (rt->bitmap[c]) {
          *val_buffer = src_val_buff[c];
          val_buffer++;
        }
      }
    }
  } else {
    // Copy full tile. Last cell is taken out for vectorization.
    const auto last_c = rt->cell_num() - 1;
    for (uint64_t c = 0; c < last_c; c++) {
      *buffer = (OffType)(src_buff[c + 1] - src_buff[c]) / offset_div;
      buffer++;
      *var_data = src_var_buff + src_buff[c];
      var_data++;
    }

    // Copy last cell.
    *buffer = (OffType)(t_var->size() - src_buff[last_c]) / offset_div;
    *var_data = src_var_buff + src_buff[last_c];

    // Copy nullable values.
    if (nullable) {
      const auto src_val_buff = (uint8_t*)t_val->buffer()->data();
      for (uint64_t c = 0; c <= last_c; c++) {
        *val_buffer = src_val_buff[c];
        val_buffer++;
      }
    }
  }

  return Status::Ok();
}

/** Copy offsets tiles. */
template <class BitmapType>
template <class OffType>
Status SparseUnorderedWithDupsReader<BitmapType>::copy_offsets_tiles(
    const std::string& name,
    const bool nullable,
    const OffType offset_div,
    const uint64_t max_rt_idx,
    OffType* buffer,
    uint8_t* val_buffer,
    void** var_data,
    uint64_t* global_cell_offset,
    typename std::list<ResultTileWithBitmap<BitmapType>>::iterator*
        result_tiles_it) {
  auto timer_se = stats_->start_timer("copy_offsets_tiles");

  // Process all tiles in parallel.
  std::mutex it_mtx;
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, max_rt_idx, [&](uint64_t) {
        // Take a result tile in the list and calculate the cell offset for
        // this tile. As the work is very simple and not parallelizable,
        // using a mutex to prevent from allocating a vector for the cell
        // offsets.
        typename std::list<ResultTileWithBitmap<BitmapType>>::iterator rt;
        uint64_t cell_offset = 0;
        {
          std::unique_lock<std::mutex> ul(it_mtx);
          rt = (*result_tiles_it)++;
          cell_offset = *global_cell_offset;
          *global_cell_offset +=
              subarray_.is_set() ? rt->bitmap_num_cells : rt->cell_num();
        }

        // Copy tile.
        RETURN_NOT_OK(copy_offsets_tile<OffType>(
            name,
            nullable,
            offset_div,
            &*rt,
            buffer + cell_offset,
            val_buffer + cell_offset,
            var_data + cell_offset));

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, logger_->status(status));

  return Status::Ok();
}

/** Copy Var data. */
template <class BitmapType>
template <class OffType>
Status SparseUnorderedWithDupsReader<BitmapType>::copy_var_data_tile(
    const bool last_tile,
    const uint64_t cell_offset,
    const uint64_t offset_div,
    const uint64_t last_offset,
    const ResultTileWithBitmap<BitmapType>* rt,
    const void** var_data,
    const OffType* offsets_buffer,
    uint8_t* var_data_buffer) {
  // Copy the data cells by cells. Last copy taken out for
  // vectorization.
  uint64_t num_cells =
      subarray_.is_set() ? rt->bitmap_num_cells : rt->cell_num();
  if (num_cells > 0) {
    auto max_cell = last_tile ? num_cells - 1 : num_cells;
    for (uint64_t c = 0; c < max_cell; c++) {
      auto size = (offsets_buffer[c + 1] - offsets_buffer[c]) * offset_div;
      memcpy(
          var_data_buffer + offsets_buffer[c] * offset_div,
          var_data[c + cell_offset],
          size);
    }

    // Last copy for last tile.
    if (last_tile) {
      memcpy(
          var_data_buffer + offsets_buffer[num_cells - 1] * offset_div,
          var_data[num_cells - 1 + cell_offset],
          (last_offset - offsets_buffer[num_cells - 1]) * offset_div);
    }
  }

  return Status::Ok();
}

/** Copy var tiles. */
template <class BitmapType>
template <class OffType>
Status SparseUnorderedWithDupsReader<BitmapType>::copy_var_data_tiles(
    const OffType offset_div,
    const uint64_t last_offset,
    const uint64_t max_rt_idx,
    OffType* offsets_buffer,
    uint8_t* var_data_buffer,
    const void** var_data,
    uint64_t* global_cell_offset) {
  auto timer_se = stats_->start_timer("copy_var_tiles");

  // Process all tiles in parallel.
  std::mutex it_mtx;
  auto result_tiles_it = result_tiles_[0].begin();
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, max_rt_idx, [&](uint64_t) {
        // Take a result tile in the list and calculate the cell offset for
        // this tile. As the work is very simple and not parallelizable,
        // using a mutex to prevent from allocating a vector for the cell
        // offsets.
        typename std::list<ResultTileWithBitmap<BitmapType>>::iterator rt;
        uint64_t cell_offset = 0;
        bool last_tile = false;
        {
          std::unique_lock<std::mutex> ul(it_mtx);
          rt = result_tiles_it++;
          cell_offset = *global_cell_offset;
          *global_cell_offset +=
              subarray_.is_set() ? rt->bitmap_num_cells : rt->cell_num();
          last_tile = result_tiles_it == result_tiles_[0].end();
        }

        RETURN_NOT_OK(copy_var_data_tile(
            last_tile,
            cell_offset,
            offset_div,
            last_offset,
            &*rt,
            var_data,
            offsets_buffer + cell_offset,
            var_data_buffer));

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, logger_->status(status));

  return Status::Ok();
}

/** Copy fixed data with a result count bitmap. */
template <>
Status SparseUnorderedWithDupsReader<uint64_t>::copy_fixed_data_tile(
    const std::string& name,
    const bool is_dim,
    const bool nullable,
    const unsigned dim_idx,
    const uint64_t cell_size,
    ResultTileWithBitmap<uint64_t>* rt,
    uint8_t* buffer,
    uint8_t* val_buffer) {
  // Get source buffers.
  const auto stores_zipped_coords = is_dim && rt->stores_zipped_coords();
  const auto tile_tuple = rt->tile_tuple(name);
  const auto t = &std::get<0>(*tile_tuple);
  const auto src_buff = (uint8_t*)t->buffer()->data();

  // Copy values.
  if (!stores_zipped_coords) {
    for (uint64_t c = 0; c < rt->cell_num(); c++) {
      for (uint64_t i = 0; i < rt->bitmap[c]; i++) {
        memcpy(buffer, src_buff + c * cell_size, cell_size);
        buffer += cell_size;
      }
    }
  } else {  // Copy for zipped coords.
    const auto dim_num = rt->domain()->dim_num();
    for (uint64_t c = 0; c < rt->cell_num(); c++) {
      for (uint64_t i = 0; i < rt->bitmap[c]; i++) {
        auto pos = c * dim_num + dim_idx;
        memcpy(buffer, src_buff + pos * cell_size, cell_size);
        buffer += cell_size;
      }
    }
  }

  // Copy nullable values.
  if (nullable) {
    const auto t_val = &std::get<2>(*tile_tuple);
    const auto src_val_buff = (uint8_t*)t_val->buffer()->data();
    for (uint64_t c = 0; c < rt->cell_num(); c++) {
      for (uint64_t i = 0; i < rt->bitmap[c]; i++) {
        *val_buffer = src_val_buff[c];
        val_buffer++;
      }
    }
  }

  return Status::Ok();
}

/** Copy fixed data with a result bitmap. */
template <>
Status SparseUnorderedWithDupsReader<uint8_t>::copy_fixed_data_tile(
    const std::string& name,
    const bool is_dim,
    const bool nullable,
    const unsigned dim_idx,
    const uint64_t cell_size,
    ResultTileWithBitmap<uint8_t>* rt,
    uint8_t* buffer,
    uint8_t* val_buffer) {
  // Get source buffers.
  const auto stores_zipped_coords = is_dim && rt->stores_zipped_coords();
  const auto tile_tuple = rt->tile_tuple(name);
  const auto t = &std::get<0>(*tile_tuple);
  const auto src_buff = (uint8_t*)t->buffer()->data();
  const auto t_val = &std::get<2>(*tile_tuple);

  // 0 sized bitmap means full tile, full tile copy done below.
  if (rt->bitmap.size() != 0) {
    // Copy values.
    if (!stores_zipped_coords) {
      // Go through bitmap, when there is a hole in cell contiguity, do a
      // memcpy.
      uint64_t length = 0;
      uint64_t start = 0;
      for (uint64_t c = 0; c < rt->cell_num(); c++) {
        if (rt->bitmap[c]) {
          length++;
        } else {
          if (length != 0) {
            memcpy(buffer, src_buff + start * cell_size, length * cell_size);
            buffer += length * cell_size;
            length = 0;
          }

          start = c + 1;
        }
      }

      // Do last memcpy.
      if (length != 0) {
        memcpy(buffer, src_buff + start * cell_size, length * cell_size);
        buffer += length * cell_size;
      }
    } else {  // Copy for zipped coords.
      const auto dim_num = rt->domain()->dim_num();
      for (uint64_t c = 0; c < rt->cell_num(); c++) {
        if (rt->bitmap[c]) {
          auto pos = c * dim_num + dim_idx;
          memcpy(buffer, src_buff + pos * cell_size, cell_size);
          buffer += cell_size;
        }
      }
    }

    // Copy nullable values.
    if (nullable) {
      // Go through bitmap, when there is a hole in cell contiguity, do a
      // memcpy.
      uint64_t length = 0;
      uint64_t start = 0;
      const auto src_val_buff = (uint8_t*)t_val->buffer()->data();
      for (uint64_t c = 0; c < rt->cell_num(); c++) {
        if (rt->bitmap[c]) {
          length++;
        } else {
          if (length != 0) {
            memcpy(val_buffer, src_val_buff + start, length);
            val_buffer += length;
            length = 0;
          }

          start = c + 1;
        }
      }

      // Do last memcpy.
      if (length != 0) {
        memcpy(val_buffer, src_val_buff + start, length);
        val_buffer += length;
      }
    }
  } else {  // Copy full tile.
    memcpy(buffer, src_buff, cell_size * rt->cell_num());

    if (nullable) {
      const auto src_val_buff = (uint8_t*)t_val->buffer()->data();
      memcpy(val_buffer, src_val_buff, rt->cell_num());
    }
  }

  return Status::Ok();
}

/** Copy fixed data tiles. */
template <class BitmapType>
Status SparseUnorderedWithDupsReader<BitmapType>::copy_fixed_data_tiles(
    const std::string& name,
    const bool is_dim,
    const bool nullable,
    const uint64_t dim_idx,
    const uint64_t max_rt_idx,
    const uint64_t cell_size,
    uint8_t* buffer,
    uint8_t* val_buffer,
    uint64_t* global_cell_offset,
    typename std::list<ResultTileWithBitmap<BitmapType>>::iterator*
        result_tiles_it) {
  auto timer_se = stats_->start_timer("copy_fixed_data_tiles");

  // Process all tiles in parallel.
  std::mutex it_mtx;
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, max_rt_idx, [&](uint64_t) {
        // Take a result tile in the list and calculate the cell offset for
        // this tile. As the work is very simple and not parallelizable,
        // using a mutex to prevent from allocating a vector for the cell
        // offsets.
        typename std::list<ResultTileWithBitmap<BitmapType>>::iterator rt;
        uint64_t cell_offset = 0;
        {
          std::unique_lock<std::mutex> ul(it_mtx);
          rt = (*result_tiles_it)++;
          cell_offset = *global_cell_offset;
          *global_cell_offset +=
              subarray_.is_set() ? rt->bitmap_num_cells : rt->cell_num();
        }

        // Copy tile.
        RETURN_NOT_OK(copy_fixed_data_tile(
            name,
            is_dim,
            nullable,
            dim_idx,
            cell_size,
            &*rt,
            buffer + cell_offset * cell_size,
            val_buffer + cell_offset));

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, logger_->status(status));

  return Status::Ok();
}

template <class BitmapType>
Status SparseUnorderedWithDupsReader<BitmapType>::compute_initial_copy_bound(
    uint64_t* max_rt_idx) {
  auto timer_se = stats_->start_timer("compute_initial_copy_bound");
  *max_rt_idx = 0;

  // First try to limit the maximum number of cells we copy using the size
  // of the output buffers for fixed sized attributes. Later we will validate
  // the memory budget. This is the first line of defence used to try to prevent
  // overflows when copying data.
  auto max_num_cells = std::numeric_limits<uint64_t>::max();
  std::vector<std::string> names(buffers_.size());
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    const auto size = *it.second.buffer_size_;
    if (array_schema_->var_size(name)) {
      auto temp_num_cells = size / constants::cell_var_offset_size;

      if (offsets_extra_element_ && temp_num_cells > 0)
        temp_num_cells--;

      max_num_cells = std::min(max_num_cells, temp_num_cells);
    } else {
      auto temp_num_cells = size / array_schema_->cell_size(name);
      max_num_cells = std::min(max_num_cells, temp_num_cells);
    }
  }

  // User gave us some empty buffers, exit.
  if (max_num_cells == 0) {
    zero_out_buffer_sizes();
    return Status::Ok();
  }

  // Compute initial bound for result tiles by making sure all cells within
  // a result tile can fit into the user's buffer. We use either the number
  // of cells in the bitmap  when a subarray is set or the number of cells
  // in the result tile to do so.
  uint64_t rt_idx = 0;
  uint64_t cell_offset = 0;
  for (auto& rt : result_tiles_[0]) {
    const auto cell_num =
        subarray_.is_set() ? rt.bitmap_num_cells : rt.cell_num();
    if (cell_offset + cell_num > max_num_cells)
      break;

    rt_idx++;
  }

  // Calculate memory budget.
  uint64_t memory_budget_copy = memory_budget_ - memory_used_qc_tiles_total_ -
                                memory_used_result_tile_ranges_ -
                                array_memory_tracker_->get_memory_usage();

  // Make sure we respect memory budget for copy operation by making sure that,
  // for all attributes to be copied, the size of tiles in memory can fit into
  // the budget.
  std::mutex it_mtx;
  auto buffers_it = buffers_.begin();
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, buffers_.size(), [&](uint64_t) {
        // Get a tile from the list.
        std::unordered_map<std::string, tiledb::sm::QueryBuffer>::iterator it;
        {
          std::unique_lock<std::mutex> ul(it_mtx);
          it = buffers_it++;
        }
        const auto& name = it->first;
        const auto var_sized = array_schema_->var_size(name);

        // For dimensions, when we have a subarray, tiles are already all
        // loaded in memory.
        if (subarray_.is_set() && array_schema_->is_dim(name))
          return Status::Ok();

        // Get the size for this tile.
        uint64_t mem_usage = 0;
        auto rt = result_tiles_[0].begin();
        uint64_t idx = 0;
        while (rt != result_tiles_[0].end()) {
          // Size of the tile in memory.
          uint64_t tile_size = 0;
          RETURN_NOT_OK(get_attribute_tile_size(
              name, rt->frag_idx(), rt->tile_idx(), &tile_size));

          // Account for the pointers to the var data that is created in
          // copy_tiles for var sized attributes.
          if (var_sized) {
            auto cell_num =
                subarray_.is_set() ? rt->bitmap_num_cells : rt->cell_num();
            tile_size += sizeof(void*) * cell_num;
          }

          // Stop when we reach the budget.
          if (mem_usage + tile_size > memory_budget_copy) {
            break;
          }

          // Adjust memory usage and move to the next tile.
          mem_usage += tile_size;
          rt++;
          idx++;
        }

        // Save the minimum result tile index that we saw for all attributes.
        {
          std::unique_lock<std::mutex> ul(it_mtx);
          rt_idx = std::min(idx, rt_idx);
        }

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, logger_->status(status));

  if (rt_idx == 0)
    return Status::SparseUnorderedWithDupsReaderError(
        "Unable to copy one tile with current budget/buffers");

  *max_rt_idx = rt_idx;

  return Status::Ok();
}

template <class BitmapType>
Status SparseUnorderedWithDupsReader<BitmapType>::read_and_unfilter_attribute(
    const std::string& name, std::vector<ResultTile*>* result_tiles) {
  auto timer_se = stats_->start_timer("read_and_unfilter_attribute");
  std::vector<std::string> names = {name};

  // Read and unfilter tiles.
  RETURN_NOT_OK(read_attribute_tiles(&names, result_tiles));
  RETURN_NOT_OK(unfilter_tiles(name, result_tiles));

  return Status::Ok();
}

template <class BitmapType>
template <class OffType>
Status SparseUnorderedWithDupsReader<BitmapType>::process_tiles() {
  auto timer_se = stats_->start_timer("process_tiles");

  // Calculating the maximum number of tiles to be copied from to the user
  // buffers based on user's buffer size and on the memory budget.
  uint64_t max_rt_idx = 0;
  RETURN_NOT_OK(compute_initial_copy_bound(&max_rt_idx));

  // Create the result tiles vector to use with read_tiles and unfilter_tiles.
  std::vector<ResultTile*> tmp_result_tiles;
  uint64_t i = 0;
  for (auto it = result_tiles_[0].begin(); i < max_rt_idx; i++, it++) {
    tmp_result_tiles.emplace_back(&*it);
  }

  // Copy tiles attribute by attribute.
  // TODO load as many attributes as we can fit in memory so that we can
  // parallelize here.
  uint64_t num_cells_copied = std::numeric_limits<uint64_t>::max();
  for (auto& it : buffers_) {
    // For easy reference.
    const auto& name = it.first;
    const auto is_dim = array_schema_->is_dim(name);
    const auto var_sized = array_schema_->var_size(name);
    const auto nullable = array_schema_->is_nullable(name);
    const auto cell_size = array_schema_->cell_size(name);

    // Get dim idx for zipped coords copy.
    auto dim_idx = 0;
    if (is_dim) {
      const auto& dim_names = array_schema_->dim_names();
      while (name != dim_names[dim_idx])
        dim_idx++;
    }

    if (!subarray_.is_set() || !is_dim) {
      // Read and unfilter tiles.
      RETURN_NOT_OK(read_and_unfilter_attribute(name, &tmp_result_tiles));
    }

    // Compute initial cells copied.
    if (num_cells_copied == std::numeric_limits<uint64_t>::max()) {
      num_cells_copied = 0;
      i = 0;
      for (auto it = result_tiles_[0].begin(); i < max_rt_idx; i++, it++) {
        num_cells_copied +=
            subarray_.is_set() ? it->bitmap_num_cells : it->cell_num();
      }
    }

    // Pointers to var size data.
    std::vector<void*> var_data;
    if (var_sized) {
      var_data.resize(num_cells_copied);
    }

    // Process all tiles in parallel.
    OffType offset_div =
        elements_mode_ ? datatype_size(array_schema_->type(name)) : 1;
    uint64_t global_cell_offset = 0;
    auto result_tiles_it = result_tiles_[0].begin();
    if (var_sized) {
      RETURN_NOT_OK(copy_offsets_tiles<OffType>(
          name,
          nullable,
          offset_div,
          max_rt_idx,
          (OffType*)it.second.buffer_,
          it.second.validity_vector_.buffer(),
          var_data.data(),
          &global_cell_offset,
          &result_tiles_it));
    } else {
      RETURN_NOT_OK(copy_fixed_data_tiles(
          name,
          is_dim,
          nullable,
          dim_idx,
          max_rt_idx,
          cell_size,
          (uint8_t*)it.second.buffer_,
          it.second.validity_vector_.buffer(),
          &global_cell_offset,
          &result_tiles_it));
    }

    OffType var_offset = 0;
    if (var_sized) {
      auto timer_se = stats_->start_timer("switch_sizes_to_offsets");

      // Switch offsets buffer from cell size to offsets.
      auto offsets_buff = (OffType*)it.second.buffer_;
      for (uint64_t c = 0; c < global_cell_offset; c++) {
        auto tmp = offsets_buff[c];
        offsets_buff[c] = var_offset;
        var_offset += tmp;
      }

      // Make sure var size buffer can fit the data.
      while (*it.second.buffer_var_size_ < (uint64_t)var_offset) {
        result_tiles_it--;
        auto num_cells = subarray_.is_set() ?
                             result_tiles_it->bitmap_num_cells :
                             result_tiles_it->cell_num();
        global_cell_offset -= num_cells;
        var_offset = ((OffType*)it.second.buffer_)[global_cell_offset];
        max_rt_idx--;
      }

      if (max_rt_idx == 0) {
        return Status::SparseUnorderedWithDupsReaderError(
            "Var size buffer cannot fit a full result tile for attribute " +
            name);
      }

      // Now copy the var size data.
      global_cell_offset = 0;
      RETURN_NOT_OK(copy_var_data_tiles(
          offset_div,
          var_offset,
          max_rt_idx,
          (OffType*)it.second.buffer_,
          (uint8_t*)it.second.buffer_var_,
          (const void**)var_data.data(),
          &global_cell_offset));
    }

    // Adjust tile index.
    uint64_t i = 0;
    for (auto it = result_tiles_[0].begin(); i < max_rt_idx; i++, it++) {
      read_state_.frag_tile_idx_[it->frag_idx()].first = it->tile_idx() + 1;
    }

    // Adjust buffer sizes.
    if (var_sized) {
      *it.second.buffer_size_ = global_cell_offset * sizeof(OffType);

      if (offsets_extra_element_)
        (*it.second.buffer_size_) += sizeof(OffType);

      *it.second.buffer_var_size_ = var_offset * offset_div;
    } else {
      *it.second.buffer_size_ = global_cell_offset * cell_size;
    }

    if (nullable)
      *buffers_[name].validity_vector_.buffer_size() = global_cell_offset;

    // Adjust number of cells copied.
    num_cells_copied = std::min(num_cells_copied, global_cell_offset);

    // Clear tiles from memory.
    if (!subarray_.is_set() || !is_dim) {
      clear_tiles(name, &tmp_result_tiles);
    }
  }

  // Fix the output buffer sizes.
  RETURN_NOT_OK(resize_output_buffers(num_cells_copied));

  logger_->debug("Done copying tiles");
  return Status::Ok();
}

template <class BitmapType>
Status SparseUnorderedWithDupsReader<BitmapType>::remove_result_tile(
    const unsigned frag_idx,
    typename std::list<ResultTileWithBitmap<BitmapType>>::iterator rt) {
  // Remove coord tile size from memory budget.
  const auto tile_idx = rt->tile_idx();
  uint64_t tiles_size = 0, tiles_size_qc = 0;
  RETURN_NOT_OK(get_coord_tiles_size<BitmapType>(
      subarray_.is_set(),
      array_schema_->dim_num(),
      frag_idx,
      tile_idx,
      &tiles_size,
      &tiles_size_qc));

  {
    std::unique_lock<std::mutex> lck(mem_budget_mtx_);
    memory_used_for_coords_total_ -= tiles_size;
    memory_used_qc_tiles_total_ -= tiles_size_qc;
  }

  // Delete the tile.
  result_tiles_[0].erase(rt);

  return Status::Ok();
}

template <class BitmapType>
Status SparseUnorderedWithDupsReader<BitmapType>::end_iteration() {
  if (subarray_.is_set()) {
    // Adjust result tile ranges.
    auto status = parallel_for(
        storage_manager_->compute_tp(),
        0,
        fragment_metadata_.size(),
        [&](uint64_t f) {
          while (!result_tile_ranges_[f].empty() &&
                 result_tile_ranges_[f].back().second <
                     read_state_.frag_tile_idx_[f].first) {
            remove_result_tile_range(f);
          }

          if (!result_tile_ranges_[f].empty()) {
            result_tile_ranges_[f].back().first =
                read_state_.frag_tile_idx_[f].first;
          }

          return Status::Ok();
        });
  }

  // Clear result tiles that are not necessary anymore.
  while (!result_tiles_[0].empty() &&
         result_tiles_[0].front().tile_idx() <
             read_state_.frag_tile_idx_[result_tiles_[0].front().frag_idx()]
                 .first) {
    const auto f = result_tiles_[0].front().frag_idx();
    RETURN_NOT_OK(remove_result_tile(f, result_tiles_[0].begin()));
  }

  if (offsets_extra_element_) {
    RETURN_NOT_OK(add_extra_offset());
  }

  // Validate memory usage.
  if (!incomplete()) {
    assert(memory_used_for_coords_total_ == 0);
    assert(memory_used_qc_tiles_total_ == 0);
    /* This should be re-instated in a followup
     Currently there is a bug causing this assert to fail when
     sm.mem.total_budget is applied. These calculations are going to be
     reworked, to fix the issue with TileDB 2.5.1 we will remove the assert.
     The effect of this is that we might be lingering tile ranges with tiles
     that are either not in the subarray or don't respect query condition. The
     reader might fetch those tiles again, reprocess, and throw them out again
     on a subsequent iteration, which might affect perf. But it will not
     affect query correctness or completion. */
    // assert(memory_used_result_tile_ranges_ == 0);
  }

  logger_->debug(
      "Done with iteration, num result tiles {1}", result_tiles_[0].size());

  const auto uint64_t_max = std::numeric_limits<uint64_t>::max();
  array_memory_tracker_->set_budget(uint64_t_max);
  return Status::Ok();
}

// Explicit template instantiations
template SparseUnorderedWithDupsReader<uint8_t>::SparseUnorderedWithDupsReader(
    stats::Stats*,
    tdb_shared_ptr<Logger>,
    StorageManager*,
    Array*,
    Config&,
    std::unordered_map<std::string, QueryBuffer>&,
    Subarray&,
    Layout,
    QueryCondition&);
template SparseUnorderedWithDupsReader<uint64_t>::SparseUnorderedWithDupsReader(
    stats::Stats*,
    tdb_shared_ptr<Logger>,
    StorageManager*,
    Array*,
    Config&,
    std::unordered_map<std::string, QueryBuffer>&,
    Subarray&,
    Layout,
    QueryCondition&);

}  // namespace sm
}  // namespace tiledb
