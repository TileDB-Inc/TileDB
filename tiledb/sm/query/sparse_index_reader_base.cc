/**
 * @file   sparse_index_reader_base.cc
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
 * This file implements class SparseIndexReaderBase.
 */

#include "tiledb/sm/query/sparse_index_reader_base.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/resource_pool.h"
#include "tiledb/sm/query/iquery_strategy.h"
#include "tiledb/sm/query/query_macros.h"
#include "tiledb/sm/query/strategy_base.h"
#include "tiledb/sm/storage_manager/open_array_memory_tracker.h"
#include "tiledb/sm/subarray/subarray.h"

namespace tiledb {
namespace sm {

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

SparseIndexReaderBase::SparseIndexReaderBase(
    stats::Stats* stats,
    tdb_shared_ptr<Logger> logger,
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout,
    QueryCondition& condition)
    : ReaderBase(
          stats,
          logger,
          storage_manager,
          array,
          config,
          buffers,
          subarray,
          layout,
          condition)
    , initial_data_loaded_(false)
    , memory_budget_(0)
    , array_memory_tracker_(nullptr)
    , memory_used_for_coords_total_(0)
    , memory_used_qc_tiles_total_(0)
    , memory_used_result_tile_ranges_(0)
    , memory_budget_ratio_coords_(0.5)
    , memory_budget_ratio_query_condition_(0.25)
    , memory_budget_ratio_tile_ranges_(0.1)
    , memory_budget_ratio_array_data_(0.1)
    , coords_loaded_(true) {
  read_state_.done_adding_result_tiles_ = false;
}

/* ****************************** */
/*        PROTECTED METHODS       */
/* ****************************** */

const typename SparseIndexReaderBase::ReadState*
SparseIndexReaderBase::read_state() const {
  return &read_state_;
}

typename SparseIndexReaderBase::ReadState* SparseIndexReaderBase::read_state() {
  return &read_state_;
}

Status SparseIndexReaderBase::init() {
  // Sanity checks
  if (storage_manager_ == nullptr)
    return logger_->status(Status::ReaderError(
        "Cannot initialize sparse global order reader; Storage manager not "
        "set"));
  if (array_schema_ == nullptr)
    return logger_->status(Status::ReaderError(
        "Cannot initialize sparse global order reader; Array schema not set"));
  if (buffers_.empty())
    return logger_->status(Status::ReaderError(
        "Cannot initialize sparse global order reader; Buffers not set"));

  // Check subarray
  RETURN_NOT_OK(check_subarray());

  // Load offset configuration options.
  bool found = false;
  offsets_format_mode_ = config_.get("sm.var_offsets.mode", &found);
  assert(found);
  if (offsets_format_mode_ != "bytes" && offsets_format_mode_ != "elements") {
    return logger_->status(
        Status::ReaderError("Cannot initialize reader; Unsupported offsets "
                            "format in configuration"));
  }
  elements_mode_ = offsets_format_mode_ == "elements";

  RETURN_NOT_OK(config_.get<bool>(
      "sm.var_offsets.extra_element", &offsets_extra_element_, &found));
  assert(found);
  RETURN_NOT_OK(config_.get<uint32_t>(
      "sm.var_offsets.bitsize", &offsets_bitsize_, &found));
  if (offsets_bitsize_ != 32 && offsets_bitsize_ != 64) {
    return logger_->status(
        Status::ReaderError("Cannot initialize reader; "
                            "Unsupported offsets bitsize in configuration"));
  }

  // Check the validity buffer sizes.
  RETURN_NOT_OK(check_validity_buffer_sizes());

  return Status::Ok();
}

template <class BitmapType>
Status SparseIndexReaderBase::get_coord_tiles_size(
    bool include_coords,
    unsigned dim_num,
    unsigned f,
    uint64_t t,
    uint64_t* tiles_size,
    uint64_t* tiles_size_qc) {
  *tiles_size = 0;

  // Add the coordinate tiles size.
  if (include_coords) {
    for (unsigned d = 0; d < dim_num; d++) {
      *tiles_size += fragment_metadata_[f]->tile_size(dim_names_[d], t);

      if (is_dim_var_size_[d]) {
        uint64_t temp = 0;
        RETURN_NOT_OK(
            fragment_metadata_[f]->tile_var_size(dim_names_[d], t, &temp));
        *tiles_size += temp;
      }
    }
  }

  // Add the result tile structure size.
  *tiles_size += sizeof(ResultTileWithBitmap<BitmapType>);

  // Add the tile bitmap size if there is a subarray.
  if (subarray_.is_set())
    *tiles_size += fragment_metadata_[f]->cell_num(t) * sizeof(BitmapType);

  // Compute query condition tile sizes.
  *tiles_size_qc = 0;
  if (!qc_loaded_names_.empty()) {
    for (auto& name : qc_loaded_names_) {
      // Calculate memory consumption for this tile.
      uint64_t tile_size = 0;
      RETURN_NOT_OK(get_attribute_tile_size(name, f, t, &tile_size));
      *tiles_size_qc += tile_size;
    }
  }

  return Status::Ok();
}

Status SparseIndexReaderBase::load_initial_data() {
  if (initial_data_loaded_)
    return Status::Ok();

  auto timer_se = stats_->start_timer("load_initial_data");
  read_state_.done_adding_result_tiles_ = false;

  // Make a list of dim/attr that will be loaded for query condition.
  if (!initial_data_loaded_) {
    if (!condition_.empty()) {
      for (auto& name : condition_.field_names()) {
        qc_loaded_names_.emplace_back(name);
      }
    }
  }

  // For easy reference.
  auto fragment_num = fragment_metadata_.size();

  // Make sure there is enough space for tiles data.
  read_state_.frag_tile_idx_.clear();
  all_tiles_loaded_.clear();
  read_state_.frag_tile_idx_.resize(fragment_num);
  all_tiles_loaded_.resize(fragment_num);

  // Calculate ranges of tiles in the subarray, if set.
  if (subarray_.is_set()) {
    // At this point, full memory budget is available.
    array_memory_tracker_->set_budget(memory_budget_);

    // Make sure there is no memory taken by the subarray.
    subarray_.clear_tile_overlap();

    // Tile ranges computation will not stop if it exceeds memory budget.
    // This is ok as it is a soft limit and will be taken into consideration
    // later.
    RETURN_NOT_OK(subarray_.precompute_all_ranges_tile_overlap(
        storage_manager_->compute_tp(), &result_tile_ranges_));

    for (auto frag_result_tile_ranges : result_tile_ranges_) {
      memory_used_result_tile_ranges_ += frag_result_tile_ranges.size() *
                                         sizeof(std::pair<uint64_t, uint64_t>);
    }

    if (memory_used_result_tile_ranges_ >
        memory_budget_ratio_tile_ranges_ * memory_budget_)
      return logger_->status(
          Status::ReaderError("Exceeded memory budget for result tile ranges"));
  }

  // Set a limit to the array memory.
  array_memory_tracker_->set_budget(
      memory_budget_ * memory_budget_ratio_array_data_);

  // Preload zipped coordinate tile offsets. Note that this will
  // ignore fragments with a version >= 5.
  std::vector<std::string> zipped_coords_names = {constants::coords};
  RETURN_CANCEL_OR_ERROR(load_tile_offsets(&subarray_, &zipped_coords_names));

  // Preload unzipped coordinate tile offsets. Note that this will
  // ignore fragments with a version < 5.
  const auto dim_num = array_schema_->dim_num();
  dim_names_.reserve(dim_num);
  is_dim_var_size_.reserve(dim_num);
  std::vector<std::string> var_size_to_load;
  for (unsigned d = 0; d < dim_num; ++d) {
    dim_names_.emplace_back(array_schema_->dimension(d)->name());
    is_dim_var_size_[d] = array_schema_->var_size(dim_names_[d]);
    if (is_dim_var_size_[d])
      var_size_to_load.emplace_back(dim_names_[d]);
  }
  RETURN_CANCEL_OR_ERROR(load_tile_offsets(&subarray_, &dim_names_));

  // Compute tile offsets to load and var size to load for attributes.
  std::vector<std::string> attr_tile_offsets_to_load;
  for (auto& it : buffers_) {
    const auto& name = it.first;
    if (array_schema_->is_dim(name))
      continue;

    attr_tile_offsets_to_load.emplace_back(name);

    if (array_schema_->var_size(name))
      var_size_to_load.emplace_back(name);
  }

  // Load tile offsets and var sizes for attributes.
  RETURN_CANCEL_OR_ERROR(load_tile_var_sizes(&subarray_, &var_size_to_load));
  RETURN_CANCEL_OR_ERROR(
      load_tile_offsets(&subarray_, &attr_tile_offsets_to_load));

  logger_->debug("Initial data loaded");
  initial_data_loaded_ = true;
  return Status::Ok();
}

Status SparseIndexReaderBase::read_and_unfilter_coords(
    bool include_coords, const std::vector<ResultTile*>* result_tiles) {
  auto timer_se = stats_->start_timer("read_and_unfilter_coords");

  // Not including coords or no query condition, exit.
  if (!include_coords && condition_.empty())
    return Status::Ok();

  if (subarray_.is_set() || include_coords) {
    // Read and unfilter zipped coordinate tiles. Note that
    // this will ignore fragments with a version >= 5.
    std::vector<std::string> zipped_coords_names = {constants::coords};
    RETURN_CANCEL_OR_ERROR(
        read_coordinate_tiles(&zipped_coords_names, result_tiles));
    RETURN_CANCEL_OR_ERROR(unfilter_tiles(constants::coords, result_tiles));

    // Read and unfilter unzipped coordinate tiles. Note that
    // this will ignore fragments with a version < 5.
    RETURN_CANCEL_OR_ERROR(read_coordinate_tiles(&dim_names_, result_tiles));
    for (const auto& dim_name : dim_names_) {
      RETURN_CANCEL_OR_ERROR(unfilter_tiles(dim_name, result_tiles));
    }
  }

  if (!condition_.empty()) {
    // Read and unfilter tiles for querty condition.
    RETURN_CANCEL_OR_ERROR(
        read_attribute_tiles(&qc_loaded_names_, result_tiles));

    for (const auto& name : qc_loaded_names_) {
      RETURN_CANCEL_OR_ERROR(unfilter_tiles(name, result_tiles));
    }
  }

  logger_->debug("Done reading and unfiltering coords tiles");
  return Status::Ok();
}

/** Template specialisation for uint64_t which does result count. */
template <>
Status SparseIndexReaderBase::compute_tile_bitmaps<uint64_t>(
    ResultTileListPerFragment<uint64_t>* result_tiles) {
  auto timer_se = stats_->start_timer("compute_tile_bitmaps");

  // For easy reference.
  const auto domain = array_schema_->domain();
  const auto dim_num = array_schema_->dim_num();
  const auto cell_order = array_schema_->cell_order();

  // No subarray set, return.
  if (!subarray_.is_set()) {
    return Status::Ok();
  }

  // Compute the number of non-default dimensions.
  unsigned non_default_dims = 0;
  for (unsigned d = 0; d < dim_num; d++) {
    if (!subarray_.is_default(d))
      non_default_dims++;
  }

  // If we have more than one non default dim, we'll need one temp bitmap per
  // thread.
  tdb_unique_ptr<ResourcePool<std::vector<uint64_t>>> bitmap_pool;
  if (non_default_dims > 1) {
    const auto num_threads =
        storage_manager_->compute_tp()->concurrency_level();
    bitmap_pool.reset(
        tdb_new(ResourcePool<std::vector<uint64_t>>, num_threads));
  }

  // Process all tiles in parallel.
  auto result_tiles_it = ResultTileIt(result_tiles);
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, result_tiles_it.size(), [&](uint64_t) {
        // Take a result tile.
        auto rt = result_tiles_it.get_next();

        // Bitmap was already computed for this tile.
        if (rt->bitmap_num_cells != std::numeric_limits<uint64_t>::max()) {
          return Status::Ok();
        }

        // Make sure we have a bitmap available if there is more than one
        // non default dimensions.
        typedef ResourceGuard<std::vector<uint64_t>, ResourcePool>
            Uint64VectorPool;
        tdb_unique_ptr<Uint64VectorPool> guard = nullptr;
        if (non_default_dims > 1)
          guard.reset(tdb_new(Uint64VectorPool, *bitmap_pool.get()));

        // Compute bitmaps one dimension at a time.
        bool use_pool_buffer = false;
        for (unsigned d = 0; d < dim_num; d++) {
          bool tile_used = false;

          // For col-major cell ordering, iterate the dimensions
          // in reverse.
          const unsigned dim_idx =
              cell_order == Layout::COL_MAJOR ? dim_num - d - 1 : d;

          // No need to compute bitmaps for default dimensions.
          if (subarray_.is_default(dim_idx))
            continue;

          // Process all ranges at once.
          // Note: this could be optimized. However, the result count is
          // slowly going to get phased out so for now this code is going
          // for simplicity.
          rt->bitmap.resize(rt->cell_num());

          // Make sure the pool buffer is ready for use.
          if (use_pool_buffer) {
            auto& pool_buffer = guard->get();
            uint64_t memset_length =
                std::min((uint64_t)pool_buffer.size(), rt->cell_num());
            memset(pool_buffer.data(), 0, memset_length * sizeof(uint64_t));
            pool_buffer.resize(rt->cell_num());
          }

          // Process all ranges in parallel.
          const auto& ranges_for_dim = subarray_.ranges_for_dim(dim_idx);
          std::mutex range_mtx;
          auto status = parallel_for(
              storage_manager_->compute_tp(),
              0,
              ranges_for_dim.size(),
              [&](uint64_t r) {
                // Figure out what to do with the tile.
                bool full_overlap = false;
                const auto& mbr =
                    fragment_metadata_[rt->frag_idx()]->mbr(rt->tile_idx());
                bool in_range = domain->dimension(dim_idx)->overlap(
                    ranges_for_dim[r], mbr[dim_idx]);
                if (in_range) {
                  tile_used = true;
                  full_overlap = domain->dimension(dim_idx)->covered(
                      mbr[dim_idx], ranges_for_dim[r]);
                } else {
                  return Status::Ok();
                }

                if (!full_overlap) {
                  // Calculate the bitmap for the cells.
                  RETURN_NOT_OK(rt->compute_results_count_sparse(
                      dim_idx,
                      ranges_for_dim[r],
                      use_pool_buffer ? &guard->get() : &rt->bitmap,
                      use_pool_buffer,
                      &rt->bitmap,
                      cell_order,
                      range_mtx));
                } else {
                  auto& buffer = use_pool_buffer ? guard->get() : rt->bitmap;
                  std::unique_lock<std::mutex> ul(range_mtx);
                  for (uint64_t c = 0; c < rt->cell_num(); ++c) {
                    buffer[c]++;
                  }
                }

                return Status::Ok();
              });
          RETURN_NOT_OK_ELSE(status, logger_->status(status));

          if (!tile_used) {
            return Status::Ok();
          }

          // Multiply the result of this dimension by the result of the last.
          if (use_pool_buffer) {
            auto& pool_buffer = guard->get();
            for (uint64_t c = 0; c < rt->cell_num(); ++c) {
              rt->bitmap[c] *= pool_buffer[c];
            }
          }

          use_pool_buffer = true;
        }

        // Compute number of cells in this tile.
        rt->bitmap_num_cells = 0;
        for (uint64_t c = 0; c < rt->cell_num(); ++c) {
          rt->bitmap_num_cells += rt->bitmap[c];
        }

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, logger_->status(status));

  logger_->debug("Done computing tile bitmaps");
  return Status::Ok();
}

/** Template specialisation for uint8_t which does result bitmap. */
template <>
Status SparseIndexReaderBase::compute_tile_bitmaps<uint8_t>(
    ResultTileListPerFragment<uint8_t>* result_tiles) {
  auto timer_se = stats_->start_timer("compute_tile_bitmaps");

  // For easy reference.
  const auto domain = array_schema_->domain();
  const auto dim_num = array_schema_->dim_num();
  const auto cell_order = array_schema_->cell_order();

  // No subarray set, return.
  if (!subarray_.is_set()) {
    return Status::Ok();
  }

  // Process all tiles in parallel.
  auto result_tiles_it = ResultTileIt(result_tiles);
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, result_tiles_it.size(), [&](uint64_t) {
        // Take a result tile.
        auto rt = result_tiles_it.get_next();

        // Bitmap was already computed for this tile.
        if (rt->bitmap_num_cells != std::numeric_limits<uint64_t>::max()) {
          return Status::Ok();
        }

        // Compute bitmaps one dimension at a time.
        bool has_previous_dim = false;
        for (unsigned d = 0; d < dim_num; d++) {
          bool tile_used = false;

          // For col-major cell ordering, iterate the dimensions
          // in reverse.
          const unsigned dim_idx =
              cell_order == Layout::COL_MAJOR ? dim_num - d - 1 : d;

          // No need to compute bitmaps for default dimensions.
          if (subarray_.is_default(dim_idx))
            continue;

          // Process all ranges in parallel.
          std::mutex bitmap_resize_mtx;
          const auto& ranges_for_dim = subarray_.ranges_for_dim(dim_idx);
          auto status = parallel_for(
              storage_manager_->compute_tp(),
              0,
              ranges_for_dim.size(),
              [&](uint64_t r) {
                // Figure out what to do with the tile.
                bool full_overlap = false;
                const auto& mbr =
                    fragment_metadata_[rt->frag_idx()]->mbr(rt->tile_idx());
                bool in_range = domain->dimension(dim_idx)->overlap(
                    ranges_for_dim[r], mbr[dim_idx]);
                if (in_range) {
                  tile_used = true;
                  full_overlap = domain->dimension(dim_idx)->covered(
                      mbr[dim_idx], ranges_for_dim[r]);
                } else {
                  return Status::Ok();
                }

                // If there is full overlap, no need to calculate anything for
                // this dim, ranges are not overlapping so all other ranges will
                // have no overlap, all previous results should stay the same.
                if (!full_overlap) {
                  {
                    // Make sure the bitmap is ready.
                    std::unique_lock<std::mutex> ul(bitmap_resize_mtx);
                    rt->bitmap.resize(rt->cell_num());
                  }

                  // Calculate the bitmap for the cells.
                  RETURN_NOT_OK(rt->compute_results_bitmap_sparse(
                      dim_idx,
                      ranges_for_dim[r],
                      has_previous_dim,
                      &rt->bitmap,
                      cell_order));
                }

                return Status::Ok();
              });
          RETURN_NOT_OK_ELSE(status, logger_->status(status));

          if (!tile_used) {
            return Status::Ok();
          }

          // If there was full overlap, consider there is no previous dim.
          has_previous_dim = rt->bitmap.size() != 0;
        }

        // Compute number of cells in this tile.
        // If bitmap was not resized, we have full overlap.
        if (rt->bitmap.size() == 0) {
          rt->bitmap_num_cells = rt->cell_num();
        } else {
          rt->bitmap_num_cells = 0;
          for (uint64_t c = 0; c < rt->cell_num(); ++c) {
            rt->bitmap_num_cells += rt->bitmap[c];
          }
        }

        return Status::Ok();
      });
  RETURN_NOT_OK_ELSE(status, logger_->status(status));

  logger_->debug("Done computing tile bitmaps");
  return Status::Ok();
}

template <class BitmapType>
Status SparseIndexReaderBase::apply_query_condition(
    ResultTileListPerFragment<BitmapType>* result_tiles) {
  auto timer_se = stats_->start_timer("apply_query_condition");

  if (!condition_.empty()) {
    // Process all tiles in parallel.
    auto result_tiles_it = ResultTileIt(result_tiles);
    auto status = parallel_for(
        storage_manager_->compute_tp(),
        0,
        result_tiles_it.size(),
        [&](uint64_t) {
          // Take a result tile.
          auto rt = result_tiles_it.get_next();

          // Set num_cells if no subarray as it's used to filter below.
          if (!subarray_.is_set()) {
            rt->bitmap_num_cells = rt->cell_num();
          }

          // Max bitmap_num_cells means the tile had no overlap, skip it.
          if (!rt->qc_processed &&
              rt->bitmap_num_cells != std::numeric_limits<uint64_t>::max()) {
            // Full overlap in bitmap calculation, make a bitmap.
            if (rt->bitmap.size() == 0) {
              rt->bitmap.resize(rt->cell_num(), 1);
              rt->bitmap_num_cells = rt->cell_num();
            }

            // Compute the result of the query condition for this tile.
            RETURN_NOT_OK(condition_.apply_sparse<BitmapType>(
                array_schema_, &*rt, rt->bitmap.data(), &rt->bitmap_num_cells));
            rt->qc_processed = true;
          }

          return Status::Ok();
        });
    RETURN_NOT_OK_ELSE(status, logger_->status(status));
  }

  logger_->debug("Done applying query condition");
  return Status::Ok();
}

Status SparseIndexReaderBase::resize_output_buffers(uint64_t cells_copied) {
  // Resize buffers if the result cell slabs was truncated.
  for (auto& it : buffers_) {
    const auto& name = it.first;
    const auto size = *it.second.buffer_size_;
    uint64_t num_cells = 0;

    if (array_schema_->var_size(name)) {
      // Get the current number of cells from the offsets buffer.
      num_cells = size / constants::cell_var_offset_size;

      // Remove an element if the extra element flag is set.
      if (offsets_extra_element_ && num_cells > 0)
        num_cells--;

      // Buffer should be resized.
      if (num_cells > cells_copied) {
        // Offsets buffer is trivial.
        *(it.second.buffer_size_) =
            cells_copied * constants::cell_var_offset_size +
            offsets_extra_element_;

        // Since the buffer is shrunk, there is an offset for the next element
        // loaded, use it.
        if (offsets_bitsize_ == 64) {
          uint64_t offset_div =
              elements_mode_ ? datatype_size(array_schema_->type(name)) : 1;
          *it.second.buffer_var_size_ =
              ((uint64_t*)it.second.buffer_)[cells_copied] * offset_div;
        } else {
          uint32_t offset_div =
              elements_mode_ ? datatype_size(array_schema_->type(name)) : 1;
          *it.second.buffer_var_size_ =
              ((uint32_t*)it.second.buffer_)[cells_copied] * offset_div;
        }
      }
    } else {
      // Always adjust the size for fixed size attributes.
      auto cell_size = array_schema_->cell_size(name);
      *(it.second.buffer_size_) = cells_copied * cell_size;
    }

    // Always adjust validity vector size, if present.
    if (num_cells > cells_copied) {
      if (it.second.validity_vector_.buffer_size() != nullptr)
        *(it.second.validity_vector_.buffer_size()) =
            num_cells * constants::cell_validity_size;
    }
  }

  return Status::Ok();
}

Status SparseIndexReaderBase::add_extra_offset() {
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    if (!array_schema_->var_size(name))
      continue;

    auto buffer = static_cast<unsigned char*>(it.second.buffer_);
    if (offsets_format_mode_ == "bytes") {
      memcpy(
          buffer + *it.second.buffer_size_ - offsets_bytesize(),
          it.second.buffer_var_size_,
          offsets_bytesize());
    } else if (offsets_format_mode_ == "elements") {
      auto elements = *it.second.buffer_var_size_ /
                      datatype_size(array_schema_->type(name));
      memcpy(
          buffer + *it.second.buffer_size_ - offsets_bytesize(),
          &elements,
          offsets_bytesize());
    } else {
      return logger_->status(Status::ReaderError(
          "Cannot add extra offset to buffer; Unsupported offsets format"));
    }
  }

  return Status::Ok();
}

void SparseIndexReaderBase::remove_result_tile_range(uint64_t f) {
  result_tile_ranges_[f].pop_back();
  {
    std::unique_lock<std::mutex> lck(mem_budget_mtx_);
    memory_used_result_tile_ranges_ -= sizeof(std::pair<uint64_t, uint64_t>);
  }
}

// Explicit template instantiations
template Status SparseIndexReaderBase::get_coord_tiles_size<uint64_t>(
    bool, unsigned, unsigned, uint64_t, uint64_t*, uint64_t*);
template Status SparseIndexReaderBase::get_coord_tiles_size<uint8_t>(
    bool, unsigned, unsigned, uint64_t, uint64_t*, uint64_t*);
template Status SparseIndexReaderBase::apply_query_condition(
    ResultTileListPerFragment<uint64_t>*);
template Status SparseIndexReaderBase::apply_query_condition(
    ResultTileListPerFragment<uint8_t>*);

}  // namespace sm
}  // namespace tiledb
