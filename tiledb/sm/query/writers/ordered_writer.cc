/**
 * @file   ordered_writer.cc
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
 * This file implements class OrderedWriter.
 */

#include "tiledb/sm/query/writers/ordered_writer.h"
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
#include "tiledb/type/apply_with_type.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb::sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

OrderedWriter::OrderedWriter(
    stats::Stats* stats,
    shared_ptr<Logger> logger,
    StrategyParams& params,
    std::vector<WrittenFragmentInfo>& written_fragment_info,
    Query::CoordsInfo& coords_info,
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
    , frag_uri_(std::nullopt) {
  if (layout_ != Layout::ROW_MAJOR && layout_ != Layout::COL_MAJOR) {
    throw StatusException(Status_WriterError(
        "Failed to initialize OrderedWriter; The ordered writer does not "
        "support layout " +
        layout_str(layout_)));
  }

  if (!array_schema_.dense()) {
    throw StatusException(
        Status_WriterError("Failed to initialize OrderedWriter; The ordered "
                           "writer does not support sparse arrays."));
  }
}

OrderedWriter::~OrderedWriter() {
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status OrderedWriter::dowork() {
  get_dim_attr_stats();

  auto timer_se = stats_->start_timer("dowork");

  check_attr_order();

  // In case the user has provided a coordinates buffer
  RETURN_NOT_OK(split_coords_buffer());

  if (check_coord_oob_) {
    RETURN_NOT_OK(check_coord_oob());
  }

  try {
    auto status = ordered_write();
    if (!status.ok()) {
      clean_up();
      return status;
    }
  } catch (...) {
    clean_up();
    std::throw_with_nested(std::runtime_error("[OrderedWriter::dowork] "));
  }

  return Status::Ok();
}

Status OrderedWriter::finalize() {
  auto timer_se = stats_->start_timer("finalize");

  return Status::Ok();
}

void OrderedWriter::reset() {
}

std::string OrderedWriter::name() {
  return "OrderedWriter";
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

void OrderedWriter::clean_up() {
  if (frag_uri_.has_value()) {
    throw_if_not_ok(resources_.vfs().remove_dir(frag_uri_.value()));
  }
}

Status OrderedWriter::ordered_write() {
  // Applicable only to ordered write on dense arrays
  assert(layout_ == Layout::ROW_MAJOR || layout_ == Layout::COL_MAJOR);
  assert(array_schema_.dense());

  auto type{array_schema_.domain().dimension_ptr(0)->type()};

  auto g = [&](auto T) {
    if constexpr (tiledb::type::TileDBIntegral<decltype(T)>) {
      return ordered_write<decltype(T)>();
    }
    return Status_WriterError(
        "Cannot write in ordered layout; Unsupported domain type");
  };
  return apply_with_type(g, type);
}

template <class T>
Status OrderedWriter::ordered_write() {
  auto timer_se = stats_->start_timer("ordered_write");

  // Create new fragment
  auto frag_meta = this->create_fragment_metadata();
  RETURN_CANCEL_OR_ERROR(create_fragment(true, frag_meta));
  frag_uri_ = frag_meta->fragment_uri();

  // Create a dense tiler
  DenseTiler<T> dense_tiler(
      query_memory_tracker_,
      &buffers_,
      &subarray_,
      stats_,
      offsets_format_mode_,
      offsets_bitsize_,
      offsets_extra_element_);
  auto tile_num = dense_tiler.tile_num();

  // Set number of tiles in the fragment metadata
  frag_meta->set_num_tiles(
      tile_num,
      frag_meta->loaded_metadata()->tile_offsets(),
      frag_meta->loaded_metadata()->tile_var_offsets(),
      frag_meta->loaded_metadata()->tile_var_sizes(),
      frag_meta->loaded_metadata()->tile_validity_offsets(),
      frag_meta->loaded_metadata()->tile_min_buffer(),
      frag_meta->loaded_metadata()->tile_max_buffer(),
      frag_meta->loaded_metadata()->tile_sums(),
      frag_meta->loaded_metadata()->tile_null_counts());
  if (!frag_meta->dense()) {
    frag_meta->loaded_metadata()->rtree().set_leaf_num(tile_num);
  }

  // Prepare, filter and write tiles for all attributes
  auto attr_num = buffers_.size();
  auto* compute_tp = &resources_.compute_tp();
  auto thread_num = compute_tp->concurrency_level();
  std::unordered_map<std::string, IndexedList<WriterTileTupleVector>> tiles;
  for (const auto& buff : buffers_) {
    tiles.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(buff.first),
        std::forward_as_tuple(query_memory_tracker_));
  }

  if (attr_num > tile_num) {  // Parallelize over attributes
    RETURN_NOT_OK(parallel_for(compute_tp, 0, attr_num, [&](uint64_t i) {
      auto buff_it = buffers_.begin();
      std::advance(buff_it, i);
      const auto& attr = buff_it->first;
      auto& attr_tile_batches = tiles.at(attr);
      return prepare_filter_and_write_tiles<T>(
          attr, attr_tile_batches, frag_meta, &dense_tiler, 1);
    }));
  } else {  // Parallelize over tiles
    for (const auto& buff : buffers_) {
      const auto& attr = buff.first;
      auto& attr_tile_batches = tiles.at(attr);
      RETURN_NOT_OK(prepare_filter_and_write_tiles<T>(
          attr, attr_tile_batches, frag_meta, &dense_tiler, thread_num));
    }
  }

  // Fix the tile metadata for var size attributes.
  if (attr_num > tile_num) {  // Parallelize over attributes
    RETURN_NOT_OK(parallel_for(compute_tp, 0, attr_num, [&](uint64_t i) {
      auto buff_it = buffers_.begin();
      std::advance(buff_it, i);
      const auto& attr = buff_it->first;
      const auto var_size = array_schema_.var_size(attr);
      if (has_min_max_metadata(attr, var_size) &&
          array_schema_.var_size(attr)) {
        auto& attr_tile_batches = tiles.at(attr);
        frag_meta->convert_tile_min_max_var_sizes_to_offsets(
            attr,
            frag_meta->loaded_metadata()->tile_min_var_buffer(),
            frag_meta->loaded_metadata()->tile_min_buffer(),
            frag_meta->loaded_metadata()->tile_max_var_buffer(),
            frag_meta->loaded_metadata()->tile_max_buffer());
        for (auto& batch : attr_tile_batches) {
          uint64_t idx = 0;
          for (auto& tile : batch) {
            frag_meta->set_tile_min_var(
                attr,
                idx,
                tile.min(),
                frag_meta->loaded_metadata()->tile_min_buffer(),
                frag_meta->loaded_metadata()->tile_min_var_buffer());
            frag_meta->set_tile_max_var(
                attr,
                idx,
                tile.max(),
                frag_meta->loaded_metadata()->tile_max_buffer(),
                frag_meta->loaded_metadata()->tile_max_var_buffer());
            idx++;
          }
        }
      }
      return Status::Ok();
    }));
  } else {  // Parallelize over tiles
    for (const auto& buff : buffers_) {
      const auto& attr = buff.first;
      auto& attr_tile_batches = tiles.at(attr);
      const auto var_size = array_schema_.var_size(attr);
      if (has_min_max_metadata(attr, var_size) &&
          array_schema_.var_size(attr)) {
        frag_meta->convert_tile_min_max_var_sizes_to_offsets(
            attr,
            frag_meta->loaded_metadata()->tile_min_var_buffer(),
            frag_meta->loaded_metadata()->tile_min_buffer(),
            frag_meta->loaded_metadata()->tile_max_var_buffer(),
            frag_meta->loaded_metadata()->tile_max_buffer());
        RETURN_NOT_OK(parallel_for(
            compute_tp, 0, attr_tile_batches.size(), [&](uint64_t b) {
              const auto& attr = buff.first;
              auto& batch = tiles.at(attr)[b];
              auto idx = b * thread_num;
              for (auto& tile : batch) {
                frag_meta->set_tile_min_var(
                    attr,
                    idx,
                    tile.min(),
                    frag_meta->loaded_metadata()->tile_min_buffer(),
                    frag_meta->loaded_metadata()->tile_min_var_buffer());
                frag_meta->set_tile_max_var(
                    attr,
                    idx,
                    tile.max(),
                    frag_meta->loaded_metadata()->tile_max_buffer(),
                    frag_meta->loaded_metadata()->tile_max_var_buffer());
                idx++;
              }
              return Status::Ok();
            }));
      }
    }
  }

  // Compute fragment min/max/sum/null count and write the fragment metadata
  frag_meta->loaded_metadata()->compute_fragment_min_max_sum_null_count();
  frag_meta->storefrag_meta->loaded_metadata_shared(), array_->get_encryption_key());

  // Add written fragment info
  RETURN_NOT_OK(add_written_fragment_info(frag_uri_.value()));

  // The following will make the fragment visible
  URI commit_uri = array_->array_directory().get_commit_uri(frag_uri_.value());
  throw_if_not_ok(resources_.vfs().touch(commit_uri));

  return Status::Ok();
}

template <class T>
Status OrderedWriter::prepare_filter_and_write_tiles(
    const std::string& name,
    IndexedList<WriterTileTupleVector>& tile_batches,
    shared_ptr<FragmentMetadata> frag_meta,
    DenseTiler<T>* dense_tiler,
    uint64_t thread_num) {
  auto timer_se = stats_->start_timer("prepare_filter_and_write_tiles");

  // For easy reference
  const auto type = array_schema_.type(name);
  const bool var = array_schema_.var_size(name);
  const auto cell_size = array_schema_.cell_size(name);
  const bool nullable = array_schema_.is_nullable(name);
  const auto& domain{array_schema_.domain()};
  const auto capacity = array_schema_.capacity();
  const auto cell_num_per_tile =
      coords_info_.has_coords_ ? capacity : domain.cell_num_per_tile();

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
  tile_batches.resize(batch_num);
  std::optional<ThreadPool::Task> write_task = nullopt;
  for (uint64_t b = 0; b < batch_num; ++b) {
    auto batch_size = (b == batch_num - 1) ? last_batch_size : thread_num;
    assert(batch_size > 0);
    tile_batches[b].reserve(batch_size);
    for (uint64_t i = 0; i < batch_size; i++) {
      tile_batches[b].emplace_back(
          array_schema_,
          cell_num_per_tile,
          var,
          nullable,
          cell_size,
          type,
          query_memory_tracker_);
    }

    {
      auto timer_se = stats_->start_timer("prepare_and_filter_tiles");
      auto st = parallel_for(
          &resources_.compute_tp(), 0, batch_size, [&](uint64_t i) {
            // Prepare and filter tiles
            auto& writer_tile = tile_batches[b][i];
            throw_if_not_ok(
                dense_tiler->get_tile(frag_tile_id + i, name, writer_tile));

            if (!var) {
              throw_if_not_ok(filter_tile(
                  name, &writer_tile.fixed_tile(), nullptr, false, false));
            } else {
              auto offset_tile = &writer_tile.offset_tile();
              throw_if_not_ok(filter_tile(
                  name, &writer_tile.var_tile(), offset_tile, false, false));
              throw_if_not_ok(
                  filter_tile(name, offset_tile, nullptr, true, false));
            }
            if (nullable) {
              throw_if_not_ok(filter_tile(
                  name, &writer_tile.validity_tile(), nullptr, false, true));
            }
            return Status::Ok();
          });
      RETURN_NOT_OK(st);
    }

    if (write_task.has_value()) {
      write_task->wait();
      RETURN_NOT_OK(write_task->get());
    }

    write_task = resources_.io_tp().execute([&, b, frag_tile_id]() {
      close_files = (b == batch_num - 1);
      RETURN_NOT_OK(write_tiles(
          0,
          tile_batches[b].size(),
          name,
          frag_meta,
          frag_tile_id,
          &tile_batches[b],
          close_files));

      return Status::Ok();
    });

    frag_tile_id += batch_size;
  }

  if (write_task.has_value()) {
    write_task->wait();
    RETURN_NOT_OK(write_task->get());
  }

  return Status::Ok();
}

}  // namespace tiledb::sm
