/**
 * @file   ordered_writer.cc
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
 * This file implements class OrderedWriter.
 */

#include "tiledb/sm/query/ordered_writer.h"
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

OrderedWriter::OrderedWriter(
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

OrderedWriter::~OrderedWriter() {
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status OrderedWriter::dowork() {
  get_dim_attr_stats();

  auto timer_se = stats_->start_timer("write");

  // In case the user has provided a coordinates buffer
  RETURN_NOT_OK(split_coords_buffer());

  if (check_coord_oob_) {
    RETURN_NOT_OK(check_coord_oob());
  }

  RETURN_NOT_OK(ordered_write());

  return Status::Ok();
}

Status OrderedWriter::finalize() {
  auto timer_se = stats_->start_timer("finalize");

  return Status::Ok();
}

void OrderedWriter::reset() {
  initialized_ = false;
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

Status OrderedWriter::ordered_write() {
  // Applicable only to ordered write on dense arrays
  assert(layout_ == Layout::ROW_MAJOR || layout_ == Layout::COL_MAJOR);
  assert(array_schema_.dense());

  auto type{array_schema_.domain().dimension_ptr(0)->type()};
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
      return logger_->status(Status_WriterError(
          "Cannot write in ordered layout; Unsupported domain type"));
  }

  return Status::Ok();
}

template <class T>
Status OrderedWriter::ordered_write() {
  auto timer_se = stats_->start_timer("filter_tile");

  // Create new fragment
  auto frag_meta = make_shared<FragmentMetadata>(HERE());
  RETURN_CANCEL_OR_ERROR(create_fragment(true, frag_meta));
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
  std::unordered_map<std::string, std::vector<WriterTileVector>> tiles;
  for (const auto& buff : buffers_) {
    tiles.emplace(buff.first, std::vector<WriterTileVector>());
  }

  if (attr_num > tile_num) {  // Parallelize over attributes
    auto st = parallel_for(compute_tp, 0, attr_num, [&](uint64_t i) {
      auto buff_it = buffers_.begin();
      std::advance(buff_it, i);
      const auto& attr = buff_it->first;
      auto& attr_tile_batches = tiles[attr];
      return prepare_filter_and_write_tiles<T>(
          attr, attr_tile_batches, frag_meta, &dense_tiler, 1);
    });
    RETURN_NOT_OK_ELSE(st, storage_manager_->vfs()->remove_dir(uri));
  } else {  // Parallelize over tiles
    for (const auto& buff : buffers_) {
      const auto& attr = buff.first;
      auto& attr_tile_batches = tiles[attr];
      RETURN_NOT_OK_ELSE(
          prepare_filter_and_write_tiles<T>(
              attr, attr_tile_batches, frag_meta, &dense_tiler, thread_num),
          storage_manager_->vfs()->remove_dir(uri));
    }
  }

  // Fix the tile metadata for var size attributes.
  if (attr_num > tile_num) {  // Parallelize over attributes
    auto st = parallel_for(compute_tp, 0, attr_num, [&](uint64_t i) {
      auto buff_it = buffers_.begin();
      std::advance(buff_it, i);
      const auto& attr = buff_it->first;
      const auto var_size = array_schema_.var_size(attr);
      if (has_min_max_metadata(attr, var_size) &&
          array_schema_.var_size(attr)) {
        auto& attr_tile_batches = tiles[attr];
        frag_meta->convert_tile_min_max_var_sizes_to_offsets(attr);
        for (auto& batch : attr_tile_batches) {
          uint64_t idx = 0;
          for (auto& tile : batch) {
            frag_meta->set_tile_min_var(attr, idx, tile.offset_tile().min());
            frag_meta->set_tile_max_var(attr, idx, tile.offset_tile().max());
            idx++;
          }
        }
      }
      return Status::Ok();
    });
    RETURN_NOT_OK_ELSE(st, storage_manager_->vfs()->remove_dir(uri));
  } else {  // Parallelize over tiles
    for (const auto& buff : buffers_) {
      const auto& attr = buff.first;
      auto& attr_tile_batches = tiles[attr];
      const auto var_size = array_schema_.var_size(attr);
      if (has_min_max_metadata(attr, var_size) &&
          array_schema_.var_size(attr)) {
        frag_meta->convert_tile_min_max_var_sizes_to_offsets(attr);
        auto st = parallel_for(
            compute_tp, 0, attr_tile_batches.size(), [&](uint64_t b) {
              const auto& attr = buff.first;
              auto& batch = tiles[attr][b];
              for (uint64_t i = 0; i < batch.size(); i++) {
                frag_meta->set_tile_min_var(
                    attr, i, batch[i].offset_tile().min());
                frag_meta->set_tile_max_var(
                    attr, i, batch[i].offset_tile().max());
              }
              return Status::Ok();
            });
        RETURN_NOT_OK_ELSE(st, storage_manager_->vfs()->remove_dir(uri));
      }
    }
  }

  // Compute fragment min/max/sum/null count
  RETURN_NOT_OK_ELSE(
      frag_meta->compute_fragment_min_max_sum_null_count(),
      storage_manager_->vfs()->remove_dir(uri));

  // Write the fragment metadata
  RETURN_CANCEL_OR_ERROR_ELSE(
      frag_meta->store(array_->get_encryption_key()),
      storage_manager_->vfs()->remove_dir(uri));

  // Add written fragment info
  RETURN_NOT_OK_ELSE(
      add_written_fragment_info(uri), storage_manager_->vfs()->remove_dir(uri));

  // The following will make the fragment visible
  auto&& [st, commit_uri] = array_->array_directory().get_commit_uri(uri);
  RETURN_NOT_OK_ELSE(st, storage_manager_->vfs()->remove_dir(uri));
  RETURN_NOT_OK_ELSE(
      storage_manager_->vfs()->touch(commit_uri.value()),
      storage_manager_->vfs()->remove_dir(uri));

  return Status::Ok();
}

template <class T>
Status OrderedWriter::prepare_filter_and_write_tiles(
    const std::string& name,
    std::vector<WriterTileVector>& tile_batches,
    shared_ptr<FragmentMetadata> frag_meta,
    DenseTiler<T>* dense_tiler,
    uint64_t thread_num) {
  auto timer_se = stats_->start_timer("prepare_filter_and_write_tiles");

  // For easy reference
  const auto type = array_schema_.type(name);
  const auto is_dim = array_schema_.is_dim(name);
  const bool var = array_schema_.var_size(name);
  const auto cell_size = array_schema_.cell_size(name);
  const auto cell_val_num = array_schema_.cell_val_num(name);
  const bool nullable = array_schema_.is_nullable(name);

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
  tile_batches.reserve(batch_num);
  for (uint64_t b = 0; b < batch_num; ++b) {
    tile_batches.emplace_back(array_schema_, name);
    auto batch_size = (b == batch_num - 1) ? last_batch_size : thread_num;
    assert(batch_size > 0);
    tile_batches[b].resize(batch_size);
    auto st = parallel_for(
        storage_manager_->compute_tp(), 0, batch_size, [&](uint64_t i) {
          // Prepare and filter tiles
          auto writer_tile = tile_batches[b][i];
          TileMetadataGenerator md_generator(
              type, is_dim, var, cell_size, cell_val_num);

          auto tile_val = nullable ? &writer_tile.validity_tile() : nullptr;
          if (nullable) {
            RETURN_NOT_OK(
                dense_tiler->get_tile_null(frag_tile_id + i, name, tile_val));
          }

          if (!var) {
            auto tile = &writer_tile.fixed_tile();
            RETURN_NOT_OK(dense_tiler->get_tile(frag_tile_id + i, name, tile));
            md_generator.process_tile(tile, nullptr, tile_val);
            tile->set_metadata(md_generator.metadata());
            RETURN_NOT_OK(filter_tile(name, tile, nullptr, false, false));
          } else {
            auto tile = &writer_tile.offset_tile();
            auto tile_var = &writer_tile.var_tile();
            RETURN_NOT_OK(dense_tiler->get_tile_var(
                frag_tile_id + i, name, tile, tile_var));
            md_generator.process_tile(tile, tile_var, tile_val);
            tile->set_metadata(md_generator.metadata());
            RETURN_NOT_OK(filter_tile(name, tile_var, tile, false, false));
            RETURN_NOT_OK(filter_tile(name, tile, nullptr, true, false));
          }
          if (nullable) {
            RETURN_NOT_OK(filter_tile(name, tile_val, nullptr, false, true));
          }
          return Status::Ok();
        });
    RETURN_NOT_OK(st);

    // Write tiles
    close_files = (b == batch_num - 1);
    RETURN_NOT_OK(write_tiles(
        name, frag_meta, frag_tile_id, &tile_batches[b], close_files));

    frag_tile_id += batch_size;
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
