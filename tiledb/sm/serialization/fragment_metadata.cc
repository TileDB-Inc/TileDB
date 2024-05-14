/**
 * @file   fragment_metadata.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file declares serialization functions for Fragment Metadata.
 */

// clang-format off
#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include "tiledb/sm/serialization/capnp_utils.h"
#endif
// clang-format on

#include <string>

#include "tiledb/common/common.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/serialization/fragment_metadata.h"

using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {
namespace serialization {

#ifdef TILEDB_SERIALIZATION

/**
 * Creates a LoadedMetadata object for a freshly deserialized fragment metadata.
 */
static FragmentMetadata::LoadedMetadata create_deserialized_loaded_metadata(
    const uint64_t num_dims_and_attrs,
    const capnp::FragmentMetadata::Reader& reader) {
  // There is a difference in the metadata loaded for versions >= 2
  bool loadedValue = reader.getVersion() <= 2;
  FragmentMetadata::LoadedMetadata loaded_metadata;
  loaded_metadata.tile_offsets_.resize(num_dims_and_attrs);
  if (reader.hasTileOffsets()) {
    std::fill_n(
        loaded_metadata.tile_offsets_.begin(),
        reader.getTileOffsets().size(),
        loadedValue);
  }
  loaded_metadata.tile_var_offsets_.resize(num_dims_and_attrs);
  if (reader.hasTileVarOffsets()) {
    std::fill_n(
        loaded_metadata.tile_var_offsets_.begin(),
        reader.getTileVarOffsets().size(),
        loadedValue);
  }
  loaded_metadata.tile_var_sizes_.resize(num_dims_and_attrs);
  if (reader.hasTileVarSizes()) {
    std::fill_n(
        loaded_metadata.tile_var_sizes_.begin(),
        reader.getTileVarSizes().size(),
        loadedValue);
  }
  loaded_metadata.tile_validity_offsets_.resize(num_dims_and_attrs);
  if (reader.hasTileMinBuffer()) {
    loaded_metadata.tile_min_.resize(reader.getTileMinBuffer().size());
  }
  if (reader.hasTileMaxBuffer()) {
    loaded_metadata.tile_max_.resize(reader.getTileMaxBuffer().size());
  }
  if (reader.hasTileSums()) {
    loaded_metadata.tile_sum_.resize(reader.getTileMaxBuffer().size());
  }
  if (reader.hasTileNullCounts()) {
    loaded_metadata.tile_null_count_.resize(reader.getTileMaxBuffer().size());
  }
  loaded_metadata.footer_ = reader.hasGtOffsets();
  return loaded_metadata;
}

static FragmentMetadata::GenericTileOffsets generic_tile_offsets_from_capnp(
    const capnp::FragmentMetadata::GenericTileOffsets::Reader& gt_reader) {
  FragmentMetadata::GenericTileOffsets gt_offsets;
  gt_offsets.rtree_ = gt_reader.getRtree();
  if (gt_reader.hasTileOffsets()) {
    gt_offsets.tile_offsets_.reserve(gt_reader.getTileOffsets().size());
    for (const auto& tile_offset : gt_reader.getTileOffsets()) {
      gt_offsets.tile_offsets_.emplace_back(tile_offset);
    }
  }
  if (gt_reader.hasTileVarOffsets()) {
    gt_offsets.tile_var_offsets_.reserve(gt_reader.getTileVarOffsets().size());
    for (const auto& tile_var_offset : gt_reader.getTileVarOffsets()) {
      gt_offsets.tile_var_offsets_.emplace_back(tile_var_offset);
    }
  }
  if (gt_reader.hasTileVarSizes()) {
    gt_offsets.tile_var_sizes_.reserve(gt_reader.getTileVarSizes().size());
    for (const auto& tile_var_size : gt_reader.getTileVarSizes()) {
      gt_offsets.tile_var_sizes_.emplace_back(tile_var_size);
    }
  }
  if (gt_reader.hasTileValidityOffsets()) {
    gt_offsets.tile_validity_offsets_.reserve(
        gt_reader.getTileValidityOffsets().size());
    for (const auto& tile_validity_offset :
         gt_reader.getTileValidityOffsets()) {
      gt_offsets.tile_validity_offsets_.emplace_back(tile_validity_offset);
    }
  }
  if (gt_reader.hasTileMinOffsets()) {
    gt_offsets.tile_min_offsets_.reserve(gt_reader.getTileMinOffsets().size());
    for (const auto& tile_min_offset : gt_reader.getTileMinOffsets()) {
      gt_offsets.tile_min_offsets_.emplace_back(tile_min_offset);
    }
  }
  if (gt_reader.hasTileMaxOffsets()) {
    gt_offsets.tile_max_offsets_.reserve(gt_reader.getTileMaxOffsets().size());
    for (const auto& tile_max_offset : gt_reader.getTileMaxOffsets()) {
      gt_offsets.tile_max_offsets_.emplace_back(tile_max_offset);
    }
  }
  if (gt_reader.hasTileSumOffsets()) {
    gt_offsets.tile_sum_offsets_.reserve(gt_reader.getTileSumOffsets().size());
    for (const auto& tile_sum_offset : gt_reader.getTileSumOffsets()) {
      gt_offsets.tile_sum_offsets_.emplace_back(tile_sum_offset);
    }
  }
  if (gt_reader.hasTileNullCountOffsets()) {
    gt_offsets.tile_null_count_offsets_.reserve(
        gt_reader.getTileNullCountOffsets().size());
    for (const auto& tile_null_count_offset :
         gt_reader.getTileNullCountOffsets()) {
      gt_offsets.tile_null_count_offsets_.emplace_back(tile_null_count_offset);
    }
  }
  gt_offsets.fragment_min_max_sum_null_count_offset_ =
      gt_reader.getFragmentMinMaxSumNullCountOffset();
  gt_offsets.processed_conditions_offsets_ =
      gt_reader.getProcessedConditionsOffsets();

  return gt_offsets;
}

shared_ptr<FragmentMetadata> fragment_metadata_from_capnp(
    const shared_ptr<const ArraySchema>& array_schema,
    const capnp::FragmentMetadata::Reader& frag_meta_reader,
    ContextResources* resources,
    shared_ptr<MemoryTracker> memory_tracker) {
  URI fragment_uri;
  if (frag_meta_reader.hasFragmentUri()) {
    // Reconstruct the fragment uri out of the received fragment name
    fragment_uri = deserialize_array_uri_to_absolute(
        frag_meta_reader.getFragmentUri().cStr(), array_schema->array_uri());
  }
  uint64_t num_dims_and_attrs = FragmentMetadata::num_dims_and_attrs(
      *array_schema,
      frag_meta_reader.getHasTimestamps(),
      frag_meta_reader.getHasDeleteMeta());

  RTree rtree(&array_schema->domain(), constants::rtree_fanout, memory_tracker);
  if (frag_meta_reader.hasRtree()) {
    auto data = frag_meta_reader.getRtree();
    // If there are no levels, we still need domain_ properly initialized
    Deserializer deserializer(data.begin(), data.size());
    // What we actually deserialize is not something written on disk in a
    // possibly historical format, but what has been serialized in
    // `fragment_metadata_to_capnp` using
    // `frag_meta.rtree().serialize(serializer)`. This means that no matter what
    // the version of a fragment is on disk, we will be serializing _on wire_ in
    // fragment_metadata_to_capnp in the "modern" (post v5) way, so we need to
    // deserialize it as well in that way.
    rtree.deserialize(
        deserializer, &array_schema->domain(), constants::format_version);
  }

  return make_shared<FragmentMetadata>(
      HERE(),
      resources,
      memory_tracker,
      array_schema,
      utils::capnp_list_to_vector<uint64_t>(
          frag_meta_reader.hasFileSizes(),
          [&] { return frag_meta_reader.getFileSizes(); }),
      utils::capnp_list_to_vector<uint64_t>(
          frag_meta_reader.hasFileVarSizes(),
          [&] { return frag_meta_reader.getFileVarSizes(); }),
      utils::capnp_list_to_vector<uint64_t>(
          frag_meta_reader.hasFileValiditySizes(),
          [&] { return frag_meta_reader.getFileValiditySizes(); }),
      std::move(fragment_uri),
      frag_meta_reader.getHasTimestamps(),
      frag_meta_reader.getHasDeleteMeta(),
      frag_meta_reader.getHasConsolidatedFooter(),
      frag_meta_reader.getSparseTileNum(),
      frag_meta_reader.getTileIndexBase(),
      utils::capnp_2d_list_to_vector<uint64_t>(
          frag_meta_reader.hasTileOffsets(),
          [&] { return frag_meta_reader.getTileOffsets(); },
          memory_tracker->get_resource(MemoryType::TILE_OFFSETS),
          num_dims_and_attrs),
      utils::capnp_2d_list_to_vector<uint64_t>(
          frag_meta_reader.hasTileVarOffsets(),
          [&] { return frag_meta_reader.getTileVarOffsets(); },
          memory_tracker->get_resource(MemoryType::TILE_OFFSETS),
          num_dims_and_attrs),
      utils::capnp_2d_list_to_vector<uint64_t>(
          frag_meta_reader.hasTileVarSizes(),
          [&] { return frag_meta_reader.getTileVarSizes(); },
          memory_tracker->get_resource(MemoryType::TILE_OFFSETS),
          num_dims_and_attrs),
      utils::capnp_2d_list_to_vector<uint64_t>(
          frag_meta_reader.hasTileValidityOffsets(),
          [&] { return frag_meta_reader.getTileValidityOffsets(); },
          memory_tracker->get_resource(MemoryType::TILE_OFFSETS),
          num_dims_and_attrs),
      utils::capnp_2d_list_to_vector<uint8_t>(
          frag_meta_reader.hasTileMinBuffer(),
          [&] { return frag_meta_reader.getTileMinBuffer(); },
          memory_tracker->get_resource(MemoryType::TILE_MIN_VALS)),
      utils::capnp_2d_list_to_vector<uint8_t, char>(
          frag_meta_reader.hasTileMinVarBuffer(),
          [&] { return frag_meta_reader.getTileMinVarBuffer(); },
          memory_tracker->get_resource(MemoryType::TILE_MIN_VALS)),
      utils::capnp_2d_list_to_vector<uint8_t>(
          frag_meta_reader.hasTileMaxBuffer(),
          [&] { return frag_meta_reader.getTileMaxBuffer(); },
          memory_tracker->get_resource(MemoryType::TILE_MAX_VALS)),
      utils::capnp_2d_list_to_vector<uint8_t, char>(
          frag_meta_reader.hasTileMaxVarBuffer(),
          [&] { return frag_meta_reader.getTileMaxVarBuffer(); },
          memory_tracker->get_resource(MemoryType::TILE_MAX_VALS)),
      utils::capnp_2d_list_to_vector<uint8_t>(
          frag_meta_reader.hasTileSums(),
          [&] { return frag_meta_reader.getTileSums(); },
          memory_tracker->get_resource(MemoryType::TILE_SUMS)),
      utils::capnp_2d_list_to_vector<uint64_t>(
          frag_meta_reader.hasTileNullCounts(),
          [&] { return frag_meta_reader.getTileNullCounts(); },
          memory_tracker->get_resource(MemoryType::TILE_NULL_COUNTS)),
      utils::capnp_2d_list_to_vector<uint8_t>(
          frag_meta_reader.hasFragmentMins(),
          [&] { return frag_meta_reader.getFragmentMins(); },
          memory_tracker->get_resource(MemoryType::FRAGMENT_MINS)),
      utils::capnp_2d_list_to_vector<uint8_t>(
          frag_meta_reader.hasFragmentMaxs(),
          [&] { return frag_meta_reader.getFragmentMaxs(); },
          memory_tracker->get_resource(MemoryType::FRAGMENT_MAXS)),
      utils::capnp_list_to_vector<uint64_t>(
          frag_meta_reader.hasFragmentSums(),
          [&] { return frag_meta_reader.getFragmentSums(); }),
      utils::capnp_list_to_vector<uint64_t>(
          frag_meta_reader.hasFragmentNullCounts(),
          [&] { return frag_meta_reader.getFragmentNullCounts(); }),
      frag_meta_reader.getVersion(),
      std::make_pair(
          frag_meta_reader.getTimestampRange()[0],
          frag_meta_reader.getTimestampRange()[1]),
      frag_meta_reader.getLastTileCellNum(),
      utils::deserialize_non_empty_domain_rv(
          frag_meta_reader.getNonEmptyDomain()),
      std::move(rtree),
      generic_tile_offsets_from_capnp(frag_meta_reader.getGtOffsets()),
      create_deserialized_loaded_metadata(
          num_dims_and_attrs, frag_meta_reader));
}

void generic_tile_offsets_to_capnp(
    const FragmentMetadata::GenericTileOffsets& gt_offsets,
    capnp::FragmentMetadata::GenericTileOffsets::Builder& gt_offsets_builder) {
  gt_offsets_builder.setRtree(gt_offsets.rtree_);
  auto& gt_tile_offsets = gt_offsets.tile_offsets_;
  if (!gt_tile_offsets.empty()) {
    auto builder = gt_offsets_builder.initTileOffsets(gt_tile_offsets.size());
    for (uint64_t i = 0; i < gt_tile_offsets.size(); ++i) {
      builder.set(i, gt_tile_offsets[i]);
    }
  }
  auto& gt_tile_var_offsets = gt_offsets.tile_var_offsets_;
  if (!gt_tile_var_offsets.empty()) {
    auto builder =
        gt_offsets_builder.initTileVarOffsets(gt_tile_var_offsets.size());
    for (uint64_t i = 0; i < gt_tile_var_offsets.size(); ++i) {
      builder.set(i, gt_tile_var_offsets[i]);
    }
  }
  auto& gt_tile_var_sizes = gt_offsets.tile_var_sizes_;
  if (!gt_tile_var_sizes.empty()) {
    auto builder =
        gt_offsets_builder.initTileVarSizes(gt_tile_var_sizes.size());
    for (uint64_t i = 0; i < gt_tile_var_sizes.size(); ++i) {
      builder.set(i, gt_tile_var_sizes[i]);
    }
  }
  auto& gt_tile_validity_offsets = gt_offsets.tile_validity_offsets_;
  if (!gt_tile_validity_offsets.empty()) {
    auto builder = gt_offsets_builder.initTileValidityOffsets(
        gt_tile_validity_offsets.size());
    for (uint64_t i = 0; i < gt_tile_validity_offsets.size(); ++i) {
      builder.set(i, gt_tile_validity_offsets[i]);
    }
  }
  auto& gt_tile_min_offsets = gt_offsets.tile_min_offsets_;
  if (!gt_tile_min_offsets.empty()) {
    auto builder =
        gt_offsets_builder.initTileMinOffsets(gt_tile_min_offsets.size());
    for (uint64_t i = 0; i < gt_tile_min_offsets.size(); ++i) {
      builder.set(i, gt_tile_min_offsets[i]);
    }
  }
  auto& gt_tile_max_offsets = gt_offsets.tile_max_offsets_;
  if (!gt_tile_max_offsets.empty()) {
    auto builder =
        gt_offsets_builder.initTileMaxOffsets(gt_tile_max_offsets.size());
    for (uint64_t i = 0; i < gt_tile_max_offsets.size(); ++i) {
      builder.set(i, gt_tile_max_offsets[i]);
    }
  }
  auto& gt_tile_sum_offsets = gt_offsets.tile_sum_offsets_;
  if (!gt_tile_sum_offsets.empty()) {
    auto builder =
        gt_offsets_builder.initTileSumOffsets(gt_tile_sum_offsets.size());
    for (uint64_t i = 0; i < gt_tile_sum_offsets.size(); ++i) {
      builder.set(i, gt_tile_sum_offsets[i]);
    }
  }
  auto& gt_tile_null_count_offsets = gt_offsets.tile_null_count_offsets_;
  if (!gt_tile_null_count_offsets.empty()) {
    auto builder = gt_offsets_builder.initTileNullCountOffsets(
        gt_tile_null_count_offsets.size());
    for (uint64_t i = 0; i < gt_tile_null_count_offsets.size(); ++i) {
      builder.set(i, gt_tile_null_count_offsets[i]);
    }
  }
  gt_offsets_builder.setFragmentMinMaxSumNullCountOffset(
      gt_offsets.fragment_min_max_sum_null_count_offset_);
  gt_offsets_builder.setProcessedConditionsOffsets(
      gt_offsets.processed_conditions_offsets_);
}

void fragment_meta_sizes_offsets_to_capnp(
    const FragmentMetadata& frag_meta,
    capnp::FragmentMetadata::Builder* frag_meta_builder) {
  auto& tile_offsets = frag_meta.tile_offsets();
  if (!tile_offsets.empty()) {
    auto builder = frag_meta_builder->initTileOffsets(tile_offsets.size());
    for (uint64_t i = 0; i < tile_offsets.size(); ++i) {
      builder.init(i, tile_offsets[i].size());
      for (uint64_t j = 0; j < tile_offsets[i].size(); ++j) {
        builder[i].set(j, tile_offsets[i][j]);
      }
    }
  }
  auto& tile_var_offsets = frag_meta.tile_var_offsets();
  if (!tile_var_offsets.empty()) {
    auto builder =
        frag_meta_builder->initTileVarOffsets(tile_var_offsets.size());
    for (uint64_t i = 0; i < tile_var_offsets.size(); ++i) {
      builder.init(i, tile_var_offsets[i].size());
      for (uint64_t j = 0; j < tile_var_offsets[i].size(); ++j) {
        builder[i].set(j, tile_var_offsets[i][j]);
      }
    }
  }
  auto& tile_var_sizes = frag_meta.tile_var_sizes();
  if (!tile_var_sizes.empty()) {
    auto builder = frag_meta_builder->initTileVarSizes(tile_var_sizes.size());
    for (uint64_t i = 0; i < tile_var_sizes.size(); ++i) {
      builder.init(i, tile_var_sizes[i].size());
      for (uint64_t j = 0; j < tile_var_sizes[i].size(); ++j) {
        builder[i].set(j, tile_var_sizes[i][j]);
      }
    }
  }
  auto& tile_validity_offsets = frag_meta.tile_validity_offsets();
  if (!tile_validity_offsets.empty()) {
    auto builder = frag_meta_builder->initTileValidityOffsets(
        tile_validity_offsets.size());
    for (uint64_t i = 0; i < tile_validity_offsets.size(); ++i) {
      builder.init(i, tile_validity_offsets[i].size());
      for (uint64_t j = 0; j < tile_validity_offsets[i].size(); ++j) {
        builder[i].set(j, tile_validity_offsets[i][j]);
      }
    }
  }
}

Status fragment_metadata_to_capnp(
    const FragmentMetadata& frag_meta,
    capnp::FragmentMetadata::Builder* frag_meta_builder) {
  const auto& relative_fragment_uri =
      serialize_array_uri_to_relative(frag_meta.fragment_uri());
  frag_meta_builder->setFragmentUri(relative_fragment_uri);
  frag_meta_builder->setHasTimestamps(frag_meta.has_timestamps());
  frag_meta_builder->setHasDeleteMeta(frag_meta.has_delete_meta());
  frag_meta_builder->setHasConsolidatedFooter(
      frag_meta.has_consolidated_footer());
  frag_meta_builder->setSparseTileNum(frag_meta.sparse_tile_num());
  frag_meta_builder->setTileIndexBase(frag_meta.tile_index_base());

  auto& file_sizes = frag_meta.file_sizes();
  if (!file_sizes.empty()) {
    auto builder = frag_meta_builder->initFileSizes(file_sizes.size());
    for (uint64_t i = 0; i < file_sizes.size(); ++i) {
      builder.set(i, file_sizes[i]);
    }
  }
  auto& file_var_sizes = frag_meta.file_var_sizes();
  if (!file_var_sizes.empty()) {
    auto builder = frag_meta_builder->initFileVarSizes(file_var_sizes.size());
    for (uint64_t i = 0; i < file_var_sizes.size(); ++i) {
      builder.set(i, file_var_sizes[i]);
    }
  }
  auto& file_validity_sizes = frag_meta.file_validity_sizes();
  if (!file_validity_sizes.empty()) {
    auto builder =
        frag_meta_builder->initFileValiditySizes(file_validity_sizes.size());
    for (uint64_t i = 0; i < file_validity_sizes.size(); ++i) {
      builder.set(i, file_validity_sizes[i]);
    }
  }

  auto& tile_min_buffer = frag_meta.tile_min_buffer();
  if (!tile_min_buffer.empty()) {
    auto builder = frag_meta_builder->initTileMinBuffer(tile_min_buffer.size());
    for (uint64_t i = 0; i < tile_min_buffer.size(); ++i) {
      builder.init(i, tile_min_buffer[i].size());
      for (uint64_t j = 0; j < tile_min_buffer[i].size(); ++j) {
        builder[i].set(j, tile_min_buffer[i][j]);
      }
    }
  }
  auto& tile_min_var_buffer = frag_meta.tile_min_var_buffer();
  if (!tile_min_var_buffer.empty()) {
    auto builder =
        frag_meta_builder->initTileMinVarBuffer(tile_min_var_buffer.size());
    for (uint64_t i = 0; i < tile_min_var_buffer.size(); ++i) {
      builder.init(i, tile_min_var_buffer[i].size());
      for (uint64_t j = 0; j < tile_min_var_buffer[i].size(); ++j) {
        builder[i].set(j, tile_min_var_buffer[i][j]);
      }
    }
  }
  auto& tile_max_buffer = frag_meta.tile_max_buffer();
  if (!tile_max_buffer.empty()) {
    auto builder = frag_meta_builder->initTileMaxBuffer(tile_max_buffer.size());
    for (uint64_t i = 0; i < tile_max_buffer.size(); ++i) {
      builder.init(i, tile_max_buffer[i].size());
      for (uint64_t j = 0; j < tile_max_buffer[i].size(); ++j) {
        builder[i].set(j, tile_max_buffer[i][j]);
      }
    }
  }
  auto& tile_max_var_buffer = frag_meta.tile_max_var_buffer();
  if (!tile_max_var_buffer.empty()) {
    auto builder =
        frag_meta_builder->initTileMaxVarBuffer(tile_max_var_buffer.size());
    for (uint64_t i = 0; i < tile_max_var_buffer.size(); ++i) {
      builder.init(i, tile_max_var_buffer[i].size());
      for (uint64_t j = 0; j < tile_max_var_buffer[i].size(); ++j) {
        builder[i].set(j, tile_max_var_buffer[i][j]);
      }
    }
  }
  auto& tile_sums = frag_meta.tile_sums();
  if (!tile_sums.empty()) {
    auto builder = frag_meta_builder->initTileSums(tile_sums.size());
    for (uint64_t i = 0; i < tile_sums.size(); ++i) {
      builder.init(i, tile_sums[i].size());
      for (uint64_t j = 0; j < tile_sums[i].size(); ++j) {
        builder[i].set(j, tile_sums[i][j]);
      }
    }
  }
  auto& tile_null_counts = frag_meta.tile_null_counts();
  if (!tile_null_counts.empty()) {
    auto builder =
        frag_meta_builder->initTileNullCounts(tile_null_counts.size());
    for (uint64_t i = 0; i < tile_null_counts.size(); ++i) {
      builder.init(i, tile_null_counts[i].size());
      for (uint64_t j = 0; j < tile_null_counts[i].size(); ++j) {
        builder[i].set(j, tile_null_counts[i][j]);
      }
    }
  }
  auto& fragment_mins = frag_meta.fragment_mins();
  if (!fragment_mins.empty()) {
    auto builder = frag_meta_builder->initFragmentMins(fragment_mins.size());
    for (uint64_t i = 0; i < fragment_mins.size(); ++i) {
      builder.init(i, fragment_mins[i].size());
      for (uint64_t j = 0; j < fragment_mins[i].size(); ++j) {
        builder[i].set(j, fragment_mins[i][j]);
      }
    }
  }
  auto& fragment_maxs = frag_meta.fragment_maxs();
  if (!fragment_maxs.empty()) {
    auto builder = frag_meta_builder->initFragmentMaxs(fragment_maxs.size());
    for (uint64_t i = 0; i < fragment_maxs.size(); ++i) {
      builder.init(i, fragment_maxs[i].size());
      for (uint64_t j = 0; j < fragment_maxs[i].size(); ++j) {
        builder[i].set(j, fragment_maxs[i][j]);
      }
    }
  }
  auto& fragment_sums = frag_meta.fragment_sums();
  if (!fragment_sums.empty()) {
    auto builder = frag_meta_builder->initFragmentSums(fragment_sums.size());
    for (uint64_t i = 0; i < fragment_sums.size(); ++i) {
      builder.set(i, fragment_sums[i]);
    }
  }
  auto& fragment_null_counts = frag_meta.fragment_null_counts();
  if (!fragment_null_counts.empty()) {
    auto builder =
        frag_meta_builder->initFragmentNullCounts(fragment_null_counts.size());
    for (uint64_t i = 0; i < fragment_null_counts.size(); ++i) {
      builder.set(i, fragment_null_counts[i]);
    }
  }

  frag_meta_builder->setVersion(frag_meta.format_version());

  auto trange_builder = frag_meta_builder->initTimestampRange(2);
  trange_builder.set(0, frag_meta.timestamp_range().first);
  trange_builder.set(1, frag_meta.timestamp_range().second);

  frag_meta_builder->setLastTileCellNum(frag_meta.last_tile_cell_num());

  auto ned_builder = frag_meta_builder->initNonEmptyDomain();
  utils::serialize_non_empty_domain_rv(
      ned_builder,
      frag_meta.non_empty_domain(),
      frag_meta.array_schema()->dim_num());

  // TODO: Can this be done better? Does this make a lot of copies?
  SizeComputationSerializer size_computation_serializer;
  frag_meta.rtree().serialize(size_computation_serializer);

  std::vector<uint8_t> buff(size_computation_serializer.size());
  Serializer serializer(buff.data(), buff.size());
  frag_meta.rtree().serialize(serializer);

  auto vec = kj::Vector<uint8_t>();
  vec.addAll(
      kj::ArrayPtr<uint8_t>(static_cast<uint8_t*>(buff.data()), buff.size()));
  frag_meta_builder->setRtree(vec.asPtr());

  auto gt_offsets_builder = frag_meta_builder->initGtOffsets();
  generic_tile_offsets_to_capnp(
      frag_meta.generic_tile_offsets(), gt_offsets_builder);

  return Status::Ok();
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
