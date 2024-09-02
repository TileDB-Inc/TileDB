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
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/serialization/fragment_metadata.h"

using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {
namespace serialization {

#ifdef TILEDB_SERIALIZATION

void generic_tile_offsets_from_capnp(
    const capnp::FragmentMetadata::GenericTileOffsets::Reader& gt_reader,
    FragmentMetadata::GenericTileOffsets& gt_offsets) {
  gt_offsets.rtree_ = gt_reader.getRtree();
  if (gt_reader.hasTileOffsets()) {
    auto tile_offsets = gt_reader.getTileOffsets();
    gt_offsets.tile_offsets_.reserve(tile_offsets.size());
    for (const auto& tile_offset : tile_offsets) {
      gt_offsets.tile_offsets_.emplace_back(tile_offset);
    }
  }
  if (gt_reader.hasTileVarOffsets()) {
    auto tile_var_offsets = gt_reader.getTileVarOffsets();
    gt_offsets.tile_var_offsets_.reserve(tile_var_offsets.size());
    for (const auto& tile_var_offset : tile_var_offsets) {
      gt_offsets.tile_var_offsets_.emplace_back(tile_var_offset);
    }
  }
  if (gt_reader.hasTileVarSizes()) {
    auto tile_var_sizes = gt_reader.getTileVarSizes();
    gt_offsets.tile_var_sizes_.reserve(tile_var_sizes.size());
    for (const auto& tile_var_size : tile_var_sizes) {
      gt_offsets.tile_var_sizes_.emplace_back(tile_var_size);
    }
  }
  if (gt_reader.hasTileValidityOffsets()) {
    auto tile_validity_offsets = gt_reader.getTileValidityOffsets();
    gt_offsets.tile_validity_offsets_.reserve(tile_validity_offsets.size());
    for (const auto& tile_validity_offset : tile_validity_offsets) {
      gt_offsets.tile_validity_offsets_.emplace_back(tile_validity_offset);
    }
  }
  if (gt_reader.hasTileMinOffsets()) {
    auto tile_min_offsets = gt_reader.getTileMinOffsets();
    gt_offsets.tile_min_offsets_.reserve(tile_min_offsets.size());
    for (const auto& tile_min_offset : tile_min_offsets) {
      gt_offsets.tile_min_offsets_.emplace_back(tile_min_offset);
    }
  }
  if (gt_reader.hasTileMaxOffsets()) {
    auto tile_max_offsets = gt_reader.getTileMaxOffsets();
    gt_offsets.tile_max_offsets_.reserve(tile_max_offsets.size());
    for (const auto& tile_max_offset : tile_max_offsets) {
      gt_offsets.tile_max_offsets_.emplace_back(tile_max_offset);
    }
  }
  if (gt_reader.hasTileSumOffsets()) {
    auto tile_sum_offsets = gt_reader.getTileSumOffsets();
    gt_offsets.tile_sum_offsets_.reserve(tile_sum_offsets.size());
    for (const auto& tile_sum_offset : tile_sum_offsets) {
      gt_offsets.tile_sum_offsets_.emplace_back(tile_sum_offset);
    }
  }
  if (gt_reader.hasTileNullCountOffsets()) {
    auto tile_null_count_offsets = gt_reader.getTileNullCountOffsets();
    gt_offsets.tile_null_count_offsets_.reserve(tile_null_count_offsets.size());
    for (const auto& tile_null_count_offset : tile_null_count_offsets) {
      gt_offsets.tile_null_count_offsets_.emplace_back(tile_null_count_offset);
    }
  }
  gt_offsets.fragment_min_max_sum_null_count_offset_ =
      gt_reader.getFragmentMinMaxSumNullCountOffset();
  gt_offsets.processed_conditions_offsets_ =
      gt_reader.getProcessedConditionsOffsets();
}

Status fragment_metadata_from_capnp(
    const shared_ptr<const ArraySchema>& fragment_array_schema,
    const capnp::FragmentMetadata::Reader& frag_meta_reader,
    shared_ptr<FragmentMetadata> frag_meta) {
  if (frag_meta_reader.hasFileSizes()) {
    auto filesizes_reader = frag_meta_reader.getFileSizes();
    frag_meta->file_sizes().reserve(filesizes_reader.size());
    for (const auto& file_size : filesizes_reader) {
      frag_meta->file_sizes().emplace_back(file_size);
    }
  }
  if (frag_meta_reader.hasFileVarSizes()) {
    auto filevarsizes_reader = frag_meta_reader.getFileVarSizes();
    frag_meta->file_var_sizes().reserve(filevarsizes_reader.size());
    for (const auto& file_var_size : filevarsizes_reader) {
      frag_meta->file_var_sizes().emplace_back(file_var_size);
    }
  }
  if (frag_meta_reader.hasFileValiditySizes()) {
    auto filevaliditysizes_reader = frag_meta_reader.getFileValiditySizes();
    frag_meta->file_validity_sizes().reserve(filevaliditysizes_reader.size());

    for (const auto& file_validity_size : filevaliditysizes_reader) {
      frag_meta->file_validity_sizes().emplace_back(file_validity_size);
    }
  }
  if (frag_meta_reader.hasFragmentUri()) {
    // Reconstruct the fragment uri out of the received fragment name
    frag_meta->fragment_uri() = deserialize_array_uri_to_absolute(
        frag_meta_reader.getFragmentUri().cStr(),
        fragment_array_schema->array_uri());
  }
  frag_meta->has_timestamps() = frag_meta_reader.getHasTimestamps();
  frag_meta->has_delete_meta() = frag_meta_reader.getHasDeleteMeta();
  frag_meta->has_consolidated_footer() =
      frag_meta_reader.getHasConsolidatedFooter();
  frag_meta->sparse_tile_num() = frag_meta_reader.getSparseTileNum();
  frag_meta->tile_index_base() = frag_meta_reader.getTileIndexBase();
  frag_meta->version() = frag_meta_reader.getVersion();

  // Set the array schema and most importantly retrigger the build
  // of the internal idx_map.
  frag_meta->set_array_schema(fragment_array_schema);
  frag_meta->set_dense(fragment_array_schema->dense());

  if (frag_meta_reader.hasArraySchemaName()) {
    frag_meta->set_schema_name(frag_meta_reader.getArraySchemaName().cStr());
  }

  LoadedFragmentMetadata::LoadedMetadata loaded_metadata;

  // num_dims_and_attrs() requires a set array schema, so it's important
  // schema is set above on the fragment metadata object.
  uint64_t num_dims_and_attrs = frag_meta->num_dims_and_attrs();

  // The tile offsets field may not be present here in some usecases such as
  // refactored query, but readers on the server side require these vectors to
  // have the first dimension properly allocated when loading their data on
  // demand.
  frag_meta->loaded_metadata()->resize_tile_offsets_vectors(num_dims_and_attrs);
  loaded_metadata.tile_offsets_.resize(num_dims_and_attrs, false);

  // There is a difference in the metadata loaded for versions >= 2
  auto loaded = frag_meta->version() <= 2 ? true : false;

  if (frag_meta_reader.hasTileOffsets()) {
    auto tileoffsets_reader = frag_meta_reader.getTileOffsets();
    uint64_t i = 0;
    for (const auto& t : tileoffsets_reader) {
      auto& last = frag_meta->loaded_metadata()->tile_offsets()[i];
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
      loaded_metadata.tile_offsets_[i++] = loaded;
    }
  }

  // The tile var offsets field may not be present here in some usecases such as
  // refactored query, but readers on the server side require these vectors to
  // have the first dimension properly allocated when loading its data on
  // demand.
  frag_meta->loaded_metadata()->resize_tile_var_offsets_vectors(
      num_dims_and_attrs);
  loaded_metadata.tile_var_offsets_.resize(num_dims_and_attrs, false);
  if (frag_meta_reader.hasTileVarOffsets()) {
    auto tilevaroffsets_reader = frag_meta_reader.getTileVarOffsets();
    uint64_t i = 0;
    for (const auto& t : tilevaroffsets_reader) {
      auto& last = frag_meta->loaded_metadata()->tile_var_offsets()[i];
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
      loaded_metadata.tile_var_offsets_[i++] = loaded;
    }
  }

  // The tile var sizes field may not be present here in some usecases such as
  // refactored query, but readers on the server side require these vectors to
  // have the first dimension properly allocated when loading its data on
  // demand.
  frag_meta->loaded_metadata()->resize_tile_var_sizes_vectors(
      num_dims_and_attrs);
  loaded_metadata.tile_var_sizes_.resize(num_dims_and_attrs, false);
  if (frag_meta_reader.hasTileVarSizes()) {
    auto tilevarsizes_reader = frag_meta_reader.getTileVarSizes();
    uint64_t i = 0;
    for (const auto& t : tilevarsizes_reader) {
      auto& last = frag_meta->loaded_metadata()->tile_var_sizes()[i];
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
      loaded_metadata.tile_var_sizes_[i++] = loaded;
    }
  }

  // This field may not be present here in some usecases such as refactored
  // query, but readers on the server side require this vector to have the first
  // dimension properly allocated when loading its data on demand.
  frag_meta->loaded_metadata()->resize_tile_validity_offsets_vectors(
      num_dims_and_attrs);
  loaded_metadata.tile_validity_offsets_.resize(num_dims_and_attrs, false);
  if (frag_meta_reader.hasTileValidityOffsets()) {
    auto tilevalidityoffsets_reader = frag_meta_reader.getTileValidityOffsets();
    uint64_t i = 0;
    for (const auto& t : tilevalidityoffsets_reader) {
      auto& last = frag_meta->loaded_metadata()->tile_validity_offsets()[i];
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
      loaded_metadata.tile_validity_offsets_[i++] = false;
    }
  }
  if (frag_meta_reader.hasTileMinBuffer()) {
    auto tileminbuffer_reader = frag_meta_reader.getTileMinBuffer();
    for (const auto& t : tileminbuffer_reader) {
      auto& last =
          frag_meta->loaded_metadata()->tile_min_buffer().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
    loaded_metadata.tile_min_.resize(tileminbuffer_reader.size(), false);
  }
  if (frag_meta_reader.hasTileMinVarBuffer()) {
    auto tileminvarbuffer_reader = frag_meta_reader.getTileMinVarBuffer();
    for (const auto& t : tileminvarbuffer_reader) {
      auto& last =
          frag_meta->loaded_metadata()->tile_min_var_buffer().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
  }
  if (frag_meta_reader.hasTileMaxBuffer()) {
    auto tilemaxbuffer_reader = frag_meta_reader.getTileMaxBuffer();
    for (const auto& t : tilemaxbuffer_reader) {
      auto& last =
          frag_meta->loaded_metadata()->tile_max_buffer().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
    loaded_metadata.tile_max_.resize(tilemaxbuffer_reader.size(), false);
  }
  if (frag_meta_reader.hasTileMaxVarBuffer()) {
    auto tilemaxvarbuffer_reader = frag_meta_reader.getTileMaxVarBuffer();
    for (const auto& t : tilemaxvarbuffer_reader) {
      auto& last =
          frag_meta->loaded_metadata()->tile_max_var_buffer().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
  }
  if (frag_meta_reader.hasTileSums()) {
    auto tilesums_reader = frag_meta_reader.getTileSums();
    for (const auto& t : tilesums_reader) {
      auto& last = frag_meta->loaded_metadata()->tile_sums().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
    loaded_metadata.tile_sum_.resize(tilesums_reader.size(), false);
  }
  if (frag_meta_reader.hasTileNullCounts()) {
    auto tilenullcounts_reader = frag_meta_reader.getTileNullCounts();
    for (const auto& t : tilenullcounts_reader) {
      auto& last =
          frag_meta->loaded_metadata()->tile_null_counts().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
    loaded_metadata.tile_null_count_.resize(
        tilenullcounts_reader.size(), false);
  }
  if (frag_meta_reader.hasFragmentMins()) {
    auto fragmentmins_reader = frag_meta_reader.getFragmentMins();
    for (const auto& t : fragmentmins_reader) {
      auto& last = frag_meta->loaded_metadata()->fragment_mins().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
  }
  if (frag_meta_reader.hasFragmentMaxs()) {
    auto fragmentmaxs_reader = frag_meta_reader.getFragmentMaxs();
    for (const auto& t : fragmentmaxs_reader) {
      auto& last = frag_meta->loaded_metadata()->fragment_maxs().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
  }
  if (frag_meta_reader.hasFragmentSums()) {
    auto fragmentsums_reader = frag_meta_reader.getFragmentSums();
    frag_meta->loaded_metadata()->fragment_sums().reserve(
        fragmentsums_reader.size());
    for (const auto& fragment_sum : fragmentsums_reader) {
      frag_meta->loaded_metadata()->fragment_sums().emplace_back(fragment_sum);
    }
  }
  if (frag_meta_reader.hasFragmentNullCounts()) {
    auto fragmentnullcounts_reader = frag_meta_reader.getFragmentNullCounts();
    frag_meta->loaded_metadata()->fragment_null_counts().reserve(
        fragmentnullcounts_reader.size());
    for (const auto& fragment_null_count : fragmentnullcounts_reader) {
      frag_meta->loaded_metadata()->fragment_null_counts().emplace_back(
          fragment_null_count);
    }
  }

  frag_meta->version() = frag_meta_reader.getVersion();
  if (frag_meta_reader.hasTimestampRange()) {
    auto timestamp_range = frag_meta_reader.getTimestampRange();
    frag_meta->timestamp_range() =
        std::make_pair(timestamp_range[0], timestamp_range[1]);
  }
  frag_meta->last_tile_cell_num() = frag_meta_reader.getLastTileCellNum();

  if (frag_meta_reader.hasRtree()) {
    auto data = frag_meta_reader.getRtree();
    auto& domain = fragment_array_schema->domain();
    // If there are no levels, we still need domain_ properly initialized
    frag_meta->loaded_metadata()->rtree().reset(
        &domain, constants::rtree_fanout);
    Deserializer deserializer(data.begin(), data.size());
    // What we actually deserialize is not something written on disk in a
    // possibly historical format, but what has been serialized in
    // `fragment_metadata_to_capnp` using
    // `frag_meta.rtree().serialize(serializer)`. This means that no matter what
    // the version of a fragment is on disk, we will be serializing _on wire_ in
    // fragment_metadata_to_capnp in the "modern" (post v5) way, so we need to
    // deserialize it as well in that way.
    frag_meta->loaded_metadata()->rtree().deserialize(
        deserializer, &domain, constants::format_version);
  }

  // It's important to do this here as init_domain depends on some fields
  // above to be properly initialized
  if (frag_meta_reader.hasNonEmptyDomain()) {
    auto reader = frag_meta_reader.getNonEmptyDomain();
    auto&& [status, ndrange] = utils::deserialize_non_empty_domain_rv(reader);
    RETURN_NOT_OK(status);
    // Whilst sparse gets its domain calculated, dense needs to have it
    // set here from the deserialized data
    if (fragment_array_schema->dense()) {
      frag_meta->init_domain(*ndrange);
    } else {
      const auto& frag0_dom = *ndrange;
      frag_meta->non_empty_domain().assign(frag0_dom.begin(), frag0_dom.end());
    }
  }

  if (frag_meta_reader.hasGtOffsets()) {
    generic_tile_offsets_from_capnp(
        frag_meta_reader.getGtOffsets(), frag_meta->generic_tile_offsets());
  }

  frag_meta->loaded_metadata()->set_loaded_metadata(loaded_metadata);

  return Status::Ok();
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
  auto& tile_offsets = frag_meta.loaded_metadata()->tile_offsets();
  if (!tile_offsets.empty()) {
    auto builder = frag_meta_builder->initTileOffsets(tile_offsets.size());
    for (uint64_t i = 0; i < tile_offsets.size(); ++i) {
      builder.init(i, tile_offsets[i].size());
      for (uint64_t j = 0; j < tile_offsets[i].size(); ++j) {
        builder[i].set(j, tile_offsets[i][j]);
      }
    }
  }
  auto& tile_var_offsets = frag_meta.loaded_metadata()->tile_var_offsets();
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
  auto& tile_var_sizes = frag_meta.loaded_metadata()->tile_var_sizes();
  if (!tile_var_sizes.empty()) {
    auto builder = frag_meta_builder->initTileVarSizes(tile_var_sizes.size());
    for (uint64_t i = 0; i < tile_var_sizes.size(); ++i) {
      builder.init(i, tile_var_sizes[i].size());
      for (uint64_t j = 0; j < tile_var_sizes[i].size(); ++j) {
        builder[i].set(j, tile_var_sizes[i][j]);
      }
    }
  }
  auto& tile_validity_offsets =
      frag_meta.loaded_metadata()->tile_validity_offsets();
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

  auto& tile_min_buffer = frag_meta.loaded_metadata()->tile_min_buffer();
  if (!tile_min_buffer.empty()) {
    auto builder = frag_meta_builder->initTileMinBuffer(tile_min_buffer.size());
    for (uint64_t i = 0; i < tile_min_buffer.size(); ++i) {
      builder.init(i, tile_min_buffer[i].size());
      for (uint64_t j = 0; j < tile_min_buffer[i].size(); ++j) {
        builder[i].set(j, tile_min_buffer[i][j]);
      }
    }
  }
  auto& tile_min_var_buffer =
      frag_meta.loaded_metadata()->tile_min_var_buffer();
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
  auto& tile_max_buffer = frag_meta.loaded_metadata()->tile_max_buffer();
  if (!tile_max_buffer.empty()) {
    auto builder = frag_meta_builder->initTileMaxBuffer(tile_max_buffer.size());
    for (uint64_t i = 0; i < tile_max_buffer.size(); ++i) {
      builder.init(i, tile_max_buffer[i].size());
      for (uint64_t j = 0; j < tile_max_buffer[i].size(); ++j) {
        builder[i].set(j, tile_max_buffer[i][j]);
      }
    }
  }
  auto& tile_max_var_buffer =
      frag_meta.loaded_metadata()->tile_max_var_buffer();
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
  auto& tile_sums = frag_meta.loaded_metadata()->tile_sums();
  if (!tile_sums.empty()) {
    auto builder = frag_meta_builder->initTileSums(tile_sums.size());
    for (uint64_t i = 0; i < tile_sums.size(); ++i) {
      builder.init(i, tile_sums[i].size());
      for (uint64_t j = 0; j < tile_sums[i].size(); ++j) {
        builder[i].set(j, tile_sums[i][j]);
      }
    }
  }
  auto& tile_null_counts = frag_meta.loaded_metadata()->tile_null_counts();
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
  auto& fragment_mins = frag_meta.loaded_metadata()->fragment_mins();
  if (!fragment_mins.empty()) {
    auto builder = frag_meta_builder->initFragmentMins(fragment_mins.size());
    for (uint64_t i = 0; i < fragment_mins.size(); ++i) {
      builder.init(i, fragment_mins[i].size());
      for (uint64_t j = 0; j < fragment_mins[i].size(); ++j) {
        builder[i].set(j, fragment_mins[i][j]);
      }
    }
  }
  auto& fragment_maxs = frag_meta.loaded_metadata()->fragment_maxs();
  if (!fragment_maxs.empty()) {
    auto builder = frag_meta_builder->initFragmentMaxs(fragment_maxs.size());
    for (uint64_t i = 0; i < fragment_maxs.size(); ++i) {
      builder.init(i, fragment_maxs[i].size());
      for (uint64_t j = 0; j < fragment_maxs[i].size(); ++j) {
        builder[i].set(j, fragment_maxs[i][j]);
      }
    }
  }
  auto& fragment_sums = frag_meta.loaded_metadata()->fragment_sums();
  if (!fragment_sums.empty()) {
    auto builder = frag_meta_builder->initFragmentSums(fragment_sums.size());
    for (uint64_t i = 0; i < fragment_sums.size(); ++i) {
      builder.set(i, fragment_sums[i]);
    }
  }
  auto& fragment_null_counts =
      frag_meta.loaded_metadata()->fragment_null_counts();
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
  RETURN_NOT_OK(utils::serialize_non_empty_domain_rv(
      ned_builder,
      frag_meta.non_empty_domain(),
      frag_meta.array_schema()->dim_num()));

  // TODO: Can this be done better? Does this make a lot of copies?
  SizeComputationSerializer size_computation_serializer;
  frag_meta.loaded_metadata()->rtree().serialize(size_computation_serializer);

  std::vector<uint8_t> buff(size_computation_serializer.size());
  Serializer serializer(buff.data(), buff.size());
  frag_meta.loaded_metadata()->rtree().serialize(serializer);

  auto vec = kj::Vector<uint8_t>();
  vec.addAll(
      kj::ArrayPtr<uint8_t>(static_cast<uint8_t*>(buff.data()), buff.size()));
  frag_meta_builder->setRtree(vec.asPtr());

  auto gt_offsets_builder = frag_meta_builder->initGtOffsets();
  generic_tile_offsets_to_capnp(
      frag_meta.generic_tile_offsets(), gt_offsets_builder);

  frag_meta_builder->setArraySchemaName(frag_meta.array_schema_name());

  return Status::Ok();
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
