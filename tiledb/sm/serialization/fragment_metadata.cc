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

Status fragment_metadata_from_capnp(
    const shared_ptr<const ArraySchema>& array_schema,
    const capnp::FragmentMetadata::Reader& frag_meta_reader,
    shared_ptr<FragmentMetadata> frag_meta) {
  if (frag_meta_reader.hasFileSizes()) {
    frag_meta->file_sizes().reserve(frag_meta_reader.getFileSizes().size());
    for (const auto& file_size : frag_meta_reader.getFileSizes()) {
      frag_meta->file_sizes().emplace_back(file_size);
    }
  }
  if (frag_meta_reader.hasFileVarSizes()) {
    frag_meta->file_var_sizes().reserve(
        frag_meta_reader.getFileVarSizes().size());
    for (const auto& file_var_size : frag_meta_reader.getFileVarSizes()) {
      frag_meta->file_var_sizes().emplace_back(file_var_size);
    }
  }
  if (frag_meta_reader.hasFileValiditySizes()) {
    frag_meta->file_validity_sizes().reserve(
        frag_meta_reader.getFileValiditySizes().size());

    for (const auto& file_validity_size :
         frag_meta_reader.getFileValiditySizes()) {
      frag_meta->file_validity_sizes().emplace_back(file_validity_size);
    }
  }
  if (frag_meta_reader.hasFragmentUri()) {
    frag_meta->fragment_uri() = URI(frag_meta_reader.getFragmentUri().cStr());
  }
  frag_meta->has_timestamps() = frag_meta_reader.getHasTimestamps();
  frag_meta->has_delete_meta() = frag_meta_reader.getHasDeleteMeta();
  frag_meta->has_consolidated_footer() =
      frag_meta_reader.getHasConsolidatedFooter();
  frag_meta->sparse_tile_num() = frag_meta_reader.getSparseTileNum();
  frag_meta->tile_index_base() = frag_meta_reader.getTileIndexBase();
  if (frag_meta_reader.hasTileOffsets()) {
    for (const auto& t : frag_meta_reader.getTileOffsets()) {
      auto& last = frag_meta->tile_offsets().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
  }
  if (frag_meta_reader.hasTileVarOffsets()) {
    for (const auto& t : frag_meta_reader.getTileVarOffsets()) {
      auto& last = frag_meta->tile_var_offsets().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
  }
  if (frag_meta_reader.hasTileVarSizes()) {
    for (const auto& t : frag_meta_reader.getTileVarSizes()) {
      auto& last = frag_meta->tile_var_sizes().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
  }
  if (frag_meta_reader.hasTileValidityOffsets()) {
    for (const auto& t : frag_meta_reader.getTileValidityOffsets()) {
      auto& last = frag_meta->tile_validity_offsets().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
  }
  if (frag_meta_reader.hasTileMinBuffer()) {
    for (const auto& t : frag_meta_reader.getTileMinBuffer()) {
      auto& last = frag_meta->tile_min_buffer().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
  }
  if (frag_meta_reader.hasTileMinVarBuffer()) {
    for (const auto& t : frag_meta_reader.getTileMinVarBuffer()) {
      auto& last = frag_meta->tile_min_var_buffer().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
  }
  if (frag_meta_reader.hasTileMaxBuffer()) {
    for (const auto& t : frag_meta_reader.getTileMaxBuffer()) {
      auto& last = frag_meta->tile_max_buffer().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
  }
  if (frag_meta_reader.hasTileMaxVarBuffer()) {
    for (const auto& t : frag_meta_reader.getTileMaxVarBuffer()) {
      auto& last = frag_meta->tile_max_var_buffer().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
  }
  if (frag_meta_reader.hasTileSums()) {
    for (const auto& t : frag_meta_reader.getTileSums()) {
      auto& last = frag_meta->tile_sums().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
  }
  if (frag_meta_reader.hasTileNullCounts()) {
    for (const auto& t : frag_meta_reader.getTileNullCounts()) {
      auto& last = frag_meta->tile_null_counts().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
  }
  if (frag_meta_reader.hasFragmentMins()) {
    for (const auto& t : frag_meta_reader.getFragmentMins()) {
      auto& last = frag_meta->fragment_mins().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
  }
  if (frag_meta_reader.hasFragmentMaxs()) {
    for (const auto& t : frag_meta_reader.getFragmentMaxs()) {
      auto& last = frag_meta->fragment_maxs().emplace_back();
      last.reserve(t.size());
      for (const auto& v : t) {
        last.emplace_back(v);
      }
    }
  }
  if (frag_meta_reader.hasFragmentSums()) {
    frag_meta->fragment_sums().reserve(
        frag_meta_reader.getFragmentSums().size());
    for (const auto& fragment_sum : frag_meta_reader.getFragmentSums()) {
      frag_meta->fragment_sums().emplace_back(fragment_sum);
    }
  }
  if (frag_meta_reader.hasFragmentNullCounts()) {
    frag_meta->fragment_null_counts().reserve(
        frag_meta_reader.getFragmentNullCounts().size());
    for (const auto& fragment_null_count :
         frag_meta_reader.getFragmentNullCounts()) {
      frag_meta->fragment_null_counts().emplace_back(fragment_null_count);
    }
  }
  frag_meta->version() = frag_meta_reader.getVersion();
  if (frag_meta_reader.hasTimestampRange()) {
    frag_meta->timestamp_range() = std::make_pair(
        frag_meta_reader.getTimestampRange()[0],
        frag_meta_reader.getTimestampRange()[1]);
  }
  frag_meta->last_tile_cell_num() = frag_meta_reader.getLastTileCellNum();

  if (frag_meta_reader.hasRtree()) {
    auto data = frag_meta_reader.getRtree();
    auto& domain = array_schema->domain();
    // If there are no levels, we still need domain_ properly initialized
    frag_meta->rtree() = RTree(&domain, constants::rtree_fanout);
    Deserializer deserializer(data.begin(), data.size());
    frag_meta->rtree().deserialize(deserializer, &domain, frag_meta->version());
  }

  // Set the array schema and most importantly retrigger the build
  // of the internal idx_map. Also set array_schema_name which is used
  // in some places in the global writer
  frag_meta->set_array_schema(array_schema);
  frag_meta->set_schema_name(array_schema->name());
  frag_meta->set_dense(array_schema->dense());

  // It's important to do this here as init_domain depends on some fields
  // above to be properly initialized
  if (frag_meta_reader.hasNonEmptyDomain()) {
    auto reader = frag_meta_reader.getNonEmptyDomain();
    auto&& [status, ndrange] = utils::deserialize_non_empty_domain_rv(reader);
    RETURN_NOT_OK(status);
    // Whilst sparse gets its domain calculated, dense needs to have it
    // set here from the deserialized data
    if (array_schema->dense()) {
      frag_meta->init_domain(*ndrange);
    } else {
      const auto& frag0_dom = *ndrange;
      frag_meta->non_empty_domain().assign(frag0_dom.begin(), frag0_dom.end());
    }
  }

  return Status::Ok();
}

Status fragment_metadata_to_capnp(
    const FragmentMetadata& frag_meta,
    capnp::FragmentMetadata::Builder* frag_meta_builder) {
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

  frag_meta_builder->setFragmentUri(frag_meta.fragment_uri());
  frag_meta_builder->setHasTimestamps(frag_meta.has_timestamps());
  frag_meta_builder->setHasDeleteMeta(frag_meta.has_delete_meta());
  frag_meta_builder->setHasConsolidatedFooter(
      frag_meta.has_consolidated_footer());
  frag_meta_builder->setSparseTileNum(frag_meta.sparse_tile_num());
  frag_meta_builder->setTileIndexBase(frag_meta.tile_index_base());

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
  RETURN_NOT_OK(utils::serialize_non_empty_domain_rv(
      ned_builder,
      frag_meta.non_empty_domain(),
      frag_meta.array_schema()->dim_num()));

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

  return Status::Ok();
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
