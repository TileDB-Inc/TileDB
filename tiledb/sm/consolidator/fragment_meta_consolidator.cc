/**
 * @file   fragment_meta_consolidator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2025 TileDB, Inc.
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
 * This file implements the FragmentMetaConsolidator class.
 */

#include "tiledb/sm/consolidator/fragment_meta_consolidator.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/fragment/fragment_identifier.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/storage_format/uri/generate_uri.h"

using namespace tiledb::common;

namespace tiledb::sm {

/* ****************************** */
/*          CONSTRUCTOR           */
/* ****************************** */

FragmentMetaConsolidator::FragmentMetaConsolidator(
    ContextResources& resources, StorageManager* storage_manager)
    : Consolidator(resources, storage_manager) {
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status FragmentMetaConsolidator::consolidate(
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  auto timer_se = stats_->start_timer("consolidate_frag_meta");

  check_array_uri(array_name);

  // Open array for reading
  Array array(resources_, URI(array_name));
  throw_if_not_ok(
      array.open(QueryType::READ, encryption_type, encryption_key, key_length));

  // Include only fragments with footers / separate basic metadata
  const auto& tmp_meta = array.fragment_metadata();
  std::vector<shared_ptr<FragmentMetadata>> meta;
  for (auto m : tmp_meta) {
    if (m->format_version() > 2)
      meta.emplace_back(m);
  }
  auto fragment_num = (unsigned)meta.size();

  // Do not consolidate if the number of fragments is not >1
  if (fragment_num < 2)
    return array.close();

  // Compute new URI
  URI uri;
  auto& array_dir = array.array_directory();
  auto first = meta.front()->fragment_uri();
  auto last = meta.back()->fragment_uri();
  auto write_version = array.array_schema_latest().write_version();
  auto name = storage_format::generate_consolidated_fragment_name(
      first, last, write_version);

  auto frag_md_uri = array_dir.get_fragment_metadata_dir(write_version);
  resources_.vfs().create_dir(frag_md_uri);
  uri = URI(frag_md_uri.to_string() + name + constants::meta_file_suffix);

  // Get the consolidated fragment metadata version
  FragmentID fragment_id{uri};
  auto meta_version = fragment_id.array_format_version();

  // Calculate offset of first fragment footer
  uint64_t offset = sizeof(uint32_t);  // Fragment num
  for (auto m : meta) {
    offset += sizeof(uint64_t);  // Name size
    if (meta_version >= 9) {
      offset += m->fragment_uri().last_path_part().size();  // Name
    } else {
      offset += m->fragment_uri().to_string().size();  // Name
    }
    offset += sizeof(uint64_t);  // Offset
  }

  // Serialize all fragment metadata footers in parallel
  std::vector<shared_ptr<WriterTile>> tiles(meta.size());
  auto status =
      parallel_for(&resources_.compute_tp(), 0, tiles.size(), [&](size_t i) {
        SizeComputationSerializer size_computation_serializer;
        meta[i]->write_footer(size_computation_serializer);
        tiles[i] = WriterTile::from_generic(
            size_computation_serializer.size(), consolidator_memory_tracker_);
        Serializer serializer(tiles[i]->data(), tiles[i]->size());
        meta[i]->write_footer(serializer);

        return Status::Ok();
      });
  throw_if_not_ok(status);

  auto serialize_data = [&](Serializer& serializer, uint64_t offset) {
    // Write number of fragments
    serializer.write<uint32_t>(fragment_num);

    // Serialize all fragment names and footer offsets into a single buffer
    for (auto m : meta) {
      // Write name size and name
      std::string name;
      if (meta_version >= 9) {
        name = m->fragment_uri().last_path_part();
      } else {
        name = m->fragment_uri().to_string();
      }
      auto name_size = (uint64_t)name.size();
      serializer.write<uint64_t>(name_size);
      serializer.write(name.c_str(), name_size);
      serializer.write<uint64_t>(offset);
      offset += m->footer_size();
    }

    // Combine serialized fragment metadata footers into a single buffer
    for (const auto& t : tiles)
      serializer.write(t->data(), t->size());
  };

  SizeComputationSerializer size_computation_serializer;
  serialize_data(size_computation_serializer, offset);

  auto tile{WriterTile::from_generic(
      size_computation_serializer.size(), consolidator_memory_tracker_)};

  Serializer serializer(tile->data(), tile->size());
  serialize_data(serializer, offset);

  // Close array
  throw_if_not_ok(array.close());

  EncryptionKey enc_key;
  throw_if_not_ok(enc_key.set_key(encryption_type, encryption_key, key_length));

  GenericTileIO tile_io(resources_, uri);
  [[maybe_unused]] uint64_t nbytes = 0;
  tile_io.write_generic(tile, enc_key, &nbytes);
  throw_if_not_ok(resources_.vfs().close_file(uri));

  return Status::Ok();
}

void FragmentMetaConsolidator::vacuum(const char* array_name) {
  if (array_name == nullptr) {
    throw std::invalid_argument(
        "Cannot vacuum fragment metadata; Array name cannot be null");
  }

  // Get the consolidated fragment metadata URIs to be deleted
  // (all except the last one)
  ArrayDirectory array_dir(resources_, URI(array_name), 0, UINT64_MAX);
  const auto& fragment_meta_uris = array_dir.fragment_meta_uris();

  // Get the latest timestamp
  uint64_t t_latest = 0;
  for (const auto& uri : fragment_meta_uris) {
    FragmentID fragment_id{uri};
    auto timestamp_range{fragment_id.timestamp_range()};
    if (timestamp_range.second > t_latest) {
      t_latest = timestamp_range.second;
    }
  }

  // Vacuum
  auto& vfs = resources_.vfs();
  auto& compute_tp = resources_.compute_tp();
  throw_if_not_ok(
      parallel_for(&compute_tp, 0, fragment_meta_uris.size(), [&](size_t i) {
        auto& uri = fragment_meta_uris[i];
        FragmentID fragment_id{uri};
        auto timestamp_range{fragment_id.timestamp_range()};
        if (timestamp_range.second != t_latest) {
          vfs.remove_file(uri);
        }
        return Status::Ok();
      }));
}

}  // namespace tiledb::sm
