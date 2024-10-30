/**
 * @file   generic_tile_io.cc
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
 * This file implements class GenericTileIO.
 */

#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/common/unreachable.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/encryption_aes256gcm_filter.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** Class for locally generated status exceptions. */
class GenericTileIOException : public StatusException {
 public:
  explicit GenericTileIOException(const std::string& msg)
      : StatusException("GenericTileIO", msg) {
  }
};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

GenericTileIO::GenericTileIO(ContextResources& resources, const URI& uri)
    : resources_(resources)
    , uri_(uri) {
}

/* ****************************** */
/*               API              */
/* ****************************** */

shared_ptr<Tile> GenericTileIO::load(
    ContextResources& resources,
    const URI& uri,
    uint64_t offset,
    const EncryptionKey& encryption_key,
    shared_ptr<MemoryTracker> memory_tracker) {
  GenericTileIO tile_io(resources, uri);

  // Get encryption key from config
  if (encryption_key.encryption_type() == EncryptionType::NO_ENCRYPTION) {
    EncryptionKey cfg_enc_key(resources.config());
    return tile_io.read_generic(
        offset, cfg_enc_key, resources.config(), memory_tracker);
  } else {
    return tile_io.read_generic(
        offset, encryption_key, resources.config(), memory_tracker);
  }

  stdx::unreachable();
}

shared_ptr<Tile> GenericTileIO::read_generic(
    uint64_t file_offset,
    const EncryptionKey& encryption_key,
    const Config& config,
    shared_ptr<MemoryTracker> memory_tracker) {
  auto&& header = read_generic_tile_header(resources_, uri_, file_offset);

  if (encryption_key.encryption_type() !=
      (EncryptionType)header.encryption_type) {
    throw GenericTileIOException(
        "Error reading generic tile; tile is encrypted with " +
        encryption_type_str((EncryptionType)header.encryption_type) +
        " but given key is for " +
        encryption_type_str(encryption_key.encryption_type()));
  }

  configure_encryption_filter(&header, encryption_key);

  const auto tile_data_offset =
      GenericTileHeader::BASE_SIZE + header.filter_pipeline_size;

  std::vector<char> filtered_data(header.persisted_size);
  shared_ptr<Tile> tile = make_shared<Tile>(
      HERE(),
      header.version_number,
      (Datatype)header.datatype,
      header.cell_size,
      0,
      header.tile_size,
      filtered_data.data(),
      header.persisted_size,
      memory_tracker->get_resource(MemoryType::GENERIC_TILE_IO),
      ThreadPool::SharedTask());

  // Read the tile.
  throw_if_not_ok(resources_.vfs().read(
      uri_,
      file_offset + tile_data_offset,
      tile->filtered_data(),
      header.persisted_size));

  // Unfilter
  assert(tile->filtered());
  header.filters.run_reverse_generic_tile(&resources_.stats(), *tile, config);
  assert(!tile->filtered());

  return tile;
}

GenericTileIO::GenericTileHeader GenericTileIO::read_generic_tile_header(
    ContextResources& resources, const URI& uri, uint64_t file_offset) {
  GenericTileHeader header;

  std::vector<uint8_t> base_buf(GenericTileHeader::BASE_SIZE);

  throw_if_not_ok(
      resources.vfs().read(uri, file_offset, base_buf.data(), base_buf.size()));

  Deserializer base_deserializer(base_buf.data(), base_buf.size());

  header.version_number = base_deserializer.read<uint32_t>();
  header.persisted_size = base_deserializer.read<uint64_t>();
  header.tile_size = base_deserializer.read<uint64_t>();
  header.datatype = base_deserializer.read<uint8_t>();
  header.cell_size = base_deserializer.read<uint64_t>();
  header.encryption_type = base_deserializer.read<uint8_t>();
  header.filter_pipeline_size = base_deserializer.read<uint32_t>();

  // Read header filter pipeline.
  std::vector<uint8_t> filter_pipeline_buf(header.filter_pipeline_size);
  throw_if_not_ok(resources.vfs().read(
      uri,
      file_offset + GenericTileHeader::BASE_SIZE,
      filter_pipeline_buf.data(),
      filter_pipeline_buf.size()));

  Deserializer filter_pipeline_deserializer(
      filter_pipeline_buf.data(), filter_pipeline_buf.size());
  auto filterpipeline{FilterPipeline::deserialize(
      filter_pipeline_deserializer,
      header.version_number,
      static_cast<Datatype>(header.datatype))};
  header.filters = std::move(filterpipeline);

  return header;
}

void GenericTileIO::store_data(
    ContextResources& resources,
    const URI& uri,
    shared_ptr<WriterTile> tile,
    const EncryptionKey& encryption_key) {
  GenericTileIO tile_io(resources, uri);
  uint64_t nbytes = 0;
  tile_io.write_generic(tile, encryption_key, &nbytes);
  throw_if_not_ok(resources.vfs().close_file(uri));
}

void GenericTileIO::write_generic(
    shared_ptr<WriterTile> tile,
    const EncryptionKey& encryption_key,
    uint64_t* nbytes) {
  // Create a header
  GenericTileHeader header;
  init_generic_tile_header(tile.get(), &header, encryption_key);

  // Filter tile
  assert(!tile->filtered());
  header.filters.run_forward(
      &resources_.stats(), tile.get(), nullptr, &resources_.compute_tp());
  header.persisted_size = tile->filtered_buffer().size();
  assert(tile->filtered());

  write_generic_tile_header(&header);

  throw_if_not_ok(resources_.vfs().write(
      uri_, tile->filtered_buffer().data(), tile->filtered_buffer().size()));

  *nbytes = GenericTileIO::GenericTileHeader::BASE_SIZE +
            header.filter_pipeline_size + header.persisted_size;
}

template <class T>
void GenericTileIO::serialize_generic_tile_header(
    T& serializer, GenericTileHeader& header) {
  serializer.write(header.version_number);
  serializer.write(header.persisted_size);
  serializer.write(header.tile_size);
  serializer.write(header.datatype);
  serializer.write(header.cell_size);
  serializer.write(header.encryption_type);
  serializer.write(header.filter_pipeline_size);
  header.filters.serialize(serializer);
}

void GenericTileIO::write_generic_tile_header(GenericTileHeader* header) {
  SizeComputationSerializer fp_size_computation_serializer;
  header->filters.serialize(fp_size_computation_serializer);
  header->filter_pipeline_size = fp_size_computation_serializer.size();

  SizeComputationSerializer size_computation_serializer;
  serialize_generic_tile_header(size_computation_serializer, *header);

  std::vector<uint8_t> data(size_computation_serializer.size());
  Serializer serializer(data.data(), data.size());
  serialize_generic_tile_header(serializer, *header);

  // Write buffer to file
  throw_if_not_ok(resources_.vfs().write(uri_, data.data(), data.size()));
}

void GenericTileIO::configure_encryption_filter(
    GenericTileHeader* header, const EncryptionKey& encryption_key) const {
  switch ((EncryptionType)header->encryption_type) {
    case EncryptionType::NO_ENCRYPTION:
      // Do nothing.
      break;
    case EncryptionType::AES_256_GCM: {
      auto* f = header->filters.get_filter<EncryptionAES256GCMFilter>();
      if (f == nullptr)
        throw GenericTileIOException(
            "Error getting generic tile; no encryption filter.");
      f->set_key(encryption_key);
      break;
    }
    default:
      throw GenericTileIOException(
          "Error getting generic tile; invalid encryption type.");
  }
}

void GenericTileIO::init_generic_tile_header(
    WriterTile* tile,
    GenericTileHeader* header,
    const EncryptionKey& encryption_key) const {
  header->tile_size = tile->size();
  header->datatype = (uint8_t)tile->type();
  header->cell_size = tile->cell_size();
  header->encryption_type = (uint8_t)encryption_key.encryption_type();

  header->filters.add_filter(CompressionFilter(
      constants::generic_tile_compressor,
      constants::generic_tile_compression_level,
      tile->type()));

  throw_if_not_ok(FilterPipeline::append_encryption_filter(
      &header->filters, encryption_key));
}

}  // namespace sm
}  // namespace tiledb
