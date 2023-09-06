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
#include "tiledb/common/unreachable.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/encryption_aes256gcm_filter.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::common;

namespace tiledb::sm {

class GenericTileIOException : public StatusException {
 public:
  explicit GenericTileIOException(const std::string& message)
      : StatusException("GenericTileIO", message) {
  }
};

void set_encryption_filter(
    GenericTileHeader& header, const EncryptionKey& key) {

  if (key.encryption_type() !=
      static_cast<EncryptionType>(header.encryption_type)) {
    throw GenericTileIOException(
        "Error reading generic tile; tile is encrypted with " +
        encryption_type_str(
            static_cast<EncryptionType>(header.encryption_type)) +
        " but given key is for " + encryption_type_str(key.encryption_type()));
  }

  switch (static_cast<EncryptionType>(header.encryption_type)) {
    case EncryptionType::NO_ENCRYPTION:
      // Do nothing.
      return;
    case EncryptionType::AES_256_GCM: {
      auto* f = header.filters.get_filter<EncryptionAES256GCMFilter>();
      if (f == nullptr) {
        throw GenericTileIOException(
            "Error getting generic tile; no encryption filter.");
      }
      f->set_key(key);
      return;
    }
    default:
      throw GenericTileIOException(
          "Error getting generic tile; invalid encryption type.");
  }
}

GenericTileHeader GenericTileIO::read_header(
    ContextResources& resources, const URI& uri, uint64_t offset) {
  GenericTileHeader header;

  ByteVec buf(GenericTileHeader::HEADER_READ_SIZE);
  auto st = resources.vfs().read(uri, offset, buf.data(), buf.size());
  if (!st.ok()) {
    throw GenericTileIOException(
        "Error reading GenericTileHeader from " + uri.to_string() + " : " +
        st.to_string());
  }

  Deserializer deserializer(buf.data(), buf.size());

  header.version_number = deserializer.read<uint32_t>();
  header.persisted_size = deserializer.read<uint64_t>();
  header.tile_size = deserializer.read<uint64_t>();
  header.datatype = deserializer.read<uint8_t>();
  header.cell_size = deserializer.read<uint64_t>();
  header.encryption_type = deserializer.read<uint8_t>();
  header.filter_pipeline_size = deserializer.read<uint32_t>();

  // If filter_pipeline_size is larger than our estimated read-ahead
  // we have to read the rest into memory.
  if (header.filter_pipeline_size > GenericTileHeader::FILTER_PIPELINE_SIZE) {
    // Resize our buffer to be large enough for the pipeline data.
    buf.resize(GenericTileHeader::BASE_SIZE + header.filter_pipeline_size);

    // Calculate the remaining bytes to read into the buffer
    uint64_t num_to_read =
        header.filter_pipeline_size - GenericTileHeader::FILTER_PIPELINE_SIZE;

    // Track the number of extra bytes required for reading.
    resources.stats().add_counter(
        "generic_tile_io_pipeline_read_extra", num_to_read);

    // Read the remaining bytes into the end of the buffer
    throw_if_not_ok(resources.vfs().read(
        uri,
        offset + GenericTileHeader::HEADER_READ_SIZE,
        buf.data() + (buf.size() - num_to_read),
        num_to_read));
  }

  deserializer = Deserializer(
      buf.data() + GenericTileHeader::BASE_SIZE, header.filter_pipeline_size);
  auto filterpipeline{FilterPipeline::deserialize(
      deserializer,
      header.version_number,
      static_cast<Datatype>(header.datatype))};
  header.filters = std::move(filterpipeline);

  return header;
}

Tile read_impl(ContextResources& resources, const URI& uri, const EncryptionKey& key, uint64_t offset) {
  auto&& header = GenericTileIO::read_header(resources, uri, offset);
  set_encryption_filter(header, key);

  // Read the tile data from storage.
  ByteVec filtered_data(header.persisted_size);
  auto st = resources.vfs().read(
      uri,
      offset + GenericTileHeader::BASE_SIZE + header.filter_pipeline_size,
      filtered_data.data(),
      header.persisted_size);
  if (!st.ok()) {
    throw GenericTileIOException(
        "Failed to read generic tile from " + uri.to_string() + " : " +
        st.to_string());
  }

  Tile tile(
      header.version_number,
      static_cast<Datatype>(header.datatype),
      header.cell_size,
      0,
      header.tile_size,
      filtered_data.data(),
      header.persisted_size);

  // Unfilter
  assert(tile.filtered());
  header.filters.run_reverse_generic_tile(
      &resources.stats(), tile, resources.config());
  assert(!tile.filtered());

  return tile;
}

Tile GenericTileIO::read(
    ContextResources& resources, const URI& uri, const EncryptionKey& key, uint64_t offset) {
  if (key.encryption_type() == EncryptionType::NO_ENCRYPTION) {
    return read_impl(resources, uri, EncryptionKey(resources.config()), offset);
  } else {
    return read_impl(resources, uri, key, offset);
  }
}

template <class T>
void serialize_header(T& serializer, GenericTileHeader& header) {
  serializer.write(header.version_number);
  serializer.write(header.persisted_size);
  serializer.write(header.tile_size);
  serializer.write(header.datatype);
  serializer.write(header.cell_size);
  serializer.write(header.encryption_type);
  serializer.write(header.filter_pipeline_size);
  header.filters.serialize(serializer);
}

void write_header(
    ContextResources& resources, const URI& uri, GenericTileHeader& header) {
  SizeComputationSerializer size_serializer;
  header.filters.serialize(size_serializer);
  header.filter_pipeline_size = size_serializer.size();

  size_serializer = SizeComputationSerializer();
  serialize_header(size_serializer, header);

  ByteVec data(size_serializer.size());
  Serializer serializer(data.data(), data.size());
  serialize_header(serializer, header);

  // Write buffer to file
  auto st = resources.vfs().write(uri, data.data(), data.size());
  if (!st.ok()) {
    throw GenericTileIOException(
        "Failed to write generic tile header to " + uri.to_string() + " : " +
        st.to_string());
  }
}

uint64_t GenericTileIO::write(
    ContextResources& resources,
    const URI& uri,
    WriterTile& tile,
    const EncryptionKey& key) {
  // Initialize our header
  GenericTileHeader header;
  header.tile_size = tile.size();
  header.datatype = static_cast<uint8_t>(tile.type());
  header.cell_size = tile.cell_size();
  header.encryption_type = static_cast<uint8_t>(key.encryption_type());
  header.filters.add_filter(CompressionFilter(
      constants::generic_tile_compressor,
      constants::generic_tile_compression_level,
      tile.type()));
  auto st = FilterPipeline::append_encryption_filter(&header.filters, key);
  if (!st.ok()) {
    throw GenericTileIOException(
        "Failed to configure encryption filter for generic tile write to " +
        uri.to_string() + " : " + st.to_string());
  }

  // Prepare tile data for writing by passing it through the filter pipeline.
  assert(!tile.filtered());
  st = header.filters.run_forward(
      &resources.stats(), &tile, nullptr, &resources.compute_tp());
  if (!st.ok()) {
    throw GenericTileIOException(
        "Failed to filter tile for generic tile write to " + uri.to_string() +
        " : " + st.to_string());
  }
  header.persisted_size = tile.filtered_buffer().size();
  assert(tile.filtered());

  write_header(resources, uri, header);

  st = resources.vfs().write(
      uri, tile.filtered_buffer().data(), tile.filtered_buffer().size());
  if (!st.ok()) {
    throw GenericTileIOException(
        "Failed to write generic tile to " + uri.to_string() + " : " +
        st.to_string());
  }

  return GenericTileHeader::BASE_SIZE + header.filter_pipeline_size +
         header.persisted_size;
}

}  // namespace tiledb::sm
