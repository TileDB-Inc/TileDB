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
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/encryption_aes256gcm_filter.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

GenericTileIO::GenericTileIO(StorageManager* storage_manager, const URI& uri)
    : storage_manager_(storage_manager)
    , uri_(uri) {
}

/* ****************************** */
/*               API              */
/* ****************************** */

tuple<Status, optional<Tile>> GenericTileIO::read_generic(
    uint64_t file_offset,
    const EncryptionKey& encryption_key,
    const Config& config) {
  auto&& [st, header_opt] =
      read_generic_tile_header(storage_manager_, uri_, file_offset);
  RETURN_NOT_OK_TUPLE(st, nullopt);
  auto& header = *header_opt;

  if (encryption_key.encryption_type() !=
      (EncryptionType)header.encryption_type) {
    return {LOG_STATUS(Status_TileIOError(
                "Error reading generic tile; tile is encrypted with " +
                encryption_type_str((EncryptionType)header.encryption_type) +
                " but given key is for " +
                encryption_type_str(encryption_key.encryption_type()))),
            nullopt};
  }

  RETURN_NOT_OK_TUPLE(
      configure_encryption_filter(&header, encryption_key), nullopt);

  const auto tile_data_offset =
      GenericTileHeader::BASE_SIZE + header.filter_pipeline_size;

  Tile tile(
      header.version_number,
      (Datatype)header.datatype,
      header.cell_size,
      0,
      header.tile_size,
      header.persisted_size);

  // Read the tile.
  RETURN_NOT_OK_TUPLE(
      storage_manager_->read(
          uri_,
          file_offset + tile_data_offset,
          tile.filtered_buffer().data(),
          header.persisted_size),
      nullopt);

  // Unfilter
  assert(tile.filtered());
  RETURN_NOT_OK_TUPLE(
      header.filters.run_reverse(
          storage_manager_->stats(),
          &tile,
          nullptr,
          storage_manager_->compute_tp(),
          config,
          nullptr),
      nullopt);
  assert(!tile.filtered());

  return {Status::Ok(), std::move(tile)};
}

tuple<Status, optional<GenericTileIO::GenericTileHeader>>
GenericTileIO::read_generic_tile_header(
    const StorageManager* sm, const URI& uri, uint64_t file_offset) {
  GenericTileHeader header;

  // Read the fixed-sized part of the header from file
  tdb_unique_ptr<Buffer> header_buff(tdb_new(Buffer));
  RETURN_NOT_OK_TUPLE(
      sm->read(
          uri, file_offset, header_buff.get(), GenericTileHeader::BASE_SIZE),
      nullopt);

  // Read header individual values
  RETURN_NOT_OK_TUPLE(
      header_buff->read(&header.version_number, sizeof(uint32_t)), nullopt);
  RETURN_NOT_OK_TUPLE(
      header_buff->read(&header.persisted_size, sizeof(uint64_t)), nullopt);
  RETURN_NOT_OK_TUPLE(
      header_buff->read(&header.tile_size, sizeof(uint64_t)), nullopt);
  RETURN_NOT_OK_TUPLE(
      header_buff->read(&header.datatype, sizeof(uint8_t)), nullopt);
  RETURN_NOT_OK_TUPLE(
      header_buff->read(&header.cell_size, sizeof(uint64_t)), nullopt);
  RETURN_NOT_OK_TUPLE(
      header_buff->read(&header.encryption_type, sizeof(uint8_t)), nullopt);
  RETURN_NOT_OK_TUPLE(
      header_buff->read(&header.filter_pipeline_size, sizeof(uint32_t)),
      nullopt);

  // Read header filter pipeline.
  header_buff->reset_size();
  header_buff->reset_offset();
  RETURN_NOT_OK_TUPLE(
      sm->read(
          uri,
          file_offset + GenericTileHeader::BASE_SIZE,
          header_buff.get(),
          header.filter_pipeline_size),
      nullopt);
  Deserializer deserializer(header_buff->data(), header_buff->size());
  auto filterpipeline{
      FilterPipeline::deserialize(deserializer, header.version_number)};
  header.filters = std::move(filterpipeline);

  return {Status::Ok(), header};
}

Status GenericTileIO::write_generic(
    Tile* tile, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  // Create a header
  GenericTileHeader header;
  RETURN_NOT_OK(init_generic_tile_header(tile, &header, encryption_key));

  // Filter tile
  assert(!tile->filtered());
  RETURN_NOT_OK(header.filters.run_forward(
      storage_manager_->stats(),
      tile,
      nullptr,
      storage_manager_->compute_tp()));
  header.persisted_size = tile->filtered_buffer().size();
  assert(tile->filtered());

  RETURN_NOT_OK(write_generic_tile_header(&header));

  RETURN_NOT_OK(storage_manager_->write(
      uri_, tile->filtered_buffer().data(), tile->filtered_buffer().size()));

  *nbytes = GenericTileIO::GenericTileHeader::BASE_SIZE +
            header.filter_pipeline_size + header.persisted_size;

  return Status::Ok();
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

Status GenericTileIO::write_generic_tile_header(GenericTileHeader* header) {
  SizeComputationSerializer fp_size_computation_serializer;
  header->filters.serialize(fp_size_computation_serializer);
  header->filter_pipeline_size = fp_size_computation_serializer.size();

  SizeComputationSerializer size_computation_serializer;
  serialize_generic_tile_header(size_computation_serializer, *header);

  std::vector<uint8_t> data(size_computation_serializer.size());
  Serializer serializer(data.data(), data.size());
  serialize_generic_tile_header(serializer, *header);

  // Write buffer to file
  Status st = storage_manager_->write(uri_, data.data(), data.size());

  return st;
}

Status GenericTileIO::configure_encryption_filter(
    GenericTileHeader* header, const EncryptionKey& encryption_key) const {
  switch ((EncryptionType)header->encryption_type) {
    case EncryptionType::NO_ENCRYPTION:
      // Do nothing.
      break;
    case EncryptionType::AES_256_GCM: {
      auto* f = header->filters.get_filter<EncryptionAES256GCMFilter>();
      if (f == nullptr)
        return Status_TileIOError(
            "Error getting generic tile; no encryption filter.");
      f->set_key(encryption_key);
      break;
    }
    default:
      return Status_TileIOError(
          "Error getting generic tile; invalid encryption type.");
  }

  return Status::Ok();
}

Status GenericTileIO::init_generic_tile_header(
    Tile* tile,
    GenericTileHeader* header,
    const EncryptionKey& encryption_key) const {
  header->tile_size = tile->size();
  header->datatype = (uint8_t)tile->type();
  header->cell_size = tile->cell_size();
  header->encryption_type = (uint8_t)encryption_key.encryption_type();

  header->filters.add_filter(CompressionFilter(
      constants::generic_tile_compressor,
      constants::generic_tile_compression_level));

  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &header->filters, encryption_key));

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
