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

Status GenericTileIO::read_generic(
    Tile** tile,
    uint64_t file_offset,
    const EncryptionKey& encryption_key,
    const Config& config) {
  tdb_unique_ptr<Tile> ret(tdb_new(Tile));
  GenericTileHeader header;
  RETURN_NOT_OK(
      read_generic_tile_header(storage_manager_, uri_, file_offset, &header));

  if (encryption_key.encryption_type() !=
      (EncryptionType)header.encryption_type)
    return LOG_STATUS(Status_TileIOError(
        "Error reading generic tile; tile is encrypted with " +
        encryption_type_str((EncryptionType)header.encryption_type) +
        " but given key is for " +
        encryption_type_str(encryption_key.encryption_type())));

  RETURN_NOT_OK(configure_encryption_filter(&header, encryption_key));

  const auto tile_data_offset =
      GenericTileHeader::BASE_SIZE + header.filter_pipeline_size;

  RETURN_NOT_OK(ret->init_filtered(
      header.version_number, (Datatype)header.datatype, header.cell_size, 0));

  // Read the tile.
  ret->filtered_buffer().expand(header.persisted_size);
  RETURN_NOT_OK(ret->alloc_data(header.tile_size));
  RETURN_NOT_OK(storage_manager_->read(
      uri_,
      file_offset + tile_data_offset,
      ret->filtered_buffer().data(),
      header.persisted_size));

  // Unfilter
  assert(ret->filtered());
  RETURN_NOT_OK(header.filters.run_reverse(
      storage_manager_->stats(),
      ret.get(),
      nullptr,
      storage_manager_->compute_tp(),
      config));
  assert(!ret->filtered());

  *tile = ret.release();
  return Status::Ok();
}

Status GenericTileIO::read_generic_tile_header(
    const StorageManager* sm,
    const URI& uri,
    uint64_t file_offset,
    GenericTileHeader* header) {
  // Read the fixed-sized part of the header from file
  tdb_unique_ptr<Buffer> header_buff(tdb_new(Buffer));
  RETURN_NOT_OK(sm->read(
      uri, file_offset, header_buff.get(), GenericTileHeader::BASE_SIZE));

  // Read header individual values
  RETURN_NOT_OK(header_buff->read(&header->version_number, sizeof(uint32_t)));
  RETURN_NOT_OK(header_buff->read(&header->persisted_size, sizeof(uint64_t)));
  RETURN_NOT_OK(header_buff->read(&header->tile_size, sizeof(uint64_t)));
  RETURN_NOT_OK(header_buff->read(&header->datatype, sizeof(uint8_t)));
  RETURN_NOT_OK(header_buff->read(&header->cell_size, sizeof(uint64_t)));
  RETURN_NOT_OK(header_buff->read(&header->encryption_type, sizeof(uint8_t)));
  RETURN_NOT_OK(
      header_buff->read(&header->filter_pipeline_size, sizeof(uint32_t)));

  // Read header filter pipeline.
  header_buff->reset_size();
  header_buff->reset_offset();
  RETURN_NOT_OK(sm->read(
      uri,
      file_offset + GenericTileHeader::BASE_SIZE,
      header_buff.get(),
      header->filter_pipeline_size));
  ConstBuffer cbuf(header_buff->data(), header_buff->size());
  auto&& [st_filterpipeline, filterpipeline]{
      FilterPipeline::deserialize(&cbuf, header->version_number)};
  if (!st_filterpipeline.ok()) {
    return st_filterpipeline;
  }
  header->filters = filterpipeline.value();

  return Status::Ok();
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

Status GenericTileIO::write_generic_tile_header(GenericTileHeader* header) {
  // Write to buffer
  auto buff = tdb_new(Buffer);
  RETURN_NOT_OK_ELSE(
      buff->write(&header->version_number, sizeof(uint32_t)), tdb_delete(buff));
  RETURN_NOT_OK_ELSE(
      buff->write(&header->persisted_size, sizeof(uint64_t)), tdb_delete(buff));
  RETURN_NOT_OK_ELSE(
      buff->write(&header->tile_size, sizeof(uint64_t)), tdb_delete(buff));
  RETURN_NOT_OK_ELSE(
      buff->write(&header->datatype, sizeof(uint8_t)), tdb_delete(buff));
  RETURN_NOT_OK_ELSE(
      buff->write(&header->cell_size, sizeof(uint64_t)), tdb_delete(buff));
  RETURN_NOT_OK_ELSE(
      buff->write(&header->encryption_type, sizeof(uint8_t)), tdb_delete(buff));

  // Write placeholder value for pipeline size.
  uint64_t pipeline_size_offset = buff->offset();
  RETURN_NOT_OK_ELSE(
      buff->write(&header->filter_pipeline_size, sizeof(uint32_t)),
      tdb_delete(buff));

  // Write pipeline to buffer
  auto orig_size = buff->size();
  RETURN_NOT_OK_ELSE(header->filters.serialize(buff), tdb_delete(buff));

  // Write actual pipeline size over placeholder.
  header->filter_pipeline_size =
      static_cast<uint32_t>(buff->size() - orig_size);
  *(static_cast<uint32_t*>(buff->value_ptr(pipeline_size_offset))) =
      header->filter_pipeline_size;

  // Write buffer to file
  Status st = storage_manager_->write(uri_, buff);

  tdb_delete(buff);

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
      RETURN_NOT_OK(f->set_key(encryption_key));
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

  RETURN_NOT_OK(header->filters.add_filter(CompressionFilter(
      constants::generic_tile_compressor,
      constants::generic_tile_compression_level)));

  RETURN_NOT_OK(FilterPipeline::append_encryption_filter(
      &header->filters, encryption_key));

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
