/**
 * @file   tile_io.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file implements class TileIO.
 */

#include "tiledb/sm/tile/tile_io.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/encryption_aes256gcm_filter.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/tile.h"

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

TileIO::TileIO(StorageManager* storage_manager, const URI& uri)
    : storage_manager_(storage_manager)
    , uri_(uri) {
  file_size_ = 0;
}

/* ****************************** */
/*               API              */
/* ****************************** */

uint64_t TileIO::file_size() const {
  return file_size_;
}

Status TileIO::is_generic_tile(
    const StorageManager* sm, const URI& uri, bool* is_generic_tile) {
  *is_generic_tile = false;

  bool is_file;
  RETURN_NOT_OK(sm->vfs()->is_file(uri, &is_file));
  if (!is_file)
    return Status::Ok();

  uint64_t file_size;
  RETURN_NOT_OK(sm->vfs()->file_size(uri, &file_size));
  if (file_size < GenericTileHeader::BASE_SIZE)
    return Status::Ok();

  GenericTileHeader header;
  RETURN_NOT_OK(read_generic_tile_header(sm, uri, 0, &header));

  auto expected_size = GenericTileHeader::BASE_SIZE +
                       header.filter_pipeline_size + header.persisted_size;
  if (file_size != expected_size)
    return Status::Ok();

  *is_generic_tile = true;
  return Status::Ok();
}

Status TileIO::read_generic(
    Tile** tile,
    uint64_t file_offset,
    const EncryptionKey& encryption_key,
    const Config& config) {
  GenericTileHeader header;
  RETURN_NOT_OK(
      read_generic_tile_header(storage_manager_, uri_, file_offset, &header));

  if (encryption_key.encryption_type() !=
      (EncryptionType)header.encryption_type)
    return LOG_STATUS(Status::TileIOError(
        "Error reading generic tile; tile is encrypted with " +
        encryption_type_str((EncryptionType)header.encryption_type) +
        " but given key is for " +
        encryption_type_str(encryption_key.encryption_type())));

  RETURN_NOT_OK(configure_encryption_filter(&header, encryption_key));

  const auto tile_data_offset =
      GenericTileHeader::BASE_SIZE + header.filter_pipeline_size;

  assert(tile);
  *tile = new Tile();
  RETURN_NOT_OK_ELSE(
      (*tile)->init_filtered(
          header.version_number,
          (Datatype)header.datatype,
          header.cell_size,
          0),
      delete *tile);

  // Read the tile.
  RETURN_NOT_OK_ELSE(
      (*tile)->filtered_buffer()->realloc(header.persisted_size), delete *tile);
  RETURN_NOT_OK_ELSE(
      storage_manager_->read(
          uri_,
          file_offset + tile_data_offset,
          (*tile)->filtered_buffer(),
          header.persisted_size),
      delete *tile);

  // Unfilter
  assert((*tile)->filtered());
  RETURN_NOT_OK_ELSE(header.filters.run_reverse(*tile, config), delete *tile);
  assert(!(*tile)->filtered());

  file_size_ = header.persisted_size;

  return Status::Ok();
}

Status TileIO::read_generic_tile_header(
    const StorageManager* sm,
    const URI& uri,
    uint64_t file_offset,
    GenericTileHeader* header) {
  // Read the fixed-sized part of the header from file
  std::unique_ptr<Buffer> header_buff(new Buffer());
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
  RETURN_NOT_OK(header->filters.deserialize(&cbuf));

  return Status::Ok();
}

Status TileIO::write_generic(
    Tile* tile, const EncryptionKey& encryption_key, uint64_t* nbytes) {
  // Reset the tile and buffer offset
  tile->reset_offset();

  // Create a header
  GenericTileHeader header;
  RETURN_NOT_OK(init_generic_tile_header(tile, &header, encryption_key));

  // Filter tile
  assert(!tile->filtered());
  RETURN_NOT_OK(header.filters.run_forward(tile));
  header.persisted_size = tile->filtered_buffer()->size();
  assert(tile->filtered());

  RETURN_NOT_OK(write_generic_tile_header(&header));
  RETURN_NOT_OK(storage_manager_->write(uri_, tile->filtered_buffer()));

  file_size_ = header.persisted_size;

  *nbytes = TileIO::GenericTileHeader::BASE_SIZE + header.filter_pipeline_size +
            header.persisted_size;

  return Status::Ok();
}

Status TileIO::write_generic_tile_header(GenericTileHeader* header) {
  // Write to buffer
  auto buff = new Buffer();
  RETURN_NOT_OK_ELSE(
      buff->write(&header->version_number, sizeof(uint32_t)), delete buff);
  RETURN_NOT_OK_ELSE(
      buff->write(&header->persisted_size, sizeof(uint64_t)), delete buff);
  RETURN_NOT_OK_ELSE(
      buff->write(&header->tile_size, sizeof(uint64_t)), delete buff);
  RETURN_NOT_OK_ELSE(
      buff->write(&header->datatype, sizeof(uint8_t)), delete buff);
  RETURN_NOT_OK_ELSE(
      buff->write(&header->cell_size, sizeof(uint64_t)), delete buff);
  RETURN_NOT_OK_ELSE(
      buff->write(&header->encryption_type, sizeof(uint8_t)), delete buff);

  // Write placeholder value for pipeline size.
  uint64_t pipeline_size_offset = buff->offset();
  RETURN_NOT_OK_ELSE(
      buff->write(&header->filter_pipeline_size, sizeof(uint32_t)),
      delete buff);

  // Write pipeline to buffer
  auto orig_size = buff->size();
  RETURN_NOT_OK_ELSE(header->filters.serialize(buff), delete buff);

  // Write actual pipeline size over placeholder.
  header->filter_pipeline_size =
      static_cast<uint32_t>(buff->size() - orig_size);
  *(static_cast<uint32_t*>(buff->value_ptr(pipeline_size_offset))) =
      header->filter_pipeline_size;

  // Write buffer to file
  Status st = storage_manager_->write(uri_, buff);

  delete buff;

  return st;
}

Status TileIO::configure_encryption_filter(
    GenericTileHeader* header, const EncryptionKey& encryption_key) const {
  switch ((EncryptionType)header->encryption_type) {
    case EncryptionType::NO_ENCRYPTION:
      // Do nothing.
      break;
    case EncryptionType::AES_256_GCM: {
      auto* f = header->filters.get_filter<EncryptionAES256GCMFilter>();
      if (f == nullptr)
        return Status::TileIOError(
            "Error getting generic tile; no encryption filter.");
      RETURN_NOT_OK(f->set_key(encryption_key));
      break;
    }
    default:
      return Status::TileIOError(
          "Error getting generic tile; invalid encryption type.");
  }

  return Status::Ok();
}

Status TileIO::init_generic_tile_header(
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
