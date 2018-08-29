/**
 * @file   tile_io.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/stats.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifndef MIN
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

TileIO::TileIO() {
  file_size_ = 0;
  storage_manager_ = nullptr;
  uri_ = URI("");
}

TileIO::TileIO(StorageManager* storage_manager, const URI& uri)
    : storage_manager_(storage_manager)
    , uri_(uri) {
  file_size_ = 0;
}

TileIO::TileIO(
    StorageManager* storage_manager, const URI& uri, uint64_t file_size)
    : file_size_(file_size)
    , storage_manager_(storage_manager)
    , uri_(uri) {
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
  if (file_size < GenericTileHeader::SIZE)
    return Status::Ok();

  GenericTileHeader header;
  RETURN_NOT_OK(read_generic_tile_header(sm, uri, 0, &header));
  if (file_size != GenericTileHeader::SIZE + header.persisted_size)
    return Status::Ok();

  *is_generic_tile = true;
  return Status::Ok();
}

Status TileIO::read_generic(Tile** tile, uint64_t file_offset) {
  GenericTileHeader header;
  RETURN_NOT_OK(
      read_generic_tile_header(storage_manager_, uri_, file_offset, &header));

  *tile = new Tile();
  RETURN_NOT_OK_ELSE(
      (*tile)->init((Datatype)header.datatype, header.cell_size, 0),
      delete *tile);

  // Try the cache first.
  bool cache_hit;
  RETURN_NOT_OK(storage_manager_->read_from_cache(
      uri_,
      file_offset + GenericTileHeader::SIZE,
      (*tile)->buffer(),
      header.tile_size,
      &cache_hit));

  // Read from disk and add to cache if it missed.
  if (!cache_hit) {
    RETURN_NOT_OK_ELSE(
        storage_manager_->read(
            uri_,
            file_offset + GenericTileHeader::SIZE,
            (*tile)->buffer(),
            header.persisted_size),
        delete *tile);

    // Decompress
    FilterPipeline pipeline;
    RETURN_NOT_OK_ELSE(
        pipeline.add_filter(CompressionFilter(
            (Compressor)header.compressor, header.compression_level)),
        delete *tile);
    RETURN_NOT_OK_ELSE(pipeline.run_reverse(*tile), delete *tile);

    STATS_COUNTER_ADD(tileio_read_num_resulting_bytes, (*tile)->size());

    RETURN_NOT_OK(storage_manager_->write_to_cache(
        uri_, file_offset + GenericTileHeader::SIZE, (*tile)->buffer()));
  }

  return Status::Ok();
}

Status TileIO::read_generic_tile_header(
    const StorageManager* sm,
    const URI& uri,
    uint64_t file_offset,
    GenericTileHeader* header) {
  // Read header from file
  std::unique_ptr<Buffer> header_buff(new Buffer());
  RETURN_NOT_OK(
      sm->read(uri, file_offset, header_buff.get(), GenericTileHeader::SIZE));

  // Read header individual values
  RETURN_NOT_OK(header_buff->read(&header->persisted_size, sizeof(uint64_t)));
  RETURN_NOT_OK(header_buff->read(&header->tile_size, sizeof(uint64_t)));
  RETURN_NOT_OK(header_buff->read(&header->datatype, sizeof(char)));
  RETURN_NOT_OK(header_buff->read(&header->cell_size, sizeof(uint64_t)));
  RETURN_NOT_OK(header_buff->read(&header->compressor, sizeof(char)));
  RETURN_NOT_OK(header_buff->read(&header->compression_level, sizeof(int)));

  STATS_COUNTER_ADD(tileio_read_num_bytes_read, GenericTileHeader::SIZE);

  return Status::Ok();
}

Status TileIO::write_generic(Tile* tile) {
  // Reset the tile and buffer offset
  tile->reset_offset();

  STATS_COUNTER_ADD(tileio_write_num_input_bytes, tile->size());

  // Compress tile
  FilterPipeline pipeline;
  RETURN_NOT_OK(pipeline.add_filter(CompressionFilter(
      constants::generic_tile_compressor,
      constants::generic_tile_compression_level)));
  RETURN_NOT_OK(pipeline.run_forward(tile));

  RETURN_NOT_OK(write_generic_tile_header(tile, tile->buffer()->size()));
  RETURN_NOT_OK(storage_manager_->write(uri_, tile->buffer()));

  STATS_COUNTER_ADD(tileio_write_num_bytes_written, tile->buffer()->size());

  return Status::Ok();
}

Status TileIO::write_generic_tile_header(Tile* tile, uint64_t persisted_size) {
  // Initializations
  uint64_t tile_size = tile->size();
  auto datatype = (char)tile->type();
  uint64_t cell_size = tile->cell_size();
  auto compressor = constants::generic_tile_compressor;
  int compression_level = constants::generic_tile_compression_level;

  // Write to buffer
  auto buff = new Buffer();
  RETURN_NOT_OK_ELSE(
      buff->write(&persisted_size, sizeof(uint64_t)), delete buff);
  RETURN_NOT_OK_ELSE(buff->write(&tile_size, sizeof(uint64_t)), delete buff);
  RETURN_NOT_OK_ELSE(buff->write(&datatype, sizeof(char)), delete buff);
  RETURN_NOT_OK_ELSE(buff->write(&cell_size, sizeof(uint64_t)), delete buff);
  RETURN_NOT_OK_ELSE(buff->write(&compressor, sizeof(char)), delete buff);
  RETURN_NOT_OK_ELSE(buff->write(&compression_level, sizeof(int)), delete buff);

  // Write to file
  Status st = storage_manager_->write(uri_, buff);

  STATS_COUNTER_ADD(tileio_write_num_input_bytes, buff->size());
  STATS_COUNTER_ADD(tileio_write_num_bytes_written, buff->size());

  delete buff;

  return st;
}

}  // namespace sm
}  // namespace tiledb
