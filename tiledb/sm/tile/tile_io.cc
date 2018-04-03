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
#include "tiledb/sm/compressors/blosc_compressor.h"
#include "tiledb/sm/compressors/bzip_compressor.h"
#include "tiledb/sm/compressors/dd_compressor.h"
#include "tiledb/sm/compressors/gzip_compressor.h"
#include "tiledb/sm/compressors/lz4_compressor.h"
#include "tiledb/sm/compressors/rle_compressor.h"
#include "tiledb/sm/compressors/zstd_compressor.h"
#include "tiledb/sm/misc/logger.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MIN(a, b) ((a) < (b) ? (a) : (b))

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

TileIO::TileIO() {
  buffer_ = nullptr;
  file_size_ = 0;
  storage_manager_ = nullptr;
  uri_ = URI("");
}

TileIO::TileIO(StorageManager* storage_manager, const URI& uri)
    : storage_manager_(storage_manager)
    , uri_(uri) {
  file_size_ = 0;
  buffer_ = new Buffer();
}

TileIO::TileIO(
    StorageManager* storage_manager, const URI& uri, uint64_t file_size)
    : file_size_(file_size)
    , storage_manager_(storage_manager)
    , uri_(uri) {
  buffer_ = new Buffer();
}

TileIO::~TileIO() {
  delete buffer_;
}

/* ****************************** */
/*               API              */
/* ****************************** */

uint64_t TileIO::file_size() const {
  return file_size_;
}

Status TileIO::read(
    Tile* tile,
    uint64_t file_offset,
    uint64_t compressed_size,
    uint64_t tile_size) {
  // Try to read from cache
  bool in_cache;
  RETURN_NOT_OK(storage_manager_->read_from_cache(
      uri_, file_offset, tile->buffer(), tile_size, &in_cache));
  if (in_cache)
    return Status::Ok();

  // No compression
  if (tile->compressor() == Compressor::NO_COMPRESSION) {
    RETURN_NOT_OK(
        storage_manager_->read(uri_, file_offset, tile->buffer(), tile_size));
  } else {  // Compression
    RETURN_NOT_OK(
        storage_manager_->read(uri_, file_offset, buffer_, compressed_size));

    // Decompress tile
    tile->reset_offset();
    tile->reset_size();
    buffer_->reset_offset();
    RETURN_NOT_OK(tile->realloc(tile_size));
    RETURN_NOT_OK(decompress_tile(tile));
    tile->reset_offset();
  }

  // Store tile in cache
  return (storage_manager_->write_to_cache(uri_, file_offset, tile->buffer()));
}

Status TileIO::read_generic(Tile** tile, uint64_t file_offset) {
  uint64_t tile_size;
  uint64_t compressed_size;
  uint64_t header_size;

  RETURN_NOT_OK(read_generic_tile_header(
      tile, file_offset, &tile_size, &compressed_size, &header_size));
  RETURN_NOT_OK_ELSE(
      read(*tile, file_offset + header_size, compressed_size, tile_size),
      delete *tile);

  return Status::Ok();
}

Status TileIO::read_generic_tile_header(
    Tile** tile,
    uint64_t file_offset,
    uint64_t* tile_size,
    uint64_t* compressed_size,
    uint64_t* header_size) {
  // Initializations
  *header_size = 3 * sizeof(uint64_t) + 2 * sizeof(char) + sizeof(int);
  char datatype;
  uint64_t cell_size;
  char compressor;
  int compression_level;

  // Read header from file
  auto header_buff = new Buffer();
  RETURN_NOT_OK_ELSE(
      storage_manager_->read(uri_, file_offset, header_buff, *header_size),
      delete header_buff);

  // Read header individual values
  RETURN_NOT_OK_ELSE(
      header_buff->read(compressed_size, sizeof(uint64_t)), delete header_buff);
  RETURN_NOT_OK_ELSE(
      header_buff->read(tile_size, sizeof(uint64_t)), delete header_buff);
  RETURN_NOT_OK_ELSE(
      header_buff->read(&datatype, sizeof(char)), delete header_buff);
  RETURN_NOT_OK_ELSE(
      header_buff->read(&cell_size, sizeof(uint64_t)), delete header_buff);
  RETURN_NOT_OK_ELSE(
      header_buff->read(&compressor, sizeof(char)), delete header_buff);
  RETURN_NOT_OK_ELSE(
      header_buff->read(&compression_level, sizeof(int)), delete header_buff);

  delete header_buff;

  *tile = new Tile();
  RETURN_NOT_OK_ELSE(
      (*tile)->init((Datatype)datatype, (Compressor)compressor, cell_size, 0),
      delete tile);

  return Status::Ok();
}

Status TileIO::write(Tile* tile, uint64_t* bytes_written) {
  // Reset the tile and buffer offset
  tile->reset_offset();
  buffer_->reset_size();
  buffer_->reset_offset();

  // Compress tile
  Compressor compressor = tile->compressor();
  if (compressor != Compressor::NO_COMPRESSION)
    RETURN_NOT_OK(compress_tile(tile));

  // Prepare to write
  auto buffer =
      (compressor == Compressor::NO_COMPRESSION) ? tile->buffer() : buffer_;
  *bytes_written = buffer->size();

  RETURN_NOT_OK(storage_manager_->write(uri_, buffer));

  return Status::Ok();
}

Status TileIO::write_generic(Tile* tile) {
  // Reset the tile and buffer offset
  tile->reset_offset();
  buffer_->reset_size();
  buffer_->reset_offset();

  // Compress tile
  Compressor compressor = tile->compressor();
  if (compressor != Compressor::NO_COMPRESSION)
    RETURN_NOT_OK(compress_tile(tile));

  auto buffer =
      (compressor == Compressor::NO_COMPRESSION) ? tile->buffer() : buffer_;

  RETURN_NOT_OK(write_generic_tile_header(tile, buffer->size()));
  RETURN_NOT_OK(storage_manager_->write(uri_, buffer));

  return Status::Ok();
}

Status TileIO::write_generic_tile_header(Tile* tile, uint64_t compressed_size) {
  // Initializations
  uint64_t tile_size = tile->size();
  auto datatype = (char)tile->type();
  uint64_t cell_size = tile->cell_size();
  auto compressor = (char)tile->compressor();
  int compression_level = tile->compression_level();

  // Write to buffer
  auto buff = new Buffer();
  RETURN_NOT_OK_ELSE(
      buff->write(&compressed_size, sizeof(uint64_t)), delete buff);
  RETURN_NOT_OK_ELSE(buff->write(&tile_size, sizeof(uint64_t)), delete buff);
  RETURN_NOT_OK_ELSE(buff->write(&datatype, sizeof(char)), delete buff);
  RETURN_NOT_OK_ELSE(buff->write(&cell_size, sizeof(uint64_t)), delete buff);
  RETURN_NOT_OK_ELSE(buff->write(&compressor, sizeof(char)), delete buff);
  RETURN_NOT_OK_ELSE(buff->write(&compression_level, sizeof(int)), delete buff);

  // Write to file
  Status st = storage_manager_->write(uri_, buff);

  delete buff;

  return st;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status TileIO::compress_tile(Tile* tile) {
  // Simple case - No coordinates
  if (!tile->stores_coords())
    return compress_one_tile(tile);

  // Split coordinates
  tile->split_coordinates();

  // Compress each dimension tile
  auto dim_num = tile->dim_num();
  auto dim_tile_size = tile->size() / dim_num;
  auto coord_size = tile->cell_size() / dim_num;
  Status st;
  for (unsigned int i = 0; i < dim_num; ++i) {
    auto buff = new Buffer(tile->cur_data(), dim_tile_size, false);
    auto dim_tile = new Tile(
        tile->type(),
        tile->compressor(),
        tile->compression_level(),
        coord_size,
        dim_num,
        buff,
        false);
    st = compress_one_tile(dim_tile);
    delete buff;
    delete dim_tile;
    RETURN_NOT_OK(st);
    tile->advance_offset(dim_tile_size);
  }

  return Status::Ok();
}

Status TileIO::compress_one_tile(Tile* tile) {
  // For easy reference
  auto level = tile->compression_level();
  auto type_size = datatype_size(tile->type());
  auto compressor = tile->compressor();
  auto type = tile->type();
  auto cell_size = tile->cell_size();
  auto tile_size = tile->size();

  // Compute necessary info for chunking
  uint64_t chunk_num, max_chunk_size, overhead;
  RETURN_NOT_OK(
      compute_chunking_info(tile, &chunk_num, &max_chunk_size, &overhead));

  // Properly reallocate buffer
  RETURN_NOT_OK(buffer_->realloc(buffer_->size() + tile_size + overhead));

  // Write number of chunks
  RETURN_NOT_OK(buffer_->write(&chunk_num, sizeof(uint64_t)));

  // Compress in chunks
  Status st;
  uint64_t compressed_chunk_size = 0;
  uint64_t left_to_compress = tile_size;
  for (uint64_t i = 0; i < chunk_num; ++i) {
    // Write chunk info
    auto chunk_size = MIN(left_to_compress, max_chunk_size);

    RETURN_NOT_OK(buffer_->write(&chunk_size, sizeof(uint64_t)));
    uint64_t buffer_offset = buffer_->offset();  // Will be used later
    RETURN_NOT_OK(buffer_->write(&compressed_chunk_size, sizeof(uint64_t)));

    // Create const buffer
    auto input_buffer = new ConstBuffer(tile->cur_data(), chunk_size);

    // Invoke the proper compressor
    switch (compressor) {
      case Compressor::GZIP:
        st = GZip::compress(level, input_buffer, buffer_);
        break;
      case Compressor::ZSTD:
        st = ZStd::compress(level, input_buffer, buffer_);
        break;
      case Compressor::LZ4:
        st = LZ4::compress(level, input_buffer, buffer_);
        break;
      case Compressor::BLOSC_LZ:
        st =
            Blosc::compress("blosclz", type_size, level, input_buffer, buffer_);
        break;
#undef BLOSC_LZ4
      case Compressor::BLOSC_LZ4:
        st = Blosc::compress("lz4", type_size, level, input_buffer, buffer_);
        break;
#undef BLOSC_LZ4HC
      case Compressor::BLOSC_LZ4HC:
        st = Blosc::compress("lz4hc", type_size, level, input_buffer, buffer_);
        break;
#undef BLOSC_SNAPPY
      case Compressor::BLOSC_SNAPPY:
        st = Blosc::compress("snappy", type_size, level, input_buffer, buffer_);
        break;
#undef BLOSC_ZLIB
      case Compressor::BLOSC_ZLIB:
        st = Blosc::compress("zlib", type_size, level, input_buffer, buffer_);
        break;
#undef BLOSC_ZSTD
      case Compressor::BLOSC_ZSTD:
        st = Blosc::compress("zstd", type_size, level, input_buffer, buffer_);
        break;
      case Compressor::RLE:
        st = RLE::compress(cell_size, input_buffer, buffer_);
        break;
      case Compressor::BZIP2:
        st = BZip::compress(level, input_buffer, buffer_);
        break;
      case Compressor::DOUBLE_DELTA:
        st = DoubleDelta::compress(type, input_buffer, buffer_);
        break;
      default:
        assert(0);
    }

    delete input_buffer;
    RETURN_NOT_OK(st);

    // Write compressed chunk size
    compressed_chunk_size =
        buffer_->size() - (buffer_offset + sizeof(uint64_t));
    std::memcpy(
        buffer_->data(buffer_offset), &compressed_chunk_size, sizeof(uint64_t));

    // Update
    left_to_compress -= chunk_size;
    tile->advance_offset(chunk_size);
  }

  assert(left_to_compress == 0);

  return Status::Ok();
}

Status TileIO::compute_chunking_info(
    Tile* tile,
    uint64_t* chunk_num,
    uint64_t* max_chunk_size,
    uint64_t* overhead) {
  // For easy reference
  auto cell_size = tile->cell_size();
  auto tile_size = tile->size();

  // Compute max chunk size
  *max_chunk_size = MIN(constants::tile_chunk_size, tile_size);
  *max_chunk_size = *max_chunk_size / cell_size * cell_size;
  uint64_t chunk_overhead = this->overhead(tile, *max_chunk_size);

  // Adjust max chunk size
  if (*max_chunk_size + chunk_overhead > constants::tile_chunk_size) {
    *max_chunk_size -= chunk_overhead;
    *max_chunk_size = (*max_chunk_size) / cell_size * cell_size;
    chunk_overhead = this->overhead(tile, *max_chunk_size);
  }

  // Handle special error
  if (*max_chunk_size + chunk_overhead > constants::tile_chunk_size) {
    return LOG_STATUS(
        Status::TileIOError("Compute chunking info failed; Consider adjusting "
                            "constants::tile_chunk_size"));
  }

  // Compute number of chunks
  *chunk_num = tile_size / (*max_chunk_size) +
               uint64_t(bool(tile_size % (*max_chunk_size)));

  // Compute overhead: equal to the compression overhead per chunk, plus 2
  // values per chunk that store the original and compressed chunk size,
  // plus a single value in the beginning for the total number of chunks.
  *overhead =
      (*chunk_num) * chunk_overhead * 2 * sizeof(uint64_t) + sizeof(uint64_t);

  return Status::Ok();
}

Status TileIO::decompress_tile(Tile* tile) {
  // Simple case - No coordinates
  if (!tile->stores_coords())
    return decompress_one_tile(tile);

  // Decompress each dimension tile
  auto dim_num = tile->dim_num();
  for (unsigned int i = 0; i < dim_num; ++i)
    RETURN_NOT_OK(decompress_one_tile(tile));

  // Zip coordinates
  tile->zip_coordinates();

  return Status::Ok();
}

Status TileIO::decompress_one_tile(Tile* tile) {
  // Read number of chunks
  uint64_t chunk_num;

  RETURN_NOT_OK(buffer_->read(&chunk_num, sizeof(uint64_t)));
  assert(chunk_num > 0);

  Status st;
  Datatype type = tile->type();
  for (uint64_t i = 0; i < chunk_num; ++i) {
    // Read original and compressed chunk size
    uint64_t chunk_size, compressed_chunk_size;
    RETURN_NOT_OK(buffer_->read(&chunk_size, sizeof(uint64_t)));
    RETURN_NOT_OK(buffer_->read(&compressed_chunk_size, sizeof(uint64_t)));

    auto input_buffer =
        new ConstBuffer(buffer_->cur_data(), compressed_chunk_size);

    // Invoke the proper decompressor
    switch (tile->compressor()) {
      case Compressor::NO_COMPRESSION:
        assert(0);
        break;
      case Compressor::GZIP:
        st = GZip::decompress(input_buffer, tile->buffer());
        break;
      case Compressor::ZSTD:
        st = ZStd::decompress(input_buffer, tile->buffer());
        break;
      case Compressor::LZ4:
        st = LZ4::decompress(input_buffer, tile->buffer());
        break;
      case Compressor::BLOSC_LZ:
#undef BLOSC_LZ4
      case Compressor::BLOSC_LZ4:
#undef BLOSC_LZ4HC
      case Compressor::BLOSC_LZ4HC:
#undef BLOSC_SNAPPY
      case Compressor::BLOSC_SNAPPY:
#undef BLOSC_ZLIB
      case Compressor::BLOSC_ZLIB:
#undef BLOSC_ZSTD
      case Compressor::BLOSC_ZSTD:
        st = Blosc::decompress(input_buffer, tile->buffer());
        break;
      case Compressor::RLE:
        st = RLE::decompress(tile->cell_size(), input_buffer, tile->buffer());
        break;
      case Compressor::BZIP2:
        st = BZip::decompress(input_buffer, tile->buffer());
        break;
      case Compressor::DOUBLE_DELTA:
        st = DoubleDelta::decompress(type, input_buffer, tile->buffer());
        break;
    }

    delete input_buffer;
    RETURN_NOT_OK(st);

    buffer_->advance_offset(compressed_chunk_size);
  }

  return st;
}

uint64_t TileIO::overhead(Tile* tile, uint64_t nbytes) const {
  switch (tile->compressor()) {
    case Compressor::GZIP:
      return GZip::overhead(nbytes);
    case Compressor::ZSTD:
      return ZStd::overhead(nbytes);
    case Compressor::LZ4:
      return LZ4::overhead(nbytes);
    case Compressor::BLOSC_LZ:
#undef BLOSC_LZ4
    case Compressor::BLOSC_LZ4:
#undef BLOSC_LZ4HC
    case Compressor::BLOSC_LZ4HC:
#undef BLOSC_SNAPPY
    case Compressor::BLOSC_SNAPPY:
#undef BLOSC_ZLIB
    case Compressor::BLOSC_ZLIB:
#undef BLOSC_ZSTD
    case Compressor::BLOSC_ZSTD:
      return Blosc::overhead(nbytes);
    case Compressor::RLE:
      return RLE::overhead(nbytes, tile->cell_size());
    case Compressor::BZIP2:
      return BZip::overhead(nbytes);
    case Compressor::DOUBLE_DELTA:
      return DoubleDelta::overhead(nbytes);
    default:
      // No compression
      return 0;
  }
}

}  // namespace sm
}  // namespace tiledb
