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
#include "tiledb/sm/misc/parallel_functions.h"

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

bool TileIO::can_compress_nbytes(uint64_t nbytes) const {
  // Conservatively, some compressors can only compress up to int32 max, so
  // use that as the limit for all compressors.
  auto limit = static_cast<uint64_t>(std::numeric_limits<int32_t>::max());
  return nbytes <= limit;
}

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
  uint64_t chunk_num, chunk_size, chunk_overhead, total_overhead;
  RETURN_NOT_OK(compute_chunking_info(
      tile, &chunk_num, &chunk_size, &chunk_overhead, &total_overhead));
  if (chunk_num == 0)
    return LOG_STATUS(
        Status::TileIOError("Compressed tile would have 0 chunks."));

  // Compress each chunk in parallel.
  std::vector<Buffer> chunk_buffers(chunk_num);
  auto tile_data = reinterpret_cast<const char*>(tile->cur_data());
  auto statuses = parallel_for(0, chunk_num, [&](uint64_t i) {
    auto& chunk_buffer = chunk_buffers[i];
    uint64_t compressed_chunk_size = 0;
    auto this_chunk_size = chunk_size;
    auto tile_data_offset = i * chunk_size;
    // The last chunk may be smaller when the chunk size does not evenly divide
    // the tile size.
    if (i == chunk_num - 1)
      this_chunk_size = tile_size - (chunk_num - 1) * chunk_size;

    // Pre-allocate enough space for the compression function.
    chunk_buffer.realloc(
        this_chunk_size + 2 * sizeof(uint64_t) + chunk_overhead);

    RETURN_NOT_OK(chunk_buffer.write(&this_chunk_size, sizeof(uint64_t)));
    uint64_t compressed_size_offset =
        chunk_buffer.offset();  // Will be used later
    RETURN_NOT_OK(chunk_buffer.write(&compressed_chunk_size, sizeof(uint64_t)));

    // Create const buffer
    ConstBuffer input_buffer(tile_data + tile_data_offset, this_chunk_size);

    // Invoke the proper compressor
    Status st = Status::Ok();
    switch (compressor) {
      case Compressor::GZIP:
        st = GZip::compress(level, &input_buffer, &chunk_buffer);
        break;
      case Compressor::ZSTD:
        st = ZStd::compress(level, &input_buffer, &chunk_buffer);
        break;
      case Compressor::LZ4:
        st = LZ4::compress(level, &input_buffer, &chunk_buffer);
        break;
      case Compressor::BLOSC_LZ:
        st = Blosc::compress(
            "blosclz", type_size, level, &input_buffer, &chunk_buffer);
        break;
#undef BLOSC_LZ4
      case Compressor::BLOSC_LZ4:
        st = Blosc::compress(
            "lz4", type_size, level, &input_buffer, &chunk_buffer);
        break;
#undef BLOSC_LZ4HC
      case Compressor::BLOSC_LZ4HC:
        st = Blosc::compress(
            "lz4hc", type_size, level, &input_buffer, &chunk_buffer);
        break;
#undef BLOSC_SNAPPY
      case Compressor::BLOSC_SNAPPY:
        st = Blosc::compress(
            "snappy", type_size, level, &input_buffer, &chunk_buffer);
        break;
#undef BLOSC_ZLIB
      case Compressor::BLOSC_ZLIB:
        st = Blosc::compress(
            "zlib", type_size, level, &input_buffer, &chunk_buffer);
        break;
#undef BLOSC_ZSTD
      case Compressor::BLOSC_ZSTD:
        st = Blosc::compress(
            "zstd", type_size, level, &input_buffer, &chunk_buffer);
        break;
      case Compressor::RLE:
        st = RLE::compress(cell_size, &input_buffer, &chunk_buffer);
        break;
      case Compressor::BZIP2:
        st = BZip::compress(level, &input_buffer, &chunk_buffer);
        break;
      case Compressor::DOUBLE_DELTA:
        st = DoubleDelta::compress(type, &input_buffer, &chunk_buffer);
        break;
      default:
        assert(0);
    }

    RETURN_NOT_OK(st);

    // Write compressed chunk size
    compressed_chunk_size = chunk_buffer.size() - 2 * sizeof(uint64_t);
    std::memcpy(
        chunk_buffer.data(compressed_size_offset),
        &compressed_chunk_size,
        sizeof(uint64_t));

    return Status::Ok();
  });

  // Check compression status
  for (const auto& st : statuses)
    RETURN_NOT_OK(st);

  // Advance tile buffer offset
  tile->advance_offset(tile->size());

  // Properly reallocate buffer
  RETURN_NOT_OK(buffer_->realloc(buffer_->size() + tile_size + total_overhead));

  // Write number of chunks
  RETURN_NOT_OK(buffer_->write(&chunk_num, sizeof(uint64_t)));

  // Compute compressed chunk offsets in total buffer.
  std::vector<uint64_t> chunk_dest(chunk_num);
  uint64_t buffer_offset = buffer_->offset();
  for (uint64_t i = 0; i < chunk_num; ++i) {
    chunk_dest[i] = buffer_offset;
    buffer_offset += chunk_buffers[i].size();
  }

  // Concatenate compressed chunks into buffer in parallel.
  auto buffer_data = reinterpret_cast<char*>(buffer_->data());
  parallel_for(
      0, chunk_num, [&chunk_buffers, &chunk_dest, &buffer_data](uint64_t i) {
        auto& chunk_buffer = chunk_buffers[i];
        std::memcpy(
            buffer_data + chunk_dest[i],
            chunk_buffer.data(),
            chunk_buffer.size());
        return Status::Ok();
      });

  // Advance buffer offset and size appropriately.
  uint64_t buffer_increase = buffer_offset - buffer_->offset();
  buffer_->advance_offset(buffer_increase);
  buffer_->advance_size(buffer_increase);

  return Status::Ok();
}

Status TileIO::compute_chunking_info(
    Tile* tile,
    uint64_t* chunk_num,
    uint64_t* chunk_size,
    uint64_t* chunk_overhead,
    uint64_t* total_overhead) {
  // For easy reference
  auto cell_size = tile->cell_size();
  auto tile_size = tile->size();

  // Compute a chunk size as a multiple of the cell size.
  *chunk_size = MIN(constants::max_tile_chunk_size, tile_size);
  *chunk_size = *chunk_size / cell_size * cell_size;
  *chunk_overhead = this->overhead(tile, *chunk_size);

  // Check valid total size
  if (!can_compress_nbytes(*chunk_size + *chunk_overhead))
    return LOG_STATUS(Status::TileIOError(
        "Cannot compress a chunk of size " + std::to_string(*chunk_size) +
        " with overhead " + std::to_string(*chunk_overhead)));

  // Compute number of chunks
  *chunk_num =
      tile_size / (*chunk_size) + uint64_t(bool(tile_size % (*chunk_size)));

  // Compute total overhead: equal to the compression total_overhead per chunk,
  // plus 2 values per chunk that store the original and compressed chunk size,
  // plus a single value in the beginning for the total number of chunks.
  *total_overhead = (*chunk_num) * (*chunk_overhead) * 2 * sizeof(uint64_t) +
                    sizeof(uint64_t);

  return Status::Ok();
}

Status TileIO::compute_decompression_chunk_info(
    Tile* tile, DecompressionChunkInfo* info) {
  // Read number of chunks
  RETURN_NOT_OK(buffer_->read(&info->chunk_num_, sizeof(uint64_t)));
  if (info->chunk_num_ == 0) {
    return LOG_STATUS(Status::TileIOError("Tile has 0 chunks."));
  }

  // Skip the size and compressed size (uint64_t) values to get the pointer
  // to the first chunk's compressed data.
  info->compressed_chunks_ =
      reinterpret_cast<const char*>(buffer_->cur_data()) + 2 * sizeof(uint64_t);
  // Decompressed chunks will be stored in the tile buffer.
  info->decompressed_chunks_ =
      reinterpret_cast<char*>(tile->buffer()->cur_data());

  // Compute chunk source and destination offsets in the input and output
  // buffers.
  info->compressed_chunk_info_.resize(info->chunk_num_);
  info->decompressed_chunk_info_.resize(info->chunk_num_);
  info->total_decompressed_bytes_ = 0;
  for (uint64_t i = 0, chunk_offset = 0; i < info->chunk_num_; i++) {
    // Read original and compressed chunk size
    uint64_t chunk_size, compressed_chunk_size;
    RETURN_NOT_OK(buffer_->read(&chunk_size, sizeof(uint64_t)));
    RETURN_NOT_OK(buffer_->read(&compressed_chunk_size, sizeof(uint64_t)));
    buffer_->advance_offset(compressed_chunk_size);

    info->compressed_chunk_info_[i] =
        std::make_pair(chunk_offset, compressed_chunk_size);
    info->decompressed_chunk_info_[i] =
        std::make_pair(info->total_decompressed_bytes_, chunk_size);

    chunk_offset += 2 * sizeof(uint64_t) + compressed_chunk_size;
    info->total_decompressed_bytes_ += chunk_size;
  }

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
  DecompressionChunkInfo info;
  RETURN_NOT_OK(compute_decompression_chunk_info(tile, &info));

  auto statuses = parallel_for(0, info.chunk_num_, [&tile, &info](uint64_t i) {
    // Get source/dest buffer information.
    auto compressed_info = info.compressed_chunk_info_[i];
    auto decompressed_info = info.decompressed_chunk_info_[i];
    auto src = info.compressed_chunks_ + compressed_info.first;
    auto src_len = compressed_info.second;
    auto dest = info.decompressed_chunks_ + decompressed_info.first;
    auto dest_len = decompressed_info.second;

    ConstBuffer input_buffer(src, src_len);
    PreallocatedBuffer output_buffer(dest, dest_len);
    Status st = Status::Ok();
    Datatype type = tile->type();

    // Invoke the proper decompressor
    switch (tile->compressor()) {
      case Compressor::NO_COMPRESSION:
        assert(0);
        break;
      case Compressor::GZIP:
        st = GZip::decompress(&input_buffer, &output_buffer);
        break;
      case Compressor::ZSTD:
        st = ZStd::decompress(&input_buffer, &output_buffer);
        break;
      case Compressor::LZ4:
        st = LZ4::decompress(&input_buffer, &output_buffer);
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
        st = Blosc::decompress(&input_buffer, &output_buffer);
        break;
      case Compressor::RLE:
        st = RLE::decompress(tile->cell_size(), &input_buffer, &output_buffer);
        break;
      case Compressor::BZIP2:
        st = BZip::decompress(&input_buffer, &output_buffer);
        break;
      case Compressor::DOUBLE_DELTA:
        st = DoubleDelta::decompress(type, &input_buffer, &output_buffer);
        break;
    }

    return st;
  });

  // Check all statuses
  for (const auto& st : statuses)
    RETURN_NOT_OK(st);

  tile->buffer()->advance_size(info.total_decompressed_bytes_);
  tile->buffer()->advance_offset(info.total_decompressed_bytes_);

  return Status::Ok();
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
