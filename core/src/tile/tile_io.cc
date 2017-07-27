/**
 * @file   tile_io.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include "tile_io.h"
#include "blosc_compressor.h"
#include "bzip_compressor.h"
#include "filesystem.h"
#include "gzip_compressor.h"
#include "logger.h"
#include "lz4_compressor.h"
#include "rle_compressor.h"
#include "utils.h"
#include "zstd_compressor.h"

#include <iostream>

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

TileIO::TileIO(
    const Configurator* config,
    const uri::URI& fragment_name,
    const Attribute* attr)
    : attr_(attr)
    , config_(config) {
  attr_filename_ = fragment_name.join_path(
      "/" + attr_->name() + Configurator::file_suffix());
  buffer_ = nullptr;
  buffer_size_ = 0;
  buffer_size_alloced_ = 0;
}

TileIO::~TileIO() {
  if (buffer_ != nullptr)
    free(buffer_);
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status TileIO::write(Tile* tile, uint64_t* bytes_written) {
  // Compress tile
  // TODO: do not compress if not needed
  // TODO: here we will put all other filters
  // TODO: here we may have to chunk the tile before compression

  RETURN_NOT_OK(compress_tile(tile));

  // Prepare to write
  IOMethod write_method = config_->write_method();
  void* buffer = (buffer_size_ == 0) ? tile->buffer() : buffer_;
  uint64_t buffer_size =
      (buffer_size_ == 0) ? tile->buffer_size() : buffer_size_;
  *bytes_written = buffer_size;

  // Write based on the chosen method
  // TODO: implement mmap write here
  if (write_method == IOMethod::WRITE) {
    return filesystem::write_to_file(
        attr_filename_.to_posix_path(), buffer, buffer_size);
  } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
    return filesystem::mpi_io_write_to_file(
        config_->mpi_comm(),
        attr_filename_.to_posix_path().c_str(),
        buffer,
        buffer_size);
#else
    // Error: MPI not supported
    return LOG_STATUS(
        Status::TileIOError("Cannot write tile; MPI not supported"));
#endif
  } else {
    return LOG_STATUS(
        Status::TileIOError("Cannot write tile; Unknown write method"));
  }
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status TileIO::compress_tile(Tile* tile) {
  // For easy reference
  Compressor compression = attr_->compressor();
  int level = attr_->compression_level();

  // Reset size of written data in the buffer
  buffer_size_ = 0;

  // Handle different compression
  switch (compression) {
    case Compressor::NO_COMPRESSION:
      return Status::Ok();
    case Compressor::GZIP:
      return compress_tile_gzip(tile, level);
    case Compressor::ZSTD:
      return compress_tile_zstd(tile, level);
    case Compressor::LZ4:
      return compress_tile_lz4(tile, level);
    case Compressor::BLOSC:
      return compress_tile_blosc(tile, level, "blosclz");
#undef BLOSC_LZ4
    case Compressor::BLOSC_LZ4:
      return compress_tile_blosc(tile, level, "lz4");
#undef BLOSC_LZ4HC
    case Compressor::BLOSC_LZ4HC:
      return compress_tile_blosc(tile, level, "lz4hc");
#undef BLOSC_SNAPPY
    case Compressor::BLOSC_SNAPPY:
      return compress_tile_blosc(tile, level, "snappy");
#undef BLOSC_ZLIB
    case Compressor::BLOSC_ZLIB:
      return compress_tile_blosc(tile, level, "zlib");
#undef BLOSC_ZSTD
    case Compressor::BLOSC_ZSTD:
      return compress_tile_blosc(tile, level, "zstd");
    case Compressor::RLE:
      return compress_tile_rle(tile, level);
    case Compressor::BZIP2:
      return compress_tile_bzip2(tile, level);
  }
}

Status TileIO::compress_tile_gzip(Tile* tile, int level) {
  // TODO: handle variable-sized tiles

  // Allocate space to store the compressed tile
  uint64_t tile_size = tile->buffer_size();
  uint64_t gzip_overhead = GZip::overhead(tile_size);
  if (buffer_ == nullptr) {
    buffer_size_alloced_ = tile_size + gzip_overhead;
    buffer_ = malloc(buffer_size_alloced_);
  }

  // Expand compressed tile if necessary
  if (tile_size + gzip_overhead > buffer_size_alloced_) {
    buffer_size_alloced_ = tile_size + gzip_overhead;
    buffer_ = realloc(buffer_, buffer_size_alloced_);
  }

  /* // TODO: handle coordinates
// Split dimensions
if (coords)
  utils::split_coordinates(tile, tile_size, dim_num, coords_size);
*/

  // Compress tile
  size_t buffer_size;  // TODO: remove this, changing the compress signature
  RETURN_NOT_OK(GZip::compress(
      utils::datatype_size(attr_->type()),
      level,
      tile->buffer(),
      tile_size,
      buffer_,
      buffer_size_alloced_,
      &buffer_size));
  buffer_size_ = buffer_size;

  return Status::Ok();
}

Status TileIO::compress_tile_zstd(Tile* tile, int level) {
  // TODO: handle variable-sized tiles

  // Allocate space to store the compressed tile
  uint64_t tile_size = tile->buffer_size();
  size_t compress_bound = ZStd::compress_bound(tile_size);

  if (buffer_ == nullptr) {
    buffer_size_alloced_ = compress_bound;
    buffer_ = malloc(buffer_size_alloced_);
  }

  // Expand compressed tile if necessary
  if (compress_bound > buffer_size_alloced_) {
    buffer_size_alloced_ = compress_bound;
    buffer_ = realloc(buffer_, buffer_size_alloced_);
  }

  /* TODO: handle coordinates.
// Split dimensions
if (coords)
  utils::split_coordinates(tile, tile_size, dim_num, coords_size);
   */

  size_t buffer_size;  // TODO: remove this, changing the compress signature
  RETURN_NOT_OK(ZStd::compress(
      utils::datatype_size(attr_->type()),
      level,
      tile->buffer(),
      tile_size,
      buffer_,
      buffer_size_alloced_,
      &buffer_size));
  buffer_size_ = buffer_size;

  return Status::Ok();
}

Status TileIO::compress_tile_lz4(Tile* tile, int level) {
  // TODO: handle variable-sized tiles

  // Allocate space to store the compressed tile
  uint64_t tile_size = tile->buffer_size();
  size_t compress_bound = LZ4::compress_bound(tile_size);

  if (buffer_ == nullptr) {
    buffer_size_alloced_ = compress_bound;
    buffer_ = malloc(buffer_size_alloced_);
  }

  // Expand compressed tile if necessary
  if (compress_bound > buffer_size_alloced_) {
    buffer_size_alloced_ = compress_bound;
    buffer_ = realloc(buffer_, buffer_size_alloced_);
  }

  /* TODO: handle coordinates.
// Split dimensions
if (coords)
  utils::split_coordinates(tile, tile_size, dim_num, coords_size);
   */

  // Compress tile
  size_t buffer_size;  // TODO: remove this, changing the compress signature
  RETURN_NOT_OK(LZ4::compress(
      utils::datatype_size(attr_->type()),
      level,
      tile->buffer(),
      tile_size,
      buffer_,
      buffer_size_alloced_,
      &buffer_size));
  buffer_size_ = buffer_size;

  // Success
  return Status::Ok();
}

Status TileIO::compress_tile_blosc(
    Tile* tile, int level, const char* compressor) {
  // TODO: handle variable-sized tiles

  // Allocate space to store the compressed tile
  uint64_t tile_size = tile->buffer_size();
  size_t compress_bound = Blosc::compress_bound(tile_size);

  if (buffer_ == nullptr) {
    buffer_size_alloced_ = compress_bound;
    buffer_ = malloc(buffer_size_alloced_);
  }

  // Expand compressed tile if necessary
  if (compress_bound > buffer_size_alloced_) {
    buffer_size_alloced_ = compress_bound;
    buffer_ = realloc(buffer_, buffer_size_alloced_);
  }

  /* TODO: handle coordinates
// Split dimensions
if (coords)
  utils::split_coordinates(tile, tile_size, dim_num, coords_size);
   */

  // Compress tile
  size_t buffer_size;  // TODO: remove this, changing the compress signature
  RETURN_NOT_OK(Blosc::compress(
      compressor,
      utils::datatype_size(attr_->type()),
      level,
      tile->buffer(),
      tile_size,
      buffer_,
      buffer_size_alloced_,
      &buffer_size));
  buffer_size_ = buffer_size;

  return Status::Ok();
}

Status TileIO::compress_tile_rle(Tile* tile, int level) {
  // TODO: handle variable-sized tiles

  // For easy reference

  /*
  Layout order = array_schema->cell_order();
  bool is_coords = false; // TODO: handle coordinates
  size_t value_size = (array_schema->var_size(attribute_id) || is_coords) ?
                          array_schema->type_size(attribute_id) :
                          array_schema->cell_size(attribute_id);
                          */

  // Allocate space to store the compressed tile
  /* TODO: handle coordinates
  if (!is_coords)
    compress_bound = RLE::compress_bound(tile_size, value_size);
  else
    compress_bound =
        utils::RLE_compress_bound_coords(tile_size, value_size, dim_num);
        */

  uint64_t tile_size = tile->buffer_size();
  uint64_t value_size = attr_->cell_size();
  size_t compress_bound = RLE::compress_bound(tile_size, value_size);

  if (buffer_ == nullptr) {
    buffer_size_alloced_ = compress_bound;
    buffer_ = malloc(buffer_size_alloced_);
  }

  // Expand compressed tile if necessary
  if (compress_bound > buffer_size_alloced_) {
    buffer_size_alloced_ = compress_bound;
    buffer_ = realloc(buffer_, buffer_size_alloced_);
  }

  // Compress tile
  size_t rle_size = 0;
  int64_t rle_coords_size = -1;
  RETURN_NOT_OK(RLE::compress(
      value_size,
      level,
      tile->buffer(),
      tile_size,
      buffer_,
      buffer_size_alloced_,
      &rle_size));
  buffer_size_ = rle_size;

  /* TODO: handle coordinates
if(is_coords) {
if (order == Layout::ROW_MAJOR) {
  RETURN_NOT_OK(utils::RLE_compress_coords_row(
      tile,
      tile_size,
      (unsigned char*)tile_compressed_,
      tile_compressed_allocated_size_,
      value_size,
      dim_num,
      &rle_coords_size));
} else if (order == Layout::COL_MAJOR) {
  RETURN_NOT_OK(utils::RLE_compress_coords_col(
      tile,
      tile_size,
      (unsigned char*)tile_compressed_,
      tile_compressed_allocated_size_,
      value_size,
      dim_num,
      &rle_coords_size));
} else {  // Error
  assert(0);
  return LOG_STATUS(
      Status::Error("Failed compressing with RLE; unsupported cell order"));
}
}
   */

  // Set actual output size
  // TODO: this can overflow for 32 bit, no checking
  // TODO: handle coordinates
  /*
  if(is_coords)
    *tile_compressed_size = static_cast<size_t>(rle_coords_size);
*/

  // Success
  return Status::Ok();
}

Status TileIO::compress_tile_bzip2(Tile* tile, int level) {
  // TODO: handle variable-sized tiles

  std::cout << "BZIP2\n";

  // Allocate space to store the compressed tile
  uint64_t tile_size = tile->buffer_size();
  size_t compress_bound = BZip::compress_bound(tile_size);

  if (buffer_ == nullptr) {
    buffer_size_alloced_ = compress_bound;
    buffer_ = malloc(buffer_size_alloced_);
  }

  // Expand compressed tile if necessary
  if (compress_bound > buffer_size_alloced_) {
    buffer_size_alloced_ = compress_bound;
    buffer_ = realloc(buffer_, buffer_size_alloced_);
  }

  /* TODO: handle coordinates.
// Split dimensions
if (coords)
  utils::split_coordinates(tile, tile_size, dim_num, coords_size);
   */

  // Compress tile
  size_t buffer_size;  // TODO: remove this, changing the compress signature
  RETURN_NOT_OK(BZip::compress(
      utils::datatype_size(attr_->type()),
      level,
      tile->buffer(),
      tile_size,
      buffer_,
      buffer_size_alloced_,
      &buffer_size));
  buffer_size_ = buffer_size;

  // Success
  return Status::Ok();
}

}  // namespace tiledb
