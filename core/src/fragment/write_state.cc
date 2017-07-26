/**
 * @file   write_state.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file implements the WriteState class.
 */
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <iostream>

#include "blosc_compressor.h"
#include "bzip_compressor.h"
#include "comparators.h"
#include "const_buffer.h"
#include "filesystem.h"
#include "gzip_compressor.h"
#include "logger.h"
#include "lz4_compressor.h"
#include "rle_compressor.h"
#include "tile.h"
#include "utils.h"
#include "write_state.h"
#include "zstd_compressor.h"

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

WriteState::WriteState(const Fragment* fragment, BookKeeping* book_keeping)
    : book_keeping_(book_keeping)
    , fragment_(fragment) {
  // For easy reference
  const ArraySchema* array_schema = fragment->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  size_t coords_size = array_schema->coords_size();

  // Initialize the number of cells written in the current tile
  tile_cell_num_.resize(attribute_num + 1);
  for (int i = 0; i < attribute_num + 1; ++i)
    tile_cell_num_[i] = 0;

  // Initialize current tiles
  tiles_.resize(attribute_num + 1);
  for (int i = 0; i < attribute_num + 1; ++i)
    tiles_[i] = nullptr;
  Tiles_.resize(attribute_num + 1);
  for (int i = 0; i < attribute_num + 1; ++i)
    Tiles_[i] = new Tile(fragment_->tile_size(i));

  tile_io_.resize(attribute_num + 1);
  for (int i = 0; i < attribute_num; ++i)
    tile_io_[i] = new TileIO(
        fragment_->array()->config(),
        fragment_->fragment_uri(),
        array_schema->Attributes()[i]);

  // TODO: handle coordinates here

  // Initialize current variable tiles
  tiles_var_.resize(attribute_num);
  for (int i = 0; i < attribute_num; ++i)
    tiles_var_[i] = nullptr;

  // Initialize tile buffer used in compression
  tile_compressed_ = nullptr;
  tile_compressed_allocated_size_ = 0;

  // Initialize current tile offsets
  tile_offsets_.resize(attribute_num + 1);
  for (int i = 0; i < attribute_num + 1; ++i)
    tile_offsets_[i] = 0;

  // Initialize current variable tile offsets
  tiles_var_offsets_.resize(attribute_num);
  for (int i = 0; i < attribute_num; ++i)
    tiles_var_offsets_[i] = 0;

  // Initialize current variable tile sizes
  tiles_var_sizes_.resize(attribute_num);
  for (int i = 0; i < attribute_num; ++i)
    tiles_var_sizes_[i] = 0;

  // Initialize the current size of the variable attribute file
  buffer_var_offsets_.resize(attribute_num);
  for (int i = 0; i < attribute_num; ++i)
    buffer_var_offsets_[i] = 0;

  // Initialize current MBR
  mbr_ = malloc(2 * coords_size);

  // Initialize current bounding coordinates
  bounding_coords_ = malloc(2 * coords_size);
}

WriteState::~WriteState() {
  // Free current tiles
  int64_t tile_num = tiles_.size();
  for (int64_t i = 0; i < tile_num; ++i)
    if (tiles_[i] != nullptr)
      free(tiles_[i]);

  for (auto& tile : Tiles_)
    delete tile;

  for (auto& tile_io : tile_io_)
    delete tile_io;

  // Free current tiles
  int64_t tile_var_num = tiles_var_.size();
  for (int64_t i = 0; i < tile_var_num; ++i)
    if (tiles_var_[i] != nullptr)
      free(tiles_var_[i]);

  // Free current compressed tile buffer
  if (tile_compressed_ != nullptr)
    free(tile_compressed_);

  // Free current MBR
  if (mbr_ != nullptr)
    free(mbr_);

  // Free current bounding coordinates
  if (bounding_coords_ != nullptr)
    free(bounding_coords_);
}

/* ****************************** */
/*           MUTATORS             */
/* ****************************** */

Status WriteState::finalize() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Write last tile (applicable only to the sparse case)
  if (tile_cell_num_[attribute_num] != 0) {
    RETURN_NOT_OK(write_last_tile());
    tile_cell_num_[attribute_num] = 0;
  }

  // Sync all attributes
  RETURN_NOT_OK(sync());

  // Success
  return Status::Ok();
}

Status WriteState::sync() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  const std::vector<int>& attribute_ids = fragment_->array()->attribute_ids();
  IOMethod write_method = fragment_->array()->config()->write_method();
#ifdef HAVE_MPI
  MPI_Comm* mpi_comm = fragment_->array()->config()->mpi_comm();
#endif
  std::string filename;

  // Sync all attributes
  for (int attribute_id : attribute_ids) {
    // For all attributes
    filename = fragment_->fragment_uri()
                   .join_path(
                       array_schema->attribute(attribute_id) +
                       Configurator::file_suffix())
                   .to_posix_path();
    if (write_method == IOMethod::WRITE) {
      RETURN_NOT_OK(filesystem::sync(filename.c_str()));
    } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
      RETURN_NOT_OK(filesystem::mpi_io_sync(mpi_comm, filename.c_str()));
#else
      // Error: MPI not supported
      return LOG_STATUS(Status::Error("Cannot sync; MPI not supported"));
#endif
    } else {
      assert(0);
    }

    // Only for variable-size attributes (they have an extra file)
    if (array_schema->var_size(attribute_id)) {
      filename = fragment_->fragment_uri()
                     .join_path(
                         array_schema->attribute(attribute_id) + "_var" +
                         Configurator::file_suffix())
                     .to_posix_path();
      if (write_method == IOMethod::WRITE) {
        RETURN_NOT_OK(filesystem::sync(filename.c_str()));
      } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
        RETURN_NOT_OK(filesystem::mpi_io_sync(mpi_comm, filename.c_str()));
#else
        // Error: MPI not supported
        return LOG_STATUS(Status::Error("Cannot sync; MPI not supported"));
#endif
      } else {
        assert(0);
      }
    }
  }

  // Sync fragment directory
  filename = fragment_->fragment_uri().to_posix_path();
  if (write_method == IOMethod::WRITE) {
    RETURN_NOT_OK(filesystem::sync(filename.c_str()));
  } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
    RETURN_NOT_OK(filesystem::mpi_io_sync(mpi_comm, filename.c_str()));
#else
    // Error: MPI not supported
    return LOG_STATUS(Status::Error("Cannot sync; MPI not supported"));
#endif
  } else {
    assert(0);
  }
  // Success
  return Status::Ok();
}

Status WriteState::sync_attribute(const std::string& attribute) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  IOMethod write_method = fragment_->array()->config()->write_method();
#ifdef HAVE_MPI
  MPI_Comm* mpi_comm = fragment_->array()->config()->mpi_comm();
#endif
  int attribute_id;
  RETURN_NOT_OK(array_schema->attribute_id(attribute, &attribute_id));
  std::string filename;

  // Sync attribute
  filename = fragment_->fragment_uri()
                 .join_path(attribute + Configurator::file_suffix())
                 .to_posix_path();
  if (write_method == IOMethod::WRITE) {
    RETURN_NOT_OK(filesystem::sync(filename.c_str()));
  } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
    RETURN_NOT_OK(filesystem::mpi_io_sync(mpi_comm, filename.c_str()));
#else
    // Error: MPI not supported
    return LOG_STATUS(
        Status::Error("Cannot sync attribute; MPI not supported"));
#endif
  } else {
    assert(0);
  }
  // Only for variable-size attributes (they have an extra file)
  if (array_schema->var_size(attribute_id)) {
    filename = fragment_->fragment_uri()
                   .join_path(attribute + "_var" + Configurator::file_suffix())
                   .to_posix_path();
    if (write_method == IOMethod::WRITE) {
      RETURN_NOT_OK(filesystem::sync(filename.c_str()));
    } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
      RETURN_NOT_OK(filesystem::mpi_io_sync(mpi_comm, filename.c_str()));
#else
      // Error: MPI not supported
      return LOG_STATUS(
          Status::Error("Cannot sync attribute; MPI not supported"));
#endif
    } else {
      assert(0);
    }
  }
  // Sync fragment directory
  filename = fragment_->fragment_uri().to_posix_path();
  if (write_method == IOMethod::WRITE) {
    RETURN_NOT_OK(filesystem::sync(filename.c_str()));
  } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
    RETURN_NOT_OK(filesystem::mpi_io_sync(mpi_comm, filename.c_str()));
#else
    // Error: MPI not supported
    return LOG_STATUS(
        Status::Error("Cannot sync attribute; MPI not supported"));
#endif
  } else {
    assert(0);
  }
  // Success
  return Status::Ok();
}

Status WriteState::write(const void** buffers, const size_t* buffer_sizes) {
  // Create fragment directory if it does not exist
  std::string fragment_name = fragment_->fragment_uri().to_posix_path();
  if (!filesystem::is_dir(fragment_name)) {
    RETURN_NOT_OK(filesystem::create_dir(fragment_name));
    // For variable length attributes, ensure an empty file exists
    // This is because if the current fragment contains no valid values for this
    // attribute, then the file never gets created. This messes up querying
    // functions
    const ArraySchema* array_schema = fragment_->array()->array_schema();
    const std::vector<int>& attribute_ids = fragment_->array()->attribute_ids();
    const std::string file_prefix =
        fragment_->fragment_uri().to_posix_path() + "/";
    // Go over var length attributes
    int attribute_id_num = attribute_ids.size();
    for (int i = 0; i < attribute_id_num; ++i) {
      if (array_schema->var_size(attribute_ids[i])) {
        std::string path = file_prefix +
                           array_schema->attribute(attribute_ids[i]) + "_var" +
                           Configurator::file_suffix();
        RETURN_NOT_OK(filesystem::create_empty_file(path));
      }
    }
  }

  // Dispatch the proper write command
  if (fragment_->mode() == ArrayMode::WRITE ||
      fragment_->mode() == ArrayMode::WRITE_SORTED_COL ||
      fragment_->mode() == ArrayMode::WRITE_SORTED_ROW) {  // SORTED
    if (fragment_->dense())                                // DENSE FRAGMENT
      return write_dense(buffers, buffer_sizes);
    else  // SPARSE FRAGMENT
      return write_sparse(buffers, buffer_sizes);
  } else if (fragment_->mode() == ArrayMode::WRITE_UNSORTED) {  // UNSORTED
    return write_sparse_unsorted(buffers, buffer_sizes);
  } else {
    return LOG_STATUS(Status::Error("Cannot write to fragment; Invalid mode"));
  }
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

Status WriteState::compress_tile(
    int attribute_id,
    unsigned char* tile,
    size_t tile_size,
    size_t* tile_compressed_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  Compressor compression = array_schema->compression(attribute_id);
  int level = array_schema->compression_level(attribute_id);

  // Handle different compression
  switch (compression) {
    case Compressor::GZIP:
      return compress_tile_gzip(
          attribute_id, tile, tile_size, level, tile_compressed_size);
    case Compressor::ZSTD:
      return compress_tile_zstd(
          attribute_id, tile, tile_size, level, tile_compressed_size);
    case Compressor::LZ4:
      return compress_tile_lz4(
          attribute_id, tile, tile_size, level, tile_compressed_size);
    case Compressor::BLOSC:
      return compress_tile_blosc(
          attribute_id,
          tile,
          tile_size,
          "blosclz",
          level,
          tile_compressed_size);
#undef BLOSC_LZ4
    case Compressor::BLOSC_LZ4:
      return compress_tile_blosc(
          attribute_id, tile, tile_size, "lz4", level, tile_compressed_size);
#undef BLOSC_LZ4HC
    case Compressor::BLOSC_LZ4HC:
      return compress_tile_blosc(
          attribute_id, tile, tile_size, "lz4hc", level, tile_compressed_size);
#undef BLOSC_SNAPPY
    case Compressor::BLOSC_SNAPPY:
      return compress_tile_blosc(
          attribute_id, tile, tile_size, "snappy", level, tile_compressed_size);
#undef BLOSC_ZLIB
    case Compressor::BLOSC_ZLIB:
      return compress_tile_blosc(
          attribute_id, tile, tile_size, "zlib", level, tile_compressed_size);
#undef BLOSC_ZSTD
    case Compressor::BLOSC_ZSTD:
      return compress_tile_blosc(
          attribute_id, tile, tile_size, "zstd", level, tile_compressed_size);
    case Compressor::RLE:
      return compress_tile_rle(
          attribute_id, tile, tile_size, level, tile_compressed_size);
    case Compressor::BZIP2:
      return compress_tile_bzip2(
          attribute_id, tile, tile_size, level, tile_compressed_size);
    case Compressor::NO_COMPRESSION:
      return Status::Ok();
  }
}

Status WriteState::compress_tile_gzip(
    int attribute_id,
    unsigned char* tile,
    size_t tile_size,
    int level,
    size_t* tile_compressed_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  int attribute_num = array_schema->attribute_num();
  bool coords = (attribute_id == attribute_num);
  size_t coords_size = array_schema->coords_size();

  // Allocate space to store the compressed tile
  if (tile_compressed_ == nullptr) {
    tile_compressed_allocated_size_ =
        tile_size + 6 + 5 * (ceil(tile_size / 16834.0));
    tile_compressed_ = malloc(tile_compressed_allocated_size_);
  }

  // Expand comnpressed tile if necessary
  if (tile_size + 6 + 5 * (ceil(tile_size / 16834.0)) >
      tile_compressed_allocated_size_) {
    tile_compressed_allocated_size_ =
        tile_size + 6 + 5 * (ceil(tile_size / 16834.0));
    tile_compressed_ =
        realloc(tile_compressed_, tile_compressed_allocated_size_);
  }

  // For easy reference
  unsigned char* tile_compressed =
      static_cast<unsigned char*>(tile_compressed_);

  // Split dimensions
  if (coords)
    utils::split_coordinates(tile, tile_size, dim_num, coords_size);

  // Compress tile
  RETURN_NOT_OK(GZip::compress(
      array_schema->type_size(attribute_id),
      level,
      tile,
      tile_size,
      tile_compressed,
      tile_compressed_allocated_size_,
      tile_compressed_size));

  return Status::Ok();
}

Status WriteState::compress_tile_zstd(
    int attribute_id,
    unsigned char* tile,
    size_t tile_size,
    int level,
    size_t* tile_compressed_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  int attribute_num = array_schema->attribute_num();
  bool coords = (attribute_id == attribute_num);
  size_t coords_size = array_schema->coords_size();

  // Allocate space to store the compressed tile
  size_t compress_bound = ZStd::compress_bound(tile_size);
  if (tile_compressed_ == nullptr) {
    tile_compressed_allocated_size_ = compress_bound;
    tile_compressed_ = malloc(compress_bound);
  }

  // Expand comnpressed tile if necessary
  if (compress_bound > tile_compressed_allocated_size_) {
    tile_compressed_allocated_size_ = compress_bound;
    tile_compressed_ = realloc(tile_compressed_, compress_bound);
  }

  // Split dimensions
  if (coords)
    utils::split_coordinates(tile, tile_size, dim_num, coords_size);

  // TODO: don't pass by reference arguments that are modified
  RETURN_NOT_OK(ZStd::compress(
      array_schema->type_size(attribute_id),
      level,
      tile,
      tile_size,
      tile_compressed_,
      tile_compressed_allocated_size_,
      tile_compressed_size))

  return Status::Ok();
}

Status WriteState::compress_tile_lz4(
    int attribute_id,
    unsigned char* tile,
    size_t tile_size,
    int level,
    size_t* tile_compressed_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  int attribute_num = array_schema->attribute_num();
  bool coords = (attribute_id == attribute_num);
  size_t coords_size = array_schema->coords_size();

  // Allocate space to store the compressed tile
  size_t compress_bound = LZ4::compress_bound(tile_size);
  if (tile_compressed_ == nullptr) {
    tile_compressed_allocated_size_ = compress_bound;
    tile_compressed_ = malloc(compress_bound);
  }

  // Expand comnpressed tile if necessary
  if (compress_bound > tile_compressed_allocated_size_) {
    tile_compressed_allocated_size_ = compress_bound;
    tile_compressed_ = realloc(tile_compressed_, compress_bound);
  }

  // Split dimensions
  if (coords)
    utils::split_coordinates(tile, tile_size, dim_num, coords_size);

  // Compress tile
  RETURN_NOT_OK(LZ4::compress(
      array_schema->type_size(attribute_id),
      level,
      tile,
      tile_size,
      tile_compressed_,
      tile_compressed_allocated_size_,
      tile_compressed_size));

  // Success
  return Status::Ok();
}

Status WriteState::compress_tile_blosc(
    int attribute_id,
    unsigned char* tile,
    size_t tile_size,
    const char* compressor,
    int level,
    size_t* tile_compressed_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  int attribute_num = array_schema->attribute_num();
  bool coords = (attribute_id == attribute_num);
  size_t coords_size = array_schema->coords_size();

  // Allocate space to store the compressed tile
  size_t compress_bound = Blosc::compress_bound(tile_size);
  if (tile_compressed_ == nullptr) {
    tile_compressed_allocated_size_ = compress_bound;
    tile_compressed_ = malloc(compress_bound);
  }
  // Expand comnpressed tile if necessary
  if (compress_bound > tile_compressed_allocated_size_) {
    tile_compressed_allocated_size_ = compress_bound;
    tile_compressed_ = realloc(tile_compressed_, compress_bound);
  }

  // Split dimensions
  if (coords)
    utils::split_coordinates(tile, tile_size, dim_num, coords_size);

  // Compress tile
  RETURN_NOT_OK(Blosc::compress(
      compressor,
      array_schema->type_size(attribute_id),
      level,
      tile,
      tile_size,
      tile_compressed_,
      tile_compressed_allocated_size_,
      tile_compressed_size));
  return Status::Ok();
}

Status WriteState::compress_tile_bzip2(
    int attribute_id,
    unsigned char* tile,
    size_t tile_size,
    int level,
    size_t* tile_compressed_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  int attribute_num = array_schema->attribute_num();
  bool coords = (attribute_id == attribute_num);
  size_t coords_size = array_schema->coords_size();

  // Allocate space to store the compressed tile
  size_t compress_bound = BZip::compress_bound(tile_size);
  if (tile_compressed_ == nullptr) {
    tile_compressed_allocated_size_ = compress_bound;
    tile_compressed_ = malloc(compress_bound);
  }

  // Expand comnpressed tile if necessary
  if (compress_bound > tile_compressed_allocated_size_) {
    tile_compressed_allocated_size_ = compress_bound;
    tile_compressed_ = realloc(tile_compressed_, compress_bound);
  }

  // Split dimensions
  if (coords)
    utils::split_coordinates(tile, tile_size, dim_num, coords_size);

  // Compress tile
  RETURN_NOT_OK(BZip::compress(
      array_schema->type_size(attribute_id),
      level,
      tile,
      tile_size,
      tile_compressed_,
      tile_compressed_allocated_size_,
      tile_compressed_size));

  // Success
  return Status::Ok();
}

Status WriteState::compress_tile_rle(
    int attribute_id,
    unsigned char* tile,
    size_t tile_size,
    int level,
    size_t* tile_compressed_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  Layout order = array_schema->cell_order();
  bool is_coords = (attribute_id == attribute_num);
  size_t value_size = (array_schema->var_size(attribute_id) || is_coords) ?
                          array_schema->type_size(attribute_id) :
                          array_schema->cell_size(attribute_id);

  // Allocate space to store the compressed tile
  size_t compress_bound;
  if (!is_coords)
    compress_bound = RLE::compress_bound(tile_size, value_size);
  else
    compress_bound =
        utils::RLE_compress_bound_coords(tile_size, value_size, dim_num);
  if (tile_compressed_ == nullptr) {
    tile_compressed_allocated_size_ = compress_bound;
    tile_compressed_ = malloc(compress_bound);
  }

  // Expand comnpressed tile if necessary
  if (compress_bound > tile_compressed_allocated_size_) {
    tile_compressed_allocated_size_ = compress_bound;
    tile_compressed_ = realloc(tile_compressed_, compress_bound);
  }

  // Compress tile
  size_t rle_size = 0;
  int64_t rle_coords_size = -1;
  if (!is_coords) {
    RETURN_NOT_OK(RLE::compress(
        value_size,
        level,
        tile,
        tile_size,
        static_cast<unsigned char*>(tile_compressed_),
        tile_compressed_allocated_size_,
        &rle_size));
  } else {
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

  // Set actual output size
  // TODO: this can overflow for 32 bit, no checking
  if (!is_coords)
    *tile_compressed_size = rle_size;
  else
    *tile_compressed_size = static_cast<size_t>(rle_coords_size);

  // Success
  return Status::Ok();
}

Status WriteState::compress_and_write_tile(int attribute_id) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  unsigned char* tile = static_cast<unsigned char*>(tiles_[attribute_id]);
  size_t tile_size = tile_offsets_[attribute_id];

  // Trivial case - No in-memory tile
  if (tile_size == 0)
    return Status::Ok();

  // Compress tile
  size_t tile_compressed_size;
  RETURN_NOT_OK(
      compress_tile(attribute_id, tile, tile_size, &tile_compressed_size));

  // Get the attribute file name
  std::string filename = fragment_->fragment_uri()
                             .join_path(
                                 array_schema->attribute(attribute_id) +
                                 Configurator::file_suffix())
                             .to_posix_path();

  // Write segment to file
  Status st;
  IOMethod write_method = fragment_->array()->config()->write_method();
  if (write_method == IOMethod::WRITE) {
    st = filesystem::write_to_file(
        filename.c_str(), tile_compressed_, tile_compressed_size);
  } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
    st = filesystem::mpi_io_write_to_file(
        fragment_->array()->config()->mpi_comm(),
        filename.c_str(),
        tile_compressed_,
        tile_compressed_size);
#else
    // Error: MPI not supported
    return LOG_STATUS(
        Status::Error("Cannot compress and write tile; MPI not supported"));
#endif
  }

  // Error
  if (!st.ok()) {
    return st;
  }

  // Append offset to book-keeping
  book_keeping_->append_tile_offset(attribute_id, tile_compressed_size);

  // Success
  return Status::Ok();
}

Status WriteState::compress_and_write_tile_var(int attribute_id) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  unsigned char* tile = static_cast<unsigned char*>(tiles_var_[attribute_id]);
  size_t tile_size = tiles_var_offsets_[attribute_id];

  // Trivial case - No in-memory tile
  if (tile_size == 0) {
    // Append offset to book-keeping
    book_keeping_->append_tile_var_offset(attribute_id, 0u);
    book_keeping_->append_tile_var_size(attribute_id, 0u);
    return Status::Ok();
  }

  // Compress tile
  size_t tile_compressed_size;
  RETURN_NOT_OK(
      compress_tile(attribute_id, tile, tile_size, &tile_compressed_size));

  // Get the attribute file name
  std::string filename = fragment_->fragment_uri()
                             .join_path(
                                 array_schema->attribute(attribute_id) +
                                 "_var" + Configurator::file_suffix())
                             .to_posix_path();

  // Write segment to file
  Status st;
  IOMethod write_method = fragment_->array()->config()->write_method();
  if (write_method == IOMethod::WRITE) {
    st = filesystem::write_to_file(
        filename.c_str(), tile_compressed_, tile_compressed_size);
  } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
    st = filesystem::mpi_io_write_to_file(
        fragment_->array()->config()->mpi_comm(),
        filename.c_str(),
        tile_compressed_,
        tile_compressed_size);
#else
    // Error: MPI not supported
    return LOG_STATUS(Status::Error(
        "Cannot compress and write variable tile; MPI not supported"));
#endif
  }

  // Error
  if (!st.ok()) {
    return st;
  }

  // Append offset to book-keeping
  book_keeping_->append_tile_var_offset(attribute_id, tile_compressed_size);
  book_keeping_->append_tile_var_size(attribute_id, tile_size);

  // Success
  return Status::Ok();
}

template <class T>
void WriteState::expand_mbr(const T* coords) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  T* mbr = static_cast<T*>(mbr_);

  // Initialize MBR
  if (tile_cell_num_[attribute_num] == 0) {
    for (int i = 0; i < dim_num; ++i) {
      mbr[2 * i] = coords[i];
      mbr[2 * i + 1] = coords[i];
    }
  } else {  // Expand MBR
    utils::expand_mbr(mbr, coords, dim_num);
  }
}

void WriteState::shift_var_offsets(
    int attribute_id,
    size_t buffer_var_size,
    const void* buffer,
    size_t buffer_size,
    void* shifted_buffer) {
  // For easy reference
  int64_t buffer_cell_num = buffer_size / sizeof(size_t);
  const size_t* buffer_s = static_cast<const size_t*>(buffer);
  size_t* shifted_buffer_s = static_cast<size_t*>(shifted_buffer);
  size_t& buffer_var_offset = buffer_var_offsets_[attribute_id];

  // Shift offsets
  for (int64_t i = 0; i < buffer_cell_num; ++i)
    shifted_buffer_s[i] = buffer_var_offset + buffer_s[i];

  // Update the last offset
  buffer_var_offset += buffer_var_size;
}

void WriteState::sort_cell_pos(
    const void* buffer,
    size_t buffer_size,
    std::vector<int64_t>& cell_pos) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  Datatype coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if (coords_type == Datatype::INT32)
    sort_cell_pos<int>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::INT64)
    sort_cell_pos<int64_t>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::FLOAT32)
    sort_cell_pos<float>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::FLOAT64)
    sort_cell_pos<double>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::INT8)
    sort_cell_pos<int8_t>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::UINT8)
    sort_cell_pos<uint8_t>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::INT16)
    sort_cell_pos<int16_t>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::UINT16)
    sort_cell_pos<uint16_t>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::UINT32)
    sort_cell_pos<uint32_t>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::UINT64)
    sort_cell_pos<uint64_t>(buffer, buffer_size, cell_pos);
}

template <class T>
void WriteState::sort_cell_pos(
    const void* buffer,
    size_t buffer_size,
    std::vector<int64_t>& cell_pos) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  int64_t buffer_cell_num = buffer_size / coords_size;
  Layout cell_order = array_schema->cell_order();
  const T* buffer_T = static_cast<const T*>(buffer);

  // Populate cell_pos
  cell_pos.resize(buffer_cell_num);
  for (int i = 0; i < buffer_cell_num; ++i)
    cell_pos[i] = i;

  // Invoke the proper sort function, based on the cell order
  if (array_schema->tile_extents() == nullptr) {  // NO TILE GRID
    // Sort cell positions
    switch (cell_order) {
      case Layout::ROW_MAJOR:
        std::sort(
            cell_pos.begin(), cell_pos.end(), SmallerRow<T>(buffer_T, dim_num));
        break;
      case Layout::COL_MAJOR:
        std::sort(
            cell_pos.begin(), cell_pos.end(), SmallerCol<T>(buffer_T, dim_num));
        break;
    }
  } else {  // TILE GRID
    // Get tile ids
    std::vector<int64_t> ids;
    ids.resize(buffer_cell_num);
    for (int i = 0; i < buffer_cell_num; ++i)
      ids[i] = array_schema->tile_id<T>(&buffer_T[i * dim_num]);
    // Sort cell positions
    switch (cell_order) {
      case Layout::ROW_MAJOR:
        std::sort(
            cell_pos.begin(),
            cell_pos.end(),
            SmallerIdRow<T>(buffer_T, dim_num, ids));
        break;
      case Layout::COL_MAJOR:
        std::sort(
            cell_pos.begin(),
            cell_pos.end(),
            SmallerIdCol<T>(buffer_T, dim_num, ids));
        break;
    }
  }
}

void WriteState::update_book_keeping(const void* buffer, size_t buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  Datatype coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if (coords_type == Datatype::INT32)
    update_book_keeping<int>(buffer, buffer_size);
  else if (coords_type == Datatype::INT64)
    update_book_keeping<int64_t>(buffer, buffer_size);
  else if (coords_type == Datatype::FLOAT32)
    update_book_keeping<float>(buffer, buffer_size);
  else if (coords_type == Datatype::FLOAT64)
    update_book_keeping<double>(buffer, buffer_size);
  else if (coords_type == Datatype::INT8)
    update_book_keeping<int8_t>(buffer, buffer_size);
  else if (coords_type == Datatype::UINT8)
    update_book_keeping<uint8_t>(buffer, buffer_size);
  else if (coords_type == Datatype::INT16)
    update_book_keeping<int16_t>(buffer, buffer_size);
  else if (coords_type == Datatype::UINT16)
    update_book_keeping<uint16_t>(buffer, buffer_size);
  else if (coords_type == Datatype::UINT32)
    update_book_keeping<uint32_t>(buffer, buffer_size);
  else if (coords_type == Datatype::UINT64)
    update_book_keeping<uint64_t>(buffer, buffer_size);
}

template <class T>
void WriteState::update_book_keeping(const void* buffer, size_t buffer_size) {
  // Trivial case
  if (buffer_size == 0)
    return;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  int64_t capacity = array_schema->capacity();
  size_t coords_size = array_schema->coords_size();
  int64_t buffer_cell_num = buffer_size / coords_size;
  const T* buffer_T = static_cast<const T*>(buffer);
  int64_t& tile_cell_num = tile_cell_num_[attribute_num];

  // Update bounding coordinates and MBRs
  for (int64_t i = 0; i < buffer_cell_num; ++i) {
    // Set first bounding coordinates
    if (tile_cell_num == 0)
      memcpy(bounding_coords_, &buffer_T[i * dim_num], coords_size);

    // Set second bounding coordinates
    memcpy(
        static_cast<char*>(bounding_coords_) + coords_size,
        &buffer_T[i * dim_num],
        coords_size);

    // Expand MBR
    expand_mbr(&buffer_T[i * dim_num]);

    // Advance a cell
    ++tile_cell_num;

    // Send MBR and bounding coordinates to book-keeping
    if (tile_cell_num == capacity) {
      book_keeping_->append_mbr(mbr_);
      book_keeping_->append_bounding_coords(bounding_coords_);
      tile_cell_num = 0;
    }
  }
}

Status WriteState::write_last_tile() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Send last MBR, bounding coordinates and tile cell number to book-keeping
  book_keeping_->append_mbr(mbr_);
  book_keeping_->append_bounding_coords(bounding_coords_);
  book_keeping_->set_last_tile_cell_num(tile_cell_num_[attribute_num]);

  // Flush the last tile for each compressed attribute (it is still in main
  // memory
  for (int i = 0; i < attribute_num + 1; ++i) {
    if (array_schema->compression(i) != Compressor::NO_COMPRESSION) {
      RETURN_NOT_OK(compress_and_write_tile(i));
      if (array_schema->var_size(i)) {
        RETURN_NOT_OK(compress_and_write_tile_var(i));
      }
    }
  }
  // Success
  return Status::Ok();
}

Status WriteState::write_dense(
    const void** buffers, const size_t* buffer_sizes) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  const std::vector<int>& attribute_ids = fragment_->array()->attribute_ids();
  int attribute_id_num = attribute_ids.size();

  // Write each attribute individually
  int buffer_i = 0;
  for (int i = 0; i < attribute_id_num; ++i) {
    if (!array_schema->var_size(attribute_ids[i])) {  // FIXED CELLS
      RETURN_NOT_OK(write_dense_attr(
          attribute_ids[i], buffers[buffer_i], buffer_sizes[buffer_i]));
      ++buffer_i;
    } else {  // VARIABLE-SIZED CELLS
      RETURN_NOT_OK(write_dense_attr_var(
          attribute_ids[i],
          buffers[buffer_i],  // offsets
          buffer_sizes[buffer_i],
          buffers[buffer_i + 1],  // actual cell values
          buffer_sizes[buffer_i + 1]));
      buffer_i += 2;
    }
  }

  // Success
  return Status::Ok();
}

Status WriteState::write_dense_attr(
    int attribute_id, const void* buffer, size_t buffer_size) {
  // Trivial case
  if (buffer_size == 0)
    return Status::Ok();

  // For easy reference
  auto buf = new ConstBuffer(buffer, buffer_size);
  Tile* tile = Tiles_[attribute_id];
  TileIO* tile_io = tile_io_[attribute_id];

  // Fill tiles and dispatch them for writing
  uint64_t bytes_written;
  do {
    RETURN_NOT_OK(tile->write(buf));
    if (tile->full()) {
      RETURN_NOT_OK(tile_io->write(tile, &bytes_written));
      book_keeping_->append_tile_offset(attribute_id, bytes_written);
      tile->reset();
    }
  } while (!buf->end());

  // Clean up
  delete buf;

  return Status::Ok();
}

Status WriteState::write_dense_attr_var(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size) {
  // Trivial case
  if (buffer_size == 0)
    return Status::Ok();

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  Compressor compression = array_schema->compression(attribute_id);

  // No compression
  if (compression == Compressor::NO_COMPRESSION)
    return write_dense_attr_var_cmp_none(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  else  // All compressions
    return write_dense_attr_var_cmp(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);

  // Sanity check
  assert(0);

  // Error
  return LOG_STATUS(Status::Error("should not happen"));
}

Status WriteState::write_dense_attr_var_cmp_none(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();

  // Write buffer with variable-sized cells to disk
  std::string filename = fragment_->fragment_uri()
                             .join_path(
                                 array_schema->attribute(attribute_id) +
                                 "_var" + Configurator::file_suffix())
                             .to_posix_path();
  IOMethod write_method = fragment_->array()->config()->write_method();
#ifdef HAVE_MPI
  MPI_Comm* mpi_comm = fragment_->array()->config()->mpi_comm();
#endif
  if (write_method == IOMethod::WRITE) {
    RETURN_NOT_OK(filesystem::write_to_file(
        filename.c_str(), buffer_var, buffer_var_size));
  } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
    RETURN_NOT_OK(filesystem::mpi_io_write_to_file(
        mpi_comm, filename.c_str(), buffer_var, buffer_var_size));
#else
    // Error: MPI not supported
    return LOG_STATUS(Status::Error(
        "Cannot write dense variable attribute; MPI not supported"));
#endif
  }
  // Recalculate offsets
  void* shifted_buffer = malloc(buffer_size);
  shift_var_offsets(
      attribute_id, buffer_var_size, buffer, buffer_size, shifted_buffer);

  // Write buffer offsets to file
  filename = fragment_->fragment_uri().to_posix_path() + "/" +
             array_schema->attribute(attribute_id) +
             Configurator::file_suffix();
  Status st;
  if (write_method == IOMethod::WRITE) {
    st = filesystem::write_to_file(
        filename.c_str(), shifted_buffer, buffer_size);
  } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
    st = filesystem::mpi_io_write_to_file(
        mpi_comm, filename.c_str(), shifted_buffer, buffer_size);
#else
    // Error: MPI not supported
    st = Status::Error(
        "Cannot write dense variable attribute; MPI not supported");
#endif
  }
  free(shifted_buffer);
  return st;
}

Status WriteState::write_dense_attr_var_cmp(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size) {
  // For easy reference
  size_t cell_size = Configurator::cell_var_offset_size();
  int64_t cell_num_per_tile = fragment_->cell_num_per_tile();
  size_t tile_size = cell_num_per_tile * cell_size;

  // Initialize local tile buffer if needed
  if (tiles_[attribute_id] == nullptr)
    tiles_[attribute_id] = malloc(tile_size);

  // Initialize local variable tile buffer if needed
  if (tiles_var_[attribute_id] == nullptr) {
    tiles_var_[attribute_id] = malloc(tile_size);
    tiles_var_sizes_[attribute_id] = tile_size;
  }

  // Recalculate offsets
  void* shifted_buffer = malloc(buffer_size);
  shift_var_offsets(
      attribute_id, buffer_var_size, buffer, buffer_size, shifted_buffer);

  // For easy reference
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  size_t& tile_offset = tile_offsets_[attribute_id];
  const size_t* buffer_s = static_cast<const size_t*>(buffer);
  const char* shifted_buffer_c = static_cast<const char*>(shifted_buffer);
  size_t buffer_offset = 0;
  char* tile_var = static_cast<char*>(tiles_var_[attribute_id]);
  size_t& tile_var_offset = tiles_var_offsets_[attribute_id];
  const char* buffer_var_c = static_cast<const char*>(buffer_var);
  size_t buffer_var_offset = 0;

  // Update total number of cells
  int64_t buffer_cell_num = buffer_size / cell_size;

  // Bytes to fill the potentially partially buffered tile
  size_t bytes_to_fill = tile_size - tile_offset;
  int64_t cell_num_to_fill = bytes_to_fill / cell_size;
  int64_t end_cell_pos = cell_num_to_fill;
  size_t bytes_to_fill_var = (end_cell_pos == buffer_cell_num) ?
                                 buffer_var_size :
                                 buffer_s[end_cell_pos];

  Status st;
  // The buffer has enough cells to fill at least one tile
  if (bytes_to_fill <= buffer_size) {
    // Fill up current tile
    memcpy(tile + tile_offset, shifted_buffer_c + buffer_offset, bytes_to_fill);
    buffer_offset += bytes_to_fill;
    tile_offset += bytes_to_fill;

    // Compress current tile and write it to disk
    st = compress_and_write_tile(attribute_id);
    if (!st.ok()) {
      free(shifted_buffer);
      return st;
    }

    // Update local tile buffer offset
    tile_offset = 0;

    // Potentially expand the variable tile buffer
    if (tile_var_offset + bytes_to_fill_var > tiles_var_sizes_[attribute_id]) {
      tiles_var_sizes_[attribute_id] = tile_var_offset + bytes_to_fill_var;
      tiles_var_[attribute_id] =
          realloc(tiles_var_[attribute_id], tiles_var_sizes_[attribute_id]);
      // Re-allocation may assign tiles_var_ to a different region of memory
      tile_var = static_cast<char*>(tiles_var_[attribute_id]);
    }

    // Fill up current variable tile
    memcpy(
        tile_var + tile_var_offset,
        buffer_var_c + buffer_var_offset,
        bytes_to_fill_var);
    buffer_var_offset += bytes_to_fill_var;
    tile_var_offset += bytes_to_fill_var;

    // Compress current tile and write it to disk
    st = compress_and_write_tile_var(attribute_id);
    if (!st.ok()) {
      free(shifted_buffer);
      return st;
    }
    // Update local tile buffer offset
    tile_var_offset = 0;
  }

  // Continue to fill and compress entire tiles
  while (buffer_offset + tile_size <= buffer_size) {
    // Prepare tile
    memcpy(tile, shifted_buffer_c + buffer_offset, tile_size);
    buffer_offset += tile_size;
    tile_offset += tile_size;

    // Compress and write current tile
    st = compress_and_write_tile(attribute_id);
    if (!st.ok()) {
      free(shifted_buffer);
      return st;
    }

    // Update local tile buffer offset
    tile_offset = 0;

    // Calculate the number of bytes to fill for the variable tile
    bytes_to_fill_var =
        (end_cell_pos + cell_num_per_tile == buffer_cell_num) ?
            buffer_var_size - buffer_var_offset :
            buffer_s[end_cell_pos + cell_num_per_tile] - buffer_s[end_cell_pos];
    end_cell_pos += cell_num_per_tile;

    // Potentially expand the variable tile buffer
    if (tile_var_offset + bytes_to_fill_var > tiles_var_sizes_[attribute_id]) {
      tiles_var_sizes_[attribute_id] = tile_var_offset + bytes_to_fill_var;
      tiles_var_[attribute_id] =
          realloc(tiles_var_[attribute_id], tiles_var_sizes_[attribute_id]);
      // Re-allocation may assign tiles_var_ to a different region of memory
      tile_var = static_cast<char*>(tiles_var_[attribute_id]);
    }

    // Fill up current variable tile
    memcpy(
        tile_var + tile_var_offset,
        buffer_var_c + buffer_var_offset,
        bytes_to_fill_var);
    buffer_var_offset += bytes_to_fill_var;
    tile_var_offset += bytes_to_fill_var;

    // Compress current tile and write it to disk
    st = compress_and_write_tile_var(attribute_id);
    if (!st.ok()) {
      free(shifted_buffer);
      return st;
    }

    // Update local tile buffer offset
    tile_var_offset = 0;
  }

  // Partially fill the (new) current tile
  bytes_to_fill = buffer_size - buffer_offset;
  if (bytes_to_fill != 0) {
    memcpy(tile + tile_offset, shifted_buffer_c + buffer_offset, bytes_to_fill);
    buffer_offset += bytes_to_fill;
    assert(buffer_offset == buffer_size);
    tile_offset += bytes_to_fill;

    // Calculate the number of bytes to fill for the variable tile
    bytes_to_fill_var = buffer_var_size - buffer_var_offset;

    // Potentially expand the variable tile buffer
    if (tile_var_offset + bytes_to_fill_var > tiles_var_sizes_[attribute_id]) {
      tiles_var_sizes_[attribute_id] = tile_var_offset + bytes_to_fill_var;
      tiles_var_[attribute_id] =
          realloc(tiles_var_[attribute_id], tiles_var_sizes_[attribute_id]);
      // Re-allocation may assign tiles_var_ to a different region of memory
      tile_var = static_cast<char*>(tiles_var_[attribute_id]);
    }

    // Fill up current variable tile
    memcpy(
        tile_var + tile_var_offset,
        buffer_var_c + buffer_var_offset,
        bytes_to_fill_var);
    buffer_var_offset += bytes_to_fill_var;
    assert(buffer_var_offset == buffer_var_size);
    tile_var_offset += bytes_to_fill_var;
  }

  // Clean up
  free(shifted_buffer);

  // Success
  return Status::Ok();
}

Status WriteState::write_sparse(
    const void** buffers, const size_t* buffer_sizes) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  const std::vector<int>& attribute_ids = fragment_->array()->attribute_ids();
  int attribute_id_num = attribute_ids.size();

  // Write each attribute individually
  int buffer_i = 0;
  for (int i = 0; i < attribute_id_num; ++i) {
    if (!array_schema->var_size(attribute_ids[i])) {  // FIXED CELLS
      RETURN_NOT_OK(write_sparse_attr(
          attribute_ids[i], buffers[buffer_i], buffer_sizes[buffer_i]));
      ++buffer_i;
    } else {  // VARIABLE-SIZED CELLS
      RETURN_NOT_OK(write_sparse_attr_var(
          attribute_ids[i],
          buffers[buffer_i],  // offsets
          buffer_sizes[buffer_i],
          buffers[buffer_i + 1],  // actual cell values
          buffer_sizes[buffer_i + 1]));
      buffer_i += 2;
    }
  }
  // Success
  return Status::Ok();
}

Status WriteState::write_sparse_attr(
    int attribute_id, const void* buffer, size_t buffer_size) {
  // Trivial case
  if (buffer_size == 0)
    return Status::Ok();

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  Compressor compression = array_schema->compression(attribute_id);

  // No compression
  if (compression == Compressor::NO_COMPRESSION)
    return write_sparse_attr_cmp_none(attribute_id, buffer, buffer_size);
  else  // All compressions
    return write_sparse_attr_cmp(attribute_id, buffer, buffer_size);
}

Status WriteState::write_sparse_attr_cmp_none(
    int attribute_id, const void* buffer, size_t buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Update book-keeping
  if (attribute_id == attribute_num)
    update_book_keeping(buffer, buffer_size);

  // Write buffer to file
  std::string filename = fragment_->fragment_uri()
                             .join_path(
                                 array_schema->attribute(attribute_id) +
                                 Configurator::file_suffix())
                             .to_posix_path();
  Status st;
  IOMethod write_method = fragment_->array()->config()->write_method();
#ifdef HAVE_MPI
  MPI_Comm* mpi_comm = fragment_->array()->config()->mpi_comm();
#endif
  if (write_method == IOMethod::WRITE) {
    st = filesystem::write_to_file(filename.c_str(), buffer, buffer_size);
  } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
    st = filesystem::mpi_io_write_to_file(
        mpi_comm, filename.c_str(), buffer, buffer_size);
#else
    // Error: MPI not supported
    return LOG_STATUS(
        Status::Error("Cannot write sparse attribute; MPI not supported"));
#endif
  }
  return st;
}

Status WriteState::write_sparse_attr_cmp(
    int attribute_id, const void* buffer, size_t buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  size_t tile_size = fragment_->tile_size(attribute_id);

  // Update book-keeping
  if (attribute_id == attribute_num)
    update_book_keeping(buffer, buffer_size);

  // Initialize local tile buffer if needed
  if (tiles_[attribute_id] == nullptr)
    tiles_[attribute_id] = malloc(tile_size);

  // For easy reference
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  size_t& tile_offset = tile_offsets_[attribute_id];
  const char* buffer_c = static_cast<const char*>(buffer);
  size_t buffer_offset = 0;

  // Bytes to fill the potentially partially buffered tile
  size_t bytes_to_fill = tile_size - tile_offset;

  // The buffer has enough cells to fill at least one tile
  if (bytes_to_fill <= buffer_size) {
    // Fill up current tile, and append offset to book-keeping
    memcpy(tile + tile_offset, buffer_c + buffer_offset, bytes_to_fill);
    buffer_offset += bytes_to_fill;
    tile_offset += bytes_to_fill;

    // Compress current tile and write it to disk
    RETURN_NOT_OK(compress_and_write_tile(attribute_id));

    // Update local tile buffer offset
    tile_offset = 0;
  }

  // Continue to fill and compress entire tiles
  while (buffer_offset + tile_size <= buffer_size) {
    // Prepare tile
    memcpy(tile, buffer_c + buffer_offset, tile_size);
    buffer_offset += tile_size;
    tile_offset += tile_size;

    // Compress current tile, append to segment.
    RETURN_NOT_OK(compress_and_write_tile(attribute_id));

    // Update local tile buffer offset
    tile_offset = 0;
  }

  // Partially fill the (new) current tile
  bytes_to_fill = buffer_size - buffer_offset;
  if (bytes_to_fill != 0) {
    memcpy(tile + tile_offset, buffer_c + buffer_offset, bytes_to_fill);
    buffer_offset += bytes_to_fill;
    assert(buffer_offset == buffer_size);
    tile_offset += bytes_to_fill;
  }

  // Success
  return Status::Ok();
}

Status WriteState::write_sparse_attr_var(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size) {
  // Trivial case
  if (buffer_size == 0)
    return Status::Ok();

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  Compressor compression = array_schema->compression(attribute_id);

  // No compression
  if (compression == Compressor::NO_COMPRESSION)
    return write_sparse_attr_var_cmp_none(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  else  //  All compressions
    return write_sparse_attr_var_cmp(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
}

Status WriteState::write_sparse_attr_var_cmp_none(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();

  // Update book-keeping
  assert(attribute_id != array_schema->attribute_num());

  // Write buffer with variable-sized cells to disk
  std::string filename = fragment_->fragment_uri()
                             .join_path(
                                 array_schema->attribute(attribute_id) +
                                 "_var" + Configurator::file_suffix())
                             .to_posix_path();
  Status st;
  IOMethod write_method = fragment_->array()->config()->write_method();
#ifdef HAVE_MPI
  MPI_Comm* mpi_comm = fragment_->array()->config()->mpi_comm();
#endif
  if (write_method == IOMethod::WRITE) {
    st = filesystem::write_to_file(
        filename.c_str(), buffer_var, buffer_var_size);
  } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
    st = filesystem::mpi_io_write_to_file(
        mpi_comm, filename.c_str(), buffer_var, buffer_var_size);
#else
    // Error: MPI not supported
    return LOG_STATUS(Status::Error(
        "Cannot write sparse variable attribute; MPI not supported"));
#endif
  }
  if (!st.ok()) {
    return st;
  }
  // Recalculate offsets
  void* shifted_buffer = malloc(buffer_size);
  shift_var_offsets(
      attribute_id, buffer_var_size, buffer, buffer_size, shifted_buffer);

  // Write buffer offsets to file
  filename = fragment_->fragment_uri()
                 .join_path(
                     array_schema->attribute(attribute_id) +
                     Configurator::file_suffix())
                 .to_posix_path();
  if (write_method == IOMethod::WRITE) {
    st = filesystem::write_to_file(
        filename.c_str(), shifted_buffer, buffer_size);
  } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
    st = filesystem::mpi_io_write_to_file(
        mpi_comm, filename.c_str(), shifted_buffer, buffer_size);
#else
    return LOG_STATUS(Status::Error(
        "Cannot write sparse variable attribute; MPI not supported"));
#endif
  }

  // Clean up
  free(shifted_buffer);

  return st;
}

Status WriteState::write_sparse_attr_var_cmp(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = Configurator::cell_var_offset_size();
  int64_t cell_num_per_tile = array_schema->capacity();
  size_t tile_size = fragment_->tile_size(attribute_id);

  // Sanity check (coordinates are always fixed-sized)
  assert(attribute_id != array_schema->attribute_num());

  // Initialize local tile buffer if needed
  if (tiles_[attribute_id] == nullptr)
    tiles_[attribute_id] = malloc(tile_size);

  // Initialize local variable tile buffer if needed
  if (tiles_var_[attribute_id] == nullptr) {
    tiles_var_[attribute_id] = malloc(tile_size);
    tiles_var_sizes_[attribute_id] = tile_size;
  }

  // Recalculate offsets
  void* shifted_buffer = malloc(buffer_size);
  shift_var_offsets(
      attribute_id, buffer_var_size, buffer, buffer_size, shifted_buffer);

  // For easy reference
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  size_t& tile_offset = tile_offsets_[attribute_id];
  const size_t* buffer_s = static_cast<const size_t*>(buffer);
  const char* shifted_buffer_c = static_cast<const char*>(shifted_buffer);
  size_t buffer_offset = 0;
  char* tile_var = static_cast<char*>(tiles_var_[attribute_id]);
  size_t& tile_var_offset = tiles_var_offsets_[attribute_id];
  const char* buffer_var_c = static_cast<const char*>(buffer_var);
  size_t buffer_var_offset = 0;

  // Update total number of cells
  int64_t buffer_cell_num = buffer_size / cell_size;

  // Bytes to fill the potentially partially buffered tile
  size_t bytes_to_fill = tile_size - tile_offset;
  int64_t cell_num_to_fill = bytes_to_fill / cell_size;
  int64_t end_cell_pos = cell_num_to_fill;
  size_t bytes_to_fill_var = (end_cell_pos == buffer_cell_num) ?
                                 buffer_var_size :
                                 buffer_s[end_cell_pos];

  // The buffer has enough cells to fill at least one tile
  if (bytes_to_fill <= buffer_size) {
    // Fill up current tile
    memcpy(tile + tile_offset, shifted_buffer_c + buffer_offset, bytes_to_fill);
    buffer_offset += bytes_to_fill;
    tile_offset += bytes_to_fill;

    // Compress current tile and write it to disk
    RETURN_NOT_OK_ELSE(
        compress_and_write_tile(attribute_id), free(shifted_buffer));

    // Update local tile buffer offset
    tile_offset = 0;

    // Potentially expand the variable tile buffer
    while (tile_var_offset + bytes_to_fill_var > tiles_var_sizes_[attribute_id])
      utils::expand_buffer(
          tiles_var_[attribute_id], tiles_var_sizes_[attribute_id]);
    // Re-allocation may assign tiles_var_ to a different region of memory
    tile_var = static_cast<char*>(tiles_var_[attribute_id]);

    // Fill up current variable tile
    memcpy(
        tile_var + tile_var_offset,
        buffer_var_c + buffer_var_offset,
        bytes_to_fill_var);
    buffer_var_offset += bytes_to_fill_var;
    tile_var_offset += bytes_to_fill_var;

    // Compress current tile and write it to disk
    RETURN_NOT_OK_ELSE(
        compress_and_write_tile_var(attribute_id), free(shifted_buffer));

    // Update local tile buffer offset
    tile_var_offset = 0;
  }

  // Continue to fill and compress entire tiles
  while (buffer_offset + tile_size <= buffer_size) {
    // Prepare tile
    memcpy(tile, shifted_buffer_c + buffer_offset, tile_size);
    buffer_offset += tile_size;
    tile_offset += tile_size;

    // Compress and write current tile
    RETURN_NOT_OK_ELSE(
        compress_and_write_tile(attribute_id), free(shifted_buffer));

    // Update local tile buffer offset
    tile_offset = 0;

    // Calculate the number of bytes to fill for the variable tile
    bytes_to_fill_var =
        (end_cell_pos + cell_num_per_tile == buffer_cell_num) ?
            buffer_var_size - buffer_var_offset :
            buffer_s[end_cell_pos + cell_num_per_tile] - buffer_s[end_cell_pos];
    end_cell_pos += cell_num_per_tile;

    // Potentially expand the variable tile buffer
    if (tile_var_offset + bytes_to_fill_var > tiles_var_sizes_[attribute_id]) {
      tiles_var_sizes_[attribute_id] = tile_var_offset + bytes_to_fill_var;
      tiles_var_[attribute_id] =
          realloc(tiles_var_[attribute_id], tiles_var_sizes_[attribute_id]);
      // Re-allocation may assign tiles_var_ to a different region of memory
      tile_var = static_cast<char*>(tiles_var_[attribute_id]);
    }

    // Fill up current variable tile
    memcpy(
        tile_var + tile_var_offset,
        buffer_var_c + buffer_var_offset,
        bytes_to_fill_var);
    buffer_var_offset += bytes_to_fill_var;
    tile_var_offset += bytes_to_fill_var;

    // Compress current tile and write it to disk
    RETURN_NOT_OK_ELSE(
        compress_and_write_tile_var(attribute_id), free(shifted_buffer));

    // Update local tile buffer offset
    tile_var_offset = 0;
  }

  // Partially fill the (new) current tile
  bytes_to_fill = buffer_size - buffer_offset;
  if (bytes_to_fill != 0) {
    memcpy(tile + tile_offset, shifted_buffer_c + buffer_offset, bytes_to_fill);
    buffer_offset += bytes_to_fill;
    assert(buffer_offset == buffer_size);
    tile_offset += bytes_to_fill;

    // Calculate the number of bytes to fill for the variable tile
    bytes_to_fill_var = buffer_var_size - buffer_var_offset;

    // Potentially expand the variable tile buffer
    if (tile_var_offset + bytes_to_fill_var > tiles_var_sizes_[attribute_id]) {
      tiles_var_sizes_[attribute_id] = tile_var_offset + bytes_to_fill_var;
      tiles_var_[attribute_id] =
          realloc(tiles_var_[attribute_id], tiles_var_sizes_[attribute_id]);
      // Re-allocation may assign tiles_var_ to a different region of memory
      tile_var = static_cast<char*>(tiles_var_[attribute_id]);
    }

    // Fill up current variable tile
    memcpy(
        tile_var + tile_var_offset,
        buffer_var_c + buffer_var_offset,
        bytes_to_fill_var);
    buffer_var_offset += bytes_to_fill_var;
    assert(buffer_var_offset == buffer_var_size);
    tile_var_offset += bytes_to_fill_var;
  }

  // Clean up
  free(shifted_buffer);

  // Success
  return Status::Ok();
}

Status WriteState::write_sparse_unsorted(
    const void** buffers, const size_t* buffer_sizes) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  const std::vector<int>& attribute_ids = fragment_->array()->attribute_ids();
  int attribute_id_num = attribute_ids.size();

  // Find the coordinates buffer
  int coords_buffer_i = -1;
  int buffer_i = 0;
  for (int i = 0; i < attribute_id_num; ++i) {
    if (attribute_ids[i] == attribute_num) {
      coords_buffer_i = buffer_i;
      break;
    }
    if (!array_schema->var_size(attribute_ids[i]))  // FIXED CELLS
      ++buffer_i;
    else  // VARIABLE-SIZED CELLS
      buffer_i += 2;
  }

  // Coordinates are missing
  if (coords_buffer_i == -1) {
    return LOG_STATUS(
        Status::Error("Cannot write sparse unsorted; Coordinates missing"));
  }

  // Sort cell positions
  std::vector<int64_t> cell_pos;
  sort_cell_pos(
      buffers[coords_buffer_i], buffer_sizes[coords_buffer_i], cell_pos);

  // Write each attribute individually
  buffer_i = 0;
  for (int i = 0; i < attribute_id_num; ++i) {
    if (!array_schema->var_size(attribute_ids[i])) {  // FIXED CELLS
      RETURN_NOT_OK(write_sparse_unsorted_attr(
          attribute_ids[i],
          buffers[buffer_i],
          buffer_sizes[buffer_i],
          cell_pos));
      ++buffer_i;
    } else {  // VARIABLE-SIZED CELLS
      RETURN_NOT_OK(write_sparse_unsorted_attr_var(
          attribute_ids[i],
          buffers[buffer_i],  // offsets
          buffer_sizes[buffer_i],
          buffers[buffer_i + 1],  // actual values
          buffer_sizes[buffer_i + 1],
          cell_pos));
      buffer_i += 2;
    }
  }

  // Success
  return Status::Ok();
}

Status WriteState::write_sparse_unsorted_attr(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const std::vector<int64_t>& cell_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  Compressor compression = array_schema->compression(attribute_id);

  // No compression
  if (compression == Compressor::NO_COMPRESSION)
    return write_sparse_unsorted_attr_cmp_none(
        attribute_id, buffer, buffer_size, cell_pos);
  else  // All compressions
    return write_sparse_unsorted_attr_cmp(
        attribute_id, buffer, buffer_size, cell_pos);
}

Status WriteState::write_sparse_unsorted_attr_cmp_none(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const std::vector<int64_t>& cell_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = array_schema->cell_size(attribute_id);
  const char* buffer_c = static_cast<const char*>(buffer);

  // Check number of cells in buffer
  int64_t buffer_cell_num = buffer_size / cell_size;
  if (buffer_cell_num != int64_t(cell_pos.size())) {
    return LOG_STATUS(Status::Error(
        std::string("Cannot write sparse unsorted; Invalid number of "
                    "cells in attribute '") +
        array_schema->attribute(attribute_id) + "'"));
  }

  // Allocate a local buffer to hold the sorted cells
  char* sorted_buffer = new char[Configurator::sorted_buffer_size()];
  size_t sorted_buffer_size = 0;

  // Sort and write attribute values in batches
  Status st;
  for (int64_t i = 0; i < buffer_cell_num; ++i) {
    // Write batch
    if (sorted_buffer_size + cell_size > Configurator::sorted_buffer_size()) {
      st = write_sparse_attr_cmp_none(
          attribute_id, sorted_buffer, sorted_buffer_size);
      if (!st.ok()) {
        delete[] sorted_buffer;
        return st;
      } else {
        sorted_buffer_size = 0;
      }
    }

    // Keep on copying the cells in the sorted order in the sorted buffer
    memcpy(
        sorted_buffer + sorted_buffer_size,
        buffer_c + cell_pos[i] * cell_size,
        cell_size);
    sorted_buffer_size += cell_size;
  }

  // Write final batch
  if (sorted_buffer_size != 0) {
    st = write_sparse_attr_cmp_none(
        attribute_id, sorted_buffer, sorted_buffer_size);
  }
  // Clean up
  delete[] sorted_buffer;
  return st;
}

Status WriteState::write_sparse_unsorted_attr_cmp(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const std::vector<int64_t>& cell_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = array_schema->cell_size(attribute_id);
  const char* buffer_c = static_cast<const char*>(buffer);

  // Check number of cells in buffer
  int64_t buffer_cell_num = buffer_size / cell_size;
  if (buffer_cell_num != int64_t(cell_pos.size())) {
    return LOG_STATUS(Status::Error(
        std::string("Cannot write sparse unsorted; Invalid number of "
                    "cells in attribute '") +
        array_schema->attribute(attribute_id) + "'"));
  }

  // Allocate a local buffer to hold the sorted cells
  char* sorted_buffer = new char[Configurator::sorted_buffer_size()];
  size_t sorted_buffer_size = 0;

  // Sort and write attribute values in batches
  Status st;
  for (int64_t i = 0; i < buffer_cell_num; ++i) {
    // Write batch
    if (sorted_buffer_size + cell_size > Configurator::sorted_buffer_size()) {
      st = write_sparse_attr_cmp(
          attribute_id, sorted_buffer, sorted_buffer_size);
      if (!st.ok()) {
        delete[] sorted_buffer;
        return st;
      } else {
        sorted_buffer_size = 0;
      }
    }

    // Keep on copying the cells in the sorted order in the sorted buffer
    memcpy(
        sorted_buffer + sorted_buffer_size,
        buffer_c + cell_pos[i] * cell_size,
        cell_size);
    sorted_buffer_size += cell_size;
  }

  // Write final batch
  if (sorted_buffer_size != 0) {
    st = write_sparse_attr_cmp(attribute_id, sorted_buffer, sorted_buffer_size);
  }
  // Clean up
  delete[] sorted_buffer;
  return st;
}

Status WriteState::write_sparse_unsorted_attr_var(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size,
    const std::vector<int64_t>& cell_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  Compressor compression = array_schema->compression(attribute_id);

  // No compression
  if (compression == Compressor::NO_COMPRESSION)
    return write_sparse_unsorted_attr_var_cmp_none(
        attribute_id,
        buffer,
        buffer_size,
        buffer_var,
        buffer_var_size,
        cell_pos);
  else  // All compressions
    return write_sparse_unsorted_attr_var_cmp(
        attribute_id,
        buffer,
        buffer_size,
        buffer_var,
        buffer_var_size,
        cell_pos);
}

Status WriteState::write_sparse_unsorted_attr_var_cmp_none(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size,
    const std::vector<int64_t>& cell_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = Configurator::cell_var_offset_size();
  size_t cell_var_size;
  const size_t* buffer_s = static_cast<const size_t*>(buffer);
  const char* buffer_var_c = static_cast<const char*>(buffer_var);

  // Check number of cells in buffer
  int64_t buffer_cell_num = buffer_size / cell_size;
  if (buffer_cell_num != int64_t(cell_pos.size())) {
    return LOG_STATUS(Status::Error(
        std::string("Cannot write sparse unsorted variable; "
                    "Invalid number of cells in attribute '") +
        array_schema->attribute(attribute_id) + "'"));
  }

  // Allocate a local buffer to hold the sorted cells
  char* sorted_buffer = new char[Configurator::sorted_buffer_size()];
  size_t sorted_buffer_size = 0;
  char* sorted_buffer_var = new char[Configurator::sorted_buffer_var_size()];
  size_t sorted_buffer_var_size = 0;

  // Sort and write attribute values in batches
  Status st;
  for (int64_t i = 0; i < buffer_cell_num; ++i) {
    // Calculate variable cell size
    cell_var_size = (cell_pos[i] == buffer_cell_num - 1) ?
                        buffer_var_size - buffer_s[cell_pos[i]] :
                        buffer_s[cell_pos[i] + 1] - buffer_s[cell_pos[i]];

    // Write batch
    if (sorted_buffer_size + cell_size > Configurator::sorted_buffer_size() ||
        sorted_buffer_var_size + cell_var_size >
            Configurator::sorted_buffer_var_size()) {
      st = write_sparse_attr_var_cmp_none(
          attribute_id,
          sorted_buffer,
          sorted_buffer_size,
          sorted_buffer_var,
          sorted_buffer_var_size);
      if (!st.ok()) {
        delete[] sorted_buffer;
        delete[] sorted_buffer_var;
        return st;
      }

      sorted_buffer_size = 0;
      sorted_buffer_var_size = 0;
    }

    // Keep on copying the cells in sorted order in the sorted buffer
    memcpy(
        sorted_buffer + sorted_buffer_size, &sorted_buffer_var_size, cell_size);
    sorted_buffer_size += cell_size;

    // Keep on copying the variable cells in sorted order in the sorted buffer
    memcpy(
        sorted_buffer_var + sorted_buffer_var_size,
        buffer_var_c + buffer_s[cell_pos[i]],
        cell_var_size);
    sorted_buffer_var_size += cell_var_size;
  }

  // Write final batch
  if (sorted_buffer_size != 0) {
    st = write_sparse_attr_var_cmp_none(
        attribute_id,
        sorted_buffer,
        sorted_buffer_size,
        sorted_buffer_var,
        sorted_buffer_var_size);
  }

  // Clean up
  delete[] sorted_buffer;
  delete[] sorted_buffer_var;

  return st;
}

Status WriteState::write_sparse_unsorted_attr_var_cmp(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size,
    const std::vector<int64_t>& cell_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = Configurator::cell_var_offset_size();
  size_t cell_var_size;
  const size_t* buffer_s = static_cast<const size_t*>(buffer);
  const char* buffer_var_c = static_cast<const char*>(buffer_var);

  // Check number of cells in buffer
  int64_t buffer_cell_num = buffer_size / cell_size;
  if (buffer_cell_num != int64_t(cell_pos.size())) {
    return LOG_STATUS(Status::Error(
        std::string("Cannot write sparse unsorted variable; "
                    "Invalid number of cells in attribute '") +
        array_schema->attribute(attribute_id) + "'"));
  }

  // Allocate a local buffer to hold the sorted cells
  char* sorted_buffer = new char[Configurator::sorted_buffer_size()];
  size_t sorted_buffer_size = 0;
  char* sorted_buffer_var = new char[Configurator::sorted_buffer_var_size()];
  size_t sorted_buffer_var_size = 0;

  // Sort and write attribute values in batches
  Status st;
  for (int64_t i = 0; i < buffer_cell_num; ++i) {
    // Calculate variable cell size
    cell_var_size = (cell_pos[i] == buffer_cell_num - 1) ?
                        buffer_var_size - buffer_s[cell_pos[i]] :
                        buffer_s[cell_pos[i] + 1] - buffer_s[cell_pos[i]];

    // Write batch
    if (sorted_buffer_size + cell_size > Configurator::sorted_buffer_size() ||
        sorted_buffer_var_size + cell_var_size >
            Configurator::sorted_buffer_var_size()) {
      st = write_sparse_attr_var_cmp(
          attribute_id,
          sorted_buffer,
          sorted_buffer_size,
          sorted_buffer_var,
          sorted_buffer_var_size);
      if (!st.ok()) {
        delete[] sorted_buffer;
        delete[] sorted_buffer_var;
        return st;
      }

      sorted_buffer_size = 0;
      sorted_buffer_var_size = 0;
    }

    // Keep on copying the cells in sorted order in the sorted buffer
    memcpy(
        sorted_buffer + sorted_buffer_size, &sorted_buffer_var_size, cell_size);
    sorted_buffer_size += cell_size;

    // Keep on copying the variable cells in sorted order in the sorted buffer
    memcpy(
        sorted_buffer_var + sorted_buffer_var_size,
        buffer_var_c + buffer_s[cell_pos[i]],
        cell_var_size);
    sorted_buffer_var_size += cell_var_size;
  }

  // Write final batch
  if (sorted_buffer_size != 0) {
    st = write_sparse_attr_var_cmp(
        attribute_id,
        sorted_buffer,
        sorted_buffer_size,
        sorted_buffer_var,
        sorted_buffer_var_size);
  }

  // Clean up
  delete[] sorted_buffer;
  delete[] sorted_buffer_var;

  // Success
  return st;
}

}  // namespace tiledb
