/**
 * @file   tile_io.h
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
 * This file defines class TileIO.
 */

#ifndef TILEDB_TILE_IO_H
#define TILEDB_TILE_IO_H

#include "attribute.h"
#include "storage_manager.h"
#include "tile.h"
#include "uri.h"

namespace tiledb {

class StorageManager;

/** Handles IO (reading/writing) for tiles. */
class TileIO {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  TileIO() = default;

  /**
   * Constructor.
   *
   * @param storage_manager The storage manager.
   * @param attr_uri The name of the file that stores attribute data.
   */
  TileIO(StorageManager* storage_manager, const URI& attr_uri);

  /** Destructor. */
  ~TileIO();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Retrieves the size of the attribute file. */
  Status file_size(uint64_t* size) const;

  /**
   * Reads into a tile from the attribute file.
   *
   * @param tile The tile to read into.
   * @param file_offset The offset in the attribute file to read from.
   * @param compressed_size The size of the compressed tile.
   * @param tile_size The size of the decompressed tile.
   * @return Status.
   */
  Status read(
      Tile* tile,
      uint64_t file_offset,
      uint64_t compressed_size,
      uint64_t tile_size);

  /**
   * Writes (appends) a tile into the attribute file.
   *
   * @param tile The tile to be written.
   * @param bytes_written The actual number of bytes written. This may be
   *     different from the tile size, since the write function may
   *     invoke a filter such as compression prior to writing to the file.
   * @return Status.
   */
  Status write(Tile* tile, uint64_t* bytes_written);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The attribute URI. */
  URI attr_uri_;

  /**
   * An internal buffer used to facilitate compression/decompression (or
   * other future filters).
   */
  Buffer* buffer_;

  /** The storage manager object. */
  StorageManager* storage_manager_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Compresses a tile. The compressed data are written in buffer_.
   *
   * @param tile The tile to be compressed.
   * @return Status
   */
  Status compress_tile(Tile* tile);

  /**
   * Compresses a tile with gzip. The compressed data are written in buffer_.
   *
   * @param tile The tile to be compressed.
   * @param level The compression level.
   * @return Status
   */
  Status compress_tile_gzip(Tile* tile, int level);

  /**
   * Compresses a tile with zstd. The compressed data are written in buffer_.
   *
   * @param tile The tile to be compressed.
   * @param level The compression level.
   * @return Status
   */
  Status compress_tile_zstd(Tile* tile, int level);

  /**
   * Compresses a tile with lz4. The compressed data are written in buffer_.
   *
   * @param tile The tile to be compressed.
   * @param level The compression level.
   * @return Status
   */
  Status compress_tile_lz4(Tile* tile, int level);

  /**
   * Compresses a tile with blosc. The compressed data are written in buffer_.
   *
   * @param tile The tile to be compressed.
   * @param level The compression level.
   * @return Status
   */
  Status compress_tile_blosc(Tile* tile, int level, const char* compressor);

  /**
   * Compresses a tile with RLE. The compressed data are written in buffer_.
   *
   * @param tile The tile to be compressed.
   * @return Status
   */
  Status compress_tile_rle(Tile* tile);

  /**
   * Compresses a tile with bzip2. The compressed data are written in buffer_.
   *
   * @param tile The tile to be compressed.
   * @param level The compression level.
   * @return Status
   */
  Status compress_tile_bzip2(Tile* tile, int level);

  /**
   * Compresses a tile with double delta. The compressed data are written in
   * buffer_.
   *
   * @param tile The tile to be compressed.
   * @return Status
   */
  Status compress_tile_double_delta(Tile* tile);

  /**
   * Decompresses buffer_ into a tile. .
   *
   * @param tile The tile where the decompressed data will be stored.
   * @param tile_size The original size of the (decompressed) tile.
   * @return Status
   */
  Status decompress_tile(Tile* tile, uint64_t tile_size);

  /**
   * Decompresses buffer_ into a tile.
   *
   * @param tile The tile where the decompressed data will be stored.
   * @param tile_size The original size of the (decompressed) tile.
   * @return Status
   */
  Status decompress_tile_double_delta(Tile* tile, uint64_t tile_size);
};

}  // namespace tiledb

#endif  // TILEDB_TILE_IO_H
