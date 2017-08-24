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

#include <uri.h>
#include "attribute.h"
#include "config.h"
#include "tile.h"

namespace tiledb {

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
   * @param config The config object.
   * @param attr_filename The name of the file that stores attribute data.
   */
  TileIO(const Config* config, const uri::URI& attr_filename);

  /** Destructor. */
  ~TileIO();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Retrieves the size of the attribute file. */
  Status file_size(off_t* size) const;

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
   * Reads from a tile into a buffer. The reason this function is in TileIO is
   * because the input tile contains only information about how to locate the
   * data in the attribute file. The tile does not actually store the data in
   * main memory. This is used for example when the IO method is a system read.
   *
   * @param tile The tile to read from.
   * @param buffer The buffer to write to.
   * @param nbytes The number of bytes to read.
   * @return Status.
   */
  Status read_from_tile(Tile* tile, void* buffer, uint64_t nbytes);

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

  /** The attribute file name. */
  uri::URI attr_filename_;

  /**
   * An internal buffer used to facilitate compression/decompression (or
   * other future filters).
   */
  Buffer* buffer_;

  /** Config object. */
  const Config* config_;

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
   * @return Status
   */
  Status compress_tile_gzip(Tile* tile, int level);

  /**
   * Compresses a tile with zstd. The compressed data are written in buffer_.
   *
   * @param tile The tile to be compressed.
   * @return Status
   */
  Status compress_tile_zstd(Tile* tile, int level);

  /**
   * Compresses a tile with lz4. The compressed data are written in buffer_.
   *
   * @param tile The tile to be compressed.
   * @return Status
   */
  Status compress_tile_lz4(Tile* tile, int level);

  /**
   * Compresses a tile with blosc. The compressed data are written in buffer_.
   *
   * @param tile The tile to be compressed.
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
   * @return Status
   */
  Status compress_tile_bzip2(Tile* tile, int level);

  /**
   * Decompresses buffer_ into a tile. .
   *
   * @param tile The tile to be decompressed.
   * @param tile_size The original size of the (decompressed) tile.
   * @return Status
   */
  Status decompress_tile(Tile* tile, uint64_t tile_size);
};

}  // namespace tiledb

#endif  // TILEDB_TILE_IO_H
