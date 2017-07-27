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
#include "configurator.h"
#include "tile.h"

namespace tiledb {

class TileIO {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  TileIO(const Configurator* config, const uri::URI& attr_filename);

  ~TileIO();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  Status write(Tile* tile, uint64_t* bytes_written);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  uri::URI attr_filename_;

  Buffer* buffer_;

  const Configurator* config_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  Status compress_tile(Tile* tile);

  Status compress_tile_gzip(Tile* tile, int level);

  Status compress_tile_zstd(Tile* tile, int level);

  Status compress_tile_lz4(Tile* tile, int level);

  Status compress_tile_blosc(Tile* tile, int level, const char* compressor);

  Status compress_tile_rle(Tile* tile, int level);

  Status compress_tile_bzip2(Tile* tile, int level);
};

}  // namespace tiledb

#endif  // TILEDB_TILE_IO_H
