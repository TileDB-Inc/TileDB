/**
 * @file   generic_tile_io.h
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
 * This file defines class GenericTileIO.
 */

#ifndef TILEDB_GENERIC_TILE_IO_H
#define TILEDB_GENERIC_TILE_IO_H

#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/storage_manager/context_resources.h"

using namespace tiledb::common;

namespace tiledb::sm {

class Tile;

/**
 * Header information for a generic tile. A generic tile is a tile residing
 * together with its metadata in one contiguous byte region of a file. This is
 * as opposed to a regular tile, where the metadata resides separately from
 * the tile data itself.
 */
struct GenericTileHeader {
  /** Constructor. */
  GenericTileHeader()
      : version_number(constants::format_version)
      , persisted_size(0)
      , tile_size(0)
      , datatype((uint8_t)Datatype::ANY)
      , cell_size(0)
      , encryption_type((uint8_t)EncryptionType::NO_ENCRYPTION)
      , filter_pipeline_size(0) {
  }

  /** Size in bytes of the non-filters part of the serialized header. */
  static const uint64_t BASE_SIZE =
      3 * sizeof(uint64_t) + 2 * sizeof(char) + 2 * sizeof(uint32_t);

  /**
   * The default number of bytes to add when reading the tile header.
   *
   * In order to skip a second read operation we can just read a few extra
   * bytes. GenericTileIO filter pipelines are currently hard coded to be
   * a gzip compression filter with an optional encryption filter. This means
   * that we know the maximum serialized pipeline size which is:
   *
   * - 1 byte for compression filter type
   * - 4 bytes for the compression filter serialized size
   * - 1 byte for the gzip compression type
   * - 4 bytes for the gzip compression level
   * - 1 byte for the encryption filter
   * - 4 bytes for the serialized encryption filter (which is zero)
   *
   * Which gives us a total of 15 bytes.
   */
  static const uint32_t FILTER_PIPELINE_SIZE = 15;

  /** The number of initial bytes to request when reading a header. */
  static const uint64_t HEADER_READ_SIZE = BASE_SIZE + FILTER_PIPELINE_SIZE;

  /** Format version number of the tile. */
  uint32_t version_number;

  /** Persisted (e.g. compressed) size of the tile. */
  uint64_t persisted_size;

  /** Uncompressed size of the tile. */
  uint64_t tile_size;

  /** Datatype of the tile. */
  uint8_t datatype;

  /** Cell size of the tile. */
  uint64_t cell_size;

  /** The type of the encryption used in filtering. */
  uint8_t encryption_type;

  /** Number of bytes in the serialized filter pipeline instance. */
  uint32_t filter_pipeline_size;

  /** Filter pipeline used to filter the tile. */
  FilterPipeline filters;
};

/** Handles IO (reading/writing) for tiles. */
class GenericTileIO {
 public:
  GenericTileIO() = delete;
  DISABLE_COPY_AND_COPY_ASSIGN(GenericTileIO);
  DISABLE_MOVE_AND_MOVE_ASSIGN(GenericTileIO);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Reads the generic tile header from the file.
   *
   * @param resources The ContextResources instance to use for reading.
   * @param uri The URI of the generic tile.
   * @param file_offset The offset where the header read will begin.
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @return GenericTileHeader The header read from storage.
   */
  static GenericTileHeader read_header(
      ContextResources& resources, const URI& uri, uint64_t offset = 0);

  /**
   * Read a Tile from persistent storage.
   *
   * @param resources The ContextResources to use.
   * @param uri The object URI.
   * @param offset The offset into the file to read from.
   * @param encryption_key The encryption key to use.
   * @return Tile with the data.
   */
  static Tile read(
      ContextResources& resources,
      const URI& uri,
      const EncryptionKey& key,
      uint64_t offset = 0);

  /**
   * Writes a tile generically to the file. This means that a header will be
   * prepended to the file before writing the tile contents. The reason is
   * that there will be no tile metadata retrieved from another source,
   * other than the file itself.
   *
   * @param tile The tile to be written.
   * @param key The encryption key to use.
   * @return uint64_t The total number of bytes written to the file.
   */
  static uint64_t write(
      ContextResources& resources,
      const URI& uri,
      WriterTile& tile,
      const EncryptionKey& key);
};

}  // namespace tiledb::sm

#endif  // TILEDB_GENERIC_TILE_IO_H
