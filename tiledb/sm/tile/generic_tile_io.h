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

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class StorageManager;
class Tile;

/** Handles IO (reading/writing) for tiles. */
class GenericTileIO {
 public:
  /* ********************************* */
  /*       PUBLIC TYPE DEFINITIONS     */
  /* ********************************* */

  /**
   * Header information for a generic tile. A generic tile is a tile residing
   * together with its metadata in one contiguous byte region of a file. This is
   * as opposed to a regular tile, where the metadata resides separately from
   * the tile data itself.
   */
  struct GenericTileHeader {
    /** Size in bytes of the non-filters part of the serialized header. */
    static const uint64_t BASE_SIZE =
        3 * sizeof(uint64_t) + 2 * sizeof(char) + 2 * sizeof(uint32_t);
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
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param storage_manager The storage manager.
   * @param uri The name of the file that stores data.
   */
  GenericTileIO(StorageManager* storage_manager, const URI& uri);

  GenericTileIO() = delete;
  DISABLE_COPY_AND_COPY_ASSIGN(GenericTileIO);
  DISABLE_MOVE_AND_MOVE_ASSIGN(GenericTileIO);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Reads a generic tile from the file. A generic tile is a tile residing
   * together with its metadata in one contiguous byte region of a file. This is
   * as opposed to a regular tile, where the metadata resides separately from
   * the tile data itself.
   *
   * Therefore, this function first reads a small header to retrieve appropriate
   * information about the tile, and then reads the tile data. Note that it
   * creates a new Tile object with the header information.
   *
   * @param tile The tile that will hold the read data.
   * @param file_offset The offset in the file to read from.
   * @param encryption_key The encryption key to use.
   * @param config The storage manager's config.
   * @return Status
   */
  Status read_generic(
      Tile** tile,
      uint64_t file_offset,
      const EncryptionKey& encryption_key,
      const Config& config);

  /**
   * Reads the generic tile header from the file.
   *
   * @param sm The StorageManager instance to use for reading.
   * @param uri The URI of the generic tile.
   * @param file_offset The offset where the header read will begin.
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param header The header to be retrieved.
   * @return Status
   */
  static Status read_generic_tile_header(
      const StorageManager* sm,
      const URI& uri,
      uint64_t file_offset,
      GenericTileHeader* header);

  /**
   * Writes a tile generically to the file. This means that a header will be
   * prepended to the file before writing the tile contents. The reason is
   * that there will be no tile metadata retrieved from another source,
   * other thant the file itself.
   *
   * @param tile The tile to be written.
   * @param encryption_key The encryption key to use.
   * @param nbytes The total number of bytes written to the file.
   * @return Status
   */
  Status write_generic(
      Tile* tile, const EncryptionKey& encryption_key, uint64_t* nbytes);

  /**
   * Writes the generic tile header to the file.
   *
   * @param header The header to write
   * @return Status
   */
  Status write_generic_tile_header(GenericTileHeader* header);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The storage manager object. */
  StorageManager* storage_manager_;

  /** The file URI. */
  URI uri_;

  /**
   * Configures the header's encryption filter with the given key.
   *
   * @param header Header instance to modify.
   * @param encryption_key The encryption key to use.
   * @return Status
   */
  Status configure_encryption_filter(
      GenericTileHeader* header, const EncryptionKey& encryption_key) const;

  /**
   * Initializes a generic tile header struct.
   *
   * This does not set the persisted size or filter pipeline size fields.
   *
   * @param tile The tile to initialize a header for
   * @param header The header to initialize
   * @param encryption_key The encryption key to use.
   * @return Status
   */
  Status init_generic_tile_header(
      Tile* tile,
      GenericTileHeader* header,
      const EncryptionKey& encryption_key) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_GENERIC_TILE_IO_H
