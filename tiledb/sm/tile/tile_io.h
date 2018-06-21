/**
 * @file   tile_io.h
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
 * This file defines class TileIO.
 */

#ifndef TILEDB_TILE_IO_H
#define TILEDB_TILE_IO_H

#include "tiledb/sm/misc/uri.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/tile.h"

namespace tiledb {
namespace sm {

class StorageManager;

/** Handles IO (reading/writing) for tiles. */
class TileIO {
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
    /** Size, in bytes, of the serialized header. */
    static const uint64_t SIZE =
        3 * sizeof(uint64_t) + 2 * sizeof(char) + sizeof(int);
    /** Compressed size of the tile. */
    uint64_t compressed_size;
    /** Uncompressed size of the tile. */
    uint64_t tile_size;
    /** Datatype of the tile. */
    char datatype;
    /** Cell size of the tile. */
    uint64_t cell_size;
    /** Compressor of the tile. */
    char compressor;
    /** Compression level of the tile. */
    int compression_level;

    /** Constructor. */
    GenericTileHeader()
        : compressed_size(0)
        , tile_size(0)
        , datatype((char)Datatype::ANY)
        , cell_size(0)
        , compressor((char)Compressor::NO_COMPRESSION)
        , compression_level(-1) {
    }
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  TileIO();

  /**
   * Constructor.
   *
   * @param storage_manager The storage manager.
   * @param uri The name of the file that stores data.
   */
  TileIO(StorageManager* storage_manager, const URI& uri);

  /**
   * Constructor.
   *
   * @param storage_manager The storage manager.
   * @param uri The name of the file that stores data.
   * @param file_size The size of the file pointed by `uri`.
   */
  TileIO(StorageManager* storage_manager, const URI& uri, uint64_t file_size);

  /** Destructor. */
  ~TileIO();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the size of the file. */
  uint64_t file_size() const;

  /**
   * Checks whether the file at the given URI is a valid generic tile.
   *
   * @param sm StorageManager instance to use.
   * @param uri The file URI.
   * @param is_generic_tile Set to `true` iff the file is a valid generic tile.
   * @return Status
   */
  static Status is_generic_tile(
      const StorageManager* sm, const URI& uri, bool* is_generic_tile);

  /**
   * Reads into a tile from the file.
   *
   * @param tile The tile to read into.
   * @param file_offset The offset in the file to read from.
   * @param compressed_size The size of the compressed tile.
   * @param tile_size The size of the decompressed tile.
   * @param cache_hit Set to true if the tile was read from the cache.
   * @return Status.
   */
  Status read(
      Tile* tile,
      uint64_t file_offset,
      uint64_t compressed_size,
      uint64_t tile_size,
      bool* cache_hit);

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
   * @return Status
   */
  Status read_generic(Tile** tile, uint64_t file_offset);

  /**
   * Reads the generic tile header from the file.
   *
   * @param sm The StorageManager instance to use for reading.
   * @param uri The URI of the generic tile.
   * @param file_offset The offset where the header read will begin.
   * @param header The header to be retrieved.
   * @return Status
   */
  static Status read_generic_tile_header(
      const StorageManager* sm,
      const URI& uri,
      uint64_t file_offset,
      GenericTileHeader* header);

  /**
   * Writes (appends) a tile into the file.
   *
   * @param tile The tile to be written.
   * @param bytes_written The actual number of bytes written. This may be
   *     different from the tile size, since the write function may
   *     invoke a filter such as compression prior to writing to the file.
   * @return Status.
   */
  Status write(Tile* tile, uint64_t* bytes_written);

  /**
   * Writes a tile generically to the file. This means that a header will be
   * prepended to the file before writing the tile contents. The reason is
   * that there will be no tile metadata retrieved from another source,
   * other thant the file itself.
   *
   * @param tile The tile to be written.
   * @return Status
   */
  Status write_generic(Tile* tile);

  /**
   * Writes the generic tile header to the file.
   *
   * @param tile The tile whose header will be written.
   * @param compressed_size The size that the (potentially) compressed tile
   *     will occupy in the file.
   * @return Status
   */
  Status write_generic_tile_header(Tile* tile, uint64_t compressed_size);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * An internal buffer used to facilitate compression/decompression (or
   * other future filters).
   */
  Buffer* buffer_;

  /** The size of the file pointed by `uri_`. */
  uint64_t file_size_;

  /** The storage manager object. */
  StorageManager* storage_manager_;

  /** The file URI. */
  URI uri_;

  /* ********************************* */
  /*      PRIVATE TYPE DEFINITIONS     */
  /* ********************************* */

  /**
   * Encapsulated information about tile chunks for decompression.
   */
  struct DecompressionChunkInfo {
    /** Number of chunks to decompress. */
    uint64_t chunk_num_;
    /** Pointer to start of compressed chunk data. */
    const char* compressed_chunks_;
    /** Pointer to start of destination buffer for decompressed chunks. */
    char* decompressed_chunks_;
    /** Vector of (offset, len) pairs for the compressed chunks. */
    std::vector<std::pair<uint64_t, uint64_t>> compressed_chunk_info_;
    /**
     * Vector of (offset, len) pairs for the decompressed chunk destinations.
     */
    std::vector<std::pair<uint64_t, uint64_t>> decompressed_chunk_info_;
    /** Total number of bytes that will be decompressed. */
    uint64_t total_decompressed_bytes_;

    /** Constructor. */
    DecompressionChunkInfo()
        : chunk_num_(0)
        , compressed_chunks_(nullptr)
        , decompressed_chunks_(nullptr)
        , total_decompressed_bytes_(0) {
    }
  };

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Checks if the given number of bytes can be compressed in a single block.
   *
   * @param nbytes Number of bytes including overhead to check.
   * @return True if the given number of bytes can be compressed
   */
  bool can_compress_nbytes(uint64_t nbytes) const;

  /**
   * Compresses a tile. The compressed data are written in buffer_.
   * Note that a coordinates tile must be split into one tile per
   * dimension. In that case *compress_one_tile* will be invoked
   * for each dimension sub-tile.
   *
   * @param tile The tile to be compressed.
   * @return Status
   */
  Status compress_tile(Tile* tile);

  /**
   * Compresses a single tile. The compressed data are written in buffer_.
   *
   * @param tile The tile to be compressed.
   * @return Status
   */
  Status compress_one_tile(Tile* tile);

  /**
   * Computes necessary info for chunking a tile upon compression.
   *
   * @param tile The tile whose chunking info is being computed.
   * @param chunk_num The computed number of chunks.
   * @param chunk_size The computed chunk size (a multiple of the cell size).
   * @param chunk_overhead The compression overhead for `chunk_size` bytes.
   * @param total_overhead The total compression overhead.
   * @return Status
   */
  Status compute_chunking_info(
      Tile* tile,
      uint64_t* chunk_num,
      uint64_t* chunk_size,
      uint64_t* chunk_overhead,
      uint64_t* total_overhead);

  /**
   * Computes necessary info for decompressing chunks of a tile.
   *
   * @param tile The tile whose chunking info is being computed.
   * @param info The info that will be computed.
   * @return Status
   */
  Status compute_decompression_chunk_info(
      Tile* tile, DecompressionChunkInfo* info);

  /**
   * Decompresses buffer_ into a tile.
   * Note that a coordinates tile was split into one tile per
   * dimension. In that case *decompress_one_tile* will be invoked
   * for each dimension sub-tile.
   *
   * @param tile The tile where the decompressed data will be stored.
   * @return Status
   */
  Status decompress_tile(Tile* tile);

  /**
   * Decompresses buffer_ into a tile.
   *
   * @param tile The tile where the decompressed data will be stored.
   * @return Status
   */
  Status decompress_one_tile(Tile* tile);

  /** Computes the compression overhead on *nbytes* of the input tile. */
  uint64_t overhead(Tile* tile, uint64_t nbytes) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_TILE_IO_H
