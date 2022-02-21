/**
 * @file   writer_tile.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file defines class WriterTile.
 */

#ifndef TILEDB_WRITER_TILE_H
#define TILEDB_WRITER_TILE_H

#include "tiledb/sm/tile/tile.h"
#include "tiledb/sm/tile/tile_metadata_generator.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * Handles tile information, with added data used by writer.
 */
class WriterTile : public Tile {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  WriterTile();

  /** Move constructor. */
  WriterTile(WriterTile&& tile);

  /** Move-assign operator. */
  WriterTile& operator=(WriterTile&& tile);

  DISABLE_COPY_AND_COPY_ASSIGN(WriterTile);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Returns the pre-filtered size of the tile data in the buffer.
   *
   * @return Pre-filtered size.
   */
  uint64_t pre_filtered_size() const;

  /**
   * Sets the pre-filtered size value to the given value.
   *
   * @param pre_filtered_size Pre-filtered size.
   */
  void set_pre_filtered_size(uint64_t pre_filtered_size);

  /**
   * Returns the tile minimum value.
   *
   * @return tile minimum value.
   */
  void* min() const;

  /**
   * Returns the tile maximum value.
   *
   * @return tile maximum value.
   * */
  void* max() const;

  /**
   * Returns the tile metadata.
   *
   * @return minimum, minimum size, maximum, maximum size, sum, null count.
   */
  std::tuple<
      const void*,
      uint64_t,
      const void*,
      uint64_t,
      const ByteVec*,
      uint64_t>
  metadata() const;

  /**
   * Sets the tile metadata.
   *
   * @param md minimum, minimum size, maximum, maximum size, sum, null count.
   */
  void set_metadata(const std::tuple<
                    const void*,
                    uint64_t,
                    const void*,
                    uint64_t,
                    const ByteVec*,
                    uint64_t>& md);
  /**
   * Write method used for var data. Resizes the internal buffer if needed.
   *
   * @param data Pointer to the data to write.
   * @param offset Offset to write into the tile buffer.
   * @param nbytes Number of bytes to write.
   * @return Status.
   */
  Status write_var(const void* data, uint64_t offset, uint64_t nbytes);

  /**
   * Sets the final size of a written tile.
   *
   * @param size Final size.
   */
  inline void final_size(uint64_t size) {
    size_ = size;
  }

  /** Swaps the contents (all field values) of this tile with the given tile. */
  void swap(WriterTile& tile);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The size in bytes of the tile data before it has been filtered. */
  uint64_t pre_filtered_size_;

  /** Minimum value for this tile. */
  ByteVec min_;

  /** Minimum value size for this tile. */
  uint64_t min_size_;

  /** Maximum value for this tile. */
  ByteVec max_;

  /** Maximum value size for this tile. */
  uint64_t max_size_;

  /** Sum of values. */
  ByteVec sum_;

  /** Count of null values. */
  uint64_t null_count_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_WRITER_TILE_H
