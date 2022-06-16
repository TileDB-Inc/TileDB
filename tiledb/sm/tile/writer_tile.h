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
class WriterTile {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  WriterTile(
      const ArraySchema& array_schema,
      const bool has_coords,
      const bool var_size,
      const bool nullable,
      const uint64_t cell_size,
      const Datatype type);

  /** Move constructor. */
  WriterTile(WriterTile&& tile);

  /** Move-assign operator. */
  WriterTile& operator=(WriterTile&& tile);

  DISABLE_COPY_AND_COPY_ASSIGN(WriterTile);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the fixed tile. */
  inline Tile& fixed_tile() {
    assert(!var_tile_.has_value());
    return fixed_tile_;
  }

  /** Returns the fixed tile. */
  inline const Tile& fixed_tile() const {
    assert(!var_tile_.has_value());
    return fixed_tile_;
  }

  /** Returns the offset tile. */
  inline Tile& offset_tile() {
    assert(var_tile_.has_value());
    return fixed_tile_;
  }

  /** Returns the offset tile. */
  inline const Tile& offset_tile() const {
    assert(var_tile_.has_value());
    return fixed_tile_;
  }

  /** Returns the var tile. */
  inline Tile& var_tile() {
    return *var_tile_;
  }

  /** Returns the var tile. */
  inline const Tile& var_tile() const {
    return *var_tile_;
  }

  /** Returns the validity tile. */
  inline Tile& validity_tile() {
    return *validity_tile_;
  }

  /** Returns the validity tile. */
  inline const Tile& validity_tile() const {
    return *validity_tile_;
  }

  /**
   * Returns the var pre-filtered size of the tile data in the buffer.
   *
   * @return Var pre-filtered size.
   */
  uint64_t var_pre_filtered_size() const;

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
  tuple<const void*, uint64_t, const void*, uint64_t, const ByteVec*, uint64_t>
  metadata() const;

  /**
   * Sets the tile metadata.
   *
   * @param md minimum, minimum size, maximum, maximum size, sum, null count.
   */
  void set_metadata(const tuple<
                    const void*,
                    uint64_t,
                    const void*,
                    uint64_t,
                    const ByteVec*,
                    uint64_t>& md);

  /**
   * Sets the final size of a written tile.
   *
   * @param size Final size.
   */
  inline void final_size(uint64_t size) {
    if (var_tile_.has_value()) {
      fixed_tile_.set_size(size * constants::cell_var_offset_size);
    } else {
      fixed_tile_.set_size(size * cell_size_);
    }

    if (validity_tile_.has_value()) {
      validity_tile_->set_size(size * constants::cell_validity_size);
    }
  }

  /**
   * Returns the cell number.
   *
   * @return Cell number.
   */
  inline uint64_t cell_num() const {
    return fixed_tile_.cell_num();
  }

  /** Swaps the contents (all field values) of this tile with the given tile. */
  void swap(WriterTile& tile);

 private:
  /* ********************************* */
  /*        PRIVATE CONSTRUCTOR        */
  /* ********************************* */
  WriterTile() = default;

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * Fixed data tile. Contains offsets for var size attribute/dimension and
   * the data itself in case of fixed sized attribute/dimension.
   */
  Tile fixed_tile_;

  /** Var data tile. */
  std::optional<Tile> var_tile_;

  /** Validity data tile. */
  std::optional<Tile> validity_tile_;

  /** Cell size for this attribute. */
  uint64_t cell_size_;

  /** The size in bytes of the var tile data before it has been filtered. */
  uint64_t var_pre_filtered_size_;

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
