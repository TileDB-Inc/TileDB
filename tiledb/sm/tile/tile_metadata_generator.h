/**
 * @file   tile_metadata_generator.h
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
 * This file defines class TileMetadataGenerator.
 */

#ifndef TILEDB_TILE_METADATA_GENERATOR_H
#define TILEDB_TILE_METADATA_GENERATOR_H

#include "tiledb/sm/tile/tile.h"

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/types.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class WriterTile;

/**
 * Sum structure used to bind a type and sum type to a sum and sum_nullable
 * functions.
 */
template <typename T, typename SUM_T>
struct Sum {
  /**
   * Compute the sum of a tile.
   *
   * @param tile Fixed data tile.
   */
  static ByteVec sum(Tile& tile);

  /**
   * Compute the sum of a nullable tile.
   *
   * @param tile Fixed data tile.
   * @param tile_validity Validity tile.
   */
  static ByteVec sum_nullable(const Tile& tile, const Tile& tile_validity);
};

/**
 * Specialization of the sum struct for int64_t sums.
 */
template <typename T>
struct Sum<T, int64_t> {
  static ByteVec sum(const Tile& tile);
  static ByteVec sum_nullable(const Tile& tile, const Tile& tile_validity);
};

/**
 * Specialization of the sum struct for uint64_t sums.
 */
template <typename T>
struct Sum<T, uint64_t> {
  static ByteVec sum(const Tile& tile);
  static ByteVec sum_nullable(const Tile& tile, const Tile& tile_validity);
};

/**
 * Specialization of the sum struct for double sums.
 */
template <typename T>
struct Sum<T, double> {
  static ByteVec sum(const Tile& tile);
  static ByteVec sum_nullable(const Tile& tile, const Tile& tile_validity);
};

// Declare static values to point to min/max default values.
#define METADATA_GENERATOR_TYPE_DATA(T, SUM_T)                 \
  template <>                                                  \
  struct metadata_generator_type_data<T> {                     \
    using type = T;                                            \
    typedef SUM_T sum_type;                                    \
    constexpr static T min = std::numeric_limits<T>::max();    \
    constexpr static T max = std::numeric_limits<T>::lowest(); \
  };

/** Convert tiledb_datatype_t to a type. **/
template <typename T>
struct metadata_generator_type_data;

METADATA_GENERATOR_TYPE_DATA(std::byte, int64_t);
METADATA_GENERATOR_TYPE_DATA(char, int64_t);
METADATA_GENERATOR_TYPE_DATA(int8_t, int64_t);
METADATA_GENERATOR_TYPE_DATA(uint8_t, uint64_t);
METADATA_GENERATOR_TYPE_DATA(int16_t, int64_t);
METADATA_GENERATOR_TYPE_DATA(uint16_t, uint64_t);
METADATA_GENERATOR_TYPE_DATA(int32_t, int64_t);
METADATA_GENERATOR_TYPE_DATA(uint32_t, uint64_t);
METADATA_GENERATOR_TYPE_DATA(int64_t, int64_t);
METADATA_GENERATOR_TYPE_DATA(uint64_t, uint64_t);
METADATA_GENERATOR_TYPE_DATA(float, double);
METADATA_GENERATOR_TYPE_DATA(double, double);

/**
 * Generate metadata for a tile using the tile, tile var, and tile validity.
 */
class TileMetadataGenerator {
 public:
  /* ****************************** */
  /*           STATIC API           */
  /* ****************************** */

  /**
   * Does this datatype have min/max metadata.
   *
   * @param type Data type.
   * @param is_dim Is it a dimension.
   * @param var_size Is the attribute var size.
   * @param cell_val_num Number of values per cell.
   * @return bool.
   */
  static bool has_min_max_metadata(
      const Datatype type,
      const bool is_dim,
      const bool var_size,
      const uint64_t cell_val_num);

  /**
   * Does this datatype have sum metadata.
   *
   * @param type Data type.
   * @param var_size Is the attribute/dimension var size?
   * @param cell_val_num Number of values per cell.
   *
   * @return bool.
   */
  static bool has_sum_metadata(
      const Datatype type, const bool var_size, const uint64_t cell_val_num);

  /**
   * Returns the min and max of a fixed data tile.
   *
   * @param type_data Type data struct.
   * @param tile Tile to process.
   * @param cell_size Cell size.
   *
   * @return minimum, maximum.
   */
  template <class T>
  static const tuple<void*, void*> min_max(
      const Tile& tile, const uint64_t cell_size);

  /**
   * Returns the min and max of a fixed data tile with nullable values.
   *
   * @param type_data Type data struct.
   * @param tile Tile to process.
   * @param tile_validity Validity tile.
   * @param cell_size Cell size.
   *
   * @return minimum, maximum, null count.
   */
  template <class T>
  static const tuple<void*, void*, uint64_t> min_max_nullable(
      const Tile& tile, const Tile& tile_validity, const uint64_t cell_size);

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * @param type Data type.
   * @param is_dim Is it a dimension.
   * @param var_size Is the attribute/dimension var size?
   * @param cell_size Cell size.
   * @param cell_val_num Number of values per cell.
   */
  TileMetadataGenerator(
      const Datatype type,
      const bool is_dim,
      const bool var_size,
      const uint64_t cell_size,
      const uint64_t cell_val_num);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Compute metatada.
   *
   * @param tile The tile.
   */
  void process_tile(WriterTile& tile);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The data type. */
  Datatype type_;

  /** Minimum value for this tile. */
  const void* min_;

  /** Minimum value size for this tile. */
  uint64_t min_size_;

  /** Maximum value for this tile. */
  const void* max_;

  /** Maximum value size for this tile. */
  uint64_t max_size_;

  /** Sum of values. */
  ByteVec sum_;

  /** Count of null values. */
  uint64_t null_count_;

  /** Cell size. */
  uint64_t cell_size_;

  /** This metadata stores sums. */
  bool has_min_max_;

  /** This metadata stores sums. */
  bool has_sum_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Process var size attribute.
   *
   * @param tile The tile.
   */

  void process_tile_var(WriterTile& tile);

  /**
   * Process fixed size attribute.
   *
   * @param tile The fixed size tile.
   * @param tile_validity The validity tile.
   */
  template <class T>
  void process_tile(WriterTile& tile);

  /**
   * Min max function for var sized attributes.
   *
   * @param value Value to compare current min against.
   * @param size Value size.
   */
  void min_max_var(const char* value, const uint64_t size);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_TILE_METADATA_GENERATOR_H
