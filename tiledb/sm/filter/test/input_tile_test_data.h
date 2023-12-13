/**
 * @file input_filter_test_data.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * TODO: Add description before merging.
 */

#ifndef TILEDB_INPUT_TILE_TEST_DATA_H
#define TILEDB_INPUT_TILE_TEST_DATA_H

#include <test/support/tdb_catch.h>
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::common;

namespace tiledb::sm {

class TileDataGenerator {
 public:
  virtual ~TileDataGenerator() = default;

  virtual uint64_t cell_size() const = 0;
  /**
   * Checks if the provide tile has the same data as a writer tile created
   * by this class.
   */
  virtual void check_tile_data(const Tile& tile) const = 0;

  virtual Datatype datatype() const = 0;

  /**
   * Returns an empty writer tile with enough room for the input data.
   */
  WriterTile create_empty_writer_tile() const {
    return WriterTile(
        constants::format_version,
        datatype(),
        cell_size(),
        original_tile_size());
  }

  /**
   * Returns a writer tile with the input test data.
   */
  virtual WriterTile create_writer_tile() const = 0;

  /**
   * Returns a tile with the data from the filtered buffer and enough room
   * for the original tile data.
   **/
  Tile create_filtered_buffer_tile(FilteredBuffer& filtered_buffer) const {
    return Tile(
        constants::format_version,
        datatype(),
        cell_size(),
        0,
        original_tile_size(),
        filtered_buffer.data(),
        filtered_buffer.size());
  }

  virtual uint64_t original_tile_size() const = 0;
};

/**
 * This class creates and owns a tile with Datatype::UINT64 data. The data
 * increases by one for each element.
 */
class SimpleFixedTileData : public TileDataGenerator {
 public:
  SimpleFixedTileData(uint64_t num_elements)
      : num_elements_{num_elements}
      , cell_size_{sizeof(uint64_t)}
      , datatype_{Datatype::UINT64}
      , original_tile_size_{num_elements_ * sizeof(uint64_t)} {
  }

  uint64_t cell_size() const override {
    return cell_size_;
  }

  void check_tile_data(const Tile& tile) const override {
    INFO("Place holder");
    for (uint64_t index = 0; index < num_elements_; ++index) {
      uint64_t element{};
      CHECK_NOTHROW(
          tile.read(&element, index * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(element == index);
    }
  }

  WriterTile create_writer_tile() const override {
    auto tile = create_empty_writer_tile();
    for (uint64_t index = 0; index < num_elements_; ++index) {
      CHECK_NOTHROW(
          tile.write(&index, index * sizeof(uint64_t), sizeof(uint64_t)));
    }
    return tile;
  }

  Datatype datatype() const override {
    return datatype_;
  }

  uint64_t original_tile_size() const override {
    return original_tile_size_;
  }

 private:
  uint64_t num_elements_;
  uint64_t cell_size_;
  Datatype datatype_;
  uint64_t original_tile_size_;
};

}  // namespace tiledb::sm

#endif
