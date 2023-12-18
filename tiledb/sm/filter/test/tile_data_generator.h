/**
 * @file tile_data_generator.h
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
 * Class for generating data on a writer data and checking for that same data
 * on another tile.
 */

#ifndef TILEDB_INPUT_TILE_TEST_DATA_H
#define TILEDB_INPUT_TILE_TEST_DATA_H

#include <test/support/tdb_catch.h>
#include <algorithm>
#include <optional>
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

  /** Returns the datatype of the original data stored in this test. */
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
   * Returns the writer tile and optional writer offsets tile.
   *
   * If the data is fixed, the offsets tile will be a null opt.
   *
   * @returns [writer_tile, writer_offsets_tile] The writer tile with the input
   * test data and the writer offsets tile with the (optional) input offsets
   * data.
   */
  virtual std::tuple<WriterTile, std::optional<WriterTile>>
  create_writer_tiles() const = 0;

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

  /** Returns the size of the original unfiltered data. */
  virtual uint64_t original_tile_size() const = 0;
};

/**
 * This is a simple tile data generator that simply contains incremental values
 * to store int he tile.
 *
 * @WARNING This test data modifies the max tile chunk size. This is required to
 * get the expected data chunks in the filtered buffer.
 */
template <typename T, Datatype type>
class IncrementTileDataGenerator : public TileDataGenerator {
 public:
  /**
   * Constructor for variable length data with multiple chunks.
   *
   * @param cells_per_value Number of cells in each value. Used for generating
   * offsets.
   */
  IncrementTileDataGenerator(const std::vector<uint64_t>& cells_per_value)
      : num_elements_{std::accumulate(
            cells_per_value.cbegin(),
            cells_per_value.cend(),
            static_cast<uint64_t>(0))}
      , cells_per_value_{cells_per_value}
      , original_tile_size_{num_elements_ * sizeof(T)} {
  }

  /** Constructor for fixed data with all data in a single chunk. */
  IncrementTileDataGenerator(uint64_t num_elements)
      : num_elements_{num_elements}
      , cells_per_value_{}
      , cells_per_chunk_{num_elements_}
      , original_tile_size_{num_elements_ * sizeof(T)} {
  }

  ~IncrementTileDataGenerator() {
    WriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
  }

  uint64_t cell_size() const override {
    return sizeof(T);
  }

  void check_tile_data(const Tile& tile) const override {
    T expected{};
    for (uint64_t index = 0; index < num_elements_; ++index) {
      T element{};
      CHECK_NOTHROW(tile.read(&element, index * sizeof(T), sizeof(T)));
      CHECK(element == expected++);
    }
  }

  std::tuple<WriterTile, std::optional<WriterTile>> create_writer_tiles()
      const override {
    // Writer tile.
    auto tile = create_empty_writer_tile();
    T value{};
    for (uint64_t index = 0; index < num_elements_; ++index) {
      CHECK_NOTHROW(tile.write(&value, index * sizeof(T), sizeof(T)));
      ++value;
    }

    // If no cells per value data, then this is fixed length data and there is
    // no offsets tile.
    if (cells_per_value_.empty()) {
      return {std::move(tile), std::nullopt};
    }

    // If cells_per_value_ is not empty, construct a vector of offsets values.
    std::vector<uint64_t> offsets;
    offsets.reserve(cells_per_value_.size() + 1);
    offsets.push_back(0);
    for (auto num_cells : cells_per_value_) {
      offsets.push_back(offsets.back() + num_cells * sizeof(uint64_t));
    }
    offsets.pop_back();

    // Write the offsets tile.
    WriterTile offsets_tile(
        constants::format_version,
        Datatype::UINT64,
        constants::cell_var_offset_size,
        offsets.size() * constants::cell_var_offset_size);
    for (uint64_t index = 0; index < offsets.size(); ++index) {
      CHECK_NOTHROW(offsets_tile.write(
          &offsets[index],
          index * constants::cell_var_offset_size,
          constants::cell_var_offset_size));
    }

    return {std::move(tile), std::move(offsets_tile)};
  }

  Datatype datatype() const override {
    return type;
  }

  uint64_t original_tile_size() const override {
    return original_tile_size_;
  }

 private:
  uint64_t num_elements_;
  std::vector<uint64_t> cells_per_value_;
  std::vector<uint64_t> cells_per_chunk_;
  uint64_t original_tile_size_;
};

}  // namespace tiledb::sm

#endif
