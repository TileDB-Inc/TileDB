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

class InputTileTestData {
 public:
  virtual ~InputTileTestData() = default;
  virtual uint64_t cell_size() const = 0;
  virtual void check_tile_data(const Tile& tile) const = 0;
  virtual WriterTile create_tile() const = 0;
  virtual uint64_t tile_size() const = 0;
};

template <typename T>
class IncreasingInputTileTestData : public InputTileTestData {
 public:
  IncreasingInputTileTestData(uint64_t num_elements)
      : num_elements_{num_elements} {
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

  uint64_t cell_size() const override {
    return num_elements_ * sizeof(T);
  }

  uint64_t tile_size() const override {
    return num_elements_ * sizeof(T);
  }

  WriterTile create_tile() const override;

 private:
  uint64_t num_elements_;
};

template <>
WriterTile IncreasingInputTileTestData<uint64_t>::create_tile() const {
  const uint64_t tile_size = num_elements_ * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  WriterTile tile(
      constants::format_version, Datatype::UINT64, cell_size, tile_size);
  for (uint64_t index = 0; index < num_elements_; ++index) {
    CHECK_NOTHROW(
        tile.write(&index, index * sizeof(uint64_t), sizeof(uint64_t)));
  }

  return tile;
}

}  // namespace tiledb::sm

#endif
