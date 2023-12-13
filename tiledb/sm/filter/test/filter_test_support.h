/**
 * @file filter_test_support.h
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

#ifndef TILEDB_FILTER_TEST_SUPPORT_H
#define TILEDB_FILTER_TEST_SUPPORT_H

#include <test/support/tdb_catch.h>
#include <vector>
#include "tiledb/sm/tile/filtered_buffer.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::sm;

namespace tiledb::sm {

class ChunkInfo {
 public:
  ChunkInfo() = default;

  ChunkInfo(
      uint32_t original_chunk_length,
      uint32_t filtered_chunk_length,
      uint32_t metadata_length);

  ChunkInfo(const FilteredBuffer& buffer, uint32_t chunk_offset);

  inline uint32_t original_chunk_length() const {
    return original_chunk_length_;
  }

  inline uint32_t filtered_chunk_length() const {
    return filtered_chunk_length_;
  }

  inline uint64_t filtered_chunk_offset() const {
    return 3 * sizeof(uint32_t) + static_cast<uint64_t>(metadata_length_);
  }

  inline uint32_t metadata_length() const {
    return metadata_length_;
  }

  inline uint64_t metadata_offset() const {
    return 3 * sizeof(uint32_t);
  }

  inline uint64_t size() const {
    return 3 * sizeof(uint32_t) +
           static_cast<uint64_t>(filtered_chunk_length_) +
           static_cast<uint64_t>(metadata_length_);
  }

 private:
  uint32_t original_chunk_length_{};
  uint32_t filtered_chunk_length_{};
  uint32_t metadata_length_{};
};

class FilteredBufferChunkInfo {
 public:
  FilteredBufferChunkInfo(const FilteredBuffer& buffer);

  inline const ChunkInfo& chunk_info(uint64_t index) const {
    return chunk_info_[index];
  }

  inline uint64_t chunk_offset(uint64_t index) const {
    return offsets_[index];
  }

  inline uint64_t nchunks() const {
    return nchunks_;
  }

  inline uint64_t size() const {
    return size_;
  }

 private:
  uint64_t nchunks_{};
  std::vector<ChunkInfo> chunk_info_;
  std::vector<uint64_t> offsets_{};
  uint64_t size_{};
};

/**
 * Helper for checking chunk data.
 *
 * @tparam Type of the filtered data.
 */
class ChunkChecker {
 public:
  virtual ~ChunkChecker() = default;

  void check(
      const FilteredBuffer& buffer,
      const FilteredBufferChunkInfo& buffer_info,
      uint64_t chunk_index) const;

  virtual void check_filtered_data(
      const FilteredBuffer& buffer,
      const ChunkInfo& chunk_info,
      uint64_t chunk_offset) const = 0;

  virtual void check_metadata(
      const FilteredBuffer& buffer,
      const ChunkInfo& chunk_info,
      uint64_t chunk_offset) const = 0;

  virtual const ChunkInfo& expected_chunk_info() const = 0;
};

template <typename T>
class GridChunkChecker : public ChunkChecker {
 public:
  GridChunkChecker(
      uint32_t original_chunk_length,
      uint32_t num_filtered_elements,
      T starting_value,
      T spacing)
      : expected_chunk_info_{original_chunk_length, static_cast<uint32_t>(num_filtered_elements * sizeof(T)), 0}
      , num_filtered_elements_{num_filtered_elements}
      , starting_value_{starting_value}
      , spacing_{spacing} {
  }

  void check_filtered_data(
      const FilteredBuffer& buffer,
      const ChunkInfo& chunk_info,
      uint64_t chunk_offset) const override {
    // Check the size of the filtered data. If it does not match the
    // expected size, then end the test as a failure.
    REQUIRE(
        chunk_info.filtered_chunk_length() ==
        expected_chunk_info_.filtered_chunk_length());

    // Check the data.
    const auto data_offset = chunk_info.filtered_chunk_offset() + chunk_offset;
    for (uint32_t index = 0; index < num_filtered_elements_; ++index) {
      auto offset = data_offset + index * sizeof(T);
      T expected_value = starting_value_ + index * spacing_;
      T actual_value = buffer.value_at_as<uint64_t>(offset);
      CHECK(actual_value == expected_value);
    }
  }

  void check_metadata(
      const FilteredBuffer&,
      const ChunkInfo& chunk_info,
      uint64_t) const override {
    CHECK(chunk_info.metadata_length() == 0);
  }

  const ChunkInfo& expected_chunk_info() const override {
    return expected_chunk_info_;
  }

 private:
  ChunkInfo expected_chunk_info_{};
  uint32_t num_filtered_elements_{};
  T starting_value_{};
  T spacing_{};
};

class FilteredBufferChecker {
 public:
  FilteredBufferChecker() = default;

  template <typename T>
  void add_grid_chunk_checker(
      uint32_t original_chunk_length,
      uint32_t num_filtered_elements,
      T starting_value,
      T spacing = 1) {
    chunk_checkers_.emplace_back(tdb_new(
        GridChunkChecker<T>,
        original_chunk_length,
        num_filtered_elements,
        starting_value,
        spacing));
  }

  void check(const FilteredBuffer& buffer) const;

 private:
  std::vector<tdb_unique_ptr<ChunkChecker>> chunk_checkers_;
};

}  // namespace tiledb::sm

#endif
