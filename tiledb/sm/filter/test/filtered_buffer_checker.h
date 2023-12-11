/**
 * @file filtered_buffer_checker.h
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
 * This file contains  helper classes for checking the data in a FilteredBuffer
 * is as expected after running a filter pipeline forward.
 *
 */

#ifndef TILEDB_FILTERED_BUFFER_CHECKER_H
#define TILEDB_FILTERED_BUFFER_CHECKER_H

#include <test/support/tdb_catch.h>
#include <vector>
#include "tiledb/sm/tile/filtered_buffer.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::sm;

namespace tiledb::sm {

class FilteredBufferChunkInfo;

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

/** Helper for checking chunk data. */
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

/**
 * A chunk checker for data that increases by a fixed amount at each subsequent
 * component. It supports checksum metadata.
 *
 * This is for testing data on file using the test-specific filters.
 *
 * @tparam Type of the filtered data.
 */
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
      , spacing_{spacing}
      , checksum_{} {
  }

  GridChunkChecker(
      uint32_t original_chunk_length,
      uint32_t num_filtered_elements,
      T starting_value,
      T spacing,
      std::vector<uint64_t> checksum)
      : expected_chunk_info_{original_chunk_length, static_cast<uint32_t>(num_filtered_elements * sizeof(T)), 0}
      , num_filtered_elements_{num_filtered_elements}
      , starting_value_{starting_value}
      , spacing_{spacing}
      , checksum_{checksum} {
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
    auto data_offset = chunk_info.filtered_chunk_offset() + chunk_offset;
    for (uint32_t index = 0; index < num_filtered_elements_; ++index) {
      auto offset = data_offset + index * sizeof(T);
      T expected_value = starting_value_ + index * spacing_;
      T actual_value = buffer.value_at_as<T>(offset);
      CHECK(actual_value == expected_value);
    }
  }

  void check_metadata(
      const FilteredBuffer& buffer,
      const ChunkInfo& chunk_info,
      uint64_t chunk_offset) const override {
    // Check the size of the metadata. If it does not match the expected
    // size, then end the test as a failure.
    REQUIRE(
        chunk_info.metadata_length() == checksum_.size() * sizeof(uint64_t));

    // Check the metadata values.
    auto metadata_offset = chunk_info.metadata_offset() + chunk_offset;
    for (uint64_t index = 0; index < checksum_.size(); ++index) {
      auto offset = metadata_offset + index * sizeof(uint64_t);
      auto actual_checksum = buffer.value_at_as<uint64_t>(offset);
      auto expected_checksum = checksum_[index];
      CHECK(actual_checksum == expected_checksum);
    }
  }

  const ChunkInfo& expected_chunk_info() const override {
    return expected_chunk_info_;
  }

 private:
  ChunkInfo expected_chunk_info_{};
  uint32_t num_filtered_elements_{};
  T starting_value_{};
  T spacing_{};
  std::vector<uint64_t> checksum_{};
};

class FilteredBufferChecker {
 public:
  template <typename T>
  static FilteredBufferChecker create_uncompressed_with_grid_chunks(
      const std::vector<uint64_t>& elements_per_chunk,
      T starting_value = 0,
      T spacing = 1) {
    FilteredBufferChecker checker{};
    T start{starting_value};
    for (auto nelements_chunk : elements_per_chunk) {
      uint32_t data_size{static_cast<uint32_t>(nelements_chunk * sizeof(T))};
      checker.add_grid_chunk_checker<T>(
          data_size, nelements_chunk, start, spacing);
      start += nelements_chunk * spacing;
    }
    return checker;
  }

  template <typename T>
  static FilteredBufferChecker create_uncompressed_with_grid_chunks(
      const std::vector<uint64_t>& elements_per_chunk,
      const std::vector<std::vector<uint64_t>>& checksum_per_chunk,
      T starting_value = 0,
      T spacing = 1) {
    FilteredBufferChecker checker{};
    T start{starting_value};
    if (elements_per_chunk.size() != checksum_per_chunk.size()) {
      throw std::runtime_error(
          "Mismatched test parameters for filtered buffer checker.");
    }
    for (uint64_t chunk_index = 0; chunk_index < elements_per_chunk.size();
         ++chunk_index) {
      auto nelements = elements_per_chunk[chunk_index];
      auto checksum = checksum_per_chunk[chunk_index];
      uint32_t data_size{static_cast<uint32_t>(nelements * sizeof(T))};
      checker.add_grid_chunk_checker<T>(
          data_size, nelements, start, spacing, checksum);
      start += nelements * spacing;
    }
    return checker;
  }

  FilteredBufferChecker() = default;

  template <typename T>
  void add_grid_chunk_checker(
      uint32_t original_chunk_length,
      uint32_t num_filtered_elements,
      T starting_value,
      T spacing) {
    chunk_checkers_.emplace_back(tdb_new(
        GridChunkChecker<T>,
        original_chunk_length,
        num_filtered_elements,
        starting_value,
        spacing));
  }

  template <typename T>
  void add_grid_chunk_checker(
      uint32_t original_chunk_length,
      uint32_t num_filtered_elements,
      T starting_value,
      T spacing,
      std::vector<uint64_t> checksum) {
    chunk_checkers_.emplace_back(tdb_new(
        GridChunkChecker<T>,
        original_chunk_length,
        num_filtered_elements,
        starting_value,
        spacing,
        checksum));
  }

  void check(const FilteredBuffer& buffer) const;

 private:
  std::vector<tdb_unique_ptr<ChunkChecker>> chunk_checkers_;
};

}  // namespace tiledb::sm

#endif
