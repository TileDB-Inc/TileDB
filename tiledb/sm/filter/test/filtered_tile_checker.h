/**
 * @file filtered_tile_checker.h
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
 *
 * This file contains helper classes for checking the data in a filtered tile
 * is as expected after running a filter pipeline forward.
 *
 * The main class here is the FilteredTileChecker. It checks all chunks in the
 * filtered tile with the use of additional helper classes.
 *
 * - Summary of lengths and data offsets.
 *
 *   - ChunkInfo: Info for chunk component lengths and offsets.
 *
 *   - FilteredTileChunkInfo: Summary of info for all chunks in a tile.
 *
 * - Checking individual chunks:
 *
 *   - ChunkChecker: Abstract base class for testing the data in a chunk is as
 *     expected.
 *
 *   - GridChunkChecker: ChunkChecker for fixed grid data with checksum
 *     metadata.
 *
 */

#ifndef TILEDB_FILTERED_BUFFER_CHECKER_H
#define TILEDB_FILTERED_BUFFER_CHECKER_H

#include <catch2/catch_test_macros.hpp>
#include <vector>
#include "tiledb/sm/tile/filtered_buffer.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::sm;

namespace tiledb::sm {

/**
 * Summary of chunk information for all chunks in a filtered tile.
 */
class FilteredTileChunkInfo;

/**
 * Stores the lengths and offsets for data in a chunk.
 */
class ChunkInfo {
 public:
  /** Constructor for an empty chunk. */
  ChunkInfo() = default;

  /**
   * Constructor from chunk component sizes.
   *
   * Uses prescriptive data lengths. Useful for defining expected chunk info
   * in a tests.
   *
   * @param original_chunk_length Length of unfiltered data.
   * @param filtered_chunk_length Length of filtered data stored in the chunk.
   * @param metadata_length Length of the metadata stored in the chunk.
   */
  ChunkInfo(
      uint32_t original_chunk_length,
      uint32_t filtered_chunk_length,
      uint32_t metadata_length)
      : original_chunk_length_{original_chunk_length}
      , filtered_chunk_length_{filtered_chunk_length}
      , metadata_length_{metadata_length} {
  }

  /** Constructor from filtered buffer and chunk offset. */
  ChunkInfo(const FilteredBuffer& buffer, uint32_t chunk_offset)
      : original_chunk_length_{buffer.value_at_as<uint32_t>(chunk_offset)}
      , filtered_chunk_length_{buffer.value_at_as<uint32_t>(
            chunk_offset + sizeof(uint32_t))}
      , metadata_length_{
            buffer.value_at_as<uint32_t>(chunk_offset + 2 * sizeof(uint32_t))} {
  }

  /** Returns the length of the original, unfiltered data. */
  inline uint32_t original_chunk_length() const {
    return original_chunk_length_;
  }

  /** Returns the length of the filtered data stored in the chunk. */
  inline uint32_t filtered_chunk_length() const {
    return filtered_chunk_length_;
  }

  /** Returns the offset from the chunk start to the filtered data. */
  inline uint64_t filtered_chunk_offset() const {
    return 3 * sizeof(uint32_t) + static_cast<uint64_t>(metadata_length_);
  }

  /** Returns the length of the metadata stored in the chunk. */
  inline uint32_t metadata_length() const {
    return metadata_length_;
  }

  /** Returns the offset from the chunk start to the metadata. */
  inline uint64_t metadata_offset() const {
    return 3 * sizeof(uint32_t);
  }

  /**
   * Returns the totol size of the data in the chunk including size
   * information.
   */
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

/**
 * Class for checking chunk data and metadata values using Catch2 testing
 * macros.
 */
class ChunkChecker {
 public:
  virtual ~ChunkChecker() = default;

  /**
   * Uses Catch2 macros to check the data in a chunk is as expected.
   *
   * @param tile The tile to check chunk data on.
   * @param tile_info The summary chunk info for all chunks in the filtered
   *     buffer.
   * @param chunk_index The index of which chunk to check.
   */
  void check(
      const FilteredBuffer& tile,
      const FilteredTileChunkInfo& tile_info,
      uint64_t chunk_index) const;

  /**
   * Uses Catch2 macros to check the data in a chunk is as expected.
   *
   * @param tile The tile to check chunk data on.
   * @param chunk_info Summary of the chunk parameters for the chunk being
   *     checked.
   * @param chunk_offset The offset from the start of the tile to the chunk
   * being checked.
   */
  virtual void check_filtered_data(
      const FilteredBuffer& tile,
      const ChunkInfo& chunk_info,
      uint64_t chunk_offset) const = 0;

  /**
   * Uses Catch2 macros to check the metadata in a chunk is as expected.
   *
   * @param tile The tile to check chunk data on.
   * @param chunk_info Summary of the chunk parameters for the chunk being
   *     checked.
   * @param chunk_offset The offset from the start of the tile to the chunk
   * being checked.
   */
  virtual void check_metadata(
      const FilteredBuffer& buffer,
      const ChunkInfo& chunk_info,
      uint64_t chunk_offset) const = 0;

 protected:
  /** Returns the chunk info that is expected for the chunk being tested. */
  virtual const ChunkInfo& expected_chunk_info() const = 0;
};

/**
 * A chunk checker for data that increases by a fixed amount at each
 * subsequent element (a grid with fixed-spacing). It supports checksum
 * metadata.
 *
 * This is for testing data on file using the test-specific filters.
 *
 * @tparam Type of the filtered data.
 */
template <typename T>
class GridChunkChecker : public ChunkChecker {
 public:
  /**
   * Constructor for checker with no metadata.
   *
   * @param original_chunk_length The length of the original, unfiltered data.
   * @param num_filtered_elements The number of filtered elements stored in the
   *        chunk.
   * @param starting_value The value of the first element in the chunk.
   * @param spacing The difference between subsequent elements.
   */
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

  /**
   * Constructor for checker with metadata.
   *
   * @param original_chunk_length The length of the original, unfiltered data.
   * @param num_filtered_elements The number of filtered elements stored in the
   *        chunk.
   * @param starting_value The value of the first element in the chunk.
   * @param spacing The difference between subsequent elements.
   * @param checksum A vector of expected checksum values.
   */
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

  /**
   * Uses Catch2 macros to check the data in a chunk stored is as expected.
   *
   * @param tile The tile to check chunk data on.
   * @param chunk_info The summary chunk info for all chunks in the filtered
   *     buffer.
   * @param chunk_offset The index of which chunk to check.
   */
  void check_filtered_data(
      const FilteredBuffer& tile,
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
      T actual_value = tile.value_at_as<T>(offset);
      CHECK(actual_value == expected_value);
    }
  }

  /**
   * Uses Catch2 macros to check the metadata in a chunk is as expected.
   *
   * @param tile The tile to check chunk data on.
   * @param chunk_info Summary of the chunk parameters for the chunk being
   *     checked.
   * @param chunk_offset The offset from the start of the tile to the chunk
   * being checked.
   */
  void check_metadata(
      const FilteredBuffer& tile,
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
      auto actual_checksum = tile.value_at_as<uint64_t>(offset);
      auto expected_checksum = checksum_[index];
      CHECK(actual_checksum == expected_checksum);
    }
  }

 protected:
  /** Returns the chunk info that is expected for the chunk being tested. */
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

/**
 * Summary of chunk information for all chunks in a filtered tile.
 */
class FilteredTileChunkInfo {
 public:
  /**
   * Constructor.
   *
   * @param buffer The FilteredBuffer to create the summary for.
   */
  FilteredTileChunkInfo(const FilteredBuffer& buffer)
      : nchunks_{buffer.value_at_as<uint64_t>(0)}
      , chunk_info_{}
      , offsets_{} {
    chunk_info_.reserve(nchunks_);
    offsets_.reserve(nchunks_);
    uint64_t current_offset = sizeof(uint64_t);
    for (uint64_t index = 0; index < nchunks_; ++index) {
      chunk_info_.emplace_back(buffer, current_offset);
      offsets_.push_back(current_offset);
      current_offset += chunk_info_[index].size();
    }
    size_ = current_offset;
  }

  /** Returns the chunk info for the requested chunk. */
  inline const ChunkInfo& chunk_info(uint64_t index) const {
    return chunk_info_[index];
  }

  /** Returns the offset for accessing the requested chunk. */
  inline uint64_t chunk_offset(uint64_t index) const {
    return offsets_[index];
  }

  /** Returns the total number of chunks. */
  inline uint64_t nchunks() const {
    return nchunks_;
  }

  /** Returns the total size of the chunk. */
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
 * Class for checking all filtered data using Catch2 macros.
 */
class FilteredTileChecker {
 public:
  /**
   * Factory for creating a FilteredTileChecker where the data forms a grid with
   * fixed spacing.
   *
   * @tparam T Datatype for the filtered data.
   * @param elements_per_chunk A vector storing the number of elements expected
   *        on each chunk.
   * @param starting_value The value of the first element.
   * @param spacing The spacing between subsequent values.
   */
  template <typename T>
  static FilteredTileChecker create_uncompressed_with_grid_chunks(
      const std::vector<uint64_t>& elements_per_chunk,
      T starting_value = 0,
      T spacing = 1) {
    FilteredTileChecker checker{};
    T start{starting_value};
    for (auto nelements_chunk : elements_per_chunk) {
      uint32_t data_size{static_cast<uint32_t>(nelements_chunk * sizeof(T))};
      checker.add_grid_chunk_checker<T>(
          data_size, static_cast<uint32_t>(nelements_chunk), start, spacing);
      start += static_cast<T>(nelements_chunk) * spacing;
    }
    return checker;
  }

  /**
   * Factory for creating a FilteredTileChecker where the data forms a grid with
   * fixed spacing.
   *
   * @tparam T Datatype for the filtered data.
   * @param elements_per_chunk A vector storing the number of elements expected
   *        on each chunk. Must be the same length as `checksum_per_chunk`.
   * @param checksum_per_chunk A vector of vectors storing checksum metadata
   * expected on each chunk. Must be the same length as `elements_per_chunk`.
   * @param starting_value The value of the first element.
   * @param spacing The spacing between subsequent values.
   */
  template <typename T>
  static FilteredTileChecker create_uncompressed_with_grid_chunks(
      const std::vector<uint64_t>& elements_per_chunk,
      const std::vector<std::vector<uint64_t>>& checksum_per_chunk,
      T starting_value = 0,
      T spacing = 1) {
    FilteredTileChecker checker{};
    T start{starting_value};
    if (elements_per_chunk.size() != checksum_per_chunk.size()) {
      throw std::runtime_error(
          "Mismatched test parameters for filtered tile checker.");
    }
    for (uint64_t chunk_index = 0; chunk_index < elements_per_chunk.size();
         ++chunk_index) {
      auto nelements = elements_per_chunk[chunk_index];
      auto checksum = checksum_per_chunk[chunk_index];
      uint32_t data_size{static_cast<uint32_t>(nelements * sizeof(T))};
      checker.add_grid_chunk_checker<T>(
          data_size,
          static_cast<uint32_t>(nelements),
          start,
          spacing,
          checksum);
      start += static_cast<T>(nelements) * spacing;
    }
    return checker;
  }

  /** Constructor for an empty tile checker. */
  FilteredTileChecker() = default;

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

  /**
   * Add an additional grid chunk checker.
   *
   * @tparam T Datatype of the filtered data.
   * @param original_chunk_length The length of the original, unfiltered data.
   * @param num_filtered_elements Number of elements in the chunk.
   * @param starting_value The value of the first element.
   * @param spacing Spacing between subsequent elements.
   * @param checksum Vector of checksums stored as metadata.
   */
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

  /**
   * Use Catch2 macros to check all chunks have the expected data stored in
   * them.
   *
   * @param tile The tile to check.
   */
  void check(const FilteredBuffer& tile) const;

 private:
  std::vector<tdb_unique_ptr<ChunkChecker>> chunk_checkers_;
};

}  // namespace tiledb::sm

#endif
