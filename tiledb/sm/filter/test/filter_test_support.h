#ifndef TILEDB_FILTER_TEST_SUPPORT_H
#define TILEDB_FILTER_TEST_SUPPORT_H

#include <test/support/tdb_catch.h>

#include "tiledb/sm/tile/filtered_buffer.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::sm;

/**
 * Helper for checking chunk data.
 *
 * @tparam Type of the filtered data.
 */
class ChunkCheckerBase {
 public:
  virtual ~ChunkCheckerBase() = default;

  void check(const FilteredBuffer& buffer, uint64_t chunk_offset) const {
    // Check the value of the original chunk length.
    CHECK(
        actual_original_chunk_length(buffer, chunk_offset) ==
        expected_original_chunk_length());

    // Check the metadata.
    check_metadata(buffer, chunk_offset);

    // Check the filtered chunk data.
    check_filtered_data(buffer, chunk_offset);
  }

  virtual void check_filtered_data(
      const FilteredBuffer& buffer, uint64_t chunk_offset) const = 0;

  virtual void check_metadata(
      const FilteredBuffer& buffer, uint64_t chunk_offset) const = 0;

  uint64_t expected_total_chunk_size() const {
    return 3 * sizeof(uint32_t) + expected_original_chunk_length() +
           expected_filtered_chunk_length() + expected_metadata_length();
  }

  uint64_t actual_total_chunk_size(
      const FilteredBuffer& buffer, uint64_t chunk_offset) const {
    return 3 * sizeof(uint32_t) +
           actual_original_chunk_length(buffer, chunk_offset) +
           actual_filtered_chunk_length(buffer, chunk_offset) +
           actual_metadata_length(buffer, chunk_offset);
  }

 protected:
  inline uint32_t actual_original_chunk_length(
      const FilteredBuffer& buffer, uint64_t chunk_offset) const {
    return buffer.value_at_as<uint32_t>(chunk_offset);
  }

  inline uint32_t actual_filtered_chunk_length(
      const FilteredBuffer& buffer, uint64_t chunk_offset) const {
    return buffer.value_at_as<uint32_t>(chunk_offset + sizeof(uint32_t));
  }

  inline uint32_t actual_metadata_length(
      const FilteredBuffer& buffer, uint64_t chunk_offset) const {
    return buffer.value_at_as<uint32_t>(chunk_offset + 2 * sizeof(uint32_t));
  }

  inline uint64_t data_offset(
      const FilteredBuffer& buffer, uint64_t chunk_offset) const {
    return metadata_offset(chunk_offset) +
           static_cast<uint64_t>(actual_metadata_length(buffer, chunk_offset));
  }

  virtual uint32_t expected_original_chunk_length() const = 0;

  virtual uint32_t expected_filtered_chunk_length() const = 0;

  virtual uint32_t expected_metadata_length() const = 0;

  inline uint64_t metadata_offset(uint64_t chunk_offset) const {
    return chunk_offset + 3 * sizeof(uint32_t);
  }
};

template <typename T>
class GridChunkChecker : public ChunkCheckerBase {
 public:
  GridChunkChecker(
      uint32_t original_chunk_length,
      uint32_t num_elements,
      T starting_value,
      T spacing)
      : original_chunk_length_{original_chunk_length}
      , num_elements_{num_elements}
      , starting_value_{starting_value}
      , spacing_{spacing} {
  }

  void check_filtered_data(
      const FilteredBuffer& buffer, uint64_t chunk_offset) const override {
    // Check the size of the filtered data. If it does not match the expected
    // size, then end the test as a failure.
    auto data_size = actual_filtered_chunk_length(buffer, chunk_offset);
    REQUIRE(data_size == expected_filtered_chunk_length());

    // Check the data.
    auto data_offset_value = data_offset(buffer, chunk_offset);
    for (uint32_t index = 0; index < num_elements_; ++index) {
      auto offset = data_offset_value + index * sizeof(T);
      T expected_value = starting_value_ + index * spacing_;
      T actual_value = buffer.value_at_as<uint64_t>(offset);
      CHECK(actual_value == expected_value);
    }
  }

  void check_metadata(
      const FilteredBuffer& buffer, uint64_t chunk_offset) const override {
    CHECK(actual_metadata_length(buffer, chunk_offset) == 0);
  }

 protected:
  uint32_t expected_original_chunk_length() const override {
    return original_chunk_length_;
  }

  uint32_t expected_filtered_chunk_length() const override {
    return num_elements_ * sizeof(T);
  }

  uint32_t expected_metadata_length() const override {
    return 0;
  }

 private:
  uint32_t original_chunk_length_{};
  uint32_t num_elements_{};
  T starting_value_{};
  T spacing_{};
};

template <typename T>
class GridTileChecker {
 public:
  GridTileChecker(uint64_t num_elements, T starting_value, T spacing)
      : num_elements_{num_elements}
      , starting_value_{starting_value}
      , spacing_{spacing} {
  }

  void check(const Tile& tile) {
    for (uint64_t index = 0; index < num_elements_; ++index) {
      T expected_value = starting_value_ + index * spacing_;
      T actual_value{};
      CHECK_NOTHROW(tile.read(&actual_value, index * sizeof(T), sizeof(T)));
      CHECK(actual_value == expected_value);
    }
  }

 private:
  uint64_t num_elements_{};
  T starting_value_{};
  T spacing_{};
};

#endif
