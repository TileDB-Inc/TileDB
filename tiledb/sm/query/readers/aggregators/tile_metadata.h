/**
 * @file   tile_metadata.h
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
 * This file defines class TileMetadata.
 */

#ifndef TILEDB_TILE_METADATA_H
#define TILEDB_TILE_METADATA_H

#include "tiledb/common/common.h"

namespace tiledb::sm {

class TileMetadataStatusException : public StatusException {
 public:
  explicit TileMetadataStatusException(const std::string& message)
      : StatusException("TileMetadata", message) {
  }
};

class TileMetadata {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param count Number of cells for this tile.
   * @param null_count Number of null values for this tile.
   * @param min Pointer to the min data.
   * @param min_size Size of the min data.
   * @param max Pointer to the max data.
   * @param max_size Size of the max data.
   * @param sum Pointer to the sum data.
   */
  TileMetadata(
      const uint64_t count,
      const uint64_t null_count,
      const void* min,
      const storage_size_t min_size,
      const void* max,
      const storage_size_t max_size,
      const void* sum)
      : count_(count)
      , null_count_(null_count)
      , min_(min)
      , min_size_(min_size)
      , max_(max)
      , max_size_(max_size)
      , sum_(sum) {
  }

  DISABLE_COPY_AND_COPY_ASSIGN(TileMetadata);
  DISABLE_MOVE_AND_MOVE_ASSIGN(TileMetadata);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the count. */
  inline uint64_t count() const {
    return count_;
  }

  /** Returns the null count. */
  inline uint64_t null_count() const {
    return null_count_;
  }

  /** Returns the min as a specific type. */
  template <typename T>
  T min_as() const {
    if constexpr (std::is_same_v<T, std::string_view>) {
      return {static_cast<const char*>(min_), min_size_};
    } else {
      if (min_size_ != sizeof(T)) {
        throw TileMetadataStatusException("Unexpected min size.");
      }

      return *static_cast<const T*>(min_);
    }
  }

  /** Returns the max as a specific type. */
  template <class T>
  T max_as() const {
    if constexpr (std::is_same_v<T, std::string_view>) {
      return {static_cast<const char*>(max_), max_size_};
    } else {
      if (max_size_ != sizeof(T)) {
        throw TileMetadataStatusException("Unexpected max size.");
      }

      return *static_cast<const T*>(max_);
    }
  }

  /** Returns the sum as a specific type. */
  template <class T>
  T sum_as() const {
    return *static_cast<const T*>(sum_);
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Count of cells. */
  const uint64_t count_;

  /** Null count. */
  const uint64_t null_count_;

  /** Min value. */
  const void* min_;

  /** Min size. */
  const storage_size_t min_size_;

  /** Max value. */
  const void* max_;

  /** Max size. */
  const storage_size_t max_size_;

  /** Sum value. */
  const void* sum_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_TILE_METADATA_H
