/**
 * @file versioning.h
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
 * This file contains the format_version_t class definition.
 */

#ifndef TILEDB_FORMAT_VERSION_H
#define TILEDB_FORMAT_VERSION_H

#include <cstdint>
#include <string>
#include <type_traits>

#include "tiledb/common/common.h"

#define TILEDB_BASE_FORMAT_VERSION 20
#define TILEDB_EXPERIMENTAL_FLAG 0b10000000000000000000000000000000

namespace tiledb::storage_format {

/**
 * The Feature enum class is used as an alias system for specific integral
 * values. These aliases help with readability in the code.
 *
 * PJD TODO: Just a heads up for PR reviewers, I expect some or all of these
 * to change. I've collected the full history of `constants::format_version`
 * in `format_spec/format_version_history.md` for easier browsing, but some
 * of these aliases are deduced from context based on the code as it previously
 * existed.
 */
enum class Feature : uint32_t {
  INITIAL_FORMAT_VERSION = 1,

  ALWAYS_SPLIT_COORDINATE_TILES = 2,
  FRAGMENT_NAME_VERSION = 2,

  PARALELLIZE_FRAGMENT_METADATA_LOADING = 3,
  NEW_SPARSE_AND_DENSE_READERS = 3,
  FRAGMENT_FOOTERS = 3,
  THREE = 3,                 // PJD: Leaving this for PR review
  VAR_SIZED_DIMENSIONS = 3,  // PJD: From load_non_empty_domain_v3_v4

  REMOVE_OLD_KV_STORAGE = 4,
  FOUR = 4,  // PJD: Leaving this for PR review

  SPLIT_COORDINATE_FILES = 5,
  ALLOW_DUPS = 5,
  EXPLICIT_FRAGMENT_URIS = 5,
  EXTENDED_DIMENSION_SERIALIZATION = 5,
  HETEROGENOUS_DIMENSION_TYPES = 5,
  SIZE_CALCULATED_SERIALIZATION = 5,
  NO_MORE_ZIPPED_COORDS = 5,  // PJD: From sparse_index_reader_base.cc:192

  ATTRIBUTE_FILL_VALUES = 6,

  NULLABLE_ATTRIBUTES = 7,
  BACKWARDS_COMPATIBLE_WRITES = 7,
  CELL_VALIDITY_FILTERS = 7,

  PERCENT_ENCODED_FILE_NAMES = 8,

  INDEXED_FILE_NAMES = 9,
  FRAGMENT_METADATA_CONSOLIDATE_RELATIVE_URIS = 9,

  ARRAY_SCHEMA_EVOLUTION = 10,

  DO_NOT_SPLIT_CELLS_ACROSS_CHUNKS = 11,
  FRAGMENT_LEVEL_STATS_METADATA = 11,
  ELEVEN = 11,  // PJD: Leaving this for PR review

  NEW_ARRAY_DIRECTORY_STRUCTURE = 12,
  COMMITS_CONSOLIDATION = 12,
  STRING_COMPRESSORS = 12,
  RLE_FILTER = 12,

  DICTIONARY_FILTER = 13,
  FRAGMENT_METADATA_HAS_TIMESTAMPS = 14,

  CONSOLIDATION_WITH_TIMESTAMPS = 15,
  FRAGMENT_METADATA_HAS_DELETES = 15,

  ADD_DELETE_STRATEGY = 16,
  DELETES = 16,
  FRAGMENT_METADATA_HAS_PROCESSED_CONDITIONS = 16,
  UPDATES = 16,

  DIMENSION_LABELS_AND_DATA_ORDER = 17,
  DATA_ORDER = 17,
  UTF8_STRING_COMPRESSORS = 17,

  NON_EXPERIMENTAL_DIMENSION_LABELS = 18,
  DIMENSION_LABELS = 18,

  VAC_FILES_USE_RELATIVE_URIS = 19,

  ENUMERATIONS = 20,
};

/**
 * The GroupVersion class exists to support for checking group features that
 * have been added over time.
 */
enum class GroupVersion : uint32_t {
  VERSIONED_NAMES = 1,
  CURRENT = 2,
};

/**
 * The current version of Enumerations.
 */
enum class EnumerationVersion : uint32_t {
  CURRENT = 0,
};

/**
 * This is a helper class for the fragment_name_version_t class.
 *
 * PJD: I'm pretty much expecting this and the corresponding
 * fragment_name_version_t to be removed during PR review. But it was the
 * best I came up with for now.
 */
enum class FragmentNameVersion : uint32_t {
  ONE = 1,
  TWO = 2,
  THREE = 3,
  FOUR = 4,
  FIVE = 5,
};

class format_version_t {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor
   *
   * @param version The version number to represent.
   */
  constexpr format_version_t(uint32_t version)
      : version_(version & (~TILEDB_EXPERIMENTAL_FLAG))
      , is_experimental_((version & TILEDB_EXPERIMENTAL_FLAG) != 0)
      , is_valid_(true) {
  }

  /**
   * Copy Constructor
   *
   * @param rhs The format_version_t to copy.
   */
  constexpr format_version_t(const format_version_t& rhs)
      : version_(rhs.version_)
      , is_experimental_(rhs.is_experimental_)
      , is_valid_(rhs.is_valid_) {
  }

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  /**
   * Copy assignment
   *
   * @param rhs The format_version_t to copy.
   */
  format_version_t& operator=(const format_version_t& rhs) {
    version_ = rhs.version_;
    is_experimental_ = rhs.is_experimental_;
    is_valid_ = rhs.is_valid_;
    return *this;
  }

  /* ********************************* */
  /*         FACTORY FUNCTIONS         */
  /* ********************************* */

  /**
   * Factory method that generates a format_version_t of the librarie's current
   * format version.
   *
   * @return A format_version_t for the current library version.
   */
  static constexpr format_version_t current_version() {
    if constexpr (is_experimental_build) {
      return format_version_t{TILEDB_BASE_FORMAT_VERSION, true, true};
    } else {
      return format_version_t{TILEDB_BASE_FORMAT_VERSION, false, true};
    }
  }

  /**
   * Factory method that generates a format_version_t for the given feature
   * or version alias.
   *
   * @param vsn Create a format_version_t equivalent to a specific version or
   * feature alias.
   */
  template <typename T>
  static constexpr format_version_t from_alias(T vsn) {
    return format_version_t{static_cast<std::underlying_type_t<T>>(vsn)};
  }

  /**
   * Factory method for an invalid version. Invalid versions can only be checked
   * for validity. All other methods on an invalid version will throw an
   * exception.
   *
   * Internally, an invalid version is represented as UINT32_MAX.
   *
   * @returns An invalid format_version_t
   */
  static constexpr format_version_t invalid_version() {
    return format_version_t{std::numeric_limits<uint32_t>::max(), false, false};
  }

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Convert this format_version_t into a uint32_t for serialization.
   *
   * @return A uint32_t equivalent to this version. Notably, if this version
   * is experimental, the high bit will be set.
   */
  constexpr uint32_t to_disk() const {
    // Don't accidentally write invalid feature numbers to disk.
    check_validity();
    if (is_experimental_) {
      return version_ | TILEDB_EXPERIMENTAL_FLAG;
    } else {
      return version_;
    }
  }

  /**
   * Convert a format_version_t into a string.
   *
   * @return A string representation of this format version.
   */
  std::string to_string() const {
    check_validity();
    if (is_experimental_) {
      return std::to_string(version_ | TILEDB_EXPERIMENTAL_FLAG);
    } else {
      return std::to_string(version_);
    }
  }

  /**
   * Convert a format_version_t into an error string. Notably, this will show
   * both the "normal" string version, and if its experimental, will also
   * show which version this is an experimental build of.
   *
   * @return A human readable string for use in error messages.
   */
  std::string to_error_string() const {
    check_validity();
    if (is_experimental_) {
      return "(" + to_string() + ", experimental build of " +
             std::to_string(version_) + ")";
    } else {
      return "(" + to_string() + ")";
    }
  }

  /**
   * Returns whether this is an experimental version.
   *
   * @return A bool indicating if the version is experimental or not.
   */
  constexpr bool is_experimental() const {
    check_validity();
    return is_experimental_;
  }

  /**
   * Returns whether this version is valid.
   *
   * @return A bool indicating if this version is valid or not.
   */
  constexpr bool is_valid() const {
    return is_valid_;
  }

  /**
   * Check if this version is before a given feature was added to the
   * code base.
   *
   * @tparam T The feature type to check against.
   * @param feature The feature to check against.
   * @return A bool indicating if this version is before the feature.
   */
  template <typename T>
  constexpr bool before_feature(const T& feature) const {
    check_validity();
    return version_ < static_cast<std::underlying_type_t<T>>(feature);
  }

  /**
   * Check if this version is the same as when a version was introduced.
   *
   * @tparam T The feature type to check against.
   * @param feature The feature to check against.
   * @return A bool indicating if this version is the same as the feature.
   */
  template <typename T>
  constexpr bool is(const T& feature) const {
    check_validity();
    return version_ == static_cast<std::underlying_type_t<T>>(feature);
  }

  /**
   * Check if the current version is contains the given feature.
   *
   * @tparam T The feature type to check against.
   * @param feature The feature to check against.
   * @return A bool indicating if this version contains the feature.
   */
  template <typename T>
  constexpr bool has_feature(const T& feature) const {
    check_validity();
    return version_ >= static_cast<std::underlying_type_t<T>>(feature);
  }

  /**
   * Check if the current version is before the given version.
   *
   * @param vsn The version to check against.
   * @return A bool indicating if this version is before the version.
   */
  constexpr bool is_older_than(const format_version_t& vsn) {
    check_validity();
    return version_ < vsn.version_;
  }

  /**
   * Check if the current version is after the given version.
   *
   * @param vsn The version to check against.
   * @return A bool indicating if this version is after the version.
   */
  constexpr bool is_newer_than(const format_version_t& vsn) {
    check_validity();
    return version_ > vsn.version_;
  }

  /**
   * Check if the current library version is capable of reading this version.
   * If the versions are not compatible, an exception is raised.
   */
  void check_read_compatibility() {
    check_validity();

    auto lib_vsn = current_version();

    // If the library version is newer or equal to the array's base version,
    // we allow reading from the array.
    if (lib_vsn.version_ >= version_) {
      return;
    }

    std::string msg = "Library version " + lib_vsn.to_error_string() +
                      " is unable to read from an array with version " +
                      to_error_string();
    throw std::invalid_argument(msg);
  }

  /**
   * Check if the current library version is capable of writing to this
   * version. If the versions are not compatible, an exception is raised.
   */
  void check_write_compatibility() {
    check_validity();

    auto lib_vsn = current_version();

    // We allow these cases:
    // 1. Both experimental, and exactly the same version
    // 2. Neither experimental, and the library version is equal to or greater
    //    than this version.
    if (lib_vsn.is_experimental_ && is_experimental_ &&
        lib_vsn.version_ == version_) {
      return;
    } else if (
        !lib_vsn.is_experimental_ && !is_experimental_ &&
        lib_vsn.version_ >= version_) {
      return;
    }

    // This library version is unable to write to this disk version.
    std::string msg = "Library version " + lib_vsn.to_error_string() +
                      " is unable to write to an array with version " +
                      to_error_string();
    throw std::invalid_argument(msg);
  }

 private:
  /* ********************************* */
  /*         PRIVATE CONSTRUCTORS      */
  /* ********************************* */

  /**
   * Constructor
   *
   * @param version The version value to use
   * @param is_experimental Whether this is an experimental version or not.
   */
  constexpr format_version_t(
      uint32_t version, bool is_experimental, bool is_valid)
      : version_(version)
      , is_experimental_(is_experimental)
      , is_valid_(is_valid) {
  }

  void check_validity() const {
    if (!is_valid_) {
      throw std::logic_error(
          "This is an invalid version and cannot be compared.");
    }
  }

  /** The version number. */
  uint32_t version_;

  /** Whether this version is experimental. */
  bool is_experimental_;

  /** Whether this version is valid. */
  bool is_valid_;
};

/**
 * The fragment_name_version_t class exists for the single function
 * tiledb::sm::utils::parse::get_fragment_name_version which guesses the
 * version of a given fragment URI.
 *
 * BIG NOTE: This should not be confused with the version that may or may not
 * exist in a fragment URI. This is a version of the URI itself as our URI
 * formats have changed over time. There's at least one major issue in
 * FragmentMetadata::get_footer_offset_and_size due to this mistake.
 */
class fragment_name_version_t {
 public:
  /**
   * Constructor
   *
   * @param version The fragment name version to represent.
   */
  constexpr fragment_name_version_t(uint32_t version)
      : version_(version) {
  }

  /**
   * less than operator
   *
   * @param vsn The FragmentNameVersion to compare against.
   * @return A bool if this fragment_name_version_t is less than the given
   * FragmentNameVersion.
   */
  constexpr bool operator<(const FragmentNameVersion& vsn) {
    return version_ <
           static_cast<std::underlying_type_t<FragmentNameVersion>>(vsn);
  }

  /**
   * Equality operator.
   *
   * @param vsn The FragmentNameVersion to compare against.
   * @return A bool if this is the same version as vsn.
   */
  constexpr bool operator==(const FragmentNameVersion& vsn) {
    return version_ ==
           static_cast<std::underlying_type_t<FragmentNameVersion>>(vsn);
  }

 private:
  /** The version represented by this fragment_name_version_t. */
  uint32_t version_;
};

}  // namespace tiledb::storage_format

namespace tiledb {
using Feature = storage_format::Feature;
using GroupVersion = storage_format::GroupVersion;
using EnumerationVersion = storage_format::EnumerationVersion;
using FragmentNameVersion = storage_format::FragmentNameVersion;

using format_version_t = storage_format::format_version_t;
using fragment_name_version_t = storage_format::fragment_name_version_t;
}  // namespace tiledb

#endif  // TILEDB_FORMAT_VERSION_H
