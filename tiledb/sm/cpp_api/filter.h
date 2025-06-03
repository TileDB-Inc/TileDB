/**
 * @file   filter.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB, Inc.
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
 * This file implements the C++ API for the TileDB Filter object.
 */

#ifndef TILEDB_CPP_API_FILTER_H
#define TILEDB_CPP_API_FILTER_H

#include "exception.h"
#include "tiledb.h"
#include "type.h"

#include <iostream>
#include <string>

namespace tiledb {

template <typename Expected, typename Actual>
class FilterOptionTypeError : public TypeError {
 public:
  FilterOptionTypeError(tiledb_filter_option_t option)
      : TypeError(
            "Cannot set filter option '" + option_name(option) +
            "' with type '" + tiledb::impl::type_to_tiledb<Actual>().name +
            "'; Option value must be '" +
            tiledb::impl::type_to_tiledb<Expected>().name + "'.") {
  }

  FilterOptionTypeError(
      tiledb_filter_option_t option, const std::string& alternate_type)
      : TypeError(
            "Cannot set filter option '" + option_name(option) +
            "' with type '" + tiledb::impl::type_to_tiledb<Actual>().name +
            "'; Option value must be '" + alternate_type + "' or '" +
            tiledb::impl::type_to_tiledb<Expected>().name + "'.") {
  }

 private:
  static std::string option_name(tiledb_filter_option_t option) {
    const char* option_name;
    tiledb_filter_option_to_str(option, &option_name);
    return {option_name};
  }
};

/**
 * Represents a filter. A filter is used to transform attribute data e.g.
 * with compression, delta encoding, etc.
 *
 * **Example:**
 *
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::Filter f(ctx, TILEDB_FILTER_ZSTD);
 * int level = 5;
 * f.set_option(TILEDB_COMPRESSION_LEVEL, &level);
 * @endcode
 */
class Filter {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Creates a Filter of the given type.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Filter f(ctx, TILEDB_FILTER_ZSTD);
   * @endcode
   *
   * @param ctx TileDB context
   * @param filter_type Enumerated type of filter
   */
  Filter(const Context& ctx, tiledb_filter_type_t filter_type)
      : ctx_(ctx) {
    tiledb_filter_t* filter;
    ctx.handle_error(
        tiledb_filter_alloc(ctx.ptr().get(), filter_type, &filter));
    filter_ = std::shared_ptr<tiledb_filter_t>(filter, deleter_);
  }

  /**
   * Creates a Filter with the input C object.
   *
   * @param ctx TileDB context
   * @param filter C API filter object
   */
  Filter(const Context& ctx, tiledb_filter_t* filter)
      : ctx_(ctx) {
    filter_ = std::shared_ptr<tiledb_filter_t>(filter, deleter_);
  }

  Filter() = delete;
  Filter(const Filter&) = default;
  Filter(Filter&&) = default;
  Filter& operator=(const Filter&) = default;
  Filter& operator=(Filter&&) = default;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns a shared pointer to the C TileDB domain object. */
  std::shared_ptr<tiledb_filter_t> ptr() const {
    return filter_;
  }

  /**
   * Sets an option on the filter. Options are filter dependent; this function
   * throws an error if the given option is not valid for the given filter.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::Filter f(ctx, TILEDB_FILTER_ZSTD);
   * f.set_option(TILEDB_COMPRESSION_LEVEL, 5);
   * @endcode
   *
   * @tparam T Type of value of option to set.
   * @param option Enumerated option to set.
   * @param value Value of option to set.
   * @return Reference to this Filter
   *
   * @throws TileDBError if the option cannot be set on the filter.
   * @throws std::invalid_argument if the option value is the wrong type.
   */
  template <
      typename T,
      typename std::enable_if_t<!std::is_pointer_v<T>, int> = 0>
  Filter& set_option(tiledb_filter_option_t option, T value) {
    auto& ctx = ctx_.get();
    option_value_typecheck<T>(option);
    ctx.handle_error(tiledb_filter_set_option(
        ctx.ptr().get(), filter_.get(), option, &value));
    return *this;
  }

  /**
   * Sets an option on the filter. Options are filter dependent; this function
   * throws an error if the given option is not valid for the given filter.
   *
   * This version of set_option performs no type checks.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::Filter f(ctx, TILEDB_FILTER_ZSTD);
   * int level = 5;
   * f.set_option(TILEDB_COMPRESSION_LEVEL, &level);
   * @endcode
   *
   * @param option Enumerated option to set.
   * @param value Value of option to set.
   * @return Reference to this Filter
   *
   * @throws TileDBError if the option cannot be set on the filter.
   *
   * @note set_option<T>(option, T value) is preferred as it is safer.
   */
  Filter& set_option(tiledb_filter_option_t option, const void* value) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_filter_set_option(
        ctx.ptr().get(), filter_.get(), option, value));
    return *this;
  }

  /**
   * Gets an option value from the filter.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::Filter f(ctx, TILEDB_FILTER_ZSTD);
   * int32_t level = f.get_option(TILEDB_COMPRESSION_LEVEL);
   * // level == -1 (the default compression level)
   * @endcode
   *
   * @tparam T Type of option value to get.
   * @param option Enumerated option to get.
   * @returns value Buffer that option value will be written to.
   *
   * @throws TileDBError if the option cannot be retrieved from the filter.
   * @throws std::invalid_argument if the option value is the wrong type.
   */
  template <typename T>
  T get_option(tiledb_filter_option_t option) {
    auto& ctx = ctx_.get();
    option_value_typecheck<T>(option);
    T value{};
    ctx.handle_error(tiledb_filter_get_option(
        ctx.ptr().get(), filter_.get(), option, &value));
    return value;
  }

  /**
   * Gets an option value from the filter.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::Filter f(ctx, TILEDB_FILTER_ZSTD);
   * int32_t level;
   * f.get_option(TILEDB_COMPRESSION_LEVEL, &level);
   * // level == -1 (the default compression level)
   * @endcode
   *
   * @tparam T Type of option value to get.
   * @param option Enumerated option to get.
   * @param value Buffer that option value will be written to.
   *
   * @throws TileDBError if the option cannot be retrieved from the filter.
   * @throws std::invalid_argument if the option value is the wrong type.
   */
  template <
      typename T,
      typename std::enable_if<std::is_arithmetic_v<T>>::type* = nullptr>
  void get_option(tiledb_filter_option_t option, T* value) {
    auto& ctx = ctx_.get();
    option_value_typecheck<T>(option);
    ctx.handle_error(tiledb_filter_get_option(
        ctx.ptr().get(), filter_.get(), option, value));
  }

  /**
   * Gets an option value from the filter.
   *
   * This version of get_option performs no type checks.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::Filter f(ctx, TILEDB_FILTER_ZSTD);
   * int32_t level;
   * f.get_option(TILEDB_COMPRESSION_LEVEL, &level);
   * // level == -1 (the default compression level)
   * @endcode
   *
   * @param option Enumerated option to get.
   * @param value Buffer that option value will be written to.
   *
   * @note The buffer pointed to by `value` must be large enough to hold the
   *    option value.
   *
   * @throws TileDBError if the option cannot be retrieved from the filter.
   *
   * @note T value = get_option<T>(option) is preferred as it is safer.
   */
  void get_option(tiledb_filter_option_t option, void* value) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_filter_get_option(
        ctx.ptr().get(), filter_.get(), option, value));
  }

  /** Gets the filter type of this filter. */
  tiledb_filter_type_t filter_type() const {
    auto& ctx = ctx_.get();
    tiledb_filter_type_t type;
    ctx.handle_error(
        tiledb_filter_get_type(ctx.ptr().get(), filter_.get(), &type));
    return type;
  }

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /** Returns the input type in string format. */
  static std::string to_str(tiledb_filter_type_t type) {
    switch (type) {
      case TILEDB_FILTER_NONE:
        return "NOOP";
      case TILEDB_FILTER_GZIP:
        return "GZIP";
      case TILEDB_FILTER_ZSTD:
        return "ZSTD";
      case TILEDB_FILTER_LZ4:
        return "LZ4";
      case TILEDB_FILTER_RLE:
        return "RLE";
      case TILEDB_FILTER_BZIP2:
        return "BZIP2";
      case TILEDB_FILTER_DOUBLE_DELTA:
        return "DOUBLE_DELTA";
      case TILEDB_FILTER_BIT_WIDTH_REDUCTION:
        return "BIT_WIDTH_REDUCTION";
      case TILEDB_FILTER_BITSHUFFLE:
        return "BITSHUFFLE";
      case TILEDB_FILTER_BYTESHUFFLE:
        return "BYTESHUFFLE";
      case TILEDB_FILTER_POSITIVE_DELTA:
        return "POSITIVE_DELTA";
      case TILEDB_FILTER_CHECKSUM_MD5:
        return "CHECKSUM_MD5";
      case TILEDB_FILTER_CHECKSUM_SHA256:
        return "CHECKSUM_SHA256";
      case TILEDB_FILTER_DICTIONARY:
        return "DICTIONARY_ENCODING";
      case TILEDB_FILTER_SCALE_FLOAT:
        return "SCALE_FLOAT";
      case TILEDB_FILTER_XOR:
        return "XOR";
      case TILEDB_FILTER_DEPRECATED:
        return "DEPRECATED";
      case TILEDB_FILTER_WEBP:
        return "WEBP";
      case TILEDB_FILTER_DELTA:
        return "DELTA";
      case TILEDB_INTERNAL_FILTER_COUNT:
        break;
    }
    return "";
  }

 private:
  /* ********************************* */
  /*          PRIVATE ATTRIBUTES       */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** An auxiliary deleter. */
  impl::Deleter deleter_;

  /** The pointer to the C TileDB filter object. */
  std::shared_ptr<tiledb_filter_t> filter_;

  /**
   * Throws an exception if the value type is not suitable for the given filter
   * option.
   *
   * @tparam T Value type of filter option
   * @param option Option to check
   * @throws invalid_argument If the value type is invalid
   */
  template <typename T>
  void option_value_typecheck(tiledb_filter_option_t option) {
    std::string type_name = tiledb::impl::type_to_tiledb<T>().name;
    const char* option_name;
    tiledb_filter_option_to_str(option, &option_name);
    switch (option) {
      case TILEDB_COMPRESSION_LEVEL:
        if constexpr (!std::is_same_v<int32_t, T>) {
          throw FilterOptionTypeError<int32_t, T>(option);
        }
        break;
      case TILEDB_BIT_WIDTH_MAX_WINDOW:
      case TILEDB_POSITIVE_DELTA_MAX_WINDOW:
        if constexpr (!std::is_same_v<uint32_t, T>) {
          throw FilterOptionTypeError<uint32_t, T>(option);
        }
        break;
      case TILEDB_SCALE_FLOAT_BYTEWIDTH:
        if constexpr (!std::is_same_v<uint64_t, T>) {
          throw FilterOptionTypeError<uint64_t, T>(option);
        }
        break;
      case TILEDB_SCALE_FLOAT_FACTOR:
      case TILEDB_SCALE_FLOAT_OFFSET:
        if constexpr (!std::is_same_v<double, T>) {
          throw FilterOptionTypeError<double, T>(option);
        }
        break;
      case TILEDB_WEBP_QUALITY:
        if constexpr (!std::is_same_v<float, T>) {
          throw FilterOptionTypeError<float, T>(option);
        }
        break;
      case TILEDB_WEBP_INPUT_FORMAT:
        if constexpr (
            !std::is_same_v<uint8_t, T> &&
            !std::is_same_v<tiledb_filter_webp_format_t, T>) {
          throw FilterOptionTypeError<uint8_t, T>(
              option, "tiledb_filter_webp_format_t");
        }
        break;
      case TILEDB_WEBP_LOSSLESS:
        if constexpr (!std::is_same_v<uint8_t, T>) {
          throw FilterOptionTypeError<uint8_t, T>(option);
        }
        break;
      case TILEDB_COMPRESSION_REINTERPRET_DATATYPE:
        if constexpr (
            !std::is_same_v<uint8_t, T> &&
            !std::is_same_v<tiledb_datatype_t, T>) {
          throw FilterOptionTypeError<uint8_t, T>(option, "tiledb_datatype_t");
        }
        break;
      default: {
        throw std::invalid_argument(
            "Invalid filter option '" + std::string(option_name) + "'");
      }
    }
  }
};

/** Gets a string representation of a filter for an output stream. */
inline std::ostream& operator<<(std::ostream& os, const Filter& f) {
  os << "Filter<" << Filter::to_str(f.filter_type()) << '>';
  return os;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_FILTER_H
