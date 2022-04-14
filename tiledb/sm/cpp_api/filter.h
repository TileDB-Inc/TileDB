/**
 * @file   filter.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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

#include "tiledb.h"

#include <iostream>
#include <string>

namespace tiledb {

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

  // @cond
  // doxygen ignore pending https://github.com/sphinx-doc/sphinx/issues/7944
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
      typename std::enable_if<std::is_arithmetic<T>::value>::type* = nullptr>
  Filter& set_option(tiledb_filter_option_t option, T value) {
    auto& ctx = ctx_.get();
    option_value_typecheck<T>(option);
    ctx.handle_error(tiledb_filter_set_option(
        ctx.ptr().get(), filter_.get(), option, &value));
    return *this;
  }
  // @endcond

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

  // @cond
  // doxygen ignore pending https://github.com/sphinx-doc/sphinx/issues/7944
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
   * @note The buffer pointed to by `value` must be large enough to hold the
   *    option value.
   *
   * @throws TileDBError if the option cannot be retrieved from the filter.
   * @throws std::invalid_argument if the option value is the wrong type.
   */
  template <
      typename T,
      typename std::enable_if<std::is_arithmetic<T>::value>::type* = nullptr>
  void get_option(tiledb_filter_option_t option, T* value) {
    auto& ctx = ctx_.get();
    option_value_typecheck<T>(option);
    ctx.handle_error(tiledb_filter_get_option(
        ctx.ptr().get(), filter_.get(), option, value));
  }
  // @endcond

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
   * @note get_option<T>(option, T* value) is preferred as it is safer.
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
    switch (option) {
      case TILEDB_COMPRESSION_LEVEL:
        if (!std::is_same<int32_t, T>::value)
          throw std::invalid_argument("Option value must be int32_t.");
        break;
      case TILEDB_BIT_WIDTH_MAX_WINDOW:
      case TILEDB_POSITIVE_DELTA_MAX_WINDOW:
        if (!std::is_same<uint32_t, T>::value)
          throw std::invalid_argument("Option value must be uint32_t.");
        break;
      default:
        throw std::invalid_argument("Invalid option type");
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
