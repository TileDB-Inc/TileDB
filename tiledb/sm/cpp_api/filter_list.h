/**
 * @file   filter_list.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file implements the C++ API for the TileDB FilterList object.
 */

#ifndef TILEDB_CPP_API_FILTER_LIST_H
#define TILEDB_CPP_API_FILTER_LIST_H

#include "context.h"
#include "filter.h"
#include "tiledb.h"

#include <iostream>
#include <string>

namespace tiledb {

/**
 * Represents an ordered list of Filters used to transform attribute data.
 *
 * **Example:**
 *
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::FilterList filter_list(ctx);
 * filter_list.add_filter({ctx, TILEDB_FILTER_BYTESHUFFLE})
 *     .add_filter({ctx, TILEDB_FILTER_BZIP2});
 * @endcode
 */
class FilterList {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Construct a FilterList.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::FilterList filter_list(ctx);
   * @endcode
   *
   * @param ctx TileDB context
   */
  FilterList(const Context& ctx)
      : ctx_(ctx) {
    tiledb_filter_list_t* filter_list;
    ctx.handle_error(tiledb_filter_list_alloc(ctx.ptr().get(), &filter_list));
    filter_list_ = std::shared_ptr<tiledb_filter_list_t>(filter_list, deleter_);
  }

  /**
   * Creates a FilterList with the input C object.
   *
   * @param ctx TileDB context
   * @param filter C API filter list object
   */
  FilterList(const Context& ctx, tiledb_filter_list_t* filter_list)
      : ctx_(ctx) {
    filter_list_ = std::shared_ptr<tiledb_filter_list_t>(filter_list, deleter_);
  }

  FilterList() = delete;
  FilterList(const FilterList&) = default;
  FilterList(FilterList&&) = default;
  FilterList& operator=(const FilterList&) = default;
  FilterList& operator=(FilterList&&) = default;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns a shared pointer to the C TileDB domain object. */
  std::shared_ptr<tiledb_filter_list_t> ptr() const {
    return filter_list_;
  }

  /**
   * Appends a filter to a filter list. Data is processed through each filter in
   * the order the filters were added.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::FilterList filter_list(ctx);
   * filter_list.add_filter({ctx, TILEDB_FILTER_BYTESHUFFLE})
   *     .add_filter({ctx, TILEDB_FILTER_BZIP2});
   * @endcode
   *
   * @param filter The filter to add
   * @return Reference to this FilterList
   */
  FilterList& add_filter(const Filter& filter) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_filter_list_add_filter(
        ctx.ptr().get(), filter_list_.get(), filter.ptr().get()));
    return *this;
  }

  /**
   * Returns a copy of the Filter in this list at the given index.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::FilterList filter_list(ctx);
   * filter_list.add_filter({ctx, TILEDB_FILTER_BYTESHUFFLE})
   *     .add_filter({ctx, TILEDB_FILTER_BZIP2});
   * auto f = filter_list.filter(1);
   * // f.filter_type() == TILEDB_FILTER_BZIP2
   * @endcode
   *
   * @param filter_index Index of filter to get
   * @return Filter
   *
   * @throws TileDBError if the index is out of range
   */
  Filter filter(uint32_t filter_index) const {
    auto& ctx = ctx_.get();
    tiledb_filter_t* filter;
    ctx.handle_error(tiledb_filter_list_get_filter_from_index(
        ctx.ptr().get(), filter_list_.get(), filter_index, &filter));
    return Filter(ctx, filter);
  }

  /**
   * Gets the maximum tile chunk size for the filter list.
   *
   * @return Maximum tile chunk size
   */
  uint32_t max_chunk_size() const {
    auto& ctx = ctx_.get();
    uint32_t max_chunk_size;
    ctx.handle_error(tiledb_filter_list_get_max_chunk_size(
        ctx.ptr().get(), filter_list_.get(), &max_chunk_size));
    return max_chunk_size;
  }

  /**
   * Returns the number of filters in this filter list.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::FilterList filter_list(ctx);
   * filter_list.add_filter({ctx, TILEDB_FILTER_BYTESHUFFLE})
   *     .add_filter({ctx, TILEDB_FILTER_BZIP2});
   * uint32_t n = filter_list.nfilters();  // n == 2
   * @endcode
   *
   * @return
   */
  uint32_t nfilters() const {
    auto& ctx = ctx_.get();
    uint32_t nfilters;
    ctx.handle_error(tiledb_filter_list_get_nfilters(
        ctx.ptr().get(), filter_list_.get(), &nfilters));
    return nfilters;
  }

  /**
   * Sets the maximum tile chunk size for the filter list.
   *
   * @param max_chunk_size Maximum tile chunk size to set
   * @return Reference to this FilterList
   */
  FilterList& set_max_chunk_size(uint32_t max_chunk_size) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_filter_list_set_max_chunk_size(
        ctx.ptr().get(), filter_list_.get(), max_chunk_size));
    return *this;
  }

 private:
  /* ********************************* */
  /*          PRIVATE ATTRIBUTES       */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** An auxiliary deleter. */
  impl::Deleter deleter_;

  /** The pointer to the C TileDB filter list object. */
  std::shared_ptr<tiledb_filter_list_t> filter_list_;
};

/** Gets a string representation of a filter for an output stream. */
inline std::ostream& operator<<(std::ostream& os, const FilterList& f) {
  os << "FilterList<" << f.max_chunk_size() << ',';
  for (uint32_t i = 0; i < f.nfilters(); i++)
    os << ' ' << f.filter(i);
  os << '>';
  return os;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_FILTER_LIST_H
