/**
 * @file   chunk_view.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * Quick and dirty implementation of a chunk view for C++23.  This is a view
 * that splits a view into subranges of uniform length, as given by the
 * constructor argument num_chunks.
 */

#ifndef TILEDB_CHUNK_VIEW_H
#define TILEDB_CHUNK_VIEW_H

#include <concepts>
#include <ranges>
#include "iterator_facade.h"

/**
 * A view that splits a view into subranges of uniform length, as delimited by
 * a chunk size value. The resulting view is a range of subranges, each of which
 * is a view into the original data range. The iterator over the chunk_view
 * is a random_access_iterator that dereferences to a subrange of the data
 * range.
 *
 * @tparam R Type of the data range, assumed to be a random access range.
 *
 * @todo R could be a view rather than a range.
 */
template <std::ranges::random_access_range R>
class chunk_view : public std::ranges::view_interface<chunk_view<R>> {
  // Forward reference of the iterator over the data range data
  template <class V>
  struct private_iterator;

  /** The type of the iterator over the chunked data range */
  using data_iterator_type = std::ranges::iterator_t<R>;

  /** The type that can index into the chunked data range */
  using data_index_type = std::ranges::range_difference_t<R>;

  /** The type dereferenced by the iterator is a subrange */
  using chunk_type = std::ranges::subrange<data_iterator_type>;

  /** The type of the iterator over the chunk view */
  using chunk_iterator = private_iterator<chunk_type>;

  /** The type of the iterator over the chunk view */
  using chunk_const_iterator = private_iterator<chunk_type const>;

 public:
  /** Default constructor */
  chunk_view() = default;
  chunk_view(const chunk_view&) = default;
  chunk_view(chunk_view&&) = default;
  chunk_view& operator=(const chunk_view&) = default;
  chunk_view& operator=(chunk_view&&) = default;

  /**
   * Constructor taking ranges for the data and index ranges, along with sizes
   */
  chunk_view(R& data, std::ranges::range_difference_t<R> num_chunks)
      : data_begin_(std::ranges::begin(data))
      , num_chunks_(num_chunks) {
  }

  /*
   * Define the necessary members for view_interface.
   */

  /** Return iterator to the beginning of the chunk view */
  auto begin() {
    return chunk_iterator(data_begin_, chunk_size_, 0);
  }

  /** Return iterator to the end of the chunk view */
  auto end() {
    return chunk_iterator(data_begin_, num_chunks_, num_chunks_);
  }

  /** Return const iterator to the beginning of the chunk view */
  auto begin() const {
    return chunk_const_iterator(data_begin_, num_chunks_, 0);
  }

  /** Return const iterator to the end of the chunk view */
  auto end() const {
    return chunk_const_iterator(data_begin_, num_chunks_, num_chunks_);
  }

  /** Return const iterator to the beginning of the chunk view */
  auto cbegin() const {
    return chunk_const_iterator(data_begin_, 0);
  }

  /** Return const iterator to the end of the chunk view */
  auto cend() const {
    return chunk_const_iterator(data_begin_, num_chunks_);
  }

  /** Return the number of subranges in the chunk view */
  auto size() const {
    return num_chunks_;
  }

 private:
  template <class V>
  struct private_iterator : public iterator_facade<private_iterator<V>> {
    using value_type = V;
    using reference_type = V&;

    /** Default constructor */
    private_iterator() = default;

    /** Primary constructor */
    private_iterator(
        data_iterator_type data_begin,
        data_index_type chunk_size,
        data_index_type index = 0)
        : data_begin_(data_begin)
        , index_(index)
        , chunk_size_(chunk_size) {
    }

    /** Primary constructor */
    private_iterator(chunk_view&& parent, data_index_type index = 0)
        : index_(index)
        , data_begin_(std::ranges::begin(parent)) {
    }

    /*************************************************************************
     * Functions needed for iterator_facade
     * Here we just supply the minimum needed to make the iterator work
     *************************************************************************/

    /*
     * The critical function for defining the iterator as it determines the
     * value type, etc and also does the dereference.  In the case of the
     * chunk_view, the value type is a subrange of the data range, and
     * the subrage return value must be a proxy object, which we return by
     * value.  This is fine, since we can't assign a subrange to another.
     * We do, however, want to be able to modify the contents of the subrange.
     */
    value_type dereference() const {
      return {data_begin_, data_begin_ + chunk_size_};
    }

    /** Advance the iterator by n */
    auto advance(data_index_type n) {
      index_ += n;
      return *this;
    }

    /** Return the distance to another iterator */
    auto distance_to(const private_iterator& other) const {
      return other.index_ - index_;
    }

    /** Compare two iterators for equality */
    bool operator==(const private_iterator& other) const {
      return data_begin_ == other.data_begin_ && index_ == other.index_;
    }

    /** Flag to indicate that the iterator is not a single pass iterator */
    static const bool single_pass_iterator = false;

    /**
     * Iterator to the beginning of the data range.  This is a copy of the
     * parent data_begin_.  We maintain the copy to avoid an indirection through
     * the parent pointer.
     */
    data_iterator_type data_begin_;

    /** The index to the current location of the iterator */
    data_index_type index_;

    /** The chunk size.  Also cached from parent. */
    data_index_type chunk_size_;
  };

  /** The beginning of the data range */
  std::ranges::iterator_t<R> data_begin_;

  /** The number of chunks in the chunk_range */
  std::ranges::range_difference_t<R> chunk_size_;

  /** The number of chunks in the chunk_range */
  std::ranges::range_difference_t<R> num_chunks_;
};

/** Deduction guide for chunk_view */
template <class R, class I>
chunk_view(R, I) -> chunk_view<std::ranges::subrange<R>>;

/**
 * Define "chunk()" cpo for creating chunk views
 */
namespace _chunk {
struct _fn {
  // @todo Should this take a viewable range?
  // template <std::ranges::viewable_range T>
  template <std::ranges::random_access_range T>
  auto constexpr operator()(T&& t) const {
    return chunk_view<T>{std::forward<T>(t)};
  }
};
}  // namespace _chunk
inline namespace _cpo {
inline constexpr auto chunk = _chunk::_fn{};
}  // namespace _cpo

#endif  // TILEDB_CHUNK_VIEW_H
