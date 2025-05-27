/**
 * @file   tiledb/common/util/permutation_view.h
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
 * This file contains the definition of the permutation_view class, which
 * applies index indirection to a random access range, as specified by the
 * indices in another random access range (the permutation).  It is required
 * that the permutation range is the same size as the data range and that it
 * contain the values 0, 1, ..., N-1, in arbitrary order, where N is the size of
 * the data range.  For a data range `r` and a permutation `p`, and a
 * permutation_view `v`, based on `r` and `p`, the expression `v[i]` is
 * equivalent to `r[p[i]]`.
 *
 * To create an inverse permutation:
 *
 * std::vector<int> perm(N);
 * proxy_sort(shuffled, perm);
 *
 * std::vector<int> i_perm(N);
 * proxy_sort(perm, i_perm);
 */

#ifndef TILEDB_PERMUTATION_VIEW_H
#define TILEDB_PERMUTATION_VIEW_H

#include <cassert>
#include <ranges>
#include "proxy_sort.h"
#include "tiledb/common/assert.h"
#include "tiledb/common/util/detail/iterator_facade.h"

/**
 * A view that creates a permutation of the underlying view, as determined by
 * a permutation range.
 *
 * @tparam R Type of the data range, assumed to be a random access range.
 * @tparam P Type of the index range, assumed to be a random access range.
 *
 * @todo R could be a view rather than a range.
 * @todo Implement with view_interface.
 */
template <
    std::ranges::random_access_range ElementRange,
    std::ranges::random_access_range IndexRange>
class permutation_view : public std::ranges::view_interface<
                             permutation_view<ElementRange, IndexRange>> {
  /** Forward reference of the iterator over the range of permuted data. */
  template <class Reference>
  struct private_iterator;

  /** The data type of the permutation view. */
  using data_iterator_type = std::ranges::iterator_t<ElementRange>;

  /** The index type of the permutation view indexing into underlying range. */
  using data_index_type = std::iter_difference_t<data_iterator_type>;

  /** The type of the iterator over the index range -- It should derference to
   * something that can index into the data range (e.g., the data_index_type)*/
  using index_iterator_type = std::ranges::iterator_t<IndexRange>;

  /** The permuted view value_type is value_type of ElementRange */
  using permuted_value_type = std::ranges::range_value_t<ElementRange>;

  /** The permuted view reference is the reference type of ElementRange */
  using permuted_reference = std::ranges::range_reference_t<ElementRange>;

  /** The type of the iterator over the permuted data range */
  using permuted_iterator = private_iterator<permuted_reference>;

  /** The type of the const iterator over the permuted data range */
  using permuted_const_iterator = private_iterator<permuted_value_type const>;

 public:
  /**
   * Primary constructor
   * @note IndexRange is not const
   */
  permutation_view(ElementRange& data, IndexRange& permutation)
      : data_begin_(std::ranges::begin(data))
      , data_end_(std::ranges::end(data))
      , index_begin_(std::ranges::begin(permutation))
      , index_end_(std::ranges::end(permutation)) {
    // @todo Should this throw an exception instead?
    iassert(
        static_cast<unsigned long>(std::ranges::size(data)) ==
        std::ranges::size(permutation));
  }

  /** Return iterator to the beginning of the permutation view */
  auto begin() {
    return permuted_iterator{data_begin_, index_begin_, 0};
  }

  /** Return iterator to the end of the permutation view */
  auto end() {
    return permuted_iterator{
        data_begin_, index_begin_, data_end_ - data_begin_};
  }

  /** Return const iterator to the beginning of the permutation view */
  auto begin() const {
    return permuted_const_iterator{data_begin_, index_begin_, 0};
  }

  /** Return const iterator to the end of the permutation view */
  auto end() const {
    return permuted_const_iterator{
        data_begin_, index_begin_, data_end_ - data_begin_};
  }

  /** Return const iterator to the beginning of the permutation view */
  auto cbegin() const {
    return permuted_const_iterator{data_begin_, index_begin_, 0};
  }

  /** Return const iterator to the end of the permutation view */
  auto cend() const {
    return permuted_const_iterator{
        data_begin_, index_begin_, data_end_ - data_begin_};
  }

  /** Size of the permutation view. */
  size_t size() const {
    return data_end_ - data_begin_;
  }

  /** Accessor.  Note use of decltype(auto).  */
  decltype(auto) operator[](size_t i) const {
    // More general? return *(data_begin_ + *(index_begin_ + i));
    return data_begin_[index_begin_[i]];
  }

  /*****************************************************************************
   * Member functions for sorting the permutation view.  These will sort the
   * permutation view with the type given of the data range.  If this is a
   * tuple, it will sort based on the first element, then the second, etc.
   * @note These functions are not part of the view interface
   * @todo Add variants to sort tuple based on given indexes, rather than by
   * default order
   ****************************************************************************/

  /**
   * Apply `proxy_sort` to the permutation view.  This will permute the index
   * range so that the indexed data range is sorted. The index range will be
   * initialized with the identity permutation.
   */
  void proxy_sort() {
    ::proxy_sort(
        std::ranges::subrange(data_begin_, data_end_),
        std::ranges::subrange(index_begin_, index_end_));
  }

  /**
   * Apply `proxy_sort_no_init` to the permutation view.  This will permute the
   * index range so that the indexed data range is sorted.  The index range is
   * assumed to be a valid permutation of [0, data.size())
   */
  void proxy_sort_no_init() {
    ::proxy_sort_no_init(
        std::ranges::subrange(data_begin_, data_end_),
        std::ranges::subrange(index_begin_, index_end_));
  }

  /**
   * Apply `proxy_sort` to the permutation view.  This will permute the index
   * range so that the indexed data range is sorted based on the comparison
   * functor `cmp`. The index range will be initialized with the identity
   * permutation.
   */
  template <class Compare>
  void proxy_sort(Compare comp) {
    ::proxy_sort(
        std::ranges::subrange(data_begin_, data_end_),
        std::ranges::subrange(index_begin_, index_end_),
        comp);
  }

  /**
   * Apply `proxy_sort` to the permutation view.  This will permute the index
   * range so that the indexed data range is sorted based on the comparison
   * functor `cmp`. The index range is assumed to be a valid permutation of [0,
   * data.size())
   */
  template <class Compare>
  void proxy_sort_no_init(Compare comp) {
    ::proxy_sort_no_init(
        std::ranges::subrange(data_begin_, data_end_),
        std::ranges::subrange(index_begin_, index_end_),
        comp);
  }

  /**
   * Apply `stable_proxy_sort` to the permutation view.  This will permute the
   * index range so that the indexed data range is sorted. The index range will
   * be initialized with the identity permutation.
   */
  void stable_proxy_sort() {
    ::stable_proxy_sort(
        std::ranges::subrange(data_begin_, data_end_),
        std::ranges::subrange(index_begin_, index_end_));
  }

  /**
   * Apply `stable_proxy_sort_no_init` to the permutation view.  This will
   * permute the index range so that the indexed data range is sorted.  The
   * index range is assumed to be a valid permutation of [0, data.size())
   */
  void stable_proxy_sort_no_init() {
    ::stable_proxy_sort_no_init(
        std::ranges::subrange(data_begin_, data_end_),
        std::ranges::subrange(index_begin_, index_end_));
  }

  /**
   * Apply `stable_proxy_sort` to the permutation view.  This will permute the
   * index range so that the indexed data range is sorted based on the
   * comparison functor `cmp`. The index range will be initialized with the
   * identity permutation.
   */
  template <class Compare>
  void stable_proxy_sort(Compare comp) {
    ::stable_proxy_sort(
        std::ranges::subrange(data_begin_, data_end_),
        std::ranges::subrange(index_begin_, index_end_),
        comp);
  }

  /**
   * Apply `stable_proxy_sort` to the permutation view.  This will permute the
   * index range so that the indexed data range is sorted based on the
   * comparison functor `cmp`. The index range is assumed to be a valid
   * permutation of [0, data.size())
   */
  template <class Compare>
  void stable_proxy_sort_no_init(Compare comp) {
    ::stable_proxy_sort_no_init(
        std::ranges::subrange(data_begin_, data_end_),
        std::ranges::subrange(index_begin_, index_end_),
        comp);
  }

 private:
  /**
   * Private iterator class for permuted view
   * @tparam Value The type of the value that the iterator dereferences to
   */
  template <class Reference>
  struct private_iterator
      : public iterator_facade<private_iterator<Reference>> {
    // using value_type_ = Value;

   public:
    /** Default constructor */
    private_iterator() = default;

    /**
     * Primary constructor
     * @param data_begin The beginning of the data range
     * @param index_begin The beginning of the index range
     * @param index Offset into the data range
     */
    private_iterator(
        data_iterator_type data_begin,
        index_iterator_type index_begin,
        data_index_type index = 0)
        : index_(index)
        , data_begin_(data_begin)
        , index_begin_(index_begin) {
    }

    /** Dereference the iterator */
    // value_type_&
    // std::ranges::range_reference_t<R>
    Reference dereference() const {
      // More general?
      // return *(data_begin_ + *(index_begin_ + index_));
      return data_begin_[index_begin_[index_]];
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

    /** Equality comparison of two iterators */
    bool operator==(const private_iterator& other) const {
      return data_begin_ == other.data_begin_ &&
             index_begin_ == other.index_begin_ && index_ == other.index_;
    }

   private:
    /** Index to current location of iterator */
    data_index_type index_;

    /** Iterator to the beginning of the data range */
    data_iterator_type data_begin_;

    /** Iterator to the beginning of the index range */
    index_iterator_type index_begin_;

    /** Flag to indicate that the iterator is not a single pass iterator */
    static const bool single_pass_iterator = false;
  };

  /** Iterator to the beginning of the data range */
  data_iterator_type data_begin_;

  /** Iterator to the end of the data range */
  data_iterator_type data_end_;

  /** const_iterator is c++23.  For now we just use an iterator to const */
  /** Iterator to the beginning of the index range */
  index_iterator_type index_begin_;

  /** Iterator to the end of the index range */
  index_iterator_type index_end_;
};

/**
 * Overload of `proxy_sort` for permutation_view.
 * @tparam R Type of data range
 * @tparam P Type of permutation range
 * @param x permutation_view to be sorted
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range P>
void proxy_sort_no_init(permutation_view<R, P>& x) {
  x.proxy_sort_no_init();
}

/**
 * Overload of `proxy_sort_no_init` for permutation_view.
 * @tparam R Type of data range
 * @tparam P Type of permutation range
 * @param x permutation_view to be sorted
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range P>
void proxy_sort(permutation_view<R, P>& x) {
  x.proxy_sort();
}

/**
 * Overload of `proxy_sort` for permutation_view.
 * @tparam R Type of data range
 * @tparam P Type of permutation range
 * @tparam Compare Type of comparison function
 * @param x permutation_view to be sorted
 * @param comp Comparison function
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range P,
    class Compare>
void proxy_sort_no_init(permutation_view<R, P>& x, Compare comp) {
  x.proxy_sort_no_init(comp);
}

/**
 * Overload of `proxy_sort_no_init` for permutation_view.
 * @tparam R Type of data range
 * @tparam P Type of permutation range
 * @tparam Compare Type of comparison function
 * @param x permutation_view to be sorted
 * @param comp Comparison function
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range P,
    class Compare>
void proxy_sort(permutation_view<R, P>& x, Compare comp) {
  x.proxy_sort(comp);
}

/**
 * Overload of `stable_proxy_sort` for permutation_view.
 * @tparam R Type of data range
 * @tparam P Type of permutation range
 * @param x permutation_view to be sorted
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range P>
void stable_proxy_sort_no_init(permutation_view<R, P>& x) {
  x.stable_proxy_sort_no_init();
}

/**
 * Overload of `stable_proxy_sort_no_init` for permutation_view.
 * @tparam R Type of data range
 * @tparam P Type of permutation range
 * @param x permutation_view to be sorted
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range P>
void stable_proxy_sort(permutation_view<R, P>& x) {
  x.stable_proxy_sort();
}

/**
 * Overload of `stable_proxy_sort` for permutation_view.
 * @tparam R Type of data range
 * @tparam P Type of permutation range
 * @tparam Compare Type of comparison function
 * @param x permutation_view to be sorted
 * @param comp Comparison function
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range P,
    class Compare>
void stable_proxy_sort_no_init(permutation_view<R, P>& x, Compare comp) {
  x.stable_proxy_sort_no_init(comp);
}

/**
 * Overload of `stable_proxy_sort_no_init` for permutation_view.
 * @tparam R Type of data range
 * @tparam P Type of permutation range
 * @tparam Compare Type of comparison function
 * @param x permutation_view to be sorted
 * @param comp Comparison function
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range P,
    class Compare>
void stable_proxy_sort(permutation_view<R, P>& x, Compare comp) {
  x.stable_proxy_sort(comp);
}

/**
 * Overloads of `std::sort` for permutation_view. We take the view "as-is",
 * i.e., we use `proxy_sort_no_init` to do the sorting.
 */
namespace std {

/**
 * Overload of `std::sort` for permutation_view.  We take the view "as-is",
 * i.e., we use `proxy_sort_no_init` to do the sorting.
 * @tparam R Type of data range
 * @tparam P Type of permutation range
 * @param x permutation_view to be sorted
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range P>
void sort(permutation_view<R, P>& x) {
  x.proxy_sort_no_init();
}

/**
 * Overload of `std::sort` for permutation_view.  We take the view "as-is",
 * i.e., we use `proxy_sort_no_init` to do the sorting.
 * @tparam R Type of data range
 * @tparam P Type of permutation range
 * @tparam Compare Type of comparison function
 * @param x permutation_view to be sorted
 * @param comp Comparison function
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range P,
    class Compare>
void sort(permutation_view<R, P>& x, Compare comp) {
  x.proxy_sort_no_init(comp);
}

/**
 * Overload of `std::stable_sort` for permutation_view.  We take the view
 * "as-is", i.e., we use `proxy_sort_no_init` to do the sorting.
 * @tparam R Type of data range
 * @tparam P Type of permutation range
 * @param x permutation_view to be sorted
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range P>
void stable_sort(permutation_view<R, P>& x) {
  x.stable_proxy_sort_no_init();
}

/**
 * Overload of `std::stable_sort` for permutation_view.  We take the view
 * "as-is", i.e., we use `proxy_sort_no_init` to do the sorting.
 * @tparam R Type of data range
 * @tparam P Type of permutation range
 * @tparam Compare Type of comparison function
 * @param x permutation_view to be sorted
 * @param comp Comparison function
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range P,
    class Compare>
void stable_sort(permutation_view<R, P>& x, Compare comp) {
  x.stable_proxy_sort_no_init(comp);
}

}  // namespace std
#endif  // TILEDB_PERMUTATION_VIEW_H
