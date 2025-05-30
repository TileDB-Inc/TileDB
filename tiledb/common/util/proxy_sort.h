/**
 * @file   tiledb/common/util/proxy_sort.h
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
 * This file implements a proxy sort algorithm that will be used as part of
 * TileDB external sort.
 *
 * Four variations of proxy_sort and stable_proxy_sort are provided
 * * proxy_sort_no_init: Sorts the permutation, given an initial permutation.
 * By default, < is used to compare, but an overload is provided that takes a
 * comparison function.
 *
 * * proxy_sort: Sorts the permutation, initializing it to the identity. By
 * default, < is used to compare, but an overload is provided that takes a
 * comparison function.
 *
 * * stable_proxy_sort_no_init: Performs stable sort of the permutation, given
 * an initial permutation. By default, < is used to compare, but an overload is
 * provided that takes a comparison function.
 *
 * * stable_proxy_sort: Performs stable sort of the permutation, initializing it
 * to the identity. By default, < is used to compare, but an overload is
 * provided that takes a comparison function.
 *
 * std::vector<int> x{3, 1, 4, 1, 5, 9, 2, 6, 5, 3};
 * std::vector<int> perm(x.size());
 * std::vector<int> expected{1, 1, 2, 3, 3, 4, 5, 5, 6, 9};
 * proxy_sort(x, perm);
 * for (size_t i = 0; i < x.size(); ++i) {
 *   CHECK(x[perm[i]] == expected[i]);
 * }
 *
 * @todo Parallelize
 */

#ifndef TILEDB_PROXY_SORT_H
#define TILEDB_PROXY_SORT_H

#include <algorithm>
#include <cassert>
#include <numeric>
#include <ranges>

#include "tiledb/common/assert.h"

/**
 * Sorts the elements of the input range `perm` such that the elements of the
 * input range x are sorted when indirected through `perm`.  Does not initialize
 * perm. It is assumed that perm is a valid permuation of [0, x.size()).
 * @tparam R Type of the random access range x
 * @tparam I Type of the random access range perm.
 * @param x Random access range to "sort"
 * @param perm Random access range of indices to be permuted such that
 * x[perm[i]] is sorted
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range I>
void proxy_sort_no_init(const R& x, I&& perm) {
  iassert(perm.size() == x.size());
  std::sort(
      std::forward<I>(perm).begin(),
      std::forward<I>(perm).end(),
      [&](auto a, auto b) { return x[a] < x[b]; });
}

/**
 * Sorts the elements of the input range `perm` such that the elements of the
 * input range x are sorted when indirected through `perm`.  Initializes perm to
 * the identity permutation.
 * @tparam R Type of the random access range x
 * @tparam I Type of the random access range perm.
 * @param x Random access range to "sort"
 * @param perm Random access range of indices to be permuted such that
 * x[perm[i]] is sorted
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range I>
void proxy_sort(R&& x, I&& perm) {
  iassert(perm.size() == x.size());
  std::iota(perm.begin(), perm.end(), 0);
  proxy_sort_no_init(std::forward<R>(x), std::forward<I>(perm));
}

/**
 * Sorts the elements of the input range `perm` such that the elements of the
 * input range x are sorted when indirected through `perm`.  Does not initialize
 * perm. It is assumed that perm is a valid permuation of [0, x.size()).
 * @tparam R Type of the random access range x
 * @tparam I Type of the random access range perm.
 * @tparam Compare Type of the comparison function
 * @param x Random access range to "sort"
 * @param perm Random access range of indices to be permuted such that
 * x[perm[i]] is sorted
 * @param comp Comparison function
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range I,
    class Compare>
void proxy_sort_no_init(const R& x, I&& perm, Compare comp) {
  iassert(perm.size() == x.size());
  std::sort(
      std::forward<I>(perm).begin(),
      std::forward<I>(perm).end(),
      [&](auto a, auto b) { return comp(x[a], x[b]); });
}

/**
 * Sorts the elements of the input range `perm` such that the elements of the
 * input range x are sorted when indirected through `perm`.  Initializes perm to
 * the identity permutation.
 * @tparam R Type of the random access range x
 * @tparam I Type of the random access range perm.
 * @tparam Compare Type of the comparison function
 * @param x Random access range to "sort"
 * @param perm Random access range of indices to be permuted such that
 * x[perm[i]] is sorted
 * @param comp Comparison function
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range I,
    class Compare>
void proxy_sort(R&& x, I&& perm, Compare comp) {
  iassert(perm.size() == x.size());
  std::iota(perm.begin(), perm.end(), 0);
  proxy_sort_no_init(std::forward<R>(x), std::forward<I>(perm), comp);
}

/**
 * Sorts the elements of the input range `perm` such that the elements of the
 * input range x are sorted when indirected through `perm`.  Does not initialize
 * perm. It is assumed that perm is a valid permuation of [0, x.size()).
 * @tparam R Type of the random access range x
 * @tparam I Type of the random access range perm.
 * @param x Random access range to "sort"
 * @param perm Random access range of indices to be permuted such that
 * x[perm[i]] is sorted
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range I>
void stable_proxy_sort_no_init(const R& x, I& perm) {
  iassert(perm.size() == x.size());
  std::stable_sort(
      perm.begin(), perm.end(), [&](auto a, auto b) { return x[a] < x[b]; });
}

/**
 * Sorts the elements of the input range `perm` such that the elements of the
 * input range x are sorted when indirected through `perm`.  Initializes perm to
 * the identity permutation.
 * @tparam R Type of the random access range x
 * @tparam I Type of the random access range perm.
 * @param x Random access range to "sort"
 * @param perm Random access range of indices to be permuted such that
 * x[perm[i]] is sorted
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range I>
void stable_proxy_sort(const R& x, I& perm) {
  iassert(perm.size() == x.size());
  std::iota(perm.begin(), perm.end(), 0);
  stable_proxy_sort_no_init(x, perm);
}

/**
 * Sorts the elements of the input range `perm` such that the elements of the
 * input range x are sorted when indirected through `perm`.  Does not initialize
 * perm. It is assumed that perm is a valid permuation of [0, x.size()).
 * @tparam R Type of the random access range x
 * @tparam I Type of the random access range perm.
 * @tparam Compare Type of the comparison function
 * @param x Random access range to "sort"
 * @param perm Random access range of indices to be permuted such that
 * x[perm[i]] is sorted
 * @param comp Comparison function
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range I,
    class Compare>
void stable_proxy_sort_no_init(const R& x, I& perm, Compare comp) {
  iassert(perm.size() == x.size());
  std::stable_sort(perm.begin(), perm.end(), [&](auto a, auto b) {
    return comp(x[a], x[b]);
  });
}

/**
 * Sorts the elements of the input range `perm` such that the elements of the
 * input range x are sorted when indirected through `perm`.  Initializes perm to
 * the identity permutation.
 * @tparam R Type of the random access range x
 * @tparam I Type of the random access range perm.
 * @tparam Compare Type of the comparison function
 * @param x Random access range to "sort"
 * @param perm Random access range of indices to be permuted such that
 * x[perm[i]] is sorted
 * @param comp Comparison function
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range I,
    class Compare>
void stable_proxy_sort(const R& x, I& perm, Compare comp) {
  passert(perm.size() == x.size());
  std::iota(perm.begin(), perm.end(), 0);
  stable_proxy_sort_no_init(x, perm, comp);
}

#endif  // TILEDB_PROXY_SORT_H
