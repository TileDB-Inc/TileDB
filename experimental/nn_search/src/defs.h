/**
 * @file   defs.h
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
 */

#ifndef TDB_DEFS_H
#define TDB_DEFS_H

#include <algorithm>
#include <cmath>
#include <future>
#include <iostream>
#include <span>
// #include <execution>
#include <memory>
#include <queue>
#include <set>

#include "timer.h"

template <class T>
using Vector = std::span<T>;

template <class V>
auto L2(V const& a, V const& b) {
  typename V::value_type sum{0};

  auto size_a = size(a);
  for (decltype(a.size()) i = 0; i < size_a; ++i) {
    auto diff = a[i] - b[i];
    sum += diff * diff;
  }
  return std::sqrt(sum);
}

template <class V>
auto cosine(V const& a, V const& b) {
  typename V::value_type sum{0};
  auto a2 = 0.0;
  auto b2 = 0.0;

  auto size_a = size(a);
  for (auto i = 0; i < size_a; ++i) {
    sum += a[i] * b[i];
    a2 += a[i] * a[i];
    b2 += b[i] * b[i];
  }
  return sum / std::sqrt(a2 * b2);
}

template <class M, class V, class Function>
auto col_sum(
    const M& m, V& v, Function f = [](auto& x) -> const auto& { return x; }) {
  int size_m = size(m);
  int size_m0 = size(m[0]);

  for (int j = 0; j < size_m; ++j) {
    decltype(v[0]) vj = v[j];
    for (int i = 0; i < size_m0; ++i) {
      vj += f(m[j][i]);
    }
    v[j] = vj;
  }
}

template <class V, class L, class I>
auto verify_top_k(V const& scores, L const& top_k, I const& g, int k, int qno) {
  if (!std::equal(
          begin(top_k), begin(top_k) + k, g.begin(), [&](auto& a, auto& b) {
            return scores[a] == scores[b];
          })) {
    std::cout << "Query " << qno << " is incorrect" << std::endl;
    for (int i = 0; i < std::min<int>(10, k); ++i) {
      std::cout << "  (" << top_k[i] << " " << scores[top_k[i]] << ") ";
    }
    std::cout << std::endl;
    for (int i = 0; i < std::min(10, k); ++i) {
      std::cout << "  (" << g[i] << " " << scores[g[i]] << ") ";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  }
}

template <class L, class I>
auto verify_top_k(L const& top_k, I const& g, int k, int qno) {
  if (!std::equal(begin(top_k), begin(top_k) + k, g.begin())) {
    std::cout << "Query " << qno << " is incorrect" << std::endl;
    for (int i = 0; i < std::min(k, 10); ++i) {
      std::cout << "  (" << top_k[i] << " " << g[i] << ")";
    }
    std::cout << std::endl;
  }
}

template <
    class T,
    class Compare = std::less<T>,
    class Allocator = std::allocator<T>>
struct fixed_min_set : public std::set<T, Compare, Allocator> {
  using base = std::set<T, Compare, Allocator>;
  using base::base;

  unsigned max_size{0};

  explicit fixed_min_set(unsigned k)
      : max_size{k} {
  }
  fixed_min_set(unsigned k, const Compare& comp)
      : base(comp)
      , max_size{k} {
  }

  bool maxed_{false};

  void insert(T const& x) {
    base::insert(x);
    if (maxed_) {
      base::erase(std::prev(base::end()));
    } else {
      if (base::size() == max_size) {
        maxed_ = true;
      }
    }
  }
};

template <class V, class L, class I>
auto get_top_k(V const& scores, L& top_k, I& index, int k) {
  std::nth_element(
      begin(index), begin(index) + k, end(index), [&](auto&& a, auto&& b) {
        return scores[a] < scores[b];
      });
  std::copy(begin(index), begin(index) + k, begin(top_k));

  std::sort(begin(top_k), end(top_k), [&](auto& a, auto& b) {
    return scores[a] < scores[b];
  });
}

template <class S, class T>
void get_top_k(
    const S& scores, T& top_k, int k, int size_q, int size_db, int nthreads) {
  life_timer _{"Get top k"};

  std::vector<int> i_index(size_db);
  std::iota(begin(i_index), end(i_index), 0);

  int q_block_size = (size_q + nthreads - 1) / nthreads;
  std::vector<std::future<void>> futs;
  futs.reserve(nthreads);

  for (int n = 0; n < nthreads; ++n) {
    int q_start = n * q_block_size;
    int q_stop = std::min<int>((n + 1) * q_block_size, size_q);

    futs.emplace_back(std::async(
        std::launch::async, [q_start, q_stop, &scores, &top_k, k, size_db]() {
          std::vector<int> index(size_db);

          for (int j = q_start; j < q_stop; ++j) {
            // std::copy(begin(i_index), end(i_index), begin(index));
            std::iota(begin(index), end(index), 0);
            get_top_k(scores[j], top_k[j], index, k);
          }
        }));
  }
  for (int n = 0; n < nthreads; ++n) {
    futs[n].get();
  }
}

#endif  // TDB_DEFS_H
