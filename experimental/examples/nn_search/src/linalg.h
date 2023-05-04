/**
 * @file   linalg.h
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
 * Header-only library of some basic linear algebra data structures and
 * operations. Uses C++23 reference implementation of mdspan from Sandia
 * National Laboratories.
 *
 */

#ifndef TDB_LINALG_H
#define TDB_LINALG_H

#include <cstddef>
#include <memory>
#include <span>
#include <vector>

#include <mdspan/mdspan.hpp>

namespace tiledb::common {

template <class T, class LayoutPolicy = Kokkos::layout_right, class I = size_t>
class Matrix
    : public Kokkos::mdspan<
          T,
          Kokkos::extents<I, std::dynamic_extent, std::dynamic_extent>, LayoutPolicy> {
  using Base = Kokkos::
      mdspan<T, Kokkos::extents<I, std::dynamic_extent, std::dynamic_extent>, LayoutPolicy>;
  using Base::Base;

  using index_type = I;

  index_type nrows_{0};
  index_type ncols_{0};

  std::unique_ptr<T[]> storage_;

 public:
  Matrix(
      index_type nrows,
      index_type ncols,
      LayoutPolicy policy = LayoutPolicy()) noexcept
      : nrows_(nrows)
      , ncols_(ncols)
      , storage_{new T[nrows_ * ncols_]} {
    Base::operator=(Base{storage_.get(), nrows_, ncols_});
  }

  Matrix(
      index_type nrows,
      index_type ncols,
      std::unique_ptr<T[]> storage,
      LayoutPolicy policy = LayoutPolicy()) noexcept
      : nrows_(nrows)
      , ncols_(ncols)
      , storage_{std::move(storage)} {
    Base::operator=(Base{storage_.get(), nrows_, ncols_});
  }

  auto data() {
    return storage_.get();
  }

  index_type num_rows() const noexcept {
    return nrows_;
  }
  index_type num_cols() const noexcept {
    return ncols_;
  }
};

template <class T, class I = size_t>
using RowMajorMatrix = Matrix<T, I, Kokkos::layout_right>;

template <class T, class I = size_t>
using ColMajorMatrix = Matrix<T, I, Kokkos::layout_left>;

}  // namespace tiledb::common

#endif  // TDB_LINALG_H
