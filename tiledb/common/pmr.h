/**
 * @file   tiledb/common/pmr.h
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
 * A centralized definition of the polymorphic resource types used by
 * TileDB.
 */

#ifndef TILEDB_COMMON_PMR_H
#define TILEDB_COMMON_PMR_H

// Currently, this is fairly broken when building with XCode 15 on macOS 13.x.
// For now I'm just going to default to using Pablo Halpern's implementation
// rather than spend time fighting compiler detection and build chains.
//
// #if __has_include(<memory_resource>)
// #include <memory_resource>
// #else
// #error Missing memory_resources header.
// #endif

#include <vector>

#include <boost/container/pmr/memory_resource.hpp>
#include <boost/container/pmr/polymorphic_allocator.hpp>
#include <boost/container/pmr/vector.hpp>

#include "common.h"

namespace tiledb::common::pmr {

using memory_resource = boost::container::pmr::memory_resource;

template <class Tp>
using pmr_vector =
    std::vector<Tp, boost::container::pmr::polymorphic_allocator<Tp>>;

memory_resource* get_default_resource();

template <class Tp>
class vector : public pmr_vector<Tp> {
 public:
  // This class exists to ensure that all uses of it are provided with a
  // valid std::pmr based allocator. This is so that as we switch from
  // std::vector to using this class we don't forget to provide the allocator
  // which is quite easy to do.
  //
  // If these constructors look confusing, just know that all we're doing is
  // copying the current definitions from cppreference and then adjusting types
  // to require the PMR based allocator.

  // I have absolutely no idea if all of these aliases are required. The
  // allocator_type is the important one. I've copied the others just in
  // case since I do know that PMR aware containers at least require
  // allocator_type.
  using value_type = typename pmr_vector<Tp>::value_type;
  using allocator_type = typename pmr_vector<Tp>::allocator_type;
  using size_type = typename pmr_vector<Tp>::size_type;
  using difference_type = typename pmr_vector<Tp>::difference_type;
  using reference = typename pmr_vector<Tp>::reference;
  using const_reference = typename pmr_vector<Tp>::const_reference;
  using pointer = typename pmr_vector<Tp>::pointer;
  using const_pointer = typename pmr_vector<Tp>::const_pointer;
  using iterator = typename pmr_vector<Tp>::iterator;
  using const_iterator = typename pmr_vector<Tp>::const_iterator;
  using reverse_iterator = typename pmr_vector<Tp>::reverse_iterator;
  using const_reverse_iterator =
      typename pmr_vector<Tp>::const_reverse_iterator;

  // Delete all default constructors because they don't require an allocator
  constexpr vector() noexcept(noexcept(allocator_type())) = delete;
  constexpr vector(const vector& other) = delete;
  constexpr vector(vector&& other) noexcept = delete;

  // Delete non-allocator aware copy and move assign.
  constexpr vector& operator=(const vector& other) = delete;
  constexpr vector& operator=(vector&& other) noexcept = delete;

  constexpr explicit vector(const allocator_type& alloc) noexcept
      : pmr_vector<Tp>(alloc) {
  }

  constexpr vector(
      size_type count, const Tp& value, const allocator_type& alloc)
      : pmr_vector<Tp>(count, value, alloc) {
  }

  constexpr explicit vector(size_type count, const allocator_type& alloc)
      : pmr_vector<Tp>(count, alloc) {
  }

  template <class InputIt>
  constexpr vector(InputIt first, InputIt last, const allocator_type& alloc)
      : pmr_vector<Tp>(first, last, alloc) {
  }

  constexpr vector(const vector& other, const allocator_type& alloc)
      : pmr_vector<Tp>(other, alloc) {
  }

  constexpr vector(vector&& other, const allocator_type& alloc)
      : pmr_vector<Tp>(other, alloc) {
  }

  constexpr vector(std::initializer_list<Tp> init, const allocator_type& alloc)
      : pmr_vector<Tp>(init, alloc) {
  }
};

}  // namespace tiledb::common::pmr

#endif  // TILEDB_COMMON_PMR_H
