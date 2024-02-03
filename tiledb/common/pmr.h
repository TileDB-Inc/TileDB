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

#include <unordered_map>
#include <vector>

#include <boost/container/pmr/memory_resource.hpp>
#include <boost/container/pmr/polymorphic_allocator.hpp>
#include <boost/container/pmr/vector.hpp>

#include "common.h"

namespace tiledb::common::pmr {

using memory_resource = boost::container::pmr::memory_resource;

/* ********************************* */
/*       PMR VECTOR DECLARATION      */
/* ********************************* */
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

/* ********************************* */
/*   PMR UNORDERED MAP DECLARATION   */
/* ********************************* */
template <
    class Key,
    class T,
    class Hash = std::hash<Key>,
    class KeyEqual = std::equal_to<Key>>
using pmr_unordered_map = std::unordered_map<
    Key,
    T,
    Hash,
    KeyEqual,
    boost::container::pmr::polymorphic_allocator<std::pair<const Key, T>>>;

template <
    class Key,
    class T,
    class Hash = std::hash<Key>,
    class KeyEqual = std::equal_to<Key>>
class unordered_map : public pmr_unordered_map<Key, T, Hash, KeyEqual> {
 public:
  // Type declarations.
  using key_type = typename pmr_unordered_map<Key, T, Hash, KeyEqual>::key_type;
  using mapped_type =
      typename pmr_unordered_map<Key, T, Hash, KeyEqual>::mapped_type;
  using value_type =
      typename pmr_unordered_map<Key, T, Hash, KeyEqual>::value_type;
  using size_type =
      typename pmr_unordered_map<Key, T, Hash, KeyEqual>::size_type;
  using difference_type =
      typename pmr_unordered_map<Key, T, Hash, KeyEqual>::difference_type;
  using hasher = typename pmr_unordered_map<Key, T, Hash, KeyEqual>::hasher;
  using key_equal =
      typename pmr_unordered_map<Key, T, Hash, KeyEqual>::key_equal;
  using allocator_type =
      typename pmr_unordered_map<Key, T, Hash, KeyEqual>::allocator_type;
  using reference =
      typename pmr_unordered_map<Key, T, Hash, KeyEqual>::reference;
  using const_reference =
      typename pmr_unordered_map<Key, T, Hash, KeyEqual>::const_reference;
  using pointer = typename pmr_unordered_map<Key, T, Hash, KeyEqual>::pointer;
  using const_pointer =
      typename pmr_unordered_map<Key, T, Hash, KeyEqual>::const_pointer;
  using iterator = typename pmr_unordered_map<Key, T, Hash, KeyEqual>::iterator;
  using const_iterator =
      typename pmr_unordered_map<Key, T, Hash, KeyEqual>::const_iterator;
  using local_iterator =
      typename pmr_unordered_map<Key, T, Hash, KeyEqual>::local_iterator;
  using node_type =
      typename pmr_unordered_map<Key, T, Hash, KeyEqual>::node_type;
  using insert_return_type =
      typename pmr_unordered_map<Key, T, Hash, KeyEqual>::insert_return_type;

  constexpr unordered_map() = delete;
  constexpr unordered_map(const unordered_map& other) = delete;
  constexpr unordered_map(unordered_map&& other) = delete;

  constexpr explicit unordered_map(
      size_type bucket_count,
      const Hash& hash,
      const key_equal& equal,
      const allocator_type& alloc)
      : pmr_unordered_map<Key, T, Hash, KeyEqual>(
            bucket_count, hash, equal, alloc) {
  }

  constexpr unordered_map(size_type bucket_count, const allocator_type& alloc)
      : pmr_unordered_map<Key, T, Hash, KeyEqual>(
            bucket_count, Hash(), KeyEqual(), alloc) {
  }

  constexpr unordered_map(
      size_type bucket_count, const Hash& hash, const allocator_type& alloc)
      : pmr_unordered_map<Key, T, Hash, KeyEqual>(
            bucket_count, hash, KeyEqual(), alloc) {
  }

  constexpr explicit unordered_map(const allocator_type& alloc)
      : pmr_unordered_map<Key, T, Hash, KeyEqual>(alloc) {
  }

  template <class InputIt>
  constexpr unordered_map(
      InputIt first,
      InputIt last,
      size_type bucket_count,
      const Hash& hash,
      const key_equal& equal,
      const allocator_type& alloc)
      : pmr_unordered_map<Key, T, Hash, KeyEqual>(
            first, last, bucket_count, hash, equal, alloc) {
  }

  template <class InputIt>
  constexpr unordered_map(
      InputIt first,
      InputIt last,
      size_type bucket_count,
      const allocator_type& alloc)
      : pmr_unordered_map<Key, T, Hash, KeyEqual>(
            first, last, bucket_count, Hash(), KeyEqual(), alloc) {
  }

  template <class InputIt>
  constexpr unordered_map(
      InputIt first,
      InputIt last,
      size_type bucket_count,
      const Hash& hash,
      const allocator_type& alloc)
      : pmr_unordered_map<Key, T, Hash, KeyEqual>(
            first, last, bucket_count, hash, KeyEqual(), alloc) {
  }

  constexpr unordered_map(
      const unordered_map& other, const allocator_type& alloc)
      : pmr_unordered_map<Key, T, Hash, KeyEqual>(other, alloc) {
  }

  constexpr unordered_map(unordered_map&& other, const allocator_type& alloc)
      : pmr_unordered_map<Key, T, Hash, KeyEqual>(other, alloc) {
  }

  constexpr unordered_map(
      std::initializer_list<value_type> init,
      size_type bucket_count,
      const Hash& hash,
      const key_equal& equal,
      const allocator_type& alloc)
      : pmr_unordered_map<Key, T, Hash, KeyEqual>(
            init, bucket_count, hash, equal, alloc) {
  }

  constexpr unordered_map(
      std::initializer_list<value_type> init,
      size_type bucket_count,
      const allocator_type& alloc)
      : pmr_unordered_map<Key, T, Hash, KeyEqual>(
            init, bucket_count, Hash(), KeyEqual(), alloc) {
  }

  constexpr unordered_map(
      std::initializer_list<value_type> init,
      size_type bucket_count,
      const Hash& hash,
      const allocator_type& alloc)
      : pmr_unordered_map<Key, T, Hash, KeyEqual>(
            init, bucket_count, hash, KeyEqual(), alloc) {
  }
};

}  // namespace tiledb::common::pmr

#endif  // TILEDB_COMMON_PMR_H
