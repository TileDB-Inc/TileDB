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

#include <list>
#include <map>
#include <unordered_map>
#include <vector>

#ifdef USE_CPP17_PMR
#include "polymorphic_allocator/polymorphic_allocator.h"
#else
#include <memory_resource>
#endif

#include "common.h"

namespace tiledb::common::pmr {

#ifdef USE_CPP17_PMR

using memory_resource = cpp17::pmr::memory_resource;

template <class Tp>
using polymorphic_allocator = cpp17::pmr::polymorphic_allocator<Tp>;

#else

using memory_resource = std::pmr::memory_resource;

template <class Tp>
using polymorphic_allocator = std::pmr::polymorphic_allocator<Tp>;
#endif

memory_resource* get_default_resource();

/* ********************************* */
/*     PMR UNIQUE_PTR DECLARATION    */
/* ********************************* */

template <class Tp>
class unique_ptr_deleter {
 public:
  unique_ptr_deleter() = delete;

  unique_ptr_deleter(memory_resource* resource, size_t nmemb, size_t alignment)
      : resource_(resource)
      , nmemb_(nmemb)
      , alignment_(alignment) {
  }

  void operator()(Tp* ptr) {
    if (ptr == nullptr) {
      return;
    }
    if (!std::is_trivially_destructible<Tp>::value) {
      // destruct in reverse order since the elements are constructed in
      // forwards order
      for (size_t i = nmemb_; i > 0; i--) {
        ptr[i - 1].~Tp();
      }
    }

    const size_t dealloc_size = nmemb_ * sizeof(Tp);
    resource_->deallocate(ptr, dealloc_size, alignment_);
  }

  memory_resource* resource_;
  size_t nmemb_;
  size_t alignment_;
};

template <class Tp>
using unique_ptr = std::unique_ptr<Tp, unique_ptr_deleter<Tp>>;

template <class Tp>
unique_ptr<Tp> make_unique(
    memory_resource* resource, size_t nmemb, size_t alignment) {
  static_assert(std::is_arithmetic_v<Tp> || std::is_same_v<Tp, std::byte>);

  auto alloc_size = nmemb * sizeof(Tp);
  Tp* data = static_cast<Tp*>(resource->allocate(alloc_size, alignment));

  if (data == nullptr) {
    throw std::bad_alloc();
  }

  auto deleter = unique_ptr_deleter<Tp>(resource, nmemb, alignment);

  return std::unique_ptr<Tp, unique_ptr_deleter<Tp>>(data, deleter);
}

template <class Tp>
unique_ptr<Tp> make_unique(memory_resource* resource, size_t nmemb) {
  return make_unique<Tp>(resource, nmemb, alignof(Tp));
}

/**
 * Constructs an object in place and returns it in a `unique_ptr`.
 */
template <class Tp, typename... Args>
unique_ptr<Tp> emplace_unique(memory_resource* resource, Args&&... args) {
  Tp* obj = static_cast<Tp*>(resource->allocate(sizeof(Tp), alignof(Tp)));
  new (obj) Tp(std::forward<Args>(args)...);

  auto deleter = unique_ptr_deleter<Tp>(resource, 1, alignof(Tp));

  return std::unique_ptr<Tp, unique_ptr_deleter<Tp>>(obj, deleter);
}

/* ********************************* */
/*         PMR LIST DECLARATION      */
/* ********************************* */
template <class Tp>
using pmr_list = std::list<Tp, polymorphic_allocator<Tp>>;

template <class Tp>
class list : public pmr_list<Tp> {
 public:
  using value_type = typename pmr_list<Tp>::value_type;
  using allocator_type = typename pmr_list<Tp>::allocator_type;
  using size_type = typename pmr_list<Tp>::size_type;
  using difference_type = typename pmr_list<Tp>::difference_type;
  using reference = typename pmr_list<Tp>::reference;
  using const_reference = typename pmr_list<Tp>::const_reference;
  using pointer = typename pmr_list<Tp>::pointer;
  using const_pointer = typename pmr_list<Tp>::const_pointer;
  using iterator = typename pmr_list<Tp>::iterator;
  using const_iterator = typename pmr_list<Tp>::const_iterator;
  using reverse_iterator = typename pmr_list<Tp>::reverse_iterator;
  using const_reverse_iterator = typename pmr_list<Tp>::const_reverse_iterator;

  // Delete all default constructors because they don't require an allocator
  list() = delete;
  list(const list& other) = delete;
  list(list&& other) = delete;

  // Delete non-allocator aware copy and move assign.
  list& operator=(const list& other) = delete;
  list& operator=(list&& other) noexcept = delete;

  explicit list(const allocator_type& alloc) noexcept
      : pmr_list<Tp>(alloc) {
  }

  explicit list(size_type count, const Tp& value, const allocator_type& alloc)
      : pmr_list<Tp>(count, value, alloc) {
  }

  explicit list(size_type count, const allocator_type& alloc)
      : pmr_list<Tp>(count, alloc) {
  }

  template <class InputIt>
  list(InputIt first, InputIt last, const allocator_type& alloc)
      : pmr_list<Tp>(first, last, alloc) {
  }

  list(const list& other, const allocator_type& alloc)
      : pmr_list<Tp>(other, alloc) {
  }

  list(list&& other, const allocator_type& alloc)
      : pmr_list<Tp>(other, alloc) {
  }

  list(std::initializer_list<Tp> init, const allocator_type& alloc)
      : pmr_list<Tp>(init, alloc) {
  }
};

/* ********************************* */
/*       PMR VECTOR DECLARATION      */
/* ********************************* */

template <class Tp>
using vector = std::vector<Tp, polymorphic_allocator<Tp>>;

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
    polymorphic_allocator<std::pair<const Key, T>>>;

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

  // Delete all default constructors because they don't require an allocator
  constexpr unordered_map() = delete;
  constexpr unordered_map(const unordered_map& other) = delete;
  constexpr unordered_map(unordered_map&& other) = delete;

  // Delete non-allocator aware copy and move assign.
  constexpr unordered_map& operator=(const unordered_map& other) = delete;
  constexpr unordered_map& operator=(unordered_map&& other) noexcept = delete;

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

/* ********************************* */
/*         PMR MAP DECLARATION       */
/* ********************************* */
template <class Key, class T, class Compare = std::less<Key>>
using pmr_map =
    std::map<Key, T, Compare, polymorphic_allocator<std::pair<const Key, T>>>;

template <class Key, class T, class Compare = std::less<Key>>
class map : public pmr_map<Key, T, Compare> {
 public:
  // Type declarations.
  using key_type = typename pmr_map<Key, T, Compare>::key_type;
  using mapped_type = typename pmr_map<Key, T, Compare>::mapped_type;
  using value_type = typename pmr_map<Key, T, Compare>::value_type;
  using size_type = typename pmr_map<Key, T, Compare>::size_type;
  using difference_type = typename pmr_map<Key, T, Compare>::difference_type;
  using key_compare = typename pmr_map<Key, T, Compare>::key_compare;
  using allocator_type = typename pmr_map<Key, T, Compare>::allocator_type;
  using reference = typename pmr_map<Key, T, Compare>::reference;
  using const_reference = typename pmr_map<Key, T, Compare>::const_reference;
  using pointer = typename pmr_map<Key, T, Compare>::pointer;
  using const_pointer = typename pmr_map<Key, T, Compare>::const_pointer;
  using iterator = typename pmr_map<Key, T, Compare>::iterator;
  using const_iterator = typename pmr_map<Key, T, Compare>::const_iterator;
  using reverse_iterator = typename pmr_map<Key, T, Compare>::reverse_iterator;
  using const_reverse_iterator =
      typename pmr_map<Key, T, Compare>::const_reverse_iterator;
  using node_type = typename pmr_map<Key, T, Compare>::node_type;
  using insert_return_type =
      typename pmr_map<Key, T, Compare>::insert_return_type;

  // Delete all default constructors because they don't require an allocator
  constexpr map() = delete;
  constexpr map(const map& other) = delete;
  constexpr map(map&& other) = delete;

  // Delete non-allocator aware copy and move assign.
  constexpr map& operator=(const map& other) = delete;
  constexpr map& operator=(map&& other) noexcept = delete;

  constexpr explicit map(const Compare& comp, const allocator_type& alloc)
      : pmr_map<Key, T, Compare>(comp, alloc) {
  }

  constexpr explicit map(const allocator_type& alloc)
      : pmr_map<Key, T, Compare>(alloc) {
  }

  template <class InputIt>
  constexpr map(
      InputIt first,
      InputIt last,
      const Compare& comp,
      const allocator_type& alloc)
      : pmr_map<Key, T, Compare>(first, last, comp, alloc) {
  }

  template <class InputIt>
  constexpr map(InputIt first, InputIt last, const allocator_type& alloc)
      : pmr_map<Key, T, Compare>(first, last, Compare(), alloc) {
  }

  constexpr map(const map& other, const allocator_type& alloc)
      : pmr_map<Key, T, Compare>(other, alloc) {
  }

  constexpr map(map&& other, const allocator_type& alloc)
      : pmr_map<Key, T, Compare>(other, alloc) {
  }

  constexpr map(
      std::initializer_list<value_type> init,
      const Compare& comp,
      const allocator_type& alloc)
      : pmr_map<Key, T, Compare>(init, comp, alloc) {
  }

  constexpr map(
      std::initializer_list<value_type> init, const allocator_type& alloc)
      : pmr_map<Key, T, Compare>(init, Compare(), alloc) {
  }

  // Declare member class value_compare.
  class value_compare : public pmr_map<Key, T, Compare>::value_compare {
   public:
    constexpr bool operator()(
        const value_type& lhs, const value_type& rhs) const;
  };
};

}  // namespace tiledb::common::pmr

#endif  // TILEDB_COMMON_PMR_H
