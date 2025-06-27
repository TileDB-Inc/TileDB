/**
 * @file tiledb/sm/array_schema/dynamic_array.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file defines a dynamic array container
 */

#ifndef TILEDB_DYNAMIC_ARRAY_H
#define TILEDB_DYNAMIC_ARRAY_H

#include <stdexcept>
#include "tiledb/common/dynamic_memory/dynamic_memory.h"
#include "tiledb/common/tag.h"

namespace tiledb::sm {
/**
 * Allocated storage for arrays of varying size.
 *
 * This class sits between the gap between standard library classes `array`
 * and `vector`. `array` only allocates fixed-length sequences, since the
 * length is given as a template argument. `vector` allocates variable-length
 * sequences, but allows them to be resized. This class allocates storage for
 * a variable- length sequence which is thereafter fixed; it can't be resized.
 *
 * This class does not fill the gap exactly, however. Filling the gap would
 * require that the array elements be default-constructed in place, as `array`
 * does. This class does not require that its content type be
 * default-constructible, since that would make it ineligible for use with a
 * C.41-compliant class without a default constructor. This class is more
 * general, allowing non-default constructible content types.
 *
 * This class, therefore, fills the gap as a special case by allowing
 * default-construction of its contents even as it does not mandate
 * default-construction. Responsibility for initialization with its user. This
 * class is a combination of (1) uninitialized storage and (2) policy-based
 * initialization at construction time.
 *
 * In addition, this class does not require its allocator to be
 * default-constructible. This means that users of the class might have
 * difficulty using swap to implement move-construction, since there may be no
 * natural way to make a nonce object to swap with.
 *
 * @tparam T The type of the element used in the array
 * @tpara Alloc Allocator for storage. Defaults to the library allocator.
 */
template <class T, class Alloc = tiledb::common::allocator<T>>
class DynamicArray {
  /**
   * Allocator instance for type T
   */
  Alloc a_;

  /**
   * The number of allocated objects.
   *
   * The allocated size in bytes is `size_*sizeof(T)`.
   */
  size_t size_;

  /**
   * `data_` is an allocated pointer to hold a sequence of `T` whose length is
   * equal to `size`. It's allocated with the class template argument `Alloc`.
   *
   * There's no invariant that `data_` not be null. There is only one case,
   * however, when it might be null, which is during move construction, when
   * it's assigned null to achieve transfer semantics.
   */
  T* data_;

 public:
  /**
   * Size type [container.requirements.general]
   */
  using size_type = size_t;

  /**
   * The null initializer does no initialization, but does compile as a tagged
   * template argument for the constructor.
   *
   * @param item Pointer to uninitialized memory in which to construct an object
   * @param index Index in the array of this item
   */
  class NullInitializer {
   public:
    inline static void initialize(
        [[maybe_unused]] T* item, [[maybe_unused]] size_t index) {
      // Arguments ignored; named only to document policy concept
    };
  };

  /**
   * Default initializer policy.
   *
   * @param item Address at which to construct an object.
   */
  class DefaultInitializer {
   public:
    inline static void initialize(T* item, size_t) {
      new (item) T();
    };
  };

  /**
   * Constructor with no initialization of contained elements.
   *
   * The allocator is not called if the size is not positive.
   *
   * @tparam SizeT Type of number-of-elements argument.
   * @param n The number of elements in the sequence to allocate
   * @param a Allocates memory for the array
   *
   * Postcondition: data_ is non-null.
   */
  template <class SizeT>
  DynamicArray(SizeT n, const Alloc& a)
      : a_(a)
      , size_(n)
      , data_(
            n > 0 ? a_.allocate(size_) :
                    throw std::logic_error(
                        "zero-length dynamic array not permitted")) {
  }

  /**
   * Constructor with policy initialization of contained elements.
   *
   * @tparam SizeT Type of number-of-elements argument.
   * @tparam I Initialization policy
   * @param n The number of elements in the sequence to allocate
   * @param a Allocates memory for the array
   * @param
   * @param args Arguments forwarded to the initialization function
   *
   * Postcondition: data_ is non-null.
   */
  template <class SizeT, class I, class... Args>
  DynamicArray(SizeT n, const Alloc& a, tiledb::common::Tag<I>, Args&&... args)
      : DynamicArray(n, a) {
    for (SizeT i = 0; i < n; ++i) {
      I::initialize(&data_[i], i, std::forward<Args>(args)...);
    }
  }

  /**
   * Constructor with default construction of contained elements.
   *
   * Arguably, it would be better to have use a named marker class to specify
   * default-initialization as the initialization policy, but using a
   * void specialization on the Tag introduces no new symbols.
   *
   * @tparam SizeT Type of number-of-elements argument.
   * @param n The number of elements in the sequence to allocate
   * @param a Allocates memory for the array
   *
   * Postcondition: data_ is non-null.
   */
  template <class SizeT>
  DynamicArray(SizeT n, const Alloc& a, tiledb::common::Tag<void>)
      : DynamicArray(n, a, tiledb::common::Tag<DefaultInitializer>{}) {
  }

  /**
   * Swap
   */
  inline void swap(DynamicArray& x) noexcept {
    std::swap(a_, x.a_);
    std::swap(size_, x.size_);
    std::swap(data_, x.data_);
  }

  /**
   * Default construction is prohibited.
   *
   * The size of the container is fixed at construction time. The only sensible
   * length for a default object is zero, which would mean a forever-empty
   * container.
   *
   * Default construction would also complicate and enlarge the class. There's
   * no requirement that the allocator be default-constructible, and it needs to
   * store a copy of its allocator for use in the destructor. Default
   * construction would mean storing an optional allocator or equivalent.
   *
   * The design decision is that it's not worth having default construction.
   */
  DynamicArray() = delete;

  /**
   * Copy construction is prohibited.
   *
   * This container allocates only-possibly initialized memory. Depending on
   * the constructor it's either non-initialized or initialized by external
   * policy. This class can't know what the semantics of a copy operation
   * should be. A user of this class that wants to make a copy of an object
   * must create and initialize the copy. It's up to the user to ensure the
   * validity of the copy.
   */
  DynamicArray(const DynamicArray&) = delete;

  /**
   * Copy assignment is prohibited. See the copy constructor for rationale.
   */
  DynamicArray& operator=(const DynamicArray&) = delete;

  /**
   * Move construction has transfer semantics.
   */
  DynamicArray(DynamicArray&& x) noexcept
      : a_(x.a_)
      , size_(x.size_)
      , data_(x.data_) {
    x.data_ = nullptr;
  }

  /**
   * Move assignment has swap semantics.
   */
  DynamicArray& operator=(DynamicArray&& x) noexcept {
    swap(x);
    return *this;
  }

  /**
   * The destructor deallocates, but does not call destructors on its
   * contents. The user of this class has the responsibility for ensuring that
   * any required destructors are called.
   */
  ~DynamicArray() {
    if (data_ != nullptr) {
      a_.deallocate(data_, size_);
    }
  }

  /**
   * Size [container.requirements.general]
   *
   * @return Size of the array in bytes.
   */
  [[nodiscard]] inline size_t size() const {
    return size_;
  }

  /**
   * Accessor to the start of the contiguous container, non-constant.
   *
   * @return Pointer to the first element of the container.
   */
  [[nodiscard]] inline T* data() {
    return data_;
  }

  /**
   * Accessor to the start of the contiguous container, constant.
   *
   * @return Pointer to the first element of the container.
   */
  [[nodiscard]] inline const T* data() const {
    return data_;
  }

  /**
   * Non-constant unchecked-index accessor [sequence.reqmts]
   */
  inline T& operator[](size_t pos) {
    return data_[pos];
  }

  /**
   * Constant unchecked-index accessor [sequence.reqmts]
   */
  inline const T& operator[](size_t pos) const {
    return data_[pos];
  }
};

/**
 * Non-member swap.
 */
template <class T, class A>
inline void swap(DynamicArray<T, A>& a, DynamicArray<T, A>& b) {
  a.swap(b);
}

/**
 * Factory for DynamicArrayStorage, tracing label
 *
 * @tparam T The type of object to allocate
 * @tparam I Initialization policy
 * @param origin Label for tracing provenance of allocations
 * @param n The number of elements in the array
 */
template <class T, class I, class SizeT, class... Args>
inline DynamicArray<T, tiledb::common::allocator<T>> MakeDynamicArray(
    tiledb::common::TracingLabel origin,
    SizeT n,
    tiledb::common::Tag<I>,
    Args&&... args) {
  if constexpr (tiledb::common::is_tracing_enabled_v<>) {
    return DynamicArray<T, tiledb::common::allocator<T>>{
        n,
        tiledb::common::allocator<T>{origin},
        tiledb::common::Tag<I>{},
        std::forward<Args>(args)...};
  } else {
    return DynamicArray<T, tiledb::common::allocator<T>>{
        n,
        tiledb::common::allocator<T>{},
        tiledb::common::Tag<I>{},
        std::forward<Args>(args)...};
  }
}

/**
 * Factory for DynamicArray, string constant
 *
 * @tparam T The type of object to allocate
 * @tparam I Initialization policy
 * @param origin Label for tracing provenance of allocations
 * @param n The number of elements in the array
 */
template <class T, class I, class SizeT, int m, class... Args>
inline DynamicArray<T, tiledb::common::allocator<T>> MakeDynamicArray(
    [[maybe_unused]] const char (&origin)[m],
    SizeT n,
    tiledb::common::Tag<I>,
    Args&&... args) {
  if constexpr (tiledb::common::is_tracing_enabled_v<>) {
    return DynamicArray<T, tiledb::common::allocator<T>>{
        n,
        tiledb::common::allocator<T>{
            tiledb::common::TracingLabel{std::string_view(origin, m - 1)}},
        tiledb::common::Tag<I>{},
        std::forward<Args>(args)...};
  } else {
    return DynamicArray<T, tiledb::common::allocator<T>>{
        n,
        tiledb::common::allocator<T>{},
        tiledb::common::Tag<I>{},
        std::forward<Args>(args)...};
  }
}

}  // namespace tiledb::sm

#endif  // TILEDB_DYNAMIC_ARRAY_H
