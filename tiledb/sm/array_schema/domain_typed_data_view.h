/**
 * @file tiledb/sm/array_schema/domain_typed_data_view.h
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
 * This file defines a data-view class for Domain
 */

#ifndef TILEDB_ARRAYSCHEMA_DOMAIN_TYPED_DATUM_VIEW_H
#define TILEDB_ARRAYSCHEMA_DOMAIN_TYPED_DATUM_VIEW_H

#include "tiledb/common/common.h"
#include "tiledb/common/types/untyped_datum.h"
#include "tiledb/sm/array_schema/domain.h"
#include <utility>

namespace tiledb::sm {
/**
 * State-free class that carries a type
 *
 * @tparam T The type carried by the tag
 */
template<class T>
struct Tag {
  using type = T;
  Tag() = default;
  Tag(const Tag&) = delete;
  Tag& operator=(const Tag&) = delete;
  Tag(Tag&&) = delete;
  Tag& operator=(Tag&&) = delete;
};
static_assert(std::is_default_constructible_v<Tag<void>>);
static_assert(!std::is_copy_constructible_v<Tag<void>>);
static_assert(!std::is_copy_assignable_v<Tag<void>>);
static_assert(!std::is_move_constructible_v<Tag<void>>);
static_assert(!std::is_move_assignable_v<Tag<void>>);

/**
 * Allocated storage for arrays of varying size.
 *
 * This class sits between the gap between standard library classes `array` and
 * `vector`. `array` only allocates fixed-length sequences, since the length is
 * given as a template argument. `vector` allocates variable-length sequences,
 * but allows them to be resized. This class allocates storage for a variable-
 * length sequence which is thereafter fixed; it can't be resized.
 *
 * This class does not exactly _fill_ the gap, however. Filling the gap would
 * require that the array elements be default-constructed in place, as `array`
 * does. This class instead simply allocates storage. Any class that uses this
 * storage has responsibility for constructing and destroying objects.
 *
 * In addition, this class does not require its allocator to be
 * default-constructible. This means that users of the class might have
 * difficulty using swap to implement move-construction, since there may be no
 * natural way to make a nonce object to swap with. Instead, this class provides
 * a template argument that allows the user to initialize the storage as soon as
 * possible after allocation. It's used within the body of constructor of this
 * class, immediately after member variable initialization.
 *
 * @tparam T The type of the element used in the array
 * @tpara Alloc Allocator for storage. Defaults to the library allocator.
 */
template <class T, class Alloc = tdb::allocator<T>>
class DynamicArrayStorage {
  using self_type = DynamicArrayStorage<T,Alloc>;

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
   * There's no invariant that data_ not be null. The only time, however, when
   * it might be null is during move construction, when it's assigned null to
   * achieve transfer semantics.
   */
  T* data_;

 public:
  template<class C>
  class NullInitializer{
   public:
    inline static void initialize(T*, size_t){};
  };

  /**
   * Ordinary constructor with a length-of-sequence argument. This is the
   * version for non-tracing allocators.
   *
   * @tparam X Nonce class present only because something must be substituted as
   * a argument for `is_tracing_enabled`.
   * @param
   * @param n The number of elements in the sequence to allocate
   *
   * Postcondition: data_ is non-null.
   */
  template <class SizeT, template<class> class I, class X, class... Args>
  DynamicArrayStorage(SizeT n, const Alloc& a, Tag<I<X>>, Args&&... args)
      : a_(a)
      , size_(n)
      , data_(a_.allocate(size_)) {
    for (SizeT i = 0; i < n; ++i) {
      I<self_type>::initialize(&data_[i], i, std::forward<Args>(args)...);
    }
  }

  /**
   * Swap
   */
  inline void swap(DynamicArrayStorage& x) noexcept {
    std::swap(a_, x.a_);
    std::swap(size_, x.size_);
    std::swap(data_, x.data_);
  }

  /**
   * Default construction is prohibited.
   */
  DynamicArrayStorage() = delete;

  /**
   * Copy construction is prohibited. The rationale is that since this
   * container uses non-initialized memory, it can't know what the semantics
   * of a copy operation should be. A user of this class that wants to make
   * a copy of this object must do so according to its own semantics.
   */
  DynamicArrayStorage(const DynamicArrayStorage&) = delete;

  /**
   * Copy assignment is prohibited. See the copy constructor for rationale.
   */
  DynamicArrayStorage& operator=(const DynamicArrayStorage&) = delete;

  /**
   * Move construction has transfer semantics.
   */
  DynamicArrayStorage(DynamicArrayStorage&& x) noexcept
    : a_(x.a_), size_(x.size_), data_(x.data_) {
    x.data_ = nullptr;
  }

  /**
   * Move assignment has swap semantics.
   */
  DynamicArrayStorage& operator=(DynamicArrayStorage&& x) noexcept {
    swap(x);
    return *this;
  }

  /**
   * The destructor deallocates, but does not call destructors on its contents.
   * The user of this class has the responsibility for ensuring that all
   * required destructors are called.
   */
  ~DynamicArrayStorage() {
    if (data_ != nullptr) {
      a_.deallocate(data_, size_);
    }
  }

  [[nodiscard]] inline size_t size() const {
    return size_;
  }

  [[nodiscard]] inline T* data() {
    return data_;
  }

  [[nodiscard]] inline const T* data() const {
    return data_;
  }

  /**
   * Constant index accessor
   */
  inline const T& operator[](size_t pos) const {
    return data_[pos];
  }
};

/**
 * Non-member swap function for std::swap to use.
 */
template <class T, class A>
inline void swap(DynamicArrayStorage<T,A>& a, DynamicArrayStorage<T,A>& b) {
  a.swap(b);
}

/*
 * Non-tracing factory for DynamicArrayStorage
 *
 * @tparam T The type of object to allocate
 * @tparam X The type of the origin label, which is ignored
 * @tparam
 * @param n The number of elements in the array
 *
 * [Note: See https://github.com/doxygen/doxygen/issues/6926 for why the
 * unnamed argument cannot yet have documentation.]
 */
//template<class T, template<class> class I = DynamicArrayStorageNullInitializer, class X, std::enable_if_t<!, int> = 0>
//inline DynamicArrayStorage<T, tdb::allocator<T>> MakeDynamicArrayStorage(X, size_t n) {
//  return DynamicArrayStorage<T,tdb::allocator<T>, I>(n, tdb::allocator<T>());
//}

/**
 * Tracing factory for DynamicArrayStorage, tracing label
 *
 * @tparam T The type of object to allocate
 * @tparam X The type of the origin label, which is ignored
 * @tparam
 * @param n The number of elements in the array
 */
//template<class T, std::enable_if_t<tdb::is_tracing_enabled<T>::value, int> = 0>
//inline DynamicArrayStorage<T, tdb::allocator<T>> MakeDynamicArrayStorage(TracingLabel origin, size_t n) {
//  return DynamicArrayStorage<T,tdb::allocator<T>>(n, tdb::allocator<T>(origin));
//}

/**
 * Tracing factory for DynamicArrayStorage, string view
 *
 * @tparam T The type of object to allocate
 * @tparam X The type of the origin label, which is ignored
 * @tparam
 * @param n The number of elements in the array
 */
//template<class T, std::enable_if_t<tdb::is_tracing_enabled<T>::value, int> = 0>
//inline DynamicArrayStorage<T, tdb::allocator<T>> MakeDynamicArrayStorage(const std::string_view& origin, size_t n) {
//  return DynamicArrayStorage<T,tdb::allocator<T>>(n, tdb::allocator<T>(TracingLabel(origin)));
//}

/**
 * Tracing factory for DynamicArrayStorage, string constant
 *
 * @tparam T The type of object to allocate
 * @tparam X The type of the origin label, which is ignored
 * @tparam
 * @param n The number of elements in the array
 */
template<class T, template<class> class I, class... Args, int m>
inline DynamicArrayStorage<T, tdb::allocator<T>> MakeDynamicArrayStorage([[maybe_unused]] const char (&origin)[m], unsigned int n, Tag<I<int>>, Args&&... args) {
  if constexpr (tdb::is_tracing_enabled<T>::value) {
    return DynamicArrayStorage<T, tdb::allocator<T>>(n),
           tdb::allocator<T>(std::string_view(origin, m - 1));
  } else {
    return DynamicArrayStorage<T, tdb::allocator<T>>{n, tdb::allocator<T>(), Tag<I<void>>{}, std::forward<Args>(args)...};
  }
}

/**
 * A datum-view class for values within a domain.
 *
 * This class must be contextually associated with a Domain object; it has no
 * class member that explicitly links a datum instance with a Domain. This
 * is a design choice that promotes efficiency by not repeating a Domain
 * reference that can be obtained without going through this class.
 *
 * A consequence of this choice is that values of this class are not typed
 * within the class itself. These values are "domain-typed" from a user's point
 * of view, but implemented as untyped, that is, only as storage. Along these
 * lines, the size of the Domain is not stored within this class.
 *
 * This class is not publicly constructible by design, in order to maintain
 * adherence to principle of C.41. It's only available through factory functions
 * in friend classes that do not expose an object until after it has been fully
 * initialized. Strictly speaking, this class in isolation is not compliant with
 * C.41, but the combination of it with its factory friends is compliant.
 */
class DomainTypedDataView {
  /// Friends with whitebox testing class
  friend class WhiteboxDomainTypedDataView;
  /**
   * Friends with the factory method in DomainBuffersView.
   */
  friend class DomainBuffersView;

 public:
  /**
   * The type of the element for each dimension
   */
  using view_type = tdb::UntypedDatumView;
  /*
   * The destructor of this class does not call destructors of the data view
   * objects it holds. This is acceptable only if the data view class is
   * trivially destructible.
   */
  static_assert(std::is_trivially_destructible_v<view_type>);

 private:
  /**
   * Storage for each of the dimension values.
   *
   * The container used at present stores its size (the number of dimension)
   * in each element, in order that it be able to deallocate memory on
   * destruction. The size of such object doesn't change for elements within
   * a specific dimension, so there's opportunity to optimize for memory use.
   * Doing this would require a container class that was able to obtain its size
   * outside of storing it in each instance. This is a rather more difficult
   * task than it might seem at first glance. The present container is used
   * as a matter of expediency over this issue.
   *
   * Invariant: array_ is non-null.
   *
   * Note on the invariant: This class represents a domain, one value for each
   * dimension. It does *not* introduce anything like a nil value to the domain,
   * since that would add a (single) new value to what's specified by the
   * cartesian product of the dimension types. If a consumer needs a nil value,
   * then `optional<DomainTypedDataView>` should be used.
   */
  DynamicArrayStorage<view_type> array_;

  /**
   * Constructor with an additional number-of-dimensions argument.
   *
   * The Domain argument is ignored, but still required to designate what Domain
   * this value is associated with. Consider it a form of compiler-enforced
   * documentation.
   *
   * @pre The number of dimensions in the associated domain must be
   * equal to `n`
   * @param origin Source code origin of a call to this function
   * @param n The number of dimensions in the associated Domain
   */
  template<template<class> class I, class... Args>
  DomainTypedDataView(const Domain& domain, Tag<I<int>>, Args&&... args)
      : array_(MakeDynamicArrayStorage<view_type, I>(HERE(), static_cast<size_t>(domain.dim_num()), Tag<I<int>>{}, domain, std::forward<Args>(args)...)) {
  }

  /**
   * Non-constant index operator only available to factory friend functions.
   * @param k Index
   * @return
   */
  [[nodiscard]] inline view_type& operator[](size_t k) {
    return array_.data()[k];
  }

 public:
  /**
   * Default constructor is prohibited.
   *
   * We don't require the allocator of this class to be default-constructible.
   * Nor do we require it to be constructible with any default argument we might
   * supply. As a result, without being given an allocator object, we can't
   * construct this one.
   */
  DomainTypedDataView() = delete;

  ~DomainTypedDataView() {
      (void) 1;
  }

  /**
   * Move construction has transfer semantics.
   */
  DomainTypedDataView(DomainTypedDataView&& x) noexcept
  : array_(move(x.array_))
  {}

  /**
   * Move assignment has exchange semantics.
   * @param x
   * @return
   */
  DomainTypedDataView& operator=(DomainTypedDataView&& x) noexcept
  {
    swap(x);
    return *this;
  }

  inline void swap(DomainTypedDataView& x) noexcept {
    std::swap(array_, x.array_);
  }

  [[nodiscard]] inline size_t size() const noexcept {
    return array_.size();
  }

  [[nodiscard]] inline const view_type* data() const noexcept {
    return array_.data();
  }

  [[nodiscard]] inline const view_type& operator[](size_t k) const noexcept {
    return array_.data()[k];
  }
};

inline void swap(DomainTypedDataView& a, DomainTypedDataView& b) {
  a.swap(b);
}

static_assert(!std::is_default_constructible_v<DomainTypedDataView>);
static_assert(!std::is_copy_constructible_v<DomainTypedDataView>);
static_assert(!std::is_copy_assignable_v<DomainTypedDataView>);
static_assert(std::is_move_constructible_v<DomainTypedDataView>);
static_assert(std::is_move_assignable_v<DomainTypedDataView>);
static_assert(std::is_swappable_v<DomainTypedDataView>);

}  // namespace tiledb::sm
#endif  // TILEDB_ARRAYSCHEMA_DOMAIN_TYPED_DATUM_VIEW_H
