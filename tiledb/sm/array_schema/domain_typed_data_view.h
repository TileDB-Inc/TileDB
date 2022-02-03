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

namespace tiledb::sm {

/**
 * Allocated storage for arrays of varying size.
 *
 * This class sets between the gap between standard library classes `array` and
 * `vector`. `array` only allocates fixed-length sequences, since the length is
 * given as a template argument. `vector` allocates variable-length sequences,
 * but allows them to be resized. This class allocates storage for a variable-
 * length sequence which is thereafter fixed; it can't be resized.
 *
 * This class does not fill the gap, however. Filling the gap would require that
 * the array elements be default-constructed in place, as `array` does. This
 * class instead simply allocates storage. Any class that uses this storage
 * has responsibility for constructing and destroying objects.
 *
 * Note that the constructors for this class are defined in pairs, one for
 * tracing allocators and one for non-tracing allocators. These pairs have
 * identical signatures, and exactly one is compiled.
 *
 * @tparam T The type of the element used in the array
 * @tpara Alloc Allocator for storage. Defaults to the library allocator.
 */
template <class T, class Alloc = tdb::allocator<T>>
class DynamicArrayStorage {
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
   */
  T* data_;

 public:
  /**
   * Ordinary constructor with a length-of-sequence argument. This is the
   * version for non-tracing allocators.
   *
   * @tparam X Nonce class present only because something must be substituted as
   * a argument for `is_tracing_enabled`.
   * @param
   * @param n The number of elements in the sequence to allocate
   *
   * [Note: See https://github.com/doxygen/doxygen/issues/6926 for why the
   * second argument cannot yet have documentation.]
   */
  template <
      class X = void,
      std::enable_if_t<!tdb::is_tracing_enabled<X>::value, int> = 0>
  DynamicArrayStorage(const std::string_view&, size_t n)
      : a_()
      , size_(n)
      , data_(a_.allocate(size_)) {
  }

  /**
   * Ordinary constructor with a length-of-sequence argument. This is the
   * version for tracing allocators.
   */
  template <
      class X = void,
      std::enable_if_t<tdb::is_tracing_enabled<X>::value, int> = 0>
  DynamicArrayStorage(const std::string_view& origin, size_t n)
      : a_(origin)
      , size_(n)
      , data_(a_.allocate(size_)) {
  }

  /**
   * Default constructor is prohibited.
   */
  DynamicArrayStorage() = delete;

  /**
   *
   */
  ~DynamicArrayStorage() {
    a_.deallocate(data_, size_);
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
  using view_type = tdb::UntypedDatumView;

 private:
  /*
   * The destructor of this class does not call destructors of the data view
   * objects it holds. This is acceptable only if the data view class is
   * trivially destructible.
   */
  static_assert(std::is_trivially_destructible_v<view_type>);

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
   */
  DynamicArrayStorage<view_type> array_;

  /**
   * Constructor with an additional number-of-dimensions argument, which is used
   * as the number of values.
   *
   * [Note: The domain argument is suppressed at the moment to avoid circular
   * inclusion. It should be added after the comparison functions are
   * rewritten.]
   *
   * The Domain argument is ignored, but still required to designate what Domain
   * this value is associated with. Consider a form of compiler-enforced
   * documentation.
   *
   * @pre The number of dimensions in the associated domain must be
   * equal to `n`
   * @param origin Source code origin of a call to this function
   * @param n The number of dimensions in the associated Domain
   */
  DomainTypedDataView(const std::string_view& origin, const Domain&, size_t n)
      : array_(origin, n) {
  }

  /**
   * Constructor with only a Domain argument.
   *
   * @param domain A Domain to associate with this object
   */
  DomainTypedDataView(const std::string_view& origin, const Domain& domain)
      : DomainTypedDataView(origin, domain, domain.dim_num()) {
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
  [[nodiscard]] inline size_t size() const {
    return array_.size();
  }

  [[nodiscard]] inline const view_type* data() const {
    return array_.data();
  }

  [[nodiscard]] inline const view_type& operator[](size_t k) const {
    return array_.data()[k];
  }
};

}  // namespace tiledb::sm
#endif  // TILEDB_ARRAYSCHEMA_DOMAIN_TYPED_DATUM_VIEW_H
