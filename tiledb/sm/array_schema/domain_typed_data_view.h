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

#include <utility>
#include "dynamic_array.h"
#include "tiledb/common/common.h"
#include "tiledb/common/types/untyped_datum.h"
#include "tiledb/sm/array_schema/domain.h"

namespace tiledb::sm {

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
  /**
   * Friends with its whitebox testing class
   */
  friend class WhiteboxDomainTypedDataView;
  /**
   * Friends with DomainBuffersView for its factory.
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
  DynamicArray<view_type> array_;

  /**
   * Constructor for use with factory functions.
   *
   * Each factory function must supply its own initialization policy class and
   * additional arguments for the initialize function. `DynamicArray` supplies
   * its arguments (an address and an index). This constructor supplies a
   * `Domain` argument.
   *
   * @tparam I Initialization policy class for DynamicArray
   * @param domain Domain with which to associate values
   */
  template <class I, class... Args>
  DomainTypedDataView(const Domain& domain, Tag<I>, Args&&... args)
      : array_(MakeDynamicArray<view_type, I>(
            HERE(),
            domain.dim_num(),
            Tag<I>{},
            domain,
            std::forward<Args>(args)...)) {
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

  /**
   * Default destructor does not call destructors on its contents.
   */
  ~DomainTypedDataView() = default;

  /**
   * Move construction has transfer semantics.
   */
  DomainTypedDataView(DomainTypedDataView&& x) noexcept
      : array_(move(x.array_)) {
  }

  /**
   * Move assignment has exchange semantics.
   */
  DomainTypedDataView& operator=(DomainTypedDataView&& x) noexcept {
    swap(x);
    return *this;
  }

  /// Swap
  inline void swap(DomainTypedDataView& x) noexcept {
    std::swap(array_, x.array_);
  }

  /**
   * Size of this object as a container. Same as the number of dimensions in the
   * associated domain.
   *
   * @return The number of data objects in this container.
   */
  [[nodiscard]] inline size_t size() const noexcept {
    return array_.size();
  }

  /**
   * Pointer to the internal container as an array of view objects.
   *
   * @return Pointer to the first element in the container
   */
  [[nodiscard]] inline const view_type* data() const noexcept {
    return array_.data();
  }

  /**
   * Indexed accessor, non-constant version.
   *
   * @param k Index into this container
   * @return A datum
   */
  [[nodiscard]] inline view_type& operator[](size_t k) noexcept {
    return array_.data()[k];
  }

  /**
   * Indexed accessor, constant version.
   *
   * @param k Index into this container
   * @return A datum
   */
  [[nodiscard]] inline const view_type& operator[](size_t k) const noexcept {
    return array_.data()[k];
  }
};

/**
 * Non-member swap.
 */
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
