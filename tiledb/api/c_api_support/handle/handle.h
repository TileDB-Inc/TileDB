/**
 * @file tiledb/api/handle/handle.h
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
 */

#ifndef TILEDB_API_HANDLE_H
#define TILEDB_API_HANDLE_H

#include "tiledb/api/c_api_support/argument_validation.h"
#include "tiledb/common/common.h"

namespace tiledb::api {

namespace detail {
template <class T>
class WhiteboxCAPIHandle;
}

/*
 * API Handles
 *
 * The C API present its objects with pointers to opaque `struct` objects. The
 * existing (legacy) policy has been manual allocation these `struct` (using
 * `tdb_new`) and returning the pointer. Implementing such a pattern with
 * smart pointers presents a challenge. On one hand, some external function
 * needs to allocate such an object, because we need a pointer to the object,
 * not just the object itself. On the other hand, the allocation must also be,
 * in some sense, inside the object, because we need to rely on its destructor
 * to release the allocation.
 *
 * Furthermore, any object that points to itself is at risk of becoming a memory
 * leak. This is not an obstacle, but rather a criterion for success. Because we
 * are using arbitrary C function calls to govern the lifespan of an object, any
 * means that does not keep itself in existence without a C++ object referencing
 * it cannot be a possible solution.
 *
 * Hence, the base handle class satisfies this requirement thus:
 *
 * 1. All handles are constructed by factory functions that perform the
 * initial allocation.
 * 2. `Handle` stores a `shared_ptr` to itself. This is similar to what
 * `std::enable_shared_from_this` does, but we want a `shared_ptr` always
 * stored, rather than the `weak_ptr` that `shared_from_this` uses.
 *
 * A handle instance, even if constructed as a temporary variable, will persist
 * in memory indefinitely. Only when it's explicitly reset will it deallocate.
 *
 * The analogy to `enable_shared_from_this` is closer than it might seem. That
 * class is a base class with a `weak_ptr` that's initialized to any
 * `shared_ptr` that's constructed with that base class. We don't have any
 * ability to change the `shared_ptr` constructor to do what we want, so we are
 * settling for something a bit clunkier: mandatory factory construction.
 */

/**
 * Handle class for objects visible through the C API.
 *
 * This class has a single responsibility: to manage allocation of API-visible
 * opaque `struct`. API callers reference these `struct` only through pointers,
 * and the library has the responsibility for creating them. This class is
 * a base class for them.
 *
 * This class is not C.41-compliant in isolation. The combination of this class
 * and construction by the factory `make_handle` is C.41-compliant.
 *
 * @tparam T A type derived from this one; this is the CRTP
 */
template <class T>
class CAPIHandle {
  friend class detail::WhiteboxCAPIHandle<T>;
  using shared_ptr_type = shared_ptr<T>;

  /**
   * A pointer to the allocation used to hold this object.
   *
   * It's default-constructed and then immediately assigned through `know_self`
   * in the factory.
   */
  shared_ptr_type self_{};

  /**
   * Initialize the pointer-to-self.
   *
   * Note that this function takes its argument by value and not by reference.
   * We want to make exactly one copy of the `shared_ptr`. We have chosen to do
   * that by using the implicit copy constructor when passing by value and an
   * explicit move constructor in the body of the function.
   *
   * @pre This object must be under management of a `shared_ptr`
   *
   * @param x A `shared_ptr` to this object
   */
  inline void know_self(shared_ptr_type x) {
    self_ = move(x);
  }

  /**
   * Reset this handle.
   *
   * This function destroys the self-reference, which leads to this object
   * being destroyed.
   */
  void reset() {
    self_.reset();
  }

 protected:
  /**
   * Default constructor is required because `self_` is initialized in the
   * factory. It's protected because naked handles are meaningless.
   */
  CAPIHandle() = default;

 public:
  /**
   * Construct a handle object and return its allocated address.
   *
   * Construction in brief:
   * - Allocate memory for derived handle as `shared_ptr`
   * - Default-construct `self_`
   * - Initialize member variables of derived handle
   * - Copy `shared_ptr` into derived handle
   * - Return plain pointer value of `shared_ptr`
   *
   * @tparam T A class derived from Handle
   * @tparam Args Argument type pack for `T` constructor
   * @param args Argument pack for `T` constructor
   * @return
   */
  template <class... Args>
  [[nodiscard]] static T* make_handle(Args&&... args) {
    auto p{make_shared<T>(HERE(), std::forward<Args>(args)...)};
    p->know_self(p);  // copy-construct for argument passed by value
    return p.get();
    /*
     * `p` goes out of scope here and is destroyed. There's a copy of `p`,
     * though, in the handle itself, so thus the newly-created object is not
     * destroyed.
     */
  }

  /**
   * Destroy a handle object
   *
   * Destruction in brief:
   * - Reset the `shared_ptr` within the object.
   * - If it was the last pointer, then the derived handle will be destroyed
   * - The member variables of the derived handle are destroyed.
   * - Self-pointer is destroyed, trivial because it has just been reset
   * - Nullify the pointer argument to ensure it can't be used again
   *
   * @tparam T A class derived from Handle
   * @param p Reference to `T*`
   */
  static void break_handle(T*& p) {
    p->reset();
    p = nullptr;
  }

  /**
   * Stored object
   *
   * @return pointer to our stored object
   */
  inline T& get() {
    return *self_.get();
  }

  /**
   * Stored object, const version
   *
   * @return const pointer to our stored object
   */
  inline const T& get() const {
    return *self_.get();
  }

  inline static std::string handle_name() {
    return std::string(T::object_type_name);
  }
};

/**
 * Generic validation of candidate handle pointers.
 *
 * This class is _only_ for implementation of handle-specific validation
 * functions. It is _not_ the case that generic validity is the only kind of
 * validity. Each handle class may add specific validation checks as well.
 *
 * @tparam T A class derived from CAPIHandle
 * @tparam E Exception type to throw if handle is not valid
 * @param p a possible pointer to an object of type T
 */
template <class T, class E = CAPIStatusException>
void ensure_handle_is_valid(const T* p) {
  if (p == nullptr) {
    throw E(std::string("Invalid TileDB ") + T::handle_name() + " object");
  }
  if (p != &p->get()) {
    throw E(T::handle_name() + " object is not self-consistent");
  }
}

/**
 * Non-throwing handle validation
 *
 * This function is a variant of `ensure_handle_is_valid` that returns a boolean
 * `false` instead of throwing. No explanations are provided, obviously.
 *
 * This function supports the specific case where we require a boolean pre-check
 * at one time and a full check at a later one. Ordinarily this is the wrong way
 * to do things. We use it, however, in the exception wrapper, whose action
 * classes cannot be fully C.41-compliant.
 */
template <class T>
bool is_handle_valid(const T* p) {
  return (p != nullptr) && (p == &p->get());
}

}  // namespace tiledb::api

#endif  // TILEDB_API_HANDLE_H
