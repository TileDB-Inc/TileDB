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
 * @section DESCRIPTION Contains definition of the CAPIHandle class.
 */

#ifndef TILEDB_API_HANDLE_H
#define TILEDB_API_HANDLE_H

#include <concepts>

#include "tiledb/api/c_api_support/argument_validation.h"
#include "tiledb/common/common.h"

namespace tiledb::api {

class CAPIHandle;

template <class T>
concept CAPIHandleImplementation =
    std::derived_from<T, CAPIHandle> && requires {
      { T::object_type_name } -> std::convertible_to<std::string_view>;
    };

/**
 * Handle class for objects visible through the C API.
 *
 * This class has a single responsibility: to manage allocation of API-visible
 * opaque `struct`. API callers reference these `struct` only through pointers,
 * and the library has the responsibility for creating them. This class is
 * a base class for them.
 */
class CAPIHandle {
 public:
  /**
   * Virtual destructor.
   */
  virtual ~CAPIHandle() = default;
};

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
 * @tparam T A class satisfying CAPIHandleImplementation
 * @param args Argument pack for `T` constructor
 * @return A pointer to T*
 */
template <CAPIHandleImplementation T>
[[nodiscard]] T* make_handle(auto&&... args) {
  return tdb_new(T, std::forward<decltype(args)>(args)...);
}

/**
 * Destroy a handle object
 *
 * Destruction in brief:
 * - Call tdb_delete on the pointer.
 * - Reset the pointer to null.
 *
 * @param p Reference to a `CAPIHandle*`
 */
template <CAPIHandleImplementation T>
void break_handle(T*& p) {
  tdb_delete(p);
  p = nullptr;
}

/**
 * Generic validation of candidate handle pointers.
 *
 * This class is _only_ for implementation of handle-specific validation
 * functions. It is _not_ the case that generic validity is the only kind of
 * validity. Each handle class may add specific validation checks as well.
 *
 * @tparam T A class satisfying CAPIHandleImplementation
 * @tparam E Exception type to throw if handle is not valid
 * @param p a possible pointer to an object of type T
 */
template <CAPIHandleImplementation T, class E = CAPIException>
void ensure_handle_is_valid(const T* p) {
  if (p == nullptr) {
    throw E(
        std::string("Invalid TileDB ") + std::string(T::object_type_name) +
        " object");
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
inline bool is_handle_valid(const CAPIHandle* p) {
  return p != nullptr;
}

}  // namespace tiledb::api

#endif  // TILEDB_API_HANDLE_H
