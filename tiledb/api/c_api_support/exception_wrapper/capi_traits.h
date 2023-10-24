/**
 * @file tiledb/api/c_api_support/exception_wrapper/capi_traits.h
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
 * This file defines the trait classes for CAPI functions.
 */

namespace tiledb::api::traits {

template <auto f>
struct CAPIFunctionNameTrait;

// The default implementation has no name member.
template <class... Args, capi_return_t (*f)(Args...)>
struct CAPIFunctionNameTrait<f> {
  static constexpr bool exists = false;
};

#define TILEDB_CAPI_NAME_TRAIT(func)                        \
  template <>                                               \
  struct tiledb::api::traits::CAPIFunctionNameTrait<func> { \
    static constexpr bool exists = true;                    \
    static constexpr const char* name = #func;              \
  }

template <auto f>
constexpr const char* get_name() {
  // We'll still fail without this, but the idea is to make it obvious that
  // the dev needs to add a new TILEDB_NAME_DEF entry in this file.
  static_assert(
      CAPIFunctionNameTrait<f>::exists,
      "Missing TILEDB_CAPI_NAME_TRAIT declaration.");
  return CAPIFunctionNameTrait<f>::name;
}

}  // namespace tiledb::api::traits
