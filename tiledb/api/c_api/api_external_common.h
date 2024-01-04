/**
 * @file tiledb/api/c_api/external_common.h
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
 * This file declares common elements of the C API.
 */

#ifndef TILEDB_CAPI_EXTERNAL_COMMON_H
#define TILEDB_CAPI_EXTERNAL_COMMON_H

/*
 * Use C headers since we need to compile externally-visible headers as C
 */
#ifndef TILEDB_CAPI_WRAPPING
#include <stddef.h>
#include <stdint.h>
#endif

/*
 * Header "tiledb_export.h" is built by CMake `GenerateExportHeader` and appears
 * in the build tree. It defines the following five macros. Each of them
 * resolves syntactically to an attribute, although that can be a nonstandard
 * compiler-specific attribute.
 *   - TILEDB_DEPRECATED
 *   - TILEDB_EXPORT
 *   - TILEDB_DEPRECATED_EXPORT
 *   - TILEDB_NO_EXPORT
 *   - TILEDB_DEPRECATED_NO_EXPORT
 */
#include "tiledb_export.h"

/*
 * Policy: Visible API calls may not throw exceptions.
 * Problem: We can't specify that in C.
 * Solution: A macro that specifies it for C++ and elides it for C
 *
 * `TILEDB_NOEXCEPT` usage:
 * 1. Header files that may appear in C programs (such as tiledb.h) must use the
 *    macro because we require API entry points not to throw. They're defined
 *    with `noexcept` in C++ definitions, and so the declaration must match, but
 *    a C declaration must omit that keyword.
 * 2. Definitions of C API entry point functions must have `noexcept` and not
 *    use the macro. These functions are compiled only as C++. They don't need
 *    the macro and we omit it for clarity. These functions are wrapped versions
 *    of API implementation functions, and the wrapper provides the `noexcept`
 *    guarantee.
 * 3. Implementation functions should not be declared noexcept. They have no way
 *    of providing uniform error handling and should defer to the wrapper for
 *    that.
 *
 * tl;dr: Only use the macro in headers for "C" code.
 */
#ifdef __cplusplus
#define TILEDB_NOEXCEPT noexcept
#else
#define TILEDB_NOEXCEPT
#endif

#ifdef __cplusplus
extern "C" {
#endif

/**
 * C API standard return type.
 *
 * The current code is mostly returning the same type, but not universally. A
 * few API function return `void`, and they all have pointer arguments which
 * are invalid if passed null. This definition anticipates two developments:
 * 1. All API functions return the same type.
 * 2. Changing the type from an integer to a structure that can return errors,
 *    warnings, and other messages.
 */
typedef int32_t capi_return_t;

/**
 * C API return code
 *
 * The status codes denote broad categories of the success or failure of an API
 * call. The single success code is TILEDB_OK. The general-purpose error code is
 * TILEDB_ERR. A few special errors are also broken out.
 */
typedef int32_t capi_status_t;

/**
 * Extract a status code from a return value.
 *
 * At the present time, this is an identity function. It's defined in
 * anticipation of converting `capi_return_t` to a `struct` that will contain
 * a status code as well as a pointer to other information.
 *
 * @param x A status code returned from a C API call
 */
#ifdef __cplusplus
inline capi_status_t tiledb_status(capi_return_t x) {
  return x;
}
#endif

/**
 * Extract a status code from a return value.
 *
 * This function is as a pure "C" equivalent for `tiledb_status`, which is an
 * inline C++ function, not visible in the "C" context.
 *
 * @param x A value returned from a CAPI call
 * @return The status code within that value
 */
TILEDB_EXPORT capi_status_t tiledb_status_code(capi_return_t x);

/**
 * @name Status codes
 */
/**@{*/
/**
 * Success.
 */
#define TILEDB_OK (0)
/**
 * An error state, not otherwise specified.
 */
#define TILEDB_ERR (-1)
/**
 * Out of memory. The implementation threw `std::bad_alloc` somewhere.
 */
#define TILEDB_OOM (-2)
/**
 * Invalid context would prevent errors from being reported correctly.
 */
#define TILEDB_INVALID_CONTEXT (-3)
/**
 * Invalid error argument would prevent errors from being reported correctly.
 */
#define TILEDB_INVALID_ERROR (-4)
/**
 * Invalid error argument would prevent errors from being reported correctly.
 */
#define TILEDB_BUDGET_UNAVAILABLE (-5)
/**@}*/

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // TILEDB_CAPI_EXTERNAL_COMMON_H
