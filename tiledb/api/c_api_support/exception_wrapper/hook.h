/**
 * @file tiledb/api/c_api_support/exception_wrapper/hook.h
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
 * This file defines macros to define C API functions.
 */

#ifndef TILEDB_EXCEPTION_WRAPPER_HOOK_H
#define TILEDB_EXCEPTION_WRAPPER_HOOK_H


/*
 * The macro wrappers all have the same form
 * ```
 *   CAPI_XXX_BEGIN(name, ...)
 *   CAPI_XXX_END(...)
 * ```
 * - The name argument is for the base name, that is, the name without the
 *   `tiledb_` prefix.
 * - The `__VA_ARGS__` argument in `*_BEGIN` is the full argument part of the
 *   function signature. By "full", that means it requires that parameter names
 *   be present.
 * - The `__VA_ARGS__` argument in `*_END` are the call arguments, that is, it's
 *   the function signature arguments without their type declarations.
 */

/// clang-format off

#define CAPI_DEFN(name) tiledb_##name
#define CAPI_IMPL(name) tiledb::api::tiledb_##name
#define CAPI_XFMR(name) tiledb::api::api_entry_##name

#define CAPI_BEGIN(root, ret) ret CAPI_DEFN(root)
#define CAPI_MIDDLE(root, xfmr) \
  noexcept {                    \
    return CAPI_XFMR(xfmr)<CAPI_IMPL(root)>
#define CAPI_END(...) \
  (__VA_ARGS__);      \
  }

#define CAPI_PLAIN_BEGIN(root, ...)             \
  CAPI_BEGIN(root, capi_return_t )(__VA_ARGS__) \
  CAPI_MIDDLE(root, plain)
#define CAPI_PLAIN_END(...) \
  CAPI_END(__VA_ARGS__)

#define CAPI_VOID_BEGIN(root, ...)     \
  CAPI_BEGIN(root, void )(__VA_ARGS__) \
  CAPI_MIDDLE(root, void)
#define CAPI_VOID_END(...) \
  CAPI_END(__VA_ARGS__)

#define CAPI_WITH_CONTEXT_BEGIN(root, ...)     \
  CAPI_BEGIN(root, capi_return_t)(__VA_ARGS__) \
  CAPI_MIDDLE(root, with_context)
#define CAPI_WITH_CONTEXT_END(...) \
  CAPI_END(__VA_ARGS__)

#define CAPI_CONTEXT_BEGIN(root, ...)          \
  CAPI_BEGIN(root, capi_return_t)(__VA_ARGS__) \
  CAPI_MIDDLE(root, context)
#define CAPI_CONTEXT_END(...) \
  CAPI_END(__VA_ARGS__)

/*
 * The argument lists for the ERROR macros omit the `error` argument.
 *
 * The ERROR wrapper is different from the others because it makes a special
 * case of the `error` argument. Its definition is at the end of the argument
 * list of the signature but at the beginning of the argument list of the call.
 *
 * Note that these macros, as written, do not handle zero-length variable
 * arguments. There are no cases where this is needed.
 */
#define CAPI_ERROR_BEGIN(root, ...)                                     \
  CAPI_BEGIN(root, capi_return_t)(__VA_ARGS__, tiledb_error_t * *error) \
  CAPI_MIDDLE(root, error)
#define CAPI_ERROR_END(...) \
  CAPI_END(error, __VA_ARGS__)
/*
 * Special case where the API name and wrapped function are not identical.
 */
#define CAPI_ERROR_BEGIN_X(root, root2, ...)                                     \
  CAPI_BEGIN(root, capi_return_t)(__VA_ARGS__, tiledb_error_t * *error) \
  CAPI_MIDDLE(root2, error)

/// clang-format on

#endif  // TILEDB_EXCEPTION_WRAPPER_HOOK_H
