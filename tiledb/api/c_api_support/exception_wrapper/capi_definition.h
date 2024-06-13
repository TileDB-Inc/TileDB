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

#ifndef TILEDB_CAPI_DEFINITION_H
#define TILEDB_CAPI_DEFINITION_H

/* clang-format off */

/*
 * Hook for additional code to be generated before each C API interface
 * function.
 */
#define CAPI_PREFIX(name)

/*
 * Declaration clause for a C API interface function. Follow with a `{ ... }`
 * block defining the body.
 */
#define CAPI_INTERFACE(root, ...) \
CAPI_PREFIX(root)                 \
capi_return_t tiledb_##root(__VA_ARGS__) noexcept

/*
 * A variant of CAPI_INTERFACE for the handful of `void` returns.
 */
#define CAPI_INTERFACE_VOID(root, ...) \
CAPI_PREFIX(root)                      \
void tiledb_##root(__VA_ARGS__) noexcept

/*
 * A variant of CAPI_INTERFACE for the handful of functions without arguments.
 */
#define CAPI_INTERFACE_NULL(root) \
CAPI_PREFIX(root)                 \
capi_return_t tiledb_##root(void) noexcept

/* clang-format on */

#endif  // TILEDB_CAPI_DEFINITION_H
