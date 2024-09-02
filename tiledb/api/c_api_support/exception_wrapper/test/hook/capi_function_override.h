/**
 * @file
 * tiledb/api/c_api_support/exception_wrapper/test/hook/capi_function_override.h
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

#ifndef TILEDB_EXCEPTION_WRAPPER_TEST_CAPI_FUNCTION_OVERRIDE_H
#define TILEDB_EXCEPTION_WRAPPER_TEST_CAPI_FUNCTION_OVERRIDE_H

#include "../logging_aspect.h"

/*
 * CAPI_PREFIX is an extension point within the macros for defining C API
 * interface functions. The override macro defines a trait class for each C API
 * implementation function. The trait class is a specialization of an otherwise
 * undefined class template. Each trait class contains the name of its
 * implementation function; the name is subsequently picked up by `class
 * LoggingAspect`.
 */
#undef CAPI_PREFIX
#define CAPI_PREFIX(root)                                    \
  template <>                                                \
  struct CAPIFunctionNameTrait<tiledb::api::tiledb_##root> { \
    static constexpr std::string_view name{"tiledb_" #root}; \
  };

/**
 * Specialization of the aspect selector to `void` overrides the default (the
 * null aspect) to compile with the logging aspect instead.
 */
template <auto f>
struct tiledb::api::CAPIFunctionSelector<f, void> {
  using aspect_type = LoggingAspect<f>;
};

#endif  // TILEDB_EXCEPTION_WRAPPER_TEST_CAPI_FUNCTION_OVERRIDE_H
