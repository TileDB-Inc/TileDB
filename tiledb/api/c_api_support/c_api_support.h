/**
 * @file tiledb/api/c_api_support/c_api_support.h
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
 * This file includes all the support functions that appear generally in C API
 * implementation functions:
 *   - Exception wrappers and error handling
 *   - Argument validation
 */

#ifndef TILEDB_CAPI_SUPPORT_H
#define TILEDB_CAPI_SUPPORT_H

#include "argument_validation.h"
#include "tiledb/api/c_api_support/cpp_string/cpp_string.h"
#include "tiledb/api/c_api_support/exception_wrapper/capi_definition.h"
#include "tiledb/api/c_api_support/exception_wrapper/exception_wrapper.h"
#include "tiledb/api/c_api_support/tracing_wrapper.h"
#if __has_include("capi_function_override.h")
#include "capi_function_override.h"
#endif

#endif  // TILEDB_CAPI_SUPPORT_H
