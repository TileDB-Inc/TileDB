/**
 * @file tiledb/sm/c_api/api_exception_safety.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2022 TileDB, Inc.
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
 * This file defines templates that wrap an API function with exception
 * handlers.
 */

#ifndef TILEDB_API_EXCEPTION_SAFETY_H
#define TILEDB_API_EXCEPTION_SAFETY_H

#include <stdexcept>
#include "tiledb.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/status.h"

struct CAPIEntryPointBase {
  static void action(const std::bad_alloc& e) {
    auto cst = tiledb::common::Status_Error(
        std::string("Internal TileDB uncaught std::bad_alloc exception; ") +
        e.what());
    LOG_STATUS(cst);
  }

  static void action(const std::exception& e) {
    auto cst = tiledb::common::Status_Error(
        std::string("Internal TileDB uncaught exception; ") + e.what());
    LOG_STATUS(cst);
  }
};

template <auto f>
struct CAPIEntryPoint;

/**
 * Specialization of `CAPIEntryPoint` for ??? return type
 */
template <class R, class... Args, R (*f)(Args...)>
struct CAPIEntryPoint<f> : CAPIEntryPointBase {
  static R function(Args... args) {
    try {
      return f(args...);
    } catch (const std::bad_alloc& e) {
      action(e);
      return TILEDB_OOM;
    } catch (const std::exception& e) {
      action(e);
      return TILEDB_ERR;
    }
  }
};

/**
 * Specialization of `CAPIEntryPoint` for return type `bool`
 */
template <class... Args, bool (*f)(Args...)>
struct CAPIEntryPoint<f> : CAPIEntryPointBase {
  static bool function(Args... args) {
    try {
      return f(args...);
    } catch (const std::bad_alloc& e) {
      action(e);
      return false;
    } catch (const std::exception& e) {
      action(e);
      return false;
    }
  }
};

/**
 * Specialization of `CAPIEntryPoint` for void return type
 */
template <class... Args, void (*f)(Args...)>
struct CAPIEntryPoint<f> : CAPIEntryPointBase {
  static void function(Args... args) {
    try {
      f(args...);
    } catch (const std::bad_alloc& e) {
      action(e);
    } catch (const std::exception& e) {
      action(e);
    }
  }
};

template <auto f>
constexpr auto api_entry = CAPIEntryPoint<f>::function;

#endif  // TILEDB_API_EXCEPTION_SAFETY_H
