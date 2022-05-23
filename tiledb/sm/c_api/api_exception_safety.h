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
#include "api_argument_validator.h"
#include "tiledb.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/status.h"

struct CAPIEntryPointBase {
  /**
   * Action on bad_alloc, without context.
   */
  static void action(const std::bad_alloc& e) {
    auto cst = tiledb::common::Status_Error(
        std::string("Out of memory, caught std::bad_alloc; ") + e.what());
    (void)LOG_STATUS(cst);
  }

  /**
   * Action on bad_alloc, with context.
   */
  static void action(tiledb_ctx_t* ctx, const std::bad_alloc& e) {
    auto cst = tiledb::common::Status_Error(
        std::string("Out of memory, caught std::bad_alloc; ") + e.what());
    (void)LOG_STATUS(cst);
    save_error(ctx, cst);
  }

  /**
   * Action on standard library exception, without context
   */
  static void action(const std::exception& e) {
    auto cst = tiledb::common::Status_Error(
        std::string("Internal TileDB uncaught exception; ") + e.what());
    (void)LOG_STATUS(cst);
  }

  /**
   * Action on standard library exception, with context
   */
  static void action(tiledb_ctx_t* ctx, const std::exception& e) {
    auto cst = tiledb::common::Status_Error(
        std::string("Internal TileDB uncaught exception; ") + e.what());
    (void)LOG_STATUS(cst);
    save_error(ctx, cst);
  }

  /**
   * Action on status exception, without context
   */
  static void action(const StatusException& e) {
    auto cst{e.extract_status()};
    (void)LOG_STATUS(cst);
  }

  /**
   * Action on status exception, with context
   */
  static void action(tiledb_ctx_t* ctx, const StatusException& e) {
    auto cst{e.extract_status()};
    (void)LOG_STATUS(cst);
    save_error(ctx, cst);
  }

  /**
   * Action on unknown exception, without context
   */
  static void action() {
    auto cst = tiledb::common::Status_Error(
        std::string("Internal TileDB uncaught unknown exception!"));
    (void)LOG_STATUS(cst);
  }

  /**
   * Action on unknown exception, with context
   */
  static void action(tiledb_ctx_t* ctx) {
    auto cst = tiledb::common::Status_Error(
        std::string("Internal TileDB uncaught unknown exception!"));
    (void)LOG_STATUS(cst);
    save_error(ctx, cst);
  }
};

template <auto f>
struct CAPIEntryPoint;

/**
 * Specialization of `CAPIEntryPoint` for ??? return type
 */
template <class... Args, class R, R (*f)(Args...)>
struct CAPIEntryPoint<f> : CAPIEntryPointBase {
  static R function(Args... args) {
    try {
      return f(args...);
    } catch (const std::bad_alloc& e) {
      action(e);
      return TILEDB_OOM;
    } catch (const StatusException& e) {
      action(e);
      return TILEDB_ERR;
    } catch (const std::exception& e) {
      action(e);
      return TILEDB_ERR;
    } catch (...) {
      action();
      return TILEDB_ERR;
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
    } catch (const StatusException& e) {
      action(e);
    } catch (const std::exception& e) {
      action(e);
    } catch (...) {
      action();
    }
  }
};

/**
 * Argument specialization of `CAPIEntryPoint` for standard API call.
 *
 * A standard API call has the standard return type `capi_return_t` and a
 * context as its first argument. This function checks the validity of the
 * context so that the wrapped function does not have to.
 */
template <class... Args, capi_return_t (*f)(tiledb_ctx_t*, Args...)>
struct CAPIEntryPoint<f> : CAPIEntryPointBase {
  static capi_return_t function(tiledb_ctx_t* ctx, Args... args) {
    /*
     * Validate context, first pass.
     * Note that the actions here are the generic ones without a context.
     */
    try {
      tiledb::api::ensure_context_is_valid_enough_for_errors(ctx);
    } catch (const std::bad_alloc& e) {
      action(e);
      return TILEDB_OOM;
    } catch (const StatusException& e) {
      action(e);
      return TILEDB_INVALID_CONTEXT;
    } catch (const std::exception& e) {
      action(e);
      return TILEDB_INVALID_CONTEXT;
    } catch (...) {
      action();
      return TILEDB_INVALID_CONTEXT;
    }
    /*
     * Validate context, second pass. Execute wrapped function.
     * Note that the actions here all have context arguments.
     */
    try {
      tiledb::api::ensure_context_is_fully_valid(ctx);
      return f(ctx, args...);
    } catch (const std::bad_alloc& e) {
      action(ctx, e);
      return TILEDB_OOM;
    } catch (const StatusException& e) {
      action(ctx, e);
      return TILEDB_ERR;
    } catch (const std::exception& e) {
      action(ctx, e);
      return TILEDB_ERR;
    } catch (...) {
      action(ctx);
      return TILEDB_ERR;
    }
  }
};

template <auto f>
constexpr auto api_entry = CAPIEntryPoint<f>::function;

/**
 * A version of `CAPIEntryPoint` for `void` return functions.
 *
 * Certain compilers (GCC 11.2 on certain platforms) were failing on CI using
 * the above specializations, claiming an ambiguity between the generic-return
 * and void-return ones. In order to move forward, we use a different template
 * name to avoid the ambiguity.
 */
template <auto f>
struct CAPIEntryPointVoid;

/**
 * Specialization of `CAPIEntryPoint` for void return type
 */
template <class... Args, void (*f)(Args...)>
struct CAPIEntryPointVoid<f> : CAPIEntryPointBase {
  static void function(Args... args) {
    try {
      f(args...);
    } catch (const std::bad_alloc& e) {
      action(e);
    } catch (const StatusException& e) {
      action(e);
    } catch (const std::exception& e) {
      action(e);
    } catch (...) {
      action();
    }
  }
};

template <auto f>
constexpr auto api_entry_void = CAPIEntryPointVoid<f>::function;

#endif  // TILEDB_API_EXCEPTION_SAFETY_H
