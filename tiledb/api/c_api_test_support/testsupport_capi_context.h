/**
 * @file tiledb/api/c_api_test_support/context/testsupport_capi_context.h
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
 * This file defines test support classes for the context section of the C API.
 *
 * The "ordinary" classes are RAII wrappers around simple array schema objects.
 * These classes are not supposed to become general-purpose wrappers, but rather
 * remain as simple objects to be used when full variation is not needed. The
 * current implementation is something of a placeholder until more general
 * mechanisms for specifying complete schema objects is available.
 */

#ifndef TILEDB_TESTSUPPORT_CAPI_CONTEXT_H
#define TILEDB_TESTSUPPORT_CAPI_CONTEXT_H

#include "tiledb/api/c_api/context/context_api_external.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/dimension/dimension_api_external.h"
#include "tiledb/api/c_api/dimension/dimension_api_internal.h"

namespace tiledb::api::test_support {

struct ordinary_context {
  tiledb_ctx_handle_t* context{nullptr};
  ordinary_context() {
    auto rc{tiledb_ctx_alloc(nullptr, &context)};
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error creating test context");
    }
    if (context == nullptr) {
      throw std::logic_error(
          "tiledb_ctx_alloc returned OK but without context");
    }
  }
  ~ordinary_context() {
    tiledb_ctx_free(&context);
  }
};

}  // namespace tiledb::api::test_support

namespace tiledb::api::test_support {

/**
 * Ordinary dimension base class
 */
class ordinary_dimension {
 public:
  tiledb::api::test_support::ordinary_context ctx{};
  tiledb_dimension_t* dimension{};
  ordinary_dimension() = delete;

  /**
   * Initialization function for `dimension` data member. It's separate to
   * allow either an external context or a just-created internal one.
   */
  void allocate_dimension(tiledb_ctx_handle_t* context, std::string name) {
    capi_return_t rc{tiledb_dimension_alloc(
        context,
        name.data(),
        TILEDB_UINT32,
        constraint.data(),
        nullptr,
        &dimension)};
    if (tiledb_status(rc) != TILEDB_OK) {
      throw std::runtime_error("error creating test dimension");
    }
  }

 protected:
  const std::array<uint32_t, 2> constraint{0, 10};
  ordinary_dimension(tiledb_ctx_handle_t* ctx, std::string name)
      : ctx{} {
    allocate_dimension(ctx, name);
  }
  explicit ordinary_dimension(std::string name)
      : ctx{} {
    allocate_dimension(ctx.context, name);
  }
  ~ordinary_dimension() {
    tiledb_dimension_free(&dimension);
  }
};

struct ordinary_dimension_d1 : public ordinary_dimension {
  /**
   * Default constructor for standalone use.
   */
  ordinary_dimension_d1()
      : ordinary_dimension{"d1"} {
  }
  /**
   * Constructor with context for use inside a domain
   */
  explicit ordinary_dimension_d1(tiledb_ctx_handle_t* ctx)
      : ordinary_dimension{ctx, "d1"} {
  }
};

struct ordinary_dimension_d2 : public ordinary_dimension {
  /**
   * Default constructor for standalone use.
   */
  ordinary_dimension_d2()
      : ordinary_dimension{"d2"} {
  }
  /**
   * Constructor with context for use inside a domain
   */
  explicit ordinary_dimension_d2(tiledb_ctx_handle_t* ctx)
      : ordinary_dimension{ctx, "d2"} {
  }
};

}  // namespace tiledb::api::test_support
#endif  // TILEDB_TESTSUPPORT_CAPI_CONTEXT_H
