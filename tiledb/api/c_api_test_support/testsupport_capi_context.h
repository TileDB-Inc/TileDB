/**
 * @file tiledb/api/c_api_support/context/testsupport_capi_context.h
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
 */

#ifndef TILEDB_TESTSUPPORT_CAPI_CONTEXT_H
#define TILEDB_TESTSUPPORT_CAPI_CONTEXT_H

#include "tiledb/api/c_api/context/context_api_external.h"
#include "tiledb/api/c_api/context/context_api_internal.h"

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
#endif  // TILEDB_TESTSUPPORT_CAPI_CONTEXT_H
