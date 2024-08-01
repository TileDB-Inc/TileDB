/**
 * @file tiledb/api/c_api_test_support/testsupport_capi_array_schema.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file defines test support for the array schema section of the C API.
 */

#ifndef TILEDB_TESTSUPPORT_CAPI_ARRAY_SCHEMA_H
#define TILEDB_TESTSUPPORT_CAPI_ARRAY_SCHEMA_H

#include "tiledb/api/c_api/array_schema/array_schema_api_external.h"
#include "tiledb/api/c_api/array_schema/array_schema_api_internal.h"

namespace tiledb::api::test_support {

/**
 * Ordinary array schema base class
 */
class ordinary_array_schema {
 public:
  tiledb_ctx_handle_t* ctx{nullptr};
  tiledb_array_schema_handle_t* schema{nullptr};

  ordinary_array_schema(tiledb_array_type_t array_type = TILEDB_SPARSE) {
    auto rc = tiledb_ctx_alloc(nullptr, &ctx);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error creating test context");
    }
    rc = tiledb_array_schema_alloc(ctx, array_type, &schema);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error creating test array_schema");
    }
    if (schema == nullptr) {
      throw std::logic_error(
          "tiledb_array_schema_alloc returned OK but without array_schema");
    }
  }

  ~ordinary_array_schema() {
    tiledb_array_schema_free(&schema);
    tiledb_ctx_free(&ctx);
  }
};

struct ordinary_array_schema_with_attr : public ordinary_array_schema {
  tiledb_attribute_t* attr{nullptr};

  ordinary_array_schema_with_attr(
      tiledb_array_type_t array_type = TILEDB_SPARSE)
      : ordinary_array_schema(array_type) {
    auto rc = tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &attr);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error creating test attribute");
    }
    rc = tiledb_array_schema_add_attribute(ctx, schema, attr);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error adding test attribute to test schema");
    }
  }

  ~ordinary_array_schema_with_attr() {
    tiledb_attribute_free(&attr);
  }
};

}  // namespace tiledb::api::test_support

#endif  // TILEDB_TESTSUPPORT_CAPI_ARRAY_SCHEMA_H
