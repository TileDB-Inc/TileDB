/**
 * @file tiledb/api/c_api_test_support/testsupport_capi_array.h
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
 * This file defines test support for the array section of the C API.
 */

#ifndef TILEDB_TESTSUPPORT_CAPI_ARRAY_H
#define TILEDB_TESTSUPPORT_CAPI_ARRAY_H

#include "test/support/src/temporary_local_directory.h"
#include "testsupport_capi_context.h"
#include "tiledb/api/c_api/array/array_api_external.h"
#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/api/c_api/array_schema/array_schema_api_internal.h"
#include "tiledb/api/c_api/attribute/attribute_api_internal.h"
#include "tiledb/api/c_api/dimension/dimension_api_internal.h"
#include "tiledb/api/c_api/domain/domain_api_internal.h"

namespace tiledb::api::test_support {

class ordinary_array_exception : public StatusException {
 public:
  explicit ordinary_array_exception(const std::string& message = "")
      : StatusException("error creating test array; ", message) {
  }
};

/**
 * Base class for an ordinary array.
 *
 * Note that this base class does not create a schema object.
 * As such, the underlying array object should not be opened or closed.
 */
class ordinary_array_without_schema {
 private:
  const char* array_uri = "unit_capi_array";

 public:
  ordinary_context context{};
  tiledb_array_handle_t* array{nullptr};

  ordinary_array_without_schema() {
    auto rc = tiledb_array_alloc(context.context, array_uri, &array);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error allocating test array");
    }
    if (array == nullptr) {
      throw std::logic_error(
          "tiledb_array_alloc returned OK but without array");
    }
  }

  ~ordinary_array_without_schema() {
    tiledb_array_free(&array);
  }

  [[nodiscard]] tiledb_ctx_handle_t* ctx() const {
    return context.context;
  }

  [[nodiscard]] const char* uri() const {
    return array_uri;
  }
};

/**
 * An ordinary array with a fully-allocated array schema object.
 */
struct ordinary_array : public ordinary_array_without_schema {
 private:
  tiledb_dimension_t* dim;
  tiledb_domain_t* domain;
  tiledb_attribute_t* attr;
  tiledb_array_schema_t* schema;
  TemporaryLocalDirectory temp_dir{"unit_capi_array"};

 public:
  ordinary_array(bool is_var = false) {
    int rc;
    // Create domain
    if (is_var) {
      rc =
          tiledb_dimension_alloc(ctx(), "dim", TILEDB_STRING_ASCII, 0, 0, &dim);
    } else {
      int dim_domain[] = {1, 4};
      int tile_extents[] = {4};
      rc = tiledb_dimension_alloc(
          ctx(), "dim", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &dim);
    }
    if (rc != TILEDB_OK) {
      throw ordinary_array_exception("tiledb_dimension_alloc failed");
    }
    rc = tiledb_domain_alloc(ctx(), &domain);
    if (rc != TILEDB_OK) {
      throw ordinary_array_exception("tiledb_domain_alloc failed");
    }
    rc = tiledb_domain_add_dimension(ctx(), domain, dim);
    if (rc != TILEDB_OK) {
      throw ordinary_array_exception("tiledb_domain_add_dimension failed");
    }

    // Create attribute
    rc = tiledb_attribute_alloc(ctx(), "attr", TILEDB_INT32, &attr);
    if (rc != TILEDB_OK) {
      throw ordinary_array_exception("tiledb_attribute_alloc failed");
    }

    // Create array schema
    rc = tiledb_array_schema_alloc(ctx(), TILEDB_SPARSE, &schema);
    if (rc != TILEDB_OK) {
      throw ordinary_array_exception("tiledb_array_schema_alloc failed");
    }
    rc = tiledb_array_schema_set_cell_order(ctx(), schema, TILEDB_ROW_MAJOR);
    if (rc != TILEDB_OK) {
      throw ordinary_array_exception(
          "tiledb_array_schema_set_cell_order failed");
    }
    rc = tiledb_array_schema_set_tile_order(ctx(), schema, TILEDB_ROW_MAJOR);
    if (rc != TILEDB_OK) {
      throw ordinary_array_exception(
          "tiledb_array_schema_set_tile_order failed");
    }
    rc = tiledb_array_schema_set_domain(ctx(), schema, domain);
    if (rc != TILEDB_OK) {
      throw ordinary_array_exception("tiledb_array_schema_set_domain failed");
    }
    rc = tiledb_array_schema_add_attribute(ctx(), schema, attr);
    if (rc != TILEDB_OK) {
      throw ordinary_array_exception(
          "tiledb_array_schema_add_attribute failed");
    }

    // Create array
    rc = tiledb_array_create(ctx(), uri(), schema);
    if (rc != TILEDB_OK) {
      throw ordinary_array_exception();
    }

    // Allocate test array
    rc = tiledb_array_alloc(ctx(), uri(), &array);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error allocating test array");
    }
    if (array == nullptr) {
      throw std::logic_error(
          "tiledb_array_alloc returned OK but without array");
    }
  }

  ~ordinary_array() {
    // Close and delete the array
    tiledb_array_close(context.context, array);
    tiledb_array_delete(context.context, uri());

    // Free allocated objects (note context will be freed by its own destructor)
    tiledb_dimension_free(&dim);
    tiledb_domain_free(&domain);
    tiledb_attribute_free(&attr);
    tiledb_array_schema_free(&schema);
    tiledb_array_free(&array);
  }

  void open(tiledb_query_type_t query_type = TILEDB_READ) {
    auto rc = tiledb_array_open(context.context, array, query_type);
    if (rc != TILEDB_OK) {
      throw std::logic_error("error opening test array");
    }
  }

  void close() {
    auto rc = tiledb_array_close(context.context, array);
    if (rc != TILEDB_OK) {
      throw std::logic_error("error closing test array");
    }
  }

  [[nodiscard]] const char* uri() {
    return temp_dir.path().c_str();
  }
};

}  // namespace tiledb::api::test_support

#endif  // TILEDB_TESTSUPPORT_CAPI_ARRAY_H
