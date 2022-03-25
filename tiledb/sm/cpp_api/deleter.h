/**
 * @file   deleter.h
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file declares the C++ API for the TileDB Deleter object.
 */

#ifndef TILEDB_CPP_API_DELETER_H
#define TILEDB_CPP_API_DELETER_H

#include "context.h"
#include "tiledb.h"
#include "tiledb_experimental.h"

namespace tiledb {

namespace impl {

/**
 * Deleter for various TileDB C types. Useful for `shared_ptr`.
 *
 * **Example:**
 *
 * @code{.cpp}
 * Context ctx;
 * Deleter deleter;
 * auto p = std::shared_ptr<tiledb_type_t>(ptr, deleter);
 * @endcode
 */
class Deleter {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  Deleter() = default;

  /* ********************************* */
  /*              DELETERS             */
  /* ********************************* */

  void operator()(tiledb_vfs_fh_t* p) const {
    tiledb_vfs_fh_free(&p);
  }

  void operator()(tiledb_array_t* p) const {
    tiledb_array_free(&p);
  }

  void operator()(tiledb_subarray_t* p) const {
    tiledb_subarray_free(&p);
  }

  void operator()(tiledb_query_t* p) const {
    tiledb_query_free(&p);
  }

  void operator()(tiledb_query_condition_t* p) const {
    tiledb_query_condition_free(&p);
  }

  void operator()(tiledb_array_schema_t* p) const {
    tiledb_array_schema_free(&p);
  }

  void operator()(tiledb_array_schema_evolution_t* p) const {
    tiledb_array_schema_evolution_free(&p);
  }

  void operator()(tiledb_attribute_t* p) const {
    tiledb_attribute_free(&p);
  }

  void operator()(tiledb_dimension_t* p) const {
    tiledb_dimension_free(&p);
  }

  void operator()(tiledb_domain_t* p) const {
    tiledb_domain_free(&p);
  }

  void operator()(tiledb_vfs_t* p) const {
    tiledb_vfs_free(&p);
  }

  void operator()(tiledb_filter_t* p) const {
    tiledb_filter_free(&p);
  }

  void operator()(tiledb_filter_list_t* p) const {
    tiledb_filter_list_free(&p);
  }

  void operator()(tiledb_fragment_info_t* p) const {
    tiledb_fragment_info_free(&p);
  }

  void operator()(tiledb_error_t* p) const {
    tiledb_error_free(&p);
  }

  void operator()(tiledb_group_t* p) const {
    tiledb_group_free(&p);
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */
};

}  // namespace impl

}  // namespace tiledb

#endif  // TILEDB_CPP_API_DELETER_H
