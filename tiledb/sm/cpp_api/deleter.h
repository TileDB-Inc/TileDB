/**
 * @file   tiledb_cpp_api_deleter.h
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

namespace tiledb {

namespace impl {

/**
 * Deleter for various TileDB C types. Useful for `shared_ptr`.
 *
 * **Example:**
 *
 * @code{.cpp}
 * Context ctx;
 * Deleter deleter(ctx);
 * auto p = std::shared_ptr<tiledb_type_t>(ptr, deleter);
 * @endcode
 */
class TILEDB_EXPORT Deleter {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  explicit Deleter(const Context& ctx)
      : ctx_(ctx) {
  }
  Deleter(const Deleter&) = default;

  /* ********************************* */
  /*              DELETERS             */
  /* ********************************* */

  void operator()(tiledb_vfs_fh_t* p) const;
  void operator()(tiledb_query_t* p) const;
  void operator()(tiledb_array_schema_t* p) const;
  void operator()(tiledb_kv_t* p) const;
  void operator()(tiledb_kv_schema_t* p) const;
  void operator()(tiledb_kv_item_t* p) const;
  void operator()(tiledb_kv_iter_t* p) const;
  void operator()(tiledb_attribute_t* p) const;
  void operator()(tiledb_dimension_t* p) const;
  void operator()(tiledb_domain_t* p) const;
  void operator()(tiledb_vfs_t* p) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;
};

}  // namespace impl

}  // namespace tiledb

#endif  // TILEDB_CPP_API_DELETER_H
