/**
 * @file   tiledb_cpp_api_deleter.cc
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file defines the C++ API for the TileDB Deleter object.
 */

#include "tiledb_cpp_api_deleter.h"

namespace tdb {

namespace impl {

void Deleter::operator()(tiledb_query_t *p) {
  auto &ctx = ctx_.get();
  ctx.handle_error(tiledb_query_free(ctx.ptr(), p));
}

void Deleter::operator()(tiledb_array_schema_t *p) {
  auto &ctx = ctx_.get();
  ctx.handle_error(tiledb_array_schema_free(ctx.ptr(), p));
}

void Deleter::operator()(tiledb_attribute_t *p) {
  auto &ctx = ctx_.get();
  ctx.handle_error(tiledb_attribute_free(ctx.ptr(), p));
}

void Deleter::operator()(tiledb_dimension_t *p) {
  auto &ctx = ctx_.get();
  ctx.handle_error(tiledb_dimension_free(ctx.ptr(), p));
}

void Deleter::operator()(tiledb_domain_t *p) {
  auto &ctx = ctx_.get();
  ctx.handle_error(tiledb_domain_free(ctx.ptr(), p));
}

void Deleter::operator()(tiledb_vfs_t *p) {
  auto &ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_free(ctx.ptr(), p));
}

}  // namespace impl

}  // namespace tdb
