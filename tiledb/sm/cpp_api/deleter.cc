/**
 * @file   tiledb_cpp_api_deleter.cc
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
 * This file defines the C++ API for the TileDB Deleter object.
 */

#include "deleter.h"

namespace tiledb {

namespace impl {

void Deleter::operator()(tiledb_vfs_fh_t* p) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_fh_free(ctx, &p));
}

void Deleter::operator()(tiledb_query_t* p) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_query_free(ctx, &p));
}

void Deleter::operator()(tiledb_kv_t* p) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_kv_close(ctx, &p));
}

void Deleter::operator()(tiledb_array_schema_t* p) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_array_schema_free(ctx, &p));
}

void Deleter::operator()(tiledb_kv_schema_t* p) const {
  ctx_.get().handle_error(tiledb_kv_schema_free(ctx_.get(), &p));
}

void Deleter::operator()(tiledb_kv_item_t* p) const {
  ctx_.get().handle_error(tiledb_kv_item_free(ctx_.get(), &p));
}

void Deleter::operator()(tiledb_kv_iter_t* p) const {
  ctx_.get().handle_error(tiledb_kv_iter_free(ctx_.get(), &p));
}

void Deleter::operator()(tiledb_attribute_t* p) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_attribute_free(ctx, &p));
}

void Deleter::operator()(tiledb_dimension_t* p) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_dimension_free(ctx, &p));
}

void Deleter::operator()(tiledb_domain_t* p) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_domain_free(ctx, &p));
}

void Deleter::operator()(tiledb_vfs_t* p) const {
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_free(ctx, &p));
}

}  // namespace impl

}  // namespace tiledb
