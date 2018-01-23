/**
 * @file  tiledb_cpp_api_map_iter.cc
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
 * This file declares the C++ API for the TileDB Map Iter object.
 */

#include "tiledb_cpp_api_map_item.h"
#include "tiledb_cpp_api_map_iter.h"
#include "tiledb_cpp_api_map.h"

namespace tdb {
  namespace impl {

    MapIter::MapIter(Map &map, bool end)
    : map_(&map), deleter_(map.context()), done_((int) end) {
      auto &ctx = map.context();
      MapSchema schema(ctx, map.uri());
      std::vector<const char*> names;
      auto attrs = schema.attributes();
      for (const auto &a : attrs) {
        names.push_back(a.first.c_str());
      }
      tiledb_kv_iter_t *p;
      ctx.handle_error(tiledb_kv_iter_create(ctx, &p, map.uri().c_str(),
                                             names.data(), (unsigned) attrs.size()));
      iter_ = std::shared_ptr<tiledb_kv_iter_t>(p, deleter_);
      this->operator++();
    }

    MapIter::~MapIter() {
      map_->flush();
    }

    MapIter &MapIter::operator++() {
      auto &ctx = map_->context();
      if (done_) return *this;
      ctx.handle_error(tiledb_kv_iter_done(ctx, iter_.get(), &done_));
      if (done_) return *this;
      tiledb_kv_item_t *p;
      ctx.handle_error(tiledb_kv_iter_here(ctx, iter_.get(), &p));
      item_ = std::unique_ptr<MapItem>(new MapItem(ctx, &p, map_));
      ctx.handle_error(tiledb_kv_iter_next(ctx, iter_.get()));
      return *this;
    }

  }
}