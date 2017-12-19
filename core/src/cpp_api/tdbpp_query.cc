/**
 * @file   tdbpp_query.cc
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
 * This file declares the C++ API for TileDB.
 */

#include "tdbpp_query.h"

tdb::Query::~Query() {
  if (_query != nullptr) {
    _ctx.get().handle_error(tiledb_query_free(_ctx.get(), _query));
  }
}

tdb::Query::Query(tdb::Context &ctx, tdb::ArrayMetadata &meta, tiledb_query_type_t type) : _ctx(ctx), _array(meta) {
  ctx.handle_error(tiledb_query_create(ctx, &_query, meta.uri().c_str(), type));
}

tdb::Query::Query(tdb::Array &array, tiledb_query_type_t type) : Query(array.context(), array.meta(), type) {}

tdb::Query &tdb::Query::operator=(tdb::Query &&o) {
  _ctx = o._ctx;
  _array = o._array;
  _var_offsets = std::move(o._var_offsets);
  _attr_buffs = std::move(o._attr_buffs);
  _attr_names = std::move(o._attr_names);
  _all_buff = std::move(o._all_buff);
  _buff_sizes = std::move(o._buff_sizes);
  _query = o._query;
  o._query = nullptr;
  return *this;
}

template<typename T>
tdb::Query &tdb::Query::subarray(const std::vector<typename T::type> &pairs) {
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_query_by_subarray(ctx, _query, pairs.data(), T::tiledb_datatype));
  return *this;
}

tdb::Query &tdb::Query::layout(tiledb_layout_t layout) {
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_query_set_layout(ctx, _query, layout));
  return *this;
}

tdb::Query &tdb::Query::attributes(const std::vector<std::string> &attrs) {
  for (const auto &a : attrs) {
    if (_array.get().attributes().count(a) == 0) {
      throw std::runtime_error("Attribute does not exist in array: " + a);
    }
  }
  _attrs = attrs;
  return *this;
}

const std::vector<uint64_t> &tdb::Query::submit() {
  _prepare_buffers();
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_query_set_buffers(ctx, _query, _attr_names.data(), _attr_names.size(), _all_buff.data(), _buff_sizes.data()));
  ctx.handle_error(tiledb_query_submit(ctx, _query));
  return _buff_sizes;
}

void tdb::Query::_prepare_buffers() {
  _all_buff.clear();
  _buff_sizes.clear();
  _attr_names.clear();
  if (_attrs.empty()) {
    for (const auto &a : _array.get().attributes()) {
      _attrs.push_back(a.first);
    }
  }

  for (const auto &a : _attrs) {
    if (_attr_buffs.count(a) == 0) {
      throw std::runtime_error("No buffer for attribute " + a);
    }
    if (_var_offsets.count(a)) {
      _all_buff.push_back(_var_offsets[a].second);
      _buff_sizes.push_back(_var_offsets[a].first);
    }
    _all_buff.push_back(_attr_buffs[a].second);
    _buff_sizes.push_back(_attr_buffs[a].first);
    _attr_names.push_back(a.data());
  }

  _all_buff.shrink_to_fit();
  _buff_sizes.shrink_to_fit();
  _attr_names.shrink_to_fit();
}
