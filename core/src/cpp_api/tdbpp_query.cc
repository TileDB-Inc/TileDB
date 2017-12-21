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

tdb::Query &tdb::Query::layout(tiledb_layout_t layout) {
  auto &ctx = _ctx.get();
  if (layout == TILEDB_UNORDERED && _array.get().type() == TILEDB_DENSE) {
    throw std::invalid_argument("Unordered layout invalid for dense arrays.");
  }
  ctx.handle_error(tiledb_query_set_layout(ctx, _query.get(), layout));
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

tdb::Query::Status tdb::Query::submit() {
  if (query_status() == Status::COMPLETE) throw std::runtime_error("Cannot submit a completed query.");
  _prepare_buffers();
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_query_set_buffers(ctx, _query.get(), _attr_names.data(), _attr_names.size(), _all_buff.data(), _buff_sizes.data()));
  ctx.handle_error(tiledb_query_submit(ctx, _query.get()));
  for (size_t i = 0; i < _attrs.size(); ++i) {
    _buff_sizes[i] = _buff_sizes[i] / _sub_tsize[i];
  }
  return query_status();
}

void tdb::Query::_Deleter::operator()(tiledb_query_t *p) {
  _ctx.get().handle_error(tiledb_query_free(_ctx.get(), p));
}

void tdb::Query::_prepare_buffers() {
  _all_buff.clear();
  _buff_sizes.clear();
  _attr_names.clear();
  _sub_tsize.clear();

  if (_attrs.empty()) {
    for (const auto &a : _array.get().attributes()) {
      _attrs.push_back(a.first);
    }
  }

  for (const auto &a : _attrs) {
    if (_attr_buffs.count(a) == 0) {
      throw std::runtime_error("No buffer for attribute " + a);
    }
    uint64_t bufsize;
    size_t tsize;
    void* ptr;
    if (_var_offsets.count(a)) {
      std::tie(bufsize, tsize, ptr) = _var_offsets[a];
      _all_buff.push_back(ptr);
      _buff_sizes.push_back(bufsize*tsize);
      _sub_tsize.push_back(tsize);
    }
    std::tie(bufsize, tsize, ptr) = _attr_buffs[a];
    _all_buff.push_back(ptr);
    _buff_sizes.push_back(bufsize*tsize);
    _attr_names.push_back(a.data());
    _sub_tsize.push_back(tsize);
  }

  _all_buff.shrink_to_fit();
  _buff_sizes.shrink_to_fit();
  _attr_names.shrink_to_fit();
}

tdb::Query::Status tdb::Query::query_status() {
  tiledb_query_status_t status;
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_query_get_status(ctx, _query.get(), &status));
  return tiledb_to_status(status);
}

tdb::Query::Status tdb::Query::attribute_status(const std::string &attr) {
  tiledb_query_status_t status;
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_query_get_attribute_status(ctx, _query.get(), attr.c_str(), &status));
  return tiledb_to_status(status);
}

tdb::Query::Status tdb::Query::tiledb_to_status(const tiledb_query_status_t &status) {
  switch (status) {
    case TILEDB_INCOMPLETE:
      return Status::INCOMPLETE;
    case TILEDB_COMPLETED:
      return Status::COMPLETE;
    case TILEDB_INPROGRESS:
      return Status::INPROGRESS;
    case TILEDB_FAILED:
      return Status::FAILED;
  }
  return Status::UNDEF;
}

std::ostream &operator<<(std::ostream &os, const tdb::Query::Status &stat) {
  switch (stat) {
    case tdb::Query::Status::INCOMPLETE:
      os << "INCOMPLETE";
      break;
    case tdb::Query::Status::INPROGRESS:
      os << "INPROGRESS";
      break;
    case tdb::Query::Status::FAILED:
      os << "FAILED";
      break;
    case tdb::Query::Status::COMPLETE:
      os << "COMPLETE";
      break;
    case tdb::Query::Status::UNDEF:
      os << "UNDEF";
      break;
  }
  return os;
}
