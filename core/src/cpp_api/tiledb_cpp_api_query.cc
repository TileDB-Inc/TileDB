/**
 * @file   tiledb_cpp_api_query.cc
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
 * This file defines the C++ API for the TileDB Query object.
 */

#include "tiledb_cpp_api_query.h"

tdb::Query &tdb::Query::set_layout(tiledb_layout_t layout) {
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_query_set_layout(ctx.ptr(), _query.get(), layout));
  return *this;
}

tdb::Query::Status tdb::Query::submit() {
  auto &ctx = _ctx.get();
  _prepare_submission();
  ctx.handle_error(tiledb_query_submit(ctx.ptr(), _query.get()));
  return query_status();
}

void tdb::Query::_prepare_submission() {
  _all_buff.clear();
  _buff_sizes.clear();
  _attr_names.clear();
  _sub_tsize.clear();

  uint64_t bufsize;
  size_t tsize;
  void *ptr;

  for (const auto &a : _attrs) {
    if (_attr_buffs.count(a) == 0) {
      throw std::runtime_error("No buffer for attribute " + a);
    }
    if (_var_offsets.count(a)) {
      std::tie(bufsize, tsize, ptr) = _var_offsets[a];
      _all_buff.push_back(ptr);
      _buff_sizes.push_back(bufsize * tsize);
      _sub_tsize.push_back(tsize);
    }
    std::tie(bufsize, tsize, ptr) = _attr_buffs[a];
    _all_buff.push_back(ptr);
    _buff_sizes.push_back(bufsize * tsize);
    _attr_names.push_back(a.c_str());
    _sub_tsize.push_back(tsize);
  }

  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_query_set_buffers(
      ctx.ptr(),
      _query.get(),
      _attr_names.data(),
      _attr_names.size(),
      _all_buff.data(),
      _buff_sizes.data()));
}

tdb::Query::Status tdb::Query::query_status() {
  tiledb_query_status_t status;
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_query_get_status(ctx.ptr(), _query.get(), &status));
  return tiledb_to_status(status);
}

tdb::Query::Status tdb::Query::attribute_status(const std::string &attr) {
  tiledb_query_status_t status;
  auto &ctx = _ctx.get();
  ctx.handle_error(tiledb_query_get_attribute_status(
      ctx.ptr(), _query.get(), attr.c_str(), &status));
  return tiledb_to_status(status);
}

tdb::Query::Status tdb::Query::tiledb_to_status(
    const tiledb_query_status_t &status) {
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

tdb::Query::Status tdb::Query::submit_async(
    void (*callback)(void *), void *data) {
  auto &ctx = _ctx.get();
  _prepare_submission();
  ctx.handle_error(
      tiledb_query_submit_async(ctx.ptr(), _query.get(), callback, data));
  return query_status();
}

tdb::Query::Status tdb::Query::submit_async() {
  submit_async(nullptr, nullptr);
  return query_status();
}

std::vector<uint64_t> tdb::Query::returned_buff_sizes() {
  std::vector<uint64_t> buffsize(_buff_sizes.size());
  for (size_t i = 0; i < _buff_sizes.size(); ++i) {
    buffsize[i] = _buff_sizes[i] / _sub_tsize[i];
  }
  return buffsize;
}

tdb::Query::Query(
    tdb::Context &ctx, const std::string &array, tiledb_query_type_t type)
    : _ctx(ctx)
    , _schema(ctx, array)
    , _deleter(_ctx) {
  tiledb_query_t *q;
  ctx.handle_error(tiledb_query_create(ctx.ptr(), &q, array.c_str(), type));
  _query = std::shared_ptr<tiledb_query_t>(q, _deleter);
  _array_attributes = _schema.attributes();
}

std::ostream &tdb::operator<<(
    std::ostream &os, const tdb::Query::Status &stat) {
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

std::string tdb::Query::to_str(tiledb_query_type_t type) {
  switch (type) {
    case TILEDB_READ:
      return "READ";
    case TILEDB_WRITE:
      return "WRITE";
  }
  return "";  // silence error
}
