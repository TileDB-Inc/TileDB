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

namespace tdb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Query::Query(
    const Context &ctx, const std::string &array_uri, tiledb_query_type_t type)
    : ctx_(ctx)
    , deleter_(ctx)
    , schema_(ctx, array_uri) {
  tiledb_query_t *q;
  ctx.handle_error(tiledb_query_create(ctx, &q, array_uri.c_str(), type));
  query_ = std::shared_ptr<tiledb_query_t>(q, deleter_);
  array_attributes_ = schema_.attributes();
}

/* ********************************* */
/*                API                */
/* ********************************* */

Query &Query::set_layout(tiledb_layout_t layout) {
  auto &ctx = ctx_.get();
  ctx.handle_error(tiledb_query_set_layout(ctx, query_.get(), layout));
  return *this;
}

Query::Status Query::submit() {
  auto &ctx = ctx_.get();
  prepare_submission();
  ctx.handle_error(tiledb_query_submit(ctx, query_.get()));
  return query_status();
}

Query::Status Query::query_status() {
  tiledb_query_status_t status;
  auto &ctx = ctx_.get();
  ctx.handle_error(tiledb_query_get_status(ctx, query_.get(), &status));
  return to_status(status);
}

Query::Status Query::attribute_status(const std::string &attr) {
  tiledb_query_status_t status;
  auto &ctx = ctx_.get();
  ctx.handle_error(tiledb_query_get_attribute_status(
      ctx, query_.get(), attr.c_str(), &status));
  return to_status(status);
}

void Query::submit_async() {
  submit_async([](){});
}

std::vector<uint64_t> Query::returned_buff_sizes() {
  std::vector<uint64_t> buffsize(buff_sizes_.size());
  for (size_t i = 0; i < buff_sizes_.size(); ++i) {
    buffsize[i] = buff_sizes_[i] / sub_tsize_[i];
  }
  return buffsize;
}

void Query::reset_buffers() {
  attrs_.clear();
  attr_buffs_.clear();
  var_offsets_.clear();
  buff_sizes_.clear();
  all_buff_.clear();
  sub_tsize_.clear();
}

/* ********************************* */
/*         STATIC FUNCTIONS          */
/* ********************************* */

Query::Status Query::to_status(const tiledb_query_status_t &status) {
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

std::string Query::to_str(tiledb_query_type_t type) {
  switch (type) {
    case TILEDB_READ:
      return "READ";
    case TILEDB_WRITE:
      return "WRITE";
  }
  return "";  // silence error
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

void Query::prepare_submission() {
  all_buff_.clear();
  buff_sizes_.clear();
  attr_names_.clear();
  sub_tsize_.clear();

  uint64_t bufsize;
  size_t tsize;
  void *ptr;

  for (const auto &a : attrs_) {
    if (attr_buffs_.count(a) == 0) {
      throw AttributeError("No buffer for attribute " + a);
    }
    if (var_offsets_.count(a)) {
      std::tie(bufsize, tsize, ptr) = var_offsets_[a];
      all_buff_.push_back(ptr);
      buff_sizes_.push_back(bufsize * tsize);
      sub_tsize_.push_back(tsize);
    }
    std::tie(bufsize, tsize, ptr) = attr_buffs_[a];
    all_buff_.push_back(ptr);
    buff_sizes_.push_back(bufsize * tsize);
    attr_names_.push_back(a.c_str());
    sub_tsize_.push_back(tsize);
  }

  auto &ctx = ctx_.get();
  ctx.handle_error(tiledb_query_set_buffers(
      ctx,
      query_.get(),
      attr_names_.data(),
      (unsigned)attr_names_.size(),
      all_buff_.data(),
      buff_sizes_.data()));
}

/* ********************************* */
/*               MISC                */
/* ********************************* */

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

}  // Namespace tdb
