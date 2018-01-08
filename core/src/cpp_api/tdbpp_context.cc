/**
 * @file   tdbpp_context.cc
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

#include "tdbpp_context.h"
#include "tdbpp_arrayschema.h"

tdb::Context::Context() {
  tiledb_ctx_t *ctx;
  tiledb_ctx_create(&ctx, NULL);
  _ctx = std::shared_ptr<tiledb_ctx_t>(ctx, tiledb_ctx_free);
  _curr_object.uri = ".";
}

void tdb::Context::set_root(const std::string &root) {
  _curr_object.uri = root;
  tiledb_object_t type;
  handle_error(tiledb_object_type(_ctx.get(), root.c_str(), &type));
  _curr_object.set(type);
}

std::string tdb::Context::find_array(const std::string &name) {
  bool found = false;
  std::string ret = "";
  for (const auto &i : *this) {
    if (i.type != Object::Type::Array)
      continue;
    if (i.uri.size() >= name.size() &&
        i.uri.substr(i.uri.size() - name.size()) == name) {
      if (found)
        throw std::runtime_error("Multiple matches found, extend search name.");
      found = true;
      ret = i.uri;
    }
  }
  return ret;
}

std::string tdb::Context::find_group(const std::string &name) {
  std::string ret;
  bool found = false;
  for (const auto &i : *this) {
    if (i.type != Object::Type::Group)
      continue;
    if (i.uri.size() >= name.size() &&
        i.uri.substr(i.uri.size() - name.size()) == name) {
      if (found)
        throw std::runtime_error("Multiple matches found, extend search path.");
      found = true;
      ret = i.uri;
    }
  }
  return ret;
}

std::vector<std::string> tdb::Context::groups() {
  std::vector<std::string> ret;
  for (const auto &i : *this) {
    if (i.type == Object::Type::Group)
      ret.push_back(i.uri);
  }
  return ret;
}

std::vector<std::string> tdb::Context::arrays() {
  std::vector<std::string> ret;
  for (const auto &i : *this) {
    if (i.type == Object::Type::Array)
      ret.push_back(i.uri);
  }
  return ret;
}

std::string tdb::Context::_handle_error() {
  tiledb_error_t *_err = nullptr;
  const char *_msg = nullptr;
  std::string _msg_str;
  tiledb_error_last(_ctx.get(), &_err);
  tiledb_error_message(_ctx.get(), _err, &_msg);
  _msg_str = std::string(_msg);
  tiledb_error_free(_ctx.get(), _err);
  return _msg_str;
}

tdb::Context tdb::Context::create_group(const std::string &group) {
  handle_error(tiledb_group_create(_ctx.get(), group.c_str()));
  return *this;
}

tdb::Context::iterator tdb::Context::begin(tiledb_walk_order_t order) {
  if (_curr_object.uri.empty())
    throw std::runtime_error("No root directory specified.");
  return iterator(*this, _curr_object.uri, order);
}

tdb::Context::iterator tdb::Context::end() {
  return begin().end();
}

tdb::Object tdb::Context::object_type(const std::string &uri) {
  Object ret;
  tiledb_object_t type;
  handle_error(tiledb_object_type(_ctx.get(), uri.c_str(), &type));
  ret.set(type);
  return ret;
}

void tdb::Context::handle_error(int ret) {
  if (ret != TILEDB_OK) {
    handle_error(ret, _handler);
  }
}

void tdb::Context::consolidate(const std::string &name) {
  handle_error(tiledb_array_consolidate(_ctx.get(), name.c_str()));
}

void tdb::Context::del(const std::string &name) {
  handle_error(tiledb_delete(_ctx.get(), name.c_str()));
}

tdb::Context::Context(const std::string &root)
    : Context() {
  set_root(root);
}

tiledb_ctx_t *tdb::Context::operator->() {
  return _ctx.get();
}

tdb::Context::operator tiledb_ctx_t *() {
  return _ctx.get();
}

const tdb::Object &tdb::Context::context_type() const {
  return _curr_object;
}

tiledb_ctx_t *tdb::Context::get() {
  return _ctx.get();
}

void tdb::Context::move(std::string oldname, std::string newname, bool force) {
  handle_error(
      tiledb_move(_ctx.get(), oldname.c_str(), newname.c_str(), force));
}

void tdb::Context::set_error_handler(std::function<void(std::string)> fn) {
  _handler = fn;
}

void tdb::Context::_default_handler(std::string msg) {
  throw std::runtime_error(msg);
}

void tdb::Context::create_array(
    const std::string &name, const tdb::ArraySchema &schema) {
  auto ctx = _ctx.get();
  handle_error(tiledb_array_schema_check(ctx, schema.ptr().get()));
  handle_error(tiledb_array_create(ctx, name.c_str(), schema.ptr().get()));
}

const std::string &tdb::Context::root() const {
  return _curr_object.uri;
}

void tdb::Context::iterator::_init(
    const std::string &root, tiledb_ctx_t *ctx, tiledb_walk_order_t order) {
  _ictx.handle_error(
      tiledb_walk(ctx, root.c_str(), order, _obj_getter, &_objs));
  _curr = 0;
}

std::ostream &tdb::operator<<(std::ostream &os, const tdb::Context &ctx) {
  os << "Ctx<" << ctx.context_type() << ">";
  return os;
}