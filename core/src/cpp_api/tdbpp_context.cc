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
#include "tdbpp_array.h"

tdb::Context::Context() {
  tiledb_ctx_t *ctx;
  tiledb_ctx_create(&ctx);
  _ctx = std::shared_ptr<tiledb_ctx_t>(ctx, tiledb_ctx_free);
  _curr_object.uri = ".";
}

void tdb::Context::set_root(const std::string &root) {
  _curr_object.uri = root;
  tiledb_object_t type;
  handle_error(tiledb_object_type(_ctx.get(), root.c_str(), &type));
  _curr_object.set(type);
  if (_curr_object.type == Object::Type::Array) throw std::runtime_error("Cannot move context to an Array.");
}

tdb::Array tdb::Context::array_find(const std::string &name) {
  auto ret = Array(*this);
  bool found = false;
  for (const auto &i : *this) {
    if (i.type != Object::Type::Array) continue;
    if (i.uri.size() >= name.size() && i.uri.substr(i.uri.size() - name.size()) == name)  {
      if (found) throw std::runtime_error("Multiple matches found, exctend search path.");
      found = true;
      ret.load(i.uri);
    }
  }
  return ret;
}

tdb::Context tdb::Context::group_find(const std::string &name) {
  auto ret = Context(*this, Object::Type::Invalid);
  bool found = false;
  for (const auto &i : *this) {
    if (i.type != Object::Type::Group) continue;
    if (i.uri.size() >= name.size() && i.uri.substr(i.uri.size() - name.size()) == name)  {
      if (found) throw std::runtime_error("Multiple matches found, exctend search path.");
      found = true;
      ret = Context(*this, i);
    }
  }
  return ret;
}

tdb::Context tdb::Context::group_get(const std::string &uri) {
  auto ret = Context(*this, uri);
  if (ret.context_type().type != Object::Type::Group) throw std::runtime_error("Group does not exist: " + uri);
  return ret;
}

std::vector<tdb::Context> tdb::Context::groups() {
  std::vector<Context> ret;
  for (const auto &i : *this) {
    if (i.type == Object::Type::Group) ret.emplace_back(*this, i);
  }
  return ret;
}

std::vector<tdb::Array> tdb::Context::arrays() {
  std::vector<Array> ret;
  for (const auto &i : *this) {
    if (i.type == Object::Type::Array) ret.emplace_back(*this, i.uri);
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

tdb::Context tdb::Context::group_create(const std::string &group) {
  handle_error(tiledb_group_create(_ctx.get(), group.c_str()));
  return *this;
}

tdb::Context::iterator tdb::Context::begin(tiledb_walk_order_t order) {
  if (_curr_object.uri.empty()) throw std::runtime_error("No root directory specified.");
  return iterator(*this, _curr_object.uri, order);
}

tdb::Context::iterator tdb::Context::end() {
  return begin().end();
}

tdb::Array tdb::Context::array_get(const std::string &uri) {
  return Array(*this, uri);
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

void tdb::Context::del(std::string &name) {
  handle_error(tiledb_delete(_ctx.get(), name.c_str()));
}

std::ostream &operator<<(std::ostream &os, const tdb::Context &ctx) {
  os << "Ctx<" << ctx.context_type() << ">";
  return os;
}

void tdb::Context::iterator::_init(const std::string &root, tiledb_ctx_t *ctx, tiledb_walk_order_t order) {
  _ictx.handle_error(tiledb_walk(ctx, root.c_str(), order, _obj_getter, &_objs));
  _curr = 0;
}
