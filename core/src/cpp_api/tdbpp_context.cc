/**
 * @file   tiledb.h
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

tdb::Context::Context() {
  tiledb_ctx_t *ctx;
  tiledb_ctx_create(&ctx);
  _ctx = std::shared_ptr<tiledb_ctx_t>(ctx, tiledb_ctx_free);
}

void tdb::Context::set_root(const std::string &root) {
  _obj.uri = root;
  tiledb_object_t type;
  handle_error(tiledb_object_type(_ctx.get(), root.c_str(), &type));
  _obj.set(type);
  if (_obj.type == Object::Type::Array) throw std::runtime_error("Cannot move context to an Array.");
}

tdb::Array tdb::Context::array_find(const std::string &name) {
  auto ret = Array(*this, "");
  bool found = false;
  for (const auto &i : *this) {
    if (i.type != Object::Type::Array) continue;
    if (i.uri.size() >= name.size() && i.uri.substr(i.uri.size() - name.size()) == name)  {
      if (found) throw std::runtime_error("Multiple matches found, exctend search path.");
      found = true;
      ret = Array(*this, i.uri);
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

void tdb::Context::handle_error(const int ret) {
  if (ret != TILEDB_OK) {
    tiledb_error_t *_err = nullptr;
    const char *_msg = nullptr;
    std::string _msg_str;
    tiledb_error_last(_ctx.get(), &_err);
    tiledb_error_message(_ctx.get(), _err, &_msg);
    _msg_str = std::string(_msg);
    tiledb_error_free(_ctx.get(), _err);
    throw std::runtime_error(_msg_str);
  }
}

tdb::Context tdb::Context::group_create(const std::string &group) {
  handle_error(tiledb_group_create(_ctx.get(), group.c_str()));
  return group_find(group); // TODO there should be better way to get abs path
}

tdb::Context::iterator tdb::Context::begin() {
  if (_obj.uri.empty()) throw std::runtime_error("No root directory specified.");
  return iterator(*this, _obj.uri);
}

tdb::Context::iterator tdb::Context::end() {
  return begin().end();
}

tdb::Array tdb::Context::array_get(const std::string &uri) {
  return Array(*this, uri);
}

std::ostream &operator<<(std::ostream &os, const tdb::Context &ctx) {
  os << ctx.context_type();
  return os;
}
