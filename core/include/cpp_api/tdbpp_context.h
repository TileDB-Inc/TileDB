/**
 * @file   tdbpp_context.h
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

#ifndef TILEDB_GENOMICS_CONTEXT_H
#define TILEDB_GENOMICS_CONTEXT_H

#include "tiledb.h"
#include "tdbpp_object.h"

#include <string>
#include <iostream>
#include <iterator>
#include <vector>
#include <memory>
#include <functional>

namespace tdb {

  class Array;
  struct ArrayMetadata;

  class Context {
  public:
    Context();
    Context(const std::string &root) : Context() {
      set_root(root);
    }
    Context(Context &ctx, const std::string &root) : _ctx(ctx._ctx) {
      set_root(root);
    }
    Context(Context &ctx, const Object obj) : _ctx(ctx._ctx), _curr_object(obj) {}
    Context(const Context &ctx) = default;
    Context(Context &&o) = default;
    Context &operator=(const Context &o) = default;
    Context &operator=(Context &&o) = default;

    void set_root(const std::string &root);

    tiledb_ctx_t *operator->() {
      return _ctx.get();
    }

    operator tiledb_ctx_t*() {
      return _ctx.get();
    }

    tiledb_ctx_t *get() {
      return _ctx.get();
    }

    const Object &context_type() const {
      return _curr_object;
    }

    class iterator: public std::iterator<std::forward_iterator_tag, Object> {
    public:
      iterator(Context &ctx, const std::string &root, tiledb_walk_order_t order): _root(root), _ictx(ctx) {
        _init(root, ctx, order);
      }

      iterator(const iterator &o) : _ictx(o._ictx) {
        _root = o._root;
        _curr = o._curr;
        _objs = o._objs;
      }

      bool operator==(const iterator &o) {
        return _curr == o._curr;
      }

      bool operator!=(const iterator &o) {
        return !operator==(o);
      }

      const Object &operator*() const {
        return _objs[_curr];
      }

      const Object &operator->() const {
        return _objs[_curr];
      }

      iterator &operator++() {
        if (_curr < _objs.size()) ++ _curr;
        return *this;
      }

      iterator end() {
        _curr = _objs.size();
        return *this;
      }

    private:
      std::string _root;
      size_t _curr;
      std::vector<Object> _objs;
      Context &_ictx;

      void _init(const std::string &root, tiledb_ctx_t *ctx, tiledb_walk_order_t order);

      static int _obj_getter(const char* path, tiledb_object_t type, void *d) {
        Object obj;
        std::vector<Object> *vec = static_cast<std::vector<Object>*>(d);
        obj.set(type);
        obj.uri = std::string(path);
        vec->push_back(std::move(obj));
        return 1;
      }
    };

    iterator begin(tiledb_walk_order_t order=TILEDB_PREORDER);

    iterator end();

    Object object_type(const std::string &uri);

    std::vector<Context> groups();

    Context group_find(const std::string &name);

    Context group_get(const std::string &uri);

    Context group_create(const std::string &group);

    std::vector<Array> arrays();

    Array array_find(const std::string &name);

    Array array_get(const std::string &uri);

    void consolidate(const std::string &name);

    void del(std::string &name);

    template<typename C>
    void handle_error(int ret, C callback) {
      if (ret != TILEDB_OK) {
        auto msg = _handle_error();
        callback(msg);
      }
    }

    void handle_error(int ret);

    void set_error_handler(std::function<void(std::string)> fn) {
      _handler = fn;
    }

  private:
    static void _default_handler(std::string msg) {
      throw std::runtime_error(msg);
    };
    std::function<void(std::string)> _handler = _default_handler;
    std::string _handle_error();
    std::shared_ptr<tiledb_ctx_t> _ctx;
    Object _curr_object;

  };

}

std::ostream &operator<<(std::ostream &os, const tdb::Context &ctx);

#endif //TILEDB_GENOMICS_CONTEXT_H
