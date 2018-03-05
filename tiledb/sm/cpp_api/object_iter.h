/**
 * @file   tiledb_cpp_api_object_iter.h
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
 * This file declares the C++ API for the TileDB ObjectIter object.
 */

#ifndef TILEDB_CPP_API_OBJECT_ITER_H
#define TILEDB_CPP_API_OBJECT_ITER_H

#include "context.h"
#include "object.h"
#include "tiledb.h"

#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

namespace tiledb {

/**
 * Enables listing TileDB objects in a directory or walking recursively an
 * entire directory tree.
 */
class TILEDB_EXPORT ObjectIter {
 public:
  /* ********************************* */
  /*         TYPE DEFINITIONS          */
  /* ********************************* */

  /** Carries data to be passed to `obj_getter`. */
  struct ObjGetterData {
    ObjGetterData(std::vector<Object>& objs, bool array, bool group, bool kv)
        : objs_(objs)
        , array_(array)
        , group_(group)
        , kv_(kv) {
    }

    std::reference_wrapper<std::vector<Object>> objs_;
    bool array_;
    bool group_;
    bool kv_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Creates an object iterator. Unless `set_recursive` is invoked, this
   * iterator will iterate only over the children of `root`. It will
   * also retrieve only TileDB-related objects.
   *
   * @param ctx The TileDB context.
   * @param root The root directory where the iteration will begin.
   */
  explicit ObjectIter(Context& ctx, const std::string& root = ".");

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Determines whether group, array and key-value objects will be iterated
   * on during the walk. The default (if the function is not invoked) is
   * `true` for all objects.
   *
   * @param group If `true`, groups will be considered.
   * @param array If `true`, arrays will be considered.
   * @param kv If `true`, key-values will be considered.
   */
  void set_iter_policy(bool group, bool array, bool kv);

  /**
   * Specifies that the iteration will be over all the directories in the
   * tree rooted at `root_`.
   *
   * @param walk_order The walk order.
   */
  void set_recursive(tiledb_walk_order_t walk_order = TILEDB_PREORDER);

  /** Disables recursive traversal. */
  void set_non_recursive();

  /** The actual iterator implementation in this class. */
  class iterator
      : public std::iterator<std::forward_iterator_tag, const Object> {
   public:
    iterator()
        : cur_obj_(0) {
    }
    explicit iterator(std::vector<Object> objs)
        : cur_obj_(0)
        , objs_(std::move(objs)) {
    }

    iterator(const iterator& o) = default;
    iterator(iterator&&) = default;
    iterator& operator=(const iterator&) = default;
    iterator& operator=(iterator&&) = default;

    bool operator==(const iterator& o) const {
      return (cur_obj_ >= objs_.size() && o.cur_obj_ >= o.objs_.size()) ||
             (cur_obj_ == o.cur_obj_ && objs_.size() == o.objs_.size());
    }

    bool operator!=(const iterator& o) const {
      return !operator==(o);
    }

    const Object& operator*() const {
      return objs_[cur_obj_];
    }

    const Object& operator->() const {
      return objs_[cur_obj_];
    }

    iterator& operator++() {
      if (cur_obj_ < objs_.size())
        ++cur_obj_;
      return *this;
    }

    iterator end() {
      cur_obj_ = objs_.size();
      return *this;
    }

   private:
    /** The current object. */
    size_t cur_obj_;

    /** A reference to the objects retrieved by the `ObjectIter` object. */
    std::vector<Object> objs_;
  };

  /** Returns an object iterator at the beginning of its iteration. */
  iterator begin();

  /** Returns an object iterator at the end of its iteration. */
  iterator end() const;

  /**
   * Callback function to be used when invoking the C TileDB functions
   * for walking through the TileDB objects in the `root_` diretory.
   * The function retrieves the visited object and stored it in the
   * object vector `obj_vec`.
   *
   * @param path The path of a visited TileDB object
   * @param type The type of the visited TileDB object.
   * @param obj_vec The vector where the visited object will be stored.
   * @return If `1` then the walk should continue to the next object.
   */
  static int obj_getter(const char* path, tiledb_object_t type, void* data);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** If `true`, arrays will be considered in the walk. */
  bool array_;

  /** The TileDB context. */
  std::reference_wrapper<Context> ctx_;

  /** If `true`, groups will be considered in the walk. */
  bool group_;

  /** If `true`, key-values will be considered in the walk. */
  bool kv_;

  /**
   * True if the iteration will recursively walk through the whole
   * directory tree.
   */
  bool recursive_;

  /** The root directory where the iteration will start from. */
  std::string root_;

  /** The walk order for the iteration. */
  tiledb_walk_order_t walk_order_;
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_OBJECT_ITER_H
