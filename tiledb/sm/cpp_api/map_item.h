/**
 * @file  tiledb_cpp_api_map_item.h
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
 * This file declares the C++ API for the TileDB Map Item object.
 */

#ifndef TILEDB_CPP_API_MAP_ITEM_H
#define TILEDB_CPP_API_MAP_ITEM_H

#include "attribute.h"
#include "exception.h"
#include "tiledb.h"

#include <functional>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#define TILEDB_SINGLE_ATTRIBUTE_MAP "value"

namespace tiledb {

/** Forward declarations. **/
class Map;

namespace impl {
class MultiMapItemProxy;
class MapItemProxy;
}  // namespace impl

/** Object representing a Map key and its values. **/
class TILEDB_EXPORT MapItem {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Load a MapItem given a pointer. **/
  MapItem(const Context& ctx, tiledb_kv_item_t** item, Map* map = nullptr)
      : ctx_(ctx)
      , deleter_(ctx)
      , map_(map) {
    item_ = std::shared_ptr<tiledb_kv_item_t>(*item, deleter_);
    *item = nullptr;
  }

  MapItem(const MapItem&) = default;
  MapItem(MapItem&& o) = default;
  MapItem& operator=(const MapItem&) = default;
  MapItem& operator=(MapItem&& o) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Checks the goodness of the key-value item. Useful when
   * checking if a retrieved key-value item exists in a map.
   */
  bool good() const {
    return item_ != nullptr;
  }

  /** Set an attribute to the given value **/
  template <typename T>
  void set(const std::string& attr, const T& val) {
    using DataT = typename impl::TypeHandler<T>;

    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_kv_item_set_value(
        ctx,
        item_.get(),
        attr.c_str(),
        DataT::data(val),
        DataT::tiledb_type,
        DataT::size(val) * sizeof(typename DataT::value_type)));
  }

  /** Get the key for the item. */
  template <typename T>
  T key() const {
    using DataT = typename impl::TypeHandler<T>;
    auto& ctx = ctx_.get();

    typename DataT::value_type* buf;
    tiledb_datatype_t type;
    uint64_t size;

    ctx.handle_error(tiledb_kv_item_get_key(
        ctx, item_.get(), (const void**)&buf, &type, &size));

    impl::type_check<T>(type, unsigned(size / sizeof(T)));
    T key;
    DataT::set(key, buf, size);
    return key;
  }

  /** Get the key datatype and size **/
  std::pair<tiledb_datatype_t, uint64_t> key_info() const {
    auto& ctx = ctx_.get();
    const void* key;
    tiledb_datatype_t type;
    uint64_t size;
    ctx.handle_error(
        tiledb_kv_item_get_key(ctx, item_.get(), &key, &type, &size));
    return {type, size};
  }

  /**
   * Get the value for a given attribute.
   *
   * @note
   * This does not check for the number of elements, but rather returns
   * the size (in elements) retrieved.
   * */
  template <typename T>
  std::pair<const T*, uint64_t> get_ptr(const std::string& attr) const {
    using DataT = typename impl::TypeHandler<T>;
    const Context& ctx = ctx_.get();

    typename DataT::value_type* data;
    tiledb_datatype_t type;
    uint64_t size;

    ctx.handle_error(tiledb_kv_item_get_value(
        ctx, item_.get(), attr.c_str(), (const void**)&data, &type, &size));

    auto num = static_cast<unsigned>(size / sizeof(typename DataT::value_type));
    impl::type_check<T>(type);  // Just check type
    return std::pair<const T*, uint64_t>(data, num);
  }

  /** Get a attribute with a given return type. **/
  template <typename T>
  T get(const std::string& attr) const {
    using DataT = typename impl::TypeHandler<T>;

    const typename DataT::value_type* data;
    unsigned num;
    std::tie(data, num) = get_ptr<typename DataT::value_type>(attr);

    T ret;
    DataT::set(ret, data, num * sizeof(typename DataT::value_type));
    return ret;
  }

  /** Get a single anon attribute. **/
  template <typename T>
  T get() const {
    return get<T>(TILEDB_SINGLE_ATTRIBUTE_MAP);
  }

  /** Get a proxy to set/get with operator[] **/
  impl::MapItemProxy operator[](const std::string& attr);

  impl::MultiMapItemProxy operator[](const std::vector<std::string>& attrs);

  /** Set a key for single anonymous attribute maps **/
  template <typename T>
  MapItem& operator=(const T& value) {
    set<T>(TILEDB_SINGLE_ATTRIBUTE_MAP, value);
    add_to_map();
    return *this;
  }

  /** Ptr to underlying object. **/
  std::shared_ptr<tiledb_kv_item_t> ptr() const {
    return item_;
  }

 private:
  friend class Map;
  friend class impl::MapItemProxy;
  friend class impl::MultiMapItemProxy;

  void add_to_map();

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Make an item with the given key. **/
  MapItem(
      const Context& ctx,
      const void* key,
      tiledb_datatype_t type,
      size_t size,
      Map* map = nullptr)
      : ctx_(ctx)
      , deleter_(ctx)
      , map_(map) {
    tiledb_kv_item_t* p;
    ctx.handle_error(tiledb_kv_item_create(ctx, &p));
    ctx.handle_error(tiledb_kv_item_set_key(ctx, p, key, type, size));
    item_ = std::shared_ptr<tiledb_kv_item_t>(p, deleter_);
  }

  /** A TileDB context reference wrapper. */
  std::reference_wrapper<const Context> ctx_;

  /** Ptr to TileDB object. **/
  std::shared_ptr<tiledb_kv_item_t> item_;

  /** Deleter for tiledb object **/
  impl::Deleter deleter_;

  /** Underlying Map **/
  Map* map_ = nullptr;
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_MAP_ITEM_H
