/**
 * @file  tiledb_cpp_api_map.h
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
 * This file declares the C++ API for the TileDB Map object.
 */
#ifndef TILEDB_CPP_API_MAP_H
#define TILEDB_CPP_API_MAP_H

#include "context.h"
#include "deleter.h"
#include "map_item.h"
#include "map_iter.h"
#include "map_schema.h"
#include "utils.h"

#include <map>
#include <memory>
#include <string>

namespace tiledb {

/**
 * A Key value store backed by a TileDB Sparse array. A Map
 * supports multiple key types and the value is defined by the
 * set of attributes in a MapSchema.
 *
 * **Example:**
 *
 * @code{.cpp}
 * // Make the map
 * tiledb::MapSchema schema(ctx);
 * schema.add_attributes(...);
 * Map::create("my_map", schema);
 *
 * // Write to the map
 * std::vector<double> key = {2345.1, 345.2};
 *
 * // Attribute values
 * int t1 = 3;
 * std::string t2{"ccc"};
 * std::array<float, 2> t3({{3.1f, 3.2f}});
 *
 * map[key][{"a1", "a2", "a3"}] = std::make_tuple(t1, t2, t3);
 * map.flush(); // Flush to disk
 *
 * // Read value from map
 * std::tuple<int, std::string, std::array<float, 2>> vals = map[key];
 * @endcode
 */
class TILEDB_EXPORT Map {
 public:
  using iterator = impl::MapIter;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Load a map.
   *
   * @param ctx Context
   * @param uri URI of map.
   */
  Map(const Context& ctx, const std::string& uri)
      : schema_(ctx, uri)
      , deleter_(ctx)
      , uri_(uri) {
    tiledb_kv_t* kv;
    ctx.handle_error(tiledb_kv_open(ctx, &kv, uri.c_str(), nullptr, 0));
    kv_ = std::shared_ptr<tiledb_kv_t>(kv, deleter_);
  }

  Map(const Map&) = default;
  Map(Map&& o) = default;
  Map& operator=(const Map&) = default;
  Map& operator=(Map&& o) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Create a map item with the given key. Once populated with attributes,
   * it can be added to a Map with map.add_item()
   */
  template <typename T>
  static MapItem create_item(const Context& ctx, const T& key) {
    using DataT = typename impl::TypeHandler<T>;
    return MapItem(
        ctx,
        DataT::data(key),
        DataT::tiledb_type,
        DataT::size(key) * sizeof(typename DataT::value_type),
        nullptr);
  }

  /**
   * Get an item from the map given a key. Throws `TileDBError` on missing
   * key.
   */
  template <typename T>
  MapItem get_item(const T& key) {
    using DataT = typename impl::TypeHandler<T>;

    auto& ctx = context();
    tiledb_kv_item_t* item;

    ctx.handle_error(tiledb_kv_get_item(
        ctx,
        kv_.get(),
        &item,
        DataT::data(key),
        DataT::tiledb_type,
        DataT::size(key) * sizeof(typename DataT::value_type)));

    return MapItem(schema_.context(), &item, this);
  }

  /** Get an item with a given key. If the item doesn't exist; create it. */
  template <typename T>
  MapItem operator[](const T& key) {
    using DataT = typename impl::TypeHandler<T>;

    auto& ctx = context();
    tiledb_kv_item_t* item;

    ctx.handle_error(tiledb_kv_get_item(
        ctx,
        kv_.get(),
        &item,
        DataT::data(key),
        DataT::tiledb_type,
        DataT::size(key) * sizeof(typename DataT::value_type)));

    if (item == nullptr) {
      MapItem mapitem = create_item(schema_.context(), key);
      mapitem.map_ = this;
      return mapitem;
    }

    return MapItem(schema_.context(), &item, this);
  }

  /**
   * Add an item to the map. This populates the map with the key and attribute
   * values.
   */
  Map& add_item(const MapItem& item) {
    auto& ctx = schema_.context();
    ctx.handle_error(tiledb_kv_add_item(ctx, kv_.get(), item.ptr().get()));
    return *this;
  }

  /** Max number of items to buffer in memory before flushing to storage. **/
  void set_max_buffered_items(uint64_t num) {
    auto& ctx = context();
    ctx.handle_error(tiledb_kv_set_max_buffered_items(ctx, kv_.get(), num));
  }

  /** Flush to storage. **/
  void flush() {
    auto& ctx = context();
    ctx.handle_error(tiledb_kv_flush(ctx, kv_.get()));
  }

  /** Get the schema of the map. **/
  const MapSchema& schema() const {
    return schema_;
  }

  /** Get the underlying context. **/
  const Context& context() const {
    return schema_.context();
  }

  /** URI **/
  const std::string& uri() const {
    return uri_;
  }

  /** Iterator to beginning of map. **/
  iterator begin() {
    iterator i(*this);
    i.init();
    return i;
  }

  /** Iterate over keys of type T */
  template <typename T>
  iterator begin() {
    iterator i(*this);
    i.limit_key_type<T>();
    i.init();
    return i;
  }

  /** Iterator to end of map. **/
  iterator end() {
    return iterator{*this, true};
  }

  /* ********************************* */
  /*               STATIC              */
  /* ********************************* */

  /** Create a new map. */
  static void create(const std::string& uri, const MapSchema& schema);

  /** Create a TileDB map from a std::map **/
  template <typename Key, typename Value>
  static void create(
      const Context& ctx,
      const std::string& uri,
      const std::map<Key, Value>& map) {
    MapSchema schema(ctx);
    auto a = Attribute::create<Value>(ctx, TILEDB_SINGLE_ATTRIBUTE_MAP);
    schema.add_attribute(a);
    create(uri, schema);
    Map m(ctx, uri);
    for (const auto& p : map) {
      m[p.first] = p.second;
    }
  }

  /** Consolidate map fragments. **/
  static void consolidate(const Context& ctx, const std::string& map);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  friend class MapItem;

  /** Schema of the map. **/
  MapSchema schema_;

  /** ptr to underlying TileDB object. **/
  std::shared_ptr<tiledb_kv_t> kv_;

  /** Closes the map on destruction. **/
  impl::Deleter deleter_;

  /** URI **/
  std::string uri_;
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_MAP_H
