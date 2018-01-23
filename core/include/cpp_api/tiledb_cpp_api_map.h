/**
 * @file  tiledb_cpp_api_map.cc
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
 * This file declares the C++ API for the TileDB Map object.
 */
#ifndef TILEDB_TILEDB_CPP_API_MAP_H
#define TILEDB_TILEDB_CPP_API_MAP_H

#include "tiledb_cpp_api_map_schema.h"
#include "tiledb_cpp_api_context.h"
#include "tiledb_cpp_api_deleter.h"
#include "tiledb_cpp_api_map_item.h"
#include "tiledb_cpp_api_map_iter.h"
#include "tiledb_cpp_api_utils.h"

#include <string>
#include <memory>

namespace tdb {

  /**
   * A Key value store backed by a TileDB Sparse array. The Map is composed
   * of MapItems. After an Item is created and populated with values, defined
   * by the MapScema, the item can be added to the map.
   */
  class Map {
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
    Map(const Context &ctx, const std::string &uri) : schema_(ctx, uri), deleter_(ctx), uri_(uri) {
      tiledb_kv_t *kv;
      ctx.handle_error(tiledb_kv_open(ctx, &kv, uri.c_str(), nullptr, 0));
      kv_ = std::shared_ptr<tiledb_kv_t>(kv, deleter_);
    }

    Map(const Map &) = default;
    Map(Map &&o) = default;
    Map &operator=(const Map &) = default;
    Map &operator=(Map &&o) = default;

    /* ********************************* */
    /*                API                */
    /* ********************************* */

    /**
     * Create a map item with the given key. Once populated with attributes,
     * it can be added to a Map with map.add_item()
     *
     * @tparam T Key type
     * @param key Key value
     * @return MapItem
     */
    template<typename T>
    static MapItem create_item(const Context &ctx, const T &key) {
      return create_item_impl(ctx, key);
    }

    /** Get an item from the map given a key. Throws TileDBError on missing key. **/
    template<typename T>
    MapItem get_item(const T &key) {
      tiledb_kv_item_t *item = get_impl<T>(key);
      if (item == nullptr) {
        throw TileDBError("Key does not exist.");
      }
      return MapItem(schema_.context(), &item, this);
    }

    /**
     * Get an item with a given key. If the item doesn't exist; create it.
     *
     * @tparam T
     * @param key
     * @return MapItem
     */
    template<typename T>
    MapItem operator[](const T &key) {
      tiledb_kv_item_t *item = get_impl<T>(key);
      if (item == nullptr) {
        MapItem mapitem = create_item(schema_.context(), key);
        mapitem.map_ = this;
        return mapitem;
      }
      return MapItem(schema_.context(), &item, this);
    }

    /** Add an item to the map. This populates the map with the key and attribute values. **/
    void add_item(const MapItem &item) {
      auto &ctx = schema_.context();
      ctx.handle_error(tiledb_kv_add_item(ctx, kv_.get(), item.ptr().get()));
    }

    /** Max number of items to buffer in memory before flushing to storage. **/
    void set_max_buffered_items(uint64_t num) {
      auto &ctx = context();
      ctx.handle_error(tiledb_kv_set_max_items(ctx, kv_.get(), num));
    }

    /** Flush to storage. **/
    void flush() {
      auto &ctx = context();
      ctx.handle_error(tiledb_kv_flush(ctx, kv_.get()));
    }

    /** Get the schema of the map. **/
    const MapSchema &schema() const {
      return schema_;
    }

    /** Get the underlying context. **/
    const Context &context() const {
      return schema_.context();
    }

    /** URI **/
    const std::string &uri() const {
      return uri_;
    }

    /** Iterator to beginning of map. **/
    iterator begin() {
      iterator i(*this);
      i.init();
      return i;
    }

    /** Iterate over keys of type T **/
    template <typename T>
    iterator begin() {
      iterator i(*this);
      i.limit_key_type<T>();
      i.init();
      return i;
    }

    /** Iterator to end of map. **/
    iterator end() {
      return {*this, true};
    }

  private:

    /* ********************************* */
    /*        TYPE SPECIALIZATIONS       */
    /* ********************************* */

    /** Get for compound types. **/
    template<typename T>
    typename std::enable_if<std::is_fundamental<typename T::value_type>::value, tiledb_kv_item_t>::type
    *get_impl(const T &key) {
      auto &ctx = context();
      tiledb_kv_item_t *item;
      ctx.handle_error(tiledb_kv_get_item(ctx, kv_.get(), &item,
                                          const_cast<void*>(reinterpret_cast<const void*>(key.data())),
                                          impl::type_from_native<typename T::value_type>::type::tiledb_datatype,
                                          sizeof(typename T::value_type) * key.size()));
      return item;
    };

    /** Get for native types. **/
    template<typename T>
    typename std::enable_if<std::is_fundamental<T>::value, tiledb_kv_item_t>::type
    *get_impl(const T &key) {
      auto &ctx = context();
      tiledb_kv_item_t *item;
      ctx.handle_error(tiledb_kv_get_item(ctx, kv_.get(), &item,
                                          const_cast<void*>(reinterpret_cast<const void*>(&key)),
                                          impl::type_from_native<T>::type::tiledb_datatype,
                                          sizeof(T)));
      return item;
    };

    /** Create for native types. **/
    template<typename T>
    static
    typename std::enable_if<std::is_fundamental<T>::value, MapItem>::type
    create_item_impl(const Context &ctx, const T &key) {
      return MapItem(ctx, const_cast<void*>(reinterpret_cast<const void*>(&key)),
                     impl::type_from_native<T>::type::tiledb_datatype,
                     sizeof(T), nullptr);
    }

    /** Create for compound types. **/
    template<typename T>
    static
    typename std::enable_if<std::is_fundamental<typename T::value_type>::value, MapItem>::type
    create_item_impl(const Context &ctx, const T &key) {
      return MapItem(ctx, const_cast<void*>(reinterpret_cast<const void*>(key.data())),
                     impl::type_from_native<typename T::value_type>::type::tiledb_datatype,
                     sizeof(typename T::value_type) * key.size(), nullptr);
    }

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
    const std::string uri_;

  };

  /* ********************************* */
  /*        NON MEMBER MAP FUNC        */
  /* ********************************* */

  /**
   * Create a new map.
   *
   * @param uri URI to make map at.
   * @param schema schema defining the map structure.
   */
  void create_map(const std::string &uri, const MapSchema &schema);

  /** Consolidate map fragments. **/
  void consolidate_map(const Context& ctx, const std::string& map);

  /** Add an item to the map. **/
  Map &operator<<(Map &map, const MapItem &item);

}

#endif //TILEDB_TILEDB_CPP_API_MAP_H
