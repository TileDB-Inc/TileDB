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

#include <string>
#include <memory>

namespace tdb {

  class Map {
  public:
    Map(const Context &ctx, const std::string &uri) : schema_(ctx, uri), deleter_(ctx) {
      tiledb_kv_t *kv;
      ctx.handle_error(tiledb_kv_open(ctx, &kv, uri.c_str(), nullptr, 0));
      kv_ = std::shared_ptr<tiledb_kv_t>(kv, deleter_);
    }

    Map(const Map &) = default;
    Map(Map &&o) = default;
    Map &operator=(const Map &) = default;
    Map &operator=(Map &&o) = default;
    ~Map() {
      flush();
    }

    template<typename T>
    typename std::enable_if<std::is_fundamental<T>::value, MapItem>::type
    create_item(const T &key) {
      return MapItem(*this,
                     const_cast<void*>(reinterpret_cast<const void*>(&key)),
                     impl::type_from_native<T>::type::tiledb_datatype,
                     sizeof(T));
    }

    template<typename T>
    typename std::enable_if<std::is_fundamental<typename T::value_type>::value, MapItem>::type
    create_item(const T &key) {
      return MapItem(*this,
                     const_cast<void*>(reinterpret_cast<const void*>(key.data())),
                     impl::type_from_native<typename T::value_type>::type::tiledb_datatype,
                     sizeof(typename T::value_type) * key.size());
    }

    template<typename T>
    typename std::enable_if<std::is_fundamental<T>::value, MapItem>::type
    get_item(const T &key) {
      auto &ctx = context();
      tiledb_kv_item_t *item;
      ctx.handle_error(tiledb_kv_get_item(ctx, kv_.get(), &item,
                                          const_cast<void*>(reinterpret_cast<const void*>(&key)),
                                          impl::type_from_native<T>::type::tiledb_datatype,
                                          sizeof(T)));
      if (item == nullptr) {
        throw TileDBError("Key does not exist.");
      }
      return MapItem(*this, &item);
    }

    template<typename T>
    typename std::enable_if<std::is_fundamental<typename T::value_type>::value, MapItem>::type
    get_item(const T &key) {
      auto &ctx = context();
      tiledb_kv_item_t *item;
      ctx.handle_error(tiledb_kv_get_item(ctx, kv_.get(), &item,
                                          const_cast<void*>(reinterpret_cast<const void*>(key.data())),
                                          impl::type_from_native<typename T::value_type>::type::tiledb_datatype,
                                          sizeof(typename T::value_type) * key.size()));
      if (item == nullptr) {
        throw TileDBError("Key does not exist.");
      }
      return MapItem(*this, &item);
    }

    void add_item(const MapItem &item) {
      auto &ctx = schema_.context();
      ctx.handle_error(tiledb_kv_add_item(ctx, kv_.get(), item.ptr().get()));
    }

    void set_max_buffered_items(uint64_t num) {
      auto &ctx = context();
      ctx.handle_error(tiledb_kv_set_max_items(ctx, kv_.get(), num));
    }

    void flush() {
      auto &ctx = context();
      ctx.handle_error(tiledb_kv_flush(ctx, kv_.get()));
    }

    const MapSchema &schema() const {
      return schema_;
    }

    const Context &context() const {
      return schema_.context();
    }

  private:
    friend class MapItem;
    MapSchema schema_;
    std::shared_ptr<tiledb_kv_t> kv_;

    /** Closes the map **/
    impl::Deleter deleter_;

  };

  void create_map(const std::string &uri, const MapSchema &schema);

  void consolidate_map(const Context& ctx, const std::string& map);

  Map &operator<<(Map &map, const MapItem &item);

}

#endif //TILEDB_TILEDB_CPP_API_MAP_H
