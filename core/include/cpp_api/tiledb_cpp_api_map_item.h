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
 * This file declares the C++ API for the TileDB Map Item object.
 */

#ifndef TILEDB_TILEDB_CPP_API_MAP_ITEM_H
#define TILEDB_TILEDB_CPP_API_MAP_ITEM_H

#include "tiledb.h"
#include "tiledb_cpp_api_exception.h"
#include "tiledb_cpp_api_attribute.h"

#include <memory>
#include <functional>
#include <type_traits>

namespace tdb {

  class Map;
  namespace impl {
    class map_item_proxy;
  }

  class MapItem {
  public:
    MapItem(const MapItem &) = default;
    MapItem(MapItem &&o) = default;
    MapItem &operator=(const MapItem &) = default;
    MapItem &operator=(MapItem &&o) = default;

    template <typename V>
    typename std::enable_if<std::is_fundamental<V>::value, void>::type
    set(const std::string &attr, const V &val) {
      using AttrT = typename impl::type_from_native<V>::type;
      const Context &ctx = get_context();
      Attribute attribute = get_attribute(attr);
      impl::type_check_attr<AttrT>(attribute, 1);
      ctx.handle_error(tiledb_kv_item_set_value(ctx, item_.get(), attr.c_str(), &val, AttrT::tiledb_datatype, sizeof(V)));
    }

    template <typename V>
    typename std::enable_if<std::is_fundamental<typename V::value_type>::value, void>::type
    set(const std::string &attr, const V &val) {
      using AttrT = typename impl::type_from_native<typename V::value_type>::type;
      const Context &ctx = get_context();
      Attribute attribute = get_attribute(attr);
      impl::type_check_attr<AttrT>(attribute, val.size());
      ctx.handle_error(tiledb_kv_item_set_value(ctx, item_.get(), attr.c_str(),
                                                const_cast<void*>(reinterpret_cast<const void*>(val.data())),
                                                AttrT::tiledb_datatype, sizeof(typename V::value_type) * val.size()));
    }

    template <typename T>
    std::pair<const T*, uint64_t> get_ptr(const std::string &attr, unsigned expected_num=TILEDB_VAR_NUM) const {
      const Context &ctx = get_context();

      Attribute attribute = get_attribute(attr);
      if (expected_num == TILEDB_VAR_NUM) {
        impl::type_check<typename impl::type_from_native<T>::type>(attribute.type());
      }
      else {
        impl::type_check_attr<typename impl::type_from_native<T>::type>(attribute, expected_num);
      }

      const T *data;
      tiledb_datatype_t type;
      uint64_t size;

      ctx.handle_error(tiledb_kv_item_get_value(ctx, item_.get(), attr.c_str(), (const void**)&data, &type, &size));

      unsigned num = static_cast<unsigned>(size/sizeof(T));
      return std::pair<const T*, uint64_t>(data, num);
    }

    template <typename T, typename V = std::vector<T>, typename = typename V::value_type>
    V get(const std::string &attr) const {
      const T *data;
      unsigned num;
      std::tie(data, num) = get_ptr<T>(attr);

      V ret;
      ret.resize(num);
      std::copy<const T*, T*>(data, data + num, const_cast<T*>(ret.data()));
      return ret;
    }

    template <typename T, unsigned N>
    std::array<T,N> get(const std::string &attr) const {
      const T *data;
      unsigned num;
      std::tie(data, num) = get_ptr<T>(attr, N);

      std::array<T,N> ret;
      std::copy(data, data + N, ret.data());
      return ret;
    }

    template <typename T>
    typename std::enable_if<std::is_fundamental<T>::value, T>::type
    get_single(const std::string &attr) const {
      const T *data;
      unsigned num;
      std::tie(data, num) = get_ptr<T>(attr, 1);
      return *data;
    }

    impl::map_item_proxy operator[](const std::string &attr);

    std::shared_ptr<tiledb_kv_item_t> ptr() const {
      return item_;
    }

  private:
    friend class Map;
    MapItem(Map &map);

    /** Private constructor; items should be made by tdb::Map **/
    MapItem(Map &map, void *key, tiledb_datatype_t type, size_t size) : MapItem(map) {
      tiledb_kv_item_t *p;
      const Context &ctx = get_context();
      ctx.handle_error(tiledb_kv_item_create(ctx, &p));
      ctx.handle_error(tiledb_kv_item_set_key(ctx, p, key, type, size));
      item_ = std::shared_ptr<tiledb_kv_item_t>(p, deleter_);
    }

    MapItem(Map &map, tiledb_kv_item_t **item) : MapItem(map) {
      item_ = std::shared_ptr<tiledb_kv_item_t>(*item, deleter_);
      *item = nullptr;
    }



    Attribute get_attribute(const std::string &name) const;

    const Context &get_context() const;

    std::reference_wrapper<Map> map_;
    std::shared_ptr<tiledb_kv_item_t> item_;
    impl::Deleter deleter_;
  };

  namespace impl {
    struct map_item_proxy {
      map_item_proxy(const std::string &attr, MapItem &item) : attr(attr), item(item) {}

      template<typename T>
      void set(const T &val) {
        item.set<T>(attr, val);
      }

      template <typename V, typename T = typename V::value_type>
      V get() const {
        return item.get<T, V>(attr);
      }

      template <typename T>
      typename std::enable_if<std::is_fundamental<T>::value, T>::type
      get() const {
        return item.get_single<T>(attr);
      }

      template <typename T>
      void operator=(const T &val) {
        set(val);
      }

      template <typename V, typename = typename V::value_type>
      operator V() {
        return get<V>();
      }

      const std::string attr;
      MapItem &item;
    };

  }

}

#endif //TILEDB_TILEDB_CPP_API_MAP_ITEM_H
