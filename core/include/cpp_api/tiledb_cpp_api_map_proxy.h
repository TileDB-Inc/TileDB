/**
 * @file  tiledb_cpp_api_map_proxy.h
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
 * This file declares the C++ API for the TileDB Map Item proxies.
 */

#ifndef TILEDB_TILEDB_CPP_API_MAP_PROXY_H_H
#define TILEDB_TILEDB_CPP_API_MAP_PROXY_H_H

#include "tiledb_cpp_api_map_item.h"

namespace tdb {
  namespace impl {

    /**
     * Proxy class for multi-attribute set and get.
     * The class facillitates tuple unrolling, and is
     * equivilant to setting one attribute at a time.
     * After assignment, the item is added to the
     * underlying map.
     **/
    class MultiMapItemProxy {
      /** Used to compiler can resolve func without user typing .get<....>() **/
      template<typename T> struct type_tag{};

    public:
      MultiMapItemProxy(const std::vector<std::string> &attrs, MapItem &item)
      : attrs(attrs), item(item) {}

      /** Get multiple attributes **/
      template <typename T>
      T get() const {
        if (attrs.size() != std::tuple_size<T>::value) {
          throw TileDBError("Attribute list size does not match tuple length.");
        }
        return get(type_tag<T>{});
      }

      /** Set the attributes **/
      template <typename T>
      void set(const T &vals) {
        if (attrs.size() != std::tuple_size<T>::value) {
          throw TileDBError("Attribute list size does not match tuple length.");
        }
        iter_tuple(vals);
        add_to_map();
      }

      /** Implicit cast to a tuple. **/
      template<typename T>
      operator T() const {
        return get<T>();
      }

      /** Set the attributes with a tuple **/
      template<typename T>
      void operator=(const T &vals) {
        return set<T>(vals);
      }

    private:

      /** Iterate over a tuple. Set version (non const) **/
      /** Base case. Do nothing and terminate recursion. **/
      template<std::size_t I = 0, typename... Tp>
      inline typename std::enable_if<I == sizeof...(Tp), void>::type
      iter_tuple(const std::tuple<Tp...>&){}

      template<std::size_t I = 0, typename... Tp>
      inline typename std::enable_if<I < sizeof...(Tp), void>::type
      iter_tuple(const std::tuple<Tp...> &t) {
        item.set(attrs[I], std::get<I>(t));
        iter_tuple<I + 1, Tp...>(t);
      }

      /** Iterate over a tuple. Get version (const) **/
      /** Base case. Do nothing and terminate recursion. **/
      template<std::size_t I = 0, typename... Tp>
      inline typename std::enable_if<I == sizeof...(Tp), void>::type
      iter_tuple(std::tuple<Tp...>&) const {}

      template<std::size_t I = 0, typename... Tp>
      inline typename std::enable_if<I < sizeof...(Tp), void>::type
      iter_tuple(std::tuple<Tp...> &t) const {
        std::get<I>(t) = item.get<typename std::tuple_element<I, std::tuple<Tp...>>::type>(attrs[I]);
        iter_tuple<I + 1, Tp...>(t);
      }

      /** Get multiple values into tuple. **/
      template <typename... Tp>
      std::tuple<Tp...> get(type_tag<std::tuple<Tp...>>) const {
        if (attrs.size() != sizeof...(Tp)) {
          throw TileDBError("Attribute list size does not match provided values.");
        }
        std::tuple<Tp...> ret;
        iter_tuple(ret);
        return ret;
      }

      /** Keyed attributes **/
      const std::vector<std::string> &attrs;

      /** Item that created proxy. **/
      MapItem &item;

      /** Add to underlying map, if associated with one. Otherwise pass. **/
      bool add_to_map() const;
    };

    /**
     * Proxy struct to set a single value with operator[].
     * If bound to a map, the item will be added after assignment.
     **/
    class MapItemProxy {
    public:
      /** Create a proxy for the given attribute and underlying MapItem **/
      MapItemProxy(const std::string &attr, MapItem &item) : attr(attr), item(item) {}

      /** Set the value **/
      template<typename T>
      void set(const T &val) {
        item.set<T>(attr, val);
        add_to_map();
      }

      /** Get the value, fundamental type. **/
      template <typename T>
      T get() const {
        return item.get<T>(attr);
      }

      /** Set value with operator= **/
      template <typename T>
      void operator=(const T &val) {
        set(val);
        add_to_map();
      }

      /** Implicit cast **/
      template <typename T>
      operator T() {
        return get<T>();
      }

      /** Bound attribute name **/
      const std::string attr;

      /** Underlying Item **/
      MapItem &item;

    private:

      /** Add to underlying map, if associated with one. Otherwise pass. **/
      bool add_to_map() const;

    };

  }
}

#endif //TILEDB_TILEDB_CPP_API_MAP_PROXY_H_H
