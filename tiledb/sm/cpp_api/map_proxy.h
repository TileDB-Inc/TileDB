/**
 * @file  tiledb_cpp_api_map_proxy.h
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
 * This file declares the C++ API for the TileDB Map Item proxies.
 */

#ifndef TILEDB_CPP_API_MAP_PROXY_H
#define TILEDB_CPP_API_MAP_PROXY_H

#include "map_item.h"

#include <tuple>

namespace tiledb {
namespace impl {

/**
 * Proxy class for multi-attribute set and get.
 * The class facilitates tuple unrolling, and is
 * equivalent to setting one attribute at a time.
 * After assignment, the item is added to the
 * underlying map.
 *
 * @details
 * This class should never be constructed explicitly.
 * Instead, it should be used to retrieve the value
 * with the correct type.
 *
 * **Example:**
 *
 * @code{.cpp}
 *   using my_cell_t = std::tuple<int, std::string, std::vector<float>>;
 *
 *   // Implicit conversion
 *   my_cell_t vals_implicit = map[100][{"a1", "a2", "a3"}];
 *
 *   // Explicit conversion
 *   auto vals_explicit = map[100][{"a1", "a2", "a3"}].get<my_cell_t>();
 *
 *   // Defer conversion
 *   auto vals_deferred = map[100][{"a1", "a2", "a3"}];
 *
 *   // vals_deferred is of type MultMapItemProxy, no values
 *   // are fetched yet.
 *
 *   // Writing & flushing map[100] here would change
 *   // the result of below.
 *
 *   auto my_fn = [](my_cell_t cell){ std::cout << std::get<0>(cell); };
 *   my_fn(vals_deferred); // Retrieve values
 *
 *   // Set new values, but does not explicity flush to storage
 *   vals_deferred = std::make_tuple(10, "str", {1.2, 3.2});
 *
 * @endcode
 **/
class TILEDB_EXPORT MultiMapItemProxy {
 public:
  MultiMapItemProxy(const std::vector<std::string>& attrs, MapItem& item)
      : attrs(attrs)
      , item(item) {
  }

  /** Get multiple attributes into an existing tuple **/
  template <typename... T>
  void get(std::tuple<T...>& tp) const {
    if (attrs.size() != sizeof...(T)) {
      throw TileDBError("Attribute list size does not match tuple length.");
    }
    get_tuple<0, T...>(tp);
  }

  /** Get multiple attributes **/
  template <typename... T>
  std::tuple<T...> get() const {
    std::tuple<T...> ret;
    get<T...>(ret);
    return ret;
  }

  /** Set the attributes **/
  template <typename... T>
  void set(const std::tuple<T...>& vals) {
    if (attrs.size() != sizeof...(T)) {
      throw TileDBError("Attribute list size does not match tuple length.");
    }
    set_tuple<0, T...>(vals);
    add_to_map();
  }

  /** Implicit cast to a tuple. **/
  template <typename... T>
  operator std::tuple<T...>() const {
    return get<T...>();
  }

  /** Set the attributes with a tuple **/
  template <typename... T>
  MultiMapItemProxy& operator=(const std::tuple<T...>& vals) {
    set<T...>(vals);
    return *this;
  }

 private:
  /** Iterate over a tuple. Set version (non const) **/
  /** Base case. Do nothing and terminate recursion. **/
  template <std::size_t I = 0, typename... T>
  inline typename std::enable_if<I == sizeof...(T), void>::type set_tuple(
      const std::tuple<T...>&) {
  }

  template <std::size_t I = 0, typename... T>
      inline typename std::enable_if <
      I<sizeof...(T), void>::type set_tuple(const std::tuple<T...>& t) {
    item.set(attrs[I], std::get<I>(t));
    set_tuple<I + 1, T...>(t);
  }

  /** Iterate over a tuple. Get version (const) **/
  /** Base case. Do nothing and terminate recursion. **/
  template <std::size_t I = 0, typename... T>
  inline typename std::enable_if<I == sizeof...(T), void>::type get_tuple(
      std::tuple<T...>&) const {
  }

  template <std::size_t I = 0, typename... T>
      inline typename std::enable_if <
      I<sizeof...(T), void>::type get_tuple(std::tuple<T...>& t) const {
    std::get<I>(t) =
        item.get<typename std::tuple_element<I, std::tuple<T...>>::type>(
            attrs[I]);
    get_tuple<I + 1, T...>(t);
  }

  /** Keyed attributes **/
  const std::vector<std::string>& attrs;

  /** Item that created proxy. **/
  MapItem& item;

  /** Add to underlying map, if associated with one. Otherwise pass. **/
  bool add_to_map() const;
};

/**
 * Proxy struct to set a single value with operator[].
 * If bound to a map, the item will be added after assignment.
 *
 * @details
 * This class should never be constructed explicitly.
 * Instead, it should be used to retrieve the value
 * with the correct type.
 *
 * **Example:**
 *
 * @code{.cpp}
 *   // Implicit conversion
 *   my_cell_t a2_implicit = map[100]["a2"];
 *
 *   // Explicit conversion
 *   auto a2_explicit = map[100]["a2"].get<std::string>();
 *
 *   // Defer conversion
 *   auto a2_deferred = map[100]["a2"];
 *   // a2_deferred is of type MapItemProxy and is symbolic.
 *
 *   // Writing & flushing map[100]["a2"] here would change
 *   // the result of below.
 *
 *   auto my_fn = [](std::string a2){ std::cout << a2; };
 *   my_fn(a2_deferred); // Retrieve value
 *
 *   // Assigning adds to the map, but does not flush
 *   a2_deferred = "new_value";
 *
 * @endcode
 **/
class TILEDB_EXPORT MapItemProxy {
 public:
  /** Create a proxy for the given attribute and underlying MapItem **/
  MapItemProxy(std::string attr, MapItem& item)
      : attr(std::move(attr))
      , item(item) {
  }

  /** Set the value **/
  template <typename T>
  void set(T val) {
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
  MapItemProxy& operator=(const T& val) {
    set(val);
    add_to_map();
    return *this;
  }

  /** Implicit cast **/
  template <typename T>
  operator T() {
    return get<T>();
  }

  /** Bound attribute name **/
  const std::string attr;

  /** Underlying Item **/
  MapItem& item;

 private:
  /** Add to underlying map, if associated with one. Otherwise pass. **/
  bool add_to_map() const;
};

}  // namespace impl
}  // namespace tiledb

#endif  // TILEDB_CPP_API_MAP_PROXY_H
