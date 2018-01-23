/**
 * @file  tiledb_cpp_api_map_iter.h
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
 * This file declares the C++ API for the TileDB Map Iter object.
 */

#ifndef TILEDB_TILEDB_CPP_API_MAP_ITER_H
#define TILEDB_TILEDB_CPP_API_MAP_ITER_H

#include "tiledb_cpp_api_type.h"
#include "tiledb_cpp_api_map_schema.h"
#include "tiledb_cpp_api_map_item.h"
#include "tiledb.h"

#include <iterator>
#include <functional>

namespace tdb {
namespace impl {

  /** Iterate over items in a map. **/
  class MapIter : public std::iterator<std::forward_iterator_tag, MapItem> {
  public:
    MapIter(Map &map, bool end=false);
    MapIter(const MapIter&) = delete;
    MapIter(MapIter&&) = default;
    MapIter &operator=(const MapIter&) = delete;
    MapIter &operator=(MapIter&&) = default;

    /** Init iter. This must manually be invoked. **/
    void init();

    /** Flush on iterator destruction. **/
    ~MapIter();

    /** Only iterate over keys with some type **/
    template<typename T>
    typename std::enable_if<std::is_fundamental<T>::value, void>::type
    limit_key_type() {
      limit_type_ = true;
      only_single_ = true;
      type_ = impl::type_from_native<T>::type::tiledb_datatype;
    }

    template<typename T, typename = typename T::value_type>
    void limit_key_type() {
      limit_type_ = true;
      only_single_ = false;
      type_ = impl::type_from_native<typename T::value_type>::type::tiledb_datatype;
    }

    /** Iterators are only equal when both are end. **/
    bool operator==(const MapIter &o) {
      return done_ == o.done_;
    }

    bool operator!=(const MapIter &o) {
      return done_ != o.done_;
    }

    MapItem &operator*() const {
      return *item_;
    }

    MapItem *operator->() const {
      return item_.get();
    }

    MapIter &operator++();

  private:

    /** Base map. **/
    Map *map_;
    Deleter deleter_;

    /** Current item **/
    std::unique_ptr<MapItem> item_;

    /** TileDB iterator object **/
    std::shared_ptr<tiledb_kv_iter_t> iter_;

    /** Whether the iterator has reached the end **/
    int done_;

    bool limit_type_ = false;
    tiledb_datatype_t type_;
    bool only_single_;
  };

}
}

#endif //TILEDB_TILEDB_CPP_API_MAP_ITER_H
