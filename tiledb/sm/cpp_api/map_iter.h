/**
 * @file  tiledb_cpp_api_map_iter.h
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
 * This file declares the C++ API for the TileDB Map Iter object.
 */

#ifndef TILEDB_CPP_API_MAP_ITER_H
#define TILEDB_CPP_API_MAP_ITER_H

#include "map_item.h"
#include "map_schema.h"
#include "tiledb.h"
#include "type.h"

#include <functional>
#include <iterator>

namespace tiledb {
namespace impl {

/** Iterate over items in a map. **/
class TILEDB_EXPORT MapIter
    : public std::iterator<std::forward_iterator_tag, MapItem> {
 public:
  explicit MapIter(Map& map, bool end = false);
  MapIter(const MapIter&) = delete;
  MapIter(MapIter&&) = default;
  MapIter& operator=(const MapIter&) = delete;
  MapIter& operator=(MapIter&&) = default;

  /** Init iter. This must manually be invoked. **/
  void init();

  /** Flush on iterator destruction. **/
  ~MapIter();

  /**
   * Only iterate over keys with some type. Only keys with the
   * underlying tiledb datatype and num will be returned.
   */
  template <typename T>
  void limit_key_type() {
    using DataT = impl::TypeHandler<T>;
    limit_type_ = true;
    type_ = DataT::tiledb_type;
    num_ = DataT::tiledb_num;
  }

  /** Disable any key filters. */
  void all_keys() {
    limit_type_ = false;
  }

  /** Iterators are only equal when both are end. **/
  bool operator==(const MapIter& o) const {
    return done_ == o.done_;
  }

  bool operator!=(const MapIter& o) const {
    return done_ != o.done_;
  }

  MapItem& operator*() const {
    return *item_;
  }

  MapItem* operator->() const {
    return item_.get();
  }

  MapIter& operator++();

 private:
  /** Base map. **/
  Map* map_;
  Deleter deleter_;

  /** Current item **/
  std::unique_ptr<MapItem> item_;

  /** TileDB iterator object **/
  std::shared_ptr<tiledb_kv_iter_t> iter_;

  /** Whether the iterator has reached the end **/
  int done_;

  /** Settings determining filters for the iterator. **/
  bool limit_type_ = false;
  tiledb_datatype_t type_;
  unsigned num_;
};

}  // namespace impl
}  // namespace tiledb

#endif  // TILEDB_CPP_API_MAP_ITER_H
