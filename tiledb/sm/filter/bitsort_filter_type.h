/**
 * @file bitsort_filter_type.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file defines the BitSortFilterMetadataType type.
 * BitSortFilterMetadataType is used to pass auxiliary information to the
 * bitsort filter from either the writer or the reader. We send a reference
 * to both a vector of dimension tiles (that we plan to modify in the bitsort
 * filter), and a reference to the domain, which will give us the dimension
 * information.
 *
 */

#include <functional>
#include <utility>
#include <vector>

#ifndef TILEDB_BITSORT_FILTER_TYPE_H
#define TILEDB_BITSORT_FILTER_TYPE_H

namespace tiledb {
namespace sm {

class Tile;
class GlobalCmpQB;
class HilbertCmpQB;

class BitSortFilterMetadataType {
 public:
  BitSortFilterMetadataType() : dim_tiles_(nullptr), comparator_(nullptr) {}

  BitSortFilterMetadataType(
      std::vector<Tile*>* dim_tiles,
      std::function<bool(const uint64_t&, const uint64_t&)>* comparator)
      : dim_tiles_(dim_tiles)
      , comparator_(comparator) {
  }

  std::vector<Tile*>& dim_tiles() {
    return *dim_tiles_;
  }

  std::function<bool(const uint64_t&, const uint64_t&)>& comparator() {
    return *comparator_;
  }

 private:
  std::vector<Tile*>* dim_tiles_;
  std::function<bool(const uint64_t&, const uint64_t&)>* comparator_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_BITSORT_FILTER_TYPE_H