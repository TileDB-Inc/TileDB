/**
 * @file  tiledb_cpp_api_map_proxy.cc
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

#include "map_proxy.h"
#include "map.h"

namespace tiledb {
impl::MapItemProxy MapItem::operator[](const std::string& attr) {
  return impl::MapItemProxy(attr, *this);
}

impl::MultiMapItemProxy MapItem::operator[](
    const std::vector<std::string>& attrs) {
  return impl::MultiMapItemProxy(attrs, *this);
}

bool impl::MapItemProxy::add_to_map() const {
  if (item.map_ != nullptr) {
    item.map_->add_item(item);
    return true;
  }
  return false;
}

bool impl::MultiMapItemProxy::add_to_map() const {
  if (item.map_ != nullptr) {
    item.map_->add_item(item);
    return true;
  }
  return false;
}
}  // namespace tiledb
