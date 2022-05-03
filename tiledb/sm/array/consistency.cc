/**
 * @file   consistency.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file implements classes ConsistencyController and ConsistencySentry.
 */

#include <iostream>

#include "tiledb/sm/array/consistency.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

ConsistencySentry::ConsistencySentry(
    ConsistencyController& registry, entry_type entry)
    : array_controller_registry_(registry)
    , entry_(entry) {
}

ConsistencySentry::~ConsistencySentry() {
  array_controller_registry_.deregister_array(entry_);
}

/* ********************************* */
/*                API                */
/* ********************************* */

ConsistencySentry ConsistencyController::make_sentry(
    const URI uri, Array& array) {
  return ConsistencySentry{*this, register_array(uri, array)};
}

entry_type ConsistencyController::register_array(const URI uri, Array& array) {
  std::lock_guard<std::mutex> lock(mtx_);
  entry_type iter = array_registry_.insert({uri, array});

  return iter;
}

void ConsistencyController::deregister_array(entry_type entry) {
  std::lock_guard<std::mutex> lock(mtx_);
  array_registry_.erase(entry);
}

bool ConsistencyController::contains(const URI uri) {
  for (entry_type iter = array_registry_.begin(); iter != array_registry_.end();
       iter++) {
    if (iter->first == uri)
      return true;
  }
  return false;
}

bool ConsistencyController::is_element_of(
    const URI uri, const URI intersecting_uri) {
  std::string prefix =
      std::string(uri.c_str())
          .substr(
              0,
              std::string(uri.c_str()).size() - (uri.last_path_part()).size());

  std::string intersecting_prefix =
      std::string(intersecting_uri.c_str())
          .substr(
              0,
              std::string(intersecting_uri.c_str()).size() -
                  (intersecting_uri.last_path_part()).size());

  return (prefix == intersecting_prefix);
}

}  // namespace sm
}  // namespace tiledb
