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

size_t ConsistencyController::registry_size() {
  return array_registry_.size();
}

bool ConsistencyController::contains(const URI uri) {
  for (entry_type iter = array_registry_.begin(); iter != array_registry_.end();
       iter++) {
    if (iter->first == uri)
      return true;
  }
  return false;
}

/* TODO: Finish; this currently returns
  `contains(registered_uri) || contains(element_uri)`. */
bool ConsistencyController::is_element_of(
    const URI registered_uri, const URI element_uri) {
  // check if registry contains 1 or 2
  // user error allowed, if contains 2 call is_element_of(2, 1)

  bool has1 = contains(registered_uri);
  if (!has1) {
    bool has2 = contains(element_uri);
    if (has2)
      return is_element_of(element_uri, registered_uri);
  } else {
    // for now just return true
    return true;
  }

  return false;
}

}  // namespace sm
}  // namespace tiledb
