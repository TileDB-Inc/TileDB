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
#include "tiledb/sm/filesystem/uri.h"

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
    const URI& uri, Array& array) {
  if (uri.is_invalid())
    throw std::runtime_error("Cannot make sentry, invalid URI.");
  return ConsistencySentry{*this, register_array(uri, array)};
}

entry_type ConsistencyController::register_array(const URI& uri, Array& array) {
  std::lock_guard<std::mutex> lock(mtx_);
  // std::cerr<<uri.c_str()<<std::endl;

  // still fails, uris not all found
  /*const char* base = "";
  entry_type iter;
  if (uri.c_str() == base) {
    iter = array_registry_.begin();
    array_registry_.insert(iter, {uri, array});
  } else {
    iter = array_registry_.insert({uri, array});
  }*/

  entry_type iter = array_registry_.insert({uri, array});

  return iter;
}

void ConsistencyController::test() {
  std::cerr << "TESTING" << std::endl;
}

void ConsistencyController::deregister_array(entry_type entry) {
  std::lock_guard<std::mutex> lock(mtx_);
  array_registry_.erase(entry);
  // std::cerr<<"Deregistration size: "<<array_registry_.size()<<std::endl;
}

size_t ConsistencyController::registry_size() {
  return array_registry_.size();
}

}  // namespace sm
}  // namespace tiledb
