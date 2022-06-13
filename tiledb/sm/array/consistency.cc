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

namespace tiledb::sm {

ConsistencySentry::ConsistencySentry(
    ConsistencyController& registry, ConsistencyController::entry_type entry)
    : parent_(registry)
    , entry_(entry) {
}

ConsistencySentry::ConsistencySentry(ConsistencySentry&& x)
    : parent_(x.parent_)
    , entry_(x.entry_) {
  x.entry_ = parent_.array_registry_.end();
}

ConsistencySentry::~ConsistencySentry() {
  if (entry_ != parent_.array_registry_.end()) {
    parent_.deregister_array(entry_);
  }
}

ConsistencySentry ConsistencyController::make_sentry(
    const URI uri, Array& array) {
  return ConsistencySentry{*this, register_array(uri, array)};
}

ConsistencyController::entry_type ConsistencyController::register_array(
    const URI uri, Array& array) {
  if (uri.empty()) {
    throw std::runtime_error(
        "[ConsistencyController::register_array] URI cannot be empty.");
  }
  std::lock_guard<std::mutex> lock(mtx_);
  return array_registry_.insert({uri, array});
}

void ConsistencyController::deregister_array(
    ConsistencyController::entry_type entry) {
  std::lock_guard<std::mutex> lock(mtx_);
  array_registry_.erase(entry);
}

bool ConsistencyController::is_open(const URI uri, Array& array) {
  for (auto iter : array_registry_) {
    if (iter.first == uri && (&iter.second) == &array)
      return true;
  }
  return false;
}

bool ConsistencyController::is_open(const URI uri) {
  for (auto iter : array_registry_) {
    if (iter.first == uri)
      return true;
  }
  return false;
}

bool ConsistencyController::is_element_of(
    const URI uri, const URI intersecting_uri) {
  std::string prefix = uri.to_string().substr(
      0, std::string(uri.c_str()).size() - (uri.last_path_part()).size());

  std::string intersecting_prefix = intersecting_uri.to_string().substr(
      0,
      std::string(intersecting_uri.c_str()).size() -
          (intersecting_uri.last_path_part()).size());

  return (prefix == intersecting_prefix);
}

size_t ConsistencyController::registry_size() {
  return array_registry_.size();
}

}  // namespace tiledb::sm
