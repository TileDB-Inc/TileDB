/**
 * @file   consistency.cc
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
 * This file implements classes ConsistencyController and ConsistencySentry.
 */

#include <iostream>

#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array/consistency.h"

using namespace tiledb::common;

namespace tiledb::sm {

ConsistencySentry::ConsistencySentry(
    ConsistencyController& registry, ConsistencyController::entry_type entry)
    : parent_(registry)
    , entry_(entry) {
    std::cerr << "SENTRY CONSTRUCTOR" << std::endl;
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
    const URI uri, Array& array, const QueryType query_type) {
  std::cerr << "MAKING SENTRY!" << std::endl;
  return ConsistencySentry{*this, register_array(uri, array, query_type)};
}

void ConsistencyController::can_lock() {
  if(mtx_.try_lock()) {
    std::cerr << "can lock" << std::endl;
    mtx_.unlock();
  } else {
    std::cerr << "nope can't lock" << std::endl;
  }
}

ConsistencyController::entry_type ConsistencyController::register_array(
    const URI uri, Array& array, const QueryType query_type) {
  if (uri.empty()) {
    throw std::runtime_error(
        "[ConsistencyController::register_array] URI cannot be empty.");
  }

  std::cerr << ">>>>>> REGISTER ARRAY" << std::endl;

  if(mtx_.try_lock()) {
    std::cerr << "locked mutex test" << std::endl;
    mtx_.unlock();
  } else {
    std::cerr << "Unable to lock mutext" << std::endl;
  }

  std::cerr << "LOCKING FOR ARRAY REGISTRATION: " << this << std::endl;
  std::lock_guard<std::mutex> lock(mtx_);
  std::cerr << "LOCK ACQUIRED" << std::endl;
  auto iter = array_registry_.find(uri);
  if (iter != array_registry_.end()) {
    if (query_type == QueryType::MODIFY_EXCLUSIVE) {
      throw std::runtime_error(
          "[ConsistencyController::register_array] Array already open; must "
          "close array before opening for exclusive modification.");
    } else {
      if (std::get<1>(iter->second) == QueryType::MODIFY_EXCLUSIVE) {
        throw std::runtime_error(
            "[ConsistencyController::register_array] Must close array opened "
            "for exclusive modification before opening an array at the same "
            "address.");
      }
    }
  }

  auto ret = array_registry_.insert({uri, array_entry(array, query_type)});
  std::cerr << "<<<<<<<< REGISTER ALMOST DONE" << std::endl;
  return ret;

  //return array_registry_.insert({uri, array_entry(array, query_type)});
}

void ConsistencyController::deregister_array(
    ConsistencyController::entry_type entry) {
  std::cerr << "locking to deregister" << std::endl;
  std::lock_guard<std::mutex> lock(mtx_);
  array_registry_.erase(entry);
}

bool ConsistencyController::is_open(const URI uri) {
  if (array_registry_.find(uri) != array_registry_.end()) {
    return true;
  }
  return false;
}
}  // namespace tiledb::sm
