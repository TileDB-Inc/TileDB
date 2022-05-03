/**
 * @file tiledb/sm/array/test/unit_consistency.cc
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
 * This file defines common test functions for class Array and its supporting
 * classes.
 */

#ifndef TILEDB_UNIT_CONSISTENCY_H
#define TILEDB_UNIT_CONSISTENCY_H

#include <catch.hpp>
#include <iostream>

#include "../array.h"
#include "../consistency.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

class ConsistencySentry;

/* ************************************* */
/*  class WhiteboxConsistencyController  */
/* ************************************* */

namespace tiledb {
namespace sm {

using entry_type = std::multimap<const URI, Array&>::const_iterator;

class WhiteboxConsistencyController : public ConsistencyController {
 public:
  WhiteboxConsistencyController() = default;
  ~WhiteboxConsistencyController() = default;

  entry_type register_array(const URI uri, Array& array) {
    return ConsistencyController::register_array(uri, array);
  }

  void deregister_array(entry_type entry) {
    ConsistencyController::deregister_array(entry);
  }

  ConsistencySentry make_sentry(const URI uri, Array& array) {
    return ConsistencyController::make_sentry(uri, array);
  }

  bool contains(const URI uri) {
    return ConsistencyController::contains(uri);
  }

  bool is_element_of(URI uri1, URI uri2) {
    return ConsistencyController::is_element_of(uri1, uri2);
  }

  size_t registry_size() {
    return array_registry_.size();
  }

  tdb_unique_ptr<Array> create_array(const URI uri) {
    ThreadPool* tp_cpu(0);
    ThreadPool* tp_io(0);
    stats::Stats stats("");
    StorageManager sm(tp_cpu, tp_io, &stats, make_shared<Logger>(HERE(), ""));
    return tdb_unique_ptr<Array>(new Array{uri, &sm, *this});
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_UNIT_CONSISTENCY_H
