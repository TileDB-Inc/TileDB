/**
 * @file tiledb/sm/array/test/unit_consistency.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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

#include <test/support/tdb_catch.h>
#include <iostream>

#include "../array.h"
#include "../consistency.h"
#include "test/support/src/mem_helpers.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/storage_format/uri/parse_uri.h"

using namespace tiledb::common;

class ConsistencySentry;

/* ************************************* */
/*  class WhiteboxConsistencyController  */
/* ************************************* */

namespace tiledb::sm {

using array_entry = std::tuple<Array&, const QueryType>;
using entry_type = std::multimap<const URI, array_entry>::const_iterator;

class WhiteboxConsistencyController : public ConsistencyController {
  shared_ptr<MemoryTracker> memory_tracker_;

 public:
  WhiteboxConsistencyController()
      : memory_tracker_(tiledb::test::get_test_memory_tracker()) {
  }

  ~WhiteboxConsistencyController() = default;

  entry_type register_array(
      const URI uri, Array& array, const QueryType query_type) {
    return ConsistencyController::register_array(uri, array, query_type);
  }

  void deregister_array(entry_type entry) {
    ConsistencyController::deregister_array(entry);
  }

  ConsistencySentry make_sentry(
      const URI uri, Array& array, const QueryType query_type) {
    return ConsistencyController::make_sentry(uri, array, query_type);
  }

  bool is_open(const URI uri) {
    return ConsistencyController::is_open(uri);
  }

  size_t registry_size() {
    return ConsistencyController::array_registry_.size();
  }

  /*
   * Warning: This does not clean up leftovers from previous failed runs.
   * Manual intervention may be required in the build tree.
   */
  tdb_unique_ptr<Array> create_array(
      ContextResources& resources, const URI uri) {
    // Create Domain
    uint64_t dim_dom[2]{0, 1};
    uint64_t tile_extent = 1;
    shared_ptr<Dimension> dim = make_shared<Dimension>(
        HERE(), std::string("dim"), Datatype::UINT64, memory_tracker_);
    dim->set_domain(&dim_dom);
    dim->set_tile_extent(&tile_extent);

    std::vector<shared_ptr<Dimension>> dims = {dim};
    shared_ptr<Domain> domain = make_shared<Domain>(
        HERE(), Layout::ROW_MAJOR, dims, Layout::ROW_MAJOR, memory_tracker_);

    // Create the ArraySchema
    shared_ptr<ArraySchema> schema = make_shared<ArraySchema>(
        HERE(), ArrayType::DENSE, tiledb::test::create_test_memory_tracker());
    schema->set_domain(domain);
    schema->add_attribute(
        make_shared<Attribute>(
            HERE(), std::string("attr"), Datatype::UINT64, false),
        false);
    EncryptionKey key;
    throw_if_not_ok(key.set_key(EncryptionType::NO_ENCRYPTION, nullptr, 0));

    // Create the (empty) array on disk.
    Array::create(resources, uri, schema, key);
    tdb_unique_ptr<Array> array(new Array{resources, uri, *this});

    return array;
  }

  tdb_unique_ptr<Array> open_array(ContextResources& resources, const URI uri) {
    // Create array
    tdb_unique_ptr<Array> array{create_array(resources, uri)};

    // Open the array
    Status st =
        array->open(QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0);
    if (!st.ok()) {
      throw std::runtime_error(
          "[WhiteboxConsistencyController] Could not open array.");
    }
    return array;
  }
};

}  // namespace tiledb::sm

#endif  // TILEDB_UNIT_CONSISTENCY_H
