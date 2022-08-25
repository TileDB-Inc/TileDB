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

#include <test/support/tdb_catch.h>
#include <iostream>

#include "../array.h"
#include "../consistency.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/storage_format/uri/parse_uri.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

class ConsistencySentry;

/* ************************************* */
/*  class WhiteboxConsistencyController  */
/* ************************************* */

namespace tiledb::sm {

using entry_type = std::multimap<const URI, Array&>::const_iterator;

class WhiteboxConsistencyController : public ConsistencyController {
 public:
  WhiteboxConsistencyController() = default;
  ~WhiteboxConsistencyController() = default;

  entry_type register_array(
      const URI uri, Array& array, const QueryType& query_type) {
    return ConsistencyController::register_array(uri, array, query_type);
  }

  void deregister_array(entry_type entry) {
    ConsistencyController::deregister_array(entry);
  }

  ConsistencySentry make_sentry(
      const URI uri, Array& array, const QueryType& query_type) {
    return ConsistencyController::make_sentry(uri, array, query_type);
  }

  bool is_open(const URI uri) {
    return ConsistencyController::is_open(uri);
  }

  size_t registry_size() {
    return ConsistencyController::array_registry_.size();
  }

  tdb_unique_ptr<Array> create_array(const URI uri, StorageManager* sm) {
    // Create Domain
    std::vector<uint64_t> dim_dom = {0, 1};
    uint64_t tile_extent = 1;
    shared_ptr<Dimension> dim =
        make_shared<Dimension>(HERE(), std::string("dim"), Datatype::UINT64);
    dim->set_domain(&dim_dom);
    dim->set_tile_extent(&tile_extent);

    std::vector<shared_ptr<Dimension>> dims = {dim};
    shared_ptr<Domain> domain =
        make_shared<Domain>(HERE(), Layout::ROW_MAJOR, dims, Layout::ROW_MAJOR);

    // Create the ArraySchema
    shared_ptr<ArraySchema> schema =
        make_shared<ArraySchema>(HERE(), ArrayType::DENSE);
    schema->set_domain(domain);
    schema->add_attribute(
        make_shared<Attribute>(
            HERE(), std::string("attr"), Datatype::UINT64, false),
        false);
    EncryptionKey key;
    key.set_key(EncryptionType::NO_ENCRYPTION, nullptr, 0);

    // Create the (empty) array on disk.
    Status st = sm->array_create(uri, schema, key);
    if (!st.ok()) {
      throw std::runtime_error(
          "[WhiteboxConsistencyController] Could not create array.");
    }
    tdb_unique_ptr<Array> array(new Array{uri, sm, *this});

    return array;
  }

  tdb_unique_ptr<Array> open_array(const URI uri, StorageManager* sm) {
    // Create array
    tdb_unique_ptr<Array> array{create_array(uri, sm)};

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
