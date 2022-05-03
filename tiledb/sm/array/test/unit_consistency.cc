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
 * This file tests the Consistency class
 */

#include <catch.hpp>
#include <iostream>

#include "unit_consistency.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

TEST_CASE("WhiteboxConsistencyController::ConsistencyController") {
  // Create a Whitebox instance
  WhiteboxConsistencyController x;
}

TEST_CASE(
    "WhiteboxConsistencyController: Empty registry",
    "[ConsistencyController][empty_registry]") {
  // Create a Whitebox instance
  WhiteboxConsistencyController x;

  // Check that nothing is registered
  REQUIRE(x.registry_size() == 0);
}

TEST_CASE(
    "WhiteboxConsistencyController: Null arrays, direct registration",
    "[ConsistencyController][null_array][direct_registration]") {
  // Create a Whitebox instance with nothing registered
  WhiteboxConsistencyController x;
  REQUIRE(x.registry_size() == 0);

  // Register an empty URI
  const URI uri_empty = URI();
  Array* array = nullptr;
  entry_type iter_empty = x.register_array(uri_empty, *array);
  REQUIRE(x.registry_size() == 1);

  // Register a non-empty URI
  const URI uri_non_empty = URI("non-empty");
  entry_type iter_non_empty = x.register_array(uri_non_empty, *array);

  // Check registration
  REQUIRE(x.registry_size() == 2);
  REQUIRE(x.contains(uri_empty) == true);
  REQUIRE(x.contains(uri_non_empty) == true);
  REQUIRE(x.is_element_of(uri_empty, uri_non_empty) == false);

  const URI uri_not_contained = URI("not_contained");
  REQUIRE(x.contains(uri_not_contained) == false);
  REQUIRE(x.is_element_of(uri_empty, uri_not_contained) == false);

  // Deregister uri_empty
  x.deregister_array(iter_empty);
  REQUIRE(x.registry_size() == 1);

  // Check contents of registry
  REQUIRE(x.contains(uri_empty) == false);
  REQUIRE(x.contains(uri_non_empty) == true);

  // Re-register uri_empty and check registry
  iter_empty = x.register_array(uri_empty, *array);
  REQUIRE(x.registry_size() == 2);
  REQUIRE(x.contains(uri_empty) == true);
  REQUIRE(x.contains(uri_non_empty) == true);
  REQUIRE(x.is_element_of(uri_empty, uri_non_empty) == false);

  // Deregister both entries
  x.deregister_array(iter_empty);
  REQUIRE(x.contains(uri_empty) == false);
  x.deregister_array(iter_non_empty);
  REQUIRE(x.contains(uri_non_empty) == false);
  REQUIRE(x.registry_size() == 0);
}

TEST_CASE(
    "WhiteboxConsistencyController: Sentry null arrays",
    "[ConsistencyController][null_array][sentry]") {
  // Create a Whitebox instance with nothing registered
  WhiteboxConsistencyController x;
  REQUIRE(x.registry_size() == 0);

  // Register an empty URI
  const URI uri_empty = URI();
  Array* array = nullptr;
  tiledb::sm::ConsistencySentry sentry_uri_empty =
      x.make_sentry(uri_empty, *array);
  REQUIRE(x.registry_size() == 1);

  // Register a non-empty URI
  const URI uri_non_empty = URI("non-empty");
  tiledb::sm::ConsistencySentry sentry_uri_non_empty =
      x.make_sentry(uri_non_empty, *array);

  // Check registration
  REQUIRE(x.registry_size() == 2);
  REQUIRE(x.contains(uri_empty) == true);
  REQUIRE(x.contains(uri_non_empty) == true);
  REQUIRE(x.is_element_of(uri_empty, uri_non_empty) == false);

  const URI uri_not_contained = URI("not_contained");
  REQUIRE(x.contains(uri_not_contained) == false);
}

TEST_CASE(
    "WhiteboxConsistencyController: Single array",
    "[ConsistencyController][array][single]") {
  WhiteboxConsistencyController x;
  const URI uri_empty = URI("");

  // Register array
  tdb_unique_ptr<Array> array_empty = x.create_array(uri_empty);
  REQUIRE(x.registry_size() == 1);
  REQUIRE(x.contains(uri_empty) == true);
  REQUIRE(x.is_element_of(uri_empty, uri_empty) == true);

  // Deregister array
  array_empty.reset();
  REQUIRE(x.registry_size() == 0);
  REQUIRE(x.contains(uri_empty) == false);
}

TEST_CASE(
    "WhiteboxConsistencyController: Vector of arrays",
    "[ConsistencyController][array][vector]") {
  WhiteboxConsistencyController x;
  std::vector<tdb_unique_ptr<Array>> arrays;
  std::vector<URI> uris = {
      URI(""), URI(""), URI("non_empty"), URI("non_empty")};

  // Register arrays
  size_t count = 0;
  for (auto uri : uris) {
    arrays.insert(arrays.begin(), x.create_array(uri));
    count++;
    REQUIRE(x.registry_size() == count);
    REQUIRE(x.contains(uri) == true);
    if (count % 2 == 0)
      REQUIRE(x.is_element_of(uri, uris[count - 1]) == true);
  }

  // Deregister arrays
  for (auto a = arrays.end(); a == arrays.begin(); --a) {
    arrays.pop_back();
    count--;
    REQUIRE(x.contains(uris[count]) == false);
    REQUIRE(x.registry_size() == count);
  }
}