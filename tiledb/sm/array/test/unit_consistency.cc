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

#include <tiledb/sm/misc/tdb_catch.h>
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
    "WhiteboxConsistencyController: Null array, direct registration",
    "[ConsistencyController][null_array][direct_registration]") {
  // Create a Whitebox instance with nothing registered
  WhiteboxConsistencyController x;
  REQUIRE(x.registry_size() == 0);

  // Try to register an empty URI
  const URI uri_empty = URI();
  Array* array = nullptr;

  REQUIRE_THROWS_AS(x.register_array(uri_empty, *array), std::exception);
  REQUIRE(x.registry_size() == 0);
  REQUIRE(x.is_open(uri_empty) == false);

  // Register a non-empty URI
  const URI uri = URI("whitebox_null_array_direct_registration");
  entry_type iter = x.register_array(uri, *array);

  // Check registration
  REQUIRE(x.registry_size() == 1);
  REQUIRE(x.is_open(uri) == true);
  REQUIRE(tiledb::sm::utils::parse::is_element_of(uri_empty, uri) == false);

  // Ensure a non-registered URI is not registered
  const URI uri_not_contained = URI("not_contained");
  REQUIRE(x.is_open(uri_not_contained) == false);
  REQUIRE(
      tiledb::sm::utils::parse::is_element_of(uri_empty, uri_not_contained) ==
      false);

  // Deregister uri
  x.deregister_array(iter);
  REQUIRE(x.registry_size() == 0);
  REQUIRE(x.is_open(uri) == false);

  // Re-register uri and check registry
  iter = x.register_array(uri, *array);
  REQUIRE(x.registry_size() == 1);
  REQUIRE(x.is_open(uri) == true);
  REQUIRE(tiledb::sm::utils::parse::is_element_of(uri_empty, uri) == false);

  // Deregister
  x.deregister_array(iter);
  REQUIRE(x.is_open(uri) == false);
  REQUIRE(x.registry_size() == 0);
}

TEST_CASE(
    "WhiteboxConsistencyController: Null array, Sentry registration",
    "[ConsistencyController][null_array][sentry]") {
  WhiteboxConsistencyController x;

  // Try to register an empty URI
  const URI uri_empty = URI();
  Array* array = nullptr;
  REQUIRE_THROWS_AS(x.make_sentry(uri_empty, *array), std::exception);
  REQUIRE(x.registry_size() == 0);
  REQUIRE(x.is_open(uri_empty) == false);

  // Register a non-empty URI
  const URI uri = URI("whitebox_null_array_sentry_registration");
  tiledb::sm::ConsistencySentry sentry_uri = x.make_sentry(uri, *array);

  // Check registration
  REQUIRE(x.registry_size() == 1);
  REQUIRE(x.is_open(uri_empty) == false);
  REQUIRE(x.is_open(uri) == true);
  REQUIRE(tiledb::sm::utils::parse::is_element_of(uri_empty, uri) == false);
}

TEST_CASE(
    "WhiteboxConsistencyController: Optional sentry, move constructors",
    "[ConsistencyController][sentry][optional]") {
  // Create a Whitebox instance with nothing registered
  WhiteboxConsistencyController x;
  REQUIRE(x.registry_size() == 0);

  // Register a URI
  const URI uri = URI("whitebox_sentry");
  Array* array = nullptr;
  tiledb::sm::ConsistencySentry sentry = x.make_sentry(uri, *array);
  REQUIRE(x.registry_size() == 1);
  REQUIRE(x.is_open(uri) == true);

  // Test move constructor
  tiledb::sm::ConsistencySentry sentry_moved(std::move(sentry));

  // Test move constructor with optional ConsistencySentry
  std::optional<tiledb::sm::ConsistencySentry> sentry_optional_moved(
      std::move(sentry_moved));

  REQUIRE(x.registry_size() == 1);
  REQUIRE(x.is_open(uri) == true);

  // Create an optional Sentry
  const URI uri_optional = URI("whitebox_optional_sentry");
  std::optional<tiledb::sm::ConsistencySentry> sentry_optional(
      x.make_sentry(uri_optional, *array));
  REQUIRE(x.registry_size() == 2);
  REQUIRE(x.is_open(uri) == true);
}

TEST_CASE(
    "WhiteboxConsistencyController: Single array",
    "[ConsistencyController][array][single]") {
  WhiteboxConsistencyController x;
  const URI uri = URI("whitebox_single_array");

  // Create a StorageManager
  Config config;
  ThreadPool tp_cpu(1);
  ThreadPool tp_io(1);
  stats::Stats stats("");
  StorageManager sm(&tp_cpu, &tp_io, &stats, make_shared<Logger>(HERE(), ""));
  Status st = sm.init(config);
  REQUIRE(st.ok());

  // Register array
  tdb_unique_ptr<Array> array = x.open_array(uri, &sm);
  REQUIRE(x.registry_size() == 1);
  REQUIRE(x.is_open(uri) == true);
  REQUIRE(tiledb::sm::utils::parse::is_element_of(uri, uri) == true);

  // Deregister array
  array.get()->close();
  REQUIRE(x.registry_size() == 0);
  REQUIRE(x.is_open(uri) == false);

  // Clean up
  sm.vfs()->remove_dir(uri);
}

TEST_CASE(
    "WhiteboxConsistencyController: Vector of arrays",
    "[ConsistencyController][array][vector]") {
  WhiteboxConsistencyController x;

  // Create a StorageManager
  Config config;
  ThreadPool tp_cpu(1);
  ThreadPool tp_io(1);
  stats::Stats stats("");
  StorageManager sm(&tp_cpu, &tp_io, &stats, make_shared<Logger>(HERE(), ""));
  Status st = sm.init(config);
  REQUIRE(st.ok());

  std::vector<tdb_unique_ptr<Array>> arrays;
  std::vector<URI> uris = {URI("whitebox_array_vector_1"),
                           URI("whitebox_array_vector_2")};

  // Register arrays
  size_t count = 0;
  for (auto uri : uris) {
    arrays.insert(arrays.begin(), x.open_array(uri, &sm));
    count++;
    REQUIRE(x.registry_size() == count);
    REQUIRE(x.is_open(uri) == true);
    if (count % 2 == 0)
      REQUIRE(
          tiledb::sm::utils::parse::is_element_of(uri, uris[count - 1]) ==
          true);
  }

  // Deregister arrays
  for (auto a = arrays.end(); a == arrays.begin(); --a) {
    a->get()->close();
    count--;
    REQUIRE(x.is_open(uris[count]) == false);
    REQUIRE(x.registry_size() == count);
  }

  // Clean up
  for (auto uri : uris) {
    sm.vfs()->remove_dir(uri);
  }
}