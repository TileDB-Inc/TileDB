/**
 * @file   unit-rest-client-remote.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests for RestClientRemote class. This class and the tests in this file are
 * only compiled when TILEDB_SERIALIZATION is enabled.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/rest/rest_client_remote.h"

TEST_CASE("REST capabilities endpoint", "[rest][version]") {
  tiledb::test::VFSTestSetup vfs_test_setup;
  if (!vfs_test_setup.is_rest()) {
    return;
  }

  tiledb::Config config;
  std::string serialization_format = GENERATE("JSON", "CAPNP");
  config["rest.server_serialization_format"] = serialization_format;

  auto expected_version =
      std::make_from_tuple<tiledb::sm::RestCapabilities::TileDBVersion>(
          tiledb::version());
  tiledb::common::ThreadPool tp(1);
  DYNAMIC_SECTION(
      "GET request to retrieve REST tiledb version - "
      << serialization_format) {
    auto rest_client = tiledb::sm::RestClientRemote(
        tiledb::test::g_helper_stats,
        config.ptr()->config(),
        tp,
        *tiledb::test::g_helper_logger(),
        tiledb::test::get_test_memory_tracker());
    tiledb::sm::RestCapabilities expected_capabilities(
        expected_version, {2, 28, 0});
    // Check on construction the capabilities are not initialized.
    REQUIRE(!rest_client.rest_capabilities_detected());
    auto actual_capabilities = rest_client.get_capabilities_from_rest();

    // If we detected a legacy REST server the test was successful but there
    // is no version response from legacy REST to validate.
    if (!actual_capabilities.legacy_) {
      REQUIRE(expected_capabilities == actual_capabilities);
    }
    // We should have detected REST capabilities, either legacy or 3.0.
    REQUIRE(rest_client.rest_capabilities_detected());
  }
  DYNAMIC_SECTION(
      "Initialization of rest_tiledb_version_ on first access - "
      << serialization_format) {
    // Construct enabled Stats for this test to verify http request count.
    auto stats = tiledb::sm::stats::Stats("capabilities_stats");
    auto rest_client = tiledb::sm::RestClientRemote(
        stats,
        config.ptr()->config(),
        tp,
        *tiledb::test::g_helper_logger(),
        tiledb::test::get_test_memory_tracker());
    // Here we don't call `get_capabilities_from_rest`, but instead attempt to
    // first access RestCapabilities directly. The RestClient should submit
    // the GET request and initialize RestCapabilities, returning the result.
    REQUIRE(!rest_client.rest_capabilities_detected());

    // Should submit capabilities request and return the TileDBVersion result.
    auto tiledb_version = rest_client.rest_tiledb_version();
    // After the access above, RestCapabilities has been initialized.
    REQUIRE(rest_client.rest_capabilities_detected());
    // Validate the TileDB version if it was provided in the response.
    if (!rest_client.get_capabilities_from_rest().legacy_) {
      // TODO: Should we use some default version for legacy?
      //    This creates an edge case where rest_tiledb_version_ is not
      //    initialized for legacy. That's fine until we start to add checks on
      //    this version, expecting it to be relevant.
      REQUIRE(tiledb_version == expected_version);
    }

    // Check there was only one request sent.
    auto match_request_count = Catch::Matchers::ContainsSubstring(
        "\"capabilities_stats.RestClient.rest_http_requests\": 1");
    REQUIRE_THAT(stats.dump(0, 0), match_request_count);

    // Further access attempts should not submit additional requests.
    auto capabilities = rest_client.get_capabilities_from_rest();
    if (!capabilities.legacy_) {
      REQUIRE(rest_client.rest_tiledb_version() == expected_version);
    }

    // Subsequent access attempts should not submit any additional requests.
    REQUIRE_THAT(stats.dump(0, 0), match_request_count);
  }
}

TEST_CASE("Test getting REST URI components", "[uri]") {
  // TODO: Add more URI combinations, relocate test alongside other URI tests.
  std::string ns = GENERATE("workspace/teamspace", "ws_1234/ts_1234");
  std::string arr = GENERATE(
      "8f039466-6e90-42ea-af53-dc0ba47d00c2",
      "array_name",
      "s3://bucket/arrays/array_name");
  tiledb::sm::URI uri("tiledb://" + ns + "/" + arr);

  std::string array_namespace, array_uri;
  REQUIRE(uri.get_rest_components(&array_namespace, &array_uri, false).ok());

  REQUIRE(array_namespace == ns);
  REQUIRE(array_uri == arr);
}
