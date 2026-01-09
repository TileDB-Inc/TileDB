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

TEST_CASE("REST capabilities endpoint", "[rest][capabilities]") {
  using tiledb::sm::RestCapabilities;
  tiledb::test::VFSTestSetup vfs_test_setup;
  if (!vfs_test_setup.is_rest()) {
    return;
  }

  tiledb::Config config;
  std::string serialization_format = GENERATE("JSON", "CAPNP");
  config["rest.server_serialization_format"] = serialization_format;

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
    // Check on construction the capabilities are not initialized.
    REQUIRE(!rest_client.rest_capabilities_detected());
    auto actual_capabilities = rest_client.get_capabilities_from_rest();

    if (rest_client.get_capabilities_from_rest().legacy_) {
      REQUIRE(RestCapabilities(nullopt, nullopt, true) == actual_capabilities);
    } else {
      // We don't know what core version to expect on TileDB-Server.
      // TileDB-Server supports clients >= 2.28.0, we can check minimum version.
      REQUIRE(
          RestCapabilities::TileDBVersion(2, 28, 0) ==
          actual_capabilities.rest_minimum_supported_version_);
      REQUIRE(actual_capabilities.legacy_ == false);
      REQUIRE(actual_capabilities.detected_);
    }
    // We should have detected REST capabilities either legacy or TileDB-Server.
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

    REQUIRE(!rest_client.rest_capabilities_detected());
    // Should submit capabilities request and return the TileDBVersion result.
    auto min_tiledb_version =
        rest_client.rest_minimum_supported_tiledb_version();

    // After the access above, RestCapabilities has been initialized.
    REQUIRE(rest_client.rest_capabilities_detected());
    // Check min version since we don't know what core version is on the server.
    std::optional<RestCapabilities::TileDBVersion> expected_min_tiledb;
    if (rest_client.get_capabilities_from_rest().legacy_) {
      expected_min_tiledb = nullopt;
    } else {
      expected_min_tiledb = {2, 28, 0};
    }

    REQUIRE(min_tiledb_version == expected_min_tiledb);

    // Check there was only one request sent.
    auto match_request_count = Catch::Matchers::ContainsSubstring(
        "\"capabilities_stats.RestClient.rest_http_requests\": 1");
    REQUIRE_THAT(stats.dump(0, 0), match_request_count);

    // These access attempts should not submit additional requests.
    REQUIRE(
        rest_client.get_capabilities_from_rest()
            .rest_minimum_supported_version_ == expected_min_tiledb);
    REQUIRE(rest_client.get_capabilities_from_rest().detected_);

    // Validate that repeated access does not submit any additional requests.
    REQUIRE_THAT(stats.dump(0, 0), match_request_count);
  }
}

TEST_CASE(
    "Invalid rest.server_address configuration",
    "[rest][server_address][config]") {
  tiledb::test::VFSTestSetup vfs_test_setup;
  if (!vfs_test_setup.is_rest()) {
    SKIP("This test requires a valid REST connection");
  }
  auto ctx = vfs_test_setup.ctx();
  auto config = ctx.config();
  config["rest.server_address"] = "(http://127.0.0.1:8181),";
  vfs_test_setup.update_config(config.ptr().get());
  ctx = vfs_test_setup.ctx();
  // Send any request to REST to validate we throw as expected.
  CHECK_THROWS_WITH(
      tiledb::Object::object(ctx, "tiledb://workspace/teamspace/array_name"),
      Catch::Matchers::ContainsSubstring("URL rjected"));
}
