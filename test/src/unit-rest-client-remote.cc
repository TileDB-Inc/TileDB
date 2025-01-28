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
  if (vfs_test_setup.is_rest()) {
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
          expected_version,
          {expected_version.major_,
           expected_version.minor_ - 1,
           expected_version.patch_});
      // Check on construction the capabilities are not initialized.
      REQUIRE(!rest_client.rest_capabilities_detected());
      auto actual_capabilities = rest_client.get_capabilities_from_rest();
      // GET request above initializes RestCapabilities and contents are valid.
      REQUIRE(expected_capabilities == actual_capabilities);
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
      REQUIRE(rest_client.rest_tiledb_version() == expected_version);
      auto match_request_count = Catch::Matchers::ContainsSubstring(
          "\"capabilities_stats.RestClient.rest_http_requests\": 1");
      REQUIRE_THAT(stats.dump(0, 0), match_request_count);

      // After the access above, RestCapabilities has been initialized.
      // Subsequent access attempts should not submit any additional requests.
      REQUIRE(rest_client.rest_capabilities_detected());
      REQUIRE(rest_client.rest_tiledb_version() == expected_version);
      REQUIRE_THAT(stats.dump(0, 0), match_request_count);
    }
  }
}
