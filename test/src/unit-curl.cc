/**
 * @file   unit-curl.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2021 TileDB Inc.
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
 * Tests for TileDB curl object.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

// clang-format off
#include "test/support/src/helpers.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/rest/curl.h" // Must be included last to avoid Windows.h
// clang-format on

#include <filesystem>
#include <fstream>

using namespace tiledb::sm;

TEST_CASE(
    "CURL: Test curl's header parsing callback", "[curl][redirect-caching]") {
  // Initialize data that in real life scenario would be initialized by
  // RestClient
  char res_data[] =
      "Location: https://test.url.domain/v1/arrays/testns/test_arr";
  size_t size = 1;
  size_t count = sizeof(res_data);

  HeaderCbData userdata;
  std::string ns_array = "testns:test_arr";
  userdata.uri = &ns_array;
  std::mutex redirect_mtx_;
  std::unordered_map<std::string, std::string> redirect_meta_;
  userdata.redirect_uri_map = &redirect_meta_;
  userdata.redirect_uri_map_lock = &redirect_mtx_;
  userdata.should_cache_redirect = true;

  size_t result = write_header_callback(&res_data, size, count, &userdata);

  // The return value should be equal to size * count
  CHECK(result == size * count);
  // The uri is not mutated inside the callback function
  CHECK(*userdata.uri == ns_array);
  // The redirect map holds a record of ns_array key and redirection url value
  CHECK(userdata.redirect_uri_map->count(ns_array) == 1);
  // The value of this redirection record matches the redirection url
  CHECK(
      userdata.redirect_uri_map->find(ns_array)->second ==
      "https://test.url.domain");

  // Initialize data that in real life scenario would be initialized by
  // RestClient
  char res_data_s3[] = "Location: tiledb://my_username/s3://my_bucket/my_array";
  count = sizeof(res_data_s3);
  ns_array = "my_bucket:my_array";
  userdata.uri = &ns_array;
  result = write_header_callback(&res_data_s3, size, count, &userdata);

  // The return value should be equal to size * count
  CHECK(result == size * count);
  // The uri is not mutated inside the callback function
  CHECK(*userdata.uri == ns_array);
  // The redirect map holds a record of ns_array key and redirection url value
  CHECK(userdata.redirect_uri_map->count(ns_array) == 1);
  // The redirect map holds both records
  CHECK(userdata.redirect_uri_map->size() == 2);
  // The value of this redirection record matches the redirection url
  CHECK(
      userdata.redirect_uri_map->find(ns_array)->second ==
      "tiledb://my_username");
}

TEST_CASE(
    "CURL: Test curl is not caching the region when requested so",
    "[curl][redirect-caching]") {
  // Initialize data that in real life scenario would be initialized by
  // RestClient
  char res_data[] =
      "Location: https://test.url.domain/v1/arrays/testns/test_arr";
  size_t size = 1;
  size_t count = sizeof(res_data);

  HeaderCbData userdata;
  std::string ns_array = "testns:test_arr";
  userdata.uri = &ns_array;
  std::mutex redirect_mtx_;
  std::unordered_map<std::string, std::string> redirect_meta_;
  userdata.redirect_uri_map = &redirect_meta_;
  userdata.redirect_uri_map_lock = &redirect_mtx_;
  userdata.should_cache_redirect = false;

  size_t result = write_header_callback(&res_data, size, count, &userdata);

  // The return value should be equal to size * count
  CHECK(result == size * count);
  // The uri is not mutated inside the callback function
  CHECK(*userdata.uri == ns_array);
  // The redirect map holds a record of ns_array key and redirection url value
  CHECK(userdata.redirect_uri_map->count(ns_array) == 0);
}

TEST_CASE(
    "RestClient: Remove trailing slash from rest_server_", "[rest-client]") {
  std::string rest_server =
      GENERATE("http://localhost:8080/", "http://localhost:8080//");
  tiledb::sm::Config cfg;
  SECTION("rest.server_address set in Config") {
    cfg.set("rest.server_address", rest_server).ok();
  }
  SECTION("rest.server_address set in environment") {
    setenv_local("TILEDB_REST_SERVER_ADDRESS", rest_server.c_str());
  }
  SECTION("rest.server_address set by loaded config file") {
    std::string cfg_file = "tiledb_config.txt";
    std::ofstream file(cfg_file);
    file << "rest.server_address " << rest_server << std::endl;
    file.close();
    cfg.load_from_file(cfg_file).ok();
    std::filesystem::remove(cfg_file);
  }

  ThreadPool tp{1};
  ContextResources resources(
      cfg, tiledb::test::g_helper_logger(), 1, 1, "test");
  auto rest_client{tiledb::sm::RestClientFactory::make(
      tiledb::test::g_helper_stats,
      cfg,
      tp,
      *tiledb::test::g_helper_logger().get(),
      resources.create_memory_tracker())};
  CHECK(rest_client->rest_server() == "http://localhost:8080");
}

TEST_CASE(
    "RestClient: Ensure custom headers are set",
    "[rest-client][custom-headers]") {
  tiledb::sm::Config cfg;
  REQUIRE(cfg.set("rest.custom_headers.abc", "def").ok());
  REQUIRE(cfg.set("rest.custom_headers.ghi", "jkl").ok());

  ContextResources resources(
      cfg, tiledb::test::g_helper_logger(), 1, 1, "test");
  const auto& extra_headers = resources.rest_client()->extra_headers();
  CHECK(extra_headers.at("abc") == "def");
  CHECK(extra_headers.at("ghi") == "jkl");
}

TEST_CASE(
    "RestClient: Ensure payer namespace is set",
    "[rest-client][payer-namespace]") {
  tiledb::sm::Config cfg;
  REQUIRE(cfg.set("rest.payer_namespace", "foo").ok());

  ContextResources resources(
      cfg, tiledb::test::g_helper_logger(), 1, 1, "test");
  const auto& extra_headers = resources.rest_client()->extra_headers();
  CHECK(extra_headers.at("X-Payer") == "foo");
}
