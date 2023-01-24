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

#include <test/support/tdb_catch.h>
#include "tiledb/sm/rest/curl.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

using namespace tiledb::sm;

TEST_CASE("CURL: Test curl's header parsing callback", "[curl]") {
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
