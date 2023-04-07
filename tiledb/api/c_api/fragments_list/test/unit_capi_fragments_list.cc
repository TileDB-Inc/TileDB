/**
 * @file tiledb/api/c_api/fragments_list/test/unit_capi_fragments_list.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 */

#include <test/support/tdb_catch.h>
#include "../fragments_list_api_external.h"
#include "../fragments_list_api_internal.h"

TEST_CASE("C API: Test fragments list", "[capi][fragments_list]") {
  tiledb::sm::URI a{tiledb::sm::URI("a")};
  tiledb::sm::URI b{tiledb::sm::URI("b")};
  std::vector<tiledb::sm::URI> uris = {a, b};
  auto f{tiledb_fragments_list_handle_t::make_handle(uris)};

  // Check fragment uris
  const char* uri_a;
  size_t len;
  REQUIRE(
      tiledb_fragments_list_get_fragment_uri(f, 0, &uri_a, &len) == TILEDB_OK);
  CHECK(tiledb::sm::URI(uri_a) == a);
  CHECK(len == strlen(a.c_str()));
  const char* uri_b;
  REQUIRE(
      tiledb_fragments_list_get_fragment_uri(f, 1, &uri_b, &len) == TILEDB_OK);
  CHECK(len == strlen(b.c_str()));
  CHECK(tiledb::sm::URI(uri_b) == b);
  REQUIRE(
      tiledb_fragments_list_get_fragment_uri(f, 2, &uri_b, &len) == TILEDB_ERR);

  // Check fragment indices
  unsigned index_a;
  REQUIRE(
      tiledb_fragments_list_get_fragment_index(f, uri_a, &index_a) ==
      TILEDB_OK);
  CHECK(index_a == 0);
  unsigned index_b;
  REQUIRE(
      tiledb_fragments_list_get_fragment_index(f, uri_b, &index_b) ==
      TILEDB_OK);
  CHECK(index_b == 1);
  const char* uri_c = {"c"};
  REQUIRE(
      tiledb_fragments_list_get_fragment_index(f, uri_c, &index_b) ==
      TILEDB_ERR);

  tiledb_fragments_list_handle_t::break_handle(f);
}
