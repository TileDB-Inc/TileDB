/**
 * @file tiledb/sm/fragment/test/unit_fragment.cc
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
#include "tiledb/sm/fragment/fragments_list.h"

using namespace tiledb::sm;

TEST_CASE("FragmentsList: Test default constructor", "[fragments_list]") {
  FragmentsList f;
  CHECK(f.empty() == true);
}

TEST_CASE("FragmentsList: Test non-default constructor", "[fragments_list]") {
  URI a{URI("a")};
  std::vector<URI> uris = {a};
  FragmentsList f(uris);
  CHECK(f.empty() == false);
}

TEST_CASE("FragmentsList", "[fragments_list]") {
  URI a{URI("a")};
  URI b{URI("b")};
  URI c{URI("c")};
  std::vector<URI> uris = {a, b, c};
  FragmentsList f(uris);
  CHECK(f.empty() == false);

  FragmentsList f_empty;
  CHECK(f_empty.empty() == true);

  SECTION("get index by URI") {
    CHECK(f.fragment_index(a) == 0);
    CHECK(f.fragment_index(b) == 1);
    CHECK(f.fragment_index(c) == 2);
    REQUIRE_THROWS_WITH(
        f.fragment_index(URI("d")),
        Catch::Matchers::ContainsSubstring("not in the FragmentsList"));
    REQUIRE_THROWS_WITH(
        f_empty.fragment_uri(0),
        Catch::Matchers::ContainsSubstring("FragmentsList is empty"));
  }

  SECTION("get URI by index") {
    CHECK(f.fragment_uri(0) == a);
    CHECK(f.fragment_uri(1) == b);
    CHECK(f.fragment_uri(2) == c);
    REQUIRE_THROWS_WITH(
        f.fragment_uri(3),
        Catch::Matchers::ContainsSubstring("no fragment at the given index"));
    REQUIRE_THROWS_WITH(
        f_empty.fragment_uri(0),
        Catch::Matchers::ContainsSubstring("FragmentsList is empty"));
  }
}
