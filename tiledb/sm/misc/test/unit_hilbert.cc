/**
 * @file   unit_hilbert.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Tests class Hilbert.
 */

#include <catch2/catch_test_macros.hpp>
#include "tiledb/sm/misc/hilbert.h"

using namespace tiledb::sm;

TEST_CASE("Hilbert: Test 2D", "[hilbert][2D]") {
  Hilbert h(4, 2);
  CHECK(h.bits() == 4);
  CHECK(h.dim_num() == 2);

  std::vector<uint64_t> coords = {2, 3};
  auto v = h.coords_to_hilbert(&coords[0]);
  CHECK(v == 9);

  coords = {0, 0};
  v = h.coords_to_hilbert(&coords[0]);
  CHECK(v == 0);

  coords = {0, 3};
  v = h.coords_to_hilbert(&coords[0]);
  CHECK(v == 5);

  coords = {3, 0};
  v = h.coords_to_hilbert(&coords[0]);
  CHECK(v == 15);

  coords = {5, 4};
  v = h.coords_to_hilbert(&coords[0]);
  CHECK(v == 35);

  coords = {4, 2};
  v = h.coords_to_hilbert(&coords[0]);
  CHECK(v == 30);
}

TEST_CASE("Hilbert: Test 3D", "[hilbert][3D]") {
  Hilbert h(4, 3);

  std::vector<uint64_t> coords = {0, 0, 0};
  auto v = h.coords_to_hilbert(&coords[0]);
  CHECK(v == 0);

  coords = {0, 0, 1};
  v = h.coords_to_hilbert(&coords[0]);
  CHECK(v == 1);

  coords = {0, 1, 0};
  v = h.coords_to_hilbert(&coords[0]);
  CHECK(v == 3);

  coords = {1, 0, 0};
  v = h.coords_to_hilbert(&coords[0]);
  CHECK(v == 7);
}

TEST_CASE("Hilbert: Test default bits, getter", "[hilbert][bits][getter]") {
  Hilbert h(3);
  CHECK(h.bits() == 21);
  CHECK(h.dim_num() == 3);
}
