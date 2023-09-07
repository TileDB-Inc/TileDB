/**
 * @file unit_random.cc
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
 *
 * This file defines tests for the Random.
 */

#include <test/support/tdb_catch.h>
#include "../random.h"

#include <iostream>

using namespace tiledb::common;

TEST_CASE("Random: generate once", "[random][generate][one]") {
  std::cerr << Random::generate_number() << std::endl;
  CHECK(true);
}

TEST_CASE("Random: generate multiple", "[random][generate][multiple]") {
  uint64_t num1 = Random::generate_number();
  uint64_t num2 = Random::generate_number();
  uint64_t num3 = Random::generate_number();
  uint64_t num4 = Random::generate_number();
  CHECK(num1 != num2);
  CHECK(num1 != num3);
  CHECK(num1 != num4);
  CHECK(num2 != num3);
  CHECK(num2 != num4);
  CHECK(num3 != num4);
}

TEST_CASE("Random: set seed, default", "[random][set_seed][default]") {
  Random::set_seed();
  CHECK(true);
}

TEST_CASE("Random: set seed, local seed", "[random][set_seed][local seed]") {
  auto local_seed = Catch::rngSeed();
  // Note: When the test is run, "Randomness seeded to: " is the local_seed
  Random::generate_number(local_seed);
  CHECK(true);
}
