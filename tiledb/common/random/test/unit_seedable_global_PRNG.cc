/**
 * @file tiledb/common/random/test/unit_seedable_global_PRNG.cc
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
 * Tests for the global seedable PRNG facility.
 */

#include <test/support/tdb_catch.h>
#include "../prng.h"
#include "../seeder.h"

#include <iostream>

using namespace tiledb::common;

TEST_CASE(
    "SeedableGlobalPRNG: Seeder, default seed",
    "[SeedableGlobalPRNG][Seeder][seed]") {
  // Default seed is std::nullopt
  Seeder seeder;
  auto seed{seeder.seed()};
  CHECK(!seed.has_value());
}

TEST_CASE(
    "SeedableGlobalPRNG: Seeder, set seed",
    "[SeedableGlobalPRNG][Seeder][set_seed]") {
  Seeder seeder;
  seeder.set_seed(0);

  // Use seed, after it's been set but not used (lifespan_state_ = 1)
  CHECK(seeder.seed().has_value());
}

TEST_CASE(
    "SeedableGlobalPRNG: operator",
    "[SeedableGlobalPRNG][operator][multiple]") {
  PRNG prng;
  auto rand_num1 = prng();
  CHECK(rand_num1 != 0);

  auto rand_num2 = prng();
  CHECK(rand_num2 != 0);
  CHECK(rand_num1 != rand_num2);

  auto rand_num3 = prng();
  CHECK(rand_num3 != 0);
  CHECK(rand_num1 != rand_num3);
  CHECK(rand_num2 != rand_num3);
}

TEST_CASE(
    "SeedableGlobalPRNG: Seeder singleton, errors",
    "[SeedableGlobalPRNG][Seeder][singleton][errors]") {
  // Note: these errors will occur because PRNG sets and uses the singleton.
  Seeder& seeder_ = Seeder::get();

  // Try to set new seed
  CHECK_THROWS_WITH(
      seeder_.set_seed(1),
      Catch::Matchers::ContainsSubstring("Seed has already been set"));

  // Try to use seed again
  CHECK_THROWS_WITH(
      seeder_.seed(),
      Catch::Matchers::ContainsSubstring("Seed can only be used once"));
}
