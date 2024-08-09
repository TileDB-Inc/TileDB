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

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "../prng.h"
#include "../random_label.h"
#include "../seeder.h"

using namespace tiledb::common;

TEST_CASE(
    "SeedableGlobalPRNG: Seeder, default seed",
    "[SeedableGlobalPRNG][Seeder][default]") {
  // Default seed is std::nullopt
  Seeder seeder;
  std::optional<uint64_t> seed;

  // Use default seed (state 0 -> 2)
  CHECK_NOTHROW(seed = seeder.seed());
  CHECK(!seed.has_value());

  // Try setting seed after it's been used (state 2)
  SECTION("try to set seed again") {
    CHECK_THROWS_WITH(
        seeder.set_seed(123),
        Catch::Matchers::ContainsSubstring("Seed has already been set"));
  }

  // Try using seed after it's been used (state 2)
  SECTION("try to use seed again") {
    CHECK_THROWS_WITH(
        seeder.seed(),
        Catch::Matchers::ContainsSubstring("Seed can only be used once"));
  }
}

TEST_CASE(
    "SeedableGlobalPRNG: Seeder, set seed",
    "[SeedableGlobalPRNG][Seeder][set_seed]") {
  // Set seed (state 0 -> 1)
  Seeder seeder;
  CHECK_NOTHROW(seeder.set_seed(123));

  SECTION("try to set seed again") {
    CHECK_THROWS_WITH(
        seeder.set_seed(456),
        Catch::Matchers::ContainsSubstring("Seed has already been set"));
  }

  // Use seed, after it's been set but not used (state 1 -> 2)
  CHECK(seeder.seed() == 123);

  // Try setting seed after it's been set & used (state 2)
  SECTION("try to set seed after it's been set and used") {
    CHECK_THROWS_WITH(
        seeder.set_seed(456),
        Catch::Matchers::ContainsSubstring("Seed has already been set"));
  }

  // Try using seed after it's been set & used (state 2)
  SECTION("try to use seed after it's been set and used") {
    CHECK_THROWS_WITH(
        seeder.seed(),
        Catch::Matchers::ContainsSubstring("Seed can only be used once"));
  }
}

TEST_CASE(
    "SeedableGlobalPRNG: operator",
    "[SeedableGlobalPRNG][operator][multiple]") {
  PRNG& prng = PRNG::get();
  // Verify that a second call succeeds.
  CHECK_NOTHROW(PRNG::get());
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
  /*
   * Retrieve a  PRNG object explicitly. This will cause the PRNG to use the
   * singleton seeder, after which subsequent calls should fail.
   */
  [[maybe_unused]] auto& x{PRNG::get()};
  Seeder& seeder_ = Seeder::get();

  SECTION("try to set new seed after it's been set") {
    CHECK_THROWS_WITH(
        seeder_.set_seed(1),
        Catch::Matchers::ContainsSubstring("Seed has already been set"));
  }

  SECTION("try to use seed after it's been used") {
    CHECK_THROWS_WITH(
        seeder_.seed(),
        Catch::Matchers::ContainsSubstring("Seed can only be used once"));
  }
}

/*
 * Verify that randomly-seeded PRNG return different numbers. This is the best
 * we can do within the ordinary way within a single-process test, the only kind
 * readily available within Catch2.
 */
TEST_CASE("SeedableGlobalPRNG: Random seeding", "[SeedableGlobalPRNG]") {
  PRNG x(RandomSeed), y(RandomSeed);
  CHECK(x() != y());
}

TEST_CASE("random_label", "[random_label]") {
  auto rand_label1 = random_label();
  CHECK(rand_label1.length() == 32);

  auto rand_label2 = random_label();
  CHECK(rand_label2.length() == 32);
  CHECK(rand_label1 != rand_label2);
}
