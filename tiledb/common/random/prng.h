/**
 * @file   prng.h
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
 * This file declares the library-wide 64-bit PRNG facility.
 */

#ifndef TILEDB_PRNG_H
#define TILEDB_PRNG_H

#include <mutex>
#include <random>

#include "tiledb/common/random/seeder.h"

namespace tiledb::common {

/**
 * Marker class for a test-only PRNG constructor
 */
class RandomSeedT {};
/**
 * Marker constant
 */
static constexpr RandomSeedT RandomSeed;

/**
 * A random number generator suitable for both production and testing.
 *
 * @section Requirements
 *
 * This PRNG must support two very different kinds of situations:
 *
 * 1. In production use (the ordinary case) the seed must be _actually_ random
 *    so that the random sequences in different processes are distinct.
 * 2. During most testing the seed must be deterministic to ensure that
 *    different test runs execute the same sequence of operations. This ensures
 *    that test failures can be replicated for diagnosis and correction.
 *    a. In particular, the seed in Catch2 test runs should be deterministic.
 * 3. Certain tests, however, require actual randomness.
 *   a. One such test verifies that actual randomness is available per (1). Such
 *      tests necessarily have the possibility of failures, i.e. of false
 *      positives, but the actual likelihood can be made extremely low.
 *   b. Stress tests execute large number of test runs searching for defects.
 *      Such tests do not generate new information when run with previously-
 *      used PRNG sequences.
 *
 * This class satisfies these requirements with the following implementation
 * choices:
 * 1. If the user has not called `set_seed()` on the global seeder (from
 *    `Seeder::get`), then the seed is taken from `std::random_device`.
 * 2. If the user has called `set_seed()` on the global seeder, that seed is
 *    used.
 * 3. This class uses a global seeder in order to support Catch2. An event
 *    handler that executes at the start of the test run calls `set_seed()`.
 *
 * @section Maturity
 *
 * This class only has a default constructor. It does not have constructors that
 * take seeds nor seeders. Such constructors would be useful for replicating
 * test runs, but would also be premature at present. There's further test
 * infrastructure required to replicate a specific test in isolation. As that
 * test infrastructure matures, so also should this class.  In the interim, in
 * order to replicate a specific test with a specific seed, the function
 * `initial_prng()` can be temporarily changed.
 *
 * This class uses a seeded PRNG to implement the random sequence. The
 * requirement is that sequences in different processes be distinct, not that
 * they be actually random. A randomly-seeded PRNG satisfies this requirement.
 * The motivation for this implementation choice is as follows:
 * 1. There is no standard hardware requirement for random number generation.
 *    While it's generally available, there are unknown variations in
 *    significant quality parameters such as the rate of random generation,
 *    duration of an RNG call, and randomness of generation (e.g. n-gram
 *    entropies).
 * 2. In order not to stress a potentially inadequate RNG, we only call it for
 *    seeding and not for every number.
 * 3. Qualifying a potential RNG implementation requires engineering resources
 *    that have not been committed as yet.
 *
 * @section Caveat
 *
 * This class uses `std::random_device` to seed the PRNG if no explicit seed is
 * set. The standard library does not require that this class use an actual RNG,
 * i.e. RNG from hardware of some kind. Indeed, certain earlier implementations
 * did not do so and were deterministic. In order to validate that this device
 * is actually random, it's necessary to run a multiprocess test to observe
 * initialization in different processes. The test suite does not contain such
 * a validation test at present.
 */
class PRNG {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Default constructor.
   *
   * If `Seeder` has been seeded, the seed will be set on the engine. Otherwise,
   * the generator is constructed with a random seed.
   */
  PRNG();

  /**
   * Constructor for random seeding.
   *
   * This constructor makes an object that is always constructed with a random
   * seed.
   *
   * @warning This constructor is only for testing. It must not be used in
   * production code, where it would thwart the ability to run tests
   * deterministically.
   */
  PRNG(RandomSeedT);

  /** Copy constructor is deleted. */
  PRNG(const PRNG&) = delete;

  /** Move constructor is deleted. */
  PRNG(PRNG&&) = delete;

  /** Copy assignment is deleted. */
  PRNG& operator=(const PRNG&) = delete;

  /** Move assignment is deleted. */
  PRNG& operator=(PRNG&&) = delete;

  /** Destructor. */
  ~PRNG() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Singleton accessor. */
  static PRNG& get();

  /** Get next in PRNG sequence. */
  uint64_t operator()();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** 64-bit mersenne twister engine for random number generation. */
  std::mt19937_64 prng_;

  /** Mutex which protects against simultaneous access to operator() body. */
  std::mutex mtx_;
};
}  // namespace tiledb::common

#endif  // TILEDB_PRNG_H
