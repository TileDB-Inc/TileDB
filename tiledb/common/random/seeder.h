/**
 * @file   seeder.h
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
 * This file declares the seeder for the library-wide 64-bit RNG facility.
 */

#ifndef TILEDB_SEEDER_H
#define TILEDB_SEEDER_H

#include <mutex>
#include <optional>
#include <stdexcept>

namespace tiledb::common {

/**
 * Class which manages the lifetime state and transitions of a seed
 * which may be used to seed a random number generator in class PRNG.
 *
 * Lifetime state and transitions can be depicted as follows:
 *    0         --->           1        --->        2
 * default   (set_seed)   seed is set  (seed)  seed is used
 *                        but unused
 *
 * Note that each transition may occur only once, i.e. a seed may only be set
 * one time and may only be used one time. This is an explicit design choice to
 * ensure that a singleton PRNG is only initialized once, and to prevent the
 * case where a seeming initialization is not the actual initialization.
 */
class Seeder {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * Default constructed as the initial state of the singleton Seeder with
   * no set seed.
   */
  Seeder() = default;

  /** Copy constructor is deleted. */
  Seeder(const Seeder&) = delete;

  /** Move constructor is deleted. */
  Seeder(Seeder&&) = delete;

  /** Copy assignment is deleted. */
  Seeder& operator=(const Seeder&) = delete;

  /** Move assignment is deleted. */
  Seeder& operator=(Seeder&&) = delete;

  /** Destructor. */
  ~Seeder() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Singleton accessor. */
  static Seeder& get();

  /**
   * Sets the seed.
   * Transitions seed lifetime_state_: 0 (default) --> 1 (set but unused).
   * Note that this may be called once and only once.
   *
   * @param seed Seed to set.
   */
  void set_seed(uint64_t seed);

  /**
   * Returns the seed, if set and not yet used.
   * Transitions seed lifetime_state_: 1 (set but unused) --> 2 (set and used).
   * Note: this may be called once and only once.
   *
   * Note: if the seed is not yet set, return std::nullopt.
   * This behavior will allow users to call the default constructor.
   * Transitions seed lifetime_state_: 0 (default) --> 2 (used).
   *
   * @return Seed, if set and not yet used. Else, throw or return nullopt.
   */
  std::optional<uint64_t> seed();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Optional seed object, set by set_seed. */
  std::optional<uint64_t> seed_{std::nullopt};

  /**
   * Numerical representation of seeder state.
   *
   * 0 = default (unseeded)
   * 1 = seeded but seed not yet used
   * 2 = seeded and seed used
   */
  int lifespan_state_{0};

  /** Mutex which protects transitions of lifespan_state_. */
  std::mutex mtx_;
};
}  // namespace tiledb::common

#endif  // TILEDB_SEEDER_H
