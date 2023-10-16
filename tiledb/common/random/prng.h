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
class PRNG {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  PRNG();

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

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Default-constructs an mt19937 engine and optionally sets the seed. */
  std::mt19937_64 prng_initial();
};
}  // namespace tiledb::common

#endif  // TILEDB_PRNG_H
