/**
 * @file   prng.cc
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
 * This file defines the library-wide 64-bit PRNG facility.
 */

#include "tiledb/common/random/prng.h"

namespace tiledb::common {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

PRNG::PRNG()
    : prng_(prng_initial())
    , mtx_{} {
}

/* ********************************* */
/*                API                */
/* ********************************* */

PRNG& PRNG::get() {
  static PRNG singleton;
  return singleton;
}

uint64_t PRNG::operator()() {
  std::lock_guard<std::mutex> lock(mtx_);
  return prng_();
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

std::mt19937_64 PRNG::prng_initial() {
  // Retrieve optional, potentially default-constructed seed.
  auto seed{Seeder::get().seed()};

  // If the seed has been set, set it on the RNG engine.
  if (seed.has_value()) {
    return std::mt19937_64{seed.value()};  // RVO
  } else {
    return {};  // RVO
  }
}

}  // namespace tiledb::common
