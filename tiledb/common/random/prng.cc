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
/**
 * 64-bit mersenne twister engine for random number generation.
 *
 * This definition is duplicated to avoid having it defined as `public` in
 * `class PRNG`.
 */
using prng_type = std::mt19937_64;

/**
 * Implementation of the random seed.
 *
 * This is a class template in order to use `if constexpr`.
 *
 * @tparam return_size_type The type of the seed to be returned
 */
template <class return_size_type>
return_size_type random_seed() {
  static constexpr size_t rng_size = sizeof(std::random_device::result_type);
  static constexpr size_t ret_size = sizeof(return_size_type);
  std::random_device rng{};
  /*
   * We will need 64 bits to adequately seed the PRNG (`ret_size`). We support
   * cases where the result size of the RNG is 64 or 32 bits (`rng_size`).
   */
  if constexpr (ret_size == rng_size) {
    return rng();
  } else if constexpr (ret_size == 2 * rng_size) {
    return (rng() << rng_size) + rng();
  } else {
    throw std::runtime_error("Unsupported combination of RNG sizes");
  }
}

/**
 * The PRNG used within the random constructor.
 */
prng_type prng_random() {
  return prng_type{random_seed<uint64_t>()};  // RVO
}

/**
 * The PRNG used within the default constructor.
 */
prng_type prng_default() {
  /*
   * Retrieve optional seed, which may or may not have been set explicitly.
   */
  auto seed{Seeder::get().seed()};
  /*
   * Use the seed if it has been set. Otherwise use a random seed.
   */
  if (seed.has_value()) {
    return prng_type{seed.value()};  // RVO
  } else {
    return prng_random();  // RVO
  }
}

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

PRNG::PRNG()
    : prng_(prng_default())
    , mtx_{} {
}

PRNG::PRNG(RandomSeedT)
    : prng_(prng_random())
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

}  // namespace tiledb::common
