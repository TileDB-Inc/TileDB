/**
 * @file   random.h
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
 * This file describes the library-wide 64-bit RNG facility.
 */

#ifndef TILEDB_RANDOM_H
#define TILEDB_RANDOM_H

#include <mutex>
#include <random>

namespace tiledb::common {

class Random {
 public:
  /**
   * @brief Set the seed object on the random number generator.
   *
   * @param local_seed An optional local seed to set on the generator.
   * Note: Catch::rngSeed() will produce the local Catch2 seed.
   */
  static void set_seed(uint64_t local_seed = 0) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (local_seed == 0) {
      std::random_device rd;
      generator_.seed(rd());
    } else {
      generator_.seed(local_seed);
    }
  }

  /**
   * @brief Generate a random number.
   *
   * @param local_seed An optional local seed to set on the generator.
   * @return random uint64_t
   *
   * Note: Catch::rngSeed() will produce the local Catch2 seed.
   */
  static uint64_t generate_number(uint64_t local_seed = 0) {
    set_seed(local_seed);
    static std::uniform_int_distribution<size_t> distribution(
        std::numeric_limits<size_t>::min(), std::numeric_limits<size_t>::max());
    return distribution(generator_);
  }

 private:
  /** 64-bit mersenne twister engine. */
  inline static std::mt19937_64 generator_;

  /** Mutex which protects the atomicity of seed-setting. */
  inline static std::mutex mtx_;
};

}  // namespace tiledb::common

#endif  // TILEDB_RANDOM_H