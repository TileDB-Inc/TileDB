/**
 * @file   seeder.cc
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
 * This file defines the seeder for the library-wide 64-bit RNG facility.
 */

#include "tiledb/common/random/seeder.h"

namespace tiledb::common {

/* ********************************* */
/*                API                */
/* ********************************* */

Seeder& Seeder::get() {
  static Seeder singleton;
  return singleton;
}

void Seeder::set_seed(uint64_t seed) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (lifespan_state_ != 0) {
    throw std::logic_error("[Seeder::set_seed] Seed has already been set.");
  } else {
    seed_ = seed;
    lifespan_state_ = 1;
  }
}

std::optional<uint64_t> Seeder::seed() {
  std::lock_guard<std::mutex> lock(mtx_);

  if (lifespan_state_ == 2) {
    throw std::logic_error(
        "[Seeder::seed] Seed can only be used once and has already been used.");
  }

  lifespan_state_ = 2;
  return seed_;
}

}  // namespace tiledb::common
