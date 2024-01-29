/**
 * @file   random_label.cc
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
 * This file defines a random label generator.
 */

#include "tiledb/common/random/random_label.h"
#include "tiledb/common/random/prng.h"

#include <iomanip>
#include <sstream>

namespace tiledb::common {

/**
 * Legacy code provides randomness using UUIDs, which are always 128 bits,
 * represented as a 32-digit hexadecimal value.
 *
 * To ensure backward compatibility, this function formats the PRNG-generated
 * values to be precisely a 32-digit hexadecimal value. Each value is padded
 * with 0s such that it makes up one 16-digit half of the full 32-digit number.
 */
std::string random_label() {
  PRNG& prng = PRNG::get();
  std::stringstream ss;

  // Generate and format a 128-bit, 32-digit hexadecimal random number
  auto rand1 = prng();
  ss << std::hex << std::setw(16) << std::setfill('0') << rand1;
  auto rand2 = prng();
  ss << std::hex << std::setw(16) << std::setfill('0') << rand2;

  // Return label string
  return ss.str();
}

}  // namespace tiledb::common
