/**
 * @file   random_label.h
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
 * This file declares a random label generator.
 */

#ifndef TILEDB_HELPERS_H
#define TILEDB_HELPERS_H

#include <string>

namespace tiledb::common {

/**
 * Returns a PRNG-generated random label with the optionally-provided prefix.
 *
 * Given prefix "tiledb-", this function will return a label with syntax
 * tiledb-<32-digit hexadecimal random number>.
 * (Ex. tiledb-f258d22d4db9139204eef2b4b5d860cc).
 *
 * Note: the random number is actually the combination of two 16-digit numbers.
 * The values are 0-padded to ensure exactly a 128-bit, 32-digit length.
 *
 * @param prefix The optional prefix of the label.
 * @return A random label.
 */
std::string random_label(std::string prefix = "");

}  // namespace tiledb::common

#endif  // TILEDB_HELPERS_H
