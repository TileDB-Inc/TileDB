/**
 * @file   generator.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file declares the generators classes for dag.
 */

#ifndef TILEDB_DAG_GENERATOR_H
#define TILEDB_DAG_GENERATOR_H

namespace tiledb::common {

/**
 * Prototype producer function object class.  This class generates a sequence of
 * integers starting at `N`, returning a new integer with every invocation of
 * `operator()`.
 */
template <class Block = size_t>
class generator {
  std::atomic<Block> N_{0};
  std::atomic<Block> i_{0};

 public:
  generator(Block N = 0)
      : N_{N}
      , i_{N} {
  }
  generator(const generator& rhs)
      : N_(rhs.N_.load())
      , i_(rhs.i_.load()) {
  }

  Block operator()() {
    return i_++;
  }
};
}  // namespace tiledb::common
#endif  // TILEDB_DAG_GENERATOR_H
