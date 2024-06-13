/**
 * @file tiledb/common/util/permute.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 */

#ifndef TILEDB_PERMUTE_H
#define TILEDB_PERMUTE_H

#include <algorithm>
#include <cassert>
#include <ranges>
#include <vector>

/**
 * Permute an array in-place according to a given permutation array.
 * @tparam Vector
 * @tparam Perm
 * @tparam Done Should generally be `std::vector<char>`
 * @param b Array to be sorted
 * @param perm Array specifying the permutation
 * @param done Array of flags indicating whether an element has been processed
 * or not.  All elements must be cleared (set to false) on entry.  Note that it
 * will be modified while the function is executing but on return, all the
 * entries will be cleared.
 */
template <
    std::ranges::random_access_range Vector,
    std::ranges::random_access_range Perm,
    std::ranges::random_access_range Done>
void permute(Vector& b, const Perm& perm, Done& done) {
  using flag_type = std::remove_cvref_t<decltype(done[0])>;
  constexpr flag_type zero_flag{0};
  constexpr flag_type one_flag{1};

  // @todo Is there a better way to track done-ness?  Hi bit of perm?
  for (size_t i = 0; i < perm.size(); ++i) {
    if (zero_flag != done[i]) {
      done[i] = zero_flag;
      continue;
    }

    size_t ix = i;
    size_t px = perm[ix];
    while (!done[px]) {
      // @todo Find with ADL???
      std::swap(b[ix], b[px]);

      done[ix] = one_flag;
      ix = px;
      px = perm[ix];
    }
  }
}

template <
    std::ranges::random_access_range Vector,
    std::ranges::random_access_range Perm>
void permute(Vector& b, const Perm& perm) {
  std::vector<char> done(perm.size(), false);
  permute(b, perm, done);
}

#endif
