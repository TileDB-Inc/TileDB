/**
 * @file   hilbert.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2021 TileDB, Inc.
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
 * This file defines class Hilbert. This class utilizes two functions from
 * John Skilling, "Programming the Hilbert Curve". In AIP, 2004.
 */

#ifndef TILEDB_HILBERT_H
#define TILEDB_HILBERT_H

#include <sys/types.h>
#include <cmath>
#include <cstdlib>
#include <iostream>

#include "tiledb/common/assert.h"

namespace tiledb {
namespace sm {

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**
 * The Hilbert curve fills a multi-dimensional space in a particular manner
 * with a 1D line. The typical operations of this class is converting a
 * multi-dimensional tuple of coordinates into a 1D Hilbert value, and
 * vice versa.
 *
 * For the 2D case, the Hilbert curve looks as follows:
 *
 \verbatim
         |
      15 |    @---@   @---@   @---@   @---@   @---@   @---@   @---@   @---@
         |    |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |
         |    @   @---@   @   @   @---@   @   @   @---@   @   @   @---@   @
         |    |           |   |           |   |           |   |           |
         |    @---@   @---@   @---@   @---@   @---@   @---@   @---@   @---@
         |        |   |           |   |           |   |           |   |
         |    @---@   @---@---@---@   @---@   @---@   @---@---@---@   @---@
         |    |                           |   |                           |
         |    @   @---@---@   @---@---@   @   @   @---@---@   @---@---@   @
         |    |   |       |   |       |   |   |   |       |   |       |   |
   Dim[1]|    @---@   @---@   @---@   @---@   @---@   @---@   @---@   @---@
         |            |           |                   |           |
         |    @---@   @---@   @---@   @---@   @---@   @---@   @---@   @---@
         |    |   |       |   |       |   |   |   |       |   |       |   |
         |    @   @---@---@   @---@---@   @---@   @---@---@   @---@---@   @
         |    |                                                           |
         |    @---@   @---@---@   @---@---@   @---@---@   @---@---@   @---@
         |        |   |       |   |       |   |       |   |       |   |
         |    @---@   @---@   @---@   @---@   @---@   @---@   @---@   @---@
         |    |           |           |           |           |           |
         |    @   @---@   @   @---@   @---@   @---@   @---@   @   @---@   @
         |    |   |   |   |   |   |       |   |       |   |   |   |   |   |
         |    @---@   @---@   @   @---@---@   @---@---@   @   @---@   @---@
         |                    |                           |
       3 |    5---6   9---@   @   @---@---@   @---@---@   @   @---@   @---@
         |    |   |   |   |   |   |       |   |       |   |   |   |   |   |
       2 |    4   7---8   @  30---@   @---@   @---@   @---@   @   @---@   @
         |    |           |           |           |           |           |
       1 |    3---2   @---@   @---@   @---@   @---@   @---@   @---@   @---@
         |        |   |       |   |       |   |       |   |       |   |
       0 |    0---1   @---@---@   @--20---@   @---@---@   @---@---@   @--255
         |
          -------------------------------------------------------------------
              0   1   2   3               Dim[0]                          15

 \endverbatim
 *
 * The Hilbert value of (2,3) is 9, whereas the coordinates corresponding to
 * Hilbert value 2 are (1,1).
 *
 * The class utilizes two functions from John Skilling's work published in:
 * John Skilling, "Programming the Hilbert Curve". In AIP, 2004
 */
class Hilbert {
 public:
  /* ********************************* */
  /*             CONSTANTS             */
  /* ********************************* */

  /**
   * Maximum number of dimensions for defining the Hilbert curve. Although the
   * Hilbert curve can be defined over arbitrary dimensionality, we limit the
   * number of dimensions because they affect the number of bits used to
   * represent a Hilbert value; in our class, a Hilbert value is a int64_t
   * number and, thus, it cannot exceed 64 bits.
   */
  static const int HC_MAX_DIM = 16;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param bits Number of bits used for coordinate values across each dimension
   * @param dim_num Number of dimensions
   */
  Hilbert(int bits, int dim_num)
      : bits_(bits)
      , dim_num_(dim_num) {
    iassert(dim_num >= 0 && dim_num < HC_MAX_DIM, "dim_num = {}", dim_num);
    iassert(
        bits * dim_num <= int(sizeof(uint64_t) * 8 - 1),
        "bits = {}, dim_num = {}",
        bits,
        dim_num);
  }

  /**
   * Constructor. The number of bits will be calculated based
   * on the number of dimensions.
   *
   * @param dim_num Number of dimensions
   */
  Hilbert(int dim_num)
      : dim_num_(dim_num) {
    iassert(dim_num >= 0 && dim_num < HC_MAX_DIM, "dim_num = {}", dim_num);
    bits_ = (sizeof(uint64_t) * 8 - 1) / dim_num_;
  }

  /** Destructor. */
  ~Hilbert() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the number of bits. */
  int bits() const {
    return bits_;
  }

  /** Returns the number of dimensions. */
  int dim_num() const {
    return dim_num_;
  }

  /**
   * Converts a set of coordinates to a Hilbert value.
   *
   * @param coords The coordinates to be converted.
   * @return The output Hilbert value.
   */
  uint64_t coords_to_hilbert(uint64_t* coords) {
    iassert(coords != nullptr);
    uint64_t ret = 0;

    // Convert coords to the transpose form of the hilbert value
    axes_to_transpose(coords, bits_, dim_num_);

    // Convert the hilbert transpose form into an int64_t hilbert value
    uint64_t c = 1;  // This is a bit shifted from right to left over coords[i]
    uint64_t h = 1;  // This is a bit shifted from right to left over hilbert
    for (int j = 0; j < bits_; ++j, c <<= 1) {
      for (int i = dim_num_ - 1; i >= 0; --i, h <<= 1) {
        if (coords[i] & c)
          ret |= h;
      }
    }

    return ret;
  }

  /**
   * Converts a Hilbert value into a set of coordinates.
   *
   * @param hilbert The Hilbert value to be converted.
   * @param coords The output coordinates.
   * @return void
   */
  void hilbert_to_coords(uint64_t hilbert, uint64_t* coords) {
    iassert(coords != nullptr);

    // Initialization
    for (int i = 0; i < dim_num_; ++i)
      coords[i] = 0;

    // Convert the int64_t hilbert value to its transpose form
    uint64_t c = 1;  // This is a bit shifted from right to left over coords[i]
    uint64_t h = 1;  // This is a bit shifted from right to left over hilbert
    for (int j = 0; j < bits_; ++j, c <<= 1) {
      for (int i = dim_num_ - 1; i >= 0; --i, h <<= 1) {
        if (hilbert & h)
          coords[i] |= c;
      }
    }

    // Convert coords to the transpose form of the hilbert value
    transpose_to_axes(coords, bits_, dim_num_);
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Number of bits for representing a coordinate per dimension. */
  int bits_;
  /** Number of dimensions. */
  int dim_num_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * From John Skilling's work. It converts the input coordinates
   * to what is called the transpose of the Hilbert value. This is done
   * in place.
   *
   * **Example**
   *
   * Let b=5 and n=3. Let the 15-bit Hilbert value of the input coordinates
   * be A B C D E a b c d e 1 2 3 4 5. The function places this number into
   * parameter X as follows:
   *
   \verbatim
             X[0] = A D b e 3                  X[1]|
             X[1] = B E c 1 4    <------->         |  /X[2]
             X[2] = C a d 2 5                axes  | /
                    high  low                      |/______

                                                         X[0]
   \endverbatim
   *
   * The X value after the function terminates is called the transpose form
   * of the Hilbert value.
   *
   * @param X Input coordinates, and output transpose (the conversion is
   *     done in place).
   * @param b Number of bits for representing a coordinate per dimension
   *     (it should be equal to bits_).
   * @param n Number of dimensions (it should be equal to dim_num_).
   * @return void
   */
  void axes_to_transpose(uint64_t* X, int b, int n) {
    uint64_t P, Q, t;
    int i;

    // Inverse undo
    for (Q = (uint64_t)1 << (b - 1); Q > 1; Q >>= 1) {
      P = Q - 1;
      if (X[0] & Q)  // invert
        X[0] ^= P;
      for (i = 1; i < n; i++)
        if (X[i] & Q)  // invert
          X[0] ^= P;
        else {  // exchange
          t = (X[0] ^ X[i]) & P;
          X[0] ^= t;
          X[i] ^= t;
        }
    }

    // Gray encode (inverse of decode)
    for (i = 1; i < n; i++)
      X[i] ^= X[i - 1];
    t = X[n - 1];
    for (i = 1; i < b; i <<= 1)
      X[n - 1] ^= X[n - 1] >> i;
    t ^= X[n - 1];
    for (i = n - 2; i >= 0; i--)
      X[i] ^= t;
  }

  /**
   * From John Skilling's work. It converts the transpose of a
   * Hilbert value into the corresponding coordinates. This is done in place.
   *
   * **Example**
   *
   * Let b=5 and n=3. Let the 15-bit Hilbert value of the output coordinates
   * be A B C D E a b c d e 1 2 3 4 5. The function takes as input the tranpose
   * form of the Hilbert value, which is stored in X as follows:
   *
   \verbatim
             X[0] = A D b e 3                  X[1]|
             X[1] = B E c 1 4    <------->         |  /X[2]
             X[2] = C a d 2 5                axes  | /
                    high  low                      |/______

                                                         X[0]
   \endverbatim
   *
   * The X value after the function terminates will contain the coordinates
   * corresponding to the Hilbert value.
   *
   * @param X Input transpose, and output coordinates (the conversion is
   *     done in place).
   * @param b Number of bits for representing a coordinate per dimension
   *     (it should be equal to bits_).
   * @param n Number of dimensions (it should be equal to dim_num_).
   * @return void
   */
  void transpose_to_axes(uint64_t* X, int b, int n) {
    uint64_t M, P, Q, t;
    int i;

    // Gray decode by H ^ (H/2)
    t = X[n - 1] >> 1;
    for (i = n - 1; i; i--)
      X[i] ^= X[i - 1];
    X[0] ^= t;

    // Undo excess work
    M = 2 << (b - 1);
    for (Q = 2; Q != M; Q <<= 1) {
      P = Q - 1;
      for (i = n - 1; i; i--)
        if (X[i] & Q)  // invert
          X[0] ^= P;
        else {  // exchange
          t = (X[0] ^ X[i]) & P;
          X[0] ^= t;
          X[i] ^= t;
        }
      if (X[0] & Q)  // invert
        X[0] ^= P;
    }
  }
};

}  // namespace sm
}  // end namespace tiledb

#endif  // TILEDB_HILBERT_H
