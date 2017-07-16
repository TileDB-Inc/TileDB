/**
 * @file   progress_bar.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines class ProgressBar.
 */

#ifndef TILEDB_PROGRESS_BAR_H
#define TILEDB_PROGRESS_BAR_H

#include <cstring>

namespace tiledb {

/** Implements a simple progress bar printed in standard output. */
class ProgressBar {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param complete The amount at which the bar must reach its maximum length.
   * @param max_length The visual length of the bar.
   * @param filler The character that fills the bar.
   */
  ProgressBar(
      double complete = COMPLETE,
      int max_length = MAX_LENGTH,
      char filler = FILLER);

  /** Destructor. */
  ~ProgressBar();

  /* ********************************* */
  /*             METHODS               */
  /* ********************************* */

  /**
   * "Loads" the progress bar with the input amount, which may trigger drawing
   * the current progress on the standard output.
   */
  void load(double amount);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The amount at which the bar reaches its maximum length. */
  double complete_;
  /** The character that fills the bar. */
  char filler_;
  /** The current amount accummulated towards completion. */
  double incomplete_;
  /** The incomplete/complete ratio upon the last print. */
  double last_ratio_;
  /** The bar current length. */
  int length_;
  /** The bar maximum length. */
  int max_length_;
  /** The current ratio incomplete/complete. */
  double ratio_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Prints the bar with its current status. */
  void print();

  /* ********************************* */
  /*          PRIVATE CONSTANTS        */
  /* ********************************* */

  /** The default complete amount of the bar. */
  static const float COMPLETE;
  /** The default filler character of the bar. */
  static const char FILLER;
  /** The default maximum length of the bar. */
  static const int MAX_LENGTH;
  /** The increase in the incomplete/complete ratio before the next print. */
  static const float RATIO_STEP;
};

}  // namespace tiledb

#endif  // TILEDB_PROGRESS_BAR
