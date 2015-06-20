/**
 * @file   progress_bar.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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

#ifndef _PROGRESS_BAR_H_
#define _PROGRESS_BAR_H_

#include <cstring>

/** The default filler character of the bar. */
#define PB_FILLER '='
/** The default maximum length of the bar. */
#define PB_MAX_LENGTH 30
/** The increase in the incomplete/complete ratio before the next print. */
#define PB_RATIO_STEP 0.01

/** Implements a simple progress bar printed in standard output. */
class ProgressBar {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** 
   * Constructor. The first argument is the amount at which the bar must
   * reach its maximum length. The second argument is the visual length
   * of the bar. The third is the character that fills the bar.
   */
  ProgressBar(double complete, int max_length = PB_MAX_LENGTH, 
              char filler = PB_FILLER);
  /** Destructor. */
  ~ProgressBar();

  // METHODS
  /** */
  void load(size_t amount); 
   

 private:
  // PRIVATE ATTRIBUTES
  /** The current amount accummulated towards completion. */
  double incomplete_; 
  /** The amount at which the bar reaches its maximum length. */
  double complete_; 
  /** The character that fills the bar. */
  char filler_;
  /** The incomplete/complete ratio upon the last print. */
  double last_ratio_;
  /** The bar current length. */
  int length_;
  /** The current ratio incomplete/complete. */
  double ratio_;
  /** The bar maximum length. */
  int max_length_;

  // PRIVATE METHODS
  /** Prints the bar with its current status. */
  void print();
};

#endif
