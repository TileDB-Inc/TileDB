/**
 * @file   progress_bar.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
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
 * This file implements class ProgressBar.
 */

#include "progress_bar.h"
#include <stdio.h>




/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ProgressBar::ProgressBar(double complete, int max_length, char filler) {
  complete_ = complete;
  filler_ = filler;
  length_ = 0;
  incomplete_ = 0;
  last_ratio_ = 0;
  max_length_ = max_length;
  ratio_ = 0;
}

ProgressBar::~ProgressBar() {
}




/* ****************************** */
/*             METHODS            */
/* ****************************** */

void ProgressBar::load(double amount) {
  incomplete_ += amount;
  if(incomplete_ > complete_)
    incomplete_ = complete_;

  ratio_ = (incomplete_ / complete_);
  length_ = ratio_ * max_length_;

  // Print bar
  if(ratio_ - last_ratio_ > PB_RATIO_STEP) { 
    print();
    last_ratio_ = ratio_;
  }
}




/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

void ProgressBar::print() {
  fprintf(stdout, "%3d%% [", (int)(ratio_ * 100));

  for(int i=0; i<length_; ++i)
    fprintf(stdout, "%c", filler_);

  for(int i=length_; i<max_length_; ++i)
    fprintf(stdout, " ");

  fprintf(stdout, "]\r");
  fflush(stdout);
}
