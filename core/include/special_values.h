/**
 * @file   special_values.h
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
 * This file defines some global special values.
 */

#ifndef SPECIAL_VALUES_H
#define SPECIAL_VALUES_H

#include <limits>

/** Deleted char. */
#define DEL_CHAR 127
/** Deleted int. */
#define DEL_INT std::numeric_limits<int>::min()
/** Deleted int64_t. */
#define DEL_INT64_T std::numeric_limits<int64_t>::min()
/** Deleted float. */
#define DEL_FLOAT std::numeric_limits<float>::min()
/** Deleted double. */
#define DEL_DOUBLE std::numeric_limits<double>::min()
/** Missing char. */
#define NULL_CHAR '\0'
/** Missing int. */
#define NULL_INT std::numeric_limits<int>::max()
/** Missing int64_t. */
#define NULL_INT64_T std::numeric_limits<int64_t>::max()
/** Missing float. */
#define NULL_FLOAT std::numeric_limits<float>::max()
/** Missing double. */
#define NULL_DOUBLE std::numeric_limits<double>::max()

#endif
