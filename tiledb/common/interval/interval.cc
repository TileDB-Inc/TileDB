/**
 * @file   interval.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * This file defines the non-template parts of class Interval.
 */

#include "interval.h"

namespace tiledb::common::detail {

/*
 * Instantiations for marker class objects.
 */
static constexpr IntervalBase::empty_set_t empty_set =
    IntervalBase::empty_set_t();
static constexpr IntervalBase::single_point_t single_point =
    IntervalBase::single_point_t();
static constexpr IntervalBase::open_t open = IntervalBase::open_t();
static constexpr IntervalBase::closed_t closed = IntervalBase::closed_t();
static constexpr IntervalBase::minus_infinity_t minus_infinity =
    IntervalBase::minus_infinity_t();
static constexpr IntervalBase::plus_infinity_t plus_infinity =
    IntervalBase::plus_infinity_t();

}  // namespace tiledb::common::detail