/**
 * @file   tiledb/common/util/block_view.h
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
 * This file implements a zip view for zipping together a set of ranges.
 * It is intended to implement the zip view as defined for C++23.  From
 * https://en.cppreference.com/w/cpp/ranges/zip_view:
 *  1) A zip_view is a range adaptor that takes one or more views, and produces
 * a view whose ith element is a tuple-like value consisting of the ith elements
 * of all views. The size of produced view is the minimum of sizes of all
 * adapted views. 2) zip is a customization point object that constructs a
 * zip_view.
 *
 */

#ifndef TILEDB_BLOCK_VIEW_H
#define TILEDB_BLOCK_VIEW_H
#endif  // TILEDB_BLOCK_VIEW_H
