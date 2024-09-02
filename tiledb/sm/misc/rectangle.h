/**
 * @file   rectangle.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * This file defines namespace rectangle.
 */

#ifndef TILEDB_RECTANGLE_H
#define TILEDB_RECTANGLE_H

#include <array>
#include <vector>

#include "tiledb/common/status.h"

using namespace tiledb::common;

namespace tiledb::sm::rectangle {

/**
 * Checks if `coords` are inside `rect`.
 *
 * @tparam T The type of the cell and subarray.
 * @param coords The coordinates to be checked.
 * @param rect The hyper-rectangle to be checked, expressed as [low, high] pairs
 *     along each dimension.
 * @param dim_num The number of dimensions for the coordinates and
 *     hyper-rectangle.
 * @return `true` if `coords` are inside `rect` and `false` otherwise.
 */
template <class T>
bool coords_in_rect(const T* coords, const T* rect, unsigned int dim_num);

/**
 * Checks if `coords` are inside `rect`.
 *
 * @tparam T The type of the cell and subarray.
 * @param coords The coordinates to be checked.
 * @param rect The hyper-rectangle to be checked, expressed as [low, high] pairs
 *     along each dimension.
 * @param dim_num The number of dimensions for the coordinates and
 *     hyper-rectangle.
 * @return `true` if `coords` are inside `rect` and `false` otherwise.
 */
template <class T>
bool coords_in_rect(
    const T* coords, const std::vector<const T*>& rect, unsigned int dim_num);

/** Returns *true* if hyper-rectangle `a` overlaps with `b`. */
template <class T>
bool overlap(const T* a, const T* b, unsigned dim_num);

/**
 * Computes the overlap between two rectangles.
 *
 * @tparam T The types of the rectangles.
 * @param a The first input rectangle.
 * @param b The second input rectangle.
 * @param o The overlap area between `a` and `b`.
 * @param overlap `true` if the two rectangles overlap and `false` otherwise.
 */
template <class T>
void overlap(const T* a, const T* b, unsigned dim_num, T* o, bool* overlap);

/**
 * Returns the percentage of coverage of hyper-rectangle `a` in `b`.
 * Note that the function assumes that `a` is fully contained in `b`.
 */
template <class T>
double coverage(const T* a, const T* b, unsigned dim_num);

/**
 * Returns the intersection between r1 and r2.
 *
 * @param r1 A vector of 1D ranges, one range per dimension as a 2-element
 *     (start, end) array.
 * @param r2 A vector of 1D ranges, one range per dimension as a 2-element
 *     (start, end) array.
 * @return The intersection between r1 and r2, similarly as a vector of
 *     (start, end) 1D ranges.
 */
template <class T>
std::vector<std::array<T, 2>> intersection(
    const std::vector<std::array<T, 2>>& r1,
    const std::vector<std::array<T, 2>>& r2);

}  // namespace tiledb::sm::rectangle

#endif  // TILEDB_RECTANGLE_H
