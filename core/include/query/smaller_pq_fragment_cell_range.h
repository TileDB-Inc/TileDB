/**
 * @file   smaller_pq_fragment_cell_range.h
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
 * This file defines class SmallerPQFragmentCellRange.
 */

#ifndef TILEDB_SMALLER_PQ_FRAGMENT_CELL_RANGE_H
#define TILEDB_SMALLER_PQ_FRAGMENT_CELL_RANGE_H

#include "array_metadata.h"
#include "pq_fragment_cell_range.h"

namespace tiledb {

/**
 * Wrapper of comparison function in the priority queue of the fragment cell
 * ranges.
 */
template <class T>
class SmallerPQFragmentCellRange {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  SmallerPQFragmentCellRange();

  /** Constructor. */
  SmallerPQFragmentCellRange(const ArrayMetadata* array_metadata);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Comparison operator. First the smallest tile id of the left range end point
   * wins, then the smallest start range endpoint, then the largest fragment id.
   */
  bool operator()(PQFragmentCellRange<T>* a, PQFragmentCellRange<T>* b) const;

 private:
  /* ********************************* */
  /*          PRIVATE ATTRIBUTES       */
  /* ********************************* */

  /** The array metadata. */
  const ArrayMetadata* array_metadata_;
};

}  // namespace tiledb

#endif  // TILEDB_SMALLER_PQ_FRAGMENT_CELL_RANGE_H
