/**
 * @file   smaller_pq_fragment_cell_range.cc
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
 * This file implements the SmallerPQFragmentCellRange class.
 */

#include "smaller_pq_fragment_cell_range.h"

#include <cassert>

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

template <class T>
SmallerPQFragmentCellRange<T>::SmallerPQFragmentCellRange() {
  array_metadata_ = nullptr;
}

/* ****************************** */
/*             API                */
/* ****************************** */

template <class T>
SmallerPQFragmentCellRange<T>::SmallerPQFragmentCellRange(
    const ArrayMetadata* array_metadata) {
  array_metadata_ = array_metadata;
}

template <class T>
bool SmallerPQFragmentCellRange<T>::operator()(
    PQFragmentCellRange<T>* a, PQFragmentCellRange<T>* b) const {
  // Sanity check
  assert(array_metadata_ != NULL);

  // First check the tile ids
  if (a->tile_id_l_ < b->tile_id_l_)
    return false;
  if (a->tile_id_l_ > b->tile_id_l_)
    return true;
  // else, check the coordinates

  // Get cell ordering information for the first range endpoints
  int cmp = array_metadata_->domain()->cell_order_cmp<T>(
      a->cell_range_, b->cell_range_);
  if (cmp < 0)  // a's range start precedes b's
    return false;
  if (cmp > 0)  // b's range start preceded a's
    return true;

  // Check empty fragments first
  if (a->fragment_id_ == PQFragmentCellRange<T>::INVALID_UINT)
    return true;
  if (b->fragment_id_ == PQFragmentCellRange<T>::INVALID_UINT)
    return false;

  // Check non-empty fragments
  if (a->fragment_id_ < b->fragment_id_)
    return true;
  if (a->fragment_id_ > b->fragment_id_)
    return false;

  // This should never happen (equal coordinates and fragment id)
  assert(0);
  return false;
}

// Explicit template instantiations
template class SmallerPQFragmentCellRange<int>;
template class SmallerPQFragmentCellRange<int64_t>;
template class SmallerPQFragmentCellRange<float>;
template class SmallerPQFragmentCellRange<double>;
template class SmallerPQFragmentCellRange<int8_t>;
template class SmallerPQFragmentCellRange<uint8_t>;
template class SmallerPQFragmentCellRange<int16_t>;
template class SmallerPQFragmentCellRange<uint16_t>;
template class SmallerPQFragmentCellRange<uint32_t>;
template class SmallerPQFragmentCellRange<uint64_t>;

}  // namespace tiledb
