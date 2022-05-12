/**
 * @file tiledb/sm/query/hilbert_order.h
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
 */

#ifndef TILEDB_QUERY_HILBERT_ORDER_H
#define TILEDB_QUERY_HILBERT_ORDER_H

#include "tiledb/common/common.h"
class Dimension;
class QueryBuffer;
class ResultCoords;

namespace tiledb::sm::hilbert_order {

uint64_t map_to_uint64(
    const Dimension& dim,
    const QueryBuffer* buff,
    uint64_t c,
    int bits,
    uint64_t max_bucket_val);

template <class RCType>
uint64_t map_to_uint64(
    const Dimension& dim,
    const RCType& coord,
    uint32_t dim_idx,
    int bits,
    uint64_t max_bucket_val);

}  // namespace tiledb::sm::hilbert_order
#endif  // TILEDB_QUERY_HILBERT_ORDER_H
