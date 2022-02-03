/**
 * @file tiledb/sm/query/hilbert_order.cc
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

#include "hilbert_order.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/result_coords.h"

namespace tiledb::sm::hilbert_order {

uint64_t map_to_uint64(
    const Dimension& dim,
    const QueryBuffer* buff,
    uint64_t c,
    int bits,
    uint64_t max_bucket_val) {
  auto d{buff->dimension_datum_at(dim, c)};
  return dim.map_to_uint64(
      d.datum().content(), d.datum().size(), bits, max_bucket_val);
}

uint64_t map_to_uint64(
    const Dimension& dim,
    const ResultCoords& coord,
    uint32_t dim_idx,
    int bits,
    uint64_t max_bucket_val) {
  auto d{coord.dimension_datum(dim, dim_idx)};
  return dim.map_to_uint64(d.content(), d.size(), bits, max_bucket_val);
}

}  // namespace tiledb::sm::hilbert_order
