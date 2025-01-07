/**
 * @file   type_traits.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * Provides type traits (concepts) which contain the interface requirements
 * for various templates.
 */

#ifndef TILEDB_COMPARE_TRAITS_H
#define TILEDB_COMPARE_TRAITS_H

#include "tiledb/common/types/untyped_datum.h"
#include "tiledb/sm/array_schema/dimension.h"

namespace tiledb::sm {

/**
 * Concept describing types which can be ordered using `CellCmpBase`
 * (see `tiledb/sm/misc/comparators.h`)
 */
template <typename T>
concept CellComparable =
    requires(const T& a, const Dimension& dim, unsigned dim_idx) {
      { a.dimension_datum(dim, dim_idx) } -> std::same_as<UntypedDatumView>;
    };

/**
 * Concept describing types which can be ordered using `GlobalCellCmp`.
 * (see `tiledb/sm/misc/comparators.h`)
 */
template <typename T>
concept GlobalCellComparable =
    CellComparable<T> and
    requires(const T& a, const Dimension& dim, unsigned d) {
      { a.coord(d) } -> std::convertible_to<const void*>;
    };

}  // namespace tiledb::sm

#endif
