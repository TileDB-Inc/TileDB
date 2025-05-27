/**
 * @file tiledb/type/datatype_queries.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file provides functions which answer queries about Datatype
 */

#ifndef TILEDB_DATATYPE_QUERIES_H
#define TILEDB_DATATYPE_QUERIES_H

#include "tiledb/type/apply_with_type.h"
#include "tiledb/type/datatype_traits.h"

namespace tiledb::type {

using tiledb::sm::Datatype;

/**
 * @return whether the `value_type` for a given `Datatype` is signed
 */
static bool has_signed_value_type(Datatype dt) {
  return apply_with_type(
      [](auto value_type) -> bool {
        return std::is_signed_v<decltype(value_type)>;
      },
      dt);
}

}  // namespace tiledb::type

#endif
