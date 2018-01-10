/**
 * @file   tiledb_cpp_api_array.h
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file declares the C++ API for the TileDB arrays.
 */

#ifndef TILEDB_CPP_API_ARRAY_H
#define TILEDB_CPP_API_ARRAY_H

#include "tiledb.h"
#include "tiledb_cpp_api_array_schema.h"
#include "tiledb_cpp_api_context.h"

namespace tdb {

/**
 * Consolidates the fragments of an array.
 *
 * @param ctx The TileDB context.
 * @param name The URI of the array.
 */
void consolidate_array(const Context& ctx, const std::string& array);

/**
 * Creates an array on disk from a schema definition.
 *
 * @param ctx The TileDB context.
 * @param name The URI of the array.
 * @param schema The array schema.
 */
void create_array(
    const Context& ctx, const std::string& array, const ArraySchema& schema);

}  // namespace tdb

#endif  // TILEDB_CPP_API_ARRAY_H
