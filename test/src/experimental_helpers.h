/**
 * @file   experimental_helpers.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file declares some test suite helper functions for experimental
 * features.
 */

#ifndef TILEDB_TEST_EXPERIMENTAL_HELPERS_H
#define TILEDB_TEST_EXPERIMENTAL_HELPERS_H

#include "catch.hpp"
#include "helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/c_api/experimental/tiledb_dimension_label.h"
#include "tiledb/sm/c_api/experimental/tiledb_struct_def.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "vfs_helpers.h"

namespace tiledb::test {

/**
 * Helper method for adding an internal dimension label to an array schema.
 *
 * @param ctx TileDB context.
 * @param array_schema The array schema to add the dimension label to.
 * @param label_name The name of the dimension label.
 * @param dim_idx The dimension index to add the label on.
 * @param label_order The label order for the dimension label.
 * @param label_domain The dimension label domain.
 * @param label_tile_extent The dimension tile extent for the label.
 * @param index_tile_extent The dimension label tile extent for the index.
 */
void add_dimension_label(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const std::string& label_name,
    const uint32_t dim_idx,
    tiledb_label_order_t label_order,
    tiledb_datatype_t label_datatype,
    const void* label_domain,
    const void* label_tile_extent,
    const void* index_tile_extent);

}  // namespace tiledb::test

#endif
