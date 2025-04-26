/**
 * @file unit_filter_pipeline_rs.h
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
 * Class for generating data on a writer data and checking for that same data
 * on another tile.
 */

#ifndef TILEDB_UNIT_FILTER_PIPELINE_RS_H
#define TILEDB_UNIT_FILTER_PIPELINE_RS_H

#include "cxxbridge/rust/cxx.h"
#include "tiledb/api/c_api/api_external_common.h"
#include "tiledb/sm/filter/filter_pipeline.h"

namespace tiledb::sm::test {

/**
 * @return a filter pipeline used in SC-65154
 */
TILEDB_EXPORT std::unique_ptr<FilterPipeline> build_pipeline_65154();

/**
 * Runs `check_run_pipeline_roundtrip` from `filter_test_support.h` against
 * some Rust data
 */
TILEDB_EXPORT void filter_pipeline_roundtrip(
    const FilterPipeline& pipeline, rust::Slice<const uint8_t> data);

}  // namespace tiledb::sm::test

#endif
