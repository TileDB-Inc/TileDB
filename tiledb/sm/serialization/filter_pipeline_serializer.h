/**
 * @file   filter_pipeline_serializer.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * This file declares serialization functions for FilterPipeline.
 */

#ifndef TILEDB_SERIALIZATION_FILTER_PIPELINE_H
#define TILEDB_SERIALIZATION_FILTER_PIPELINE_H

#include <unordered_map>

#include "tiledb/common/status.h"

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {
namespace serialization {

Status filter_pipeline_serialize_to_capnp(
    const FilterPipeline* filter_pipeline,
    capnp::FilterPipeline::Builder* filter_pipeline_builder);

Status filter_pipeline_deserialize_from_capnp(
    const capnp::FilterPipeline::Reader& filter_pipeline_reader,
    tdb_unique_ptr<FilterPipeline>* filter_pipeline);


}  // namespace serialization
}  // namespace sm
}  // namespace tiledb

#endif