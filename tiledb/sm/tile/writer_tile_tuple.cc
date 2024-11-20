/**
 * @file   writer_tile_tuple.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file implements class WriterTileTuple.
 */

#include "tiledb/sm/tile/writer_tile_tuple.h"
#include "tiledb/sm/array_schema/domain.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

WriterTileTuple::WriterTileTuple(
    const ArraySchema& array_schema,
    const uint64_t cell_num_per_tile,
    const bool var_size,
    const bool nullable,
    const uint64_t cell_size,
    const Datatype type,
    shared_ptr<MemoryTracker> memory_tracker,
    ContextResources* resources)
    : memory_tracker_(memory_tracker)
    , fixed_tile_(
          array_schema.write_version(),
          var_size ? constants::cell_var_offset_type : type,
          var_size ? constants::cell_var_offset_size : cell_size,
          var_size ? cell_num_per_tile * constants::cell_var_offset_size :
                     cell_num_per_tile * cell_size,
          memory_tracker_,
          resources)
    , cell_size_(cell_size)
    , var_pre_filtered_size_(0)
    , min_size_(0)
    , max_size_(0)
    , null_count_(0)
    , cell_num_(cell_num_per_tile) {
  if (var_size) {
    var_tile_.emplace(
        array_schema.write_version(),
        type,
        datatype_size(type),
        cell_num_per_tile * constants::cell_var_offset_size,
        memory_tracker_,
        resources);
  }

  if (nullable) {
    validity_tile_.emplace(
        array_schema.write_version(),
        constants::cell_validity_type,
        constants::cell_validity_size,
        cell_num_per_tile * constants::cell_validity_size,
        memory_tracker_,
        resources);
  }
}

/* ****************************** */
/*               API              */
/* ****************************** */

void WriterTileTuple::set_metadata(
    const void* min,
    const uint64_t min_size,
    const void* max,
    const uint64_t max_size,
    const ByteVec& sum,
    const uint64_t null_count) {
  min_.resize(min_size);
  min_size_ = min_size;
  if (min != nullptr) {
    memcpy(min_.data(), min, min_size);
  }

  max_.resize(max_size);
  max_size_ = max_size;
  if (max != nullptr) {
    memcpy(max_.data(), max, max_size);
  }

  sum_ = sum;
  null_count_ = null_count;

  if (var_tile_.has_value()) {
    var_pre_filtered_size_ = var_tile_->size();
  }
}

}  // namespace sm
}  // namespace tiledb
