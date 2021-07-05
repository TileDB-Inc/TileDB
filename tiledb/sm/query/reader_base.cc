/**
 * @file   reader_base.cc
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
 * This file implements class ReaderBase.
 */

#include "tiledb/sm/query/reader_base.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/query/strategy_base.h"
#include "tiledb/sm/subarray/subarray.h"

namespace tiledb {
namespace sm {

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

ReaderBase::ReaderBase(
    stats::Stats* stats,
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout,
    QueryCondition& condition)
    : StrategyBase(
          stats, storage_manager, array, config, buffers, subarray, layout)
    , condition_(condition) {
  if (array != nullptr)
    fragment_metadata_ = array->fragment_metadata();
}

/* ****************************** */
/*        PROTECTED METHODS       */
/* ****************************** */

void ReaderBase::reset_buffer_sizes() {
  for (auto& it : buffers_) {
    *(it.second.buffer_size_) = it.second.original_buffer_size_;
    if (it.second.buffer_var_size_ != nullptr)
      *(it.second.buffer_var_size_) = it.second.original_buffer_var_size_;
    if (it.second.validity_vector_.buffer_size() != nullptr)
      *(it.second.validity_vector_.buffer_size()) =
          it.second.original_validity_vector_size_;
  }
}

void ReaderBase::zero_out_buffer_sizes() {
  for (auto& buffer : buffers_) {
    if (buffer.second.buffer_size_ != nullptr)
      *(buffer.second.buffer_size_) = 0;
    if (buffer.second.buffer_var_size_ != nullptr)
      *(buffer.second.buffer_var_size_) = 0;
    if (buffer.second.validity_vector_.buffer_size() != nullptr)
      *(buffer.second.validity_vector_.buffer_size()) = 0;
  }
}

Status ReaderBase::check_subarray() const {
  if (subarray_.layout() == Layout::GLOBAL_ORDER && subarray_.range_num() != 1)
    return LOG_STATUS(Status::ReaderError(
        "Cannot initialize reader; Multi-range subarrays with "
        "global order layout are not supported"));

  return Status::Ok();
}

Status ReaderBase::check_validity_buffer_sizes() const {
  // Verify that the validity buffer size for each
  // nullable attribute is large enough to contain
  // a validity value for each cell.
  for (const auto& it : buffers_) {
    const std::string& name = it.first;
    if (array_schema_->is_nullable(name)) {
      const uint64_t buffer_size = *it.second.buffer_size_;

      uint64_t min_cell_num = 0;
      if (array_schema_->var_size(name)) {
        min_cell_num = buffer_size / constants::cell_var_offset_size;

        // If the offsets buffer contains an extra element to mark
        // the offset to the end of the data buffer, we do not need
        // a validity value for that extra offset.
        if (offsets_extra_element_)
          min_cell_num = std::min<uint64_t>(0, min_cell_num - 1);
      } else {
        min_cell_num = buffer_size / array_schema_->cell_size(name);
      }

      const uint64_t buffer_validity_size =
          *it.second.validity_vector_.buffer_size();
      const uint64_t cell_validity_num =
          buffer_validity_size / constants::cell_validity_size;

      if (cell_validity_num < min_cell_num) {
        std::stringstream ss;
        ss << "Buffer sizes check failed; Invalid number of validity cells "
              "given for ";
        ss << "attribute '" << name << "'";
        ss << " (" << cell_validity_num << " < " << min_cell_num << ")";
        return LOG_STATUS(Status::ReaderError(ss.str()));
      }
    }
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
