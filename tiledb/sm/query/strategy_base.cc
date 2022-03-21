/**
 * @file   strategy_base.cc
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
 * This file implements class StrategyBase. It is a contract that defines the
 * operations that a query can call on readers or writers.
 */

#include "tiledb/sm/query/strategy_base.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"

namespace tiledb {
namespace sm {

/* ****************************** */
/*          CONSTRUCTORS          */
/* ****************************** */

StrategyBase::StrategyBase(
    stats::Stats* stats,
    tdb_shared_ptr<Logger> logger,
    StorageManager* storage_manager,
    Array* array,
    Config& config,
    std::unordered_map<std::string, QueryBuffer>& buffers,
    Subarray& subarray,
    Layout layout)
    : stats_(stats)
    , logger_(logger)
    , array_(array)
    , array_schema_(array->array_schema_latest())
    , config_(config)
    , buffers_(buffers)
    , layout_(layout)
    , storage_manager_(storage_manager)
    , subarray_(subarray)
    , offsets_format_mode_(Config::SM_OFFSETS_FORMAT_MODE)
    , offsets_extra_element_(false)
    , offsets_bitsize_(constants::cell_var_offset_size * 8) {
}

stats::Stats* StrategyBase::stats() const {
  return stats_;
}

/* ****************************** */
/*       PROTECTED METHODS        */
/* ****************************** */

void StrategyBase::get_dim_attr_stats() const {
  for (const auto& it : buffers_) {
    const auto& name = it.first;
    auto var_size = array_schema_.var_size(name);
    if (array_schema_.is_attr(name)) {
      stats_->add_counter("attr_num", 1);
      if (var_size) {
        stats_->add_counter("attr_var_num", 1);
      } else {
        stats_->add_counter("attr_fixed_num", 1);
      }
      if (array_schema_.is_nullable(name)) {
        stats_->add_counter("attr_nullable_num", 1);
      }
    } else {
      stats_->add_counter("dim_num", 1);
      if (var_size) {
        stats_->add_counter("dim_var_num", 1);
      } else {
        if (name == constants::coords) {
          stats_->add_counter("dim_zipped_num", 1);
        } else {
          stats_->add_counter("dim_fixed_num", 1);
        }
      }
    }
  }
}

std::string StrategyBase::offsets_mode() const {
  return offsets_format_mode_;
}

Status StrategyBase::set_offsets_mode(const std::string& offsets_mode) {
  offsets_format_mode_ = offsets_mode;

  return Status::Ok();
}

bool StrategyBase::offsets_extra_element() const {
  return offsets_extra_element_;
}

Status StrategyBase::set_offsets_extra_element(bool add_extra_element) {
  offsets_extra_element_ = add_extra_element;

  return Status::Ok();
}

uint32_t StrategyBase::offsets_bitsize() const {
  return offsets_bitsize_;
}

Status StrategyBase::set_offsets_bitsize(const uint32_t bitsize) {
  if (bitsize != 32 && bitsize != 64) {
    return logger_->status(Status_ReaderError(
        "Cannot set offset bitsize to " + std::to_string(bitsize) +
        "; Only 32 and 64 are acceptable bitsize values"));
  }

  offsets_bitsize_ = bitsize;
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
