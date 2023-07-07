/**
 * @file   iaggregator.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file defines class IAggregator.
 */

#ifndef TILEDB_IAGGREGATOR_H
#define TILEDB_IAGGREGATOR_H

#include "tiledb/common/status.h"
#include "tiledb/sm/enums/layout.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class QueryBuffer;
class AggregateBuffer;
class IAggregator;

typedef std::unordered_map<std::string, shared_ptr<IAggregator>>
    DefaultChannelAggregates;

class IAggregator {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  virtual ~IAggregator() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the field name for the aggregator. */
  virtual std::string field_name() = 0;

  /** Returns if the aggregate needs to be recomputed on overflow. */
  virtual bool need_recompute_on_overflow() = 0;

  /** Returns if the aggregation is var sized or not. */
  virtual bool var_sized() = 0;

  /**
   * Validate the result buffer.
   *
   * @param output_field_name Name for the output buffer.
   * @param buffers Query buffers.
   */
  virtual void validate_output_buffer(
      std::string output_field_name,
      std::unordered_map<std::string, QueryBuffer>& buffers) = 0;

  /**
   * Aggregate data using the aggregator.
   *
   * @param input_data Input data for aggregation.
   */
  virtual void aggregate_data(AggregateBuffer& input_data) = 0;

  /**
   * Copy final data to the user buffer.
   *
   * @param output_field_name Name for the output buffer.
   * @param buffers Query buffers.
   */
  virtual void copy_to_user_buffer(
      std::string output_field_name,
      std::unordered_map<std::string, QueryBuffer>& buffers) = 0;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_IAGGREGATOR_H
