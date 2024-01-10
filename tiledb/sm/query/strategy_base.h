/**
 * @file   strategy_base.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file defines class StrategyBase.
 */

#ifndef TILEDB_STRATEGY_BASE_H
#define TILEDB_STRATEGY_BASE_H

#include "tiledb/common/common.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/status.h"
#include "tiledb/common/usage_token.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/storage_manager/storage_manager_declaration.h"

namespace tiledb {
namespace sm {

class Array;
class ArraySchema;
enum class Layout : uint8_t;
class Subarray;
class QueryBuffer;

/** Processes read or write queries. */
class StrategyBase {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  StrategyBase(
      stats::Stats* stats,
      shared_ptr<Logger> logger,
      StorageManager* storage_manager,
      Array* array,
      Config& config,
      std::unordered_map<std::string, QueryBuffer>& buffers,
      Subarray& subarray,
      Layout layout);

  /** Destructor. */
  ~StrategyBase() = default;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns `stats_`. */
  stats::Stats* stats() const;

  /** Returns the configured offsets format mode. */
  std::string offsets_mode() const;

  /** Sets the offsets format mode. */
  Status set_offsets_mode(const std::string& offsets_mode);

  /** Returns `True` if offsets are configured to have an extra element. */
  bool offsets_extra_element() const;

  /** Sets if offsets are configured to have an extra element. */
  Status set_offsets_extra_element(bool add_extra_element);

  /** Returns the configured offsets bitsize */
  uint32_t offsets_bitsize() const;

  /** Sets the bitsize of offsets */
  Status set_offsets_bitsize(const uint32_t bitsize);

 protected:
  /* ********************************* */
  /*        PROTECTED ATTRIBUTES       */
  /* ********************************* */

  /** The class stats. */
  stats::Stats* stats_;

  /** The class logger. */
  shared_ptr<Logger> logger_;

  /** The array. */
  const Array* array_;

  tdb::UsageToken usage_token_;

  /** The array schema. */
  const ArraySchema& array_schema_;

  /** The config for query-level parameters only. */
  Config& config_;

  /**
   * Maps attribute/dimension names to their buffers.
   * `TILEDB_COORDS` may be used for the special zipped coordinates
   * buffer.
   * */
  std::unordered_map<std::string, QueryBuffer>& buffers_;

  /** The layout of the cells in the result of the subarray. */
  Layout layout_;

  /** The storage manager. */
  StorageManager* storage_manager_;

  /** The query subarray (initially the whole domain by default). */
  Subarray& subarray_;

  /** The offset format used for variable-sized attributes. */
  std::string offsets_format_mode_;

  /**
   * If `true`, an extra element that points to the end of the values buffer
   * will be added in the end of the offsets buffer of var-sized attributes
   */
  bool offsets_extra_element_;

  /** The offset bitsize used for variable-sized attributes. */
  uint32_t offsets_bitsize_;

  /* ********************************* */
  /*          PROTECTED METHODS        */
  /* ********************************* */

  /**
   * Gets statistics about the number of attributes and dimensions in
   * the query.
   */
  void get_dim_attr_stats() const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_STRATEGY_BASE_H
