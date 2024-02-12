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
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/storage_manager/storage_manager_declaration.h"

namespace tiledb {
namespace sm {

class OpenedArray;
class ArraySchema;
class IAggregator;
enum class Layout : uint8_t;
class MemoryTracker;
class Subarray;
class QueryBuffer;
class QueryCondition;

using DefaultChannelAggregates =
    std::unordered_map<std::string, shared_ptr<IAggregator>>;

/**
 * Class used to pass in common parameters to strategies. This will make it
 * easier to change parameters moving fowards.
 */
class StrategyParams {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  StrategyParams(
      StorageManager* storage_manager,
      shared_ptr<OpenedArray> array,
      Config& config,
      std::unordered_map<std::string, QueryBuffer>& buffers,
      std::unordered_map<std::string, QueryBuffer>& aggregate_buffers,
      Subarray& subarray,
      Layout layout,
      std::optional<QueryCondition>& condition,
      DefaultChannelAggregates& default_channel_aggregates,
      bool skip_checks_serialization,
      MemoryTracker* memory_tracker)
      : storage_manager_(storage_manager)
      , array_(array)
      , config_(config)
      , buffers_(buffers)
      , aggregate_buffers_(aggregate_buffers)
      , subarray_(subarray)
      , layout_(layout)
      , condition_(condition)
      , default_channel_aggregates_(default_channel_aggregates)
      , skip_checks_serialization_(skip_checks_serialization)
      , memory_tracker_(memory_tracker) {
  }

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Return the storage manager. */
  inline StorageManager* storage_manager() {
    return storage_manager_;
  };

  /** Return the array. */
  inline shared_ptr<OpenedArray> array() {
    return array_;
  };

  /** Return the config. */
  inline Config& config() {
    return config_;
  };

  /** Return the buffers. */
  inline std::unordered_map<std::string, QueryBuffer>& buffers() {
    return buffers_;
  };

  /** Return the aggregate buffers. */
  inline std::unordered_map<std::string, QueryBuffer>& aggregate_buffers() {
    return aggregate_buffers_;
  };

  /** Return the subarray. */
  inline Subarray& subarray() {
    return subarray_;
  };

  /** Return the layout. */
  inline Layout layout() {
    return layout_;
  };

  /** Return the condition. */
  inline std::optional<QueryCondition>& condition() {
    return condition_;
  }

  /** Return the default channel aggregates. */
  inline DefaultChannelAggregates& default_channel_aggregates() {
    return default_channel_aggregates_;
  }

  /** Return if we should skip checks for serialization. */
  inline bool skip_checks_serialization() {
    return skip_checks_serialization_;
  }

  inline MemoryTracker* memory_tracker() {
    return memory_tracker_;
  }

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** Storage manager. */
  StorageManager* storage_manager_;

  /** Array. */
  shared_ptr<OpenedArray> array_;

  /** Config for query-level parameters only. */
  Config& config_;

  /** Buffers. */
  std::unordered_map<std::string, QueryBuffer>& buffers_;

  /** Aggregate buffers. */
  std::unordered_map<std::string, QueryBuffer>& aggregate_buffers_;

  /** Query subarray (initially the whole domain by default). */
  Subarray& subarray_;

  /** Layout of the cells in the result of the subarray. */
  Layout layout_;

  /** Query condition. */
  std::optional<QueryCondition>& condition_;

  /** Default channel aggregates. */
  DefaultChannelAggregates& default_channel_aggregates_;

  /** Skip checks for serialization. */
  bool skip_checks_serialization_;

  /** Memory tracker. */
  MemoryTracker* memory_tracker_;
};

/** Processes read or write queries. */
class StrategyBase {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  StrategyBase(
      stats::Stats* stats, shared_ptr<Logger> logger, StrategyParams& params);

  /** Destructor. */
  ~StrategyBase() = default;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns `stats_`. */
  stats::Stats* stats() const;

  /**
   * Populate the owned stats instance with data.
   * To be removed when the class will get a C41 constructor.
   */
  void set_stats(const stats::StatsData& data);

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

  /**
   * A shared pointer to the opened array which ensures that the query can
   * still access it even after the array is closed.
   */
  shared_ptr<OpenedArray> array_;

  /** The array schema. */
  const ArraySchema& array_schema_;

  /** The config for query-level parameters only. */
  Config& config_;

  /**
   * Maps attribute/dimension names to their buffers.
   * `TILEDB_COORDS` may be used for the special zipped coordinates
   * buffer.
   */
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
