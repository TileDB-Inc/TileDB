/**
 * @file   strategy_base.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/storage_manager/cancellation_source.h"
#include "tiledb/sm/storage_manager/context_resources.h"

namespace tiledb::sm {

class OpenedArray;
class ArraySchema;
class IAggregator;
enum class Layout : uint8_t;
class LocalQueryStateMachine;
class MemoryTracker;
class Subarray;
class QueryBuffer;

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
      ContextResources& resources,
      shared_ptr<MemoryTracker> array_memory_tracker,
      shared_ptr<MemoryTracker> query_memory_tracker,
      LocalQueryStateMachine& query_state_machine,
      CancellationSource cancellation_source,
      shared_ptr<OpenedArray> array,
      Config& config,
      optional<uint64_t> memory_budget,
      std::unordered_map<std::string, QueryBuffer>& buffers,
      std::unordered_map<std::string, QueryBuffer>& aggregate_buffers,
      Subarray& subarray,
      Layout layout,
      QueryPredicates& predicates,
      DefaultChannelAggregates& default_channel_aggregates,
      bool skip_checks_serialization)
      : resources_(resources)
      , array_memory_tracker_(array_memory_tracker)
      , query_memory_tracker_(query_memory_tracker)
      , query_state_machine_(query_state_machine)
      , cancellation_source_(cancellation_source)
      , array_(array)
      , config_(config)
      , memory_budget_(memory_budget)
      , buffers_(buffers)
      , aggregate_buffers_(aggregate_buffers)
      , subarray_(subarray)
      , layout_(layout)
      , predicates_(predicates)
      , default_channel_aggregates_(default_channel_aggregates)
      , skip_checks_serialization_(skip_checks_serialization) {
  }

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Accessor for the resources.
   */
  inline ContextResources& resources() {
    return resources_;
  }

  /** Return the array memory tracker. */
  inline shared_ptr<MemoryTracker> array_memory_tracker() {
    return array_memory_tracker_;
  }

  inline shared_ptr<MemoryTracker> query_memory_tracker() {
    return query_memory_tracker_;
  }

  inline LocalQueryStateMachine& query_state_machine() const {
    return query_state_machine_;
  }

  inline CancellationSource cancellation_source() const {
    return cancellation_source_;
  }

  /** Return the array. */
  inline shared_ptr<OpenedArray> array() {
    return array_;
  };

  /** Return the config. */
  inline Config& config() {
    return config_;
  };

  /** Return the memory budget, if set. */
  inline optional<uint64_t> memory_budget() {
    return memory_budget_;
  }

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
    return predicates_.condition_;
  }

  /** Return the default channel aggregates. */
  inline DefaultChannelAggregates& default_channel_aggregates() {
    return default_channel_aggregates_;
  }

  /** Return if we should skip checks for serialization. */
  inline bool skip_checks_serialization() {
    return skip_checks_serialization_;
  }

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** Resources used to perform operations. */
  ContextResources& resources_;

  /** Array Memory tracker. */
  shared_ptr<MemoryTracker> array_memory_tracker_;

  /** Query Memory tracker. */
  shared_ptr<MemoryTracker> query_memory_tracker_;

  /** Query state machine */
  LocalQueryStateMachine& query_state_machine_;

  /** Cancellation source */
  CancellationSource cancellation_source_;

  /** Array. */
  shared_ptr<OpenedArray> array_;

  /** Config for query-level parameters only. */
  Config& config_;

  /**
   * Memory budget for the query. If set to nullopt, the value will be obtained
   * from the sm.mem.total_budget config option.
   */
  optional<uint64_t> memory_budget_;

  /** Buffers. */
  std::unordered_map<std::string, QueryBuffer>& buffers_;

  /** Aggregate buffers. */
  std::unordered_map<std::string, QueryBuffer>& aggregate_buffers_;

  /** Query subarray (initially the whole domain by default). */
  Subarray& subarray_;

  /** Layout of the cells in the result of the subarray. */
  Layout layout_;

  /** Query predicates. */
  QueryPredicates& predicates_;

  /** Default channel aggregates. */
  DefaultChannelAggregates& default_channel_aggregates_;

  /** Skip checks for serialization. */
  bool skip_checks_serialization_;
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
  inline stats::Stats* stats() const {
    return stats_;
  }

  /**
   * Populate the owned stats instance with data.
   * To be removed when the class will get a C41 constructor.
   *
   * @param data Data to populate the stats with.
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

  /**
   * Cancel any ongoing processing at the next opportunity.
   */
  void cancel();

 protected:
  /* ********************************* */
  /*        PROTECTED ATTRIBUTES       */
  /* ********************************* */

  /** Resources used for operations. */
  ContextResources& resources_;

  /** The array memory tracker. */
  shared_ptr<MemoryTracker> array_memory_tracker_;

  /** The query memory tracker. */
  shared_ptr<MemoryTracker> query_memory_tracker_;

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

  /**
   * State machine of the query under which this strategy is executing.
   *
   * Execution of query operation may be interrupted by asynchronous events that
   * are tracked through the state machine. Operations should poll the state
   * machine periodically and cease processing if the query has been cancelled,
   * for example, or is otherwise not in a state where processing should
   * proceed.
   *
   * @section Maturity
   *
   * At present the state machine cannot be declared `const` because the
   * cancellation event must be generated at the same point where the
   * cancellation source is checked. When the cancellation source goes away,
   * query code will only need to check the state and will no longer need to
   * generate events.
   */
  LocalQueryStateMachine& query_state_machine_;

  /**
   * The source for external cancellation events.
   *
   * @section Maturity
   *
   * This is a transitional member variable. It's required at present because
   * the presence of a cancellation is held at the context level and must be
   * polled for. When cancellation is pushed down from the top, there will no
   * longer be a need for this variable.
   */
  CancellationSource cancellation_source_;

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

  /**
   * Throws an exception if the query is cancelled.
   */
  void throw_if_cancelled() const;

  /**
   * Predicate function whether the query has been cancelled.
   */
  bool cancelled() const;

  /**
   * Process any pending external cancellation order.
   *
   * If there's a pending external cancellation, this function generates a
   * `cancel` event on the local state machine of the query being processed.
   */
  void process_external_cancellation();
};

}  // namespace tiledb::sm

#endif  // TILEDB_STRATEGY_BASE_H
