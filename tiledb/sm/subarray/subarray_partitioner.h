/**
 * @file   subarray_partitioner.h
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
 * This file defines class SubarrayPartitioner.
 */

#ifndef TILEDB_SUBARRAY_PARTITIONER_H
#define TILEDB_SUBARRAY_PARTITIONER_H

#include <list>

#include <unordered_map>
#include "tiledb/common/common.h"
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/subarray/subarray.h"

using namespace tiledb::common;

namespace tiledb::sm {

/**
 * Iterates over partitions of a subarray in a way that the results
 * produced if the partition was submitted for a read query
 * can (approximately) fit the user-specified budget for various array
 * attributes. A partition returned by the partitioner (which works
 * similar to an iterator) is always a ``Subarray`` object. The
 * partitioner maintains certain state in order to be able to
 * produce the next partition until it is done.
 */
class SubarrayPartitioner {
 public:
  /* ********************************* */
  /*           TYPE DEFINITIONS        */
  /* ********************************* */

  /**
   * Result budget (in bytes) for an attribute/dimension used for
   * partitioning.
   */
  struct ResultBudget {
    /**
     * Size for fixed-sized attributes/dimensions or offsets of var-sized
     * attributes/dimensions.
     */
    uint64_t size_fixed_;

    /** Size of values for var-sized attributes/dimensions. */
    uint64_t size_var_;

    /** Size of validity for nullable attributes. */
    uint64_t size_validity_;
  };

  /**
   * Stores information about the current partition. A partition is
   * always a ``Subarray`` object. In addition to that object, this
   * struct contains also some information about the interval of
   * ranges from the original subarray that the partition has
   * been constructed from. This interval ``[start_, end_]`` refers
   * to the indices of the ranges from the original subarray in
   * their flattened 1D order as specified by the layout of the
   * subarray. This additional information helps to potentially
   * further split the current partition, if the read query
   * deems it necessary (i.e., this will be used to update the
   * partitioner state as well).
   */
  struct PartitionInfo {
    /** The current partition. */
    Subarray partition_;
    /**
     * The start range index from the original subarray that the
     * current partition has been constructed from.
     */
    uint64_t start_;

    /**
     * The end range index from the original subarray that the
     * current partition has been constructed from. This is an
     * inclusive index.
     */
    uint64_t end_;

    /**
     * ``true`` if the partition came from splitting a multi-range
     * subarray that was put into ``state_.multi_range_``.
     */
    bool split_multi_range_ = false;
  };

  /**
   * Stores the current state of the partitioner, which will be
   * used to derive the next partition when requested. This involves
   * the range interval from the original subarray that the next
   * partition will be constructed from, as well as a list of
   * single-range subarrays. The latter is used in the case where
   * a partition was computed on a single-range subarray that
   * had to be split further. The list contains all the subarrays
   * that resulted throughout this splitting process and are
   * next in line to produce the next partition.
   */
  struct State {
    /**
     * The start range index from the original subarray that the
     * next partition will be constructed from.
     */
    uint64_t start_;
    /**
     * The end range index from the original subarray that the
     * next partition will be constructed from.
     */
    uint64_t end_;
    /**
     * This is a list of subarrays that resulted from splitting a
     * single-range subarray to produce the current partition. The
     * list stores the remaining single-range subarray as a set
     * of single-range partitions that need to be explored next.
     */
    std::list<Subarray> single_range_;
    /**
     * This is a list of subarrays that resulted from splitting a
     * multi-range subarray "slab" to produce the current partition
     * (applicable only to ROW_MAJOR and COL_MAJOR layouts).
     * The list stores the remaining multi-range subarray slab as a
     * set of multi-range partitions that need to be explored next.
     */
    std::list<Subarray> multi_range_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  SubarrayPartitioner();

  /** Constructor. */
  SubarrayPartitioner(
      const Config* config,
      const Subarray& subarray,
      uint64_t memory_budget,
      uint64_t memory_budget_var,
      uint64_t memory_budget_validity,
      ThreadPool* compute_tp,
      stats::Stats* parent_stats,
      shared_ptr<Logger> logger);

  /** Destructor. */
  ~SubarrayPartitioner();

  /** Copy constructor. This performs a deep copy. */
  SubarrayPartitioner(const SubarrayPartitioner& partitioner);

  /** Move constructor. */
  SubarrayPartitioner(SubarrayPartitioner&& partitioner) noexcept;

  /** Copy-assign operator. This performs a deep copy. */
  SubarrayPartitioner& operator=(const SubarrayPartitioner& partitioner);

  /** Move-assign operator. */
  SubarrayPartitioner& operator=(SubarrayPartitioner&& partitioner) noexcept;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns the current partition. */
  const Subarray& current() const;

  /** Returns the current partition. */
  Subarray& current();

  /** Returns the current partition info. */
  const PartitionInfo* current_partition_info() const;

  /** Returns the current partition info. */
  PartitionInfo* current_partition_info();

  /**
   * Returns ``true`` if there are no more partitions, i.e., if the
   * partitioner iterator is done.
   */
  bool done() const;

  /**
   * Gets result size budget (in bytes) for the input fixed-sized
   * attribute/dimension.
   */
  Status get_result_budget(const char* name, uint64_t* budget) const;

  /**
   * Gets result size budget (in bytes) for the input var-sized
   * attribute/dimension.
   */
  Status get_result_budget(
      const char* name, uint64_t* budget_off, uint64_t* budget_val) const;

  /**
   * Gets result size budget (in bytes) for the input fixed-sized
   * nullable attribute.
   */
  Status get_result_budget_nullable(
      const char* name, uint64_t* budget, uint64_t* budget_validity) const;

  /**
   * Gets result size budget (in bytes) for the input var-sized
   * nullable attribute.
   */
  Status get_result_budget_nullable(
      const char* name,
      uint64_t* budget_off,
      uint64_t* budget_val,
      uint64_t* budget_validity) const;

  /**
   * Returns a pointer to mapping containing all attribute/dimension result
   * budgets that have been set.
   */
  const std::unordered_map<std::string, ResultBudget>* get_result_budgets()
      const;

  /**
   * Gets the memory budget (in bytes).
   *
   * @param budget The budget for the fixed-sized attributes and the
   *     offsets of the var-sized attributes.
   * @param budget_var The budget for the var-sized attributes.
   * @return Status
   */
  Status get_memory_budget(
      uint64_t* budget, uint64_t* budget_var, uint64_t* budget_validity) const;

  /**
   * The partitioner iterates over the partitions of the subarray it is
   * associated with. This function advances to compute the next partition
   * based on the specified budget. If this cannot be retrieved because
   * the current partition cannot be split further (typically because it
   * is a single cell whose estimated result does not fit in the budget),
   * then the function does not advance to the next partition and sets
   * ``unsplittable`` to ``true``.
   */
  Status next(bool* unsplittable);

  /**
   * Sets the memory budget (in bytes).
   *
   * @param budget The budget for the fixed-sized attributes and the
   *     offsets of the var-sized attributes.
   * @param budget_var The budget for the var-sized attributes.
   * @param budget_validity The budget for validity vectors.
   * @return Status
   */
  Status set_memory_budget(
      uint64_t budget, uint64_t budget_var, uint64_t budget_validity);

  /**
   * Sets result size budget (in bytes) for the input fixed-sized
   * attribute/dimension.
   */
  Status set_result_budget(const char* name, uint64_t budget);

  /**
   * Sets result size budget (in bytes) for the input var-sized
   * attribute/dimension.
   */
  Status set_result_budget(
      const char* name, uint64_t budget_off, uint64_t budget_val);

  /**
   * Sets result size budget (in bytes) for the input fixed-sized,
   * nullable attribute.
   */
  Status set_result_budget_nullable(
      const char* name, uint64_t budget, uint64_t budget_validity);

  /**
   * Sets result size budget (in bytes) for the input var-sized
   * nullable attribute.
   */
  Status set_result_budget_nullable(
      const char* name,
      uint64_t budget_off,
      uint64_t budget_val,
      uint64_t budget_validity);

  /**
   * Splits the current partition and updates the state, retrieving
   * a new current partition. This function is typically called
   * by the reader when the current partition was estimated to fit
   * the results, but that was not eventually true.
   */
  Status split_current(bool* unsplittable);

  /** Returns the state. */
  const State* state() const;

  /** Returns the state. */
  State* state();

  /** Returns the subarray. */
  const Subarray& subarray() const;

  /** Returns the subarray. */
  Subarray& subarray();

  /** Returns `stats_`. */
  const stats::Stats& stats() const;

  /** Populate the owned stats instance with data.
   * To be removed when the class will get a C41 constructor.
   *
   * @param data Data to populate the stats with.
   */
  void set_stats(const stats::StatsData& data);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The class stats. */
  stats::Stats* stats_;

  /** UID of the logger instance */
  inline static std::atomic<uint64_t> logger_id_ = 0;

  /** The class logger. */
  shared_ptr<Logger> logger_;

  /** The config. */
  const Config* config_;

  /** The subarray the partitioner will iterate on to produce partitions. */
  Subarray subarray_;

  /** Result size budget (in bytes) for all attributes/dimensions. */
  std::unordered_map<std::string, ResultBudget> budget_;

  /** The current partition info. */
  PartitionInfo current_;

  /** The state information for the remaining partitions to be produced. */
  State state_;

  /**
   * The memory budget for the fixed-sized attributes and the offsets
   * of the var-sized attributes.
   */
  uint64_t memory_budget_;

  /** The memory budget for the var-sized attributes. */
  uint64_t memory_budget_var_;

  /** The memory budget for the validity vectors. */
  uint64_t memory_budget_validity_;

  /**
   * If true, do not consider estimated result sizes when
   * determining if a partition should be split.
   */
  bool skip_split_on_est_size_;

  /**
   * If true, do not consider a memory budget overflow an error when the
   * partition is a unary range.
   */
  bool skip_unary_partitioning_budget_check_;

  /** The thread pool for compute-bound tasks. */
  ThreadPool* compute_tp_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * After computation of the ``[current_.start_, current_.end_]`` interval
   * of ranges (in the 1D flattened order) of the subarray that fit in the
   * budget, this function must calibrate ``current_.end_`` so that the
   * interval corresponds to either (i) a full slab of ranges, i.e.,
   * full rows or columns (depending on the layout) of ranges, or (ii) a
   * single partial row or column (applicable only to UNORDERED layout) of
   * the ranges. The reason is that the next partition to be stored in
   * ``current_.partition_`` must always have a proper ``Subarray`` structure,
   * consisting of a set of 1D ranges per dimension, that all together form
   * a set of multiple ND ranges (produced by the cross produce of the 1D
   * ranges).
   *
   * For ROW_MAJOR and COL_MAJOR layouts, this function may set
   * ``must_split_slab`` to ``true`` if after calibrating the range interval
   * to form a "slab", that slab does not entirely fit in the result budget
   * and needs splitting along the splitting dimension (that depends on
   * the layout).
   */
  Status calibrate_current_start_end(bool* must_split_slab);

  /** Returns a deep copy of this SubarrayPartitioner. */
  SubarrayPartitioner clone() const;

  /**
   * Computes the range interval ``[current_.start_, current_.end_]``
   * needed to compute the next partition to set to ``current_.partition_``.
   *
   * If the interval is a single range, which does not fit in the budget,
   * then the function sets ``found`` to ``false`` and ``true`` otherwise.
   */
  Status compute_current_start_end(bool* found);

  /**
   * Applicable only when the `range` layout is GLOBAL_ORDER.
   * Computes the splitting value and dimension for the input range.
   * If `range` is whithin a single space tile, then `unsplittable`
   * is set to `true`.
   */
  void compute_splitting_value_on_tiles(
      const Subarray& range,
      unsigned* splitting_dim,
      ByteVecValue* splitting_value,
      bool* unsplittable);

  /**
   * Computes the splitting value and dimension for the input range.
   * In case of real domains, if this function may not be able to find a
   * splitting value and set ``unsplittable`` to ``true``. Value
   * `normal_order` is `true` if after the split, the first range
   * precedes the second in the query layout. Otherwise, it is
   * the reverse order (this is used in global order reads when
   * the cell order is Hilbert).
   */
  void compute_splitting_value_single_range(
      const Subarray& range,
      unsigned* splitting_dim,
      ByteVecValue* splitting_value,
      bool* normal_order,
      bool* unsplittable);

  /**
   * Same as `compute_splitting_value_single_range` but this is applicable
   * only to global order reads when the cell order is Hilbert.
   */
  void compute_splitting_value_single_range_hilbert(
      const Subarray& range,
      unsigned* splitting_dim,
      ByteVecValue* splitting_value,
      bool* normal_order,
      bool* unsplittable);

  /**
   * Computes the splitting value and dimension for
   * ``state_.multi_range_.front()``. In case of real domains, if this
   * function may not be able to find a splitting value and set
   * ``unsplittable`` to ``true``. Value
   * `normal_order` is `true` if after the split, the first range
   * precedes the second in the query layout. Otherwise, it is
   * the reverse order (this is used in global order reads when
   * the cell order is Hilbert).
   */
  Status compute_splitting_value_multi_range(
      unsigned* splitting_dim,
      uint64_t* splitting_range,
      ByteVecValue* splitting_value,
      bool* normal_order,
      bool* unsplittable);

  /** Returns ``true`` if the input partition must be split. */
  bool must_split(Subarray* partition);

  /**
   * It computes the next partition from a multi-range subarray, which may
   * need to be split and added to the list of multi-range subarray
   * partitions. If the next partition cannot be produced,
   * ``unsplittable`` is set to ``true``.
   */
  Status next_from_multi_range(bool* unsplittable);

  /**
   * It handles the case where ``state_.single_range_`` is non-empty, which
   * means that the next partition must be produced from the remaining
   * single-range subarray represented by ``state_.single_range_``.
   * If the next partition cannot be produced, ``unsplittable`` is set
   * to ``true``.
   */
  Status next_from_single_range(bool* unsplittable);

  /**
   * Splits the top single range, or sets ``unsplittable`` to ``true``
   * if that is not possible.
   */
  Status split_top_single_range(bool* unsplittable);

  /**
   * Splits the top multi-range, or sets ``unsplittable`` to ``true``
   * if that is not possible.
   */
  Status split_top_multi_range(bool* unsplittable);

  /**
   * Swaps the contents (all field values) of this subarray partitioner with
   * the given partitioner.
   */
  void swap(SubarrayPartitioner& partitioner);

  /**
   * Maps the input `range` to `range_uint64` that uses only
   * uint64 values, with the number of bits calculated by
   * the Hilbert order on the array dimensions. These values
   * will be used as coordinates to calculate Hilbert values.
   *
   * @param range The input ND range.
   * @param range_uint64 The mapped ND range to be calculated.
   * @param unsplittable Set to `true` if the mapped ND `range_uint64`
   *     is unary and `false` otherwise.
   * @return void
   */
  void compute_range_uint64(
      const Subarray& range,
      std::vector<std::array<uint64_t, 2>>* range_uint64,
      bool* unsplittable) const;

  /**
   * Calculates the splitting dimension for Hilbert cell order,
   * based on the mapped uint64 range.
   */
  void compute_splitting_dim_hilbert(
      const std::vector<std::array<uint64_t, 2>>& range_uint64,
      uint32_t* splitting_dim) const;

  /**
   * Given the input mapped `range_uint64` on the splitting
   * dimension, it calcuates the real spliting value
   * for the original range (i.e., in the original dimension domain,
   * not the mapped uint64 domain).
   */
  void compute_splitting_value_hilbert(
      const std::array<uint64_t, 2>& range_uint64,
      uint32_t splitting_dim,
      ByteVecValue* splitting_value) const;
};

}  // namespace tiledb::sm

#endif  // TILEDB_SUBARRAY_PARTITIONER_H
