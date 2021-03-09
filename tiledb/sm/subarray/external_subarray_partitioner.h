/**
 * @file   external_subarray_partitioner.h
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
 * This file defines class ExternalSubarrayPartitioner.
 */

#ifndef TILEDB_EXTERNAL_SUBARRAY_PARTITIONER_H
#define TILEDB_EXTERNAL_SUBARRAY_PARTITIONER_H

#include <list>

#include <unordered_map>
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * Iterates over partitions of a subarray in a way that the results
 * produced if the partition was submitted for a read query
 * can (approximately) fit the user-specified budget for various array
 * attributes. A partition returned by the partitioner (which works
 * similar to an iterator) is always a ``Subarray`` object. The
 * partitioner maintains certain state in order to be able to
 * produce the next partition until it is done.
 */
class ExternalSubarrayPartitioner {
 public:
  /* ********************************* */
  /*           TYPE DEFINITIONS        */
  /* ********************************* */

  //TBD: ???


  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ExternalSubarrayPartitioner();

  /** Constructor. */
  ExternalSubarrayPartitioner(
      const Subarray& subarray,
      uint64_t memory_budget,
      uint64_t memory_budget_var,
      uint64_t memory_budget_validity,
      ThreadPool* compute_tp);

  /** Destructor. */
  ~ExternalSubarrayPartitioner();

  /** Copy constructor. This performs a deep copy. */
  ExternalSubarrayPartitioner(const ExternalSubarrayPartitioner& partitioner);

  /** Move constructor. */
  ExternalSubarrayPartitioner(ExternalSubarrayPartitioner&& partitioner) noexcept;

  /** Copy-assign operator. This performs a deep copy. */
  ExternalSubarrayPartitioner& operator=(const ExternalSubarrayPartitioner& partitioner);

  /** Move-assign operator. */
  ExternalSubarrayPartitioner& operator=(ExternalSubarrayPartitioner&& partitioner) noexcept;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns the current partition. */
  Subarray& current();

  /** Returns the current partition info. */
  const SubarrayPartitioner::PartitionInfo* current_partition_info() const;

  /** Returns the current partition info. */
  SubarrayPartitioner::PartitionInfo* current_partition_info();

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
  const std::unordered_map<std::string, SubarrayPartitioner::ResultBudget>* get_result_budgets()
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
  const SubarrayPartitioner::State* state() const;

  /** Returns the state. */
  SubarrayPartitioner::State* state();

  /** Returns the subarray. */
  const Subarray* subarray() const;

  /** Returns the subarray. */
  Subarray* subarray();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** */
  SubarrayPartitioner subarray_partitioner_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Returns a deep copy of this ExternalSubarrayPartitioner. */
  ExternalSubarrayPartitioner clone() const;

  /**
   * Swaps the contents (all field values) of this subarray partitioner with
   * the given partitioner.
   */
  void swap(ExternalSubarrayPartitioner& partitioner);

};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_EXTERNAL_SUBARRAY_PARTITIONER_H
