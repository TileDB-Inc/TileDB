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
 * This file defines C++ API for the TileDB SubarrayPartitioner object.
 */

#ifndef TILEDB_CPP_API_SUBARRAY_PARTITIONER_H
#define TILEDB_CPP_API_SUBARRAY_PARTITIONER_H

#include "array.h"
#include "context.h"
#include "core_interface.h"
#include "deleter.h"
#include "exception.h"
#include "subarray.h"
#include "tiledb.h"
#include "type.h"
#include "utils.h"

namespace tiledb {

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

  // TBD: ???

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  SubarrayPartitioner();

  /** Constructor. */
  SubarrayPartitioner(
      const Context& ctx,
      const Subarray& subarray,
      uint64_t memory_budget,
      uint64_t memory_budget_var,
      uint64_t memory_budget_validity)
      : ctx_(ctx)
      , subarray_(subarray) {
    tiledb_subarray_partitioner_t* capi_subarray_partitioner;
    ctx.handle_error(tiledb_subarray_partitioner_alloc(
        ctx.ptr().get(),
        subarray.ptr().get(),
        &capi_subarray_partitioner,
        memory_budget,
        memory_budget_var,
        memory_budget_validity));
    subarray_partitioner_ = std::shared_ptr<tiledb_subarray_partitioner_t>(
        capi_subarray_partitioner, deleter_);
  }

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Sets the layout of the associated subarray.
   *
   * @param layout When used with a write query, this specifies the order of the
   * cells provided by the user in the buffers. For a read query, this specifies
   *     the order of the cells that will be retrieved as results and stored
   *     in the user buffers. The layout can be one of the following:
   *    - `TILEDB_COL_MAJOR`:
   *      This means column-major order with respect to the subarray.
   *    - `TILEDB_ROW_MAJOR`:
   *      This means row-major order with respect to the subarray.
   *    - `TILEDB_GLOBAL_ORDER`:
   *      This means that cells are stored or retrieved in the array global
   *      cell order.
   *    - `TILEDB_UNORDERED`:
   *      This is applicable only to writes for sparse arrays, or for sparse
   *      writes to dense arrays. It specifies that the cells are unordered and,
   *      hence, TileDB must sort the cells in the global cell order prior to
   *      writing.
   * @return Reference to this Subarray
   */
  SubarrayPartitioner& set_layout(tiledb_layout_t layout) {
    ctx_.get().handle_error(tiledb_subarray_partitioner_set_layout(
        ctx_.get().ptr().get(), layout, subarray_partitioner_.get()));
    return *this;
  }

  /**
   * Compute a complete series of subarray partitions, retained to be accessed
   * with get_partition().
   */
  SubarrayPartitioner& compute() {
    ctx_.get().handle_error(tiledb_subarray_partitioner_compute(
        ctx_.get().ptr().get(), subarray_partitioner_.get()));
    return *this;
  }

  /**
   * Get the number of partitions in the currently computed series.
   *
   * @return the number of partitions available
   */
  int32_t get_partition_num() {
    uint64_t num;
    ctx_.get().handle_error(tiledb_subarray_partitioner_get_partition_num(
        ctx_.get().ptr().get(), &num, subarray_partitioner_.get()));
    return num;
  }

  /**
   * Retrieve a (subarray) partition from within the currently computed series.
   */
  SubarrayPartitioner& get_partition(
      uint64_t part_id, Subarray& retrieved_subarray) {
    ctx_.get().handle_error(tiledb_subarray_partitioner_get_partition(
        ctx_.get().ptr().get(),
        subarray_partitioner_.get(),
        part_id,
        retrieved_subarray.capi_subarray()));
    return *this;
  }

  /**
   * Sets result size budget (in bytes) for the input fixed-sized
   * attribute/dimension.
   */
  SubarrayPartitioner& set_result_budget(
      const char* attrname, uint64_t budget) {
    ctx_.get().handle_error(tiledb_subarray_partitioner_set_result_budget(
        ctx_.get().ptr().get(), attrname, budget, subarray_partitioner_.get()));
    return *this;
  }

  /**
   * Sets result size budget (in bytes) for the input var-sized
   * attribute/dimension.
   */
  SubarrayPartitioner& set_result_budget_var_attr(
      const char* attrname, uint64_t budget_off, uint64_t budget_val) {
    ctx_.get().handle_error(
        tiledb_subarray_partitioner_set_result_budget_var_attr(
            ctx_.get().ptr().get(),
            attrname,
            budget_off,
            budget_val,
            subarray_partitioner_.get()));
    return *this;
  }

  /**
   * Sets the memory budget (in bytes).
   *
   * @param budget The budget for the fixed-sized attributes and the
   *     offsets of the var-sized attributes.
   * @param budget_var The budget for the var-sized attributes.
   * @param budget_validity The budget for validity vectors.
   */
  SubarrayPartitioner& set_memory_budget(
      uint64_t budget, uint64_t budget_var, uint64_t budget_validity) {
    ctx_.get().handle_error(tiledb_subarray_partitioner_set_memory_budget(
        ctx_.get().ptr().get(),
        budget,
        budget_var,
        budget_validity,
        subarray_partitioner_.get()));
    return *this;
  }

  /**
   * Returns the associated C TileDB subarray object.
   */
  std::shared_ptr<tiledb_subarray_partitioner_t> ptr() const {
    return subarray_partitioner_;
  }

  /**
   * Gets result size budget (in bytes) for the input fixed-sized
   * attribute/dimension.
   */
  const SubarrayPartitioner& get_result_budget_fixed(
      const char* name, uint64_t* budget) const {
    ctx_.get().handle_error(tiledb_subarray_partitioner_get_result_budget_fixed(
        ctx_.get().ptr().get(), name, budget, subarray_partitioner_.get()));
    return *this;
  }

  /**
   * Gets result size budget (in bytes) for the input var-sized
   * attribute/dimension.
   */
  /**
   * Gets result size budget (in bytes) for the input var-sized
   * attribute/dimension.
   */
  const SubarrayPartitioner& get_result_budget_var(
      const char* name, uint64_t* budget_off, uint64_t* budget_val) const {
    ctx_.get().handle_error(tiledb_subarray_partitioner_get_result_budget_var(
        ctx_.get().ptr().get(),
        name,
        budget_off,
        budget_val,
        subarray_partitioner_.get()));
    return *this;
  }

  /**
   * Gets result size budget (in bytes) for the input fixed-sized
   * nullable attribute.
   */
  const SubarrayPartitioner& get_result_budget_nullable_fixed(
      const char* name, uint64_t* budget, uint64_t* budget_validity) const {
    ctx_.get().handle_error(
        tiledb_subarray_partitioner_get_result_budget_nullable_fixed(
            ctx_.get().ptr().get(),
            name,
            budget,
            budget_validity,
            subarray_partitioner_.get()));
    return *this;
  }

  /**
   * Gets result size budget (in bytes) for the input var-sized
   * nullable attribute.
   */
  const SubarrayPartitioner& get_result_budget_nullable_var(
      const char* name,
      uint64_t* budget_off,
      uint64_t* budget_val,
      uint64_t* budget_validity) const {
    ctx_.get().handle_error(
        tiledb_subarray_partitioner_get_result_budget_nullable_var(
            ctx_.get().ptr().get(),
            name,
            budget_off,
            budget_val,
            budget_validity,
            subarray_partitioner_.get()));
    return *this;
  }

  /**
   * Sets result size budget (in bytes) for the input fixed-sized,
   * nullable attribute.
   */
  SubarrayPartitioner& set_result_budget_nullable_fixed(
      const char* name, uint64_t budget, uint64_t budget_validity) {
    ctx_.get().handle_error(
        tiledb_subarray_partitioner_set_result_budget_nullable_fixed(
            ctx_.get().ptr().get(),
            name,
            budget,
            budget_validity,
            subarray_partitioner_.get()));
    return *this;
  }

  /**
   * Sets result size budget (in bytes) for the input var-sized
   * nullable attribute.
   */
  SubarrayPartitioner& set_result_budget_nullable_var(
      const char* name,
      uint64_t budget_off,
      uint64_t budget_val,
      uint64_t budget_validity) {
    ctx_.get().handle_error(
        tiledb_subarray_partitioner_set_result_budget_nullable_var(
            ctx_.get().ptr().get(),
            name,
            budget_off,
            budget_val,
            budget_validity,
            subarray_partitioner_.get()));
    return *this;
  }

#if 0
//TBD: internally available core SubarrayPartitioner() methods, do we need to support any more not 
//currently in capi?

  /** Returns the current partition. */
//  Subarray& current() {
//    return subarray_partitioner_->partitioner_->current();
//  }

  /** Returns the current partition info. */
  //TBD: need cppapi PartitionInfo entity as well...
  //const SubarrayPartitioner::PartitionInfo* current_partition_info() const {
  //  return subarray_partitioner_->partitioner_->current_partition_info();
  //}

  /** Returns the current partition info. */
  //TBD: apparently need cppapi PartitionInfo entity as well...
  //SubarrayPartitioner::PartitionInfo* current_partition_info() {
  //  return subarray_partitioner_->partitioner_->current_partition_info();
  //}

  /**
   * Returns ``true`` if there are no more partitions, i.e., if the
   * partitioner iterator is done.
   */
//  bool done() const {
//    return subarray_partitioner_->partitioner_->done();
//  }

  /**
   * Returns a pointer to mapping containing all attribute/dimension result
   * budgets that have been set.
   */
  const std::unordered_map<std::string, SubarrayPartitioner::ResultBudget>* get_result_budgets() const {
    return subarray_partitioner_->partitioner_->get_result_budgets();
  }

  /**
   * Gets the memory budget (in bytes).
   *
   * @param budget The budget for the fixed-sized attributes and the
   *     offsets of the var-sized attributes.
   * @param budget_var The budget for the var-sized attributes.
   * @return Status
   */
  Status get_memory_budget(
      uint64_t* budget, uint64_t* budget_var, uint64_t* budget_validity) const {
    return subarray_partitioner_->partitioner_->get_memory_budget(budget, budget_var, budget_validity);
  }

  /**
   * The partitioner iterates over the partitions of the subarray it is
   * associated with. This function advances to compute the next partition
   * based on the specified budget. If this cannot be retrieved because
   * the current partition cannot be split further (typically because it
   * is a single cell whose estimated result does not fit in the budget),
   * then the function does not advance to the next partition and sets
   * ``unsplittable`` to ``true``.
   */
  Status next(bool* unsplittable) {
    return subarray_partitioner_->partitioner_->next();
  }

  /**
   * Splits the current partition and updates the state, retrieving
   * a new current partition. This function is typically called
   * by the reader when the current partition was estimated to fit
   * the results, but that was not eventually true.
   */
  Status split_current(bool* unsplittable) {
    return subarray_partitioner_->partitioner_->split_current(
        unsplittable);
  }

  /** Returns the state. */
  const SubarrayPartitioner::State* state() const {
    return subarray_partitioner_->partitioner_->state()
  }

  /** Returns the state. */
  SubarrayPartitioner::State* state() {
    return subarray_partitioner_->partitioner_->state()
  }

#endif

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** */
  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** The TileDB array. */
  //  std::reference_wrapper<const Array> array_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** The initial subarray entity used for construcvtion.  */
  std::reference_wrapper<const Subarray> subarray_;

  /** The subarray partitioner entity itself.  */
  std::shared_ptr<tiledb_subarray_partitioner_t> subarray_partitioner_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Returns a deep copy of this SubarrayPartitioner. */
  SubarrayPartitioner clone() const;

  /**
   * Swaps the contents (all field values) of this subarray partitioner with
   * the given partitioner.
   */
  void swap(SubarrayPartitioner& partitioner);
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_SUBARRAY_PARTITIONER_H
