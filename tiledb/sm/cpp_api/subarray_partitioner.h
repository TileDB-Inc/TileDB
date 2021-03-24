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

//#include <list>

//#include <unordered_map>
//#include "tiledb/common/thread_pool.h"
//#include "tiledb/sm/misc/constants.h"
//#include "tiledb/sm/subarray/subarray_partitioner.h"

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

  //TBD: ???


  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  SubarrayPartitioner();

  /** Constructor. */
  SubarrayPartitioner(
      const Context &ctx,
      const Subarray& subarray,
      uint64_t memory_budget,
      uint64_t memory_budget_var,
      uint64_t memory_budget_validity)//;,
      //ThreadPool* compute_tp);
      : ctx_(ctx) 
      , subarray_(subarray) {
    tiledb_subarray_partitioner_t* capi_subarray_partitioner;
    ctx.handle_error(tiledb_subarray_partitioner_alloc(
        ctx.ptr().get(), subarray.ptr().get(), &capi_subarray_partitioner, memory_budget, memory_budget_var, memory_budget_validity));
    subarray_partitioner_ = std::shared_ptr<tiledb_subarray_partitioner_t>(
        capi_subarray_partitioner, deleter_);
  }

  /** Copy constructor. This performs a deep copy. */
//  SubarrayPartitioner(const SubarrayPartitioner& partitioner);

  /** Move constructor. */
//  SubarrayPartitioner(SubarrayPartitioner&& partitioner) noexcept;

  /** Copy-assign operator. This performs a deep copy. */
//  SubarrayPartitioner& operator=(const SubarrayPartitioner& partitioner);

  /** Move-assign operator. */
//  SubarrayPartitioner& operator=(SubarrayPartitioner&& partitioner) noexcept;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Set the subarray layout. */
  void set_layout(tiledb_layout_t layout){
    ctx_.get().handle_error(tiledb_subarray_partitioner_set_layout(ctx_.get().ptr().get(),
        layout, subarray_partitioner_.get()));
  }

  /** Set a custom layout */
  void set_custom_layout(
      const char** ordered_dim_names, uint32_t ordered_dim_names_length) {
    ctx_.get().handle_error(
        tiledb_subarray_partitioner_set_custom_layout(ctx_.get().ptr().get(), ordered_dim_names, ordered_dim_names_length, subarray_partitioner_.get()));
  }

  /** Compute the entire series of subarray partitions. */
  void compute() {
    ctx_.get().handle_error(tiledb_subarray_partitioner_compute(
        ctx_.get().ptr().get(), subarray_partitioner_.get()));
  }

  /** Get the number of partitions in the currently computed series. */
  int32_t get_partition_num() {
    uint64_t num;
    ctx_.get().handle_error(tiledb_subarray_partitioner_get_partition_num(
        ctx_.get().ptr().get(), &num, subarray_partitioner_.get()));
    return num;
  }

  /** Retrieve a partition (subarray) from within the currently comptued series. */
  void get_partition(uint64_t part_id, Subarray& retrieved_subarray) {
  //TBD: how to handle...
    ctx_.get().handle_error(tiledb_subarray_partitioner_get_partition(
      ctx_.get().ptr().get(),
        subarray_partitioner_.get(),
        part_id,
        retrieved_subarray.capi_subarray()));
  }

  void set_result_budget(const char* attrname, uint64_t budget) {
    ctx_.get().handle_error(tiledb_subarray_partitioner_set_result_budget(
      ctx_.get().ptr().get(),
      attrname,
      budget,
      subarray_partitioner_.get()));
  }

  void set_result_budget_var_attr(
      const char* attrname,
      uint64_t budget_off,
      uint64_t budget_val) {
    ctx_.get().handle_error(tiledb_subarray_partitioner_set_result_budget_var_attr(
        ctx_.get().ptr().get(), attrname, budget_off, budget_val, subarray_partitioner_.get()));
  }

  void set_memory_budget(
      uint64_t budget,
      uint64_t budget_var,
      uint64_t budget_validity) {
    ctx_.get().handle_error(tiledb_subarray_partitioner_set_memory_budget(
        ctx_.get().ptr().get(),
        budget,
        budget_var,
        budget_validity,
        subarray_partitioner_.get()));
  }

  /** Returns the C TileDB subarray object. */
  std::shared_ptr<tiledb_subarray_partitioner_t> ptr() const {
    return subarray_partitioner_;
  }

  /**
   * Gets result size budget (in bytes) for the input fixed-sized
   * attribute/dimension.
   */
  void get_result_budget(const char* name, uint64_t* budget) const {
    //return subarray_partitioner_->partitioner_->get_result_budget(name, budget);
    ctx_.get().handle_error(
        tiledb_subarray_partitioner_get_result_budget1(ctx_.get().ptr().get(), name, budget, subarray_partitioner_.get()));
  }

  /**
   * Gets result size budget (in bytes) for the input var-sized
   * attribute/dimension.
   */
  void get_result_budget(
      const char* name, uint64_t* budget_off, uint64_t* budget_val) const {
    //return subarray_partitioner_->partitioner_->get_result_budget(
    //    name, budget_off, budget_val);
    ctx_.get().handle_error(tiledb_subarray_partitioner_get_result_budget2(
        ctx_.get().ptr().get(),
        name,
        budget_off,
        budget_val,
        subarray_partitioner_.get()));
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
   * Gets result size budget (in bytes) for the input fixed-sized
   * attribute/dimension.
   */
  Status get_result_budget(const char* name, uint64_t* budget) const {
    return subarray_partitioner_->partitioner_->get_result_budget(name, budget);
  }

  /**
   * Gets result size budget (in bytes) for the input var-sized
   * attribute/dimension.
   */
  Status get_result_budget(
      const char* name, uint64_t* budget_off, uint64_t* budget_val) const {
    return subarray_partitioner_->partitioner_->get_result_budget(
        name, budget_off, budget_val);
  }

  /**
   * Gets result size budget (in bytes) for the input fixed-sized
   * nullable attribute.
   */
  Status get_result_budget_nullable(
      const char* name, uint64_t* budget, uint64_t* budget_validity) const {
    return subarray_partitioner_->partitioner_->get_result_budget_nullable(
        name, budget, budget_validity);
  }

  /**
   * Gets result size budget (in bytes) for the input var-sized
   * nullable attribute.
   */
  Status get_result_budget_nullable(
      const char* name,
      uint64_t* budget_off,
      uint64_t* budget_val,
      uint64_t* budget_validity) const {
    return subarray_partitioner_->partitioner_->get_result_budget_nullable(
        name, budget_off, budget_val, budget_validity);
  }

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
   * Sets the memory budget (in bytes).
   *
   * @param budget The budget for the fixed-sized attributes and the
   *     offsets of the var-sized attributes.
   * @param budget_var The budget for the var-sized attributes.
   * @param budget_validity The budget for validity vectors.
   * @return Status
   */
  Status set_memory_budget(
      uint64_t budget, uint64_t budget_var, uint64_t budget_validity) {
    return subarray_partitioner_->partitioner_->set_memory_budget(
        budget, budget_var, budget_validity);
  }

  /**
   * Sets result size budget (in bytes) for the input fixed-sized
   * attribute/dimension.
   */
  Status set_result_budget(const char* name, uint64_t budget) {
    return subarray_partitioner_->partitioner_->set_result_budget(
        name, budget);
  }

  /**
   * Sets result size budget (in bytes) for the input var-sized
   * attribute/dimension.
   */
  Status set_result_budget(
      const char* name, uint64_t budget_off, uint64_t budget_val) {
    return subarray_partitioner_->partitioner_->set_result_budget(
        name, budget_off, budget_valy);
  }

  /**
   * Sets result size budget (in bytes) for the input fixed-sized,
   * nullable attribute.
   */
  Status set_result_budget_nullable(
      const char* name, uint64_t budget, uint64_t budget_validity) {
    return subarray_partitioner_->partitioner_->set_result_budget_nullable(
        name, budget, budget_validity);
  }

  /**
   * Sets result size budget (in bytes) for the input var-sized
   * nullable attribute.
   */
  Status set_result_budget_nullable(
      const char* name,
      uint64_t budget_off,
      uint64_t budget_val,
      uint64_t budget_validity) {
    return subarray_partitioner_->partitioner_->set_result_budget_nullabe(
        name, budget_off, budget_val, budget_validity);
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
