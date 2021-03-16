/**
 * @file   external_subarray_partitioner.cc
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
 * This file implements class ExternalSubarrayPartitioner.
 */

#include "tiledb/sm/subarray/external_subarray_partitioner.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/hilbert.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/stats/stats.h"

#include <iomanip>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifndef MAX
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#endif

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ExternalSubarrayPartitioner::ExternalSubarrayPartitioner() = default;

ExternalSubarrayPartitioner::ExternalSubarrayPartitioner(
    const Subarray& subarray,
    uint64_t memory_budget,
    uint64_t memory_budget_var,
    uint64_t memory_budget_validity,
    ThreadPool* const compute_tp)
	: subarray_partitioner_(subarray, memory_budget, memory_budget_var, memory_budget_validity, compute_tp) {
}

ExternalSubarrayPartitioner::~ExternalSubarrayPartitioner() = default;

ExternalSubarrayPartitioner::ExternalSubarrayPartitioner(const ExternalSubarrayPartitioner& partitioner)
    : ExternalSubarrayPartitioner() {
  auto clone = partitioner.clone();
  swap(clone);
}

ExternalSubarrayPartitioner::ExternalSubarrayPartitioner(
    ExternalSubarrayPartitioner&& partitioner) noexcept
    : ExternalSubarrayPartitioner() {
  swap(partitioner);
}

ExternalSubarrayPartitioner& ExternalSubarrayPartitioner::operator=(
    const ExternalSubarrayPartitioner& partitioner) {
  auto clone = partitioner.clone();
  swap(clone);

  return *this;
}

ExternalSubarrayPartitioner& ExternalSubarrayPartitioner::operator=(
    ExternalSubarrayPartitioner&& partitioner) noexcept {
  swap(partitioner);

  return *this;
}

/* ****************************** */
/*               API              */
/* ****************************** */

Subarray& ExternalSubarrayPartitioner::current() {
  return subarray_partitioner_.current_partition_info()->partition_;
}

const SubarrayPartitioner::PartitionInfo*
ExternalSubarrayPartitioner::current_partition_info() const {
  return subarray_partitioner_.current_partition_info();
}

SubarrayPartitioner::PartitionInfo*
ExternalSubarrayPartitioner::current_partition_info() {
  return subarray_partitioner_.current_partition_info();
}

bool ExternalSubarrayPartitioner::done() const {
  return subarray_partitioner_.done();
}

Status ExternalSubarrayPartitioner::get_result_budget(
    const char* name, uint64_t* budget) const {
  return subarray_partitioner_.get_result_budget(name, budget);
}

Status ExternalSubarrayPartitioner::get_result_budget(
    const char* name, uint64_t* budget_off, uint64_t* budget_val) const {

  return subarray_partitioner_.get_result_budget(name, budget_off, budget_val);
}

Status ExternalSubarrayPartitioner::get_result_budget_nullable(
    const char* name, uint64_t* budget, uint64_t* budget_validity) const {

  return subarray_partitioner_.get_result_budget_nullable(name, budget, budget_validity);
}

Status ExternalSubarrayPartitioner::get_result_budget_nullable(
    const char* name,
    uint64_t* budget_off,
    uint64_t* budget_val,
    uint64_t* budget_validity) const {
  return subarray_partitioner_.get_result_budget_nullable(name, budget_off, budget_val, budget_validity);
}

const std::unordered_map<std::string, SubarrayPartitioner::ResultBudget>*
ExternalSubarrayPartitioner::get_result_budgets() const {
  return subarray_partitioner_.get_result_budgets();
}

Status ExternalSubarrayPartitioner::get_memory_budget(
    uint64_t* budget, uint64_t* budget_var, uint64_t* budget_validity) const {
  return subarray_partitioner_.get_memory_budget(budget, budget_var, budget_validity);
}

Status ExternalSubarrayPartitioner::next(bool* unsplittable) {
  return subarray_partitioner_.next(unsplittable);
}

Status ExternalSubarrayPartitioner::set_result_budget(
    const char* name, uint64_t budget) {
  return subarray_partitioner_.set_result_budget(name, budget);
}

Status ExternalSubarrayPartitioner::set_result_budget(
    const char* name, uint64_t budget_off, uint64_t budget_val) {
  return subarray_partitioner_.set_result_budget(name, budget_off, budget_val);
}

Status ExternalSubarrayPartitioner::set_result_budget_nullable(
    const char* name, uint64_t budget, uint64_t budget_validity) {

  return subarray_partitioner_.set_result_budget_nullable(name, budget, budget_validity);
	
}

Status ExternalSubarrayPartitioner::set_result_budget_nullable(
    const char* name,
    uint64_t budget_off,
    uint64_t budget_val,
    uint64_t budget_validity) {
  return subarray_partitioner_.set_result_budget_nullable(name, budget_off, budget_val, budget_validity);
	
}

Status ExternalSubarrayPartitioner::set_memory_budget(
    uint64_t budget, uint64_t budget_var, uint64_t budget_validity) {
  return subarray_partitioner_.set_memory_budget(budget, budget_var, budget_validity);
}

Status ExternalSubarrayPartitioner::split_current(bool* unsplittable) {
  return subarray_partitioner_.split_current(unsplittable);
}

const SubarrayPartitioner::State* ExternalSubarrayPartitioner::state() const {
  return subarray_partitioner_.state();
  //return &state_;
}

SubarrayPartitioner::State* ExternalSubarrayPartitioner::state() {
  return subarray_partitioner_.state();
  //return &state_;
}

const Subarray* ExternalSubarrayPartitioner::subarray() const {
  return subarray_partitioner_.subarray();
  //return &subarray_;
}

Subarray* ExternalSubarrayPartitioner::subarray() {
  return subarray_partitioner_.subarray();
  //return &subarray_;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

ExternalSubarrayPartitioner ExternalSubarrayPartitioner::clone() const {
  ExternalSubarrayPartitioner clone(*this);
  return clone;
}


void ExternalSubarrayPartitioner::swap(ExternalSubarrayPartitioner& partitioner) {
  std::swap(*this, partitioner);
}
