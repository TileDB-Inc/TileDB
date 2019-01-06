/**
 * @file   subarray.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file implements class Subarray.
 */

#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/rtree/rtree.h"

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Subarray::Subarray() {
  array_ = nullptr;
  layout_ = Layout::UNORDERED;
  result_est_size_computed_ = false;
}

Subarray::Subarray(const Array* array, Layout layout)
    : array_(array)
    , layout_(layout) {
  result_est_size_computed_ = false;
  auto dim_num = array->array_schema()->dim_num();
  auto domain_type = array->array_schema()->domain()->type();
  for (uint32_t i = 0; i < dim_num; ++i)
    ranges_.emplace_back(domain_type);
}

Subarray::Subarray(const Subarray& subarray)
    : Subarray() {
  // Make a deep-copy clone
  auto clone = subarray.clone();
  // Swap with the clone
  swap(clone);
}

Subarray::Subarray(Subarray&& subarray)
    : Subarray() {
  // Swap with the argument
  swap(subarray);
}

Subarray& Subarray::operator=(const Subarray& subarray) {
  // Make a deep-copy clone
  auto clone = subarray.clone();
  // Swap with the clone
  swap(clone);

  return *this;
}

Subarray& Subarray::operator=(Subarray&& subarray) {
  // Swap with the argument
  swap(subarray);

  return *this;
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status Subarray::add_missing_ranges() {
  auto dim_num = array_->array_schema()->dim_num();
  auto domain = (unsigned char*)array_->array_schema()->domain()->domain();
  for (unsigned i = 0; i < dim_num; ++i) {
    auto range_size = ranges_[i].range_size_;
    if (ranges_[i].range_num() == 0) {
      ranges_[i].add_range(&(domain[i * range_size]));
    }
  }

  return Status::Ok();
}

Status Subarray::add_range(uint32_t dim_idx, const void* range) {
  if (range == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot add range to dimension; Range cannot be null"));

  auto dim_num = array_->array_schema()->dim_num();
  if (dim_idx >= dim_num)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot add range to dimension; Invalid dimension index"));

  auto type = array_->array_schema()->domain()->type();
  switch (type) {
    case Datatype::INT8:
      return add_range<int8_t>(dim_idx, (const int8_t*)range);
    case Datatype::UINT8:
      return add_range<uint8_t>(dim_idx, (const uint8_t*)range);
    case Datatype::INT16:
      return add_range<int16_t>(dim_idx, (const int16_t*)range);
    case Datatype::UINT16:
      return add_range<uint16_t>(dim_idx, (const uint16_t*)range);
    case Datatype::INT32:
      return add_range<int32_t>(dim_idx, (const int32_t*)range);
    case Datatype::UINT32:
      return add_range<uint32_t>(dim_idx, (const uint32_t*)range);
    case Datatype::INT64:
      return add_range<int64_t>(dim_idx, (const int64_t*)range);
    case Datatype::UINT64:
      return add_range<uint64_t>(dim_idx, (const uint64_t*)range);
    case Datatype::FLOAT32:
      return add_range<float>(dim_idx, (const float*)range);
    case Datatype::FLOAT64:
      return add_range<double>(dim_idx, (const double*)range);
    default:
      return LOG_STATUS(Status::SubarrayError(
          "Cannot add range to dimension; Unsupported subarray domain type"));
  }

  return Status::Ok();
}

const Array* Subarray::array() const {
  return array_;
}

void Subarray::clear() {
  ranges_.clear();
}

uint32_t Subarray::dim_num() const {
  return array_->array_schema()->dim_num();
}

const void* Subarray::domain() const {
  return array_->array_schema()->domain()->domain();
}

Status Subarray::get_range(
    uint32_t dim_idx, uint64_t range_idx, const void** range) const {
  auto dim_num = array_->array_schema()->dim_num();
  if (dim_idx >= dim_num)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get range; Invalid dimension index"));

  auto range_num = ranges_[dim_idx].range_num();
  if (range_idx >= range_num)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get range; Invalid range index"));

  *range = ranges_[dim_idx].get_range(range_idx);

  return Status::Ok();
}

Status Subarray::get_range_num(uint32_t dim_idx, uint64_t* range_num) const {
  auto dim_num = array_->array_schema()->dim_num();
  if (dim_idx >= dim_num)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get number of ranges for a dimension; "
                              "Invalid dimension index"));

  *range_num = ranges_[dim_idx].range_num();

  return Status::Ok();
}

Layout Subarray::layout() const {
  return layout_;
}

// TODO: Move this to the partitioner
Status Subarray::next_partition() {
  //  TODO
  return Status::Ok();
}

// TODO: Move this to the partitioner
bool Subarray::no_more_partitions() const {
  // TODO
  return true;
}

Status Subarray::get_result_budget(const char* attr_name, uint64_t* budget) {
  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get result budget; Invalid attribute"));

  // Check attribute name
  auto attr = array_->array_schema()->attribute(attr_name);
  if (attr == nullptr)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get result budget; Invalid attribute"));

  // Check budget pointer
  if (budget == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get result budget; Invalid budget input"));

  // Check if the attribute is fixed-sized
  if (attr->var_size())
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get result budget; Attribute must be fixed-sized"));

  // Check if budget has been set
  auto b_it = budget_.find(attr_name);
  if (b_it == budget_.end())
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get result budget; Budget not set for the input attribute"));

  // Get budget
  *budget = b_it->second.size_fixed_;

  return Status::Ok();
}

Status Subarray::get_result_budget(
    const char* attr_name, uint64_t* budget_off, uint64_t* budget_val) {
  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get result budget; Invalid attribute"));

  // Check attribute
  auto attr = array_->array_schema()->attribute(attr_name);
  if (attr == nullptr)
    return LOG_STATUS(
        Status::SubarrayError("Cannot get result budget; Invalid attribute"));

  // Check budget pointer
  if (budget_off == nullptr || budget_val == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get result budget; Invalid budget input"));

  // Check if the attribute is var-sized
  if (!attr->var_size())
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get result budget; Attribute must be var-sized"));

  // Check if budget has been set
  auto b_it = budget_.find(attr_name);
  if (b_it == budget_.end())
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get result budget; Budget not set for the input attribute"));

  // Get budget
  *budget_off = b_it->second.size_fixed_;
  *budget_val = b_it->second.size_var_;

  return Status::Ok();
}

Status Subarray::get_est_result_size(const char* attr_name, uint64_t* size) {
  if (array_->array_schema()->dense())
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; Feature not "
                              "supported for dense arrays yet"));

  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Invalid attribute"));

  // Check attribute
  auto attr = array_->array_schema()->attribute(attr_name);
  if (attr_name != constants::coords && attr == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Invalid attribute"));

  // Check budget pointer
  if (size == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Invalid size input"));

  // Check if the attribute is fixed-sized
  if (attr_name != constants::coords && attr->var_size())
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Attribute must be fixed-sized"));

  // Compute tile overlap for each fragment
  compute_est_result_size();
  *size = (uint64_t)ceil(est_result_size_[attr_name].size_fixed_);

  return Status::Ok();
}

Status Subarray::get_est_result_size(
    const char* attr_name, uint64_t* size_off, uint64_t* size_val) {
  if (array_->array_schema()->dense())
    return LOG_STATUS(
        Status::SubarrayError("Cannot get estimated result size; Feature not "
                              "supported for dense arrays yet"));

  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Invalid attribute"));

  // Check attribute
  auto attr = array_->array_schema()->attribute(attr_name);
  if (attr == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Invalid attribute"));

  // Check budget pointer
  if (size_off == nullptr || size_val == nullptr)
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Invalid size input"));

  // Check if the attribute is var-sized
  if (!attr->var_size())
    return LOG_STATUS(Status::SubarrayError(
        "Cannot get estimated result size; Attribute must be var-sized"));

  // Compute tile overlap for each fragment
  compute_est_result_size();
  *size_off = (uint64_t)ceil(est_result_size_[attr_name].size_fixed_);
  *size_val = (uint64_t)ceil(est_result_size_[attr_name].size_var_);

  return Status::Ok();
}

uint64_t Subarray::range_num() const {
  if (ranges_.empty())
    return 0;

  uint64_t ret = 1;
  for (const auto& r : ranges_)
    ret *= r.range_num();

  return ret;
}

template <class T>
std::vector<const T*> Subarray::range(uint64_t range_idx) const {
  std::vector<const T*> ret;
  uint64_t tmp_idx = range_idx;
  auto dim_num = this->dim_num();
  auto cell_order = array_->array_schema()->cell_order();
  auto layout = (layout_ == Layout::UNORDERED) ? cell_order : layout_;

  if (layout == Layout::ROW_MAJOR) {
    for (unsigned i = 0; i < dim_num; ++i) {
      ret.push_back((T*)(ranges_[i].get_range(tmp_idx / range_offsets_[i])));
      tmp_idx %= range_offsets_[i];
    }
  } else if (layout == Layout::COL_MAJOR) {
    for (unsigned i = dim_num - 1;; --i) {
      ret.push_back((T*)(ranges_[i].get_range(tmp_idx / range_offsets_[i])));
      tmp_idx %= range_offsets_[i];
      if (i == 0)
        break;
    }
    std::reverse(ret.begin(), ret.end());
  } else {
    assert(false);
  }

  return ret;
}

// TODO: Move this to the partitioner
Status Subarray::set_result_budget(const char* attr_name, uint64_t budget) {
  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(
        Status::SubarrayError("Cannot set result budget; Invalid attribute"));

  // Check attribute
  auto attr = array_->array_schema()->attribute(attr_name);
  if (attr == nullptr)
    return LOG_STATUS(
        Status::SubarrayError("Cannot set result budget; Invalid attribute"));

  // Check if the attribute is fixed-sized
  if (attr->var_size())
    return LOG_STATUS(Status::SubarrayError(
        "Cannot set result budget; Attribute must be fixed-sized"));

  budget_[attr_name] = {(double)budget, 0.0};

  return Status::Ok();
}

// TODO: Move this to the partitioner
Status Subarray::set_result_budget(
    const char* attr_name, uint64_t budget_off, uint64_t budget_val) {
  // Check attribute name
  if (attr_name == nullptr)
    return LOG_STATUS(
        Status::SubarrayError("Cannot set result budget; Invalid attribute"));

  // Check attribute
  auto attr = array_->array_schema()->attribute(attr_name);
  if (attr == nullptr)
    return LOG_STATUS(
        Status::SubarrayError("Cannot set result budget; Invalid attribute"));

  // Check if the attribute is var-sized
  if (!attr->var_size())
    return LOG_STATUS(Status::SubarrayError(
        "Cannot set result budget; Attribute must be var-sized"));

  budget_[attr_name] = {(double)budget_off, (double)budget_val};

  return Status::Ok();
}

Datatype Subarray::type() const {
  return array_->array_schema()->domain()->type();
}

// TODO: Move this to the partitioner
bool Subarray::unsplittable() const {
  // TODO
  return false;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

template <class T>
Status Subarray::add_range(uint32_t dim_idx, const T* range) {
  assert(dim_idx < array_->array_schema()->dim_num());

  // Must reset the tile overlap
  result_est_size_computed_ = false;

  // Check for NaN
  RETURN_NOT_OK(check_nan<T>(range));

  // Check range bounds
  if (range[0] > range[1])
    return LOG_STATUS(
        Status::SubarrayError("Cannot add range to dimension; Lower range "
                              "bound cannot be larger than the higher bound"));

  // Check range against the domain
  auto domain = (T*)array_->array_schema()->domain()->domain();
  if (range[0] < domain[2 * dim_idx] || range[1] > domain[2 * dim_idx + 1])
    return LOG_STATUS(
        Status::SubarrayError("Cannot add range to dimension; Range must be in "
                              "the domain the subarray is constructed from"));

  // Add the range
  ranges_[dim_idx].add_range(range);

  return Status::Ok();
}

void Subarray::compute_range_offsets() {
  range_offsets_.clear();

  auto dim_num = this->dim_num();
  auto cell_order = array_->array_schema()->cell_order();
  auto layout = (layout_ == Layout::UNORDERED) ? cell_order : layout_;

  if (layout == Layout::COL_MAJOR) {
    range_offsets_.push_back(1);
    if (dim_num > 1) {
      for (unsigned int i = 1; i < dim_num; ++i)
        range_offsets_.push_back(
            range_offsets_.back() * ranges_[i - 1].range_num());
    }
  } else if (layout == Layout::ROW_MAJOR) {
    range_offsets_.push_back(1);
    if (dim_num > 1) {
      for (unsigned int i = dim_num - 2;; --i) {
        range_offsets_.push_back(
            range_offsets_.back() * ranges_[i + 1].range_num());
        if (i == 0)
          break;
      }
    }
    std::reverse(range_offsets_.begin(), range_offsets_.end());
  } else {
    assert(false);
  }
}

void Subarray::compute_est_result_size() {
  if (result_est_size_computed_)
    return;

  auto type = array_->array_schema()->domain()->type();
  switch (type) {
    case Datatype::INT8:
      compute_est_result_size<int8_t>();
      break;
    case Datatype::UINT8:
      compute_est_result_size<uint8_t>();
      break;
    case Datatype::INT16:
      compute_est_result_size<int16_t>();
      break;
    case Datatype::UINT16:
      compute_est_result_size<uint16_t>();
      break;
    case Datatype::INT32:
      compute_est_result_size<int32_t>();
      break;
    case Datatype::UINT32:
      compute_est_result_size<uint32_t>();
      break;
    case Datatype::INT64:
      compute_est_result_size<int64_t>();
      break;
    case Datatype::UINT64:
      compute_est_result_size<uint64_t>();
      break;
    case Datatype::FLOAT32:
      compute_est_result_size<float>();
      break;
    case Datatype::FLOAT64:
      compute_est_result_size<double>();
      break;
    default:
      assert(false);
  }
}

template <class T>
void Subarray::compute_est_result_size() {
  if (result_est_size_computed_)
    return;

  compute_range_offsets();
  tile_overlap_.clear();
  est_result_size_.clear();
  std::mutex mtx;

  // Prepare estimated result size vector for all attributes and coords
  auto attributes = array_->array_schema()->attributes();
  std::vector<ResultSize> est_result_size_vec;
  for (unsigned i = 0; i < attributes.size() + 1; ++i)
    est_result_size_vec.emplace_back(ResultSize{0.0, 0.0});

  // Compute estimated result in parallel over fragments and ranges
  auto meta = array_->fragment_metadata();
  auto fragment_num = meta.size();
  tile_overlap_.resize(fragment_num);
  auto range_num = this->range_num();
  auto statuses_1 = parallel_for(0, fragment_num, [&](unsigned i) {
    auto statuses_2 = parallel_for(0, range_num, [&](unsigned j) {
      auto rtree = meta[i]->rtree();
      auto range = this->range<T>(j);
      auto overlap = rtree->get_tile_overlap<T>(range);
      for (unsigned a = 0; a < attributes.size(); ++a) {
        auto attr_name = attributes[a]->name();
        bool var_size = attributes[a]->var_size();
        auto result_size =
            compute_est_result_size(attr_name, var_size, i, overlap);
        std::lock_guard<std::mutex> block(mtx);
        est_result_size_vec[a].size_fixed_ += result_size.size_fixed_;
        est_result_size_vec[a].size_var_ += result_size.size_var_;
      }
      auto result_size =
          compute_est_result_size(constants::coords, false, i, overlap);
      std::lock_guard<std::mutex> block(mtx);
      est_result_size_vec.back().size_fixed_ += result_size.size_fixed_;
      est_result_size_vec.back().size_var_ += result_size.size_var_;
      return Status::Ok();
    });
    return Status::Ok();
  });

  // Amplify result estimation
  for (auto& r : est_result_size_vec) {
    r.size_fixed_ *= constants::est_result_size_amplification;
    r.size_var_ *= constants::est_result_size_amplification;
  }

  // Set the estimated result size map
  for (unsigned i = 0; i < attributes.size(); ++i)
    est_result_size_[attributes[i]->name()] = est_result_size_vec[i];
  est_result_size_[constants::coords] = est_result_size_vec.back();
  result_est_size_computed_ = true;
}

Subarray::ResultSize Subarray::compute_est_result_size(
    const std::string& attr_name,
    bool var_size,
    unsigned fragment_idx,
    const TileOverlap& overlap) const {
  ResultSize ret{0.0, 0.0};
  auto meta = array_->fragment_metadata()[fragment_idx];

  // Parse tile ranges
  for (const auto& tr : overlap.tile_ranges_) {
    for (uint64_t tid = tr.first; tid <= tr.second; ++tid) {
      if (!var_size) {
        ret.size_fixed_ += meta->tile_size(attr_name, tid);
      } else {
        ret.size_fixed_ += meta->tile_size(attr_name, tid);
        ret.size_var_ += meta->tile_var_size(attr_name, tid);
      }
    }
  }

  // Parse individual tiles
  for (const auto& t : overlap.tiles_) {
    auto tid = t.first;
    auto ratio = t.second;
    if (!var_size) {
      ret.size_fixed_ += meta->tile_size(attr_name, tid) * ratio;
    } else {
      ret.size_fixed_ += meta->tile_size(attr_name, tid) * ratio;
      ret.size_var_ += meta->tile_var_size(attr_name, tid) * ratio;
    }
  }

  return ret;
}

Subarray Subarray::clone() const {
  Subarray clone;
  clone.array_ = array_;
  clone.budget_ = budget_;
  clone.layout_ = layout_;
  clone.ranges_ = ranges_;
  clone.range_offsets_ = range_offsets_;
  clone.tile_overlap_ = tile_overlap_;
  clone.result_est_size_computed_ = result_est_size_computed_;

  return clone;
}

void Subarray::swap(Subarray& subarray) {
  std::swap(array_, subarray.array_);
  std::swap(budget_, subarray.budget_);
  std::swap(layout_, subarray.layout_);
  std::swap(ranges_, subarray.ranges_);
  std::swap(range_offsets_, subarray.range_offsets_);
  std::swap(tile_overlap_, subarray.tile_overlap_);
  std::swap(result_est_size_computed_, subarray.result_est_size_computed_);
}

}  // namespace sm
}  // namespace tiledb
