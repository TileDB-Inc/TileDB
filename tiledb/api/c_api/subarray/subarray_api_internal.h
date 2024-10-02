/**
 * @file tiledb/api/c_api/subarray/subarray_api_internal.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file declares the internals of the subarray section of the C API.
 */

#ifndef TILEDB_CAPI_SUBARRAY_INTERNAL_H
#define TILEDB_CAPI_SUBARRAY_INTERNAL_H

#include "subarray_api_external.h"

#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"

/** Handle `struct` for API Subarray objects. */
struct tiledb_subarray_handle_t
    : public tiledb::api::CAPIHandle<tiledb_subarray_handle_t> {
  /** Type name */
  static constexpr std::string_view object_type_name{"subarray"};

 private:
  using subarray_type = shared_ptr<tiledb::sm::Subarray>;
  subarray_type subarray_;

 public:
  template <class... Args>
  explicit tiledb_subarray_handle_t(Args... args)
      : subarray_{make_shared<tiledb::sm::Subarray>(
            HERE(), std::forward<Args>(args)...)} {
  }

  explicit tiledb_subarray_handle_t(const tiledb::sm::Subarray& subarray)
      : subarray_{make_shared<tiledb::sm::Subarray>(HERE(), subarray)} {
  }

  /**
   * Constructor from `shared_ptr<Subarray>` copies the shared pointer.
   */
  explicit tiledb_subarray_handle_t(const subarray_type& x)
      : subarray_(x) {
  }

  subarray_type subarray() const {
    return subarray_;
  }

  void add_label_range(
      const std::string& label_name, const void* start, const void* end) {
    subarray_->add_label_range(label_name, start, end);
  }

  void add_label_range_var(
      const std::string& label_name,
      const void* start,
      uint64_t start_size,
      const void* end,
      uint64_t end_size) {
    subarray_->add_label_range_var(
        label_name, start, start_size, end, end_size);
  }

  void add_point_ranges(
      unsigned dim_idx, const void* start, uint64_t count, bool check = true) {
    subarray_->add_point_ranges(dim_idx, start, count, check);
  }

  void add_range(unsigned dim_idx, const void* start, const void* end) {
    subarray_->add_range(dim_idx, start, end);
  }

  void add_range_by_name(
      const std::string& dim_name, const void* start, const void* end) {
    subarray_->add_range_by_name(dim_name, start, end);
  }

  void add_range_unsafe(uint32_t dim_idx, const Range& range) {
    subarray_->add_range_unsafe(dim_idx, range);
  }

  void add_range_var(
      unsigned dim_idx,
      const void* start,
      uint64_t start_size,
      const void* end,
      uint64_t end_size) {
    subarray_->add_range_var(dim_idx, start, start_size, end, end_size);
  }

  void add_range_var_by_name(
      const std::string& dim_name,
      const void* start,
      uint64_t start_size,
      const void* end,
      uint64_t end_size) {
    subarray_->add_range_var_by_name(
        dim_name, start, start_size, end, end_size);
  }

  bool coalesce_ranges() const {
    return subarray_->coalesce_ranges();
  }

  const std::vector<Range>& get_attribute_ranges(
      const std::string& attr_name) const {
    return subarray_->get_attribute_ranges(attr_name);
  }

  const std::string& get_label_name(const uint32_t dim_index) const {
    return subarray_->get_label_name(dim_index);
  }

  void get_label_range(
      const std::string& label_name,
      uint64_t range_idx,
      const void** start,
      const void** end) const {
    subarray_->get_label_range(label_name, range_idx, start, end);
  }

  void get_label_range_num(
      const std::string& label_name, uint64_t* range_num) const {
    subarray_->get_label_range_num(label_name, range_num);
  }

  void get_label_range_var(
      const std::string& label_name,
      uint64_t range_idx,
      void* start,
      void* end) const {
    subarray_->get_label_range_var(label_name, range_idx, start, end);
  }

  void get_label_range_var_size(
      const std::string& label_name,
      uint64_t range_idx,
      uint64_t* start_size,
      uint64_t* end_size) const {
    subarray_->get_label_range_var_size(
        label_name, range_idx, start_size, end_size);
  }

  void get_range(
      uint32_t dim_idx,
      uint64_t range_idx,
      const void** start,
      const void** end) const {
    subarray_->get_range(dim_idx, range_idx, start, end);
  }

  void get_range_from_name(
      const std::string& dim_name,
      uint64_t range_idx,
      const void** start,
      const void** end) const {
    subarray_->get_range_from_name(dim_name, range_idx, start, end);
  }

  void get_range_num(uint32_t dim_idx, uint64_t* range_num) const {
    subarray_->get_range_num(dim_idx, range_num);
  }

  void get_range_num_from_name(
      const std::string& dim_name, uint64_t* range_num) const {
    subarray_->get_range_num_from_name(dim_name, range_num);
  }

  void get_range_var(
      unsigned dim_idx, uint64_t range_idx, void* start, void* end) const {
    subarray_->get_range_var(dim_idx, range_idx, start, end);
  }

  void get_range_var_from_name(
      const std::string& dim_name,
      uint64_t range_idx,
      void* start,
      void* end) const {
    subarray_->get_range_var_from_name(dim_name, range_idx, start, end);
  }

  void get_range_var_size(
      uint32_t dim_idx,
      uint64_t range_idx,
      uint64_t* start_size,
      uint64_t* end_size) const {
    subarray_->get_range_var_size(dim_idx, range_idx, start_size, end_size);
  }

  void get_range_var_size_from_name(
      const std::string& dim_name,
      uint64_t range_idx,
      uint64_t* start_size,
      uint64_t* end_size) const {
    subarray_->get_range_var_size_from_name(
        dim_name, range_idx, start_size, end_size);
  }

  bool has_label_ranges(const uint32_t dim_index) const {
    return subarray_->has_label_ranges(dim_index);
  }

  const std::vector<Range>& ranges_for_dim(uint32_t dim_idx) const {
    return subarray_->ranges_for_dim(dim_idx);
  }

  void set_attribute_ranges(
      const std::string& attr_name, const std::vector<Range>& ranges) {
    subarray_->set_attribute_ranges(attr_name, ranges);
  }

  void set_coalesce_ranges(bool coalesce_ranges = true) {
    subarray_->set_coalesce_ranges(coalesce_ranges);
  }

  void set_config(
      const tiledb::sm::QueryType query_type,
      const tiledb::sm::Config& config) {
    subarray_->set_config(query_type, config);
  }

  void set_subarray(const void* subarray) {
    subarray_->set_subarray(subarray);
  }
};

namespace tiledb::api {

/**
 * Returns after successfully validating a subarray object.
 *
 * @param subarray Possibly-valid pointer to a subarray object.
 */
inline void ensure_subarray_is_valid(const tiledb_subarray_t* subarray) {
  ensure_handle_is_valid(subarray);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_SUBARRAY_INTERNAL_H
