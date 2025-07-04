/**
 * @file tiledb/api/c_api/array_schema/array_schema_api_internal.h
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
 * This file declares the internals of the array_schema section of the C API.
 */

#ifndef TILEDB_CAPI_ARRAY_SCHEMA_INTERNAL_H
#define TILEDB_CAPI_ARRAY_SCHEMA_INTERNAL_H

#include "array_schema_api_external.h"

#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/layout.h"

/** Handle `struct` for API ArraySchema objects. */
struct tiledb_array_schema_handle_t
    : public tiledb::api::CAPIHandle<tiledb_array_schema_handle_t> {
  /** Type name */
  static constexpr std::string_view object_type_name{"array_schema"};

 private:
  using array_schema_type = shared_ptr<tiledb::sm::ArraySchema>;
  array_schema_type array_schema_;

 public:
  template <class... Args>
  explicit tiledb_array_schema_handle_t(Args... args)
      : array_schema_{make_shared<tiledb::sm::ArraySchema>(
            HERE(), std::forward<Args>(args)...)} {
  }

  explicit tiledb_array_schema_handle_t(
      const tiledb::sm::ArraySchema& array_schema)
      : array_schema_{
            make_shared<tiledb::sm::ArraySchema>(HERE(), array_schema)} {
  }

  /**
   * Constructor from `shared_ptr<ArraySchema>` copies the shared pointer.
   */
  explicit tiledb_array_schema_handle_t(const array_schema_type& x)
      : array_schema_(x) {
  }

  array_schema_type array_schema() const {
    return array_schema_;
  }

  Status add_attribute(
      shared_ptr<const tiledb::sm::Attribute> attr, bool check_special = true) {
    array_schema_->add_attribute(attr, check_special);
    return Status::Ok();
  }

  void add_dimension_label(
      tiledb::sm::ArraySchema::dimension_size_type dim_id,
      const std::string& name,
      tiledb::sm::DataOrder label_order,
      tiledb::sm::Datatype label_type,
      bool check_name = true) {
    array_schema_->add_dimension_label(
        dim_id, name, label_order, label_type, check_name);
  }

  void load_enumeration(tiledb_ctx_t* ctx, const char* enumeration_name) {
    load_enumeration_into_schema(
        ctx->context(), enumeration_name, *array_schema_);
  }

  shared_ptr<const tiledb::sm::Enumeration> get_enumeration(
      const char* name) const {
    return array_schema_->get_enumeration(name);
  }

  void add_enumeration(shared_ptr<const tiledb::sm::Enumeration> enmr) {
    return array_schema_->add_enumeration(enmr);
  }

  bool allows_dups() const {
    return array_schema_->allows_dups();
  }

  tiledb::sm::ArrayType array_type() const {
    return array_schema_->array_type();
  }

  const tiledb::sm::URI& array_uri() const {
    return array_schema_->array_uri();
  }

  tiledb::sm::ArraySchema::attribute_size_type attribute_num() const {
    return array_schema_->attribute_num();
  }

  uint64_t capacity() const {
    return array_schema_->capacity();
  }

  tiledb::sm::Layout cell_order() const {
    return array_schema_->cell_order();
  }

  const tiledb::sm::FilterPipeline& cell_validity_filters() const {
    return array_schema_->cell_validity_filters();
  }

  const tiledb::sm::FilterPipeline& cell_var_offsets_filters() const {
    return array_schema_->cell_var_offsets_filters();
  }

  void check(const tiledb::sm::Config& cfg) const {
    array_schema_->check(cfg);
  }

  const tiledb::sm::FilterPipeline& coords_filters() const {
    return array_schema_->coords_filters();
  }

  const tiledb::sm::DimensionLabel& dimension_label(
      tiledb::sm::ArraySchema::dimension_label_size_type i) const {
    return array_schema_->dimension_label(i);
  }

  const tiledb::sm::DimensionLabel& dimension_label(
      const std::string& name) const {
    return array_schema_->dimension_label(name);
  }

  const tiledb::sm::Dimension* dimension_ptr(
      tiledb::sm::ArraySchema::dimension_size_type i) const {
    return array_schema_->dimension_ptr(i);
  }

  const tiledb::sm::Dimension* dimension_ptr(const std::string& name) const {
    return array_schema_->dimension_ptr(name);
  }

  tiledb::sm::ArraySchema::dimension_label_size_type dim_label_num() const {
    return array_schema_->dim_label_num();
  }

  shared_ptr<tiledb::sm::CurrentDomain> get_current_domain() const {
    return array_schema_->get_current_domain();
  }

  Status has_attribute(const std::string& name, bool* has_attr) const {
    *has_attr = array_schema_->has_attribute(name);
    return Status::Ok();
  }

  bool is_dim_label(const std::string& name) const {
    return array_schema_->is_dim_label(name);
  }

  Status set_allows_dups(bool allows_dups) {
    array_schema_->set_allows_dups(allows_dups);
    return Status::Ok();
  }

  void set_capacity(uint64_t capacity) {
    array_schema_->set_capacity(capacity);
  }

  void set_current_domain(
      shared_ptr<tiledb::sm::CurrentDomain> current_domain) {
    array_schema_->set_current_domain(current_domain);
  }

  void set_dimension_label_filter_pipeline(
      const std::string& label_name,
      const tiledb::sm::FilterPipeline& pipeline) {
    array_schema_->set_dimension_label_filter_pipeline(label_name, pipeline);
  }

  void set_dimension_label_tile_extent(
      const std::string& label_name,
      const tiledb::sm::Datatype type,
      const void* tile_extent) {
    array_schema_->set_dimension_label_tile_extent(
        label_name, type, tile_extent);
  }

  Status set_domain(shared_ptr<tiledb::sm::Domain> domain) {
    array_schema_->set_domain(domain);
    return Status::Ok();
  }

  Status set_cell_order(tiledb::sm::Layout cell_order) {
    array_schema_->set_cell_order(cell_order);
    return Status::Ok();
  }

  Status set_cell_validity_filter_pipeline(
      const tiledb::sm::FilterPipeline& pipeline) {
    array_schema_->set_cell_validity_filter_pipeline(pipeline);
    return Status::Ok();
  }

  Status set_cell_var_offsets_filter_pipeline(
      const tiledb::sm::FilterPipeline& pipeline) {
    array_schema_->set_cell_var_offsets_filter_pipeline(pipeline);
    return Status::Ok();
  }

  Status set_coords_filter_pipeline(
      const tiledb::sm::FilterPipeline& pipeline) {
    array_schema_->set_coords_filter_pipeline(pipeline);
    return Status::Ok();
  }

  Status set_tile_order(tiledb::sm::Layout tile_order) {
    array_schema_->set_tile_order(tile_order);
    return Status::Ok();
  }

  shared_ptr<const tiledb::sm::Attribute> shared_attribute(
      tiledb::sm::ArraySchema::attribute_size_type id) const {
    return array_schema_->shared_attribute(id);
  }

  shared_ptr<const tiledb::sm::Attribute> shared_attribute(
      const std::string& name) const {
    return array_schema_->shared_attribute(name);
  }

  shared_ptr<tiledb::sm::Domain> shared_domain() const {
    return array_schema_->shared_domain();
  }

  tiledb::sm::Layout tile_order() const {
    return array_schema_->tile_order();
  }

  std::pair<uint64_t, uint64_t> timestamp_range() const {
    return array_schema_->timestamp_range();
  }

  format_version_t version() const {
    return array_schema_->version();
  }
};

namespace tiledb::api {

/**
 * Returns after successfully validating an array schema.
 *
 * @param array_schema Possibly-valid pointer to an array schema
 */
inline void ensure_array_schema_is_valid(
    const tiledb_array_schema_t* array_schema) {
  ensure_handle_is_valid(array_schema);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_ARRAY_SCHEMA_INTERNAL_H
