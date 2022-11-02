/**
 * @file tiledb/sm/dimension_label/dimension_label.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 */

#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/dimension_label_reference.h"
#include "tiledb/sm/array_schema/dimension_label_schema.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/label_order.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/group/group_v1.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/type/range/range.h"

using namespace tiledb::common;

namespace tiledb::sm {

DimensionLabel::DimensionLabel(const URI& uri, StorageManager* storage_manager)
    : uri_(uri)
    , storage_manager_(storage_manager)
    , indexed_array_(make_shared<Array>(
          HERE(), uri.join_path(indexed_array_name), storage_manager))
    , schema_{nullptr} {
}

void DimensionLabel::close() {
  throw_if_not_ok(indexed_array_->close());
}

const Dimension* DimensionLabel::index_dimension() const {
  if (!schema_)
    throw std::logic_error(
        "DimensionLabel schema does not exist. DimensionLabel must be opened.");
  return schema_->index_dimension();
}

void DimensionLabel::is_compatible(
    const DimensionLabelReference& dim_label_ref, const Dimension* dim) const {
  if (!schema_)
    throw std::logic_error(
        "DimensionLabel schema does not exist. DimensionLabel must be opened.");

  // Check the dimension label schema matches the definition provided in
  // the dimension label reference.
  if (!schema_->is_compatible_label(dim)) {
    throw DimensionLabelStatusException(
        "Error opening dimension label; Found dimension label does not match "
        "array dimension.");
  }
  if (schema_->label_order() != dim_label_ref.label_order()) {
    throw DimensionLabelStatusException(
        "Error opening dimension label; The label order of the loaded "
        "dimension label is " +
        label_order_str(schema_->label_order()) +
        ", but the expected label order was " +
        label_order_str(dim_label_ref.label_order()) + ".");
  }
  if (schema_->label_type() != dim_label_ref.label_type()) {
    throw DimensionLabelStatusException(
        "Error opening dimension label; The label datatype of the loaded "
        "dimension label is " +
        datatype_str(schema_->label_type()) +
        ", but the expected label datatype was " +
        datatype_str(dim_label_ref.label_type()) + ".");
  }
  if (!(schema_->label_domain() == dim_label_ref.label_domain())) {
    throw DimensionLabelStatusException(
        "Error opening dimension label; The label domain of the loaded "
        "dimension label does not match the expected domain.");
  }
  if (schema_->label_cell_val_num() != dim_label_ref.label_cell_val_num()) {
    throw DimensionLabelStatusException(
        "Error opening dimension label; The label cell value number of the "
        "loaded dimension label is " +
        std::to_string(schema_->label_cell_val_num()) +
        ", but the expected label cell value number was " +
        std::to_string(dim_label_ref.label_cell_val_num()) + ".");
  }
}

const Attribute* DimensionLabel::label_attribute() const {
  if (!schema_)
    throw std::logic_error(
        "DimensionLabel schema does not exist. DimensionLabel must be opened.");
  return schema_->label_attribute();
}

LabelOrder DimensionLabel::label_order() const {
  if (!schema_)
    throw std::logic_error(
        "DimensionLabel schema does not exist. DimensionLabel must be opened.");
  return schema_->label_order();
}

void DimensionLabel::open(
    QueryType query_type,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  // Explicitly set timestamp so it's the same for both arrays.
  uint64_t timestamp_start{0};
  uint64_t timestamp_end{utils::time::timestamp_now_ms()};
  return open(
      query_type,
      timestamp_start,
      timestamp_end,
      encryption_type,
      encryption_key,
      key_length);
}

void DimensionLabel::open(
    QueryType query_type,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  // Open indexed array and get schema.
  throw_if_not_ok(indexed_array_->open(
      query_type,
      timestamp_start,
      timestamp_end,
      encryption_type,
      encryption_key,
      key_length));
  auto&& [index_status, indexed_array_schema] =
      indexed_array_->get_array_schema();
  throw_if_not_ok(index_status);

  // Open the dimension label schema.
  load_schema(indexed_array_schema.value());

  // Set the query type.
  query_type_ = query_type;
}

void DimensionLabel::load_schema(shared_ptr<ArraySchema> indexed_array_schema) {
  // Get dimension label schema metadata
  GroupV1 label_group{uri_, storage_manager_};
  throw_if_not_ok(label_group.open(QueryType::READ));
  Datatype datatype;
  uint32_t value_num;

  // - Read format version.
  const void* format_version_value;
  throw_if_not_ok(label_group.get_metadata(
      "__dimension_label_format_version",
      &datatype,
      &value_num,
      &format_version_value));
  if (!format_version_value)
    throw std::runtime_error(
        "[DimensionLabel::load_schema] Unable to load dimension label schema; "
        "Failed to read dimension label schema format version.");
  if (datatype != Datatype::UINT32)
    throw std::runtime_error(
        "[DimensionLabel::load_schema] Unable to load dimension label schema; "
        "Unexpected datatype for the format version datatype.");
  if (value_num != 1)
    throw std::runtime_error(
        "[DimensionLabel::load_schema] Unable to load dimension label schema; "
        "Unexpected number of values for the format version.");
  format_version_t format_version{
      *static_cast<const format_version_t*>(format_version_value)};
  if (format_version > 1)
    throw StatusException(
        "DimensionLabel",
        "Cannot load dimension label schema. Version is newer than current "
        "library version.");

  // - Read label order
  const void* label_order_value;
  throw_if_not_ok(label_group.get_metadata(
      "__label_order", &datatype, &value_num, &label_order_value));
  if (!label_order_value)
    throw std::runtime_error(
        "[DimensionLabel::load_schema] Unable to load dimension label schema; "
        "Failed to read the label order of the dimension label.");
  if (datatype != Datatype::UINT8)
    throw std::runtime_error(
        "[DimensionLabel::load_schema] Unable to load dimension label schema; "
        "Unexpected datatype for the index attribute id.");
  if (value_num != 1)
    throw std::runtime_error(
        "[DimensionLabel::load_schema] Unable to load dimension label schema; "
        "Unexpected number of values for the index attribute id.");
  auto label_order =
      label_order_from_int(*static_cast<const uint8_t*>(label_order_value));

  // - Close group
  throw_if_not_ok(label_group.close());

  // Get array schemas
  schema_ = make_shared<DimensionLabelSchema>(
      HERE(), label_order, indexed_array_schema);
}

QueryType DimensionLabel::query_type() const {
  if (query_type_.has_value())
    return query_type_.value();
  throw std::runtime_error(
      "[DimensionLabel::query_type] No query type set; dimension label has "
      "not been opened");
}

const DimensionLabelSchema& DimensionLabel::schema() const {
  if (!schema_) {
    throw std::logic_error(
        "DimensionLabel schema does not exist. DimensionLabel must be opened.");
  }
  return *schema_;
}

void create_dimension_label(
    const URI& uri,
    StorageManager& storage_manager,
    const DimensionLabelSchema& schema) {
  // Create the group for the dimension label.
  throw_if_not_ok(storage_manager.group_create(uri.to_string()));

  // Create the arrays inside the group.
  EncryptionKey key;
  throw_if_not_ok(key.set_key(EncryptionType::NO_ENCRYPTION, nullptr, 0));
  std::optional<std::string> indexed_array_name{
      DimensionLabel::indexed_array_name};
  throw_if_not_ok(storage_manager.array_create(
      uri.join_path(indexed_array_name.value()),
      schema.indexed_array_schema(),
      key));

  // Open dimension label group.
  GroupV1 label_group{uri, &storage_manager};
  throw_if_not_ok(label_group.open(QueryType::WRITE));

  // Add metadata to group.
  const format_version_t format_version{1};
  throw_if_not_ok(label_group.put_metadata(
      "__dimension_label_format_version",
      Datatype::UINT32,
      1,
      &format_version));
  uint8_t label_order_int{static_cast<uint8_t>(schema.label_order())};
  throw_if_not_ok(label_group.put_metadata(
      "__label_order", Datatype::UINT8, 1, &label_order_int));

  // Add arrays to group.
  throw_if_not_ok(label_group.mark_member_for_addition(
      URI(indexed_array_name.value(), false), true, indexed_array_name));

  // Close group.
  throw_if_not_ok(label_group.close());
}

}  // namespace tiledb::sm
