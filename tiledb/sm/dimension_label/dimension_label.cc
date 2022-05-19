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
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
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
    , labelled_array_(make_shared<Array>(
          HERE(), uri.join_path(labelled_array_name), storage_manager))
    , schema_{nullptr} {
}

void DimensionLabel::close() {
  throw_if_not_ok(indexed_array_->close());
  throw_if_not_ok(labelled_array_->close());
}

const Attribute* DimensionLabel::index_attribute() const {
  if (!schema_)
    throw std::logic_error(
        "DimensionLabel schema does not exist. DimensionLabel must be opened.");
  return schema_->index_attribute();
}

const Dimension* DimensionLabel::index_dimension() const {
  if (!schema_)
    throw std::logic_error(
        "DimensionLabel schema does not exist. DimensionLabel must be opened.");
  return schema_->index_dimension();
}

const Attribute* DimensionLabel::label_attribute() const {
  if (!schema_)
    throw std::logic_error(
        "DimensionLabel schema does not exist. DimensionLabel must be opened.");
  return schema_->label_attribute();
}

const Dimension* DimensionLabel::label_dimension() const {
  if (!schema_)
    throw std::logic_error(
        "DimensionLabel schema does not exist. DimensionLabel must be opened.");
  return schema_->label_dimension();
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
  throw_if_not_ok(indexed_array_->open(
      query_type,
      timestamp_start,
      timestamp_end,
      encryption_type,
      encryption_key,
      key_length));
  throw_if_not_ok(labelled_array_->open(
      query_type,
      timestamp_start,
      timestamp_end,
      encryption_type,
      encryption_key,
      key_length));
  load_schema();
}

void DimensionLabel::open_without_fragments(
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  throw_if_not_ok(indexed_array_->open_without_fragments(
      encryption_type, encryption_key, key_length));
  throw_if_not_ok(labelled_array_->open_without_fragments(
      encryption_type, encryption_key, key_length));
  load_schema();
}

void DimensionLabel::load_schema() {
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
        "Unable to load dimension label schema; Failed to read dimension label "
        "schema format version.");
  if (datatype != Datatype::UINT32)
    throw std::runtime_error(
        "Unable to load dimension label schema; Unexpected datatype for the "
        "format version datatype.");
  if (value_num != 1)
    throw std::runtime_error(
        "Unable to load dimension label schema; Unexpected number of values "
        "for the format version.");
  uint32_t format_version{*static_cast<const uint32_t*>(format_version_value)};
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
        "Unable to load dimension label schema; Failed to read the label order "
        "of the dimension label.");
  if (datatype != Datatype::UINT8)
    throw std::runtime_error(
        "Unable to load dimension label schema; Unexpected datatype for the "
        "index attribute id.");
  if (value_num != 1)
    throw std::runtime_error(
        "Unable to load dimension label schema; Unexpected number of values "
        "for the index attribute id.");
  LabelOrder label_order{*static_cast<const LabelOrder*>(label_order_value)};
  // - Close group
  throw_if_not_ok(label_group.close());
  // Get array schemas
  auto&& [label_status, label_schema] = labelled_array_->get_array_schema();
  throw_if_not_ok(label_status);
  auto&& [index_status, index_schema] = indexed_array_->get_array_schema();
  throw_if_not_ok(index_status);
  schema_ = make_shared<DimensionLabelSchema>(
      HERE(), label_order, index_schema.value(), label_schema.value());
}

QueryType DimensionLabel::query_type() const {
  QueryType indexed_query_type;
  throw_if_not_ok(indexed_array_->get_query_type(&indexed_query_type));
  QueryType labelled_query_type;
  throw_if_not_ok(labelled_array_->get_query_type(&labelled_query_type));
  if (indexed_query_type != labelled_query_type)
    throw std::runtime_error(
        "Dimension label opened with inconsistent query types.");
  return indexed_query_type;
}

void create_dimension_label(
    const URI& uri,
    StorageManager& storage_manager,
    const DimensionLabelSchema& schema) {
  // Create the group for the dimension label.
  storage_manager.group_create(uri.to_string());
  // Create the arrays inside the group.
  EncryptionKey key;
  key.set_key(EncryptionType::NO_ENCRYPTION, nullptr, 0);
  std::optional<std::string> indexed_array_name{
      DimensionLabel::indexed_array_name};
  storage_manager.array_create(
      uri.join_path(indexed_array_name.value()),
      schema.indexed_array_schema(),
      key);
  std::optional<std::string> labelled_array_name{
      DimensionLabel::labelled_array_name};
  storage_manager.array_create(
      uri.join_path(labelled_array_name.value()),
      schema.labelled_array_schema(),
      key);
  // Open dimension label group.
  GroupV1 label_group{uri, &storage_manager};
  label_group.open(QueryType::WRITE);
  // Add metadata to group.
  const uint32_t format_version{1};
  label_group.put_metadata(
      "__dimension_label_format_version", Datatype::UINT32, 1, &format_version);
  uint8_t label_order_int{static_cast<uint8_t>(schema.label_order())};
  label_group.put_metadata(
      "__label_order", Datatype::UINT8, 1, &label_order_int);
  // Add arrays to group.
  label_group.mark_member_for_addition(
      URI(indexed_array_name.value(), false), true, indexed_array_name);
  label_group.mark_member_for_addition(
      URI(labelled_array_name.value(), false), true, labelled_array_name);
  // Close group.
  label_group.close();
}

}  // namespace tiledb::sm
