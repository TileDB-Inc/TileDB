/**
 * @file   dimension_label.cc
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
 * @section DESCRIPTION
 *
 * This file defines the DimensionLabel class.
 */

#include "dimension_label.h"
#include "tiledb/sm/buffer/buffer.h"

namespace tiledb::sm {

/************************************************/
/*               Dimension Label                */
/************************************************/

DimensionLabel::DimensionLabel(
    LabelType label_type,
    DimensionLabel::BaseSchema& schema,
    shared_ptr<DimensionLabelMapping> label_index_map)
    : label_type_(label_type)
    , schema_(schema)
    , label_index_map_(label_index_map) {
}

tuple<Status, shared_ptr<DimensionLabel>> DimensionLabel::create_uniform(
    DimensionLabel::BaseSchema&& schema) {
  if (schema.label_cell_val_num != 1 || schema.index_cell_val_num != 1)
    return {Status_DimensionLabelError(
                "Unable to create uniform dimension label; both label and "
                "index must have cell value of length 1"),
            nullptr};
  try {
    return {Status::Ok(),
            make_shared<DimensionLabel>(
                HERE(),
                LabelType::LABEL_UNIFORM,
                schema,
                create_uniform_mapping(
                    schema.label_datatype,
                    schema.label_domain,
                    schema.index_datatype,
                    schema.index_domain))};
  } catch (std::logic_error& err) {
    std::string msg{err.what()};
    return {Status_DimensionLabelError(
                "Unable to create uniform dimension label; " + msg),
            nullptr};
  }
}

tuple<Status, shared_ptr<DimensionLabel>> DimensionLabel::deserialize(
    ConstBuffer* buff,
    uint32_t version,
    Datatype index_datatype,
    uint32_t index_cell_val_num,
    const Range& index_domain) {
  // Load dimension label type
  uint8_t label_type_data{};
  auto status = buff->read(&label_type_data, sizeof(uint8_t));
  if (!status.ok())
    return {status, nullptr};
  LabelType label_type{label_type_data};
  // Load base dimension label data
  auto&& [schema_status, schema] = DimensionLabel::BaseSchema::deserialize(
      buff, version, index_datatype, index_cell_val_num, index_domain);
  if (!status.ok())
    return {status, nullptr};
  // Load mapping parameters and data type.
  switch (label_type) {
    case LabelType::LABEL_UNIFORM:
      return DimensionLabel::create_uniform(std::move(schema.value()));
    default:
      return {
          Status_DimensionLabelError(
              "Unabel to create dimension label; The requested dimension label "
              "type is not supported " +
              label_type_str(label_type)),
          nullptr};
  }
}

tuple<Status, Range> DimensionLabel::index_range(const Range& labels) const {
  try {
    return {Status::Ok(), label_index_map_->index_range(labels)};
  } catch (std::logic_error& err) {
    std::string msg{err.what()};
    return {Status_DimensionLabelError(
                "Unable to get index range from label range; " + msg),
            Range()};
  }
}

Status DimensionLabel::serialize(Buffer* buff, uint32_t version) const {
  // Write the dimension label type.
  auto label_type_data = static_cast<uint8_t>(label_type_);
  RETURN_NOT_OK(buff->write(&label_type_data, sizeof(uint8_t)));
  // Write base schema.
  RETURN_NOT_OK(schema_.serialize(buff, version));
  // Write metadata for specific mapping - currently always zero.
  uint32_t num_metadata{0};
  RETURN_NOT_OK(buff->write(&num_metadata, sizeof(uint32_t)));
  return Status::Ok();
}

/************************************************/
/*        Dimension Label Base Schema           */
/************************************************/

DimensionLabel::BaseSchema::BaseSchema(
    const std::string& name,
    Datatype label_datatype,
    uint32_t label_cell_val_num,
    const Range& label_domain,
    Datatype index_datatype,
    uint32_t index_cell_val_num,
    const Range& index_domain)
    : name(name)
    , label_datatype(label_datatype)
    , label_cell_val_num(label_cell_val_num)
    , label_domain(label_domain)
    , index_datatype(index_datatype)
    , index_cell_val_num(index_cell_val_num)
    , index_domain(index_domain) {
}

// Format:
// dimension label size (uint32_t)
// dimension label name (c-string)
// label datatype (uint8_t)
// label number of values per cell (uint32_t)
// label domain size (uint64_t)
// label domain (void* - domain size)
// label metadata size (uint32_t - specific to filter type)
// label metadata (specific to filter type)
tuple<Status, optional<DimensionLabel::BaseSchema>>
DimensionLabel::BaseSchema::deserialize(
    ConstBuffer* buff,
    uint32_t,
    Datatype index_datatype,
    uint32_t index_cell_val_num,
    const Range& index_domain) {
  // Load dimension label name
  uint32_t dimension_name_size;
  auto status = buff->read(&dimension_name_size, sizeof(uint32_t));
  if (!status.ok())
    return {status, nullopt};
  std::string name;
  name.resize(dimension_name_size);
  status = buff->read(&name[0], dimension_name_size);
  if (!status.ok())
    return {status, nullopt};
  // Load label datatype
  uint8_t datatype_data{};
  status = buff->read(&datatype_data, sizeof(uint8_t));
  if (!status.ok())
    return {status, nullopt};
  Datatype label_datatype{datatype_data};
  // Load the number values in a cell for the label
  uint32_t label_cell_val_num{};
  status = buff->read(&label_cell_val_num, sizeof(uint32_t));
  if (!status.ok())
    return {status, nullopt};
  // Load the label domain
  uint64_t domain_size{};
  status = buff->read(&domain_size, sizeof(uint64_t));
  if (!status.ok())
    return {status, nullopt};
  Range label_domain;
  if (domain_size != 0) {
    std::vector<uint8_t> tmp(domain_size);
    status = buff->read(&tmp[0], domain_size);
    label_domain = Range(&tmp[0], domain_size);
  }
  return {Status::Ok(),
          DimensionLabel::BaseSchema(
              name,
              label_datatype,
              label_cell_val_num,
              label_domain,
              index_datatype,
              index_cell_val_num,
              index_domain)};
}

// Format:
// dimension label size (uint32_t)
// dimension label name (c-string)
// label datatype (uint8_t)
// label number of values per cell (uint32_t)
// label domain size (uint64_t)
// label domain (void* - domain size)
// label metadata size (uint32_t - specific to filter type)
// label metadata (specific to filter type)
Status DimensionLabel::BaseSchema::serialize(Buffer* buff, uint32_t) const {
  // Write the dimension label name size and name
  auto name_size = (uint32_t)name.size();
  RETURN_NOT_OK(buff->write(&name_size, sizeof(uint32_t)));
  RETURN_NOT_OK(buff->write(name.c_str(), name_size));
  // Write the dimension label datatype
  auto label_datatype_data = static_cast<uint8_t>(label_datatype);
  RETURN_NOT_OK(buff->write(&label_datatype_data, sizeof(uint8_t)));
  // Write the dimension label number of values per cell
  RETURN_NOT_OK(buff->write(&label_cell_val_num, sizeof(uint32_t)));
  // Write the dimension label domain size and domain
  if (datatype_is_string(label_datatype)) {
    // sanity check: domain should be empty for string datatypes
    if (!label_domain.empty())
      throw std::logic_error("Domain should be empty for string dimensions");
    uint64_t domain_size{0};
    RETURN_NOT_OK(buff->write(&domain_size, sizeof(uint64_t)));
  } else {
    uint64_t domain_size{2 * datatype_size(label_datatype)};
    RETURN_NOT_OK(buff->write(&domain_size, sizeof(uint64_t)));
    RETURN_NOT_OK(buff->write(label_domain.data(), domain_size));
  }
  return Status::Ok();
}

}  // namespace tiledb::sm
