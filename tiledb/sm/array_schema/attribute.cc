/**
 * @file   attribute.cc
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
 * This file implements class Attribute.
 */

#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/misc/parse_argument.h"
#include "tiledb/type/range/range.h"

#include <cassert>
#include <iostream>
#include <sstream>

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Attribute::Attribute()
    : Attribute("", Datatype::CHAR, false) {
}

Attribute::Attribute(
    const std::string& name, const Datatype type, const bool nullable) {
  name_ = name;
  type_ = type;
  nullable_ = nullable;
  cell_val_num_ = (type == Datatype::ANY) ? constants::var_num : 1;
  set_default_fill_value();
}

Attribute::Attribute(
    const std::string& name,
    Datatype type,
    bool nullable,
    uint32_t cell_val_num,
    const FilterPipeline& filter_pipeline,
    const ByteVecValue& fill_value,
    uint8_t fill_value_validity)
    : cell_val_num_(cell_val_num)
    , nullable_(nullable)
    , filters_(filter_pipeline)
    , name_(name)
    , type_(type)
    , fill_value_(fill_value)
    , fill_value_validity_(fill_value_validity) {
}

Attribute::Attribute(const Attribute* attr) {
  assert(attr != nullptr);
  name_ = attr->name();
  type_ = attr->type();
  cell_val_num_ = attr->cell_val_num();
  nullable_ = attr->nullable();
  filters_ = attr->filters_;
  fill_value_ = attr->fill_value_;
  fill_value_validity_ = attr->fill_value_validity_;
}

Attribute::~Attribute() = default;

/* ********************************* */
/*                API                */
/* ********************************* */

uint64_t Attribute::cell_size() const {
  if (var_size())
    return constants::var_size;

  return cell_val_num_ * datatype_size(type_);
}

unsigned int Attribute::cell_val_num() const {
  return cell_val_num_;
}

tuple<Status, optional<Attribute>> Attribute::deserialize(
    ConstBuffer* buff, const uint32_t version) {
  // Load attribute name
  uint32_t attribute_name_size;
  RETURN_NOT_OK_TUPLE(
      buff->read(&attribute_name_size, sizeof(uint32_t)), nullopt);

  std::string name;
  name.resize(attribute_name_size);
  RETURN_NOT_OK_TUPLE(buff->read(&name[0], attribute_name_size), nullopt);

  // Load type
  uint8_t type;
  RETURN_NOT_OK_TUPLE(buff->read(&type, sizeof(uint8_t)), nullopt);

  Datatype datatype = static_cast<Datatype>(type);

  // Load cell_val_num
  uint32_t cell_val_num;
  RETURN_NOT_OK_TUPLE(buff->read(&cell_val_num, sizeof(uint32_t)), nullopt);

  // Load filter pipeline
  auto&& [st_filterpipeline, filterpipeline]{
      FilterPipeline::deserialize(buff, version)};
  if (!st_filterpipeline.ok()) {
    return {st_filterpipeline, nullopt};
  }

  // Load fill value
  uint64_t fill_value_size = 0;
  ByteVecValue fill_value;
  if (version >= 6) {
    RETURN_NOT_OK_TUPLE(
        buff->read(&fill_value_size, sizeof(uint64_t)), nullopt);
    assert(fill_value_size > 0);
    fill_value.resize(fill_value_size);
    fill_value.shrink_to_fit();
    RETURN_NOT_OK_TUPLE(
        buff->read(fill_value.data(), fill_value_size), nullopt);
  } else {
    fill_value = default_fill_value(datatype, cell_val_num);
  }

  // Load nullable flag
  bool nullable = false;
  if (version >= 7) {
    RETURN_NOT_OK_TUPLE(buff->read(&nullable, sizeof(bool)), nullopt);
  }

  // Load validity fill value
  uint8_t fill_value_validity = 0;
  if (version >= 7) {
    RETURN_NOT_OK_TUPLE(
        buff->read(&fill_value_validity, sizeof(uint8_t)), nullopt);
  }

  return {Status::Ok(),
          optional<Attribute>(
              std::in_place,
              name,
              datatype,
              nullable,
              cell_val_num,
              filterpipeline.value(),
              fill_value,
              fill_value_validity)};
}
void Attribute::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;
  // Dump
  fprintf(out, "### Attribute ###\n");
  fprintf(out, "- Name: %s\n", name_.c_str());
  fprintf(out, "- Type: %s\n", datatype_str(type_).c_str());
  fprintf(out, "- Nullable: %s\n", (nullable_ ? "true" : "false"));
  if (!var_size())
    fprintf(out, "- Cell val num: %u\n", cell_val_num_);
  else
    fprintf(out, "- Cell val num: var\n");
  fprintf(out, "- Filters: %u", (unsigned)filters_.size());
  filters_.dump(out);
  fprintf(out, "\n");
  fprintf(out, "- Fill value: %s", fill_value_str().c_str());
  if (nullable_) {
    fprintf(out, "\n");
    fprintf(out, "- Fill value validity: %u", fill_value_validity_);
  }
  fprintf(out, "\n");
}

const FilterPipeline& Attribute::filters() const {
  return filters_;
}

const std::string& Attribute::name() const {
  return name_;
}

// ===== FORMAT =====
// attribute_name_size (uint32_t)
// attribute_name (string)
// type (uint8_t)
// cell_val_num (uint32_t)
// filter_pipeline (see FilterPipeline::serialize)
// fill_value_size (uint64_t)
// fill_value (uint8_t[])
// nullable (bool)
// fill_value_validity (uint8_t)
const Status Attribute::serialize(Buffer* buff, const uint32_t version) const {
  // Write attribute name
  auto attribute_name_size = (uint32_t)name_.size();
  RETURN_NOT_OK(buff->write(&attribute_name_size, sizeof(uint32_t)));
  RETURN_NOT_OK(buff->write(name_.c_str(), attribute_name_size));

  // Write type
  auto type = (uint8_t)type_;
  RETURN_NOT_OK(buff->write(&type, sizeof(uint8_t)));

  // Write cell_val_num_
  RETURN_NOT_OK(buff->write(&cell_val_num_, sizeof(uint32_t)));

  // Write filter pipeline
  RETURN_NOT_OK(filters_.serialize(buff));

  // Write fill value
  if (version >= 6) {
    auto fill_value_size = (uint64_t)fill_value_.size();
    assert(fill_value_size != 0);
    RETURN_NOT_OK(buff->write(&fill_value_size, sizeof(uint64_t)));
    RETURN_NOT_OK(buff->write(fill_value_.data(), fill_value_.size()));
  }

  // Write nullable
  if (version >= 7)
    RETURN_NOT_OK(buff->write(&nullable_, sizeof(bool)));

  // Write validity fill value
  if (version >= 7)
    RETURN_NOT_OK(buff->write(&fill_value_validity_, sizeof(uint8_t)));

  return Status::Ok();
}

Status Attribute::set_cell_val_num(unsigned int cell_val_num) {
  if (type_ == Datatype::ANY)
    return LOG_STATUS(Status_AttributeError(
        "Cannot set number of values per cell; Attribute datatype `ANY` is "
        "always variable-sized"));

  cell_val_num_ = cell_val_num;
  set_default_fill_value();

  return Status::Ok();
}

Status Attribute::set_nullable(const bool nullable) {
  nullable_ = nullable;
  return Status::Ok();
}

Status Attribute::get_nullable(bool* const nullable) {
  *nullable = nullable_;
  return Status::Ok();
}

Status Attribute::set_filter_pipeline(const FilterPipeline* pipeline) {
  if (pipeline == nullptr)
    return LOG_STATUS(Status_AttributeError(
        "Cannot set filter pipeline to attribute; Pipeline cannot be null"));

  for (unsigned i = 0; i < pipeline->size(); ++i) {
    if (datatype_is_real(type_) &&
        pipeline->get_filter(i)->type() == FilterType::FILTER_DOUBLE_DELTA)
      return LOG_STATUS(
          Status_AttributeError("Cannot set DOUBLE DELTA filter to an "
                                "attribute with a real datatype"));
  }

  if (type_ == Datatype::STRING_ASCII && var_size() && pipeline->size() > 1) {
    if (pipeline->has_filter(FilterType::FILTER_RLE)) {
      return LOG_STATUS(Status_AttributeError(
          "RLE filter cannot be combined with other filters when applied to "
          "variable length string attributes"));
    } else if (pipeline->has_filter(FilterType::FILTER_DICTIONARY)) {
      return LOG_STATUS(Status_AttributeError(
          "Dictionary-encoding filter cannot be combined with other filters "
          "when applied to variable length string attributes"));
    }
  }

  filters_ = *pipeline;

  return Status::Ok();
}

void Attribute::set_name(const std::string& name) {
  name_ = name;
}

Status Attribute::set_fill_value(const void* value, uint64_t size) {
  if (value == nullptr) {
    return LOG_STATUS(Status_AttributeError(
        "Cannot set fill value; Input value cannot be null"));
  }

  if (size == 0) {
    return LOG_STATUS(
        Status_AttributeError("Cannot set fill value; Input size cannot be 0"));
  }

  if (nullable()) {
    return LOG_STATUS(
        Status_AttributeError("Cannot set fill value; Attribute is nullable"));
  }

  if (!var_size() && size != cell_size()) {
    return LOG_STATUS(Status_AttributeError(
        "Cannot set fill value; Input size is not the same as cell size"));
  }

  fill_value_.resize(size);
  fill_value_.shrink_to_fit();
  std::memcpy(fill_value_.data(), value, size);

  return Status::Ok();
}

Status Attribute::get_fill_value(const void** value, uint64_t* size) const {
  if (value == nullptr) {
    return LOG_STATUS(Status_AttributeError(
        "Cannot get fill value; Input value cannot be null"));
  }

  if (size == nullptr) {
    return LOG_STATUS(Status_AttributeError(
        "Cannot get fill value; Input size cannot be null"));
  }

  if (nullable()) {
    return LOG_STATUS(
        Status_AttributeError("Cannot get fill value; Attribute is nullable"));
  }

  *value = fill_value_.data();
  *size = (uint64_t)fill_value_.size();

  return Status::Ok();
}

Status Attribute::set_fill_value(
    const void* value, uint64_t size, uint8_t valid) {
  if (value == nullptr) {
    return LOG_STATUS(Status_AttributeError(
        "Cannot set fill value; Input value cannot be null"));
  }

  if (size == 0) {
    return LOG_STATUS(
        Status_AttributeError("Cannot set fill value; Input size cannot be 0"));
  }

  if (!nullable()) {
    return LOG_STATUS(Status_AttributeError(
        "Cannot set fill value; Attribute is not nullable"));
  }

  if (!var_size() && size != cell_size()) {
    return LOG_STATUS(Status_AttributeError(
        "Cannot set fill value; Input size is not the same as cell size"));
  }

  fill_value_.resize(size);
  fill_value_.shrink_to_fit();
  std::memcpy(fill_value_.data(), value, size);
  fill_value_validity_ = valid;

  return Status::Ok();
}

Status Attribute::get_fill_value(
    const void** value, uint64_t* size, uint8_t* valid) const {
  if (value == nullptr) {
    return LOG_STATUS(Status_AttributeError(
        "Cannot get fill value; Input value cannot be null"));
  }

  if (size == nullptr) {
    return LOG_STATUS(Status_AttributeError(
        "Cannot get fill value; Input size cannot be null"));
  }

  if (!nullable()) {
    return LOG_STATUS(Status_AttributeError(
        "Cannot get fill value; Attribute is not nullable"));
  }

  *value = fill_value_.data();
  *size = (uint64_t)fill_value_.size();
  *valid = fill_value_validity_;

  return Status::Ok();
}

const ByteVecValue& Attribute::fill_value() const {
  return fill_value_;
}

uint8_t Attribute::fill_value_validity() const {
  return fill_value_validity_;
}

Datatype Attribute::type() const {
  return type_;
}

bool Attribute::var_size() const {
  return cell_val_num_ == constants::var_num;
}

bool Attribute::nullable() const {
  return nullable_;
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

void Attribute::set_default_fill_value() {
  auto fill_value = constants::fill_value(type_);
  auto fill_size = datatype_size(type_);
  auto cell_num = (var_size()) ? 1 : cell_val_num_;

  fill_value_.resize(cell_num * fill_size);
  fill_value_.shrink_to_fit();
  uint64_t offset = 0;
  auto buff = (unsigned char*)fill_value_.data();
  for (uint64_t i = 0; i < cell_num; ++i) {
    std::memcpy(buff + offset, fill_value, fill_size);
    offset += fill_size;
  }

  fill_value_validity_ = 0;
}

ByteVecValue Attribute::default_fill_value(
    Datatype datatype, uint32_t cell_val_num) {
  ByteVecValue fillvalue;
  auto fill_value = constants::fill_value(datatype);
  auto fill_size = datatype_size(datatype);
  if (cell_val_num == constants::var_num) {
    cell_val_num = 1;
  }
  fillvalue.resize(cell_val_num * fill_size);
  fillvalue.shrink_to_fit();
  uint64_t offset = 0;
  auto buff = (unsigned char*)fillvalue.data();
  for (uint64_t i = 0; i < cell_val_num; ++i) {
    std::memcpy(buff + offset, fill_value, fill_size);
    offset += fill_size;
  }
  return fillvalue;
}

std::string Attribute::fill_value_str() const {
  std::string ret;

  auto v_size = datatype_size(type_);
  uint64_t num = fill_value_.size() / v_size;
  auto v = fill_value_.data();
  for (uint64_t i = 0; i < num; ++i) {
    ret += utils::parse::to_str(v, type_);
    v += v_size;
    if (i != num - 1)
      ret += ", ";
  }

  return ret;
}

}  // namespace sm
}  // namespace tiledb
