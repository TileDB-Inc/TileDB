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
#include "tiledb/sm/enums/data_order.h"
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

/** Class for locally generated status exceptions. */
class AttributeStatusException : public StatusException {
 public:
  explicit AttributeStatusException(const std::string& msg)
      : StatusException("Attribute", msg) {
  }
};

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Attribute::Attribute()
    : Attribute("", Datatype::CHAR, false) {
}

Attribute::Attribute(
    const std::string& name, const Datatype type, const bool nullable)
    : cell_val_num_(type == Datatype::ANY ? constants::var_num : 1)
    , nullable_(nullable)
    , name_(name)
    , type_(type)
    , order_(DataOrder::UNORDERED_DATA) {
  set_default_fill_value();
}

Attribute::Attribute(
    const std::string& name,
    Datatype type,
    uint32_t cell_val_num,
    DataOrder order)
    : cell_val_num_(cell_val_num)
    , nullable_(false)
    , name_(name)
    , type_(type)
    , order_(order) {
  set_default_fill_value();

  // If ordered, check the number of values of cells is supported.
  if (order_ != DataOrder::UNORDERED_DATA) {
    ensure_ordered_attribute_datatype_is_valid(type_);
    if (type == Datatype::STRING_ASCII) {
      if (cell_val_num_ != constants::var_num) {
        throw std::invalid_argument(
            "Ordered attributes with datatype '" + datatype_str(type_) +
            "' must have cell_val_num=1.");
      }
    } else {
      if (cell_val_num_ != 1) {
        throw std::invalid_argument(
            "Ordered attributes with datatype '" + datatype_str(type_) +
            "' must have cell_val_num=1.");
      }
    }
  }
}

Attribute::Attribute(
    const std::string& name,
    Datatype type,
    bool nullable,
    uint32_t cell_val_num,
    const FilterPipeline& filter_pipeline,
    const ByteVecValue& fill_value,
    uint8_t fill_value_validity,
    DataOrder order)
    : cell_val_num_(cell_val_num)
    , nullable_(nullable)
    , filters_(filter_pipeline)
    , name_(name)
    , type_(type)
    , fill_value_(fill_value)
    , fill_value_validity_(fill_value_validity)
    , order_(order) {
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
  order_ = attr->order_;
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

Attribute Attribute::deserialize(
    Deserializer& deserializer, const uint32_t version) {
  // Load attribute name
  auto attribute_name_size = deserializer.read<uint32_t>();
  std::string name(
      deserializer.get_ptr<char>(attribute_name_size), attribute_name_size);

  // Load type
  auto type = deserializer.read<uint8_t>();

  Datatype datatype = static_cast<Datatype>(type);

  // Load cell_val_num
  auto cell_val_num = deserializer.read<uint32_t>();

  // Load filter pipeline
  auto filterpipeline{FilterPipeline::deserialize(deserializer, version)};

  // Load fill value
  uint64_t fill_value_size = 0;
  ByteVecValue fill_value;
  if (version >= 6) {
    fill_value_size = deserializer.read<uint64_t>();
    assert(fill_value_size > 0);
    fill_value.resize(fill_value_size);
    fill_value.shrink_to_fit();
    deserializer.read(fill_value.data(), fill_value_size);
  } else {
    fill_value = default_fill_value(datatype, cell_val_num);
  }

  // Load nullable flag
  bool nullable = false;
  if (version >= 7) {
    nullable = deserializer.read<bool>();
  }

  // Load validity fill value
  uint8_t fill_value_validity = 0;
  if (version >= 7) {
    fill_value_validity = deserializer.read<uint8_t>();
  }

  // Load order
  DataOrder order{DataOrder::UNORDERED_DATA};
  if (version >= 17) {
    order = data_order_from_int(deserializer.read<uint8_t>());
  }

  return Attribute(
      name,
      datatype,
      nullable,
      cell_val_num,
      filterpipeline,
      fill_value,
      fill_value_validity,
      order);
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
  if (order_ != DataOrder::UNORDERED_DATA) {
    fprintf(out, "\n");
    fprintf(out, "- Data ordering: %s", data_order_str(order_).c_str());
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
// order (uint8_t)
void Attribute::serialize(
    Serializer& serializer, const uint32_t version) const {
  // Write attribute name
  auto attribute_name_size = static_cast<uint32_t>(name_.size());
  serializer.write<uint32_t>(attribute_name_size);
  serializer.write(name_.data(), attribute_name_size);

  // Write type
  serializer.write<uint8_t>(static_cast<uint8_t>(type_));

  // Write cell_val_num_
  serializer.write<uint32_t>(cell_val_num_);

  // Write filter pipeline
  filters_.serialize(serializer);

  // Write fill value
  if (version >= 6) {
    auto fill_value_size = static_cast<uint64_t>(fill_value_.size());
    assert(fill_value_size != 0);
    serializer.write<uint64_t>(fill_value_size);
    serializer.write(fill_value_.data(), fill_value_.size());
  }

  // Write nullable
  if (version >= 7) {
    serializer.write<uint8_t>(nullable_);
  }

  // Write validity fill value
  if (version >= 7) {
    serializer.write<uint8_t>(fill_value_validity_);
  }

  // Write order
  if (version >= 17) {
    serializer.write<uint8_t>(static_cast<uint8_t>(order_));
  }
}

void Attribute::set_cell_val_num(unsigned int cell_val_num) {
  if (type_ == Datatype::ANY) {
    throw AttributeStatusException(
        "Cannot set number of values per cell; Attribute datatype `ANY` is "
        "always variable-sized");
  }

  if (order_ != DataOrder::UNORDERED_DATA && type_ != Datatype::STRING_ASCII &&
      cell_val_num != 1) {
    throw AttributeStatusException(
        "Cannot set number of values per cell; An ordered attribute with "
        "datatype '" +
        datatype_str(type_) + "' can only have cell_val_num=1.");
  }

  cell_val_num_ = cell_val_num;
  set_default_fill_value();
}

void Attribute::set_nullable(const bool nullable) {
  if (nullable && order_ != DataOrder::UNORDERED_DATA) {
    throw AttributeStatusException(
        "Cannot set to nullable; An ordered attribute cannot be nullable.");
  }
  nullable_ = nullable;
}

void Attribute::set_filter_pipeline(const FilterPipeline& pipeline) {
  // TODO: move this in to FilterPipeline::check_filter_types
  if ((type_ == Datatype::STRING_ASCII || type_ == Datatype::STRING_UTF8) &&
      var_size() && pipeline.size() > 1) {
    if (pipeline.has_filter(FilterType::FILTER_RLE) &&
        pipeline.get_filter(0)->type() != FilterType::FILTER_RLE) {
      throw AttributeStatusException(
          "RLE filter must be the first filter to apply when used on a "
          "variable length string attribute");
    }
    if (pipeline.has_filter(FilterType::FILTER_DICTIONARY) &&
        pipeline.get_filter(0)->type() != FilterType::FILTER_DICTIONARY) {
      throw AttributeStatusException(
          "Dictionary filter must be the first filter to apply when used on a "
          "variable length string attribute");
    }
  }

  FilterPipeline::check_filter_types(pipeline, type_);

  filters_ = pipeline;
}

void Attribute::set_name(const std::string& name) {
  name_ = name;
}

void Attribute::set_fill_value(const void* value, uint64_t size) {
  if (value == nullptr) {
    throw AttributeStatusException(
        "Cannot set fill value; Input value cannot be null");
  }

  if (size == 0) {
    throw AttributeStatusException(
        "Cannot set fill value; Input size cannot be 0");
  }

  if (nullable()) {
    throw AttributeStatusException(
        "Cannot set fill value; Attribute is nullable");
  }

  if (!var_size() && size != cell_size()) {
    throw AttributeStatusException(
        "Cannot set fill value; Input size is not the same as cell size");
  }

  fill_value_.resize(size);
  fill_value_.shrink_to_fit();
  std::memcpy(fill_value_.data(), value, size);
}

void Attribute::get_fill_value(const void** value, uint64_t* size) const {
  if (value == nullptr) {
    throw AttributeStatusException(
        "Cannot get fill value; Input value cannot be null");
  }

  if (size == nullptr) {
    throw AttributeStatusException(
        "Cannot get fill value; Input size cannot be null");
  }

  if (nullable()) {
    throw AttributeStatusException(
        "Cannot get fill value; Attribute is nullable");
  }

  *value = fill_value_.data();
  *size = (uint64_t)fill_value_.size();
}

void Attribute::set_fill_value(
    const void* value, uint64_t size, uint8_t valid) {
  if (value == nullptr) {
    throw AttributeStatusException(
        "Cannot set fill value; Input value cannot be null");
  }

  if (size == 0) {
    throw AttributeStatusException(
        "Cannot set fill value; Input size cannot be 0");
  }

  if (!nullable()) {
    throw AttributeStatusException(
        "Cannot set fill value; Attribute is not nullable");
  }

  if (!var_size() && size != cell_size()) {
    throw AttributeStatusException(
        "Cannot set fill value; Input size is not the same as cell size");
  }

  fill_value_.resize(size);
  fill_value_.shrink_to_fit();
  std::memcpy(fill_value_.data(), value, size);
  fill_value_validity_ = valid;
}

void Attribute::get_fill_value(
    const void** value, uint64_t* size, uint8_t* valid) const {
  if (value == nullptr) {
    throw AttributeStatusException(
        "Cannot get fill value; Input value cannot be null");
  }

  if (size == nullptr) {
    throw AttributeStatusException(
        "Cannot get fill value; Input size cannot be null");
  }

  if (!nullable()) {
    throw AttributeStatusException(
        "Cannot get fill value; Attribute is not nullable");
  }

  *value = fill_value_.data();
  *size = (uint64_t)fill_value_.size();
  *valid = fill_value_validity_;
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

DataOrder Attribute::order() const {
  return order_;
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
