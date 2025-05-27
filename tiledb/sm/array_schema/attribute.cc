/**
 * @file   attribute.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
#include "tiledb/common/assert.h"
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

namespace tiledb::sm {

/** Class for locally generated status exceptions. */
class AttributeStatusException : public StatusException {
 public:
  explicit AttributeStatusException(const std::string& msg)
      : StatusException("Attribute", msg) {
  }
};

using AttributeException = AttributeStatusException;

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

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
  if (order_ != DataOrder::UNORDERED_DATA) {
    ensure_ordered_attribute_datatype_is_valid(type_);
  }
  validate_cell_val_num(cell_val_num_);
  set_default_fill_value();
}

Attribute::Attribute(
    const std::string& name,
    Datatype type,
    bool nullable,
    uint32_t cell_val_num,
    const FilterPipeline& filter_pipeline,
    const ByteVecValue& fill_value,
    uint8_t fill_value_validity,
    DataOrder order,
    std::optional<std::string> enumeration_name)
    : cell_val_num_(cell_val_num)
    , nullable_(nullable)
    , filters_(filter_pipeline)
    , name_(name)
    , type_(type)
    , fill_value_(fill_value)
    , fill_value_validity_(fill_value_validity)
    , order_(order)
    , enumeration_name_(enumeration_name) {
  validate_cell_val_num(cell_val_num_);
}

/* ********************************* */
/*                API                */
/* ********************************* */

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
  auto filterpipeline{
      FilterPipeline::deserialize(deserializer, version, datatype)};

  // Load fill value
  uint64_t fill_value_size = 0;
  ByteVecValue fill_value;
  if (version >= 6) {
    fill_value_size = deserializer.read<uint64_t>();
    iassert(fill_value_size > 0);
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

  std::optional<std::string> enmr_name;
  if (version >= constants::enumerations_min_format_version) {
    auto enmr_name_length = deserializer.read<uint32_t>();
    if (enmr_name_length > 0) {
      std::string enmr_name_value;
      enmr_name_value.resize(enmr_name_length);
      deserializer.read(enmr_name_value.data(), enmr_name_length);
      enmr_name = enmr_name_value;
    }
  }

  return Attribute(
      name,
      datatype,
      nullable,
      cell_val_num,
      filterpipeline,
      fill_value,
      fill_value_validity,
      order,
      enmr_name);
}

const FilterPipeline& Attribute::filters() const {
  return filters_;
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
    iassert(fill_value_size != 0);
    serializer.write<uint64_t>(fill_value_size);
    serializer.write(fill_value_.data(), fill_value_.size());
  }

  // Write nullable
  if (version >= 7) {
    /*
     * Data coherence across platforms relies on the C++ integral conversion
     * rule that mandates `false` be converted to 0 and `true` to 1.
     */
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

  // Write enumeration URI
  if (version >= constants::enumerations_min_format_version) {
    if (enumeration_name_.has_value()) {
      auto enmr_name_size =
          static_cast<uint32_t>(enumeration_name_.value().size());
      serializer.write<uint32_t>(enmr_name_size);
      serializer.write(enumeration_name_.value().data(), enmr_name_size);
    } else {
      serializer.write<uint32_t>(0);
    }
  }
}

void Attribute::set_cell_val_num(unsigned int cell_val_num) {
  validate_cell_val_num(cell_val_num);
  cell_val_num_ = cell_val_num;
  set_default_fill_value();
}

void Attribute::validate_cell_val_num(unsigned int cell_val_num) const {
  if (type_ == Datatype::ANY && cell_val_num != constants::var_num) {
    throw AttributeException(
        "Cannot set number of values per cell; Attribute datatype `ANY` is "
        "always variable-sized");
  }

  // If ordered, check the number of values of cells is supported.
  if (order_ != DataOrder::UNORDERED_DATA) {
    if (type_ == Datatype::STRING_ASCII) {
      if (cell_val_num != constants::var_num) {
        throw AttributeException(
            "Cannot set number of values per cell; Ordered attributes with "
            "datatype '" +
            datatype_str(type_) +
            "' must have `cell_val_num=constants::var_num`.");
      }
    } else {
      if (cell_val_num != 1) {
        throw AttributeException(
            "Ordered attributes with datatype '" + datatype_str(type_) +
            "' must have `cell_val_num=1`.");
      }
    }
  }

  // check zero last so we get the more informative error first
  if (cell_val_num == 0) {
    throw AttributeException("Cannot set zero values per cell");
  }
}

void Attribute::set_nullable(const bool nullable) {
  if (nullable && order_ != DataOrder::UNORDERED_DATA) {
    throw AttributeStatusException(
        "Cannot set to nullable; An ordered attribute cannot be nullable.");
  }
  nullable_ = nullable;
}

void Attribute::set_filter_pipeline(const FilterPipeline& pipeline) {
  FilterPipeline::check_filter_types(pipeline, type_, var_size());
  filters_ = pipeline;
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

void Attribute::set_enumeration_name(std::optional<std::string> enmr_name) {
  if (enmr_name.has_value() && enmr_name.value().empty()) {
    throw AttributeStatusException(
        "Invalid enumeration name, name must not be empty.");
  }
  enumeration_name_ = enmr_name;
}

std::optional<std::string> Attribute::get_enumeration_name() const {
  return enumeration_name_;
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

}  // namespace tiledb::sm

std::ostream& operator<<(std::ostream& os, const tiledb::sm::Attribute& a) {
  os << "### Attribute ###\n";
  os << "- Name: " << a.name() << std::endl;
  os << "- Type: " << datatype_str(a.type()) << std::endl;
  os << "- Nullable: " << (a.nullable() ? "true" : "false") << std::endl;
  if (!a.var_size())
    os << "- Cell val num: " << a.cell_val_num() << std::endl;
  else
    os << "- Cell val num: var\n";
  os << "- Filters: " << a.filters().size();
  os << a.filters();
  os << std::endl;

  os << "- Fill value: ";
  auto v_size = tiledb::sm::datatype_size(a.type());
  uint64_t num = a.fill_value().size() / v_size;
  auto v = a.fill_value().data();
  for (uint64_t i = 0; i < num; ++i) {
    os << tiledb::sm::utils::parse::to_str(v, a.type());
    v += v_size;
    if (i != num - 1)
      os << ", ";
  }

  if (a.nullable()) {
    os << std::endl;
    os << "- Fill value validity: " << std::to_string(a.fill_value_validity());
  }
  if (a.order() != tiledb::sm::DataOrder::UNORDERED_DATA) {
    os << std::endl;
    os << "- Data ordering: " << data_order_str(a.order());
  }
  if (a.get_enumeration_name().has_value()) {
    os << std::endl;
    os << "- Enumeration name: " << a.get_enumeration_name().value();
  }
  os << std::endl;

  return os;
}
