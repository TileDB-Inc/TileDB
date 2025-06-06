/**
 * @file tiledb/sm/array_schema/dimension_label.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2023 TileDB, Inc.
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
 */

#include "tiledb/sm/array_schema/dimension_label.h"
#include "tiledb/common/common.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/data_order.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/type/range/range.h"

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::sm {

/** Class for locally generated status exceptions. */
class DimensionLabelStatusException : public StatusException {
 public:
  explicit DimensionLabelStatusException(const std::string& msg)
      : StatusException("DimensionLabel", msg) {
  }
};

DimensionLabel::DimensionLabel(
    dimension_size_type dim_id,
    const std::string& dim_label_name,
    const URI& uri,
    const std::string& label_attr_name,
    DataOrder label_order,
    Datatype label_type,
    uint32_t label_cell_val_num,
    shared_ptr<ArraySchema> schema,
    bool is_external,
    bool relative_uri)
    : dim_id_(dim_id)
    , dim_label_name_(dim_label_name)
    , uri_(uri)
    , label_attr_name_(label_attr_name)
    , label_order_(label_order)
    , label_type_(label_type)
    , label_cell_val_num_(label_cell_val_num)
    , schema_(schema)
    , is_external_(is_external)
    , relative_uri_(relative_uri) {
  if (dim_label_name.size() == 0) {
    throw std::invalid_argument(
        "Cannot create dimension label reference; Cannot set the dimension "
        "label name to an empty string.");
  }
  if (uri.to_string().size() == 0) {
    throw std::invalid_argument(
        "Cannot create dimension label reference; Cannot set the URI to an "
        "empty string.");
  }
  if (label_attr_name.size() == 0) {
    throw std::invalid_argument(
        "Cannot create dimension label reference; Cannot set the label "
        "attribute name to an empty string.");
  }

  // Check label type.
  ensure_dimension_datatype_is_valid(label_type);
  if (label_type == Datatype::STRING_ASCII) {
    if (label_cell_val_num != constants::var_num) {
      throw std::invalid_argument(
          "Cannot create dimension label reference; Invalid number of values "
          "per coordinate for the string dimension "
          "label.");
    }
  } else {
    if (label_cell_val_num != 1) {
      throw std::invalid_argument(
          "Cannot create dimension label reference; Invalid number of values "
          "per coordiante; Currently only one value per coordinate is "
          "supported for non-string dimension labels.");
    }
  }

  // Check the label order is valid.
  if (label_order == DataOrder::UNORDERED_DATA) {
    throw std::invalid_argument(
        "Cannot create dimension label reference; Unordered dimension labels "
        "are not yet supported.");
  }

  // Check URI is relative if it is internal to the array.
  if (!is_external_ && !relative_uri_) {
    throw std::invalid_argument(
        "Cannot create dimension label reference; Dimension labels stored by "
        "the array must have a relative URI.");
  }
}

DimensionLabel::DimensionLabel(
    dimension_size_type dim_id,
    const std::string& dim_label_name,
    const URI& uri,
    const Dimension* dim,
    DataOrder label_order,
    Datatype label_type,
    shared_ptr<MemoryTracker> memory_tracker)
    : dim_id_(dim_id)
    , dim_label_name_(dim_label_name)
    , uri_(uri)
    , label_attr_name_("label")
    , label_order_(label_order)
    , label_type_(label_type)
    , label_cell_val_num_(
          label_type == Datatype::STRING_ASCII ? constants::var_num : 1)
    , schema_(make_shared<ArraySchema>(
          HERE(),
          (label_order == DataOrder::UNORDERED_DATA ? ArrayType::SPARSE :
                                                      ArrayType::DENSE),
          memory_tracker))
    , is_external_(false)
    , relative_uri_(true) {
  auto index_type{dim->type()};
  if (!(datatype_is_integer(index_type) || datatype_is_datetime(index_type) ||
        datatype_is_time(index_type))) {
    throw std::invalid_argument(
        "Failed to create dimension label schema; Currently labels are not "
        "support on dimensions with datatype Datatype::" +
        datatype_str(index_type));
  }

  // Check the label data type is valid.
  try {
    ensure_dimension_datatype_is_valid(label_type);
  } catch (...) {
    std::throw_with_nested(std::invalid_argument(
        "Datatype Datatype::" + datatype_str(label_type) +
        " is not a valid dimension datatype."));
  }

  // Check the label order is valid.
  if (label_order == DataOrder::UNORDERED_DATA) {
    throw std::invalid_argument(
        "Unordered dimension labels are not yet supported.");
  }

  // Create and set dimension label domain.
  std::vector<shared_ptr<Dimension>> index_dims{
      make_shared<Dimension>(HERE(), "index", index_type, memory_tracker)};
  throw_if_not_ok(index_dims.back()->set_domain(dim->domain().data()));
  throw_if_not_ok(
      index_dims.back()->set_tile_extent(dim->tile_extent().data()));
  throw_if_not_ok(schema_->set_domain(make_shared<Domain>(
      HERE(),
      Layout::ROW_MAJOR,
      index_dims,
      Layout::ROW_MAJOR,
      memory_tracker)));

  // Create and set dimension label attribute.
  auto label_attr = make_shared<Attribute>(
      HERE(), "label", label_type, label_cell_val_num_, label_order);
  throw_if_not_ok(schema_->add_attribute(label_attr));

  // Check the array schema is valid.
  schema_->check_without_config();
}

// FORMAT:
//| Field                      | Type       |
//| -------------------------- | ---------- |
//| Dimension ID               | `uint32_t` |
//| Label order                | `uint8_t`  |
//| Dimension label name length| `uint64_t` |
//| Dimension label name       | `char []`  |
//| Relative URI               | `bool`     |
//| URI length                 | `uint64_t` |
//| URI                        | `char []`  |
//| Label attribute name length| `uint32_t` |
//| Label attribute name       | `char []`  |
//| Label datatype             | `uint8_t`  |
//| Label cell_val_num         | `uint32_t` |
//| Is external                | `bool`     |
shared_ptr<DimensionLabel> DimensionLabel::deserialize(
    Deserializer& deserializer, uint32_t) {
  try {
    // Read dimension ID
    dimension_size_type dim_id = deserializer.read<uint32_t>();

    // Read dimension label name
    uint32_t dim_label_name_size = deserializer.read<uint32_t>();
    std::string dim_label_name(
        deserializer.get_ptr<char>(dim_label_name_size), dim_label_name_size);

    // Read dimension label URI
    auto relative_uri = deserializer.read<bool>();
    auto uri_size = deserializer.read<uint64_t>();
    std::string uri(deserializer.get_ptr<char>(uri_size), uri_size);

    // Read dimension label name
    uint32_t label_attr_name_size = deserializer.read<uint32_t>();
    std::string label_attr_name(
        deserializer.get_ptr<char>(label_attr_name_size), label_attr_name_size);

    // Read label order
    auto label_order = data_order_from_int(deserializer.read<uint8_t>());

    // Read label datatype
    auto label_type = static_cast<Datatype>(deserializer.read<uint8_t>());

    // Read label cell value number
    auto label_cell_val_num = deserializer.read<uint32_t>();

    // Read if dimension label is external
    auto is_external = deserializer.read<bool>();

    // Construct and return a shared pointer to a DimensionLabel
    return make_shared<DimensionLabel>(
        HERE(),
        dim_id,
        dim_label_name,
        URI(uri, !relative_uri),
        label_attr_name,
        label_order,
        label_type,
        label_cell_val_num,
        nullptr,
        is_external,
        relative_uri);
  } catch (std::exception& e) {
    std::throw_with_nested(
        std::runtime_error("[DimensionLabel::deserialize] "));
  }
}

const shared_ptr<ArraySchema> DimensionLabel::schema() const {
  if (!schema_) {
    throw StatusException(
        "DimensionLabel",
        "Cannot return dimension label schema; No schema is set.");
  }
  return schema_;
}

// FORMAT:
//| Field                      | Type       |
//| -------------------------- | ---------- |
//| Dimension ID               | `uint32_t` |
//| Label order                | `uint8_t`  |
//| Dimension label name length| `uint32_t` |
//| Dimension label name       | `char []`  |
//| Relative URI               | `bool`     |
//| URI length                 | `uint64_t` |
//| URI                        | `char []`  |
//| Label attribute name length| `uint32_t` |
//| Label attribute name       | `char []`  |
//| Label datatype             | `uint8_t`  |
//| Label cell_val_num         | `uint32_t` |
//| Is external                | `bool`     |
void DimensionLabel::serialize(Serializer& serializer, uint32_t) const {
  // Read dimension ID
  serializer.write<uint32_t>(dim_id_);

  // Read dimension label name
  auto dim_label_name_size = static_cast<uint32_t>(dim_label_name_.size());
  serializer.write<uint32_t>(dim_label_name_size);
  serializer.write(dim_label_name_.c_str(), dim_label_name_size);

  // Read dimension label URI
  serializer.write<bool>(relative_uri_);
  uint64_t uri_size = (uri_.to_string().size());
  serializer.write<uint64_t>(uri_size);
  serializer.write(uri_.c_str(), uri_size);

  // Read dimension label name
  auto label_attr_name_size = static_cast<uint32_t>(label_attr_name_.size());
  serializer.write<uint32_t>(label_attr_name_size);
  serializer.write(label_attr_name_.c_str(), label_attr_name_size);

  // Read label order
  serializer.write<uint8_t>(static_cast<uint8_t>(label_order_));

  // Read label datatype
  auto label_type_int = static_cast<uint8_t>(label_type_);
  serializer.write<uint8_t>(label_type_int);

  // Read label cell value number
  serializer.write<uint32_t>(label_cell_val_num_);

  // Write if the dimension label is external
  serializer.write<uint8_t>(static_cast<uint8_t>(is_external_));
}

}  // namespace tiledb::sm

std::ostream& operator<<(
    std::ostream& os, const tiledb::sm::DimensionLabel& dl) {
  os << "### Dimension Label ###\n";
  os << "- Dimension Index: " << dl.dimension_index() << std::endl;
  os << "- Dimension Label Name: " << dl.name() << std::endl;
  os << "- URI: " << dl.uri().to_string() << std::endl;
  os << "- Label Attribute Name: " << dl.name() << std::endl;
  os << "- Label Type: " << datatype_str(dl.label_type()) << std::endl;
  (dl.label_cell_val_num() == tiledb::sm::constants::var_num) ?
      os << "- Label cell val num: var\n" :
      os << "- Label cell val num: " << dl.label_cell_val_num() << std::endl;

  return os;
}
