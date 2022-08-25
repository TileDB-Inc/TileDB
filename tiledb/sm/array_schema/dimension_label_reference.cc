/**
 * @file tiledb/sm/array_schema/dimension_label_reference.cc
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
 */

#include "tiledb/sm/array_schema/dimension_label_reference.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/label_order.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/type/range/range.h"

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::sm {

DimensionLabelReference::DimensionLabelReference(
    dimension_size_type dim_id,
    const std::string& name,
    const URI& uri,
    LabelOrder label_order,
    Datatype label_type,
    uint32_t label_cell_val_num,
    const Range& label_domain,
    shared_ptr<const DimensionLabelSchema> schema,
    bool is_external,
    bool relative_uri)
    : dim_id_(dim_id)
    , name_(name)
    , uri_(uri)
    , label_order_(label_order)
    , label_type_(label_type)
    , label_cell_val_num_(label_cell_val_num)
    , label_domain_(label_domain)
    , schema_(schema)
    , is_external_(is_external)
    , relative_uri_(relative_uri) {
  if (name.size() == 0)
    throw std::invalid_argument(
        "Invalid dimension label name; Cannot set name to an empty string.");
  if (uri.to_string().size() == 0)
    throw std::invalid_argument(
        "Invalid dimension label uri; Cannot set the URI to an emptry string.");
  ensure_dimension_datatype_is_valid(label_type);
  if (label_type == Datatype::STRING_ASCII) {
    if (label_cell_val_num != constants::var_num)
      throw std::invalid_argument(
          "Invalid number of values per coordinate for the string dimension "
          "label.");
    if (!label_domain.empty())
      throw std::invalid_argument(
          "Invalid domain; Setting the domain with type '" +
          datatype_str(label_type) + "' is not allowed.");
  } else {
    if (label_cell_val_num != 1)
      throw std::invalid_argument(
          "Invalid number of values per coordiante; Currently only one value "
          "per coordinate is supported for non-string dimension labels.");
    if (label_domain.var_size())
      throw std::invalid_argument(
          "Invalid domain; The label domain for a dimension label with label "
          "type '" +
          datatype_str(label_type) + "cannot be variable.");
    if (label_domain.size() != 2 * datatype_size(label_type))
      throw std::invalid_argument(
          "Invalid domain; The size of the label domain does not match the "
          "size of the datatype.");
  }
  if (!is_external_ && !relative_uri_)
    throw std::invalid_argument(
        "Cannot create dimension label reference; Dimension labels stored by "
        "the array must have a relative URI.");
}

DimensionLabelReference::DimensionLabelReference(
    dimension_size_type dim_id,
    const std::string& name,
    const URI& uri,
    LabelOrder label_order,
    Datatype label_type,
    uint32_t label_cell_val_num,
    const Range& label_domain,
    shared_ptr<const DimensionLabelSchema> schema)
    : DimensionLabelReference(
          dim_id,
          name,
          uri,
          label_order,
          label_type,
          label_cell_val_num,
          label_domain,
          schema,
          false,
          true) {
}

// FORMAT:
//| Field                     | Type       |
//| ------------------------- | ---------- |
//| Dimension ID              | `uint32_t` |
//| Label order               | `uint8_t` |
//| Label datatype            | `uint8_t`  |
//| Label cell_val_num        | `uint32_t` |
//| Is external               | `uint8_t`  |
//| Relative URI              | `uint8_t`  |
//| Label domain size         | `uint64_t` |
//| Name length               | `uint64_t` |
//| URI length                | `uint64_t` |
//| Label domain data         | `uint8_t[]`|
//| Name                      | `char []`  |
//| URI                       | `char []`  |
shared_ptr<DimensionLabelReference> DimensionLabelReference::deserialize(
    Deserializer& deserializer, uint32_t) {
  try {
    // Read imension ID
    dimension_size_type dim_id = deserializer.read<uint32_t>();
    // Read label order
    uint8_t label_order_int = deserializer.read<uint8_t>();
    auto label_order = label_order_from_int(label_order_int);
    // Read label datatype
    uint8_t label_type_int = deserializer.read<uint8_t>();
    auto label_type = static_cast<Datatype>(label_type_int);
    // Read label cell value number
    uint32_t label_cell_val_num = deserializer.read<uint32_t>();
    // Read if the label is external
    uint8_t is_external_int = deserializer.read<uint8_t>();
    auto is_external = static_cast<bool>(is_external_int);
    // Read if the label URI is relative
    uint8_t relative_uri_int = deserializer.read<uint8_t>();
    auto relative_uri = static_cast<bool>(relative_uri_int);
    // Read the length of the domain
    uint64_t label_domain_size = deserializer.read<uint64_t>();
    // Read the length of the name string
    uint64_t name_size = deserializer.read<uint64_t>();
    // Read the length of the name string
    uint64_t uri_size = deserializer.read<uint64_t>();
    // Read label name
    Range label_domain;
    if (label_domain_size != 0) {
      std::vector<uint8_t> tmp(label_domain_size);
      memcpy(
          tmp.data(),
          deserializer.get_ptr<uint8_t>(label_domain_size),
          label_domain_size);
      label_domain = Range(&tmp[0], label_domain_size);
    }
    // Read label name
    std::string name;
    name.resize(name_size);
    memcpy(name.data(), deserializer.get_ptr<uint8_t>(name_size), name_size);
    // Read label URI
    std::string uri;
    uri.resize(uri_size);
    memcpy(uri.data(), deserializer.get_ptr<uint8_t>(uri_size), uri_size);
    // Construct and return a shared pointer to a DimensionLabelReference
    return make_shared<DimensionLabelReference>(
        HERE(),
        dim_id,
        name,
        URI(uri, !relative_uri),
        label_order,
        label_type,
        label_cell_val_num,
        label_domain,
        nullptr,
        is_external,
        relative_uri);
  } catch (std::exception& e) {
    std::throw_with_nested(
        std::runtime_error("[DimensionLabelReference::deserialize] "));
  }
}

void DimensionLabelReference::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;
  fprintf(out, "### Dimension Label ###\n");
  fprintf(out, "- Dimension Index: %i\n", dim_id_);
  fprintf(out, "- Name: %s\n", name_.c_str());
  fprintf(out, "- URI: %s\n", uri_.c_str());
  fprintf(out, "- Label Type: %s\n", datatype_str(label_type_).c_str());
  (label_cell_val_num_ == constants::var_num) ?
      fprintf(out, "- Label cell val num: var\n") :
      fprintf(out, "- Label cell val num: %u\n", label_cell_val_num_);
  fprintf(
      out,
      "- Label domain: %s\n",
      range_str(label_domain_, label_type_).c_str());
  fprintf(out, "\n");
}

// FORMAT:
//| Field                     | Type       |
//| ------------------------- | ---------- |
//| Dimension ID              | `uint32_t` |
//| Label order               | `uint8_t` |
//| Label datatype            | `uint8_t`  |
//| Label cell_val_num        | `uint32_t` |
//| Is external               | `uint8_t`  |
//| Relative URI              | `uint8_t`  |
//| Label domain size         | `uint64_t` |
//| Name length               | `uint64_t` |
//| URI length                | `uint64_t` |
//| Label domain data         | `uint8_t[]`|
//| Name                      | `char []`  |
//| URI                       | `char []`  |
void DimensionLabelReference::serialize(
    Serializer& serializer, uint32_t) const {
  serializer.write<uint32_t>(dim_id_);
  auto label_order_int = static_cast<uint8_t>(label_order_);
  serializer.write<uint8_t>(label_order_int);
  auto label_type_int = static_cast<uint8_t>(label_type_);
  serializer.write<uint8_t>(label_type_int);
  serializer.write<uint32_t>(label_cell_val_num_);
  auto is_external_int = static_cast<uint8_t>(is_external_);
  serializer.write<uint8_t>(is_external_int);
  auto relative_uri_int = static_cast<uint8_t>(relative_uri_);
  serializer.write<uint8_t>(relative_uri_int);
  uint64_t label_domain_size =
      datatype_is_string(label_type_) ? 0 : 2 * datatype_size(label_type_);
  serializer.write<uint64_t>(label_domain_size);
  uint64_t name_size{name_.size()};
  serializer.write<uint64_t>(name_size);
  uint64_t uri_size{uri_.to_string().size()};
  serializer.write<uint64_t>(uri_size);
  serializer.write(label_domain_.data(), label_domain_size);
  serializer.write(name_.c_str(), name_size);
  serializer.write(uri_.c_str(), uri_size);
}

}  // namespace tiledb::sm
