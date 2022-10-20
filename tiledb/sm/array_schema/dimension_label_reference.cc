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

/** Class for locally generated status exceptions. */
class DimensionLabelReferenceStatusException : public StatusException {
 public:
  explicit DimensionLabelReferenceStatusException(const std::string& msg)
      : StatusException("DimensionLabelReference", msg) {
  }
};

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
  if (name.size() == 0) {
    throw std::invalid_argument(
        "Cannot create dimension label reference; Cannot set name to an empty "
        "string.");
  }
  if (uri.to_string().size() == 0) {
    throw std::invalid_argument(
        "Cannot create dimension label reference; Cannot set the URI to an "
        "emptry string.");
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
    if (!label_domain.empty() && label_domain.var_size()) {
      throw std::invalid_argument(
          "Cannot create dimension label reference; The label domain for a "
          "dimension label with label type '" +
          datatype_str(label_type) + "must be variable.");
    }
  } else {
    if (label_cell_val_num != 1) {
      throw std::invalid_argument(
          "Cannot create dimension label reference; Invalid number of values "
          "per coordiante; Currently only one value per coordinate is "
          "supported for non-string dimension labels.");
    }
    if (!label_domain.empty()) {
      if (label_domain.var_size()) {
        throw std::invalid_argument(
            "Cannot create dimension label reference; The label domain for a "
            "dimension label with label type '" +
            datatype_str(label_type) + "cannot be variable.");
      }
      if (!label_domain.empty() &&
          label_domain.size() != 2 * datatype_size(label_type)) {
        throw std::invalid_argument(
            "Cannot create dimension label reference; The size of the label "
            "domain does not match the size of the datatype.");
      }
    }
    if (!is_external_ && !relative_uri_) {
      throw std::invalid_argument(
          "Cannot create dimension label reference; Dimension labels stored by "
          "the array must have a relative URI.");
    }
  }
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
//| Label order               | `uint8_t`  |
//| Name length               | `uint64_t` |
//| Name                      | `char []`  |
//| Relative URI              | `bool`     |
//| URI length                | `uint64_t` |
//| URI                       | `char []`  |
//| Label datatype            | `uint8_t`  |
//| Label cell_val_num        | `uint32_t` |
//| Label domain size         | `uint64_t` |
//| Label domain start size   | `uint64_t` |
//| Label domain data         | `uint8_t[]`|
//| Is external               | `bool`     |
shared_ptr<DimensionLabelReference> DimensionLabelReference::deserialize(
    Deserializer& deserializer, uint32_t) {
  try {
    // Read dimension ID
    dimension_size_type dim_id = deserializer.read<uint32_t>();

    // Read dimension label name
    uint64_t name_size = deserializer.read<uint64_t>();
    std::string name(deserializer.get_ptr<char>(name_size), name_size);

    // Read dimension label URI
    auto relative_uri = deserializer.read<bool>();
    auto uri_size = deserializer.read<uint64_t>();
    std::string uri(deserializer.get_ptr<char>(uri_size), uri_size);

    // Read label order
    auto label_order = label_order_from_int(deserializer.read<uint8_t>());

    // Read label datatype
    auto label_type = static_cast<Datatype>(deserializer.read<uint8_t>());

    // Read label cell value number
    auto label_cell_val_num = deserializer.read<uint32_t>();

    // Read label domain
    Range label_domain;
    auto label_domain_size = deserializer.read<uint64_t>();
    auto label_domain_start_size = deserializer.read<uint64_t>();
    if (label_domain_size != 0) {
      if (label_domain_start_size == 0) {
        label_domain = Range(
            deserializer.get_ptr<uint8_t>(label_domain_size),
            label_domain_size);
      } else {
        auto label_domain_end_size =
            label_domain_size - label_domain_start_size;
        auto range_start =
            deserializer.get_ptr<uint8_t>(label_domain_start_size);
        auto range_end = deserializer.get_ptr<uint8_t>(label_domain_end_size);
        label_domain = Range(
            range_start,
            label_domain_start_size,
            range_end,
            label_domain_end_size);
      }
    }

    // Read if dimension label is external
    auto is_external = deserializer.read<bool>();

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
//| Label order               | `uint8_t`  |
//| Name length               | `uint64_t` |
//| Name                      | `char []`  |
//| Relative URI              | `bool`     |
//| URI length                | `uint64_t` |
//| URI                       | `char []`  |
//| Label datatype            | `uint8_t`  |
//| Label cell_val_num        | `uint32_t` |
//| Label domain size         | `uint64_t` |
//| Label domain start size   | `uint64_t` |
//| Label domain data         | `uint8_t[]`|
//| Is external               | `bool`     |
void DimensionLabelReference::serialize(
    Serializer& serializer, uint32_t) const {
  // Read dimension ID
  serializer.write<uint32_t>(dim_id_);

  // Read dimension label namea
  uint64_t name_size{name_.size()};
  serializer.write<uint64_t>(name_size);
  serializer.write(name_.c_str(), name_size);

  // Read dimension label URI
  serializer.write<bool>(relative_uri_);
  uint64_t uri_size = (uri_.to_string().size());
  serializer.write<uint64_t>(uri_size);
  serializer.write(uri_.c_str(), uri_size);

  // Read label order
  serializer.write<uint8_t>(static_cast<uint8_t>(label_order_));

  // Read label datatype
  auto label_type_int = static_cast<uint8_t>(label_type_);
  serializer.write<uint8_t>(label_type_int);

  // Read label cell value number
  serializer.write<uint32_t>(label_cell_val_num_);

  // Read label domain
  // Note: start_size is 0 for non-variable length ranges.
  serializer.write<uint64_t>(label_domain_.size());
  serializer.write<uint64_t>(label_domain_.start_size());
  serializer.write(label_domain_.data(), label_domain_.size());

  // Write if the dimension label is external
  serializer.write<uint8_t>(static_cast<uint8_t>(is_external_));
}

}  // namespace tiledb::sm
