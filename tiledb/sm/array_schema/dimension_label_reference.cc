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
    bool is_external,
    bool relative_uri)
    : dim_id_(dim_id)
    , name_(name)
    , uri_(uri)
    , label_order_(label_order)
    , label_type_(label_type)
    , label_cell_val_num_(label_cell_val_num)
    , label_domain_(label_domain)
    , schema_(nullptr)
    , is_external_(is_external)
    , relative_uri_(relative_uri) {
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
    : dim_id_(dim_id)
    , name_(name)
    , uri_(uri)
    , label_order_(label_order)
    , label_type_(label_type)
    , label_cell_val_num_(label_cell_val_num)
    , label_domain_(label_domain)
    , schema_(schema)
    , is_external_(false)
    , relative_uri_(true) {
}

// FORMAT:
//| Field                     | Type       |
//| ------------------------- | ---------- |
//| Dimension ID              | `uint32_t` |
//| Label order               | `uint32_t` |
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
    ConstBuffer* buff, uint32_t) {
  // Read dimension ID
  dimension_size_type dim_id;
  auto st = buff->read(&dim_id, sizeof(dim_id));
  if (!st.ok())
    throw std::runtime_error(
        "[DimensionLabelReference::deserialize] Failed to load the dimension "
        "index for the dimension label.");
  // Read label order
  uint8_t label_order_int;
  st = buff->read(&label_order_int, sizeof(label_order_int));
  if (!st.ok())
    throw std::runtime_error(
        "[DimensionLabelReference::deserialize] Failed to load the dimension "
        "label order.");
  // Read label datatype
  uint8_t label_type_int;
  st = buff->read(&label_type_int, sizeof(label_type_int));
  if (!st.ok())
    throw std::runtime_error(
        "[DimensionLabelReference::deserialize] Failed to load the dimension "
        "label datatype.");
  // Read label cell value number
  uint32_t label_cell_val_num;
  st = buff->read(&label_cell_val_num, sizeof(label_cell_val_num));
  if (!st.ok())
    throw std::runtime_error(
        "[DimensionLabelReference::deserialize] Failed to load the dimension "
        "label cell value number.");
  // Read if the label is external.
  bool is_external;
  st = buff->read(&is_external, sizeof(is_external));
  if (!st.ok())
    throw std::runtime_error(
        "[DimensionLabelReference::deserialize] Failed to load if the "
        "dimension label is external to the array.");
  // Read if the label URI is relative
  bool relative_uri;
  st = buff->read(&relative_uri, sizeof(relative_uri));
  if (!st.ok())
    throw std::runtime_error(
        "[DimensionLabelReference::deserialize] Failed to load if the "
        "dimension label URI is relative.");
  // Read the length of the domain
  uint64_t label_domain_size;
  st = buff->read(&label_domain_size, sizeof(label_domain_size));
  if (!st.ok())
    throw std::runtime_error(
        "[DimensionLabelReference::deserialize] Failed to load domain size of "
        "the dimension label domain.");
  // Read the length of the name string
  uint64_t name_size;
  st = buff->read(&name_size, sizeof(name_size));
  if (!st.ok())
    throw std::runtime_error(
        "[DimensionLabelReference::deserialize] Failed to load the size of "
        "dimension label name.");
  // Read the length of the name string
  uint64_t uri_size;
  st = buff->read(&uri_size, sizeof(uri_size));
  if (!st.ok())
    throw std::runtime_error(
        "[DimensionLabelReference::deserialize] Failed to load the size of "
        "dimension label URI.");
  // Read label name
  Range label_domain;
  if (label_domain_size != 0) {
    std::vector<uint8_t> tmp(label_domain_size);
    st = buff->read(&tmp[0], label_domain_size);
    label_domain = Range(&tmp[0], label_domain_size);
    if (!st.ok())
      throw std::runtime_error(
          "[DimensionLabelReference::deserialize] Failed to load dimension "
          "label domain.");
  }
  // Read label name
  std::string name;
  name.resize(name_size);
  st = buff->read(&name[0], name_size);
  if (!st.ok())
    throw std::runtime_error(
        "[DimensionLabelReference::deserialize] Failed to load dimension label "
        "name.");
  // Read label URI
  std::string uri;
  uri.resize(uri_size);
  st = buff->read(&uri[0], uri_size);
  if (!st.ok())
    throw std::runtime_error(
        "[DimensionLabelReference::deserialize] Failed to load dimension "
        "label URI.");
  return make_shared<DimensionLabelReference>(
      HERE(),
      dim_id,
      name,
      URI(uri, !relative_uri),
      static_cast<LabelOrder>(label_order_int),
      static_cast<Datatype>(label_type_int),
      label_cell_val_num,
      label_domain,
      is_external,
      relative_uri);
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
//| Label order               | `uint32_t` |
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
void DimensionLabelReference::serialize(Buffer* buff, uint32_t) const {
  throw_if_not_ok(buff->write(&dim_id_, sizeof(dimension_size_type)));
  uint8_t label_order_int{static_cast<uint8_t>(label_order_)};
  throw_if_not_ok(buff->write(&label_order_int, sizeof(label_order_int)));
  uint8_t label_type_int{static_cast<uint8_t>(label_type_)};
  throw_if_not_ok(buff->write(&label_type_int, sizeof(label_order_int)));
  throw_if_not_ok(buff->write(&label_cell_val_num_, sizeof(uint32_t)));
  throw_if_not_ok(buff->write(&is_external_, sizeof(is_external_)));
  throw_if_not_ok(buff->write(&relative_uri_, sizeof(relative_uri_)));
  uint64_t label_domain_size =
      datatype_is_string(label_type_) ? 0 : 2 * datatype_size(label_type_);
  throw_if_not_ok(buff->write(&label_domain_size, sizeof(label_domain_size)));
  uint64_t name_size{name_.size()};
  throw_if_not_ok(buff->write(&name_size, sizeof(name_size)));
  uint64_t uri_size{uri_.to_string().size()};
  throw_if_not_ok(buff->write(&uri_size, sizeof(uri_size)));
  throw_if_not_ok(buff->write(label_domain_.data(), label_domain_size));
  throw_if_not_ok(buff->write(name_.c_str(), name_size));
  throw_if_not_ok(buff->write(uri_.c_str(), uri_size));
}

}  // namespace tiledb::sm
