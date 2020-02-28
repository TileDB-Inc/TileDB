/**
 * @file   attribute.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"

#include <cassert>

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Attribute::Attribute()
    : Attribute("", Datatype::CHAR) {
}

Attribute::Attribute(const std::string& name, Datatype type) {
  name_ = name;
  type_ = type;
  cell_val_num_ = (type == Datatype::ANY) ? constants::var_num : 1;
}

Attribute::Attribute(const Attribute* attr) {
  assert(attr != nullptr);
  name_ = attr->name();
  type_ = attr->type();
  cell_val_num_ = attr->cell_val_num();
  filters_ = attr->filters_;
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

// ===== FORMAT =====
// attribute_name_size (uint32_t)
// attribute_name (string)
// type (uint8_t)
// cell_val_num (uint32_t)
// filter_pipeline (see FilterPipeline::serialize)
Status Attribute::deserialize(ConstBuffer* buff) {
  // Load attribute name
  uint32_t attribute_name_size;
  RETURN_NOT_OK(buff->read(&attribute_name_size, sizeof(uint32_t)));
  name_.resize(attribute_name_size);
  RETURN_NOT_OK(buff->read(&name_[0], attribute_name_size));

  // Load type
  uint8_t type;
  RETURN_NOT_OK(buff->read(&type, sizeof(uint8_t)));
  type_ = (Datatype)type;

  // Load cell_val_num_
  RETURN_NOT_OK(buff->read(&cell_val_num_, sizeof(uint32_t)));

  // Load filter pipeline
  RETURN_NOT_OK(filters_.deserialize(buff));

  return Status::Ok();
}

void Attribute::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;
  // Dump
  fprintf(out, "### Attribute ###\n");
  fprintf(out, "- Name: %s\n", name_.c_str());
  fprintf(out, "- Type: %s\n", datatype_str(type_).c_str());
  if (!var_size())
    fprintf(out, "- Cell val num: %u\n", cell_val_num_);
  else
    fprintf(out, "- Cell val num: var\n");
  fprintf(out, "- Filters: %u", (unsigned)filters_.size());
  filters_.dump(out);
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
Status Attribute::serialize(Buffer* buff) {
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

  return Status::Ok();
}

Status Attribute::set_cell_val_num(unsigned int cell_val_num) {
  if (type_ == Datatype::ANY)
    return LOG_STATUS(Status::AttributeError(
        "Cannot set number of values per cell; Attribute datatype `ANY` is "
        "always variable-sized"));

  cell_val_num_ = cell_val_num;

  return Status::Ok();
}

Status Attribute::set_filter_pipeline(const FilterPipeline* pipeline) {
  if (pipeline == nullptr)
    return LOG_STATUS(Status::AttributeError(
        "Cannot set filter pipeline to attribute; Pipeline cannot be null"));

  for (unsigned i = 0; i < pipeline->size(); ++i) {
    if (datatype_is_real(type_) &&
        pipeline->get_filter(i)->type() == FilterType::FILTER_DOUBLE_DELTA)
      return LOG_STATUS(
          Status::AttributeError("Cannot set DOUBLE DELTA filter to a "
                                 "dimension with a real datatype"));
  }

  filters_ = *pipeline;

  return Status::Ok();
}

void Attribute::set_name(const std::string& name) {
  name_ = name;
}

Datatype Attribute::type() const {
  return type_;
}

bool Attribute::var_size() const {
  return cell_val_num_ == constants::var_num;
}

}  // namespace sm
}  // namespace tiledb
