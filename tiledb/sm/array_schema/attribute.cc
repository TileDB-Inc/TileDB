/**
 * @file   attribute.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"

#include <cassert>

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Attribute::Attribute() {
  name_ = "";
  type_ = Datatype::CHAR;
  cell_val_num_ = 1;
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

Compressor Attribute::compressor() const {
  auto compressor = filters_.get_filter<CompressionFilter>();
  return compressor == nullptr ? Compressor::NO_COMPRESSION :
                                 compressor->compressor();
}

int Attribute::compression_level() const {
  auto compressor = filters_.get_filter<CompressionFilter>();
  return compressor == nullptr ? -1 : compressor->compression_level();
}

// ===== FORMAT =====
// attribute_name_size (unsigned int)
// attribute_name (string)
// type (char)
// cell_val_num (unsigned int)
// filter_pipeline (see FilterPipeline::serialize)
Status Attribute::deserialize(ConstBuffer* buff) {
  // Load attribute name
  unsigned int attribute_name_size;
  RETURN_NOT_OK(buff->read(&attribute_name_size, sizeof(unsigned int)));
  name_.resize(attribute_name_size);
  RETURN_NOT_OK(buff->read(&name_[0], attribute_name_size));

  // Load type
  char type;
  RETURN_NOT_OK(buff->read(&type, sizeof(char)));
  type_ = (Datatype)type;

  // Load cell_val_num_
  RETURN_NOT_OK(buff->read(&cell_val_num_, sizeof(unsigned int)));

  // Load filter pipeline
  RETURN_NOT_OK(filters_.deserialize(buff));

  return Status::Ok();
}

void Attribute::dump(FILE* out) const {
  // Dump
  fprintf(out, "### Attribute ###\n");
  fprintf(out, "- Name: %s\n", is_anonymous() ? "<anonymous>" : name_.c_str());
  fprintf(out, "- Type: %s\n", datatype_str(type_).c_str());
  fprintf(out, "- Compressor: %s\n", compressor_str(compressor()).c_str());
  fprintf(out, "- Compression level: %d\n", compression_level());

  if (!var_size())
    fprintf(out, "- Cell val num: %u\n", cell_val_num_);
  else
    fprintf(out, "- Cell val num: var\n");
}

const FilterPipeline* Attribute::filters() const {
  return &filters_;
}

const std::string& Attribute::name() const {
  return name_;
}

bool Attribute::is_anonymous() const {
  return name_.empty() ||
         utils::parse::starts_with(name_, constants::default_attr_name);
}

// ===== FORMAT =====
// attribute_name_size (unsigned int)
// attribute_name (string)
// type (char)
// cell_val_num (unsigned int)
// filter_pipeline (see FilterPipeline::serialize)
Status Attribute::serialize(Buffer* buff) {
  // Write attribute name
  auto attribute_name_size = (unsigned int)name_.size();
  RETURN_NOT_OK(buff->write(&attribute_name_size, sizeof(unsigned int)));
  RETURN_NOT_OK(buff->write(name_.c_str(), attribute_name_size));

  // Write type
  auto type = (char)type_;
  RETURN_NOT_OK(buff->write(&type, sizeof(char)));

  // Write cell_val_num_
  RETURN_NOT_OK(buff->write(&cell_val_num_, sizeof(unsigned int)));

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

void Attribute::set_compressor(Compressor compressor) {
  auto filter = filters_.get_filter<CompressionFilter>();
  if (filter == nullptr)
    filters_.add_filter(CompressionFilter(compressor, -1));
  else
    filter->set_compressor(compressor);
}

void Attribute::set_compression_level(int compression_level) {
  auto filter = filters_.get_filter<CompressionFilter>();
  if (filter == nullptr)
    filters_.add_filter(
        CompressionFilter(Compressor::NO_COMPRESSION, compression_level));
  else
    filter->set_compression_level(compression_level);
}

Status Attribute::set_filter_pipeline(const FilterPipeline* pipeline) {
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
