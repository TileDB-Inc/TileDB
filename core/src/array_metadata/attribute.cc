/**
 * @file   attribute.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include "attribute.h"
#include "const_buffer.h"
#include "utils.h"

#include <cassert>

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Attribute::Attribute() = default;

Attribute::Attribute(const char* name, Datatype type) {
  // Set name
  if (name != nullptr)
    name_ = name;

  // Set type
  type_ = type;

  // Set default compressor and compression level
  cell_val_num_ = 1;
  compressor_ = Compressor::NO_COMPRESSION;
  compression_level_ = -1;
}

Attribute::Attribute(const Attribute* attr) {
  assert(attr != nullptr);

  name_ = attr->name();
  type_ = attr->type();
  cell_val_num_ = attr->cell_val_num();
  compressor_ = attr->compressor();
  compression_level_ = attr->compression_level();
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
  return compressor_;
}

int Attribute::compression_level() const {
  return compression_level_;
}

// ===== FORMAT =====
// attribute_name_size (unsigned int)
// attribute_name (string)
// type (char)
// compressor (char)
// compression_level (int)
// cell_val_num (unsigned int)
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

  // Load compressor
  char compressor;
  RETURN_NOT_OK(buff->read(&compressor, sizeof(char)));
  compressor_ = (Compressor)compressor;
  RETURN_NOT_OK(buff->read(&compression_level_, sizeof(int)));

  // Load cell_val_num_
  RETURN_NOT_OK(buff->read(&cell_val_num_, sizeof(unsigned int)));

  return Status::Ok();
}

void Attribute::dump(FILE* out) const {
  // Retrieve type and compressor strings
  const char* type_s = datatype_str(type_);
  const char* compressor_s = compressor_str(compressor_);

  // Dump
  fprintf(out, "### Attribute ###\n");
  fprintf(out, "- Name: %s\n", is_anonymous() ? "<anonymous>" : name_.c_str());
  fprintf(out, "- Type: %s\n", type_s);
  fprintf(out, "- Compressor: %s\n", compressor_s);
  fprintf(out, "- Compression level: %d\n", compression_level_);

  if (!var_size())
    fprintf(out, "- Cell val num: %u\n", cell_val_num_);
  else
    fprintf(out, "- Cell val num: var\n");
}

const std::string& Attribute::name() const {
  return name_;
}

bool Attribute::is_anonymous() const {
  return name_.empty() ||
         utils::starts_with(name_, constants::default_attr_name);
}

// ===== FORMAT =====
// attribute_name_size (unsigned int)
// attribute_name (string)
// type (char)
// compressor (char)
// compression_level (int)
// cell_val_num (unsigned int)
Status Attribute::serialize(Buffer* buff) {
  // Write attribute name
  auto attribute_name_size = (unsigned int)name_.size();
  RETURN_NOT_OK(buff->write(&attribute_name_size, sizeof(unsigned int)));
  RETURN_NOT_OK(buff->write(name_.c_str(), attribute_name_size));

  // Write type
  auto type = (char)type_;
  RETURN_NOT_OK(buff->write(&type, sizeof(char)));

  // Write compressor
  auto compressor = (char)compressor_;
  RETURN_NOT_OK(buff->write(&compressor, sizeof(char)));
  RETURN_NOT_OK(buff->write(&compression_level_, sizeof(int)));

  // Write cell_val_num_
  RETURN_NOT_OK(buff->write(&cell_val_num_, sizeof(unsigned int)));

  return Status::Ok();
}

void Attribute::set_cell_val_num(unsigned int cell_val_num) {
  cell_val_num_ = cell_val_num;
}

void Attribute::set_compressor(Compressor compressor) {
  compressor_ = compressor;
}

void Attribute::set_compression_level(int compression_level) {
  compression_level_ = compression_level;
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

}  // namespace tiledb
