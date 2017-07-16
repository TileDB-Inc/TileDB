/**
 * @file   metadata_schema.cc
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
 * This file implements class MetadataSchema.
 */

#include "metadata_schema.h"
#include <cassert>
#include <iostream>

/* ********************************* */
/*               MACROS              */
/* ********************************* */

namespace tiledb {

/* ********************************* */
/*    CONSTRUCTORS & DESTRUCTORS     */
/* ********************************* */

MetadataSchema::MetadataSchema() {
  array_schema_ = new ArraySchema();
  add_dimensions();
}

MetadataSchema::MetadataSchema(const ArraySchema* array_schema) {
  array_schema_ = new ArraySchema(array_schema);
}

MetadataSchema::MetadataSchema(const MetadataSchema* metadata_schema) {
  array_schema_ = new ArraySchema(metadata_schema->array_schema());
}

MetadataSchema::MetadataSchema(const char* metadata_name) {
  array_schema_ = new ArraySchema(metadata_name);
  array_schema_->set_array_type(ArrayType::SPARSE);
  add_dimensions();
}

MetadataSchema::~MetadataSchema() {
  if (array_schema_ != nullptr)
    delete array_schema_;
}

/* ********************************* */
/*              ACCESSORS            */
/* ********************************* */

const std::string& MetadataSchema::metadata_name() const {
  return array_schema_->array_name();
}

const ArraySchema* MetadataSchema::array_schema() const {
  return array_schema_;
}

const Attribute* MetadataSchema::attr(int id) const {
  return array_schema_->attr(id);
}

unsigned int MetadataSchema::attr_num() const {
  return array_schema_->attr_num();
}

uint64_t MetadataSchema::capacity() const {
  return array_schema_->capacity();
}

Layout MetadataSchema::cell_order() const {
  return array_schema_->cell_order();
}

Status MetadataSchema::check() const {
  return array_schema_->check();
}

void MetadataSchema::dump(FILE* out) const {
  array_schema_->dump(out);
}

Layout MetadataSchema::tile_order() const {
  return array_schema_->tile_order();
}

/* ********************************* */
/*              MUTATORS             */
/* ********************************* */

void MetadataSchema::add_attribute(const Attribute* attr) {
  array_schema_->add_attribute(attr);
}

Status MetadataSchema::load(const char* metadata_name) {
  if (array_schema_ != nullptr)
    delete array_schema_;

  array_schema_ = new ArraySchema();
  return array_schema_->load(
      metadata_name, Configurator::metadata_schema_filename());
}

void MetadataSchema::set_capacity(uint64_t capacity) {
  array_schema_->set_capacity(capacity);
}

void MetadataSchema::set_cell_order(Layout cell_order) {
  array_schema_->set_cell_order(cell_order);
}

void MetadataSchema::set_tile_order(Layout tile_order) {
  array_schema_->set_tile_order(tile_order);
}

Status MetadataSchema::store(const std::string& dir) {
  return array_schema_->store(dir, Configurator::metadata_schema_filename());
}

/* ********************************* */
/*         PRIVATE METHODS           */
/* ********************************* */

void MetadataSchema::add_dimensions() {
  // Create dimensions
  uint32_t domain[8] = {
      0, UINT32_MAX, 0, UINT32_MAX, 0, UINT32_MAX, 0, UINT32_MAX};
  Dimension* dim1 = new Dimension(
      Configurator::key_dim1_name(), Datatype::UINT32, &domain[0], nullptr);
  Dimension* dim2 = new Dimension(
      Configurator::key_dim2_name(), Datatype::UINT32, &domain[2], nullptr);
  Dimension* dim3 = new Dimension(
      Configurator::key_dim3_name(), Datatype::UINT32, &domain[4], nullptr);
  Dimension* dim4 = new Dimension(
      Configurator::key_dim4_name(), Datatype::UINT32, &domain[6], nullptr);

  // Add dimensions to schema
  array_schema_->add_dimension(dim1);
  array_schema_->add_dimension(dim2);
  array_schema_->add_dimension(dim3);
  array_schema_->add_dimension(dim4);

  // Clean up
  delete dim1;
  delete dim2;
  delete dim3;
  delete dim4;
}

}  // namespace tiledb
