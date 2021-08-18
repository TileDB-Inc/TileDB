/**
 * @file   attribute_builder.h
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
 * This file defines the Attribute builder for interaction with the C API.
 */

#ifndef TILEDB_ATTRIBUTE_BUILDER_H
#define TILEDB_ATTRIBUTE_BUILDER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/misc/types.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class AttributeBuilder {
 public:
  typedef struct PrivateAttribute {
    std::string name_ = "";
    Datatype type_ = Datatype::CHAR;
    bool nullable_ = false;
    uint32_t cell_val_num_ = 1;
    FilterPipeline filters_ = FilterPipeline();
    uint64_t cell_size_ = 1;
    ByteVecValue fill_value_ = ByteVecValue();
    uint8_t fill_value_validity_ = 0;
    Attribute* attr_ = nullptr;
    bool built_ = false;
  } PrivateAttribute;

  // Create private attribute.
  PrivateAttribute private_attr_;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  AttributeBuilder()
      : private_attr_() {
  }

  AttributeBuilder(
      const std::string& name, const Datatype type, bool nullable = false)
      : private_attr_() {
    private_attr_.name_ = name;
    private_attr_.type_ = type;
    private_attr_.nullable_ = nullable;
    private_attr_.cell_val_num_ =
        (type == Datatype::ANY) ? constants::var_num : 1;
    set_default_fill_value();
  }

  AttributeBuilder(Attribute* attr)
      : private_attr_() {
    assert(attr != nullptr);
    private_attr_.name_ = attr->name();
    private_attr_.type_ = attr->type();
    private_attr_.cell_val_num_ = attr->cell_val_num();
    private_attr_.nullable_ = attr->nullable();
    private_attr_.filters_ = attr->filters();
    private_attr_.fill_value_ = attr->fill_value();
    private_attr_.fill_value_validity_ = attr->fill_value_validity();
  }

  ~AttributeBuilder() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  tdb_unique_ptr<Attribute> build() {
    private_attr_.attr_ = new (std::nothrow) Attribute(
        private_attr_.name_, private_attr_.type_, private_attr_.nullable_);
    private_attr_.built_ = true;
    tdb_unique_ptr<Attribute> attr(private_attr_.attr_);

    attr->set_cell_val_num(private_attr_.cell_val_num_);
    attr->set_filter_pipeline(&private_attr_.filters_);
    if (private_attr_.nullable_) {
      attr->set_fill_value(
          private_attr_.fill_value_.data(),
          (uint64_t)private_attr_.fill_value_.size(),
          private_attr_.fill_value_validity_);
    } else {
      attr->set_fill_value(
          private_attr_.fill_value_.data(),
          (uint64_t)private_attr_.fill_value_.size());
    }

    return attr;
  }

  bool built() {
    return private_attr_.built_;
  }

  uint64_t cell_size() const {
    if (var_size())
      return constants::var_size;

    return private_attr_.cell_val_num_ * datatype_size(private_attr_.type_);
  }

  unsigned int cell_val_num() const {
    return private_attr_.cell_val_num_;
  }

  void dump(FILE* out) const {
    if (out == nullptr)
      out = stdout;
    // Dump
    fprintf(out, "### Attribute ###\n");
    fprintf(out, "- Name: %s\n", private_attr_.name_.c_str());
    fprintf(out, "- Type: %s\n", datatype_str(private_attr_.type_).c_str());
    fprintf(
        out, "- Nullable: %s\n", (private_attr_.nullable_ ? "true" : "false"));
    if (!var_size())
      fprintf(out, "- Cell val num: %u\n", private_attr_.cell_val_num_);
    else
      fprintf(out, "- Cell val num: var\n");
    fprintf(out, "- Filters: %u", (unsigned)private_attr_.filters_.size());
    private_attr_.filters_.dump(out);
    fprintf(out, "\n");
    fprintf(out, "- Fill value: %s", fill_value_str().c_str());
    if (private_attr_.nullable_) {
      fprintf(out, "\n");
      fprintf(
          out, "- Fill value validity: %u", private_attr_.fill_value_validity_);
    }
    fprintf(out, "\n");
  }

  const FilterPipeline& filters() const {
    return private_attr_.filters_;
  }

  const std::string& name() const {
    return private_attr_.name_;
  }

  Status set_cell_val_num(unsigned int cell_val_num) {
    if (private_attr_.type_ == Datatype::ANY)
      return LOG_STATUS(Status::AttributeBuilderError(
          "Cannot set number of values per cell; Attribute datatype `ANY` is "
          "always variable-sized"));

    private_attr_.cell_val_num_ = cell_val_num;
    set_default_fill_value();

    return Status::Ok();
  }

  Status set_nullable(const bool nullable) {
    private_attr_.nullable_ = nullable;
    return Status::Ok();
  }

  Status get_nullable(bool* const nullable) {
    *nullable = private_attr_.nullable_;
    return Status::Ok();
  }

  Status set_filter_pipeline(const FilterPipeline* pipeline) {
    if (pipeline == nullptr)
      return LOG_STATUS(Status::AttributeBuilderError(
          "Cannot set filter pipeline to attribute; Pipeline cannot be null"));

    for (unsigned i = 0; i < pipeline->size(); ++i) {
      if (datatype_is_real(private_attr_.type_) &&
          pipeline->get_filter(i)->type() == FilterType::FILTER_DOUBLE_DELTA)
        return LOG_STATUS(
            Status::AttributeBuilderError("Cannot set DOUBLE DELTA filter to a "
                                          "dimension with a real datatype"));
    }

    private_attr_.filters_ = *pipeline;

    return Status::Ok();
  }

  void set_name(const std::string& name) {
    private_attr_.name_ = name;
  }

  Status set_fill_value(const void* value, uint64_t size) {
    if (value == nullptr) {
      return LOG_STATUS(Status::AttributeBuilderError(
          "Cannot set fill value; Input value cannot be null"));
    }

    if (size == 0) {
      return LOG_STATUS(Status::AttributeBuilderError(
          "Cannot set fill value; Input size cannot be 0"));
    }

    if (nullable()) {
      return LOG_STATUS(Status::AttributeBuilderError(
          "Cannot set fill value; Attribute is nullable"));
    }

    if (!var_size() && size != cell_size()) {
      return LOG_STATUS(Status::AttributeBuilderError(
          "Cannot set fill value; Input size is not the same as cell size"));
    }

    private_attr_.fill_value_.resize(size);
    private_attr_.fill_value_.shrink_to_fit();
    std::memcpy(private_attr_.fill_value_.data(), value, size);

    return Status::Ok();
  }

  Status get_fill_value(const void** value, uint64_t* size) const {
    if (value == nullptr) {
      return LOG_STATUS(Status::AttributeBuilderError(
          "Cannot get fill value; Input value cannot be null"));
    }

    if (size == nullptr) {
      return LOG_STATUS(Status::AttributeBuilderError(
          "Cannot get fill value; Input size cannot be null"));
    }

    if (nullable()) {
      return LOG_STATUS(Status::AttributeBuilderError(
          "Cannot get fill value; Attribute is nullable"));
    }

    *value = private_attr_.fill_value_.data();
    *size = (uint64_t)private_attr_.fill_value_.size();

    return Status::Ok();
  }

  Status set_fill_value(const void* value, uint64_t size, uint8_t valid) {
    if (value == nullptr) {
      return LOG_STATUS(Status::AttributeBuilderError(
          "Cannot set fill value; Input value cannot be null"));
    }

    if (size == 0) {
      return LOG_STATUS(Status::AttributeBuilderError(
          "Cannot set fill value; Input size cannot be 0"));
    }

    if (!nullable()) {
      return LOG_STATUS(Status::AttributeBuilderError(
          "Cannot set fill value; Attribute is not nullable"));
    }

    if (!var_size() && size != cell_size()) {
      return LOG_STATUS(Status::AttributeBuilderError(
          "Cannot set fill value; Input size is not the same as cell size"));
    }

    private_attr_.fill_value_.resize(size);
    private_attr_.fill_value_.shrink_to_fit();
    std::memcpy(private_attr_.fill_value_.data(), value, size);
    private_attr_.fill_value_validity_ = valid;

    return Status::Ok();
  }

  Status get_fill_value(
      const void** value, uint64_t* size, uint8_t* valid) const {
    if (value == nullptr) {
      return LOG_STATUS(Status::AttributeBuilderError(
          "Cannot get fill value; Input value cannot be null"));
    }

    if (size == nullptr) {
      return LOG_STATUS(Status::AttributeBuilderError(
          "Cannot get fill value; Input size cannot be null"));
    }

    if (!nullable()) {
      return LOG_STATUS(Status::AttributeBuilderError(
          "Cannot get fill value; Attribute is not nullable"));
    }

    *value = private_attr_.fill_value_.data();
    *size = (uint64_t)private_attr_.fill_value_.size();
    *valid = private_attr_.fill_value_validity_;

    return Status::Ok();
  }

  const ByteVecValue& fill_value() const {
    return private_attr_.fill_value_;
  }

  uint8_t fill_value_validity() const {
    return private_attr_.fill_value_validity_;
  }

  Datatype type() const {
    return private_attr_.type_;
  }

  bool var_size() const {
    return private_attr_.cell_val_num_ == constants::var_num;
  }

  bool nullable() const {
    return private_attr_.nullable_;
  }

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  void set_default_fill_value() {
    auto fill_value = constants::fill_value(private_attr_.type_);
    auto fill_size = datatype_size(private_attr_.type_);
    auto cell_num = (var_size()) ? 1 : private_attr_.cell_val_num_;

    private_attr_.fill_value_.resize(cell_num * fill_size);
    private_attr_.fill_value_.shrink_to_fit();
    uint64_t offset = 0;
    auto buff = (unsigned char*)private_attr_.fill_value_.data();
    for (uint64_t i = 0; i < cell_num; ++i) {
      std::memcpy(buff + offset, fill_value, fill_size);
      offset += fill_size;
    }

    private_attr_.fill_value_validity_ = 0;
  }

  std::string fill_value_str() const {
    std::string ret;
    auto v_size = datatype_size(private_attr_.type_);
    uint64_t num = private_attr_.fill_value_.size() / v_size;
    auto v = private_attr_.fill_value_.data();
    for (uint64_t i = 0; i < num; ++i) {
      ret += utils::parse::to_str(v, private_attr_.type_);
      v += v_size;
      if (i != num - 1)
        ret += ", ";
    }

    return ret;
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ATTRIBUTE_BUILDER_H