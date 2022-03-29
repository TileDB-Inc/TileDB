/**
 * @file   dimension_label.cc
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
 *
 * @section DESCRIPTION
 *
 * This file defines the DimensionLabel class.
 */

#include "dimension_label.h"

namespace tiledb::sm {

/************************************************/
/*               Dimension Label                */
/************************************************/

DimensionLabel::DimensionLabel(
    LabelType label_type,
    DimensionLabel::BaseSchema& schema,
    shared_ptr<DimensionLabelMapping> label_index_map)
    : label_type_(label_type)
    , schema_(schema)
    , label_index_map_(label_index_map) {
}

tuple<Status, shared_ptr<DimensionLabel>> DimensionLabel::create_uniform(
    DimensionLabel::BaseSchema&& schema) {
  if (schema.label_cell_val_num != 1 || schema.index_cell_val_num != 1)
    return {Status_DimensionLabelError(
                "Unable to create uniform dimension label; both label and "
                "index must have cell value of length 1"),
            nullptr};
  try {
    return {Status::Ok(),
            make_shared<DimensionLabel>(
                HERE(),
                LabelType::LABEL_UNIFORM,
                schema,
                create_uniform_mapping(
                    schema.label_datatype,
                    schema.label_domain,
                    schema.index_datatype,
                    schema.index_domain))};
  } catch (std::logic_error& err) {
    std::string msg{err.what()};
    return {Status_DimensionLabelError(
                "Unable to create uniform dimension label; " + msg),
            nullptr};
  }
}

tuple<Status, Range> DimensionLabel::index_range(const Range& labels) const {
  try {
    return {Status::Ok(), label_index_map_->index_range(labels)};
  } catch (std::logic_error& err) {
    std::string msg{err.what()};
    return {Status_DimensionLabelError(
                "Unable to get index range from label range; " + msg),
            Range()};
  }
}

/************************************************/
/*        Dimension Label Base Schema           */
/************************************************/

DimensionLabel::BaseSchema::BaseSchema(
    const std::string& name,
    Datatype label_datatype,
    uint32_t label_cell_val_num,
    const Range& label_domain,
    Datatype index_datatype,
    uint32_t index_cell_val_num,
    const Range& index_domain)
    : name(name)
    , label_datatype(label_datatype)
    , label_cell_val_num(label_cell_val_num)
    , label_domain(label_domain)
    , index_datatype(index_datatype)
    , index_cell_val_num(index_cell_val_num)
    , index_domain(index_domain) {
}

}  // namespace tiledb::sm
