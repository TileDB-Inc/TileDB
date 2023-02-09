/**
 * @file   dimension_label.h
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
 * This file declares the C++ API for the TileDB Dimension Label object.
 */

#ifndef TILEDB_CPP_API_DIMENSION_LABEL_H
#define TILEDB_CPP_API_DIMENSION_LABEL_H

#include "context.h"
#include "deleter.h"
#include "exception.h"
#include "object.h"
#include "tiledb.h"
#include "type.h"

#include <array>
#include <functional>
#include <memory>
#include <sstream>
#include <type_traits>

namespace tiledb {

/**
 * Describes a dimension label of an Array.
 *
 * @details
 * A dimension label specifies the details of supplementary data that can be
 * used to query an array in place of one of the dimensions. This class provides
 * read-only properties of the dimension label that can be accessed after schema
 * creation.
 */
class DimensionLabel {
 public:
  DimensionLabel(const Context& ctx, tiledb_dimension_label_t* dim_label)
      : ctx_{ctx}
      , deleter_{}
      , dim_label_{
            std::shared_ptr<tiledb_dimension_label_t>(dim_label, deleter_)} {
  }

  DimensionLabel(const DimensionLabel&) = default;
  DimensionLabel(DimensionLabel&&) = default;
  DimensionLabel& operator=(const DimensionLabel&) = default;
  DimensionLabel& operator=(DimensionLabel&&) = default;

  /** Returns the index of the dimension the labels are applied to. */
  uint32_t dimension_index() const {
    auto& ctx = ctx_.get();
    uint32_t dim_index;
    ctx.handle_error(tiledb_dimension_label_get_dimension_index(
        ctx.ptr().get(), dim_label_.get(), &dim_index));
    return dim_index;
  }

  /** Returns the name of the attribute the label data is stored on. */
  std::string label_attr_name() const {
    auto& ctx = ctx_.get();
    const char* label_attr_name;
    ctx.handle_error(tiledb_dimension_label_get_label_attr_name(
        ctx.ptr().get(), dim_label_.get(), &label_attr_name));
    return label_attr_name;
  }

  /** Returns the number of values per cell in the labels. */
  uint32_t label_cell_val_num() const {
    auto& ctx = ctx_.get();
    uint32_t label_cell_val_num;
    ctx.handle_error(tiledb_dimension_label_get_label_cell_val_num(
        ctx.ptr().get(), dim_label_.get(), &label_cell_val_num));
    return label_cell_val_num;
  }

  /** Returns the data order of the labels. */
  tiledb_data_order_t label_order() const {
    auto& ctx = ctx_.get();
    tiledb_data_order_t label_order;
    ctx.handle_error(tiledb_dimension_label_get_label_order(
        ctx.ptr().get(), dim_label_.get(), &label_order));
    return label_order;
  }

  /** Returns the datatype of the labels. */
  tiledb_datatype_t label_type() const {
    auto& ctx = ctx_.get();
    tiledb_datatype_t label_type;
    ctx.handle_error(tiledb_dimension_label_get_label_type(
        ctx.ptr().get(), dim_label_.get(), &label_type));
    return label_type;
  }

  /** Returns he name of the dimension label. */
  std::string name() const {
    auto& ctx = ctx_.get();
    const char* name;
    ctx.handle_error(tiledb_dimension_label_get_name(
        ctx.ptr().get(), dim_label_.get(), &name));
    return name;
  }

  /** Returns the C TileDB dimension label object pointer. */
  std::shared_ptr<tiledb_dimension_label_t> ptr() const {
    return dim_label_;
  }

  /** Returns a string with the location of the dimension label array. */
  std::string uri() const {
    auto& ctx = ctx_.get();
    const char* uri;
    ctx.handle_error(tiledb_dimension_label_get_uri(
        ctx.ptr().get(), dim_label_.get(), &uri));
    return uri;
  }

 private:
  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** An auxiliary deleter. */
  impl::Deleter deleter_;

  /** The pointer to the C TileDB dimension label object. */
  std::shared_ptr<tiledb_dimension_label_t> dim_label_;
};

}  // namespace tiledb

#endif
