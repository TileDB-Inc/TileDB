/**
 * @file   query_experimental.h
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
 * This file declares the C++ experimental API for the query.
 */

#ifndef TILEDB_CPP_API_QUERY_EXPERIMENTAL_H
#define TILEDB_CPP_API_QUERY_EXPERIMENTAL_H

#include "array_schema_experimental.h"
#include "context.h"
#include "dimension_label_experimental.h"
#include "query_channel.h"
#include "tiledb.h"

namespace tiledb {
class QueryExperimental {
 public:
  /**
   * Sets the update value.
   *
   * Note that more than one update value may be set on a query.
   *
   * @param ctx TileDB context.
   * @param query Query object.
   * @param field_name The attribute name.
   * @param update_value The value to set.
   * @param update_value_size The byte size of `update_value`.
   * @return Reference to this Query
   */
  static void add_update_value_to_query(
      const Context& ctx,
      Query& query,
      const char* field_name,
      const void* update_value,
      uint64_t update_value_size) {
    ctx.handle_error(tiledb_query_add_update_value(
        ctx.ptr().get(),
        query.ptr().get(),
        field_name,
        update_value,
        update_value_size));
  }

  /**
   * Get the number of relevant fragments from the subarray. Should only be
   * called after size estimation was asked for.
   *
   * @param ctx TileDB context.
   * @param query Query object.
   * @return Number of relevant fragments.
   */
  static uint64_t get_relevant_fragment_num(Context& ctx, const Query& query) {
    uint64_t relevant_fragment_num = 0;
    ctx.handle_error(tiledb_query_get_relevant_fragment_num(
        ctx.ptr().get(), query.ptr().get(), &relevant_fragment_num));
    return relevant_fragment_num;
  }

  /**
   * Sets the data for a fixed/var-sized attribute/dimension.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
   * std::vector<int> data_a1 = {0, 1, 2, 3};
   * Query query(ctx, array);
   * query.set_data_buffer("a1", data_a1);
   * @endcode
   *
   * @tparam T Attribute/Dimension value type
   * @param name Attribute/Dimension name
   * @param buf Buffer vector with elements of the attribute/dimension type.
   **/
  template <typename T>
  static Query& set_data_buffer(
      Query& query, const std::string& name, std::vector<T>& buf) {
    // Checks
    if (name == "__coords") {
      impl::type_check<T>(query.schema_.domain().type());
    } else if (query.schema_.has_attribute(name)) {
      impl::type_check<T>(query.schema_.attribute(name).type());
    } else if (query.schema_.domain().has_dimension(name)) {
      impl::type_check<T>(query.schema_.domain().dimension(name).type());
    } else if (ArraySchemaExperimental::has_dimension_label(
                   query.ctx_, query.schema_, name)) {
      impl::type_check<T>(ArraySchemaExperimental::dimension_label(
                              query.ctx_, query.schema_, name)
                              .label_type());
    } else {
      throw TileDBError(
          std::string("Cannot set buffer; No attribute, dimension, or "
                      "dimension label named '") +
          name + "' exists");
    }

    return query.set_data_buffer(name, buf.data(), buf.size(), sizeof(T));
  }

  /**
   * Sets the data for a fixed/var-sized attribute/dimension.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
   * int data_a1[] = {0, 1, 2, 3};
   * Query query(ctx, array);
   * QueryExperimental::set_data_buffer(query, "a1", data_a1, 4);
   * @endcode
   *
   * @note set_data_buffer(std::string, std::vector) is preferred as it is
   * safer.
   *
   * @tparam T Value type of the buffer
   * @param name Name of the attribute, dimension, or dimension label.
   * @param buff Buffer array pointer with elements of the
   *     attribute/dimension type.
   * @param nelements Number of array elements.
   */
  template <typename T>
  static Query& set_data_buffer(
      Query& query, const std::string& name, T* buff, uint64_t nelements) {
    // Checks
    if (name == "__coords") {
      impl::type_check<T>(query.schema_.domain().type());
    } else if (query.schema_.has_attribute(name)) {
      impl::type_check<T>(query.schema_.attribute(name).type());
    } else if (query.schema_.domain().has_dimension(name)) {
      impl::type_check<T>(query.schema_.domain().dimension(name).type());
    } else if (ArraySchemaExperimental::has_dimension_label(
                   query.ctx_, query.schema_, name)) {
      impl::type_check<T>(ArraySchemaExperimental::dimension_label(
                              query.ctx_, query.schema_, name)
                              .label_type());
    } else {
      throw TileDBError(
          std::string("Cannot set buffer; No attribute, dimension, or "
                      "dimension label named '") +
          name + "' exists");
    }

    return query.set_data_buffer(name, buff, nelements, sizeof(T));
  }

  /**
   * Sets the data for a fixed/var-sized attribute/dimension.
   *
   * @note This unsafe version does not perform type checking; the given buffer
   * is assumed to be the correct type, and the size of an element in the given
   * buffer is assumed to be the size of the datatype of the attribute.
   *
   * @param name Name of the attribute, dimension, or dimension label
   * @param buff Buffer array pointer with elements of the attribute type.
   * @param nelements Number of array elements in buffer
   **/
  static Query& set_data_buffer(
      Query& query, const std::string& name, void* buff, uint64_t nelements) {
    // Compute element size (in bytes).
    size_t element_size = 0;
    if (name == "__coords") {
      element_size = tiledb_datatype_size(query.schema_.domain().type());
    } else if (query.schema_.has_attribute(name)) {
      element_size = tiledb_datatype_size(query.schema_.attribute(name).type());
    } else if (query.schema_.domain().has_dimension(name)) {
      element_size =
          tiledb_datatype_size(query.schema_.domain().dimension(name).type());
    } else if (ArraySchemaExperimental::has_dimension_label(
                   query.ctx_, query.schema_, name)) {
      element_size =
          tiledb_datatype_size(ArraySchemaExperimental::dimension_label(
                                   query.ctx_, query.schema_, name)
                                   .label_type());
    } else {
      throw TileDBError(
          std::string("Cannot set buffer; No attribute, dimension, or "
                      "dimension label named '") +
          name + "' exists");
    }

    // Set the data buffer.
    return query.set_data_buffer(name, buff, nelements, element_size);
  }

  /**
   * Sets the data for a fixed/var-sized attribute/dimension.
   *
   * @param name Name of the attribute, dimension, or dimension label.
   * @param data Pre-allocated string buffer.
   */
  static Query& set_data_buffer(
      Query& query, const std::string& name, std::string& data) {
    // Checks
    if (query.schema_.has_attribute(name)) {
      impl::type_check<char>(query.schema_.attribute(name).type());
    } else if (query.schema_.domain().has_dimension(name)) {
      impl::type_check<char>(query.schema_.domain().dimension(name).type());
    } else if (ArraySchemaExperimental::has_dimension_label(
                   query.ctx_, query.schema_, name)) {
      impl::type_check<char>(ArraySchemaExperimental::dimension_label(
                                 query.ctx_, query.schema_, name)
                                 .label_type());
    } else {
      throw TileDBError(
          std::string("Cannot set buffer; No attribute, dimension, or "
                      "dimension label named '") +
          name + "' exists");
    }

    // Set the data buffer.
    return query.set_data_buffer(name, &data[0], data.size(), sizeof(char));
  }

  /**
   * Returns the number of elements for dimension labels in the result buffers
   * from a read query. This is a map from the dimension label name to a pair of
   * values.
   *
   * The first is number of elements (offsets) for var size labels, and the
   * second is number of elements in the data buffer. For fixed sized labels,
   * the first is always 0.
   *
   * For variable sized labels the first value is the number of cells read, i.e.
   * the number of offsets read for the dimension label. The second value is the
   * total number of elements in the data buffer. For example, a read query on a
   * variable-length `float` dimension label that reads three cells would return
   * 3 for the first number in the pair. If the total amount of `floats` read
   * across the three cells was 10, then the second number in the pair would be
   * 10.
   *
   * For fixed-length labels, the first value is always 0. The second value is
   * the total number of elements in the data buffer. For example, a read query
   * on a single `float` dimension label that reads three cells would return 3
   * for the second value. A read query on a `float` dimension label with
   * cell_val_num 2 that reads three cells would return 3 * 2 = 6 for the second
   * value.
   *
   * If the query has not been submitted, an empty map is returned.
   */
  static std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
  result_buffer_elements_labels(const Query& query) {
    // Query hasn't been submitted
    if (query.buff_sizes_.empty()) {
      return {};
    }
    auto elements = query.result_buffer_elements();
    for (const auto& b_it : query.buff_sizes_) {
      auto attr_name = b_it.first;
      auto size_tuple = b_it.second;
      // Only update result elements for dimension labels.
      if (!ArraySchemaExperimental::has_dimension_label(
              query.ctx_, query.schema_, attr_name)) {
        continue;
      }
      auto var_label = ArraySchemaExperimental::dimension_label(
                           query.ctx_, query.schema_, attr_name)
                           .label_cell_val_num() == TILEDB_VAR_NUM;
      auto element_size = query.element_sizes_.find(attr_name)->second;
      elements[attr_name] = var_label ?
                                std::pair<uint64_t, uint64_t>(
                                    std::get<0>(size_tuple) / sizeof(uint64_t),
                                    std::get<1>(size_tuple) / element_size) :
                                std::pair<uint64_t, uint64_t>(
                                    0, std::get<1>(size_tuple) / element_size);
    }
    return elements;
  }

  /**
   * Returns the number of elements in the result buffers from a read query.
   * This is a map from the dimension label name to a tuple of values.
   *
   * The first is number of elements (offsets) for var size labels, and the
   * second is number of elements in the data buffer. For fixed sized labels,
   * the first is always 0. The third element is the size of the validity
   * bytemap buffer.
   *
   * For variable sized labels: the first value is the number of cells read,
   * i.e. the number of offsets read for the dimension label. The second value
   * is the total number of elements in the data buffer. For example, a read
   * query on a variable-length `float` dimension label that reads three cells
   * would return 3 for the first number in the pair. If the total amount of
   * `floats` read across the three cells was 10, then the second number in the
   * pair would be 10.
   *
   * For fixed-length labels, the first value is always 0. The second value is
   * the total number of elements in the data buffer. For example, a read query
   * on a single `float` dimension label that reads three cells would return 3
   * for the second value. A read query on a `float` dimension label with
   * cell_val_num 2 that reads three cells would return 3 * 2 = 6 for the second
   * value.
   *
   * If the query has not been submitted, an empty map is returned.
   */
  static std::
      unordered_map<std::string, std::tuple<uint64_t, uint64_t, uint64_t>>
      result_buffer_elements_nullable_labels(const Query& query) {
    // Query hasn't been submitted
    if (query.buff_sizes_.empty()) {
      return {};
    }
    auto elements = query.result_buffer_elements_nullable();
    for (const auto& b_it : query.buff_sizes_) {
      auto attr_name = b_it.first;
      auto size_tuple = b_it.second;
      // Only update result elements for dimension labels.
      if (!ArraySchemaExperimental::has_dimension_label(
              query.ctx_, query.schema_, attr_name)) {
        continue;
      }
      auto var_label = ArraySchemaExperimental::dimension_label(
                           query.ctx_, query.schema_, attr_name)
                           .label_cell_val_num() == TILEDB_VAR_NUM;
      auto element_size = query.element_sizes_.find(attr_name)->second;
      elements[attr_name] = var_label ?
                                std::tuple<uint64_t, uint64_t, uint64_t>(
                                    std::get<0>(size_tuple) / sizeof(uint64_t),
                                    std::get<1>(size_tuple) / element_size,
                                    std::get<2>(size_tuple) / sizeof(uint8_t)) :
                                std::tuple<uint64_t, uint64_t, uint64_t>(
                                    0,
                                    std::get<1>(size_tuple) / element_size,
                                    std::get<2>(size_tuple) / sizeof(uint8_t));
    }
    return elements;
  }

  /**
   * Get a `QueryChannel` instance that represents the default channel
   * of the query passed as argument
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * auto array = Array(ctx, uri, TILEDB_READ);
   * Query query(ctx, array);
   * QueryChannel default_channel =
   *    QueryExperimental::get_default_channel(query);
   * default_channel.apply_aggregate("Count", CountOperation{});
   *
   * uint64_t count = 0;
   * uint64_t size = 1;
   * query.set_data_buffer("Count", &count, size);
   * query.submit();
   * @endcode
   *
   * @param query Query object.
   * @return The default query channel.
   */
  static QueryChannel get_default_channel(const Query& query) {
    return QueryChannel::create_default_channel(query);
  }

  /**
   * Create an aggregate operation that operates on a single input field
   * and produces a single output
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * auto array = Array(ctx, uri, TILEDB_READ);
   * Query query(ctx, array);
   * Subarray subarray(ctx, array);
   * subarray.add_range("dim", 1, 5);
   * query.set_subarray(subarray);
   * QueryChannel default_channel =
   *    QueryExperimental::get_default_channel(query);
   * ChannelOperation operation =
   *    QueryExperimental::create_unary_aggregate<SumOperator>(query, "a");
   * default_channel.apply_aggregate("Sum", operation);
   *
   * double sum = 0;
   * uint64_t size = 1;
   * query.set_data_buffer("Sum", &sum, size);
   * query.submit();
   * @endcode
   *
   * @tparam Op The channel operator type
   * @param query Query object.
   * @param input_field The attribute name as input for the aggregate
   * @return The aggregate operation
   */
  template <
      class Op,
      std::enable_if_t<std::is_base_of_v<ChannelOperator, Op>, bool> = true>
  static ChannelOperation create_unary_aggregate(
      const Query& query, const std::string& input_field) {
    return ChannelOperation::create<Op>(query, input_field);
  }
};
}  // namespace tiledb

#endif  // TILEDB_CPP_API_QUERY_EXPERIMENTAL_H
