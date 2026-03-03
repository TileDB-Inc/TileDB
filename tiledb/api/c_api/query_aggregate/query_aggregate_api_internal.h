/**
 * @file tiledb/api/c_api/query_aggregate/query_aggregate_api_internal.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file declares the internal query aggregates mechanics of the C API for
 * TileDB.
 */

#ifndef TILEDB_CAPI_QUERY_AGGREGATE_INTERNAL_H
#define TILEDB_CAPI_QUERY_AGGREGATE_INTERNAL_H

#include "tiledb/api/c_api_support/argument_validation.h"
#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/readers/aggregators/operation.h"
#include "tiledb/sm/query/readers/aggregators/query_channel.h"

struct tiledb_channel_operation_handle_t : public tiledb::api::CAPIHandle {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{
      "tiledb_channel_operation_t"};

 private:
  std::shared_ptr<tiledb::sm::Operation> operation_;

 public:
  /**
   * Default constructor doesn't make sense
   */
  tiledb_channel_operation_handle_t() = delete;

  /**
   * Ordinary constructor.
   * @param operation An internal operation object
   */
  explicit tiledb_channel_operation_handle_t(
      const shared_ptr<tiledb::sm::Operation>& operation)
      : operation_{operation} {
  }

  [[nodiscard]] inline shared_ptr<tiledb::sm::IAggregator> aggregator() const {
    return operation_->aggregator();
  }
};

/* Forward declaration */
namespace tiledb::sm {
class Query;
}

struct tiledb_query_channel_handle_t : public tiledb::api::CAPIHandle {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"tiledb_query_channel_t"};

 private:
  std::shared_ptr<class tiledb::sm::QueryChannel> channel_;

 public:
  /**
   * Default constructor doesn't make sense
   */
  tiledb_query_channel_handle_t() = delete;

  /**
   * Ordinary constructor.
   * @param query The query object that owns the channel
   */
  tiledb_query_channel_handle_t(
      std::shared_ptr<class tiledb::sm::QueryChannel> channel)
      : channel_(channel) {
  }

  inline void add_aggregate(
      const char* output_field,
      const tiledb_channel_operation_handle_t* operation) {
    auto& query{channel_->query()};
    if (query.is_aggregate(output_field)) {
      throw tiledb::api::CAPIStatusException(
          "An aggregate operation for output field: " +
          std::string(output_field) + " already exists.");
    }

    // Add the aggregator the the default channel as this is the only channel
    // type we currently support
    query.add_aggregator_to_default_channel(
        output_field, operation->aggregator());
  }

  inline tiledb::sm::Query& query() {
    return channel_->query();
  }
};

struct tiledb_channel_operator_handle_t : public tiledb::api::CAPIHandle {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{
      "tiledb_channel_operator_handle_t"};

 private:
  std::string name_;

 public:
  /**
   * Default constructor doesn't make sense
   */
  tiledb_channel_operator_handle_t() = delete;

  /**
   * Ordinary constructor.
   * @param op An enum specifying the type of operator
   * @param name A string representation of the operator
   */
  explicit tiledb_channel_operator_handle_t(const std::string& name)
      : name_{name} {
  }

  [[nodiscard]] inline std::string name() const {
    return name_;
  }

  [[nodiscard]] std::shared_ptr<tiledb::sm::Operation> make_operation(
      const tiledb::sm::FieldInfo& fi) const;
};

#endif  // TILEDB_CAPI_QUERY_AGGREGATE_INTERNAL_H
