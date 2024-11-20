/**
 * @file tiledb/api/c_api/query_field/query_field_api_internal.h
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
 * This file declares the internal mechanics of the query field C API
 */

#ifndef TILEDB_CAPI_QUERY_FIELD_INTERNAL_H
#define TILEDB_CAPI_QUERY_FIELD_INTERNAL_H

#include "tiledb/api/c_api/query_aggregate/query_aggregate_api_internal.h"
#include "tiledb/api/c_api/query_field/query_field_api_external_experimental.h"
#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/query/query.h"

class FieldOrigin {
 public:
  virtual tiledb_field_origin_t origin() = 0;
  virtual ~FieldOrigin() = default;
};

class FieldFromDimension : public FieldOrigin {
 public:
  virtual tiledb_field_origin_t origin() override;
};

class FieldFromAttribute : public FieldOrigin {
 public:
  virtual tiledb_field_origin_t origin() override;
};

class FieldFromAggregate : public FieldOrigin {
 public:
  virtual tiledb_field_origin_t origin() override;
};

struct tiledb_query_field_handle_t : public tiledb::api::CAPIHandle {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"tiledb_query_field_t"};

 private:
  tiledb::sm::Query* query_;
  std::string field_name_;
  std::shared_ptr<FieldOrigin> field_origin_;
  tiledb::sm::Datatype type_;
  uint32_t cell_val_num_;
  bool is_nullable_;
  std::shared_ptr<tiledb::sm::QueryChannel> channel_;

 public:
  /**
   * Default constructor doesn't make sense
   */
  tiledb_query_field_handle_t() = delete;

  /**
   * Ordinary constructor.
   * @param query The query object that owns the channel
   */
  tiledb_query_field_handle_t(tiledb_query_t* query, const char* field_name);

  tiledb_field_origin_t origin() {
    return field_origin_->origin();
  }
  tiledb::sm::Datatype type() {
    return type_;
  }
  uint32_t cell_val_num() {
    return cell_val_num_;
  }
  bool is_nullable() const {
    return is_nullable_;
  }
  tiledb_query_channel_handle_t* channel() {
    return make_handle<tiledb_query_channel_handle_t>(channel_);
  }
};

#endif  // TILEDB_CAPI_QUERY_FIELD_INTERNAL_H
