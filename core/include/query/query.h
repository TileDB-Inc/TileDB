/**
 * @file   query.h
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
 * This file defines class Query.
 */

#ifndef TILEDB_QUERY_H
#define TILEDB_QUERY_H

#include "array.h"
#include "attribute_buffer.h"
#include "bookkeeping.h"
#include "dimension_buffer.h"
#include "metadata.h"
#include "query_status.h"
#include "query_type.h"
#include "status.h"

#include <map>
#include <vector>

namespace tiledb {

/** Manages a TileDB query object. */
class Query {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  Query() = default;

  explicit Query(Array* array);

  explicit Query(Metadata* metadata);

  ~Query();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  const Array* array() const;

  ArrayType array_type() const;

  bool async() const;

  const std::vector<AttributeBuffer*>& attribute_buffers() const;

  Bookkeeping* bookkeeping() const;

  Status check() const;

  const std::vector<DimensionBuffer*>& dimension_buffers() const;

    Status overflow(const char* attr, bool* overflow);

  void set_async(void* (*callback)(void*), void* callback_data);

  Status set_attribute_buffer(
      const char* name, void* buffer, uint64_t buffer_size);

  Status set_attribute_buffer(
      const char* name,
      void* buffer,
      uint64_t buffer_size,
      void* buffer_var,
      uint64_t buffer_var_size);

  void set_bookkeeping(Bookkeeping* bookkeeping);

  Status set_dimension_buffer(
      const char* name, void* buffer, uint64_t buffer_size);

  void set_status(QueryStatus status);

  Status set_subarray(const void* subarray);

  void set_query_type(QueryType query_type);

  QueryStatus status() const;

  QueryType query_type() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  const Array* array_;

  bool async_;

  std::vector<AttributeBuffer*> attribute_buffers_;

  std::map<std::string, AttributeBuffer*> attribute_buffers_map_;

  Bookkeeping* bookkeeping_;

  void* (*callback_)(void*);

  void* callback_data_;

  std::vector<DimensionBuffer*> dimension_buffers_;

  std::map<std::string, DimensionBuffer*> dimension_buffers_map_;

  const Metadata* metadata_;

  QueryStatus status_;

  void* subarray_;

  QueryType query_type_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  AttributeBuffer* attribute_buffer(const std::string& name);

  DimensionBuffer* dimension_buffer(const std::string& name);

  void set_default();
};

}  // namespace tiledb

#endif  // TILEDB_QUERY_H
