/**
 * @file   tiledb_cpp_api_array.h
 *
 * @author Ravi Gaddipati
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
 * This file declares the C++ API for TileDB array operations.
 */

#ifndef TILEDB_CPP_API_ARRAY_H
#define TILEDB_CPP_API_ARRAY_H

#include "array_schema.h"
#include "context.h"
#include "tiledb.h"
#include "type.h"

#include <unordered_map>
#include <vector>

namespace tiledb {

/**
 * TileDB array class.
 */
class Array {
 public:
  /**
   * Constructor.
   *
   * @param array_uri The array URI.
   */
  Array(
      const Context& ctx,
      const std::string& array_uri,
      tiledb_query_type_t query_type)
      : ctx_(ctx)
      , schema_(ArraySchema(ctx, (tiledb_array_schema_t*)nullptr))
      , uri_(array_uri) {
    tiledb_array_t* array;
    ctx.handle_error(tiledb_array_alloc(ctx, array_uri.c_str(), &array));
    array_ = std::shared_ptr<tiledb_array_t>(array, deleter_);
    ctx.handle_error(tiledb_array_open(ctx, array, query_type));

    tiledb_array_schema_t* array_schema;
    ctx.handle_error(tiledb_array_get_schema(ctx, array, &array_schema));
    schema_ = ArraySchema(ctx, array_schema);
  }

  Array(const Array&) = default;
  Array(Array&& array) = default;
  Array& operator=(const Array&) = default;
  Array& operator=(Array&& o) = default;

  ~Array() {
    close();
  }

  /** Returns the array URI. */
  std::string uri() const {
    return uri_;
  }

  /** Returns a shared pointer to the C TileDB array object. */
  std::shared_ptr<tiledb_array_t> ptr() const {
    return array_;
  }

  /** Auxiliary operator for getting the underlying C TileDB object. */
  operator tiledb_array_t*() const {
    return array_.get();
  }

  /** Opens the array. */
  void open(tiledb_query_type_t query_type) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_open(ctx, array_.get(), query_type));
    tiledb_array_schema_t* array_schema;
    ctx.handle_error(tiledb_array_get_schema(ctx, array_.get(), &array_schema));
    schema_ = ArraySchema(ctx, array_schema);
  }

  /** Re-opens the array. */
  void reopen() {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_reopen(ctx, array_.get()));
    tiledb_array_schema_t* array_schema;
    ctx.handle_error(tiledb_array_get_schema(ctx, array_.get(), &array_schema));
    schema_ = ArraySchema(ctx, array_schema);
  }

  /** Closes the array. */
  void close() {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_close(ctx, array_.get()));
  }

  /**
   * Consolidates the fragments of an array into a single fragment.
   *
   * You must first finalize all queries to the array before consolidation can
   * begin (as consolidation temporarily acquires an exclusive lock on the
   * array).
   *
   * @param ctx TileDB context
   * @param uri Array URI
   */
  static void consolidate(const Context& ctx, const std::string& uri) {
    ctx.handle_error(tiledb_array_consolidate(ctx, uri.c_str()));
  }

  /** Creates an array on persistent storage from a schema definition. **/
  static void create(const std::string& uri, const ArraySchema& schema) {
    auto& ctx = schema.context();
    ctx.handle_error(tiledb_array_schema_check(ctx, schema));
    ctx.handle_error(tiledb_array_create(ctx, uri.c_str(), schema));
  }

  /**
   * Get the non-empty domain of the array. This returns the bounding
   * coordinates for each dimension.
   *
   * @tparam T Domain datatype
   * @return Vector of dim names with a {lower, upper} pair. Inclusive.
   *         Empty vector if the array has no data.
   */
  template <typename T>
  std::vector<std::pair<std::string, std::pair<T, T>>> non_empty_domain() {
    impl::type_check<T>(schema_.domain().type());
    std::vector<std::pair<std::string, std::pair<T, T>>> ret;

    auto dims = schema_.domain().dimensions();
    std::vector<T> buf(dims.size() * 2);
    int empty;

    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_get_non_empty_domain(
        ctx, array_.get(), buf.data(), &empty));

    if (empty)
      return ret;

    for (size_t i = 0; i < dims.size(); ++i) {
      auto domain = std::pair<T, T>(buf[i * 2], buf[(i * 2) + 1]);
      ret.push_back(
          std::pair<std::string, std::pair<T, T>>(dims[i].name(), domain));
    }

    return ret;
  }

  /**
   * Compute an upper bound on the buffer elements needed to read a subarray.
   *
   * @tparam T The domain datatype
   * @param uri The array URI.
   * @param schema The array schema
   * @param subarray Targeted subarray.
   * @return The maximum number of elements for each array attribute (plus
   *     coordinates for sparse arrays).
   *     Note that two numbers are returned per attribute. The first
   *     is the maximum number of elements in the offset buffer
   *     (0 for fixed-sized attributes and coordinates),
   *     and the second is the maximum number of elements of the value buffer.
   */
  template <typename T>
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
  max_buffer_elements(const std::vector<T>& subarray) {
    auto ctx = ctx_.get();
    impl::type_check<T>(schema_.domain().type(), 1);

    // Handle attributes
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> ret;
    auto schema_attrs = schema_.attributes();
    uint64_t attr_size, type_size;
    for (const auto& a : schema_attrs) {
      auto var = a.second.cell_val_num() == TILEDB_VAR_NUM;
      auto name = a.second.name();
      type_size = tiledb_datatype_size(a.second.type());

      if (var) {
        uint64_t size_off, size_val;
        ctx.handle_error(tiledb_array_max_buffer_size_var(
            ctx,
            array_.get(),
            name.c_str(),
            subarray.data(),
            &size_off,
            &size_val));
        ret[a.first] = std::pair<uint64_t, uint64_t>(
            size_off / TILEDB_OFFSET_SIZE, size_val / type_size);
      } else {
        ctx.handle_error(tiledb_array_max_buffer_size(
            ctx, array_.get(), name.c_str(), subarray.data(), &attr_size));
        ret[a.first] = std::pair<uint64_t, uint64_t>(0, attr_size / type_size);
      }
    }

    // Handle coordinates
    type_size = tiledb_datatype_size(schema_.domain().type());
    ctx.handle_error(tiledb_array_max_buffer_size(
        ctx, array_.get(), TILEDB_COORDS, subarray.data(), &attr_size));
    ret[TILEDB_COORDS] =
        std::pair<uint64_t, uint64_t>(0, attr_size / type_size);

    return ret;
  }

  /** Returns the query type the array was opened with. */
  tiledb_query_type_t query_type() const {
    auto& ctx = ctx_.get();
    tiledb_query_type_t query_type;
    ctx.handle_error(
        tiledb_array_get_query_type(ctx, array_.get(), &query_type));
    return query_type;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** Pointer to the TileDB C array object. */
  std::shared_ptr<tiledb_array_t> array_;

  /** The array schema. */
  ArraySchema schema_;

  /** The array URI. */
  std::string uri_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_ARRAY_H
