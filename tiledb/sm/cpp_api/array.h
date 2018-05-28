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
  Array(const Context& ctx, const std::string& array_uri)
      : ctx_(ctx)
      , schema_(ArraySchema(ctx, (tiledb_array_schema_t*)nullptr))
      , uri_(array_uri) {
    tiledb_array_t* array;
    ctx.handle_error(tiledb_array_alloc(ctx, array_uri.c_str(), &array));
    ctx.handle_error(tiledb_array_open(ctx, array));
    array_ = std::shared_ptr<tiledb_array_t>(array, deleter_);

    tiledb_array_schema_t* array_schema;
    ctx.handle_error(tiledb_array_get_schema(ctx, array, &array_schema));
    schema_ = ArraySchema(ctx, array_schema);
  }

  Array(const Array&) = default;
  Array(Array&& array) = default;
  Array& operator=(const Array&) = default;
  Array& operator=(Array&& o) = default;

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
  void open() {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_array_open(ctx, array_.get()));
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

    std::vector<uint64_t> sizes;
    auto attrs = schema_.attributes();
    std::vector<const char*> names;

    unsigned nbuffs = 0, attr_num = 0;
    for (const auto& a : attrs) {
      nbuffs += a.second.cell_val_num() == TILEDB_VAR_NUM ? 2 : 1;
      ++attr_num;
      names.push_back(a.first.c_str());
    }
    if (schema_.array_type() == TILEDB_SPARSE) {
      ++nbuffs;
      ++attr_num;
      names.push_back(TILEDB_COORDS);
    }

    sizes.resize(nbuffs);
    ctx.handle_error(tiledb_array_compute_max_read_buffer_sizes(
        ctx,
        array_.get(),
        subarray.data(),
        names.data(),
        attr_num,
        sizes.data()));

    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> ret;
    unsigned sid = 0;
    for (const auto& a : attrs) {
      auto var = a.second.cell_val_num() == TILEDB_VAR_NUM;
      ret[a.first] =
          var ? std::pair<uint64_t, uint64_t>(
                    sizes[sid] / TILEDB_OFFSET_SIZE,
                    sizes[sid + 1] / tiledb_datatype_size(a.second.type())) :
                std::pair<uint64_t, uint64_t>(
                    0, sizes[sid] / tiledb_datatype_size(a.second.type()));
      sid += var ? 2 : 1;
    }

    if (schema_.array_type() == TILEDB_SPARSE)
      ret[TILEDB_COORDS] = std::pair<uint64_t, uint64_t>(
          0, sizes.back() / tiledb_datatype_size(schema_.domain().type()));

    return ret;
  }

  /**
   * Partitions a subarray into multiple subarrays to meet buffer
   * size constraints. Layout is used to split the subarray by
   * the optimal axis.
   *
   * @tparam T Domain type
   * @param uri Array URI
   * @param schema Array Schema
   * @param subarray Subarray to be decomposed
   * @param attrs Attributes being queried
   * @param buffer_sizes Buffer size limits
   * @param layout Layout of query
   * @return list of subarrays
   */
  template <typename T>
  std::vector<std::vector<T>> partition_subarray(
      const std::vector<T>& subarray,
      const std::vector<std::string>& attrs,
      const std::vector<size_t>& buffer_sizes,
      tiledb_layout_t layout) {
    impl::type_check<T>(schema_.domain().type());

    const auto& schema_attrs = schema_.attributes();
    std::vector<const char*> attrnames;
    std::vector<uint64_t> buff_sizes_scaled;
    unsigned expected_buff_cnt = 0;

    for (auto& a : attrs) {
      if (schema_attrs.count(a) == 0)
        throw AttributeError("Attribute does not exist: " + a);
      attrnames.push_back(a.data());
      if (schema_attrs.at(a).variable_sized()) {
        buff_sizes_scaled.push_back(
            buffer_sizes[expected_buff_cnt] * TILEDB_OFFSET_SIZE);
        ++expected_buff_cnt;
      }
      buff_sizes_scaled.push_back(
          buffer_sizes[expected_buff_cnt] *
          tiledb_datatype_size(schema_attrs.at(a).type()));
      ++expected_buff_cnt;
    }
    if (expected_buff_cnt != buffer_sizes.size())
      throw TileDBError(
          "buffer_sizes size does not match number of provided attributes.");
    if (subarray.size() != schema_.domain().ndim() * 2)
      throw TileDBError("Subarray should have array ndim * 2 values.");

    auto& ctx = ctx_.get();
    T** partition_buf;
    uint64_t npartitions;

    ctx.handle_error(tiledb_array_partition_subarray(
        ctx,
        array_.get(),
        subarray.data(),
        layout,
        attrnames.data(),
        (unsigned)attrnames.size(),
        buff_sizes_scaled.data(),
        (void***)&partition_buf,
        &npartitions));

    std::vector<std::vector<T>> ret;

    if (partition_buf != nullptr) {
      for (uint64_t i = 0; i < npartitions; ++i) {
        ret.emplace_back(partition_buf[i], partition_buf[i] + subarray.size());
        free(partition_buf[i]);
      }
      free(partition_buf);
    }

    return ret;
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
