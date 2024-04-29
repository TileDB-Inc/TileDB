/**
 * @file   query_deprecated.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * C++ Deprecated API for TileDB Query.
 */

#include "tiledb.h"

/**
 * Submit an async query, with callback. Call returns immediately.
 *
 * Deprecated, call `submit()` on another thread instead.
 *
 * @note Same notes apply as `Query::submit()`.
 *
 * **Example:**
 * @code{.cpp}
 * // Create query
 * tiledb::Query query(...);
 * // Submit with callback
 * query.submit_async([]() { std::cout << "Callback: query completed.\n"; });
 * @endcode
 *
 * @param callback Callback function.
 */
template <typename Fn>
TILEDB_DEPRECATED void submit_async(const Fn& callback) {
  std::function<void(void*)> wrapper = [&](void*) { callback(); };
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb::impl::tiledb_query_submit_async_func(
      ctx.ptr().get(), query_.get(), &wrapper, nullptr));
}

/**
 * Submit an async query, with no callback. Call returns immediately.
 *
 * Deprecated, call `submit()` on another thread instead.
 *
 * @note Same notes apply as `Query::submit()`.
 *
 * **Example:**
 * @code{.cpp}
 * // Create query
 * tiledb::Query query(...);
 * // Submit with no callback
 * query.submit_async();
 * @endcode
 */
TILEDB_DEPRECATED void submit_async() {
  submit_async([]() {});
}

/**
 * Adds a 1D range along a subarray dimension index, in the form
 * (start, end, stride). The datatype of the range
 * must be the same as the dimension datatype.
 *
 * **Example:**
 *
 * @code{.cpp}
 * // Set a 1D range on dimension 0, assuming the domain type is int64.
 * int64_t start = 10;
 * int64_t end = 20;
 * // Stride is optional
 * subarray.add_range(0, start, end);
 * @endcode
 *
 * @tparam T The dimension datatype
 * @param dim_idx The index of the dimension to add the range to.
 * @param start The range start to add.
 * @param end The range end to add.
 * @param stride The range stride to add.
 * @return Reference to this Query
 */
template <class T>
TILEDB_DEPRECATED Query& add_range(
    uint32_t dim_idx, T start, T end, T stride = 0) {
  impl::type_check<T>(schema_.domain().dimension(dim_idx).type());
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_query_add_range(
      ctx.ptr().get(),
      query_.get(),
      dim_idx,
      &start,
      &end,
      (stride == 0) ? nullptr : &stride));
  return *this;
}

/**
 * Adds a 1D range along a subarray dimension name, specified by its name, in
 * the form (start, end, stride). The datatype of the range must be the same
 * as the dimension datatype.
 *
 * **Example:**
 *
 * @code{.cpp}
 * // Set a 1D range on dimension "rows", assuming the domain type is int64.
 * int64_t start = 10;
 * int64_t end = 20;
 * const std::string dim_name = "rows";
 * // Stride is optional
 * subarray.add_range(dim_name, start, end);
 * @endcode
 *
 * @tparam T The dimension datatype
 * @param dim_name The name of the dimension to add the range to.
 * @param start The range start to add.
 * @param end The range end to add.
 * @param stride The range stride to add.
 * @return Reference to this Query
 */
template <class T>
TILEDB_DEPRECATED Query& add_range(
    const std::string& dim_name, T start, T end, T stride = 0) {
  impl::type_check<T>(schema_.domain().dimension(dim_name).type());
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_query_add_range_by_name(
      ctx.ptr().get(),
      query_.get(),
      dim_name.c_str(),
      &start,
      &end,
      (stride == 0) ? nullptr : &stride));
  return *this;
}

/**
 * Adds a 1D string range along a subarray dimension index, in the form
 * (start, end). Applicable only to variable-sized dimensions
 *
 * **Example:**
 *
 * @code{.cpp}
 * // Set a 1D range on variable-sized string dimension "rows"
 * std::string start = "ab"";
 * std::string end = "d";
 * // Stride is optional
 * subarray.add_range(0, start, end);
 * @endcode
 *
 * @tparam T The dimension datatype
 * @param dim_idx The index of the dimension to add the range to.
 * @param start The range start to add.
 * @param end The range end to add.
 * @return Reference to this Query
 */
TILEDB_DEPRECATED
Query& add_range(
    uint32_t dim_idx, const std::string& start, const std::string& end) {
  impl::type_check<char>(schema_.domain().dimension(dim_idx).type());
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_query_add_range_var(
      ctx.ptr().get(),
      query_.get(),
      dim_idx,
      start.c_str(),
      start.size(),
      end.c_str(),
      end.size()));
  return *this;
}

/**
 * Adds a 1D string range along a subarray dimension name, in the form (start,
 * end). Applicable only to variable-sized dimensions
 *
 * **Example:**
 *
 * @code{.cpp}
 * // Set a 1D range on variable-sized string dimension "rows"
 * std::string start = "ab"";
 * std::string end = "d";
 * const std::string dim_name = "rows";
 * // Stride is optional
 * subarray.add_range(dim_name, start, end);
 * @endcode
 *
 * @tparam T The dimension datatype
 * @param dim_name The name of the dimension to add the range to.
 * @param start The range start to add.
 * @param end The range end to add.
 * @return Reference to this Query
 */
TILEDB_DEPRECATED
Query& add_range(
    const std::string& dim_name,
    const std::string& start,
    const std::string& end) {
  impl::type_check<char>(schema_.domain().dimension(dim_name).type());
  auto& ctx = ctx_.get();
  ctx.handle_error(tiledb_query_add_range_var_by_name(
      ctx.ptr().get(),
      query_.get(),
      dim_name.c_str(),
      start.c_str(),
      start.size(),
      end.c_str(),
      end.size()));
  return *this;
}

/**
 * Retrieves the number of ranges for a given dimension index.
 *
 * **Example:**
 *
 * @code{.cpp}
 * unsigned dim_idx = 0;
 * uint64_t range_num = query.range_num(dim_idx);
 * @endcode
 *
 * @param dim_idx The dimension index.
 * @return The number of ranges.
 */
TILEDB_DEPRECATED
uint64_t range_num(unsigned dim_idx) const {
  auto& ctx = ctx_.get();
  uint64_t range_num;
  ctx.handle_error(tiledb_query_get_range_num(
      ctx.ptr().get(), query_.get(), dim_idx, &range_num));
  return range_num;
}

/**
 * Retrieves the number of ranges for a given dimension name.
 *
 * **Example:**
 *
 * @code{.cpp}
 * unsigned dim_name = "rows";
 * uint64_t range_num = query.range_num(dim_name);
 * @endcode
 *
 * @param dim_name The dimension name.
 * @return The number of ranges.
 */
TILEDB_DEPRECATED
uint64_t range_num(const std::string& dim_name) const {
  auto& ctx = ctx_.get();
  uint64_t range_num;
  ctx.handle_error(tiledb_query_get_range_num_from_name(
      ctx.ptr().get(), query_.get(), dim_name.c_str(), &range_num));
  return range_num;
}

/**
 * Retrieves a range for a given dimension index and range id.
 * The template datatype must be the same as that of the
 * underlying array.
 *
 * **Example:**
 *
 * @code{.cpp}
 * unsigned dim_idx = 0;
 * unsigned range_idx = 0;
 * auto range = query.range<int32_t>(dim_idx, range_idx);
 * @endcode
 *
 * @tparam T The dimension datatype.
 * @param dim_idx The dimension index.
 * @param range_idx The range index.
 * @return A triplet of the form (start, end, stride).
 */
template <class T>
TILEDB_DEPRECATED std::array<T, 3> range(unsigned dim_idx, uint64_t range_idx) {
  impl::type_check<T>(schema_.domain().dimension(dim_idx).type());
  auto& ctx = ctx_.get();
  const void *start, *end, *stride;
  ctx.handle_error(tiledb_query_get_range(
      ctx.ptr().get(),
      query_.get(),
      dim_idx,
      range_idx,
      &start,
      &end,
      &stride));
  std::array<T, 3> ret = {
      {*(const T*)start,
       *(const T*)end,
       (stride == nullptr) ? 0 : *(const T*)stride}};
  return ret;
}

/**
 * Retrieves a range for a given dimension name and range id.
 * The template datatype must be the same as that of the
 * underlying array.
 *
 * **Example:**
 *
 * @code{.cpp}
 * unsigned dim_name = "rows";
 * unsigned range_idx = 0;
 * auto range = query.range<int32_t>(dim_name, range_idx);
 * @endcode
 *
 * @tparam T The dimension datatype.
 * @param dim_name The dimension name.
 * @param range_idx The range index.
 * @return A triplet of the form (start, end, stride).
 */
template <class T>
TILEDB_DEPRECATED std::array<T, 3> range(
    const std::string& dim_name, uint64_t range_idx) {
  impl::type_check<T>(schema_.domain().dimension(dim_name).type());
  auto& ctx = ctx_.get();
  const void *start, *end, *stride;
  ctx.handle_error(tiledb_query_get_range_from_name(
      ctx.ptr().get(),
      query_.get(),
      dim_name.c_str(),
      range_idx,
      &start,
      &end,
      &stride));
  std::array<T, 3> ret = {
      {*(const T*)start,
       *(const T*)end,
       (stride == nullptr) ? 0 : *(const T*)stride}};
  return ret;
}

/**
 * Retrieves a range for a given variable length string dimension index and
 * range id.
 *
 * **Example:**
 *
 * @code{.cpp}
 * unsigned dim_idx = 0;
 * unsigned range_idx = 0;
 * std::array<std::string, 2> range = query.range(dim_idx, range_idx);
 * @endcode
 *
 * @param dim_idx The dimension index.
 * @param range_idx The range index.
 * @return A pair of the form (start, end).
 */
TILEDB_DEPRECATED
std::array<std::string, 2> range(unsigned dim_idx, uint64_t range_idx) {
  impl::type_check<char>(schema_.domain().dimension(dim_idx).type());
  auto& ctx = ctx_.get();
  uint64_t start_size, end_size;
  ctx.handle_error(tiledb_query_get_range_var_size(
      ctx.ptr().get(),
      query_.get(),
      dim_idx,
      range_idx,
      &start_size,
      &end_size));

  std::string start;
  start.resize(start_size);
  std::string end;
  end.resize(end_size);
  ctx.handle_error(tiledb_query_get_range_var(
      ctx.ptr().get(), query_.get(), dim_idx, range_idx, &start[0], &end[0]));
  std::array<std::string, 2> ret = {{std::move(start), std::move(end)}};
  return ret;
}

/**
 * Retrieves a range for a given variable length string dimension name and
 * range id.
 *
 * **Example:**
 *
 * @code{.cpp}
 * unsigned dim_name = "rows";
 * unsigned range_idx = 0;
 * std::array<std::string, 2> range = query.range(dim_name, range_idx);
 * @endcode
 *
 * @param dim_name The dimension name.
 * @param range_idx The range index.
 * @return A pair of the form (start, end).
 */
TILEDB_DEPRECATED
std::array<std::string, 2> range(
    const std::string& dim_name, uint64_t range_idx) {
  impl::type_check<char>(schema_.domain().dimension(dim_name).type());
  auto& ctx = ctx_.get();
  uint64_t start_size, end_size;
  ctx.handle_error(tiledb_query_get_range_var_size_from_name(
      ctx.ptr().get(),
      query_.get(),
      dim_name.c_str(),
      range_idx,
      &start_size,
      &end_size));

  std::string start;
  start.resize(start_size);
  std::string end;
  end.resize(end_size);
  ctx.handle_error(tiledb_query_get_range_var_from_name(
      ctx.ptr().get(),
      query_.get(),
      dim_name.c_str(),
      range_idx,
      &start[0],
      &end[0]));
  std::array<std::string, 2> ret = {{std::move(start), std::move(end)}};
  return ret;
}

/**
 * The query set_subarray function has been deprecated.
 * See the documentation for Subarray::set_subarray(), or use other
 * Subarray provided APIs.
 * Consult the current documentation for more information.
 *
 * Sets a subarray, defined in the order dimensions were added.
 * Coordinates are inclusive. For the case of writes, this is meaningful only
 * for dense arrays, and specifically dense writes.
 *
 * @note `set_subarray(std::vector<T>)` is preferred as it is safer.
 *
 * **Example:**
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::Array array(ctx, array_name, TILEDB_READ);
 * int subarray[] = {0, 3, 0, 3};
 * Query query(ctx, array);
 * query.set_subarray(subarray, 4);
 * @endcode
 *
 * @tparam T Type of array domain.
 * @param pairs Subarray pointer defined as an array of [start, stop] values
 * per dimension.
 * @param size The number of subarray elements.
 */
template <typename T = uint64_t>
TILEDB_DEPRECATED Query& set_subarray(const T* pairs, uint64_t size) {
  impl::type_check<T>(schema_.domain().type());
  auto& ctx = ctx_.get();
  if (size != schema_.domain().ndim() * 2) {
    throw SchemaMismatch(
        "Subarray should have num_dims * 2 values: (low, high) for each "
        "dimension.");
  }
  ctx.handle_error(
      tiledb_query_set_subarray(ctx.ptr().get(), query_.get(), pairs));
  return *this;
}

/**
 * Sets a subarray, defined in the order dimensions were added.
 * Coordinates are inclusive. For the case of writes, this is meaningful only
 * for dense arrays, and specifically dense writes.
 *
 * **Example:**
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::Array array(ctx, array_name, TILEDB_READ);
 * std::vector<int> subarray = {0, 3, 0, 3};
 * Query query(ctx, array);
 * query.set_subarray(subarray);
 * @endcode
 *
 * @tparam Vec Vector datatype. Should always be a vector of the domain type.
 * @param pairs The subarray defined as a vector of [start, stop] coordinates
 * per dimension.
 */
template <typename Vec>
TILEDB_DEPRECATED Query& set_subarray(const Vec& pairs) {
  return set_subarray(pairs.data(), pairs.size());
}

/**
 * Sets a subarray, defined in the order dimensions were added.
 * Coordinates are inclusive. For the case of writes, this is meaningful only
 * for dense arrays, and specifically dense writes.
 *
 * **Example:**
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::Array array(ctx, array_name, TILEDB_READ);
 * Query query(ctx, array);
 * query.set_subarray({0, 3, 0, 3});
 * @endcode
 *
 * @tparam T Type of array domain.
 * @param pairs List of [start, stop] coordinates per dimension.
 */
template <typename T = uint64_t>
TILEDB_DEPRECATED Query& set_subarray(const std::initializer_list<T>& l) {
  return set_subarray(std::vector<T>(l));
}

/**
 * Sets a subarray, defined in the order dimensions were added.
 * Coordinates are inclusive.
 *
 * @note set_subarray(std::vector) is preferred and avoids an extra copy.
 *
 * @tparam T Type of array domain.
 * @param pairs The subarray defined as pairs of [start, stop] per dimension.
 */
template <typename T = uint64_t>
TILEDB_DEPRECATED Query& set_subarray(
    const std::vector<std::array<T, 2>>& pairs) {
  std::vector<T> buf;
  buf.reserve(pairs.size() * 2);
  std::for_each(pairs.begin(), pairs.end(), [&buf](const std::array<T, 2>& p) {
    buf.push_back(p[0]);
    buf.push_back(p[1]);
  });
  return set_subarray(buf);
}

/**
 * Set the coordinate buffer.
 *
 * The coordinate buffer has been deprecated. Set the coordinates for
 * each individual dimension with the `set_buffer` API. Consult the current
 * documentation for more information.
 *
 * **Example:**
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
 * // Write to points (0,1) and (2,3) in a 2D array with int domain.
 * int coords[] = {0, 1, 2, 3};
 * Query query(ctx, array);
 * query.set_layout(TILEDB_UNORDERED).set_coordinates(coords, 4);
 * @endcode
 *
 * @note set_coordinates(std::vector<T>) is preferred as it is safer.
 *
 * @tparam T Type of array domain.
 * @param buf Coordinate array buffer pointer
 * @param size The number of elements in the coordinate array buffer
 * **/
template <typename T>
TILEDB_DEPRECATED Query& set_coordinates(T* buf, uint64_t size) {
  impl::type_check<T>(schema_.domain().type());
  return set_data_buffer("__coords", buf, size);
}

/**
 * Set the coordinate buffer for unordered queries.
 *
 * The coordinate buffer has been deprecated. Set the coordinates for
 * each individual dimension with the `set_buffer` API. Consult the current
 * documentation for more information.
 *
 * **Example:**
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
 * // Write to points (0,1) and (2,3) in a 2D array with int domain.
 * std::vector<int> coords = {0, 1, 2, 3};
 * Query query(ctx, array);
 * query.set_layout(TILEDB_UNORDERED).set_coordinates(coords);
 * @endcode
 *
 * @tparam Vec Vector datatype. Should always be a vector of the domain type.
 * @param buf Coordinate vector
 * **/
template <typename Vec>
TILEDB_DEPRECATED Query& set_coordinates(Vec& buf) {
  return set_coordinates(buf.data(), buf.size());
}

/**
 * Sets a buffer for a fixed-sized attribute/dimension.
 *
 * **Example:**
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
 * int data_a1[] = {0, 1, 2, 3};
 * Query query(ctx, array);
 * query.set_buffer("a1", data_a1, 4);
 * @endcode
 *
 * @note set_buffer(std::string, std::vector) is preferred as it is safer.
 *
 * @tparam T Attribute/Dimension value type
 * @param name Attribute/Dimension name
 * @param buff Buffer array pointer with elements of the
 *     attribute/dimension type.
 * @param nelements Number of array elements
 **/
template <typename T>
TILEDB_DEPRECATED Query& set_buffer(
    const std::string& name, T* buff, uint64_t nelements) {
  // Checks
  auto is_attr = schema_.has_attribute(name);
  auto is_dim = schema_.domain().has_dimension(name);
  if (name != "__coords" && !is_attr && !is_dim)
    throw TileDBError(
        std::string("Cannot set buffer; Attribute/Dimension '") + name +
        "' does not exist");
  else if (is_attr)
    impl::type_check<T>(schema_.attribute(name).type());
  else if (is_dim)
    impl::type_check<T>(schema_.domain().dimension(name).type());
  else if (name == "__coords")
    impl::type_check<T>(schema_.domain().type());

  return set_data_buffer(name, buff, nelements, sizeof(T));
}

/**
 * Sets a buffer for a fixed-sized attribute/dimension.
 *
 * **Example:**
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
 * std::vector<int> data_a1 = {0, 1, 2, 3};
 * Query query(ctx, array);
 * query.set_buffer("a1", data_a1);
 * @endcode
 *
 * @tparam T Attribute/Dimension value type
 * @param name Attribute/Dimension name
 * @param buf Buffer vector with elements of the attribute/dimension type.
 **/
template <typename T>
TILEDB_DEPRECATED Query& set_buffer(
    const std::string& name, std::vector<T>& buf) {
  return set_data_buffer(name, buf.data(), buf.size(), sizeof(T));
}

/**
 * Sets a buffer for a fixed-sized attribute/dimension.
 *
 * @note This unsafe version does not perform type checking; the given buffer
 * is assumed to be the correct type, and the size of an element in the given
 * buffer is assumed to be the size of the datatype of the attribute.
 *
 * @param name Attribute/Dimension name
 * @param buff Buffer array pointer with elements of the attribute type.
 * @param nelements Number of array elements in buffer
 **/
TILEDB_DEPRECATED Query& set_buffer(
    const std::string& name, void* buff, uint64_t nelements) {
  // Checks
  auto is_attr = schema_.has_attribute(name);
  auto is_dim = schema_.domain().has_dimension(name);
  if (name != "__coords" && !is_attr && !is_dim)
    throw TileDBError(
        std::string("Cannot set buffer; Attribute/Dimension '") + name +
        "' does not exist");

  // Compute element size (in bytes).
  size_t element_size = 0;
  if (name == "__coords")
    element_size = tiledb_datatype_size(schema_.domain().type());
  else if (is_attr)
    element_size = tiledb_datatype_size(schema_.attribute(name).type());
  else if (is_dim)
    element_size =
        tiledb_datatype_size(schema_.domain().dimension(name).type());

  return set_data_buffer(name, buff, nelements, element_size);
}

/**
 * Sets a buffer for a variable-sized attribute/dimension.
 *
 * **Example:**
 *
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
 * int data_a1[] = {0, 1, 2, 3};
 * uint64_t offsets_a1[] = {0, 8};
 * Query query(ctx, array);
 * query.set_buffer("a1", offsets_a1, 2, data_a1, 4);
 * @endcode
 *
 * @note set_buffer(std::string, std::vector, std::vector) is preferred as it
 * is safer.
 *
 * @tparam T Attribute/Dimension value type
 * @param name Attribute/Dimension name
 * @param offsets Offsets array pointer where a new element begins in the data
 *        buffer.
 * @param offsets_nelements Number of elements in offsets buffer.
 * @param data Buffer array pointer with elements of the attribute type.
 *        For variable sized attributes, the buffer should be flattened.
 * @param data_nelements Number of array elements in data buffer.
 **/
template <typename T>
TILEDB_DEPRECATED Query& set_buffer(
    const std::string& name,
    uint64_t* offsets,
    uint64_t offset_nelements,
    T* data,
    uint64_t data_nelements) {
  // Checks
  auto is_attr = schema_.has_attribute(name);
  auto is_dim = schema_.domain().has_dimension(name);
  if (!is_attr && !is_dim)
    throw TileDBError(
        std::string("Cannot set buffer; Attribute/Dimension '") + name +
        "' does not exist");
  else if (is_attr)
    impl::type_check<T>(schema_.attribute(name).type());
  else if (is_dim)
    impl::type_check<T>(schema_.domain().dimension(name).type());

  set_data_buffer(name, data, data_nelements, sizeof(T));
  set_offsets_buffer(name, offsets, offset_nelements);
  return *this;
}

/**
 * Sets a buffer for a variable-sized attribute/dimension.
 *
 * @note This unsafe version does not perform type checking; the given buffer
 * is assumed to be the correct type, and the size of an element in the given
 * buffer is assumed to be the size of the datatype of the attribute.
 *
 * @param name Attribute/Dimension name
 * @param offsets Offsets array pointer where a new element begins in the data
 *        buffer.
 * @param offsets_nelements Number of elements in offsets buffer.
 * @param data Buffer array pointer with elements of the attribute type.
 * @param data_nelements Number of array elements in data buffer.
 **/
TILEDB_DEPRECATED Query& set_buffer(
    const std::string& name,
    uint64_t* offsets,
    uint64_t offset_nelements,
    void* data,
    uint64_t data_nelements) {
  // Checks
  auto is_attr = schema_.has_attribute(name);
  auto is_dim = schema_.domain().has_dimension(name);
  if (!is_attr && !is_dim)
    throw TileDBError(
        std::string("Cannot set buffer; Attribute/Dimension '") + name +
        "' does not exist");

  // Compute element size (in bytes).
  auto type = is_attr ? schema_.attribute(name).type() :
                        schema_.domain().dimension(name).type();
  size_t element_size = tiledb_datatype_size(type);

  set_data_buffer(name, data, data_nelements, element_size);
  set_offsets_buffer(name, offsets, offset_nelements);
  return *this;
}

/**
 * Sets a buffer for a variable-sized attribute/dimension.
 *
 * **Example:**
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
 * std::vector<int> data_a1 = {0, 1, 2, 3};
 * std::vector<uint64_t> offsets_a1 = {0, 8};
 * Query query(ctx, array);
 * query.set_buffer("a1", offsets_a1, data_a1);
 * @endcode
 *
 * @tparam T Attribute/Dimension value type
 * @param name Attribute/Dimension name
 * @param offsets Offsets where a new element begins in the data buffer.
 * @param data Buffer vector with elements of the attribute type.
 *        For variable sized attributes, the buffer should be flattened. E.x.
 *        an attribute of type std::string should have a buffer Vec type of
 *        std::string, where the values of each cell are concatenated.
 **/
template <typename T>
TILEDB_DEPRECATED Query& set_buffer(
    const std::string& name,
    std::vector<uint64_t>& offsets,
    std::vector<T>& data) {
  // Checks
  auto is_attr = schema_.has_attribute(name);
  auto is_dim = schema_.domain().has_dimension(name);
  if (!is_attr && !is_dim)
    throw TileDBError(
        std::string("Cannot set buffer; Attribute/Dimension '") + name +
        "' does not exist");
  else if (is_attr)
    impl::type_check<T>(schema_.attribute(name).type());
  else if (is_dim)
    impl::type_check<T>(schema_.domain().dimension(name).type());

  set_data_buffer(name, data.data(), data.size(), sizeof(T));
  set_offsets_buffer(name, offsets.data(), offsets.size());
  return *this;
}

/**
 * Sets a buffer for a variable-sized attribute/dimension.
 *
 * @tparam T Attribute/Dimension value type
 * @param attr Attribute/Dimension name
 * @param buf Pair of offset, data buffers
 **/
template <typename T>
TILEDB_DEPRECATED Query& set_buffer(
    const std::string& name,
    std::pair<std::vector<uint64_t>, std::vector<T>>& buf) {
  // Checks
  auto is_attr = schema_.has_attribute(name);
  auto is_dim = schema_.domain().has_dimension(name);
  if (!is_attr && !is_dim)
    throw TileDBError(
        std::string("Cannot set buffer; Attribute/Dimension '") + name +
        "' does not exist");
  else if (is_attr)
    impl::type_check<T>(schema_.attribute(name).type());
  else if (is_dim)
    impl::type_check<T>(schema_.domain().dimension(name).type());

  set_data_buffer(name, buf.second);
  set_offsets_buffer(name, buf.first);
  return *this;
}

/**
 * Sets a buffer for a string-typed variable-sized attribute/dimension.
 *
 * @param name Attribute/Dimension name
 * @param offsets Offsets where a new element begins in the data buffer.
 * @param data Pre-allocated string buffer.
 **/
TILEDB_DEPRECATED Query& set_buffer(
    const std::string& name,
    std::vector<uint64_t>& offsets,
    std::string& data) {
  // Checks
  auto is_attr = schema_.has_attribute(name);
  auto is_dim = schema_.domain().has_dimension(name);
  if (!is_attr && !is_dim)
    throw TileDBError(
        std::string("Cannot set buffer; Attribute/Dimension '") + name +
        "' does not exist");
  else if (is_attr)
    impl::type_check<char>(schema_.attribute(name).type());
  else if (is_dim)
    impl::type_check<char>(schema_.domain().dimension(name).type());

  set_data_buffer(name, &data[0], data.size(), sizeof(char));
  set_offsets_buffer(name, offsets.data(), offsets.size());
  return *this;
}

/**
 * Sets a buffer for a fixed-sized, nullable attribute.
 *
 * **Example:**
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
 * int data_a1[] = {0, 1, 2, 3};
 * uint8_t validity_bytemap[] = {1, 1, 0, 1};
 * Query query(ctx, array);
 * query.set_buffer("a1", data_a1, 4, validity_bytemap, 4);
 * @endcode
 *
 * @note set_buffer_nullable(std::string, std::vector, std::vector)
 * is preferred as it is safer.
 *
 * @tparam T Attribute value type
 * @param name Attribute name
 * @param data Buffer array pointer with elements of the
 *     attribute type.
 * @param data_nelements Number of array elements in `data`
 * @param validity_bytemap The validity bytemap buffer.
 * @param validity_bytemap_nelements The number of values within
 *     `validity_bytemap_nelements`
 **/
template <typename T>
TILEDB_DEPRECATED Query& set_buffer_nullable(
    const std::string& name,
    T* data,
    uint64_t data_nelements,
    uint8_t* validity_bytemap,
    uint64_t validity_bytemap_nelements) {
  // Checks
  auto is_attr = schema_.has_attribute(name);
  if (!is_attr)
    throw TileDBError(
        std::string("Cannot set buffer; Attribute '") + name +
        "' does not exist");
  else
    impl::type_check<T>(schema_.attribute(name).type());

  set_data_buffer(name, data, data_nelements);
  set_validity_buffer(name, validity_bytemap, validity_bytemap_nelements);
  return *this;
}

/**
 * Sets a buffer for a fixed-sized, nullable attribute.
 *
 * **Example:**
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
 * std::vector<int> data_a1 = {0, 1, 2, 3};
 * std::vector<uint8_t> validity_bytemap = {1, 1, 0, 1};
 * Query query(ctx, array);
 * query.set_buffer("a1", data_a1, validity_bytemap);
 * @endcode
 *
 * @tparam T Attribute value type
 * @param name Attribute name
 * @param buf Buffer vector with elements of the attribute/dimension type.
 * @param validity_bytemap Buffer vector with elements of the attribute
 *     validity values.
 **/
template <typename T>
TILEDB_DEPRECATED Query& set_buffer_nullable(
    const std::string& name,
    std::vector<T>& buf,
    std::vector<uint8_t>& validity_bytemap) {
  set_data_buffer(name, buf.data(), buf.size(), sizeof(T));
  set_validity_buffer(name, validity_bytemap.data(), validity_bytemap.size());
  return *this;
}

/**
 * Sets a buffer for a fixed-sized, nullable attribute.
 *
 * @note This unsafe version does not perform type checking; the given buffer
 * is assumed to be the correct type, and the size of an element in the given
 * buffer is assumed to be the size of the datatype of the attribute.
 *
 * @param name Attribute name
 * @param data Buffer array pointer with elements of the attribute type.
 * @param data_nelements Number of array elements in buffer
 * @param validity_bytemap The validity bytemap buffer.
 * @param validity_bytemap_nelements The number of values within
 *     `validity_bytemap_nelements`
 **/
TILEDB_DEPRECATED Query& set_buffer_nullable(
    const std::string& name,
    void* data,
    uint64_t data_nelements,
    uint8_t* validity_bytemap,
    uint64_t validity_bytemap_nelements) {
  // Checks
  auto is_attr = schema_.has_attribute(name);
  if (!is_attr)
    throw TileDBError(
        std::string("Cannot set buffer; Attribute '") + name +
        "' does not exist");

  // Compute element size (in bytes).
  size_t element_size = tiledb_datatype_size(schema_.attribute(name).type());

  set_data_buffer(name, data, data_nelements, element_size);
  set_validity_buffer(name, validity_bytemap, validity_bytemap_nelements);
  return *this;
}

/**
 * Sets a buffer for a variable-sized, nullable attribute.
 *
 * @note This unsafe version does not perform type checking; the given buffer
 * is assumed to be the correct type, and the size of an element in the given
 * buffer is assumed to be the size of the datatype of the attribute.
 *
 * @param name Attribute name
 * @param offsets Offsets array pointer where a new element begins in the data
 *        buffer.
 * @param offsets_nelements Number of elements in offsets buffer.
 * @param data Buffer array pointer with elements of the attribute type.
 * @param data_nelements Number of array elements in data buffer.
 * @param validity_bytemap The validity bytemap buffer.
 * @param validity_bytemap_nelements The number of values within
 *     `validity_bytemap_nelements`
 **/
TILEDB_DEPRECATED Query& set_buffer_nullable(
    const std::string& name,
    uint64_t* offsets,
    uint64_t offset_nelements,
    void* data,
    uint64_t data_nelements,
    uint8_t* validity_bytemap,
    uint64_t validity_bytemap_nelements) {
  // Checks
  auto is_attr = schema_.has_attribute(name);
  if (!is_attr)
    throw TileDBError(
        std::string("Cannot set buffer; Attribute '") + name +
        "' does not exist");

  // Compute element size (in bytes).
  auto type = schema_.attribute(name).type();
  size_t element_size = tiledb_datatype_size(type);

  set_data_buffer(name, data, data_nelements, element_size);
  set_offsets_buffer(name, offsets, offset_nelements);
  set_validity_buffer(name, validity_bytemap, validity_bytemap_nelements);
  return *this;
}

/**
 * Sets a buffer for a variable-sized, nullable attribute.
 *
 * **Example:**
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::Array array(ctx, array_name, TILEDB_WRITE);
 * std::vector<int> data_a1 = {0, 1, 2, 3};
 * std::vector<uint64_t> offsets_a1 = {0, 8};
 * std::vector<uint8_t> validity_bytemap = {1, 1, 0, 1};
 * Query query(ctx, array);
 * query.set_buffer("a1", offsets_a1, data_a1, validity_bytemap);
 * @endcode
 *
 * @tparam T Attribute value type
 * @param name Attribute name
 * @param offsets Offsets where a new element begins in the data buffer.
 * @param data Buffer vector with elements of the attribute type.
 *        For variable sized attributes, the buffer should be flattened. E.x.
 *        an attribute of type std::string should have a buffer Vec type of
 *        std::string, where the values of each cell are concatenated.
 * @param validity_bytemap Buffer vector with elements of the attribute
 *     validity values.
 **/
template <typename T>
TILEDB_DEPRECATED Query& set_buffer_nullable(
    const std::string& name,
    std::vector<uint64_t>& offsets,
    std::vector<T>& data,
    std::vector<uint8_t>& validity_bytemap) {
  // Checks
  auto is_attr = schema_.has_attribute(name);
  if (!is_attr)
    throw TileDBError(
        std::string("Cannot set buffer; Attribute '") + name +
        "' does not exist");
  else
    impl::type_check<T>(schema_.attribute(name).type());

  set_data_buffer(name, data.data(), data.size(), sizeof(T));
  set_offsets_buffer(name, offsets.data(), offsets.size());
  set_validity_buffer(name, validity_bytemap.data(), validity_bytemap.size());
  return *this;
}

/**
 * Sets a buffer for a variable-sized, nullable attribute.
 *
 * @tparam T Attribute value type
 * @param attr Attribute name
 * @param buf Pair of offset, data, validity bytemap buffers
 **/
template <typename T>
TILEDB_DEPRECATED Query& set_buffer_nullable(
    const std::string& name,
    std::tuple<std::vector<uint64_t>, std::vector<T>, std::vector<uint8_t>>&
        buf) {
  // Checks
  auto is_attr = schema_.has_attribute(name);
  if (!is_attr)
    throw TileDBError(
        std::string("Cannot set buffer; Attribute '") + name +
        "' does not exist");
  impl::type_check<T>(schema_.attribute(name).type());

  set_data_buffer(name, std::get<1>(buf));
  set_offsets_buffer(name, std::get<0>(buf));
  set_validity_buffer(name, std::get<2>(buf));
  return *this;
}

/**
 * Sets a buffer for a string-typed variable-sized, nullable attribute.
 *
 * @param name Attribute name
 * @param offsets Offsets where a new element begins in the data buffer.
 * @param data Pre-allocated string buffer.
 * @param validity_bytemap Buffer vector with elements of the attribute
 *     validity values.
 **/
TILEDB_DEPRECATED Query& set_buffer_nullable(
    const std::string& name,
    std::vector<uint64_t>& offsets,
    std::string& data,
    std::vector<uint8_t>& validity_bytemap) {
  // Checks
  auto is_attr = schema_.has_attribute(name);
  if (!is_attr)
    throw TileDBError(
        std::string("Cannot set buffer; Attribute '") + name +
        "' does not exist");
  else
    impl::type_check<char>(schema_.attribute(name).type());

  set_data_buffer(name, &data[0], data.size(), sizeof(char));
  set_offsets_buffer(name, offsets.data(), offsets.size());
  set_validity_buffer(name, validity_bytemap.data(), validity_bytemap.size());
  return *this;
}

/**
 * Gets a buffer for a fixed-sized attribute/dimension.
 *
 * @param name Attribute/dimension name
 * @param data Buffer array pointer with elements of the attribute type.
 * @param data_nelements Number of array elements.
 * @param element_size Size of array elements (in bytes).
 **/
TILEDB_DEPRECATED Query& get_buffer(
    const std::string& name,
    void** data,
    uint64_t* data_nelements,
    uint64_t* element_size) {
  auto ctx = ctx_.get();
  uint64_t* data_nbytes = nullptr;
  auto elem_size_iter = element_sizes_.find(name);
  if (elem_size_iter == element_sizes_.end()) {
    throw TileDBError(
        "[TileDB::C++API] Error: No buffer set for attribute '" + name + "'!");
  }

  ctx.handle_error(tiledb_query_get_data_buffer(
      ctx.ptr().get(), query_.get(), name.c_str(), data, &data_nbytes));

  assert(*data_nbytes % elem_size_iter->second == 0);

  *data_nelements = (*data_nbytes) / elem_size_iter->second;
  *element_size = elem_size_iter->second;

  return *this;
}

/**
 * Gets a buffer for a var-sized attribute/dimension.
 *
 * @param name Attribute/dimension name
 * @param offsets Offsets array pointer with elements of uint64_t type.
 * @param offsets_nelements Number of array elements.
 * @param data Buffer array pointer with elements of the attribute type.
 * @param data_nelements Number of array elements.
 * @param element_size Size of array elements (in bytes).
 **/
TILEDB_DEPRECATED Query& get_buffer(
    const std::string& name,
    uint64_t** offsets,
    uint64_t* offsets_nelements,
    void** data,
    uint64_t* data_nelements,
    uint64_t* element_size) {
  auto ctx = ctx_.get();
  uint64_t* offsets_nbytes = nullptr;
  uint64_t* data_nbytes = nullptr;
  auto elem_size_iter = element_sizes_.find(name);
  if (elem_size_iter == element_sizes_.end()) {
    throw TileDBError(
        "[TileDB::C++API] Error: No buffer set for attribute '" + name + "'!");
  }

  ctx.handle_error(tiledb_query_get_data_buffer(
      ctx.ptr().get(), query_.get(), name.c_str(), data, &data_nbytes));
  ctx.handle_error(tiledb_query_get_offsets_buffer(
      ctx.ptr().get(), query_.get(), name.c_str(), offsets, &offsets_nbytes));

  assert(*data_nbytes % elem_size_iter->second == 0);
  assert(*offsets_nbytes % sizeof(uint64_t) == 0);

  *data_nelements = (*data_nbytes) / elem_size_iter->second;
  *offsets_nelements = (*offsets_nbytes) / sizeof(uint64_t);
  *element_size = elem_size_iter->second;

  return *this;
}

/**
 * Gets a buffer for a fixed-sized, nullable attribute.
 *
 * @param name Attribute name
 * @param data Buffer array pointer with elements of the attribute type.
 * @param data_nelements Number of array elements.
 * @param data_element_size Size of array elements (in bytes).
 * @param validity_bytemap Buffer array pointer with elements of the
 *     attribute validity values.
 * @param validity_bytemap_nelements Number of validity bytemap elements.
 **/
TILEDB_DEPRECATED Query& get_buffer_nullable(
    const std::string& name,
    void** data,
    uint64_t* data_nelements,
    uint64_t* data_element_size,
    uint8_t** validity_bytemap,
    uint64_t* validity_bytemap_nelements) {
  auto ctx = ctx_.get();
  uint64_t* data_nbytes = nullptr;
  uint64_t* validity_bytemap_nbytes = nullptr;
  auto elem_size_iter = element_sizes_.find(name);
  if (elem_size_iter == element_sizes_.end()) {
    throw TileDBError(
        "[TileDB::C++API] Error: No buffer set for attribute '" + name + "'!");
  }

  ctx.handle_error(tiledb_query_get_data_buffer(
      ctx.ptr().get(), query_.get(), name.c_str(), data, &data_nbytes));
  ctx.handle_error(tiledb_query_get_validity_buffer(
      ctx.ptr().get(),
      query_.get(),
      name.c_str(),
      validity_bytemap,
      &validity_bytemap_nbytes));

  assert(*data_nbytes % elem_size_iter->second == 0);

  *data_nelements = *data_nbytes / elem_size_iter->second;
  *data_element_size = elem_size_iter->second;
  *validity_bytemap_nelements = *validity_bytemap_nbytes / sizeof(uint8_t);

  return *this;
}

/**
 * Gets a buffer for a var-sized, nullable attribute.
 *
 * @param name Attribute name
 * @param offsets Offsets array pointer with elements of uint64_t type.
 * @param offsets_nelements Number of array elements.
 * @param data Buffer array pointer with elements of the attribute type.
 * @param data_nelements Number of array elements.
 * @param element_size Size of array elements (in bytes).
 * @param validity_bytemap Buffer array pointer with elements of the
 *     attribute validity values.
 * @param validity_bytemap_nelements Number of validity bytemap elements.
 **/
TILEDB_DEPRECATED Query& get_buffer_nullable(
    const std::string& name,
    uint64_t** offsets,
    uint64_t* offsets_nelements,
    void** data,
    uint64_t* data_nelements,
    uint64_t* element_size,
    uint8_t** validity_bytemap,
    uint64_t* validity_bytemap_nelements) {
  auto ctx = ctx_.get();
  uint64_t* offsets_nbytes = nullptr;
  uint64_t* data_nbytes = nullptr;
  uint64_t* validity_bytemap_nbytes = nullptr;
  auto elem_size_iter = element_sizes_.find(name);
  if (elem_size_iter == element_sizes_.end()) {
    throw TileDBError(
        "[TileDB::C++API] Error: No buffer set for attribute '" + name + "'!");
  }

  ctx.handle_error(tiledb_query_get_data_buffer(
      ctx.ptr().get(), query_.get(), name.c_str(), data, &data_nbytes));
  ctx.handle_error(tiledb_query_get_offsets_buffer(
      ctx.ptr().get(), query_.get(), name.c_str(), offsets, &offsets_nbytes));
  ctx.handle_error(tiledb_query_get_validity_buffer(
      ctx.ptr().get(),
      query_.get(),
      name.c_str(),
      validity_bytemap,
      &validity_bytemap_nbytes));

  assert(*data_nbytes % elem_size_iter->second == 0);
  assert(*offsets_nbytes % sizeof(uint64_t) == 0);

  *data_nelements = (*data_nbytes) / elem_size_iter->second;
  *offsets_nelements = (*offsets_nbytes) / sizeof(uint64_t);
  *element_size = elem_size_iter->second;
  *validity_bytemap_nelements = *validity_bytemap_nbytes / sizeof(uint8_t);

  return *this;
}
