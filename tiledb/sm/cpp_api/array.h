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
class TILEDB_EXPORT Array {
 public:
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
  static void consolidate(const Context& ctx, const std::string& uri);

  /** Creates an array on persistent storage from a schema definition. **/
  static void create(const std::string& uri, const ArraySchema& schema);

  /**
   * Get the non-empty domain of an array. This returns the bounding
   * coordinates for each dimension.
   *
   * @tparam T Domain datatype
   * @param uri array name
   * @param schema array schema
   * @return Vector of dim names with a {lower, upper} pair. Inclusive.
   *         Empty vector if the array has no data.
   */
  template <typename T>
  static std::vector<std::pair<std::string, std::pair<T, T>>> non_empty_domain(
      const std::string& uri, const ArraySchema& schema) {
    impl::type_check<T>(schema.domain().type());

    std::vector<std::pair<std::string, std::pair<T, T>>> ret;

    auto dims = schema.domain().dimensions();
    std::vector<T> buf(dims.size() * 2);
    auto& ctx = schema.context();
    int empty;
    ctx.handle_error(tiledb_array_get_non_empty_domain(
        ctx, uri.c_str(), buf.data(), &empty));
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
   * Get the non-empty domain of an array. This returns the bounding
   * coordinates for each dimension.
   *
   * @tparam T domain datatype
   * @param ctx The TileDB context.
   * @param uri Array name.
   * @return Vector of dim names with a {lower, upper} pair. Inclusive.
   *         Empty vector if the array has no data.
   */
  template <typename T>
  static std::vector<std::pair<std::string, std::pair<T, T>>> non_empty_domain(
      const Context& ctx, const std::string& uri) {
    ArraySchema schema(ctx, uri);
    return non_empty_domain<T>(uri, schema);
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
  static std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
  max_buffer_elements(
      const std::string& uri,
      const ArraySchema& schema,
      const std::vector<T>& subarray) {
    auto ctx = schema.context();
    impl::type_check<T>(schema.domain().type(), 1);

    std::vector<uint64_t> sizes;
    auto attrs = schema.attributes();
    std::vector<const char*> names;

    unsigned nbuffs = 0, attr_num = 0;
    for (const auto& a : attrs) {
      nbuffs += a.second.cell_val_num() == TILEDB_VAR_NUM ? 2 : 1;
      ++attr_num;
      names.push_back(a.first.c_str());
    }
    if (schema.array_type() == TILEDB_SPARSE) {
      ++nbuffs;
      ++attr_num;
      names.push_back(TILEDB_COORDS);
    }

    sizes.resize(nbuffs);
    ctx.handle_error(tiledb_array_compute_max_read_buffer_sizes(
        ctx,
        uri.c_str(),
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

    if (schema.array_type() == TILEDB_SPARSE)
      ret[TILEDB_COORDS] = std::pair<uint64_t, uint64_t>(
          0, sizes.back() / tiledb_datatype_size(schema.domain().type()));

    return ret;
  }

  /**
   * Compute an upper bound on the buffer elements needed to read a subarray.
   *
   * @tparam T The domain datatype
   * @param ctx The TileDB context
   * @param uri The array URI.
   * @param subarray Targeted subarray.
   * @return The maximum number of elements for each array attribute (plus
   *     coordinates for sparse arrays).
   *     Note that two numbers are returned per attribute. The first
   *     is the maximum number of elements in the offset buffer
   *     (0 for fixed-sized attributes and coordinates),
   *     and the second is the maximum number of elements of the value buffer.
   */
  template <typename T>
  static std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
  max_buffer_elements(
      const Context& ctx,
      const std::string& uri,
      const std::vector<T>& subarray) {
    ArraySchema schema(ctx, uri);
    return max_buffer_elements<T>(uri, schema, subarray);
  }
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_ARRAY_H
