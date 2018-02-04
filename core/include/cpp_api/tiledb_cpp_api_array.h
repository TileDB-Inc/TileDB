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
 * This file declares the C++ API for the TileDB arrays.
 */

#ifndef TILEDB_CPP_API_ARRAY_H
#define TILEDB_CPP_API_ARRAY_H

#include "tiledb.h"
#include "tiledb_cpp_api_array_schema.h"
#include "tiledb_cpp_api_context.h"
#include "tiledb_cpp_api_type.h"

#include <unordered_map>
#include <vector>

namespace tdb {
namespace Array {

/**
 * Consolidates the fragments of an array.
 *
 * @param ctx The TileDB context.
 * @param uri The URI of the array.
 */
void consolidate(const Context &ctx, const std::string &uri);

/**
 * Creates an array on disk from a schema definition.
 *
 * @param ctx The TileDB context.
 * @param uri The URI of the array.
 * @param schema The array schema.
 */
void create(const std::string &uri, const ArraySchema &schema);

/**
 * Get the non-empty domain of an array. This returns the bounding dims
 * for each dimension.
 *
 * @tparam T Dimension type
 * @param uri array name
 * @param schema array schema
 * @return Map of dim names to a {lower, upper} pair. Inclusive.
 *         Empty map if the array has no data.
 */
template <typename T, typename DataT = typename impl::type_from_native<T>::type>
std::unordered_map<std::string, std::pair<T, T>> non_empty_domain(
    const std::string &uri, const ArraySchema &schema) {
  impl::type_check<DataT>(schema.domain().type());

  std::unordered_map<std::string, std::pair<T, T>> ret;

  auto dims = schema.domain().dimensions();
  std::vector<T> buf(dims.size() * 2);
  auto &ctx = schema.context();
  int empty;
  ctx.handle_error(
      tiledb_array_get_non_empty_domain(ctx, uri.c_str(), buf.data(), &empty));
  if (empty)
    return ret;

  for (size_t i = 0; i < dims.size(); ++i) {
    ret[dims[i].name()] = std::pair<T, T>(buf[i * 2], buf[(i * 2) + 1]);
  }

  return ret;
}

/**
 * Compute an upper bound on buffer sizes to read a subarray.
 * @tparam T Dimension datatype
 * @param uri array name
 * @param schema array schema
 * @param subarray Subarray to compute sizes for
 * @return The buffer sizes. Note that one buffer size corresponds
 *         to a fixed-sized attributes, and two buffer sizes for a
 *         variable-sized attribute (the first is the size of the
 *         offsets, whereas the second is the size of the actual
 *         variable-sized cell values.
 */
template <typename T, typename DataT = typename impl::type_from_native<T>::type>
std::vector<uint64_t> upper_bound_buffer_sizes(
    const std::string &uri,
    const ArraySchema &schema,
    const std::vector<T> &subarray) {
  impl::type_check<DataT>(schema.domain().type());

  std::vector<uint64_t> ret;
  auto attrs = schema.attributes();
  std::vector<const char *> names;

  unsigned nbuffs = 0;
  for (const auto &a : attrs) {
    nbuffs += a.second.cell_val_num() == TILEDB_VAR_NUM ? 2 : 1;
    names.push_back(a.first.c_str());
  }

  ret.resize(nbuffs);
  auto &ctx = schema.context();
  ctx.handle_error(tiledb_array_compute_max_read_buffer_sizes(
      ctx,
      uri.c_str(),
      subarray.data(),
      names.data(),
      (unsigned)attrs.size(),
      ret.data()));
  return ret;
}

}  // namespace Array
}  // namespace tdb

#endif  // TILEDB_CPP_API_ARRAY_H
