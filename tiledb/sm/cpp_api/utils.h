/**
 * @file   tiledb_cpp_api_utils.h
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
 * Utils for C++ API.
 */

#ifndef TILEDB_CPP_API_UTILS_H
#define TILEDB_CPP_API_UTILS_H

#include "exception.h"
#include "tiledb.h"

#include <algorithm>
#include <array>
#include <functional>
#include <iostream>

namespace tiledb {

/**
 * Convert an (offset, data) vector pair into a single vector of vectors.
 *
 * @tparam T underlying attribute datatype
 * @tparam E Cell type. usually std::vector<T> or std::string. Must be
 *         constructable by {std::vector<T>::iterator, std::vector<T>::iterator}
 * @param offsets Offsets vector. This specifies the start offset of each
 *        cell in the data vector.
 * @param data Data vector. Flat data buffer will cell contents.
 * @param num_offsets Number of offset elements populated by query. If the
 *        entire buffer is to be grouped, use offsets.size().
 * @param num_data Number of data elements populated by query. If the
 *        entire buffer is to be grouped, use data.size().
 * @return std::vector<E>
 */
template <typename T, typename E = typename std::vector<T>>
std::vector<E> group_by_cell(
    const std::vector<uint64_t>& offsets,
    const std::vector<T>& data,
    uint64_t num_offsets,
    uint64_t num_data) {
  std::vector<E> ret;
  ret.reserve(num_offsets);
  for (unsigned i = 0; i < num_offsets; ++i) {
    ret.emplace_back(
        data.begin() + offsets[i],
        (i == num_offsets - 1) ? data.begin() + num_data :
                                 data.begin() + offsets[i + 1]);
  }
  return ret;
}

/**
 * Convert an (offset, data) vector pair into a single vector of vectors.
 * Uses whole vectors.
 *
 * @tparam T underlying attribute datatype
 * @tparam E Cell type. usually std::vector<T> or std::string. Must be
 *         constructable by {std::vector<T>::iterator, std::vector<T>::iterator}
 * @param offsets Offsets vector
 * @param data Data vector
 * @return std::vector<E>
 */
template <typename T, typename E = typename std::vector<T>>
std::vector<E> group_by_cell(
    const std::vector<uint64_t>& offsets, const std::vector<T>& data) {
  return group_by_cell<T, E>(offsets, data, offsets.size(), data.size());
}

/**
 * Convert an (offset, data) vector pair into a single vector of vectors.
 *
 * @tparam T underlying attribute datatype
 * @tparam E Cell type. usually std::vector<T> or std::string. Must be
 *         constructable by {std::vector<T>::iterator, std::vector<T>::iterator}
 * @param buff pair<offset_vec, data_vec>
 * @param num_offsets Number of offset elements populated by query
 * @param num_data Number of data elements populated by query.
 * @return std::vector<E>
 */
template <typename T, typename E = typename std::vector<T>>
std::vector<E> group_by_cell(
    const std::pair<std::vector<uint64_t>, std::vector<T>>& buff,
    uint64_t num_offsets,
    uint64_t num_data) {
  return group_by_cell<T, E>(buff.first, buff.second, num_offsets, num_data);
}

/**
 * Group by cell at runtime.
 *
 * @tparam T underlying attribute datatype
 * @tparam E Cell type. usually std::vector<T> or std::string. Must be
 *         constructable by {std::vector<T>::iterator, std::vector<T>::iterator}
 * @param buff data buffer
 * @param el_per_cell Number of elements per cell to group together
 * @param num_buff Number of elements populated by query. To group whole buffer,
 *     use buff.size()
 * @return std::vector<E>
 */
template <typename T, typename E = typename std::vector<T>>
std::vector<E> group_by_cell(
    const std::vector<T>& buff, uint64_t el_per_cell, uint64_t num_buff) {
  std::vector<E> ret;
  if (buff.size() % el_per_cell != 0) {
    throw std::invalid_argument(
        "Buffer is not a multiple of elements per cell.");
  }
  ret.reserve(buff.size() / el_per_cell);
  for (uint64_t i = 0; i < num_buff; i += el_per_cell) {
    ret.emplace_back(buff.begin() + i, buff.begin() + i + el_per_cell);
  }
  return ret;
}

/**
 * Group by cell at runtime. Uses whole data buffer.
 *
 * @tparam T Element type
 * @tparam E Cell type. usually std::vector<T> or std::string. Must be
 *         constructable by {std::vector<T>::iterator, std::vector<T>::iterator}
 * @param buff data buffer
 * @param el_per_cell Number of elements per cell to group together
 * @return std::vector<E>
 */
template <typename T, typename E = typename std::vector<T>>
std::vector<E> group_by_cell(const std::vector<T>& buff, uint64_t el_per_cell) {
  return group_by_cell<T, E>(buff, el_per_cell, buff.size());
}

/**
 * Group a data vector into a a vector of arrays
 *
 * @tparam N Elements per cell
 * @tparam T Array element type
 * @param buff data buff to group
 * @param num_buff Number of elements in buff that were populated by the query.
 * @return std::vector<std::array<T,N>>
 */
template <uint64_t N, typename T>
std::vector<std::array<T, N>> group_by_cell(
    const std::vector<T>& buff, uint64_t num_buff) {
  std::vector<std::array<T, N>> ret;
  if (buff.size() % N != 0) {
    throw std::invalid_argument(
        "Buffer is not a multiple of elements per cell.");
  }
  ret.reserve(buff.size() / N);
  for (unsigned i = 0; i < num_buff; i += N) {
    std::array<T, N> a;
    for (unsigned j = 0; j < N; ++j) {
      a[j] = buff[i + j];
    }
    ret.insert(ret.end(), std::move(a));
  }
  return ret;
}

/**
 * Group a data vector into a a vector of arrays.
 * Uses whole data buffer.
 *
 * @tparam N Elements per cell
 * @tparam T Array element type
 * @param buff data buff to group
 * @return std::vector<std::array<T,N>>
 */
template <uint64_t N, typename T>
std::vector<std::array<T, N>> group_by_cell(const std::vector<T>& buff) {
  return group_by_cell<N, T>(buff, buff.size());
}

/**
 * Unpack a vector of variable sized attributes into a data and offset buffer.
 *
 * @tparam T Vector type. T::value_type is considered the underlying data
 * element type. Should be vector or string.
 * @tparam R T::value_type, deduced
 * @param data data to unpack
 * @return pair where .first is the offset buffer, and .second is data buffer
 */

template <typename T, typename R = typename T::value_type>
std::pair<std::vector<uint64_t>, std::vector<R>> ungroup_var_buffer(
    const std::vector<T>& data) {
  std::pair<std::vector<uint64_t>, std::vector<R>> ret;
  ret.first.push_back(0);
  for (const auto& v : data) {
    ret.second.insert(std::end(ret.second), std::begin(v), std::end(v));
    ret.first.push_back(ret.first.back() + v.size());
  }
  ret.first.pop_back();
  return ret;
}

/**
 * Take a vector-of-vectors and flatten it into a single vector.
 * @tparam V Container type
 * @tparam T Return element type
 * @param vec
 * @return std::vector<T>
 */
template <typename V, typename T = typename V::value_type::value_type>
std::vector<T> flatten(const V& vec) {
  std::vector<T> ret;

  size_t size = 0;
  std::for_each(
      std::begin(vec), std::end(vec), [&size](const typename V::value_type& i) {
        size += i.size();
      });
  ret.reserve(size);

  std::for_each(
      std::begin(vec), std::end(vec), [&ret](const typename V::value_type& i) {
        std::copy(std::begin(i), std::end(i), std::back_inserter(ret));
      });
  return ret;
};

namespace impl {

/** Check an error, free, and throw if there is one. **/
inline void check_config_error(tiledb_error_t* err) {
  if (err != nullptr) {
    const char* msg;
    tiledb_error_message(err, &msg);
    tiledb_error_free(&err);
    throw TileDBError("Config Iterator Error: " + std::string(msg));
  }
}

}  // namespace impl

}  // namespace tiledb

#endif  // TILEDB_CPP_API_UTILS_H
