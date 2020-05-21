/**
 * @file   tiledb_cpp_api_utils.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * Convert an (offset, data) vector pair into a single vector of vectors. Useful
 * for "unpacking" variable-length attribute data from a read query result in
 * offsets + data form to a vector of per-cell data.
 *
 * The offsets must be given in units of bytes.
 *
 * **Example:**
 * @code{.cpp}
 * std::vector<uint64_t> offsets;
 * std::vector<char> data;
 * ...
 * query.set_buffer("attr_name", offsets, data);
 * query.submit();
 * ...
 * auto attr_results = query.result_buffer_elements()["attr_name"];
 *
 * // cell_vals length will be equal to the number of cells read by the query.
 * // Each element is a std::vector<char> with each cell's data for "attr_name"
 * auto cell_vals =
 *   group_by_cell(offsets, data, attr_results.first, attr_results.second);
 *
 * // Reconstruct a std::string value for the first cell:
 * std::string cell_val(cell_vals[0].data(), cell_vals[0].size());
 * @endcode
 *
 * @note This function, and the other utility functions, copy all of the input
 * data when constructing their return values. Thus, these may be expensive
 * for large amounts of data.
 *
 * @tparam T Underlying attribute datatype
 * @tparam E Cell type. usually ``std::vector<T>`` or ``std::string``. Must be
 *         constructable by ``{std::vector<T>::iterator,
 *         std::vector<T>::iterator}``
 * @param offsets Offsets vector. This specifies the start offset in bytes
 *        of each cell in the data vector.
 * @param data Data vector. Flat data buffer with cell contents.
 * @param num_offsets Number of offset elements populated by query. If the
 *        entire buffer is to be grouped, pass ``offsets.size()``.
 * @param num_data Number of data elements populated by query. If the
 *        entire buffer is to be grouped, pass ``data.size()``.
 * @return ``std::vector<E>``
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
        data.begin() + (offsets[i] / sizeof(T)),
        (i == num_offsets - 1) ? data.begin() + num_data :
                                 data.begin() + (offsets[i + 1] / sizeof(T)));
  }
  return ret;
}

/**
 * Convert an (offset, data) vector pair into a single vector of vectors. Useful
 * for "unpacking" variable-length attribute data from a read query result in
 * offsets + data form to a vector of per-cell data.
 *
 * The offsets must be given in units of bytes.
 *
 * **Example:**
 * @code{.cpp}
 * std::vector<uint64_t> offsets;
 * std::vector<char> data;
 * ...
 * query.set_buffer("attr_name", offsets, data);
 * query.submit();
 * ...
 * auto attr_results = query.result_buffer_elements()["attr_name"];
 *
 * // cell_vals length will be equal to the number of cells read by the query.
 * // Each element is a std::vector<char> with each cell's data for "attr_name"
 * auto cell_vals =
 *   group_by_cell(std::make_pair(offsets, data),
 *                 attr_results.first, attr_results.second);
 *
 * // Reconstruct a std::string value for the first cell:
 * std::string cell_val(cell_vals[0].data(), cell_vals[0].size());
 * @endcode
 *
 * @tparam T Underlying attribute datatype
 * @tparam E Cell type. usually ``std::vector<T>`` or ``std::string``. Must be
 *         constructable by ``{std::vector<T>::iterator,
 * std::vector<T>::iterator}``
 * @param buff Pair of (offset_vec, data_vec) to be grouped.
 * @param num_offsets Number of offset elements populated by query.
 * @param num_data Number of data elements populated by query.
 * @return ``std::vector<E>``
 */
template <typename T, typename E = typename std::vector<T>>
std::vector<E> group_by_cell(
    const std::pair<std::vector<uint64_t>, std::vector<T>>& buff,
    uint64_t num_offsets,
    uint64_t num_data) {
  return group_by_cell<T, E>(buff.first, buff.second, num_offsets, num_data);
}

/**
 * Convert a generic (offset, data) vector pair into a single vector of vectors.
 * The offsets must be given in units of bytes.
 *
 * **Example:**
 * @code{.cpp}
 * std::vector<char> buf = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'};
 * std::vector<uint64_t> offsets = {0, 5};
 * auto grouped = group_by_cell<char, std::string>(offsets, buf);
 * // grouped.size() == 2
 * // grouped[0] == "abcde"
 * // grouped[1] == "fghi"
 * @endcode
 *
 * @tparam T Underlying attribute datatype
 * @tparam E Cell type. usually ``std::vector<T>`` or ``std::string``. Must be
 *         constructable by ``{std::vector<T>::iterator,
 * std::vector<T>::iterator}``
 * @param offsets Offsets vector
 * @param data Data vector
 * @return ``std::vector<E>``
 */
template <typename T, typename E = typename std::vector<T>>
std::vector<E> group_by_cell(
    const std::vector<uint64_t>& offsets, const std::vector<T>& data) {
  return group_by_cell<T, E>(offsets, data, offsets.size(), data.size());
}

/**
 * Convert a vector of elements into a vector of fixed-length vectors.
 *
 * **Example:**
 * @code{.cpp}
 * std::vector<char> buf = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'};
 * auto grouped = group_by_cell(buf, 3, buf.size());
 * std::string grp1(grouped[0].begin(), grouped[0].end());  // "abc"
 * std::string grp2(grouped[1].begin(), grouped[1].end());  // "def"
 * std::string grp3(grouped[2].begin(), grouped[2].end());  // "ghi"
 *
 * // Throws an exception because buf.size() is not divisible by 2:
 * // group_by_cell(buf, 2, buf.size());
 * @endcode
 *
 * @tparam T Underlying attribute datatype
 * @tparam E Cell type. usually ``std::vector<T>`` or ``std::string``. Must be
 *         constructable by ``{std::vector<T>::iterator,
 * std::vector<T>::iterator}``
 * @param buff Data buffer to group
 * @param el_per_cell Number of elements per cell to group together
 * @param num_buff Number of elements populated by query. To group whole buffer,
 *     pass ``buff.size()``.
 * @return ``std::vector<E>``
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
 * Convert a vector of elements into a vector of fixed-length vectors.
 *
 * **Example:**
 * @code{.cpp}
 * std::vector<char> buf = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'};
 * auto grouped = group_by_cell(buf, 3);
 * std::string grp1(grouped[0].begin(), grouped[0].end());  // "abc"
 * std::string grp2(grouped[1].begin(), grouped[1].end());  // "def"
 * std::string grp3(grouped[2].begin(), grouped[2].end());  // "ghi"
 *
 * // Throws an exception because buf.size() is not divisible by 2:
 * // group_by_cell(buf, 2);
 * @endcode
 *
 * @tparam T Element type
 * @tparam E Cell type. usually ``std::vector<T>`` or ``std::string``. Must be
 *         constructable by ``{std::vector<T>::iterator,
 * std::vector<T>::iterator}``
 * @param buff Data buffer to group
 * @param el_per_cell Number of elements per cell to group together
 * @return ``std::vector<E>``
 */
template <typename T, typename E = typename std::vector<T>>
std::vector<E> group_by_cell(const std::vector<T>& buff, uint64_t el_per_cell) {
  return group_by_cell<T, E>(buff, el_per_cell, buff.size());
}

/**
 * Convert a vector of elements into a vector of fixed-length arrays.
 *
 * **Example:**
 * @code{.cpp}
 * std::vector<char> buf = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'};
 * auto grouped = group_by_cell<3>(buf, buf.size());
 * std::string grp1(grouped[0].begin(), grouped[0].end());  // "abc"
 * std::string grp2(grouped[1].begin(), grouped[1].end());  // "def"
 * std::string grp3(grouped[2].begin(), grouped[2].end());  // "ghi"
 *
 * // Throws an exception because buf.size() is not divisible by 2:
 * // group_by_cell<2>(buf, buf.size());
 * @endcode
 *
 * @tparam N Elements per cell
 * @tparam T Array element type
 * @param buff Data buffer to group
 * @param num_buff Number of elements in buff that were populated by the query.
 * @return ``std::vector<std::array<T,N>>``
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
 * Convert a vector of elements into a vector of fixed-length arrays.
 *
 * **Example:**
 * @code{.cpp}
 * std::vector<char> buf = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'};
 * auto grouped = group_by_cell<3>(buf);
 * std::string grp1(grouped[0].begin(), grouped[0].end());  // "abc"
 * std::string grp2(grouped[1].begin(), grouped[1].end());  // "def"
 * std::string grp3(grouped[2].begin(), grouped[2].end());  // "ghi"
 *
 * // Throws an exception because buf.size() is not divisible by 2:
 * // group_by_cell<2>(buf);
 * @endcode
 *
 * @tparam N Elements per cell
 * @tparam T Array element type
 * @param buff data buff to group
 * @return ``std::vector<std::array<T,N>>``
 */
template <uint64_t N, typename T>
std::vector<std::array<T, N>> group_by_cell(const std::vector<T>& buff) {
  return group_by_cell<N, T>(buff, buff.size());
}

/**
 * Unpack a vector of variable sized attributes into a data and offset buffer.
 * The offset buffer result is in units of bytes.
 *
 * **Example:**
 * @code{.cpp}
 * std::vector<char> buf = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'};
 * // For the sake of example, group buf into groups of 3 elements:
 * auto grouped = group_by_cell(buf, 3);
 * // Ungroup into offsets, data pair.
 * auto p = ungroup_var_buffer(grouped);
 * auto offsets = p.first;  // {0, 3, 6}
 * auto data = p.second;   // {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'}
 * @endcode
 *
 * @tparam T Vector type. ``T::value_type`` is considered the underlying data
 * element type. Should be vector or string.
 * @tparam R ``T::value_type``, deduced
 * @param data Data buffer to unpack
 * @return pair where ``.first`` is the offset buffer, and ``.second`` is data
 * buffer
 */

template <typename T, typename R = typename T::value_type>
std::pair<std::vector<uint64_t>, std::vector<R>> ungroup_var_buffer(
    const std::vector<T>& data) {
  std::pair<std::vector<uint64_t>, std::vector<R>> ret;
  ret.first.push_back(0);
  for (const auto& v : data) {
    ret.second.insert(std::end(ret.second), std::begin(v), std::end(v));
    ret.first.push_back(ret.first.back() + v.size() * sizeof(R));
  }
  ret.first.pop_back();
  return ret;
}

/**
 * Convert a vector-of-vectors and flatten it into a single vector.
 *
 * **Example:**
 * @code{.cpp}
 * std::vector<std::string> v = {"a", "bb", "ccc"};
 * auto flat_v = flatten(v);
 * std::string s(flat_v.begin(), flat_v.end()); // "abbccc"
 *
 * std::vector<std::vector<double>> d = {{1.2, 2.1}, {2.3, 3.2}, {3.4, 4.3}};
 * auto flat_d = flatten(d);  // {1.2, 2.1, 2.3, 3.2, 3.4, 4.3};
 * @endcode
 *
 * @tparam V Container type
 * @tparam T Return element type
 * @param vec Vector to flatten
 * @return ``std::vector<T>``
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
}

namespace impl {

/** Check an error, free, and throw if there is one. **/
inline void check_config_error(tiledb_error_t* err) {
  if (err != nullptr) {
    const char* msg_cstr;
    tiledb_error_message(err, &msg_cstr);
    std::string msg = "Config Error: " + std::string(msg_cstr);
    tiledb_error_free(&err);
    throw TileDBError(msg);
  }
}

}  // namespace impl

}  // namespace tiledb

#endif  // TILEDB_CPP_API_UTILS_H
