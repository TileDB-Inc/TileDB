/**
 * @file   tdbpp_query.h
 *
 * @author Ravi Gaddipati
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
 * This file declares the C++ API for TileDB.
 */

#ifndef TILEDB_TDBPP_QUERY_H
#define TILEDB_TDBPP_QUERY_H

#include "tdbpp_context.h"
#include "tdbpp_array.h"
#include "tdbpp_type.h"
#include "tiledb.h"
#include "tdbpp_utils.h"

#include <functional>
#include <memory>
#include <set>
#include <iterator>
#include <type_traits>

namespace tdb {

  /**
   * Construct and execute read/write queries on a tdb::Array
   */
  class Query {
  public:
    enum class Status {FAILED, COMPLETE, INPROGRESS, INCOMPLETE, UNDEF};

    Query(ArrayMetadata &meta, tiledb_query_type_t type) : _ctx(meta.context()), _array(meta), _deleter(_ctx) {
      tiledb_query_t *q;
      _ctx.get().handle_error(tiledb_query_create(_ctx.get(), &q, meta.name().c_str(), type));
      _query = std::shared_ptr<tiledb_query_t>(q, _deleter);
      _array_attributes = _array.get().attributes();
    }
    /**
     * Create a new query bound to a particular array.
     * @param array
     * @param type Read or Write query.
     */
    Query(Array &array, tiledb_query_type_t type=TILEDB_READ) : Query(array.meta(), type) {}
    Query(const Query&) = default;
    Query(Query&& o) = default;
    Query &operator=(const Query&) = default;
    Query &operator=(Query&& o) = default;

    /**
     * Data layout of the buffers.
     * @param layout
     * @return *this
     */
    Query &layout(tiledb_layout_t layout);

    /**
     * Set the list of buffers to be provided.
     * @param attrs Attributes to query, plus TILEDB_COORDS if needed.
     * @return *this
     */
    Query &buffer_list(const std::vector<std::string> &attrs);

    /**
     * @tparam T should be a type from tdb::type::*
     * @param pair vector of pairs defining each dimensions [start,stop]. Inclusive.
     */
    template<typename T>
    Query &subarray(const std::vector<typename T::type> &pairs) {
      auto &ctx = _ctx.get();
      _type_check<T>(_array.get().domain().type());
      if (pairs.size() != _array.get().domain().size() * 2) {
        throw std::invalid_argument("Subarray should have num_dims * 2 values: (low, high) for each dimension.");
      }
      ctx.handle_error(tiledb_query_set_subarray(ctx, _query.get(), pairs.data(), T::tiledb_datatype));
      _subarray_cells = pairs[1] - pairs[0] + 1;
      for (unsigned i = 2; i < pairs.size() - 1; i+= 2) {
        _subarray_cells = _subarray_cells * (pairs[i+1] - pairs[i] + 1);
      }
      return *this;
    }

    template<typename T=uint64_t>
    typename std::enable_if<std::is_fundamental<T>::value, Query>::type
    &subarray(const std::vector<T> &pairs) {
      return subarray<typename type::type_from_native<T>::type>(pairs);
    };

    template<typename T>
    Query &subarray(const std::vector<std::array<typename T::type, 2>> &pairs) {
      auto &ctx = _ctx.get();
      ctx.handle_error(tiledb_query_set_subarray(ctx, _query.get(), pairs.data(), T::tiledb_datatype));
      _subarray_cells = pairs[0][1] - pairs[0][0] + 1;
      for (unsigned i = 1; i < pairs.size(); ++i) {
        _subarray_cells = _subarray_cells * (pairs[i][1] - pairs[i][0] + 1);
      }
      return *this;
    };

    template<typename T=uint64_t>
    typename std::enable_if<std::is_fundamental<T>::value, Query>::type
    &subarray(const std::vector<std::array<T, 2>> &pairs) {
      return subarray<typename type::type_from_native<T>::type>(pairs);
    };

    /**
     * Set a buffer for a particular attribute
     * @tparam T buffer type, tdb::type::*
     * @param attr Attribute name
     * @param buf data buffer
     * @return *this
     */
    template<typename T>
    Query &set_buffer(const std::string &attr, std::vector<typename T::type> &buf) {
      _type_check_attr<T>(attr, true);
      _attr_buffs[attr] = std::make_tuple<uint64_t, uint64_t, void*>(buf.size(), sizeof(typename T::type), buf.data());
      return *this;
    }

    template<typename T>
    typename std::enable_if<std::is_fundamental<T>::value, Query>::type
    &set_buffer(const std::string &attr, std::vector<T> &buf) {
      return set_buffer<typename type::type_from_native<T>::type>(attr, buf);
    };

    /**
     * Set a buffer for a particular attribute (variable size)
     * @tparam T buffer type, tdb::type::*
     * @param attr Attribute name
     * @param offsets list of offsets for the data buffer
     * @param buf data buffer
     * @return *this
     */
    template<typename T>
    Query &set_buffer(const std::string &attr, std::vector<uint64_t> &offsets, std::vector<typename T::type> &buf) {
      _type_check_attr<T>(attr, false);
      _var_offsets[attr] = std::make_tuple<uint64_t, uint64_t, void*>(offsets.size(), sizeof(uint64_t), offsets.data());
      _attr_buffs[attr] = std::make_tuple<uint64_t, uint64_t, void*>(buf.size(), sizeof(typename T::type), buf.data());
      return *this;
    }

    template<typename T>
    typename std::enable_if<std::is_fundamental<T>::value, Query>::type
    &set_buffer(const std::string &attr, std::vector<uint64_t> &offsets, std::vector<T> &buf) {
      return set_buffer<typename type::type_from_native<T>::type>(attr, offsets, buf);
    };

    /**
     * Set a buffer for a particular attribute (variable size)
     * @tparam T buffer type, tdb::type::*
     * @param attr Attribute name
     * @param buf buffer, a pair of offsets and data buffs
     * @return *this
     */
    template<typename T>
    Query &set_buffer(const std::string &attr, std::pair<std::vector<uint64_t>, std::vector<typename T::type>> &buf) {
      return set_buffer<T>(attr, buf.first, buf.second);
    }

    template<typename T>
    typename std::enable_if<std::is_fundamental<T>::value, Query>::type
    &set_buffer(const std::string &attr, std::pair<std::vector<uint64_t>, std::vector<T>> &buf) {
      return set_buffer<typename type::type_from_native<T>::type>(attr, buf);
    };

    /**
     * Resize a buffer for a particular attribute. Attempts to find an ideal buffer size.
     * @tparam DataT tdb::type::*
     * @tparam DomainT type of the dimensions, tdb::type::*
     * @param attr Attribute name
     * @param buff databuff to resize
     * @param max_el upper bound on buffer size, in number of elements
     * @return *this
     */
    template<typename DataT, typename DomainT=type::UINT64>
    Query &resize_buffer(const std::string &attr, std::vector<typename DataT::type> &buff, uint64_t max_el=0) {
      uint64_t num = 1;
      if (_array_attributes.count(attr)) {
        num = _array_attributes.at(attr).num();
        if (num == TILEDB_VAR_NUM) throw std::runtime_error("Offsets required for var size attribute.");
      } else if (_special_attributes.count(attr)) {
        if (attr == TILEDB_COORDS) num = 2;
      } else {
        throw std::out_of_range("Invalid attribute: " + attr);
      }

      _make_buffer_impl<DataT, DomainT>(attr, buff, num, max_el);
      return *this;
    }

    template<typename T, typename D=uint64_t>
    typename std::enable_if<std::is_fundamental<T>::value, Query>::type
    &resize_buffer(const std::string &attr, std::vector<T> &buff, uint64_t max_el=0) {
      return resize_buffer<typename type::type_from_native<T>::type, typename type::type_from_native<D>::type>(attr, buff, max_el);
    };


    /**
     * Resize a buffer for a particular (varsize) attribute. Attempts to find an ideal buffer size.
     * @tparam DataT tdb::type::*
     * @tparam DomainT type of the dimensions, tdb::type::*
     * @param attr Attribute name
     * @param offsets Offsets buffer
     * @param buff databuff to resize
     * @param expected_size Average expected size of the attribute
     * @param max_offset Max size for the offset buffer. Limits the number of cells read. If max_el is undefined,
     *  this will set max_el = max_offset*expected_size
     * @param max_el upper bound on buffer size, in number of elements.
     * @return *this
     */
    template<typename DataT, typename DomainT=type::UINT64>
    Query &resize_buffer(const std::string &attr, std::vector<uint64_t> &offsets,
                         std::vector<typename DataT::type> &buff,
                         uint64_t expected_size = 1, uint64_t max_offset = 0, uint64_t max_el = 0) {
      uint64_t num;
      if (_array_attributes.count(attr)) {
        num = _array_attributes.at(attr).num();
        if (num != TILEDB_VAR_NUM) throw std::runtime_error("Offsets provided for fixed size attribute.");
      } else if (!_special_attributes.count(attr)) {
        throw std::out_of_range("Invalid attribute: " + attr);
      }
      if (max_offset && !max_el) max_el = max_offset * expected_size;
      num = _make_buffer_impl<DataT, DomainT>(attr, buff, expected_size, max_el);
      uint64_t offset_size = num/expected_size;
      if (max_offset != 0 && offset_size > max_offset) offset_size = max_offset;
      offsets.resize(offset_size);
      return *this;
    }

    template<typename T, typename D=uint64_t>
    typename std::enable_if<std::is_fundamental<T>::value, Query>::type
    &resize_buffer(const std::string &attr, std::vector<uint64_t> &offsets,
                   std::vector<T> &buff, uint64_t expected_size = 1, uint64_t max_offset = 0, uint64_t max_el = 0) {
      return resize_buffer<typename type::type_from_native<T>::type,
                           typename type::type_from_native<D>::type>(attr, offsets, buff, expected_size, max_offset, max_el);
    };

    /**
     * Make a simple buffer for a fixed size attribute.
     * @tparam DataT tdb::type::*
     * @tparam DomainT tdb::type::*, underlying dimension type
     * @param attr attribute name
     * @param max_el upper bound on buffer size, number of elements
     * @return Buffer
     */
    template<typename DataT, typename DomainT=type::UINT64>
    std::vector<typename DataT::type> make_buffer(const std::string &attr, uint64_t max_el = 0) {
      std::vector<typename DataT::type> ret;
      resize_buffer<DataT, DomainT>(attr, ret, max_el);
      return ret;
    };

    template<typename T, typename D=uint64_t>
    typename std::enable_if<std::is_fundamental<T>::value, std::vector<T>>::type
    make_buffer(const std::string &attr, uint64_t max_el = 0) {
      return make_buffer<typename type::type_from_native<T>::type, typename type::type_from_native<D>::type>(attr, max_el);
    };


    /**
     * Make a pair of buffers for a variable sized attr
     * @tparam DataT tdb::type::*
     * @tparam DomainT tdb::type::*, underlying dimension type
     * @param attr attribute name
     * @param expected expected size of the attribute
     * @param max_offset upper bound on number of cells buffer can hold
     * @param max_el upper bound on data buffer
     * @return pair<offset buff,data buff>
     */
    template<typename DataT, typename DomainT=type::UINT64>
    std::pair<std::vector<uint64_t>, std::vector<typename DataT::type>>
    make_var_buffers(const std::string &attr, uint64_t expected = 1, uint64_t max_offset = 0, uint64_t max_el = 0) {
      std::vector<typename DataT::type> ret;
      std::vector<uint64_t> offsets;
      resize_buffer<DataT, DomainT>(attr, offsets, ret, expected, max_offset, max_el);
      return {offsets, ret};
    };

    template<typename T, typename D=uint64_t>
    typename std::enable_if<std::is_fundamental<T>::value, std::pair<std::vector<uint64_t>, std::vector<T>>>::type
    make_var_buffers(const std::string &attr, uint64_t expected = 1, uint64_t max_offset = 0, uint64_t max_el = 0) {
      return make_var_buffers<typename type::type_from_native<T>::type,
                              typename type::type_from_native<D>::type>(attr, expected, max_offset, max_el);
    };

    /**
     * Clear all attribute buffers.
     */
    void reset_buffers() {
      _attr_buffs.clear();
      _var_offsets.clear();
      _buff_sizes.clear();
      _all_buff.clear();
      _sub_tsize.clear();
    }

    static Status tiledb_to_status(const tiledb_query_status_t &status);

    /**
     * Get the status of the current query.
     * @return
     */
    Status query_status();

    /**
     * Get the query status of a particular attribute.
     * @param attr attribute name
     * @return Status
     */
    Status attribute_status(const std::string &attr);

    /**
     * Submit the query. Call will block until query is complete.
     * @return
     */
    Status submit();

    /**
     * Submit an async query.
     * @return
     */
    Status submit_async();

    /**
     * Submit an async query, with callback.
     * @param callback Callback function.
     * @param data data to pass to callback.
     * @return Status of submitted query.
     */
    Status submit_async(void* (*callback)(void*), void* data);

    /**
     * @return Returned buffer sizes, in number of elements.
     */
    std::vector<uint64_t> returned_buff_sizes();

  private:
    struct _Deleter {
      _Deleter(Context& ctx) : _ctx(ctx) {}
      _Deleter(const _Deleter&) = default;
      void operator()(tiledb_query_t *p);
    private:
      std::reference_wrapper<Context> _ctx;
    };

    /**
     * Collate buffers and attach them to the query.
     */
    void _prepare_submission();

    template<typename DataT>
    void _type_check(tiledb_datatype_t type) {
      if (DataT::tiledb_datatype != type) {
        throw std::invalid_argument("Attempting to use buffer of type " + std::string(DataT::name) +
                                    " for attribute of type " + type::from_tiledb(type));
      }
    }

    /**
     * Check if type matches the attribute and expected num
     * @tparam DataT Type attr should be, tdb::type::*
     * @param attr Attribute name
     * @param varcmp If we expect the attribute to be variable length
     */
    template<typename DataT>
    void _type_check_attr(const std::string &attr, bool varcmp) {
      if (_array_attributes.count(attr)) {
        // Type check if an attribute
        _type_check<DataT>(_array_attributes.at(attr).type());
        if (varcmp == (_array.get().attributes().at(attr).num() == TILEDB_VAR_NUM)) {
          throw std::invalid_argument("Offsets must be provided for variable length attributes.");
        }
      } else if (!_special_attributes.count(attr)) {
        throw std::invalid_argument("Invalid attribute: " + attr);
      }
    };

    /**
     * Gets the ideal buffer size using the underlying array dimensions and attr size.
     * @tparam DomainT
     * @param elements_per_cell
     * @return
     */
    template<typename DomainT>
    uint64_t _get_buffer_size(uint64_t elements_per_cell) {
      if (_subarray_cells != 0) {
        elements_per_cell = elements_per_cell * _subarray_cells;
      } else {
        for (const auto &dim : _array.get().domain().dimensions()) {
          const auto &d = dim.domain<DomainT>();
          elements_per_cell = elements_per_cell * (d.second - d.first + 1);
        }
      }
      return elements_per_cell;
    }

    /**
     * Computes the required buffer size to hold a query result.
     * @tparam DataT Datatype of attrbute, tdb::type::*
     * @tparam DomainT Datatype of Domain, tdb::type::*
     * @param attr Attribute name
     * @param buff Buffer to resize
     * @param num Number of elements per cell
     * @param max_el Upper bound on buffer size (# of elements)
     * @return Ideal buffer size. buff is resized to this, bound by max_el.
     */
    template<typename DataT, typename DomainT=type::UINT64>
    uint64_t _make_buffer_impl(const std::string &attr,
                               std::vector<typename DataT::type> &buff,
                               uint64_t num=1, uint64_t max_el=0) {
      tiledb_datatype_t type;
      if (attr == TILEDB_COORDS) type = _array.get().domain().type();
      else type = _array_attributes.at(attr).type();
      _type_check<DataT>(type);
      num = _get_buffer_size<DomainT>(num);
      buff.resize((max_el != 0 && num > max_el) ? max_el : num);
      return num;
    }

    // Special underlying attribute types to skip type checking for
    const std::set<std::string> _special_attributes{TILEDB_COORDS,};

    // On init get the attributes the underlying arraymetadata defines
    std::unordered_map<std::string, Attribute> _array_attributes;

    std::reference_wrapper<Context> _ctx;
    std::reference_wrapper<ArrayMetadata> _array;
    _Deleter _deleter;
    std::vector<std::string> _attrs;

    // Size of the vector, size of vector::value_type, vector.data()
    std::unordered_map<std::string, std::tuple<uint64_t, uint64_t, void*>> _var_offsets;
    std::unordered_map<std::string, std::tuple<uint64_t, uint64_t, void*>> _attr_buffs;

    // Vectors to compile buffers into the format C API expects
    std::vector<uint64_t> _sub_tsize; // Keeps track of vector value_type sizes to convert back at return
    std::vector<const char*> _attr_names;
    std::vector<void*> _all_buff;
    std::vector<uint64_t> _buff_sizes;
    uint64_t _subarray_cells = 0; // Number of cells set by subarray, influences resize_buffer
    std::shared_ptr<tiledb_query_t> _query;

  };

  /**
   * Covert an offset, data vector pair into a single vector of vectors.
   * @tparam T underlying datatype
   * @param offsets Offsets vector
   * @param buff data vector
   * @param num_offset num offset elements populated by query
   * @param num_buff num data elements populated by query.
   * @return std::vector<std::vector<T>>
   */
  template <typename T>
  std::vector<std::vector<T>>
  group_by_cell(const std::vector<uint64_t> &offsets, const std::vector<T> &buff,
                uint64_t num_offset, uint64_t num_buff) {
    std::vector<std::vector<T>> ret;
    ret.reserve(num_offset);
    for (unsigned i = 0; i < num_offset; ++i) {
      ret.emplace_back(buff.begin() + offsets[i], (i == num_offset - 1) ? buff.begin()+num_buff : buff.begin()+offsets[i+1]);
    }
    return ret;
  }

  /**
   * Covert an offset, data vector pair into a single vector of vectors.
   * @tparam T underlying datatype
   * @param buff pair<offset_vec, data_vec>
   * @param num_offset num offset elements populated by query
   * @param num_buff num data elements populated by query.
   * @return std::vector<std::vector<T>>
   */
  template <typename T>
  std::vector<std::vector<T>>
  group_by_cell(const std::pair<std::vector<uint64_t>, std::vector<T>> &buff,
                uint64_t num_offset, uint64_t num_buff) {
    return group_by_cell(buff.first, buff.second, num_offset, num_buff);
  }

  /**
   * Group by cell at runtime.
   * @tparam T Element type
   * @param buff data buffer
   * @param el_per_cell Number of elements per cell to group together
   * @param num_buff Number of elements populated by query. To group whole buffer, use buff.size()
   * @return
   */
  template<typename T>
  std::vector<std::vector<T>>
  group_by_cell(const std::vector<T> &buff, uint64_t el_per_cell, uint64_t num_buff) {
    std::vector<std::vector<T>> ret;
    if (buff.size() % el_per_cell != 0) {
      throw std::invalid_argument("Buffer is not a multiple of elements per cell.");
    }
    ret.reserve(buff.size()/el_per_cell);
    for (uint64_t i = 0; i < num_buff; i+= el_per_cell) {
      ret.emplace_back(buff.begin() + i, buff.begin() + i + el_per_cell);
    }
    return ret;
  }

  /**
   * Group a data vector into a a vector of arrays
   * @tparam N Elements per cell
   * @tparam T Array element type
   * @param buff data buff to group
   * @param num_buff Number of elements in buff that were populated by the query.
   * @return std::vector<std::array<T,N>>
   */
  template<uint64_t N, typename T> std::vector<std::array<T,N>>
  group_by_cell(const std::vector<T> &buff, uint64_t num_buff) {
    std::vector<std::array<T,N>> ret;
    if (buff.size() % N != 0) {
      throw std::invalid_argument("Buffer is not a multiple of elements per cell.");
    }
    ret.reserve(buff.size()/N);
    for (unsigned i = 0; i < num_buff; i+= N) {
      std::array<T,N> a;
      for (unsigned j = 0; j < N; ++j) {
        a[j] = buff[i+j];
      }
      ret.insert(ret.end(), std::move(a));
    }
    return ret;
  }


  /**
   * Unpack a vector of variable sized attributes into a data and offset buffer.
   * @tparam T Vector type. T::value_type is considered the underlying data element type. Should be vector or string.
   * @tparam R T::value_type, deduced
   * @param data data to unpack
   * @return pair where .first is the offset buffer, and .second is data buffer
   */
  template<typename T, typename R=typename T::value_type>
  std::pair<std::vector<uint64_t>, std::vector<R>>
  make_var_buffers(const std::vector<T> &data) {
    std::pair<std::vector<uint64_t>, std::vector<R>> ret;
    ret.first.push_back(0);
    for (const auto &v : data) {
      ret.second.insert(std::end(ret.second), std::begin(v), std::end(v));
      ret.first.push_back(ret.first.back() + v.size());
    }
    ret.first.pop_back();
    return ret;
  };


}

std::ostream &operator<<(std::ostream &os, const tdb::Query::Status &stat);

#endif //TILEDB_TDBPP_QUERY_H
