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

#include <functional>

namespace tdb {


  class Query {
  public:
    Query(Context &ctx, ArrayMetadata &meta, tiledb_query_type_t type);
    Query(Array &array, tiledb_query_type_t type=TILEDB_READ);
    Query(const Query&) = delete;
    Query(Query&& o) :_ctx(o._ctx), _array(o._array) {
      *this = std::move(o);
    }
    Query &operator=(const Query&) = delete;
    Query &operator=(Query&& o);
    ~Query();

    enum class Status {FAILED, COMPLETE, INPROGRESS, INCOMPLETE, UNDEF};

    /**
     * @tparam T should be a type from tdb::type::*
     * @param pair
     */
    template<typename T>
    Query &subarray(const std::vector<typename T::type> &pairs);

    Query &layout(tiledb_layout_t layout);

    Query &attributes(const std::vector<std::string> &attrs);

    template<typename T>
    Query &set_buffer(const std::string &attr, std::vector<typename T::type> &buf) {
      const auto &type = _array.get().attributes().at(attr).type();
      if (T::tiledb_datatype != type) {
        throw std::invalid_argument("Attempting to use buffer of type " + std::string(T::name) +
                                    " for attribute of type " + type::from_tiledb(type));
      }
      if (_array.get().attributes().at(attr).num() == TILEDB_VAR_NUM) {
        throw std::invalid_argument("Offsets must be provided for variable length attributes.");
      }
      _attr_buffs[attr] = std::make_tuple<uint64_t, size_t, void*>(buf.size(), sizeof(typename T::type), buf.data());
      return *this;
    }

    template<typename T>
    Query &set_buffer(const std::string &attr, std::vector<typename T::type> &buf, std::vector<uint64_t> &offsets) {
      const auto &type = _array.get().attributes().at(attr).type();
      if (T::tiledb_datatype != type) {
        throw std::invalid_argument("Attempting to use buffer of type " + std::string(T::name) +
                                    " for attribute of type " + type::from_tiledb(type));
      }
      if (_array.get().attributes().at(attr).num() != TILEDB_VAR_NUM) {
        throw std::invalid_argument("Offsets provided for non-variable length attributes.");
      }
      _var_offsets[attr] = std::make_tuple<uint64_t, size_t, void*>(offsets.size(), sizeof(uint64_t), offsets.data());
      _attr_buffs[attr] = std::make_tuple<uint64_t, size_t, void*>(buf.size(), sizeof(typename T::type), buf.data());
      return *this;
    }

    template<typename DataT, typename DomainT=type::UINT64>
    Query &resize_buffer(const std::string &attr, std::vector<typename DataT::type> &buff, unsigned max_el=0) {
      auto num = _array.get().attributes().at(attr).num();
      if (num == TILEDB_VAR_NUM) throw std::runtime_error("Use resize_var_buffer for variable size attributes.");
      _make_buffer_impl<DataT, DomainT>(attr, buff, 1, max_el);
      set_buffer<DataT>(attr, buff);
      return *this;
    }

    template<typename DataT, typename DomainT=type::UINT64>
    Query &resize_var_buffer(const std::string &attr, std::vector<uint64_t> &offsets,
                             std::vector<typename DataT::type> &buff, unsigned expected_size=1, unsigned max_el=0) {
      auto num = _array.get().attributes().at(attr).num();
      if (num != TILEDB_VAR_NUM) throw std::runtime_error("Use resize_buffer for fixed size attributes.");
      num = _make_buffer_impl<DataT, DomainT>(attr, buff, expected_size, max_el);
      std::cout << num << '\n';
      offsets.resize(num / expected_size);
      set_buffer<DataT>(attr, buff, offsets);
      return *this;
    }

    const std::vector<uint64_t> &buff_sizes() const {
      return _buff_sizes;
    }

    Status status();

    Status submit();

  private:
    void _prepare_buffers();

    template<typename DataT, typename DomainT=type::UINT64>
    unsigned _make_buffer_impl(const std::string &attr, std::vector<typename DataT::type> &buff,
                               unsigned expected_varnum=1, unsigned max_el=0) {
      auto num = _array.get().attributes().at(attr).num();
      if (num == TILEDB_VAR_NUM) num = expected_varnum;
      for (const auto& n : _array.get().domain().dimension_names()) {
        const auto &d = _array.get().domain().get_dimension(n).domain<DomainT>();
        num = num * (d.second - d.first + 1);
      }
      if (max_el != 0 && num > max_el) num = max_el;
      buff.resize(num);
      return num;
    }

    std::reference_wrapper<Context> _ctx;
    std::reference_wrapper<ArrayMetadata> _array;
    std::vector<std::string> _attrs;
    // Size of the vector, size of vector::value_type, vector.data()
    std::unordered_map<std::string, std::tuple<uint64_t, size_t, void*>> _var_offsets;
    std::unordered_map<std::string, std::tuple<uint64_t, size_t, void*>> _attr_buffs;

    std::vector<uint64_t> _sub_tsize; // Keeps track of vector value_type sizes to convert back at return
    std::vector<const char*> _attr_names;
    std::vector<void*> _all_buff;
    std::vector<uint64_t> _buff_sizes;
    tiledb_query_t *_query;

  };

}

std::ostream &operator<<(std::ostream &os, const tdb::Query::Status &stat);

#endif //TILEDB_TDBPP_QUERY_H
