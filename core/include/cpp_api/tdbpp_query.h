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

namespace tdb {

  class Query {
  public:
    enum class Status {FAILED, COMPLETE, INPROGRESS, INCOMPLETE, UNDEF};

    Query(Context &ctx, ArrayMetadata &meta, tiledb_query_type_t type) : _ctx(ctx), _array(meta), _deleter(ctx) {
      tiledb_query_t *q;
      ctx.handle_error(tiledb_query_create(ctx, &q, meta.name().c_str(), type));
      _query = std::shared_ptr<tiledb_query_t>(q, _deleter);
    }
    Query(Array &array, tiledb_query_type_t type=TILEDB_READ) : Query(array.context(), array.meta(), type) {}
    Query(const Query&) = default;
    Query(Query&& o) = default;
    Query &operator=(const Query&) = default;
    Query &operator=(Query&& o) = default;

    Query &layout(tiledb_layout_t layout);

    Query &attributes(const std::vector<std::string> &attrs);

    /**
 * @tparam T should be a type from tdb::type::*
 * @param pair
 */
    template<typename T>
    Query &subarray(const std::vector<typename T::type> &pairs) {
      auto &ctx = _ctx.get();
      auto type = _array.get().domain().type();
      if (T::tiledb_datatype != type) {
        throw std::invalid_argument("Attempting to use subarray of type " + std::string(T::name) +
                                    " for domain of type " + type::from_tiledb(type));
      }
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
    Query &resize_buffer(const std::string &attr, std::vector<typename DataT::type> &buff, uint64_t max_el=0) {
      auto num = _array.get().attributes().at(attr).num();
      if (num == TILEDB_VAR_NUM) throw std::runtime_error("Use resize_var_buffer for variable size attributes.");
      _make_buffer_impl<DataT, DomainT>(attr, buff, 1, max_el);
      set_buffer<DataT>(attr, buff);
      return *this;
    }

    template<typename DataT, typename DomainT=type::UINT64>
    Query &resize_var_buffer(const std::string &attr, std::vector<uint64_t> &offsets, std::vector<typename DataT::type> &buff,
                             uint64_t expected_size=1, uint64_t max_offset=0, uint64_t max_el=0) {
      auto num = _array.get().attributes().at(attr).num();
      if (num != TILEDB_VAR_NUM) throw std::runtime_error("Use resize_buffer for fixed size attributes.");
      num = _make_buffer_impl<DataT, DomainT>(attr, buff, expected_size, max_el);
      uint64_t offset_size = num/expected_size;
      if (max_offset != 0 && offset_size > max_offset) offset_size = max_offset;
      offsets.resize(offset_size);
      set_buffer<DataT>(attr, buff, offsets);
      return *this;
    }

    static Status tiledb_to_status(const tiledb_query_status_t &status);

    Status query_status();

    Status attribute_status(const std::string &attr);

    Status submit();

    Status submit_async();

    Status submit_async(void* (*callback)(void*), void* data);

    std::vector<uint64_t> buff_sizes();

  private:
    struct _Deleter {
      _Deleter(Context& ctx) : _ctx(ctx) {}
      _Deleter(const _Deleter&) = default;
      void operator()(tiledb_query_t *p);
    private:
      std::reference_wrapper<Context> _ctx;
    };

    void _prepare_submission();


    template<typename DataT, typename DomainT=type::UINT64>
    unsigned _make_buffer_impl(const std::string &attr, std::vector<typename DataT::type> &buff,
                               uint64_t expected_varnum=1, uint64_t max_el=0) {
      auto num = _array.get().attributes().at(attr).num();
      if (num == TILEDB_VAR_NUM) num = expected_varnum;
      if (_subarray_cells != 0) {
        num = expected_varnum * _subarray_cells;
      } else {
        for (const auto &dim : _array.get().domain().dimensions()) {
          const auto &d = dim.domain<DomainT>();
          num = num * (d.second - d.first + 1);
        }
      }
      auto constrained = num;
      if (max_el != 0 && num > max_el) constrained = max_el;
      buff.resize(constrained);
      return num;
    }

    std::reference_wrapper<Context> _ctx;
    std::reference_wrapper<ArrayMetadata> _array;
    _Deleter _deleter;
    std::vector<std::string> _attrs;
    // Size of the vector, size of vector::value_type, vector.data()
    std::unordered_map<std::string, std::tuple<uint64_t, size_t, void*>> _var_offsets;
    std::unordered_map<std::string, std::tuple<uint64_t, size_t, void*>> _attr_buffs;

    std::vector<uint64_t> _sub_tsize; // Keeps track of vector value_type sizes to convert back at return
    std::vector<const char*> _attr_names;
    std::vector<void*> _all_buff;
    std::vector<uint64_t> _buff_sizes;
    unsigned _subarray_cells = 0;
    std::shared_ptr<tiledb_query_t> _query;

  };

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

  template<typename T>
  std::vector<std::vector<T>>
  group_by_cell(const std::vector<T> &buff, unsigned el_per_cell) {
    std::vector<std::vector<T>> ret;
    if (buff.size() % el_per_cell != 0) throw std::invalid_argument("Buffer is not a multiple of elements per cell.");
    ret.reserve(buff.size()/el_per_cell);
    for (unsigned i = 0; i < buff.size(); i+= el_per_cell) {
      ret.emplace_back(buff.begin() + i, buff.begin() + i + el_per_cell);
    }
    return ret;
  }

}

std::ostream &operator<<(std::ostream &os, const tdb::Query::Status &stat);

#endif //TILEDB_TDBPP_QUERY_H
