/**
 * @file   tdbpp_buffer.h
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
 */

#ifndef TILEDB_TDBPP_BUFFER_H
#define TILEDB_TDBPP_BUFFER_H

#include "tiledb.h"
#include "tdbpp_type.h"

#include <vector>

namespace tdb {
  template <typename T, Offset=uint64_t, typename Allocator=std::allacator<T>>
  class Buffer : private std::vector<typename T::type, Allocator> {
  public:
    using NativeT = typename T::type;
    using BaseVector = std::vector<NativeT, Allocator>;

    tiledb_datatype_t tiledb_datatype() const {
      return T::tiledb_datatype;
    }
    std::string datatype_name() const {
      return T::name;
    }
    unsigned int num() const {
      return _num;
    }
    void set_num(unsigned int num) {
      _num = num;
    }
    void lock() { _lock = true; }
    void unlock() {_lock = false; }
    bool locked() const { return _lock; }

    reference operator[](size_type pos) {
      if (_lock) throw std::runtime_error("Cannot access data when buffer is locked.");
      else return BaseVector::operator[](pos);
    }

    const_reference operator[](size_type pos) const {
      if (_lock) throw std::runtime_error("Cannot access data when buffer is locked.");
      else return BaseVector::operator[](pos);
    }

    reference at(size_type pos) {
      if (_lock) throw std::runtime_error("Cannot access data when buffer is locked.");
      else return BaseVector::at(pos);
    }

    const_reference at(size_type pos) const {
      if (_lock) throw std::runtime_error("Cannot access data when buffer is locked.");
      else return BaseVector::at(pos);
    }

    void push_back(const NativeT &value) {
      if (_lock) throw std::runtime_error("Cannot access data when buffer is locked.");
      else return BaseVector::push_back(value);
    }

    void push_back(NativeT &&value) {
      if (_lock) throw std::runtime_error("Cannot access data when buffer is locked.");
      else return BaseVector::push_back(std::move(value));
    }

    iterator begin() {
      if (_lock) throw std::runtime_error("Cannot access data when buffer is locked.");
      else return BaseVector::begin();
    }

    const_iterator begin() const {
      if (_lock) throw std::runtime_error("Cannot access data when buffer is locked.");
      else return BaseVector::begin();
    }

    pointer data() noexcept {
      _lock = true;
      return BaseVector::data();
    }

  private:
    bool _lock = false;
    unsigned int _num;

  };
}

#endif //TILEDB_TDBPP_BUFFER_H
