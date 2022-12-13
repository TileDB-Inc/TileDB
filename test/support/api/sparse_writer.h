/**
 * @file sparse_array.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Implementation of the v3 API
 */

#ifndef TILEDB_API_V3_SPARSE_WRITER_H
#define TILEDB_API_V3_SPARSE_WRITER_H

#include <functional>
#include <map>
#include <memory>
#include <vector>

#include "sparse_array.h"
#include "details/writer.h"

namespace tiledb::test::api::v3 {

class SparserWriter;

class Cell {
  public:
    Cell(SparseWriter* writer) : writer_(writer);

  private:
    SparserWriter* writer_;
}

class SparseWriter : public details::Writer {
  public:
    SparseWriter(SparseArray& array)
        : details::Writer()
        , array_(array) {

      auto dims = array_.get_dimensions();
      num_dims_ = dims.size();
      for(auto it : dims) {
        buffer_idx_[it.name()] = buffers_.size();
        buffers_.push_back(it.buffer());
      }

      auto attrs = array_.get_attributes();
      for(auto it : attrs) {
        buffer_idx_[it.name] = buffers_.size();
        buffers_.push_back(it.buffer());
      }
    }

    template<typename... Args>
    Cell operator(Args... args) {
      assert(sizeof...(Args) == N, "Invalid number of dimensions");

    }


    void write() {
    }

  private:
    SparseArray& array_;

    size_t num_dims_;
    std::unordered_map<std::string, size_t> buffer_idx_;
    std::vector<WriterBuffer> buffers_;
};

} // namespace tiledb::test::api::v3

#endif // TILEDB_API_V3_SPARSE_WRITER_H
