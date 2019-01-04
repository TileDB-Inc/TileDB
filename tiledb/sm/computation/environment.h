/**
 * @file   environment.h
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
 * This file defines class Environment.
 */

#ifndef TILEDB_ENVIRONMENT_H
#define TILEDB_ENVIRONMENT_H

#include <unordered_map>

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {
namespace computation {

class Environment {
 public:
  Environment(uint64_t num_cells);

  Status bind(
      const std::string& name,
      Datatype datatype,
      void* buffer,
      uint64_t buffer_size);

  Status bind_output(void* buffer, uint64_t buffer_size);

  uint64_t num_cells() const;

  Status output(Buffer** output);

  bool lookup(
      const std::string& name, Datatype* datatype, ConstBuffer* buffer) const;

 private:
  struct BufferAndType {
    Buffer buffer;
    Datatype datatype;
  };

  const std::string output_name_ = "__expr_output";

  std::unordered_map<std::string, BufferAndType> buffers_;

  /** Number of array cells in the input. */
  uint64_t num_cells_;
};

}  // namespace computation
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ENVIRONMENT_H
