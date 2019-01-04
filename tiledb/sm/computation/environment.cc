/**
 * @file   environment.cc
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
 * This file implements class Environment.
 */

#include "tiledb/sm/computation/environment.h"
#include "tiledb/sm/buffer/const_buffer.h"

namespace tiledb {
namespace sm {
namespace computation {

Environment::Environment(uint64_t num_cells)
    : num_cells_(num_cells) {
}

Status Environment::bind(
    const std::string& name,
    Datatype datatype,
    void* buffer,
    uint64_t buffer_size) {
  if (buffers_.count(name) > 0)
    return Status::ExprError(
        "Cannot bind buffer for name; buffer already bound.");

  // TODO: Buffer class does not properly obey the rule of 5, so we are
  // explicit here.
  Buffer buff(buffer, buffer_size);
  buffers_[name].buffer.swap(buff);
  buffers_[name].datatype = datatype;

  return Status::Ok();
}

Status Environment::bind_output(void* buffer, uint64_t buffer_size) {
  // TODO: do we need an inferred output datatype?
  Datatype datatype = Datatype::UINT8;
  return bind(output_name_, datatype, buffer, buffer_size);
}

uint64_t Environment::num_cells() const {
  return num_cells_;
}

Status Environment::output(Buffer** output) {
  auto it = buffers_.find(output_name_);
  if (it == buffers_.end())
    return Status::ExprError(
        "Cannot get output buffer from environment; no such buffer.");

  *output = &it->second.buffer;

  return Status::Ok();
}

bool Environment::lookup(
    const std::string& name, Datatype* datatype, ConstBuffer* buffer) const {
  auto it = buffers_.find(name);
  if (it == buffers_.end())
    return false;

  *buffer = ConstBuffer(it->second.buffer.data(), it->second.buffer.size());
  *datatype = it->second.datatype;
  return true;
}

}  // namespace computation
}  // namespace sm
}  // namespace tiledb
