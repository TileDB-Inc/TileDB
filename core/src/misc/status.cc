/**
 * @file   status.cc
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
 * A Status object encapsulates the result of an operation.  It may indicate
 * success, or it may indicate an error with an associated error message.
 *
 * Multiple threads can invoke const methods on a Status without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same Status must use
 * external synchronization.
 */

#include "status.h"
#include <assert.h>

namespace tiledb {

Status::Status(StatusCode code, const std::string& msg, int16_t posix_code) {
  assert(code != StatusCode::Ok);
  size_t msg_size = msg.size();
  // assert(msg_size < std::numeric_limits<uint32_t>::max());
  const uint32_t size = static_cast<uint32_t>(msg_size);
  char* result = new char[size + 7];
  memcpy(result, &size, sizeof(size));
  result[4] = static_cast<char>(code);
  memcpy(result + 5, &posix_code, sizeof(posix_code));
  memcpy(result + 7, msg.c_str(), msg_size);
  state_ = result;
}

const char* Status::copy_state(const char* state) {
  uint32_t size;
  memcpy(&size, state, sizeof(size));
  char* result = new char[size + 7];
  memcpy(result, state, size + 7);
  return result;
}

std::string Status::to_string() const {
  std::string result(code_to_string());
  if (state_ == nullptr) {
    return result;
  }
  result.append(": ");
  uint32_t size;
  memcpy(&size, state_, sizeof(size));
  result.append(reinterpret_cast<const char*>(state_ + 7), size);
  return result;
}
std::string Status::code_to_string() const {
  if (state_ == nullptr) {
    return "Ok";
  }
  const char* type;
  switch (code()) {
    case StatusCode::Ok:
      type = "Ok";
      break;
    case StatusCode::Error:
      type = "Error";
      break;
    case StatusCode::StorageManager:
      type = "[TileDB::StorageManager] Error";
      break;
    case StatusCode::WriteState:
      type = "[TileDB::WriteState] Error";
      break;
    case StatusCode::Fragment:
      type = "[TileDB::Fragment] Error";
      break;
    case StatusCode::Bookkeeping:
      type = "[TileDB::Bookkeeping] Error";
      break;
    case StatusCode::Array:
      type = "[TileDB::Array] Error";
      break;
    case StatusCode::ArraySchema:
      type = "[TileDB::ArraySchema] Error";
      break;
    case StatusCode::ArrayIt:
      type = "[TileDB::ArrayIterator] Error";
      break;
    case StatusCode::ASRS:
      type = "[TileDB::ArraySortedReadState] Error";
      break;
    case StatusCode::ASWS:
      type = "[TileDB::ArraySortedWriteState] Error";
      break;
    case StatusCode::Metadata:
      type = "[TileDB::Metadata] Error";
      break;
    case StatusCode::OS:
      type = "[TileDB::OS] Error";
      break;
    case StatusCode::IO:
      type = "[TileDB::IO] Error";
      break;
    case StatusCode::Mem:
      type = "[TileDB::Mem] Error";
      break;
    case StatusCode::MMap:
      type = "[TileDB::MMap] Error";
      break;
    case StatusCode::GZip:
      type = "[TileDB::GZip] Error";
      break;
    case StatusCode::Compression:
      type = "[TileDB::Compression] Error";
      break;
    case StatusCode::AIO:
      type = "[TileDB::AIO] Error";
      break;
    default:
      type = "[TileDB::?] Error:";
  }
  return std::string(type);
}

int16_t Status::posix_code() const {
  int16_t code = -1;
  if (state_ == nullptr) {
    return code;
  }
  memcpy(&code, state_ + 5, sizeof(code));
  return code;
}

}  // namespace tiledb
