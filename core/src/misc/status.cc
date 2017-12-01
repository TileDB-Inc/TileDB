/**
 * @file   status.cc
 *
 * @section LICENSE
 *
 * The BSD License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 *            Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of Google Inc. nor the names of its contributors may be used
 * to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
  auto size = static_cast<uint32_t>(msg_size);
  auto result = new char[size + 7];
  memcpy(result, &size, sizeof(size));
  result[4] = static_cast<char>(code);
  memcpy(result + 5, &posix_code, sizeof(posix_code));
  memcpy(result + 7, msg.c_str(), msg_size);
  state_ = result;
}

const char* Status::copy_state(const char* state) {
  uint32_t size;
  memcpy(&size, state, sizeof(size));
  auto result = new char[size + 7];
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
  result.append(static_cast<const char*>(state_ + 7), size);
  return result;
}

std::string Status::code_to_string() const {
  if (state_ == nullptr)
    return "Ok";

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
    case StatusCode::FragmentMetadata:
      type = "[TileDB::FragmentMetadata] Error";
      break;
    case StatusCode::ArrayMetadata:
      type = "[TileDB::ArrayMetadata] Error";
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
    case StatusCode::IO:
      type = "[TileDB::IO] Error";
      break;
    case StatusCode::Mem:
      type = "[TileDB::Mem] Error";
      break;
    case StatusCode::GZip:
      type = "[TileDB::GZip] Error";
      break;
    case StatusCode::Compression:
      type = "[TileDB::Compression] Error";
      break;
    case StatusCode::Tile:
      type = "[TileDB::Tile] Error";
      break;
    case StatusCode::TileIO:
      type = "[TileDB::TileIO] Error";
      break;
    case StatusCode::Buffer:
      type = "[TileDB::Buffer] Error";
      break;
    case StatusCode::Query:
      type = "[TileDB::Query] Error";
      break;
    case StatusCode::VFS:
      type = "[TileDB::VFS] Error";
      break;
    case StatusCode::ConstBuffer:
      type = "[TileDB::ConstBuffer] Error";
      break;
    case StatusCode::Dimension:
      type = "[TileDB::Dimension] Error";
      break;
    case StatusCode::Domain:
      type = "[TileDB::Domain] Error";
      break;
    case StatusCode::Consolidation:
      type = "[TileDB::Consolidation] Error";
      break;
    case StatusCode::LRUCache:
      type = "[TileDB::LRUCache] Error";
      break;
    default:
      type = "[TileDB::?] Error:";
  }
  return std::string(type);
}

int16_t Status::posix_code() const {
  int16_t code = -1;
  if (state_ == nullptr)
    return code;

  memcpy(&code, state_ + 5, sizeof(code));
  return code;
}

}  // namespace tiledb
