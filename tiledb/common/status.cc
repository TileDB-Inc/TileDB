/**
 * @file   status.cc
 *
 * @section LICENSE
 *
 * The BSD License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#include "tiledb/common/status.h"
#include <cassert>

namespace tiledb::common {

/**
 * General constructor for arbitrary status
 */
Status::Status(StatusCode code, const std::string_view& msg) {
  assert(code != StatusCode::Ok);
  size_t msg_size = msg.size();
  // assert(msg_size < std::numeric_limits<uint32_t>::max());
  auto size = static_cast<uint32_t>(msg_size);
  auto result = tdb_new_array(char, size + 7);
  memcpy(result, &size, sizeof(size));
  result[4] = static_cast<char>(code);
  int16_t reserved = 0;
  memcpy(result + 5, &reserved, sizeof(reserved));
  memcpy(result + 7, msg.data(), msg_size);
  state_ = result;
}

const char* Status::copy_state(const char* state) {
  uint32_t size;
  memcpy(&size, state, sizeof(size));
  auto result = tdb_new_array(char, size + 7);
  memcpy(result, state, size + 7);
  return result;
}

std::string Status::to_string() const {
  auto sc =
      (state_ == nullptr) ? StatusCode::Ok : static_cast<StatusCode>(state_[4]);
  std::string result = common::to_string(sc);
  if (state_ == nullptr) {
    return result;
  }
  result.append(": ");
  uint32_t size;
  memcpy(&size, state_, sizeof(size));
  result.append(static_cast<const char*>(state_ + 7), size);
  return result;
}

}  // namespace tiledb::common
