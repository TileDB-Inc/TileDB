/**
 * @file   status.cc
 *
 * @section LICENSE
 *
 * The BSD License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
#include <cassert>

namespace tiledb::common {

Status::Status(
    const std::string_view& vicinity, const std::string_view& message) {
  auto msg_size = message.size();
  // Initialize `state_` first so member pointers are available.
  state_ = tdb_new_array(char, allocation_size(msg_size));
  origin_() = vicinity;
  message_size_() = static_cast<message_size_type>(msg_size);
  memcpy(message_text_ptr_(), message.data(), msg_size);
}

void Status::copy_state(const Status& st) {
  if (st.state_ == nullptr) {
    state_ = nullptr;
    return;
  }
  auto state_size = allocation_size(st.message_size_());
  state_ = tdb_new_array(char, state_size);
  memcpy(const_cast<char*>(state_), st.state_, state_size);
}

std::string Status::to_string() const {
  if (state_ == nullptr) {
    return Ok_text_;
  }
  std::string result{origin_()};
  result.append(": ");
  result.append(message_text_ptr_(), message_size_());
  return result;
}

}  // namespace tiledb::common
