/**
 * @file   uuid.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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
 * This file defines a platform-independent UUID generator.
 */

#include <mutex>
#include <vector>

#include "tiledb/sm/misc/uuid.h"

#ifdef _WIN32
#include <Rpc.h>
#else
#include <openssl/err.h>
#include <openssl/rand.h>
#include <cstdio>
#endif

namespace tiledb {
namespace sm {
namespace uuid {

/** Mutex to guard UUID generation. */
static std::mutex uuid_mtx;

#ifdef _WIN32

/**
 * Generate a UUID using Win32 RPC API.
 */
Status generate_uuid_win32(std::string* uuid_str) {
  if (uuid_str == nullptr)
    return Status::UtilsError("Null UUID string argument");

  UUID uuid;
  RPC_STATUS rc = UuidCreate(&uuid);
  if (rc != RPC_S_OK)
    return Status::UtilsError("Unable to generate Win32 UUID: creation error");

  char* buf = nullptr;
  rc = UuidToStringA(&uuid, reinterpret_cast<RPC_CSTR*>(&buf));
  if (rc != RPC_S_OK)
    return Status::UtilsError(
        "Unable to generate Win32 UUID: string conversion error");

  *uuid_str = std::string(buf);

  rc = RpcStringFreeA(reinterpret_cast<RPC_CSTR*>(&buf));
  if (rc != RPC_S_OK)
    return Status::UtilsError("Unable to generate Win32 UUID: free error");

  return Status::Ok();
}

#else

/**
 * Generate a UUID using OpenSSL.
 *
 * Initially from: https://gist.github.com/kvelakur/9069c9896577c3040030
 * "Generating a Version 4 UUID using OpenSSL"
 */
Status generate_uuid_openssl(std::string* uuid_str) {
  if (uuid_str == nullptr)
    return Status::UtilsError("Null UUID string argument");

  union {
    struct {
      uint32_t time_low;
      uint16_t time_mid;
      uint16_t time_hi_and_version;
      uint8_t clk_seq_hi_res;
      uint8_t clk_seq_low;
      uint8_t node[6];
    };
    uint8_t __rnd[16];
  } uuid;

  int rc = RAND_bytes(uuid.__rnd, sizeof(uuid));
  if (rc < 1) {
    char err_msg[256];
    ERR_error_string_n(ERR_get_error(), err_msg, sizeof(err_msg));
    return Status::UtilsError(
        "Cannot generate random bytes with OpenSSL: " + std::string(err_msg));
  }

  // Refer Section 4.2 of RFC-4122
  // https://tools.ietf.org/html/rfc4122#section-4.2
  uuid.clk_seq_hi_res = (uint8_t)((uuid.clk_seq_hi_res & 0x3F) | 0x80);
  uuid.time_hi_and_version =
      (uint16_t)((uuid.time_hi_and_version & 0x0FFF) | 0x4000);

  // Format the UUID as a string.
  char buf[128];
  rc = snprintf(
      buf,
      sizeof(buf),
      "%08x-%04x-%04x-%02x%02x-%02x%02x%02x%02x%02x%02x",
      uuid.time_low,
      uuid.time_mid,
      uuid.time_hi_and_version,
      uuid.clk_seq_hi_res,
      uuid.clk_seq_low,
      uuid.node[0],
      uuid.node[1],
      uuid.node[2],
      uuid.node[3],
      uuid.node[4],
      uuid.node[5]);

  if (rc < 0)
    return Status::UtilsError("Error formatting UUID string");

  *uuid_str = std::string(buf);

  return Status::Ok();
}

#endif

Status generate_uuid(std::string* uuid, bool hyphenate) {
  if (uuid == nullptr)
    return Status::UtilsError("Null UUID string argument");

  std::string uuid_str;
  {
    // OpenSSL is not threadsafe, so grab a lock here. We are locking in the
    // Windows case as well just to be careful.
    std::unique_lock<std::mutex> lck(uuid_mtx);
#ifdef _WIN32
    RETURN_NOT_OK(generate_uuid_win32(&uuid_str));
#else
    RETURN_NOT_OK(generate_uuid_openssl(&uuid_str));
#endif
  }

  uuid->clear();
  for (unsigned i = 0; i < uuid_str.length(); i++) {
    if (uuid_str[i] == '-' && !hyphenate)
      continue;
    uuid->push_back(uuid_str[i]);
  }

  return Status::Ok();
}

}  // namespace uuid
}  // namespace sm
}  // namespace tiledb