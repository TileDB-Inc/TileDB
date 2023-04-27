/**
 * @file   uuid.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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

#include <cstdio>
#include <mutex>
#include <vector>

#include "tiledb/sm/misc/uuid.h"

#ifdef _WIN32
#include <windows.h>
#ifndef NT_SUCCESS
#define NT_SUCCESS(Status) (((NTSTATUS)(Status)) >= 0)
#endif
#ifndef BCRYPT_RNG_ALG_HANDLE
#define BCRYPT_RNG_ALG_HANDLE ((BCRYPT_ALG_HANDLE)0x00000081)
#endif
#else
#include <openssl/err.h>
#include <openssl/rand.h>
#endif

namespace tiledb::sm::uuid {
struct Uuid {
  uint32_t time_low;
  uint16_t time_mid;
  uint16_t time_hi_and_version;
  uint8_t clk_seq_hi_res;
  uint8_t clk_seq_low;
  uint8_t node[6];
};
/**
 * Generate a UUID using the system's cryptographic random number generator.
 *
 * Initially from: https://gist.github.com/kvelakur/9069c9896577c3040030
 * "Generating a Version 4 UUID using OpenSSL"
 */
std::string generate_uuid(bool hyphenate) {
  Uuid uuid = {};

  {
#ifdef _WIN32
    int rc = BCryptGenRandom(
        BCRYPT_RNG_ALG_HANDLE,
        reinterpret_cast<uint8_t*>(&uuid),
        sizeof(uuid),
        0);
    if (!NT_SUCCESS(rc)) {
      throw std::runtime_error("Cannot generate GUID: BCryptGenRandom failed.");
    }
#else
    int rc = RAND_bytes(reinterpret_cast<uint8_t*>(&uuid), sizeof(uuid));
    if (rc < 1) {
      char err_msg[256];
      ERR_error_string_n(ERR_get_error(), err_msg, sizeof(err_msg));
      return Status_UtilsError(
          "Cannot generate random bytes with OpenSSL: " + std::string(err_msg));
    }
#endif
  }

  // Refer Section 4.2 of RFC-4122
  // https://tools.ietf.org/html/rfc4122#section-4.2
  uuid.clk_seq_hi_res = (uint8_t)((uuid.clk_seq_hi_res & 0x3F) | 0x80);
  uuid.time_hi_and_version =
      (uint16_t)((uuid.time_hi_and_version & 0x0FFF) | 0x4000);

  // Format the UUID as a string.
  const char* format_str =
      hyphenate ? "%08x-%04x-%04x-%02x%02x-%02x%02x%02x%02x%02x%02x" :
                  "%08x%04x%04x%02x%02x%02x%02x%02x%02x%02x%02x";
  char buf[128];
  int rc = snprintf(
      buf,
      sizeof(buf),
      format_str,
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
    throw std::runtime_error("Error formatting UUID string");

  return std::string(buf);
}

}  // namespace tiledb::sm::uuid
