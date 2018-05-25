/**
 * @file   openssl_state.cc
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
 This file defines the OpenSSL state, if OpenSSL is present.
 */

#include "tiledb/sm/global_state/openssl_state.h"

#ifdef _WIN32

namespace tiledb {
namespace sm {
namespace global_state {

Status init_openssl() {
  return Status::Ok();
}

}  // namespace global_state
}  // namespace sm
}  // namespace tiledb

#else

#include <openssl/crypto.h>
#include <openssl/opensslv.h>
#include <memory>
#include <mutex>
#include <vector>

namespace tiledb {
namespace sm {
namespace global_state {

// OpenSSL older than 1.1.0 needs lock callbacks to be set.
#if ((OPENSSL_VERSION_NUMBER & 0xff000000) >> 24 < 0x10) || \
    ((OPENSSL_VERSION_NUMBER & 0x00ff0000) >> 16 < 0x10)

/** Vector of lock objects for use by OpenSSL. */
static std::vector<std::unique_ptr<std::mutex>> openssl_locks;

/**
 * Lock callback provided for OpenSSL.
 *
 * @param mode If set to CRYPTO_LOCK, lock. Else unlock.
 * @param n The index of the lock object.
 * @param file For debugging, calling file.
 * @param line For debugging, calling line number.
 */
static void openssl_lock_cb(int mode, int n, const char* file, int line) {
  (void)file;
  (void)line;
  if (mode & CRYPTO_LOCK) {
    openssl_locks.at(n)->lock();
  } else {
    openssl_locks.at(n)->unlock();
  }
}

Status init_openssl() {
  int num_locks = CRYPTO_num_locks();
  for (int i = 0; i < num_locks; i++) {
    openssl_locks.push_back(std::unique_ptr<std::mutex>(new std::mutex()));
  }

  CRYPTO_set_locking_callback(openssl_lock_cb);
  return Status::Ok();
}

#else  // OpenSSL >= 1.1.0

Status init_openssl() {
  return Status::Ok();
}

#endif

}  // namespace global_state
}  // namespace sm
}  // namespace tiledb

#endif