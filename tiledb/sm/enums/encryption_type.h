/**
 * @file encryption_type.h
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
 * This defines the TileDB EncryptionType enum that maps to
 * tiledb_encryption_type_t C-API enum.
 */

#ifndef TILEDB_ENCRYPTION_TYPE_H
#define TILEDB_ENCRYPTION_TYPE_H

#include <cassert>
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

/** Defines an encryption type. */
enum class EncryptionType : uint8_t {
#define TILEDB_ENCRYPTION_TYPE_ENUM(id) id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_ENCRYPTION_TYPE_ENUM
};

/** Returns the string representation of the input encryption type. */
inline const std::string& encryption_type_str(EncryptionType encryption_type) {
  switch (encryption_type) {
    case EncryptionType::NO_ENCRYPTION:
      return constants::no_encryption_str;
    case EncryptionType::AES_256_GCM:
      return constants::aes_256_gcm_str;
    default:
      assert(0);
      return constants::empty_str;
  }
}

/** Returns the encryption type given a string representation. */
inline Status encryption_type_enum(
    const std::string& encryption_type_str, EncryptionType* encryption_type) {
  if (encryption_type_str == constants::no_encryption_str)
    *encryption_type = EncryptionType::NO_ENCRYPTION;
  else if (encryption_type_str == constants::aes_256_gcm_str)
    *encryption_type = EncryptionType::AES_256_GCM;
  else {
    return Status::Error("Invalid EncryptionType " + encryption_type_str);
  }
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ENCRYPTION_TYPE_H
