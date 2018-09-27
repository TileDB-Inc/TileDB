/**
 * @file   encryption_key_validation.h
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
 * This file declares a class for encryption key validation.
 */

#ifndef TILEDB_ENCRYPTION_KEY_VALIDATION_H
#define TILEDB_ENCRYPTION_KEY_VALIDATION_H

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/encryption/encryption.h"
#include "tiledb/sm/encryption/encryption_key.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

/**
 * Class which securely validates that a given encryption key is the same as a
 * previously given encryption key, without storing the key itself.
 */
class EncryptionKeyValidation {
 public:
  /**
   * Checks an encryption key.
   *
   * On the first invocation, the given key is used to encrypt a buffer of known
   * data. On subsequent invocations, the given key is used to decrypt the
   * internal encrypted buffer, which is checked for correctness against the
   * known data.
   *
   * An error is returned if the key is invalid.
   *
   * @param encryption_key The encryption key to check.
   * @return Status
   */
  Status check_encryption_key(const EncryptionKey& encryption_key);

 private:
  /** Constant string value used to check encryption keys. */
  static const std::string ENCRYPTION_KEY_CHECK_DATA;

  /** Buffer holding the encrypted bytes for the check data. */
  Buffer encryption_key_check_data_;

  /** Buffer holding the IV bytes for the check data. */
  Buffer encryption_key_check_data_iv_;

  /** Buffer holding the tag bytes for the check data. */
  Buffer encryption_key_check_data_tag_;

  /**
   * Encrypt a known value with the given key and store the encrypted data.
   *
   * @param encryption_key The encryption key to use.
   * @return Status
   */
  Status init_encryption_key_check_data(const EncryptionKey& encryption_key);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ENCRYPTION_KEY_VALIDATION_H