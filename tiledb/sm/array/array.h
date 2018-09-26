/**
 * @file   array.h
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
 This file defines class Array.
 */

#ifndef TILEDB_ARRAY_H
#define TILEDB_ARRAY_H

#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/storage_manager/open_array.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

namespace tiledb {
namespace sm {

/**
 * An array object to be opened for reads/writes. An ``Array`` instance
 * is associated with the timestamp it is opened at.
 */
class Array {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Array(const URI& array_uri, StorageManager* storage_manager);

  /** Destructor. */
  ~Array();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the array schema. */
  ArraySchema* array_schema() const;

  /** Returns the array URI. */
  const URI& array_uri() const;

  /**
   * Computes an upper bound on the buffer sizes required for a read
   * query, for the input attributes.
   *
   * @param subarray The subarray to focus on. Note that it must have the same
   *     underlying type as the array domain.
   * @param attributes The attributes to focus on.
   * @param buffer_sizes The buffer sizes to be retrieved. This is a map from
   *     the attribute (or coordinates) name to a pair of sizes (in bytes).
   *     For fixed-sized attributes, the second size is ignored. For var-sized
   *     attributes, the first is the offsets size and the second is the
   *     values size.
   * @return Status
   */
  Status compute_max_buffer_sizes(
      const void* subarray,
      const std::vector<std::string>& attributes,
      std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
          max_buffer_sizes) const;

  /**
   * Opens the array for reading/writing at the current timestamp.
   *
   * @param query_type The mode in which the array is opened.
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @return Status
   */
  Status open(QueryType query_type, const void* encryption_key);

  /** Closes the array and frees all memory. */
  Status close();

  /**
   * Returns the fragment metadata of the array. If the array is not open,
   * an empty vector is returned.
   */
  std::vector<FragmentMetadata*> fragment_metadata() const;

  /**
   * Returns `true` if the array is empty at the time it is opened.
   * The funciton returns `false` if the array is not open.
   */
  bool is_empty() const;

  /** Returns `true` if the array is open. */
  bool is_open() const;

  /** Retrieves the array schema. Errors if the array is not open. */
  Status get_array_schema(ArraySchema** array_schema) const;

  /** Retrieves the query type. Errors if the array is not open. */
  Status get_query_type(QueryType* qyery_type) const;

  /**
   * Returns the max buffer size given a fixed-sized attribute and
   * a subarray. Errors if the array is not open.
   */
  Status get_max_buffer_size(
      const char* attribute, const void* subarray, uint64_t* buffer_size);

  /**
   * Returns the max buffer size given a var-sized attribute and
   * a subarray. Errors if the array is not open.
   */
  Status get_max_buffer_size(
      const char* attribute,
      const void* subarray,
      uint64_t* buffer_off_size,
      uint64_t* buffer_val_size);

  /**
   * Returns the encryption type used for the array.
   */
  EncryptionType get_encryption_type() const;

  /**
   * Returns a ConstBuffer instance holding a pointer to the user's private
   * encryption key.
   */
  ConstBuffer get_encryption_key() const;

  /**
   * Re-opens the array. This effectively updates the "view" of the array,
   * by loading the fragments potentially written after the last time
   * the array was opened. Errors if the array is not open.
   *
   * @note Applicable only for reads, it errors if the array was opened
   *     for writes.
   */
  Status reopen();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array URI. */
  URI array_uri_;

  /**
   * The private encryption key used to encrypt the array.
   *
   * Note: this is the only place in TileDB where the user's private key bytes
   * should be stored. Wherever a key is needed, a pointer to this memory region
   * should be passed instead of a copy of the bytes.
   */
  Buffer encryption_key_;

  /** The type of encryption used to encrypt the array. */
  EncryptionType encryption_type_;

  /** `True` if the array has been opened. */
  std::atomic<bool> is_open_;

  /** The open array that will receive the read or write queries. */
  OpenArray* open_array_;

  /** The snapshot at which the `open_array_` got opened. */
  uint64_t snapshot_;

  /** TileDB storage manager. */
  StorageManager* storage_manager_;

  /** Stores the max buffer sizes requested last time by the user .*/
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
      last_max_buffer_sizes_;

  /**
   * This is the last subarray used by the user to retrieve the
   * max buffer sizes.
   */
  void* last_max_buffer_sizes_subarray_;

  /** Mutex for thread-safety. */
  std::mutex mtx_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Clears the cached max buffer sizes and subarray. */
  void clear_last_max_buffer_sizes();

  /**
   * Computes the maximum buffer sizes for all attributes given a subarray,
   * which are cached locally in the instance.
   */
  Status compute_max_buffer_sizes(const void* subarray);

  /**
   * Copies the given encryption key into this Array's private buffer.
   *
   * @param encryption_type Type of encryption key
   * @param key_bytes Key bytes (from user).
   * @return Status
   */
  Status set_encryption_key(
      EncryptionType encryption_type, const void* key_bytes);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ARRAY_H
