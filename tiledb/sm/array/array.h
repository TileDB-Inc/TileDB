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

#include "tiledb/sm/encryption/encryption_key.h"
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
   * query, for a given subarray and set of attributes.
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
   * Opens the array for reading/writing.
   *
   * @param query_type The mode in which the array is opened.
   * @param encryption_type The encryption type of the array
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   */
  Status open(
      QueryType query_type,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /**
   * Opens the array for reading, loading only the input fragments.
   * Note that the order of the input fragments matters; later
   * fragments in the list may overwrite earlier ones.
   *
   * @param query_type The query type. This should always be READ. It
   *    is here only for sanity check.
   * @param fragments The fragments to open the array with.
   * @param encryption_type The encryption type of the array
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   *
   * @note Applicable only to reads.
   */
  Status open(
      QueryType query_type,
      const std::vector<FragmentInfo>& fragments,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /**
   * Opens the array for reading at a given timestamp.
   *
   * @param query_type The query type. This should always be READ. It
   *    is here only for sanity check.
   * @param timestamp The timestamp at which to open the array.
   * @param encryption_type The encryption type of the array
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   *
   * @note Applicable only to reads.
   */
  Status open(
      QueryType query_type,
      uint64_t timestamp,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

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
   * Returns a reference to the private encryption key.
   */
  const EncryptionKey& get_encryption_key() const;

  /**
   * Re-opens the array. This effectively updates the "view" of the array,
   * by loading the fragments potentially written after the last time
   * the array was opened. Errors if the array is not open.
   *
   * @note Applicable only for reads, it errors if the array was opened
   *     for writes.
   */
  Status reopen();

  /**
   * Re-opens the array at a specific timestamp.
   *
   * @note Applicable only for reads, it errors if the array was opened
   *     for writes.
   */
  Status reopen(uint64_t timestamp);

  /** Returns the timestamp at which the array was opened. */
  uint64_t timestamp() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array schema. */
  ArraySchema* array_schema_;

  /** The array URI. */
  URI array_uri_;

  /**
   * The private encryption key used to encrypt the array.
   *
   * Note: this is the only place in TileDB where the user's private key bytes
   * should be stored. Wherever a key is needed, a pointer to this memory region
   * should be passed instead of a copy of the bytes.
   */
  EncryptionKey encryption_key_;

  /** The metadata of the fragments the array was opened with. */
  std::vector<FragmentMetadata*> fragment_metadata_;

  /** `True` if the array has been opened. */
  std::atomic<bool> is_open_;

  /** The query type the array was opened for. */
  QueryType query_type_;

  /**
   * The timestamp at which the `open_array_` got opened. In TileDB,
   * timestamps are in ms elapsed since 1970-01-01 00:00:00 +0000 (UTC).
   */
  uint64_t timestamp_;

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
  mutable std::mutex mtx_;

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
   * Computes an upper bound on the buffer sizes required for a read
   * query, for a given subarray and set of attributes. Note that
   * the attributes are already set in `max_buffer_sizes`
   *
   * @param subarray The subarray to focus on. Note that it must have the same
   *     underlying type as the array domain.
   * @param max_buffer_sizes The buffer sizes to be retrieved. This is a map
   * from the attribute (or coordinates) name to a pair of sizes (in bytes). For
   * fixed-sized attributes, the second size is ignored. For var-sized
   *     attributes, the first is the offsets size and the second is the
   *     values size.
   * @return Status
   */
  Status compute_max_buffer_sizes(
      const void* subarray,
      std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
          max_buffer_sizes_) const;

  /**
   * Computes an upper bound on the buffer sizes required for a read
   * query, for a given subarray and set of attributes. Note that
   * the attributes are already set in `max_buffer_sizes`
   *
   * @tparam T The domain type
   * @param subarray The subarray to focus on. Note that it must have the same
   *     underlying type as the array domain.
   * @param max_buffer_sizes The buffer sizes to be retrieved. This is a map
   *     from an attribute to a size pair. For fixed-sized attributes, only
   *     the first size is useful. For var-sized attributes, the first size
   *     is the offsets size, and the second size is the values size.
   * @return Status
   */
  template <class T>
  Status compute_max_buffer_sizes(
      const T* subarray,
      std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
          max_buffer_sizes) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ARRAY_H
