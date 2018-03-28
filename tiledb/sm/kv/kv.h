/**
 * @file   kv.h
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
 This file defines class KV.
 */

#ifndef TILEDB_KV_H
#define TILEDB_KV_H

#include "tiledb/sm/storage_manager/storage_manager.h"

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/kv/kv_item.h"
#include "tiledb/sm/misc/status.h"

#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace tiledb {
namespace sm {

/**
 * This is a key-value store. It enables both reading/writing with
 * thread- and process-safety. Upon writes, the written items are
 * available for reading. Written items are buffered and periodically
 * flushed into the disk. The user can call `flush` to force-write
 * the buffered items to the disk at any point. All items are
 * flushed upon `close` or destroying a `KV` object.
 */
class KV {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  explicit KV(StorageManager* storage_manager);

  /** Destructor. */
  ~KV();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Adds a key-value item to the store. */
  Status add_item(const KVItem* kv_item);

  /** Flushes the buffered written items to persistent storage. */
  Status flush();

  /**
   * Gets a key-value item from the key-value store. This function first
   * searches in the buffered items.
   *
   * @param key The key to query on.
   * @param key_type The key type.
   * @param key_size The key size.
   * @param kv_item The key-value item result.
   * @return Status
   */
  Status get_item(
      const void* key, Datatype key_type, uint64_t key_size, KVItem** kv_item);

  /**
   * Gets a key-value item from the key-value store based on its hash value.
   * This function does not search in the buffered items.
   *
   * @param hash The hash of the key-value item.
   * @param kv_item The key-value item result.
   * @return Status
   */
  Status get_item(const KVItem::Hash& hash, KVItem** kv_item);

  /**
   * Initializes the key-value store for reading/writing.
   *
   * @param uri The URI of the key-value store.
   * @param attributes The attributes of the key-value store schema to focus on.
   *     Use `nullptr` to indicate **all** attributes.
   * @param attribute_num The number of attributes.
   * @param include_keys If `true` the special key attributes will be included.
   * @return Status
   *
   * @note If the key-value store will be used for writes, `attributes` **must**
   *     be set to `nullptr`, indicating that all attributes will participate in
   *     the write.
   */
  Status init(
      const std::string& uri,
      const char** attributes,
      unsigned attribute_num,
      bool include_keys = false);

  /** Clears the key-value store. */
  void clear();

  /**
   * Finalizes the key-value store and frees all memory. All buffered values
   * will be flushed to persistent storage.
   */
  Status finalize();

  /** Sets the number of maximum written items buffered before being flushed. */
  Status set_max_buffered_items(uint64_t max_items);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * The attributes with which the key-value store is opened.
   * Note that these exclude the special key attributes and coordinates.
   */
  std::vector<std::string> attributes_;

  /**
   * Indicates whether an attribute is variable-sized or not.
   * There is a one-to-one correspondence with `read_attributes_`.
   * This is a map (attribute) -> (`true` for var size or `false` otherwise)
   */
  std::vector<bool> read_attribute_var_sizes_;

  /**
   * Types of `read_attributes_`. This is necessary when getting key-value
   * items, since these should also store the types of each
   * attribute value.
   */
  std::vector<Datatype> read_attribute_types_;

  /** These are the attributes to be passed to a TileDB read query. */
  char** read_attributes_;

  /** The number of read query attributes. */
  unsigned read_attribute_num_;

  /** Buffers to be used in read queries. */
  void** read_buffers_;

  /** The read buffer sizes. */
  uint64_t* read_buffer_sizes_;

  /** The number of read buffers. */
  unsigned read_buffer_num_;

  /**
   * Buffers holding the actual data to be read across all attributes.
   * The buffers are sorted in the order the attributes are sorted
   * in `read_attributes_`. Note that a variable-sized attribute
   * adds two adjacent buffers in the vector, the first is for offsets
   * and the second for the actual variable data.
   */
  std::vector<Buffer*> read_buff_vec_;

  /** The corresponding types of `attributes_`. */
  std::vector<Datatype> types_;

  /**
   * Indicates whether an attribute is variable-sized or not.
   * There is a one-to-one correspondence with `write_attributes_`.
   * This is a map (attribute) -> (`true` for var size or `false` otherwise)
   */
  std::vector<bool> write_attribute_var_sizes_;

  /** These are the attributes to be passed to a TileDB write query. */
  char** write_attributes_;

  /** The number of write query attributes. */
  unsigned write_attribute_num_;

  /** Buffers to be used in write queries. */
  void** write_buffers_;

  /** The write buffer sizes. */
  uint64_t* write_buffer_sizes_;

  /** The number of write buffers. */
  unsigned write_buffer_num_;

  /**
   * Buffers holding the actual data to be written across all attributes.
   * The buffers are sorted in the order the attributes are sorted
   * in `write_attributes_`. Note that a variable-sized attribute
   * adds two adjacent buffers in the vector, the first is for offsets
   * and the second for the actual variable data.
   */
  std::vector<Buffer*> write_buff_vec_;

  /**
   * `True` if the key-value store is good for writes. This happens when
   * the user specifies `nullptr` for attributes in `open`, meaning that
   * all attributes must be participate in the write.
   */
  bool write_good_;

  /** Items to be written to disk indexed on their hash. */
  std::map<KVItem::Hash, KVItem*> items_;

  /** The key-value URI.*/
  URI kv_uri_;

  /** Maximum number of items to be buffered. */
  uint64_t max_items_;

  /** The key-value store schema. */
  ArraySchema* schema_;

  /** TileDB storage manager. */
  StorageManager* storage_manager_;

  /** Mutex for thread-safety. */
  std::mutex mtx_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Adds the key contents to the appropriate write buffers. */
  Status add_key(const KVItem::Key& key);

  /**
   * Adds a value to a write buffer.
   *
   * @param value The value to be stored.
   * @param bid The index of the write buffer to store the value.
   * @param var `True` if the attribute is variable-sized.
   * @return Status
   */
  Status add_value(const KVItem::Value& value, unsigned bid, bool var);

  /** Frees memory of items. */
  void clear_items();

  /** It deletes the attributes passed in the read/write queries. */
  void clear_query_attributes();

  /** Clears the read/write query buffers. */
  void clear_query_buffers();

  /** Clears the read buffers. */
  void clear_read_buffers();

  /** Clears the read buffer vector. */
  void clear_read_buff_vec();

  /** Clears the write buffers. */
  void clear_write_buffers();

  /** Clears the write buffer vector. */
  void clear_write_buff_vec();

  /** Allocates space for the read buffers. */
  Status init_read_buffers();

  /** Prepares the attributes to be passed to the read/write TileDB queries. */
  Status prepare_query_attributes();

  /** Prepares the attributes to be passed to the read TileDB queries. */
  Status prepare_read_attributes();

  /** Prepares the buffers for reading. */
  Status prepare_read_buffers();

  /** Prepares the attributes to be passed to the write TileDB queries. */
  Status prepare_write_attributes();

  /** Prepares the buffers for writing. */
  Status prepare_write_buffers();

  /** Populates the write buffers with the buffered key-value items. */
  Status populate_write_buffers();

  /**
   * Reads a key-value item from persistent storage and into the local
   * read buffers, given the input key information.
   *
   * @param hash The hash of the key to search on.
   * @param found Set to `true` if the item is found, and `false` otherwise.
   * @return Status
   */
  Status read_item(const KVItem::Hash& hash, bool* found);

  /**
   * Reallocate memory for read buffers that results in an incomplete
   * read query.
   */
  Status realloc_read_buffers();

  /** Submits a read query. */
  Status submit_read_query(const uint64_t* subarray);

  /** Submits a write query. */
  Status submit_write_query();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_KV_H
