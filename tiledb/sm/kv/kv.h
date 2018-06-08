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

  /** The tile capacity of the KV schema. */
  uint64_t capacity() const;

  /**
   * Returns `true` if the kv contains written unflushed items buffered
   * in main memory.
   */
  bool dirty() const;

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
   * Checks if the key-value store contains a particular key.
   *
   * @param key The key to query on.
   * @param key_type The key type.
   * @param key_size The key size.
   * @param has_key Set to `true` if the key exists and `false` otherwise.
   * @return Status
   */
  Status has_key(
      const void* key, Datatype key_type, uint64_t key_size, bool* has_key);

  /**
   * Initializes the key-value store for reading/writing.
   *
   * @param uri The URI of the key-value store.
   * @param attributes The attributes of the key-value store schema to focus on.
   *     Use an empty vector to indicate **all** attributes.
   * @return Status
   *
   * @note If the key-value store will be used for writes, `attributes` **must**
   *     be empty, indicating that all attributes will participate in
   *     the write.
   */
  Status init(const URI& uri, const std::vector<std::string>& attributes);

  /** Clears the key-value store. */
  void clear();

  /**
   * Finalizes the key-value store and frees all memory. All buffered values
   * will be flushed to persistent storage.
   */
  Status finalize();

  /** Re-opens the key-value store for reads. */
  Status reopen();

  /** The open array used for dispatching read queries. */
  OpenArray* open_array_for_reads() const;

  /** Sets the number of maximum written items buffered before being flushed. */
  Status set_max_buffered_items(uint64_t max_items);

  /**
   * Returns the snapshot at which the underlying array got opened for
   * reads.
   */
  uint64_t snapshot() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * These are the attributes provided during opening the KV (or all
   * the attributes in the array schema if no attributes were provided.
   */
  std::vector<std::string> attributes_;

  /** These are the corresponding types of `attributes_`. */
  std::vector<Datatype> attribute_types_;

  /** The array object that will receive the read queries. */
  OpenArray* open_array_for_reads_;

  /** The array object that will receive the write queries. */
  OpenArray* open_array_for_writes_;

  /**
   * Buffers to be used in read queries. This is a map from the
   * attribute (or coordinates) name to a pair of buffers. For fixed-sized
   * attributes, the second buffer is ignored. For var-sized attributes, the
   * first buffer is for the offsets the second for the values.
   */
  std::unordered_map<std::string, std::pair<void*, void*>> read_buffers_;

  /**
   * The read buffer sizes. This is a map from the attribute (or coordinates)
   * name to a pair of sizes (in bytes). For fixed-sized attributes, the
   * second size is ignored. For var-sized attributes, the first is the
   * offsets size and the second the values size.
   *
   * Note that these are different from `read_buffer_alloced_sizes_`.
   * `read_buffer_sizes_` are used in the read queries and may be altered
   * by the queries to reflect the useful data in the buffers.
   */
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
      read_buffer_sizes_;

  /**
   * The read buffer allocated sizes. This is a map from the attribute
   * (or coordinates) name to a pair of sizes (in bytes). For fixed-sized
   * attributes, the second size is ignored. For var-sized attributes,
   * the first is the offsets size and the second the values size.
   */
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
      read_buffer_alloced_sizes_;

  /** The snapshot at which the `open_array_for_reads_` got opened. */
  uint64_t snapshot_;

  /**
   * Buffers to be used in write queries. This is a map from the
   * attribute (or coordinates) name to a pair of buffers. For fixed-sized
   * attributes, the second buffer is ignored. For var-sized attributes, the
   * first buffer is for the offsets the second for the values.
   */
  std::unordered_map<std::string, std::pair<Buffer*, Buffer*>> write_buffers_;

  /**
   * The write buffer sizes. This is a map from the attribute (or coordinates)
   * name to a pair of sizes (in bytes). For fixed-sized attributes, the
   * second size is ignored. For var-sized attributes, the first is the
   * offsets size and the second the values size.
   */
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
      write_buffer_sizes_;

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
  const ArraySchema* schema_;

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
   * @param attribute The attribute the value belongs to.
   * @param value The value to be stored.
   * @return Status
   */
  Status add_value(const std::string& attribute, const KVItem::Value& value);

  /** Frees memory of items. */
  void clear_items();

  /** Clears the read buffers.*/
  void clear_read_buffers();

  /** Clears the write buffers.*/
  void clear_write_buffers();

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

  /** Sets the read query buffers. */
  Status set_read_query_buffers(Query* query);

  /** Sets the write query buffers. */
  Status set_write_query_buffers(Query* query);

  /** Submits a read query. */
  Status submit_read_query(const uint64_t* subarray);

  /** Submits a write query. */
  Status submit_write_query();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_KV_H
