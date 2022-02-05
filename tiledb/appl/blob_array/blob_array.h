/**
 * @file   file.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#ifndef TILEDB_BLOB_ARRAY_H
#define TILEDB_BLOB_ARRAY_H

#include "tiledb/appl/blob_array/blob_array_schema.h"
#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/filesystem/vfs_file_handle.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

using namespace tiledb::common;

namespace tiledb {
namespace appl {
// namespace sm {

// using namespace tiledb::sm;
using Config = tiledb::sm::Config;

/**
 * An blob_array object to be opened for reads/writes. An ``BlobArray`` instance
 * is associated with the timestamp it is opened at.
 */
class BlobArray : public tiledb::sm::Array {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  BlobArray(const URI& array_uri, StorageManager* storage_manager);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Create BlobArray array on disk using default settings
   * @param config
   * @return
   */
  Status create([[maybe_unused]] const Config* config);

  /**
   * Read input file and store in BlobArray array
   * @param file URI of file to store
   * @param config TileDB Config object for settings.
   * @return Status
   */
  Status save_from_uri(const URI& file, const Config* config);

  /**
   * Read input file and store in BlobArray array
   * @param file VFSFileHandle of file to store
   * @param config TileDB Config object for settings.
   * @return Status
   */
  Status save_from_vfs_fh(VFSFileHandle* file, const Config* config);
  /**
   * Read input file and store in BlobArray array
   * @param data void buffer of bytes to store
   * @param size size of input buffer.
   * @param config TileDB Config object for settings.
   * @return Status
   */
  Status save_from_buffer(
      void* data, uint64_t size, [[maybe_unused]] const Config* config);

  /**
   * Export BlobArray array to URI.
   * @param file URI to write to.
   * @param config TileDB Config object for settings.
   * @return Status
   */
  Status export_to_uri(const URI& file, const Config* config);

  /**
   * Export BlobArray array to VFSFileHandle.
   * @param file VFSFileHandle to write to.
   * @param offset offset to start reading file from.
   * @param config TileDB Config object for settings.
   * @return Status
   */
  Status export_to_vfs_fh(
      VFSFileHandle* file, [[maybe_unused]] const Config* config);

  /**
   * Get size based on current opened file
   * @return size
   */
  uint64_t size();

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */
 private:
  BlobArraySchema blob_array_schema_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

 private:
#if 01
  //  std::optional<EncryptionKey> get_encryption_key_from_config(const Config&
  //  config) const;
  tdb_unique_ptr<EncryptionKey> get_encryption_key_from_config(
      const Config& config) const;
#endif

#if 0
  /**
   * Get mime type from libmagic
   * @param data void buffer with first part of file (up to 1kb) for magic
   * detection
   * @param size size of buffer
   * @return mime type or nullptr if none detected
   */
  static const char* libmagic_get_mime(void* data, uint64_t size);

  /**
   * Get mime encoding from libmagic
   * @param data void buffer with first part of file (up to 1kb) for magic
   * detection
   * @param size size of buffer
   * @return mime encoding or nullptr if none detected
   */
  static const char* libmagic_get_mime_encoding(void* data, uint64_t size);

  /**
   * Store mime type in array metadata
   * @param file_metadata buffer with first part of file for magic detection
   * @param metadata_read_size size of buffer
   * @return Status
   */
  Status store_mime_type(
      const Buffer& file_metadata, uint64_t metadata_read_size);

  /**
   * Store mime encoding in array metadata
   * @param file_metadata buffer with first part of file for magic detection
   * @param metadata_read_size size of buffer
   * @return Status
   */
  Status store_mime_encoding(
      const Buffer& file_metadata, uint64_t metadata_read_size);
#endif
};

}  // namespace appl
}  // namespace tiledb

#endif  // TILEDB_BLOB_ARRAY_H
