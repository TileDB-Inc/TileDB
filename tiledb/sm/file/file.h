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

#ifndef TILEDB_FILE_H
#define TILEDB_FILE_H

#include "tiledb/common/status.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/file/file_schema.h"
#include "tiledb/sm/filesystem/vfs_file_handle.h"
#include "tiledb/sm/misc/uri.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
/**
 * An file object to be opened for reads/writes. An ``File`` instance
 * is associated with the timestamp it is opened at.
 */
class File : public Array {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  File(const URI& array_uri, StorageManager* storage_manager);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Opens the file for reading at a timestamp retrieved from the config
   * or for writing.
   *
   * @param query_type The mode in which the file is opened.
   * @param encryption_type The encryption type of the file
   * @param encryption_key If the file is encrypted, the private encryption
   *    key. For unencrypted files, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   */
  Status open(
      QueryType query_type,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length) override;

  /**
   * Opens the file for reading without fragments.
   *
   * @param encryption_type The encryption type of the file
   * @param encryption_key If the file is encrypted, the private encryption
   *    key. For unencrypted files, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   *
   * @note Applicable only to reads.
   */
  Status open(
      QueryType query_type,
      uint64_t timestamp_start,
      uint64_t timestamp_end,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length) override;

  /**
   * Create file array on disk using default settings
   * @param config
   * @return
   */
  Status create([[maybe_unused]] const Config* config);

  /**
   * Create file array based on input file to heuristics
   * @param file file uri
   * @param config TileDB Config object for settings.
   * @return Status
   */
  Status create_from_uri(const URI& file, const Config* config);

  /**
   * Create file array based on input file to heuristics
   * @param file file uri
   * @param config TileDB Config object for settings.
   * @return Status
   */
  Status create_from_vfs_fh(
      const VFSFileHandle* file, [[maybe_unused]] const Config* config);

  /**
   * Read input file and store in file array
   * @param in FILE* handle for input file
   * @param config TileDB Config object for settings.
   * @return Status
   */
  Status save_from_file_handle(FILE* in, const Config* config);

  /**
   * Read input file and store in file array
   * @param file URI of file to store
   * @param config TileDB Config object for settings.
   * @return Status
   */
  Status save_from_uri(const URI& file, const Config* config);

  /**
   * Read input file and store in file array
   * @param file VFSFileHandle of file to store
   * @param config TileDB Config object for settings.
   * @return Status
   */
  Status save_from_vfs_fh(VFSFileHandle* file, const Config* config);

  /**
   * Read input file and store in file array
   * @param data void buffer of bytes to store
   * @param size size of input buffer.
   * @param config TileDB Config object for settings.
   * @return Status
   */
  Status save_from_buffer(
      void* data, uint64_t size, [[maybe_unused]] const Config* config);

  /**
   * Export file array to FILE handle
   * @param out FILE* handle to write to
   * @param config TileDB Config object for settings.
   * @return Status
   */
  Status export_to_file_handle(
      FILE* out, [[maybe_unused]] const Config* config);

  /**
   * Export file array to URI.
   * @param file URI to write to.
   * @param config TileDB Config object for settings.
   * @return Status
   */
  Status export_to_uri(const URI& file, const Config* config);

  /**
   * Export file array to VFSFileHandle.
   * @param file VFSFileHandle to write to.
   * @param offset offset to start reading file from.
   * @param config TileDB Config object for settings.
   * @return Status
   */
  Status export_to_vfs_fh(
      VFSFileHandle* file, [[maybe_unused]] const Config* config);

  /**
   * Export file array to buffer.
   * @param data buffer to write to.
   * @param size size to write to buffer.
   * @param file_offset offset to start reading file from.
   * @param config TileDB Config object for settings.
   * @return Status
   */
  Status export_to_buffer(
      void* data,
      uint64_t* size,
      uint64_t file_offset,
      [[maybe_unused]] const Config* config);

  /**
   * Get size based on current opened file
   * @return size
   */
  uint64_t size();

  /**
   * Get size based on current opened file
   * @param size pointer to size
   * @return Status
   */
  Status size(uint64_t* size);

  /**
   * Get mime type based on current opened file
   * @param mime pointer to mime_type from metadata
   * @param size size of mime_type string
   * @return Status
   */
  Status mime_type(const char** mime_type, uint32_t* size);

  /**
   * Get mime encoding based on current opened file
   * @param mime pointer to mime_encoding from metadata
   * @param size size of mime_encoding string
   * @return Status
   */
  Status mime_encoding(const char** mime_encoding, uint32_t* size);

  /**
   * Get original file name based on current opened file
   * @param name original name from metadata
   * @param size size of original_name string
   * @return Status
   */
  Status original_name(const char** name, uint32_t* size);

  /**
   * Get original file extension based on current opened file
   * @param ext pointer to extension from metadata
   * @param size size of extension string
   * @return Status
   */
  Status file_extension(const char** ext, uint32_t* size);

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */
 private:
  FileSchema file_schema_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

 private:
  //  std::optional<EncryptionKey> get_encryption_key_from_config(const Config&
  //  config) const;
  tdb_unique_ptr<EncryptionKey> get_encryption_key_from_config(
      const Config& config) const;

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
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILE_H
