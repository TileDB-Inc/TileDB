/**
 * @file filesystem_base.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2025 TileDB, Inc.
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
 * This file declares the Filesystem base class.
 */

#ifndef TILEDB_FILESYSTEMBASE_H
#define TILEDB_FILESYSTEMBASE_H

#include "tiledb/common/exception/exception.h"
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/sm/filesystem/ls_scanner.h"
#include "uri.h"

#include <vector>

namespace tiledb::sm {
class IOError : public StatusException {
 public:
  explicit IOError(const std::string& message)
      : StatusException("IO Error", message) {
  }
};

class FilesystemException : public StatusException {
 public:
  explicit FilesystemException(const std::string& message)
      : StatusException("Filesystem", message) {
  }
};

class UnsupportedOperation : public FilesystemException {
 public:
  explicit UnsupportedOperation(const std::string& operation)
      : FilesystemException(std::string(
            operation + " is not supported on the given filesystem.")) {
  }
};

class UnsupportedURI : public FilesystemException {
 public:
  explicit UnsupportedURI(const std::string& uri)
      : FilesystemException("Unsupported URI scheme: " + uri) {
  }
};

class FilesystemBase {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  FilesystemBase() = default;

  virtual ~FilesystemBase() = default;

  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */
  struct BufferedChunk {
    std::string uri;
    uint64_t size;

    BufferedChunk()
        : uri("")
        , size(0) {
    }
    BufferedChunk(std::string chunk_uri, uint64_t chunk_size)
        : uri(chunk_uri)
        , size(chunk_size) {
    }
  };

  /**
   * Multipart upload state definition used in the serialization of remote
   * global order writes. This state is a generalization of
   * the multipart upload state types currently defined independently by each
   * backend implementation.
   */
  struct MultiPartUploadState {
    struct CompletedParts {
      optional<std::string> e_tag;
      uint64_t part_number;
    };

    uint64_t part_number;
    optional<std::string> upload_id;
    optional<std::vector<BufferedChunk>> buffered_chunks;
    std::vector<CompletedParts> completed_parts;
    Status status;
  };

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Checks if the filesystem supports the given URI.
   *
   * - s3.supports_uri(s3://test) will return true.
   * - posix.supports_uri(s3://test) will return false.
   *
   * @param uri The URI to check.
   * @return `true` if `uri` is supported on the filesystem, `false` otherwise.
   */
  virtual bool supports_uri(const URI& uri) const = 0;

  /**
   * Creates a directory.
   *
   * - On S3, this is a noop.
   * - On all other backends, if the directory exists, the function
   *   just succeeds without doing anything.
   *
   * @param uri The URI of the directory.
   */
  virtual void create_dir(const URI& uri) const = 0;

  /**
   * Creates an empty file.
   *
   * @param uri The URI of the file.
   */
  virtual void touch(const URI& uri) const = 0;

  /**
   * Checks if a directory exists.
   *
   * @param uri The URI to check for existence.
   * @return True if the directory exists, else False.
   */
  virtual bool is_dir(const URI& uri) const = 0;

  /**
   * Checks if a file exists.
   *
   * @param uri The URI to check for existence.
   * @return True if the file exists, else False.
   */
  virtual bool is_file(const URI& uri) const = 0;

  /**
   * Removes a given directory (recursive)
   *
   * @param uri The uri of the directory to be removed
   */
  virtual void remove_dir(const URI& uri) const = 0;

  /**
   * Removes a given empty directory.
   *
   * @invariant Currently valid only for local filesystems.
   *
   * @param path The path of the directory.
   */
  virtual void remove_dir_if_empty(const URI& uri) const;

  /**
   * Deletes a file.
   *
   * @param uri The URI of the file.
   */
  virtual void remove_file(const URI& uri) const = 0;

  /**
   * Retrieves the size of a file.
   *
   * @param uri The URI of the file.
   * @return The file size.
   */
  virtual uint64_t file_size(const URI& uri) const = 0;

  /**
   * Retrieves all the entries contained in the parent.
   *
   * @param parent The target directory to list.
   * @return All entries that are contained in the parent
   */
  virtual std::vector<tiledb::common::filesystem::directory_entry>
  ls_with_sizes(const URI& parent) const = 0;

  /**
   * Lists objects and object information that start with `prefix`, invoking
   * the ResultFilter on each entry collected.
   *
   * Currently this API is only supported for local files, S3, Azure, and GCS.
   *
   * @param parent The parent prefix to list sub-paths.
   * @param f The ResultFilter to invoke on each object for filtering.
   * @param recursive Whether to list the objects recursively.
   * @return Vector of results with each entry being a pair of the string URI
   *    and object size.
   */
  virtual LsObjects ls_filtered(
      const URI& parent, ResultFilter f, bool recursive) const;

  /**
   * Lists objects and object information that start with `prefix`, invoking
   * the ResultFilter on each entry collected.
   *
   * Currently this API is only supported for local files, S3, Azure, and GCS.
   *
   * @param parent The parent prefix to list sub-paths.
   * @param f The ResultFilterV2 to invoke on each object for filtering.
   * @param recursive Whether to list the objects recursively.
   * @return Vector of results with each entry being a pair of the string URI
   *    and object size.
   */
  virtual LsObjects ls_filtered_v2(
      const URI& parent, ResultFilterV2 f, bool recursive) const;

  /**
   * Renames a file.
   * Both URI must be of the same backend type. (e.g. both s3://, file://, etc)
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  virtual void move_file(const URI& old_uri, const URI& new_uri) const;

  /**
   * Renames a directory.
   * Both URI must be of the same backend type. (e.g. both s3://, file://, etc)
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  virtual void move_dir(const URI& old_uri, const URI& new_uri) const;

  /**
   * Copies a file.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  virtual void copy_file(const URI& old_uri, const URI& new_uri);

  /**
   * Copies directory.
   * Both URI must be of the same backend type. (e.g. both s3://, file://, etc)
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  virtual void copy_dir(const URI& old_uri, const URI& new_uri);

  /**
   * Whether or not to use the read-ahead cache.
   *
   * Defaults to `true` for object-store, `false` for local filesystems.
   */
  virtual bool use_read_ahead_cache() const;

  /**
   * Reads from a file.
   *
   * @param uri The URI of the file.
   * @param offset The offset where the read begins.
   * @param buffer The buffer to read into.
   * @param nbytes Number of bytes to read.
   */
  virtual uint64_t read(
      const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) = 0;

  /**
   * Flushes an object store file.
   *
   * @invariant Performs a sync on local filesystem files.
   *
   * @param uri The URI of the file.
   * @param finalize For S3 objects only. If `true`, flushes as a result of a
   * remote global order write `finalize()` call.
   */
  virtual void flush(
      const URI& uri, [[maybe_unused]] bool finalize = false) = 0;

  /**
   * Syncs a local file.
   *
   * @invariant Valid only for local filesystems.
   *
   * @param uri The URI of the file.
   */
  virtual void sync(const URI& uri) const;

  /**
   * Used in serialization of global order writes to set the multipart upload
   * state in the internal maps of cloud backends during deserialization.
   *
   * @invariant Currently valid only for S3 filesystems.
   *
   * @param uri The file uri used as key in the internal map of the backend.
   * @param state The multipart upload state info.
   */
  virtual void set_multipart_upload_state(
      const URI& uri, const MultiPartUploadState& state);

  /**
   * Used in serialization to share the multipart upload state among cloud
   * executors during global order writes.
   *
   * @invariant Currently valid only for S3 filesystems.
   *
   * @param uri The file uri used as key in the internal map of the backend.
   * @return A MultiPartUploadState object.
   */
  virtual std::optional<MultiPartUploadState> multipart_upload_state(
      const URI& uri);

  /**
   * Used in remote global order writes to flush the internal
   * in-memory buffer for an URI that backends maintain to modulate the
   * frequency of multipart upload requests.
   *
   * @invariant Currently valid only for S3 filesystems.
   *
   * @param uri The file uri identifying the backend file buffer.
   */
  virtual void flush_multipart_file_buffer(const URI& uri);

  /**
   * Writes the contents of a buffer into a file.
   *
   * @param uri The URI of the file.
   * @param buffer The buffer to write from.
   * @param buffer_size The buffer size.
   * @param remote_global_order_write For S3 objects only. If `true`, performs
   * a remote global order write.
   */
  virtual void write(
      const URI& uri,
      const void* buffer,
      uint64_t buffer_size,
      [[maybe_unused]] bool remote_global_order_write = false) = 0;

  /**
   * Checks if an object store bucket exists.
   *
   * @invariant Valid only for object store filesystems.
   *
   * @param uri The name of the object store bucket.
   * @return True if the bucket exists, false otherwise.
   */
  virtual bool is_bucket(const URI& uri) const;

  /**
   * Checks if an object-store bucket is empty.
   *
   * @invariant Valid only for object store filesystems.
   *
   * @param uri The name of the object store bucket.
   * @return True if the bucket is empty, false otherwise.
   */
  virtual bool is_empty_bucket(const URI& uri) const;

  /**
   * Creates an object store bucket.
   *
   * @invariant Valid only for object store filesystems.
   *
   * @param uri The name of the bucket to be created.
   */
  virtual void create_bucket(const URI& uri) const;

  /**
   * Deletes an object store bucket.
   *
   * @invariant Valid only for object store filesystems.
   *
   * @param uri The name of the bucket to be deleted.
   */
  virtual void remove_bucket(const URI& uri) const;

  /**
   * Deletes the contents of an object store bucket.
   *
   * @invariant Valid only for object store filesystems.
   *
   * @param uri The name of the bucket to be emptied.
   */
  virtual void empty_bucket(const URI& uri) const;
};

}  // namespace tiledb::sm

#endif  // TILEDB_FILESYSTEMBASE_H
