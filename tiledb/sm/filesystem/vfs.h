/**
 * @file   vfs.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * This file declares the VFS class.
 */

#ifndef TILEDB_VFS_H
#define TILEDB_VFS_H

#include <functional>
#include <list>
#include <set>
#include <string>
#include <vector>

#include "filesystem_base.h"
#include "ls_scanner.h"
#include "tiledb/common/common.h"
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/common/macros.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/cache/lru_cache.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/mem_filesystem.h"
#include "tiledb/sm/misc/cancelable_tasks.h"
#include "tiledb/sm/stats/stats.h"
#include "uri.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#ifdef HAVE_GCS
#include "tiledb/sm/filesystem/gcs.h"
#endif  // HAVE_GCS

#ifdef HAVE_S3
#include "tiledb/sm/filesystem/s3.h"
#endif  // HAVE_S3

#ifdef HAVE_HDFS
#include "tiledb/sm/filesystem/hdfs_filesystem.h"
#endif  // HAVE_HDFS

#ifdef HAVE_AZURE
#include "tiledb/sm/filesystem/azure.h"
#endif  // HAVE_AZURE

namespace tiledb::common {
class LogDurationInstrument;
}

using namespace tiledb::common;
using tiledb::common::filesystem::directory_entry;

namespace tiledb::sm {

namespace filesystem {
class VFSException : public StatusException {
 public:
  explicit VFSException(const std::string& message)
      : StatusException("VFS", message) {
  }
};

class BuiltWithout : public VFSException {
 public:
  explicit BuiltWithout(const std::string& filesystem)
      : VFSException("TileDB was built without " + filesystem + "support") {
  }
};

class UnsupportedOperation : public VFSException {
 public:
  explicit UnsupportedOperation(const std::string& operation)
      : VFSException(operation + " across filesystems is not supported yet") {
  }
};

class UnsupportedURI : public VFSException {
 public:
  explicit UnsupportedURI(const std::string& uri)
      : VFSException("Unsupported URI scheme: " + uri) {
  }
};

// Local filesystem is always enabled
static constexpr bool local_enabled = true;

#ifdef _WIN32
static constexpr bool windows_enabled = true;
static constexpr bool posix_enabled = false;
#else
static constexpr bool windows_enabled = false;
static constexpr bool posix_enabled = true;
#endif

#ifdef HAVE_GCS
static constexpr bool gcs_enabled = true;
#else
static constexpr bool gcs_enabled = false;
#endif  // HAVE_GCS

#ifdef HAVE_S3
static constexpr bool s3_enabled = true;
#else
static constexpr bool s3_enabled = false;
#endif  // HAVE_S3

#ifdef HAVE_HDFS
static constexpr bool hdfs_enabled = true;
#else
static constexpr bool hdfs_enabled = false;
#endif  // HAVE_HDFS

#ifdef HAVE_AZURE
static constexpr bool azure_enabled = true;
#else
static constexpr bool azure_enabled = false;
#endif  // HAVE_AZURE
}  // namespace filesystem

class Tile;

enum class Filesystem : uint8_t;
enum class VFSMode : uint8_t;

/** The VFS configuration parameters. */
struct VFSParameters {
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  enum struct ReadLoggingMode {
    DISABLED,
    FRAGMENTS,
    FRAGMENT_FILES,
    ALL_FILES,
    ALL_READS,
    ALL_READS_ALWAYS
  };

  VFSParameters() = delete;

  VFSParameters(const Config& config)
      : min_parallel_size_(
            config.get<uint64_t>("vfs.min_parallel_size").value())
      , read_ahead_cache_size_(
            config.get<uint64_t>("vfs.read_ahead_cache_size").value())
      , read_ahead_size_(config.get<uint64_t>("vfs.read_ahead_size").value())
      , log_operations_(
            config.get<bool>("vfs.log_operations", Config::must_find))
      , read_logging_mode_(ReadLoggingMode::DISABLED) {
    auto log_mode = config.get<std::string>("vfs.read_logging_mode").value();
    if (log_mode == "") {
      read_logging_mode_ = ReadLoggingMode::DISABLED;
    } else if (log_mode == "fragments") {
      read_logging_mode_ = ReadLoggingMode::FRAGMENTS;
    } else if (log_mode == "fragment_files") {
      read_logging_mode_ = ReadLoggingMode::FRAGMENT_FILES;
    } else if (log_mode == "all_files") {
      read_logging_mode_ = ReadLoggingMode::ALL_FILES;
    } else if (log_mode == "all_reads") {
      read_logging_mode_ = ReadLoggingMode::ALL_READS;
    } else if (log_mode == "all_reads_always") {
      read_logging_mode_ = ReadLoggingMode::ALL_READS_ALWAYS;
    } else {
      std::stringstream ss;
      ss << "Invalid read logging mode '" << log_mode << "'. "
         << "Use one of 'fragments', 'fragment_files', 'all_files', "
         << "'all_reads', or 'all_reads_always'. Read logging is currently "
         << "disabled.";
      LOG_WARN(ss.str());
    }
  };

  ~VFSParameters() = default;

  /** The minimum number of bytes in a parallel operation. */
  uint64_t min_parallel_size_;

  /** The byte size of the read-ahead cache. */
  uint64_t read_ahead_cache_size_;

  /** The byte size to read-ahead for each read. */
  uint64_t read_ahead_size_;

  /** Whether to log all VFS operations. */
  bool log_operations_;

  /** The read logging mode to use. */
  ReadLoggingMode read_logging_mode_;
};

/** The base parameters for class VFS. */
struct VFSBase {
  VFSBase() = delete;

  VFSBase(stats::Stats* const parent_stats)
      : stats_(parent_stats->create_child("VFS")){};

  ~VFSBase() = default;

 protected:
  /** The class stats. */
  stats::Stats* stats_;
};

/** The HDFS filesystem. */
#ifdef HAVE_HDFS
class HDFS_within_VFS {
  /** Private member variable */
  HDFS hdfs_;

 protected:
  template <typename... Args>
  HDFS_within_VFS(Args&&... args)
      : hdfs_(std::forward<Args>(args)...) {
  }

  /** Protected accessor for the HDFS object. */
  inline HDFS& hdfs() {
    return hdfs_;
  }

  /** Protected accessor for the const HDFS object. */
  inline const HDFS& hdfs() const {
    return hdfs_;
  }
};
#else
class HDFS_within_VFS {
 protected:
  template <typename... Args>
  HDFS_within_VFS(Args&&...) {
  }  // empty constructor
};
#endif

/** The S3 filesystem. */
#ifdef HAVE_S3
class S3_within_VFS {
  /** Private member variable */
  S3 s3_;

 protected:
  template <typename... Args>
  S3_within_VFS(Args&&... args)
      : s3_(std::forward<Args>(args)...) {
  }

  /** Protected accessor for the S3 object. */
  inline S3& s3() {
    return s3_;
  }

  /** Protected accessor for the const S3 object. */
  inline const S3& s3() const {
    return s3_;
  }
};
#else
class S3_within_VFS {
 protected:
  template <typename... Args>
  S3_within_VFS(Args&&...) {
  }  // empty constructor
};
#endif

/**
 * This class implements a virtual filesystem that directs filesystem-related
 * function execution to the appropriate backend based on the input URI.
 */
class VFS : private VFSBase, protected HDFS_within_VFS, S3_within_VFS {
 public:
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
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor.
   * @param parent_stats The parent stats to inherit from.
   * @param logger The logger to use. Optional, can be nullptr.
   * @param compute_tp Thread pool for compute-bound tasks.
   * @param io_tp Thread pool for io-bound tasks.
   * @param config Configuration parameters.
   **/
  VFS(stats::Stats* parent_stats,
      Logger* logger,
      ThreadPool* compute_tp,
      ThreadPool* io_tp,
      const Config& config);

  /** Destructor. */
  ~VFS() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(VFS);
  DISABLE_MOVE_AND_MOVE_ASSIGN(VFS);

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns the absolute path of the input string (mainly useful for
   * posix URI's).
   *
   * @param path The input path.
   * @return The string with the absolute path.
   */
  static std::string abs_path(std::string_view path);

  /**
   * Return a config object containing the VFS parameters. All other non-VFS
   * parameters are set to default values.
   */
  Config config() const;

  /**
   * Creates a directory.
   *
   * - On S3, this is a noop.
   * - On all other backends, if the directory exists, the function
   *   just succeeds without doing anything.
   *
   * @param uri The URI of the directory.
   * @return Status
   */
  Status create_dir(const URI& uri) const;

  /**
   * Creates an empty file.
   *
   * @param uri The URI of the file.
   * @return Status
   */
  Status touch(const URI& uri) const;

  /**
   * Cancels all background or queued tasks.
   */
  Status cancel_all_tasks();

  /**
   * Creates an object store bucket.
   *
   * @param uri The name of the bucket to be created.
   * @return Status
   */
  Status create_bucket(const URI& uri) const;

  /**
   * Returns the size of the files in the input directory.
   * This function is **recursive**, i.e., it will calculate
   * the sum of the files in the entire directory tree rooted
   * at `dir_name`.
   *
   * @param dir_name The input directory.
   * @param dir_size The directory size to be retrieved, as the
   *     sum of the files one level deep.
   * @return Status
   */
  Status dir_size(const URI& dir_name, uint64_t* dir_size) const;

  /**
   * Deletes an object store bucket.
   *
   * @param uri The name of the bucket to be deleted.
   * @return Status
   */
  Status remove_bucket(const URI& uri) const;

  /**
   * Deletes the contents of an object store bucket.
   *
   * @param uri The name of the bucket to be emptied.
   * @return Status
   */
  Status empty_bucket(const URI& uri) const;

  /**
   * Removes a given directory (recursive)
   *
   * @param uri The uri of the directory to be removed
   * @return Status
   */
  Status remove_dir(const URI& uri) const;

  /**
   * Removes a given empty directory. No exceptions are raised if the directory
   * was not empty.
   *
   * This function does nothing when the URI points to an object store.
   *
   * @param uri The uri of the directory to be removed
   */
  void remove_dir_if_empty(const URI& uri) const;

  /**
   * Deletes directories in parallel from the given vector of directories.
   *
   * @param compute_tp The compute-bound ThreadPool.
   * @param uris The URIs of the directories.
   */
  void remove_dirs(ThreadPool* compute_tp, const std::vector<URI>& uris) const;

  /**
   * Deletes a file.
   *
   * @param uri The URI of the file.
   * @return Status
   */
  Status remove_file(const URI& uri) const;

  /**
   * Deletes files in parallel from the given vector of files.
   *
   * @param compute_tp The compute-bound ThreadPool.
   * @param uris The URIs of the files.
   */
  void remove_files(ThreadPool* compute_tp, const std::vector<URI>& uris) const;

  /**
   * Deletes files in parallel from the given vector of timestamped files.
   *
   * @param compute_tp The compute-bound ThreadPool.
   * @param uris The TimestampedURIs of the files.
   */
  void remove_files(
      ThreadPool* compute_tp, const std::vector<TimestampedURI>& uris) const;

  /**
   * Retrieves the size of a file.
   *
   * @param uri The URI of the file.
   * @param size The file size to be retrieved.
   * @return Status
   */
  Status file_size(const URI& uri, uint64_t* size) const;

  /**
   * Checks if a directory exists.
   *
   * @param uri The URI of the directory.
   * @param is_dir Set to `true` if the directory exists and `false` otherwise.
   * @return Status
   *
   * @note For S3, this function will return `true` if there is an object
   *     with prefix `uri/` (TileDB will append `/` internally to `uri`
   *     only if it does not exist), and `false` othewise.
   */
  Status is_dir(const URI& uri, bool* is_dir) const;

  /**
   * Checks if a file exists.
   *
   * @param uri The URI of the file.
   * @param is_file Set to `true` if the file exists and `false` otherwise.
   * @return Status
   */
  Status is_file(const URI& uri, bool* is_file) const;

  /**
   * Checks if an object store bucket exists.
   *
   * @param uri The name of the object store bucket.
   * @return is_bucket Set to `true` if the bucket exists and `false` otherwise.
   * @return Status
   */
  Status is_bucket(const URI& uri, bool* is_bucket) const;

  /**
   * Checks if an object-store bucket is empty.
   *
   * @param uri The name of the object store bucket.
   * @param is_empty Set to `true` if the bucket is empty and `false` otherwise.
   */
  Status is_empty_bucket(const URI& uri, bool* is_empty) const;

  /**
   * Retrieves all the URIs that have the first input as parent.
   *
   * @param parent The target directory to list.
   * @param uris The URIs that are contained in the parent.
   * @return Status
   */
  Status ls(const URI& parent, std::vector<URI>* uris) const;

  /**
   * Retrieves all the entries contained in the parent.
   *
   * @param parent The target directory to list.
   * @return All entries that are contained in the parent
   */
  std::vector<directory_entry> ls_with_sizes(const URI& parent) const;

  /**
   * Lists objects and object information that start with `prefix`, invoking
   * the FilePredicate on each entry collected and the DirectoryPredicate on
   * common prefixes for pruning.
   *
   * Currently this API is only supported for local files, S3 and Azure.
   *
   * @param parent The parent prefix to list sub-paths.
   * @param f The FilePredicate to invoke on each object for filtering.
   * @param d The DirectoryPredicate to invoke on each common prefix for
   *    pruning. This is currently unused, but is kept here for future support.
   * @param recursive Whether to list the objects recursively.
   * @return Vector of results with each entry being a pair of the string URI
   *    and object size.
   */
  template <FilePredicate F, DirectoryPredicate D = DirectoryFilter>
  LsObjects ls_filtered(
      const URI& parent,
      [[maybe_unused]] F f,
      [[maybe_unused]] D d,
      bool recursive) const {
    LsObjects results;
    try {
      if (parent.is_file()) {
#ifdef _WIN32
        results = win_.ls_filtered(parent, f, d, recursive);
#else
        results = posix_.ls_filtered(parent, f, d, recursive);
#endif
      } else if (parent.is_s3()) {
#ifdef HAVE_S3
        results = s3().ls_filtered(parent, f, d, recursive);
#else
        throw filesystem::VFSException("TileDB was built without S3 support");
#endif
      } else if (parent.is_gcs()) {
#ifdef HAVE_GCS
        results = gcs_.ls_filtered(parent, f, d, recursive);
#else
        throw filesystem::VFSException("TileDB was built without GCS support");
#endif
      } else if (parent.is_azure()) {
#ifdef HAVE_AZURE
        results = azure_.ls_filtered(parent, f, d, recursive);
#else
        throw filesystem::VFSException(
            "TileDB was built without Azure support");
#endif
      } else if (parent.is_hdfs()) {
#ifdef HAVE_HDFS
        throw filesystem::VFSException(
            "Recursive ls over " + parent.backend_name() +
            " storage backend is not supported.");
#else
        throw filesystem::VFSException("TileDB was built without HDFS support");
#endif
      } else {
        throw filesystem::VFSException(
            "Recursive ls over " + parent.backend_name() +
            " storage backend is not supported.");
      }
    } catch (LsStopTraversal& e) {
      // Do nothing, callback signaled to stop traversal.
    } catch (...) {
      // Rethrow exception thrown by the callback.
      throw;
    }
    return results;
  }

  /**
   * Recursively lists objects and object information that start with `prefix`,
   * invoking the FilePredicate on each entry collected and the
   * DirectoryPredicate on common prefixes for pruning.
   *
   * Currently this API is only supported for local files, S3, Azure and GCS.
   *
   * @param parent The parent prefix to list sub-paths.
   * @param f The FilePredicate to invoke on each object for filtering.
   * @param d The DirectoryPredicate to invoke on each common prefix for
   *    pruning. This is currently unused, but is kept here for future support.
   * @return Vector of results with each entry being a pair of the string URI
   *    and object size.
   */
  template <FilePredicate F, DirectoryPredicate D = DirectoryFilter>
  LsObjects ls_recursive(
      const URI& parent,
      [[maybe_unused]] F f,
      [[maybe_unused]] D d = accept_all_dirs) const {
    return ls_filtered(parent, f, d, true);
  }

  /**
   * Renames a file.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   * @return Status
   */
  Status move_file(const URI& old_uri, const URI& new_uri);

  /**
   * Renames a directory.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   * @return Status
   */
  Status move_dir(const URI& old_uri, const URI& new_uri);

  /**
   * Copies a file.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   * @return Status
   */
  Status copy_file(const URI& old_uri, const URI& new_uri);

  /**
   * Copies directory.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   * @return Status
   */
  Status copy_dir(const URI& old_uri, const URI& new_uri);

  /**
   * Reads from a file.
   *
   * @param uri The URI of the file.
   * @param offset The offset where the read begins.
   * @param buffer The buffer to read into.
   * @param nbytes Number of bytes to read.
   * @param use_read_ahead Whether to use the read-ahead cache.
   * @return Status
   */
  Status read(
      const URI& uri,
      uint64_t offset,
      void* buffer,
      uint64_t nbytes,
      bool use_read_ahead = true);

  /** Checks if a given filesystem is supported. */
  bool supports_fs(Filesystem fs) const;

  /** Checks if the backend required to access the given URI is supported. */
  bool supports_uri_scheme(const URI& uri) const;

  /**
   * Syncs (flushes) a file. Note that for S3 this is a noop.
   *
   * @param uri The URI of the file.
   * @return Status
   */
  Status sync(const URI& uri);

  /**
   * Opens a file in a given mode.
   *
   *
   * @param uri The URI of the file.
   * @param mode The mode in which the file is opened:
   *     - READ <br>
   *       The file is opened for reading. An error is returned if the file
   *       does not exist.
   *     - WRITE <br>
   *       The file is opened for writing. If the file exists, it will be
   *       overwritten.
   *     - APPEND <b>
   *       The file is opened for writing. If the file exists, the write
   *       will start from the end of the file. Note that S3 does not
   *       support this operation and, thus, an error will be thrown in
   *       that case.
   * @return Status
   */
  Status open_file(const URI& uri, VFSMode mode);

  /**
   * Closes a file, flushing its contents to persistent storage.
   *
   * @param uri The URI of the file.
   * @return Status
   */
  Status close_file(const URI& uri);

  /**
   * Closes a file, flushing its contents to persistent storage.
   * This function has special S3 logic tailored to work best with remote
   * global order writes.
   *
   * @param uri The URI of the file.
   */
  void finalize_and_close_file(const URI& uri);

  /**
   * Writes the contents of a buffer into a file.
   *
   * @param uri The URI of the file.
   * @param buffer The buffer to write from.
   * @param buffer_size The buffer size.
   * @param remote_global_order_write Remote global order write
   * @return Status
   */
  Status write(
      const URI& uri,
      const void* buffer,
      uint64_t buffer_size,
      bool remote_global_order_write = false);

  /**
   * Used in serialization to share the multipart upload state
   * among cloud executors during global order writes
   *
   * @param uri The file uri used as key in the internal map of the backend
   * @return A pair of status and VFS::MultiPartUploadState object.
   */
  std::pair<Status, std::optional<MultiPartUploadState>> multipart_upload_state(
      const URI& uri);

  /**
   * Used in serialization of global order writes to set the multipart upload
   * state in the internal maps of cloud backends during deserialization
   *
   * @param uri The file uri used as key in the internal map of the backend
   * @param state The multipart upload state info
   * @return Status
   */
  Status set_multipart_upload_state(
      const URI& uri, const MultiPartUploadState& state);

  /**
   * Used in remote global order writes to flush the internal
   * in-memory buffer for an URI that backends maintain to modulate the
   * frequency of multipart upload requests.
   *
   * @param uri The file uri identifying the backend file buffer
   * @return Status
   */
  Status flush_multipart_file_buffer(const URI& uri);

  inline stats::Stats* stats() const {
    return stats_;
  }

 private:
  /* ********************************* */
  /*        PRIVATE DATATYPES          */
  /* ********************************* */

  /**
   * Represents a sub-range of data within a URI file at a
   * specific file offset.
   */
  struct ReadAheadBuffer {
    /* ********************************* */
    /*            CONSTRUCTORS           */
    /* ********************************* */

    /** Value Constructor. */
    ReadAheadBuffer(const uint64_t offset, Buffer&& buffer)
        : offset_(offset)
        , buffer_(std::move(buffer)) {
    }

    /** Move Constructor. */
    ReadAheadBuffer(ReadAheadBuffer&& other)
        : offset_(other.offset_)
        , buffer_(std::move(other.buffer_)) {
    }

    /* ********************************* */
    /*             OPERATORS             */
    /* ********************************* */

    /** Move-Assign Operator. */
    ReadAheadBuffer& operator=(ReadAheadBuffer&& other) {
      offset_ = other.offset_;
      buffer_ = std::move(other.buffer_);
      return *this;
    }

    DISABLE_COPY_AND_COPY_ASSIGN(ReadAheadBuffer);

    /* ********************************* */
    /*             ATTRIBUTES            */
    /* ********************************* */

    /** The offset within the associated URI. */
    uint64_t offset_;

    /** The buffered data at `offset`. */
    Buffer buffer_;
  };

  /**
   * An LRU cache of `ReadAheadBuffer` objects keyed by a URI string.
   */
  class ReadAheadCache : public LRUCache<std::string, ReadAheadBuffer> {
   public:
    /* ********************************* */
    /*     CONSTRUCTORS & DESTRUCTORS    */
    /* ********************************* */

    /** Constructor. */
    ReadAheadCache(const uint64_t max_cached_buffers)
        : LRUCache(max_cached_buffers) {
    }

    /** Destructor. */
    virtual ~ReadAheadCache() = default;

    /* ********************************* */
    /*                API                */
    /* ********************************* */

    /**
     * Attempts to read a buffer from the cache.
     *
     * @param uri The URI associated with the buffer to cache.
     * @param offset The offset that buffer starts at within the URI.
     * @param buffer The buffer to cache.
     * @param nbytes The number of bytes within the buffer.
     * @param success True if `buffer` was read from the cache.
     * @return Status
     */
    Status read(
        const URI& uri,
        const uint64_t offset,
        void* const buffer,
        const uint64_t nbytes,
        bool* const success) {
      assert(success);
      *success = false;

      // Store the URI's string representation.
      const std::string uri_str = uri.to_string();

      // Protect access to the derived LRUCache routines.
      std::lock_guard<std::mutex> lg(lru_mtx_);

      // Check that a cached buffer exists for `uri`.
      if (!has_item(uri_str))
        return Status::Ok();

      // Store a reference to the cached buffer.
      const ReadAheadBuffer* const ra_buffer = get_item(uri_str);

      // Check that the read offset is not below the offset of
      // the cached buffer.
      if (offset < ra_buffer->offset_)
        return Status::Ok();

      // Calculate the offset within the cached buffer that corresponds
      // to the requested read offset.
      const uint64_t offset_in_buffer = offset - ra_buffer->offset_;

      // Check that both the start and end positions of the requested
      // read range reside within the cached buffer.
      if (offset_in_buffer + nbytes > ra_buffer->buffer_.size())
        return Status::Ok();

      // Copy the subrange of the cached buffer that satisfies the caller's
      // read request back into their output `buffer`.
      std::memcpy(
          buffer,
          static_cast<uint8_t*>(ra_buffer->buffer_.data()) + offset_in_buffer,
          nbytes);

      // Touch the item to make it the most recently used item.
      touch_item(uri_str);

      *success = true;
      return Status::Ok();
    }

    /**
     * Writes a cached buffer for the given uri.
     *
     * @param uri The URI associated with the buffer to cache.
     * @param offset The offset that buffer starts at within the URI.
     * @param buffer The buffer to cache.
     * @return Status
     */
    Status insert(const URI& uri, const uint64_t offset, Buffer&& buffer) {
      // Protect access to the derived LRUCache routines.
      std::lock_guard<std::mutex> lg(lru_mtx_);

      const uint64_t size = buffer.size();
      ReadAheadBuffer ra_buffer(offset, std::move(buffer));
      LRUCache<std::string, ReadAheadBuffer>::insert(
          uri.to_string(), std::move(ra_buffer), size);
      return Status::Ok();
    }

   private:
    /* ********************************* */
    /*         PRIVATE ATTRIBUTES        */
    /* ********************************* */

    // Protects LRUCache routines.
    std::mutex lru_mtx_;
  };

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

#ifdef HAVE_AZURE
  Azure azure_;
#endif

#ifdef HAVE_GCS
  GCS gcs_;
#endif

#ifdef _WIN32
  Win win_;
#else
  Posix posix_;
#endif

  /** The in-memory filesystem which is always supported */
  MemFilesystem memfs_;

  /**
   * Config.
   *
   * Note: This object is stored on the VFS for:
   * use of API 'tiledb_vfs_get_config'.
   * pass-by-reference initialization of filesystems' config_ member variables.
   **/
  Config config_;

  /** Logger. */
  Logger* logger_;

  /** The set with the supported filesystems. */
  std::set<Filesystem> supported_fs_;

  /** Thread pool for compute-bound tasks. */
  ThreadPool* compute_tp_;

  /** Thread pool for io-bound tasks. */
  ThreadPool* io_tp_;

  /** Wrapper for tracking and canceling certain tasks on 'thread_pool' */
  CancelableTasks cancelable_tasks_;

  /** The read-ahead cache. */
  tdb_unique_ptr<ReadAheadCache> read_ahead_cache_;

  /** The VFS configuration parameters. */
  VFSParameters vfs_params_;

  /** The URIs previously read by this instance. */
  std::unordered_set<std::string> reads_logged_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Reads from a file by calling the specific backend read function.
   *
   * @param uri The URI of the file.
   * @param offset The offset where the read begins.
   * @param buffer The buffer to read into.
   * @param nbytes Number of bytes to read.
   * @param use_read_ahead Whether to use the read-ahead cache.
   * @return Status
   */
  Status read_impl(
      const URI& uri,
      uint64_t offset,
      void* buffer,
      uint64_t nbytes,
      bool use_read_ahead);

  /**
   * Executes a read, using the read-ahead cache as necessary.
   *
   * @param read_fn The read routine to execute.
   * @param uri The URI of the file.
   * @param offset The offset where the read begins.
   * @param buffer The buffer to read into.
   * @param nbytes Number of bytes to read.
   * @param use_read_ahead Whether to use the read-ahead cache.
   * @return Status
   */
  Status read_ahead_impl(
      const std::function<Status(
          const URI&, off_t, void*, uint64_t, uint64_t, uint64_t*)>& read_fn,
      const URI& uri,
      const uint64_t offset,
      void* const buffer,
      const uint64_t nbytes,
      const bool use_read_ahead);

  /**
   * Retrieves the backend-specific max number of parallel operations for VFS
   * read.
   */
  Status max_parallel_ops(const URI& uri, uint64_t* ops) const;

  /**
   * Log a read operation. The format of the log message depends on the
   * config value `vfs.read_logging_mode`.
   *
   * @param uri The URI being read.
   * @param offset The offset being read from.
   * @param nbytes The number of bytes requested.
   */
  void log_read(const URI& uri, uint64_t offset, uint64_t nbytes);

  /**
   * Creates a LogDurationInstrument, if enabled in config.
   *
   * @param uri The URI of the object being operated on.
   * @param operation_name The name of the operation. Defaults to the name of
   * the calling function.
   */
  optional<LogDurationInstrument> make_log_duration_instrument(
      const URI& uri, const std::string_view operation_name) const;

  /**
   * Creates a LogDurationInstrument, if enabled in config.
   *
   * @param uri The URI of the object being operated on.
   * @param operation_name The name of the operation. Defaults to the name of
   * the calling function.
   */
  optional<LogDurationInstrument> make_log_duration_instrument(
      const URI& source,
      const URI& destination,
      const std::string_view operation_name) const;
};

}  // namespace tiledb::sm

#endif  // TILEDB_VFS_H
