/**
 * @file   status.h
 *
 * @section LICENSE
 *
 * The BSD License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 *            Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of Google Inc. nor the names of its contributors may be used
 * to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * @section DESCRIPTION
 *
 * A Status object encapsulates the result of an operation.  It may indicate
 * success, or it may indicate an error with an associated error message.
 *
 * Multiple threads can invoke const methods on a Status without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same Status must use
 * external synchronization.
 *
 * This code has been adopted from the LevelDB project
 * (https://github.com/google/leveldb).
 */

#ifndef TILEDB_STATUS_H
#define TILEDB_STATUS_H

#include <cstdint>
#include <cstring>
#include <string>

#include "tiledb/common/heap_memory.h"

namespace tiledb {
namespace common {

#define RETURN_NOT_OK(s) \
  do {                   \
    Status _s = (s);     \
    if (!_s.ok()) {      \
      return _s;         \
    }                    \
  } while (false)

#define RETURN_NOT_OK_ELSE(s, else_) \
  do {                               \
    Status _s = (s);                 \
    if (!_s.ok()) {                  \
      else_;                         \
      return _s;                     \
    }                                \
  } while (false)

enum class StatusCode : char {
  Ok,
  Error,
  StorageManager,
  FragmentMetadata,
  ArraySchema,
  Metadata,
  IO,
  Mem,
  GZip,
  Compression,
  Tile,
  TileIO,
  ChunkedBuffer,
  Buffer,
  Query,
  ValidityVector,
  VFS,
  ConstBuffer,
  Dimension,
  Domain,
  Consolidator,
  LRUCache,
  KV,
  KVItem,
  KVIter,
  Config,
  Utils,
  FS_S3,
  FS_AZURE,
  FS_GCS,
  FS_HDFS,
  FS_MEM,
  Attribute,
  WriteCellSlabIter,
  Reader,
  Writer,
  PreallocatedBuffer,
  Filter,
  Encryption,
  Array,
  VFSFileHandleError,
  ContextError,
  SubarrayError,
  SubarrayPartitionerError,
  RTreeError,
  CellSlabIterError,
  RestError,
  SerializationError,
  ChecksumError,
  ThreadPoolError,
  FragmentInfoError,
  QueryConditionError
};

class Status {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor with success status (empty state). */
  Status()
      : state_(nullptr) {
  }

  /** Consturctor with a code and error message. */
  Status(StatusCode code, const std::string& msg)
      : Status(code, msg, -1) {
  }

  /** Destructor. */
  ~Status() {
    tdb_delete_array(state_);
  }

  /** Copy the specified status. */
  Status(const Status& s);

  /** Assign status. */
  void operator=(const Status& s);

  /**  Return a success status **/
  static Status Ok() {
    return Status();
  }

  /**  Return a generic Error class Status **/
  static Status Error(const std::string& msg) {
    return Status(StatusCode::Error, msg, -1);
  }

  /** Return a StorageManager error class Status with a given message **/
  static Status StorageManagerError(const std::string& msg) {
    return Status(StatusCode::StorageManager, msg, -1);
  }

  /** Return a FragmentMetadata error class Status with a given message **/
  static Status FragmentMetadataError(const std::string& msg) {
    return Status(StatusCode::FragmentMetadata, msg, -1);
  }

  /** Return a ArraySchema error class Status with a given message **/
  static Status ArraySchemaError(const std::string& msg) {
    return Status(StatusCode::ArraySchema, msg, -1);
  }

  /** Return a Metadata error class Status with a given message **/
  static Status MetadataError(const std::string& msg) {
    return Status(StatusCode::Metadata, msg, -1);
  }

  /** Return a IO error class Status with a given message **/
  static Status IOError(const std::string& msg) {
    return Status(StatusCode::IO, msg, -1);
  }

  /** Return a Memory error class Status with a given message **/
  static Status MemError(const std::string& msg) {
    return Status(StatusCode::Mem, msg, -1);
  }

  /** Return a ArrayError error class Status with a given message **/
  static Status GZipError(const std::string& msg) {
    return Status(StatusCode::GZip, msg, -1);
  }

  /** Return a ArrayError error class Status with a given message **/
  static Status ChecksumError(const std::string& msg) {
    return Status(StatusCode::ChecksumError, msg, -1);
  }

  /** Return a ArrayError error class Status with a given message **/
  static Status CompressionError(const std::string& msg) {
    return Status(StatusCode::Compression, msg, -1);
  }

  /** Return a TileError error class Status with a given message **/
  static Status TileError(const std::string& msg) {
    return Status(StatusCode::Tile, msg, -1);
  }

  /** Return a TileIOError error class Status with a given message **/
  static Status TileIOError(const std::string& msg) {
    return Status(StatusCode::TileIO, msg, -1);
  }

  /** Return a ChunkedBufferError error class Status with a given message **/
  static Status ChunkedBufferError(const std::string& msg) {
    return Status(StatusCode::ChunkedBuffer, msg, -1);
  }

  /** Return a BufferError error class Status with a given message **/
  static Status BufferError(const std::string& msg) {
    return Status(StatusCode::Buffer, msg, -1);
  }

  /** Return a QueryError error class Status with a given message **/
  static Status QueryError(const std::string& msg) {
    return Status(StatusCode::Query, msg, -1);
  }

  /** Return a ValidityVectorError error class Status with a given message **/
  static Status ValidityVectorError(const std::string& msg) {
    return Status(StatusCode::ValidityVector, msg, -1);
  }

  /** Return a VFSError error class Status with a given message **/
  static Status VFSError(const std::string& msg) {
    return Status(StatusCode::VFS, msg, -1);
  }

  /** Return a ConstBufferError error class Status with a given message **/
  static Status ConstBufferError(const std::string& msg) {
    return Status(StatusCode::ConstBuffer, msg, -1);
  }

  /** Return a DimensionError error class Status with a given message **/
  static Status DimensionError(const std::string& msg) {
    return Status(StatusCode::Dimension, msg, -1);
  }

  /** Return a DomainError error class Status with a given message **/
  static Status DomainError(const std::string& msg) {
    return Status(StatusCode::Domain, msg, -1);
  }

  /** Return a ConsolidatorError error class Status with a given message **/
  static Status ConsolidatorError(const std::string& msg) {
    return Status(StatusCode::Consolidator, msg, -1);
  }

  /** Return a LRUCacheError error class Status with a given message **/
  static Status LRUCacheError(const std::string& msg) {
    return Status(StatusCode::LRUCache, msg, -1);
  }

  /** Return a KVError error class Status with a given message **/
  static Status KVError(const std::string& msg) {
    return Status(StatusCode::KV, msg, -1);
  }

  /** Return a KVItemError error class Status with a given message **/
  static Status KVItemError(const std::string& msg) {
    return Status(StatusCode::KVItem, msg, -1);
  }

  /** Return a KVIterError error class Status with a given message **/
  static Status KVIterError(const std::string& msg) {
    return Status(StatusCode::KVIter, msg, -1);
  }

  /** Return a ConfigError error class Status with a given message **/
  static Status ConfigError(const std::string& msg) {
    return Status(StatusCode::Config, msg, -1);
  }

  /** Return a UtilsError error class Status with a given message **/
  static Status UtilsError(const std::string& msg) {
    return Status(StatusCode::Utils, msg, -1);
  }

  /** Return a UtilsError error class Status with a given message **/
  static Status S3Error(const std::string& msg) {
    return Status(StatusCode::FS_S3, msg, -1);
  }

  /** Return a UtilsError error class Status with a given message **/
  static Status AzureError(const std::string& msg) {
    return Status(StatusCode::FS_AZURE, msg, -1);
  }

  /** Return a UtilsError error class Status with a given message **/
  static Status GCSError(const std::string& msg) {
    return Status(StatusCode::FS_GCS, msg, -1);
  }

  /** Return a UtilsError error class Status with a given message **/
  static Status HDFSError(const std::string& msg) {
    return Status(StatusCode::FS_HDFS, msg, -1);
  }

  /** Return a MemFSError error class Status with a given message **/
  static Status MemFSError(const std::string& msg) {
    return Status(StatusCode::FS_MEM, msg, -1);
  }

  /** Return a AttributeError error class Status with a given message **/
  static Status AttributeError(const std::string& msg) {
    return Status(StatusCode::Attribute, msg, -1);
  }

  /** Return a WriteCellSlabIterError error class Status with a given message
   * **/
  static Status WriteCellSlabIterError(const std::string& msg) {
    return Status(StatusCode::WriteCellSlabIter, msg, -1);
  }

  /** Return a ReaderError error class Status with a given message **/
  static Status ReaderError(const std::string& msg) {
    return Status(StatusCode::Reader, msg, -1);
  }

  /** Return a WriterError error class Status with a given message **/
  static Status WriterError(const std::string& msg) {
    return Status(StatusCode::Writer, msg, -1);
  }

  /** Return a PreallocatedBufferError error class Status with a given message
   * **/
  static Status PreallocatedBufferError(const std::string& msg) {
    return Status(StatusCode::PreallocatedBuffer, msg, -1);
  }

  /** Return a FilterError error class Status with a given message **/
  static Status FilterError(const std::string& msg) {
    return Status(StatusCode::Filter, msg, -1);
  }

  /** Return a EncryptionError error class Status with a given message **/
  static Status EncryptionError(const std::string& msg) {
    return Status(StatusCode::Encryption, msg, -1);
  }

  /** Return an ArrayError error class Status with a given message **/
  static Status ArrayError(const std::string& msg) {
    return Status(StatusCode::Array, msg, -1);
  }

  /** Return a VFSFileHandle error class Status with a given message **/
  static Status VFSFileHandleError(const std::string& msg) {
    return Status(StatusCode::VFSFileHandleError, msg, -1);
  }

  /** Return a ContextError error class Status with a given message **/
  static Status ContextError(const std::string& msg) {
    return Status(StatusCode::ContextError, msg, -1);
  }

  /** Return a SubarrayError error class Status with a given message **/
  static Status SubarrayError(const std::string& msg) {
    return Status(StatusCode::SubarrayError, msg, -1);
  }

  /** Return a SubarrayPartitionerError error class Status with a given message
   * **/
  static Status SubarrayPartitionerError(const std::string& msg) {
    return Status(StatusCode::SubarrayPartitionerError, msg, -1);
  }

  /** Return a RTreeError error class Status with a given message **/
  static Status RTreeError(const std::string& msg) {
    return Status(StatusCode::RTreeError, msg, -1);
  }

  /** Return a CellSlabIterError error class Status with a given message **/
  static Status CellSlabIterError(const std::string& msg) {
    return Status(StatusCode::CellSlabIterError, msg, -1);
  }

  /** Return a RestError error class Status with a given message **/
  static Status RestError(const std::string& msg) {
    return Status(StatusCode::RestError, msg, -1);
  }

  /** Return a SerializationError error class Status with a given message **/
  static Status SerializationError(const std::string& msg) {
    return Status(StatusCode::SerializationError, msg, -1);
  }

  /** Return a ThreadPoolError error class Status with a given message **/
  static Status ThreadPoolError(const std::string& msg) {
    return Status(StatusCode::ThreadPoolError, msg, -1);
  }

  /** Return a FragmentInfoError error class Status with a given message **/
  static Status FragmentInfoError(const std::string& msg) {
    return Status(StatusCode::FragmentInfoError, msg, -1);
  }

  /** Return a QueryConditionError error class Status with a given message **/
  static Status QueryConditionError(const std::string& msg) {
    return Status(StatusCode::QueryConditionError, msg, -1);
  }

  /** Returns true iff the status indicates success **/
  bool ok() const {
    return (state_ == nullptr);
  }

  /**
   * Return a std::string representation of this status object suitable for
   * printing.  Return "Ok" for success.
   */
  std::string to_string() const;

  /** Return a string representation of the status code **/
  std::string code_to_string() const;

  /** Get the POSIX code associated with this Status, -1 if None. **/
  int16_t posix_code() const;

  /** Return the status code of this Status object **/
  StatusCode code() const {
    return (
        (state_ == nullptr) ? StatusCode::Ok :
                              static_cast<StatusCode>(state_[4]));
  }

  /** Return an std::string copy of the Status message **/
  std::string message() const {
    uint32_t length;
    memcpy(&length, state_, sizeof(length));
    std::string msg;
    msg.append((state_ + 7), length);
    return msg;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * OK status has a NULL state_.  Otherwise, state_ is a new[] array
   * of the following form:
   *    state_[0..3] == length of message
   *    state_[4]    == code
   *    state_[5..6] == posix_code
   *    state_[7..]  == message
   */
  const char* state_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Private constructor. */
  Status(StatusCode code, const std::string& msg, int16_t posix_code);

  /** Clones and returns the input state (allocates memory). */
  static const char* copy_state(const char* s);
};

inline Status::Status(const Status& s) {
  state_ = (s.state_ == nullptr) ? nullptr : copy_state(s.state_);
}

inline void Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and when both s and *this are ok.
  if (state_ != s.state_) {
    tdb_delete_array(state_);
    state_ = (s.state_ == nullptr) ? nullptr : copy_state(s.state_);
  }
}

}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_STATUS_H
