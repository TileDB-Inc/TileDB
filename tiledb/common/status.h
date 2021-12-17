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
#include <optional>
#include <string>
#include <tuple>
using std::tuple, std::optional, std::nullopt;

#include "status_code.h"
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

#define RETURN_NOT_OK_TUPLE(s)   \
  do {                           \
    Status _s = (s);             \
    if (!_s.ok()) {              \
      return {_s, std::nullopt}; \
    }                            \
  } while (false)

class Status {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor with success status (empty state). */
  Status()
      : state_(nullptr) {
  }

  /**
   * General constructor for arbitrary status
   */
  Status(StatusCode code, const std::string_view& msg);

  /**
   * General constructor for arbitrary status
   */
  Status(StatusCode code, const std::string& msg)
      : Status(code, std::string_view{msg}) {
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

  /** Returns true iff the status indicates success **/
  bool ok() const {
    return (state_ == nullptr);
  }

  /**
   * Return a std::string representation of this status object suitable for
   * printing.  Return "Ok" for success.
   */
  std::string to_string() const;

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
   *    state_[5..6] == reserved
   *    state_[7..]  == message
   */
  const char* state_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

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

inline Status Status_Error(const std::string& msg) {
  return {StatusCode::Error, msg};
};

/** Return a StorageManager error class Status with a given message **/
inline Status Status_StorageManagerError(const std::string& msg) {
  return Status(StatusCode::StorageManager, msg);
}
/**  Return a success status **/
inline Status Status_Ok() {
  return Status();
}
/** Return a FragmentMetadata error class Status with a given message **/
inline Status Status_FragmentMetadataError(const std::string& msg) {
  return Status(StatusCode::FragmentMetadata, msg);
}
/** Return a ArraySchema error class Status with a given message **/
inline Status Status_ArraySchemaError(const std::string& msg) {
  return Status(StatusCode::ArraySchema, msg);
}
/** Return a ArraySchemaEvolution error class Status with a given message **/
inline Status Status_ArraySchemaEvolutionError(const std::string& msg) {
  return Status(StatusCode::ArraySchemaEvolution, msg);
}
/** Return a Metadata error class Status with a given message **/
inline Status Status_MetadataError(const std::string& msg) {
  return Status(StatusCode::Metadata, msg);
}
/** Return a IO error class Status with a given message **/
inline Status Status_IOError(const std::string& msg) {
  return Status(StatusCode::IO, msg);
}
/** Return a GZip error class Status with a given message **/
inline Status Status_GZipError(const std::string& msg) {
  return Status(StatusCode::GZip, msg);
}
/** Return a ChecksumError error class Status with a given message **/
inline Status Status_ChecksumError(const std::string& msg) {
  return Status(StatusCode::ChecksumError, msg);
}
/** Return a Compression error class Status with a given message **/
inline Status Status_CompressionError(const std::string& msg) {
  return Status(StatusCode::Compression, msg);
}
/** Return a Tile error class Status with a given message **/
inline Status Status_TileError(const std::string& msg) {
  return Status(StatusCode::Tile, msg);
}
/** Return a TileIO error class Status with a given message **/
inline Status Status_TileIOError(const std::string& msg) {
  return Status(StatusCode::TileIO, msg);
}
/** Return a Buffer error class Status with a given message **/
inline Status Status_BufferError(const std::string& msg) {
  return Status(StatusCode::Buffer, msg);
}
/** Return a Query error class Status with a given message **/
inline Status Status_QueryError(const std::string& msg) {
  return Status(StatusCode::Query, msg);
}
/** Return a ValidityVector error class Status with a given message **/
inline Status Status_ValidityVectorError(const std::string& msg) {
  return Status(StatusCode::ValidityVector, msg);
}
/** Return a Status_VFSError error class Status with a given message **/
inline Status Status_VFSError(const std::string& msg) {
  return Status(StatusCode::VFS, msg);
}
/** Return a Dimension error class Status with a given message **/
inline Status Status_DimensionError(const std::string& msg) {
  return Status(StatusCode::Dimension, msg);
}
/** Return a Domain error class Status with a given message **/
inline Status Status_DomainError(const std::string& msg) {
  return Status(StatusCode::Domain, msg);
}
/** Return a Consolidator error class Status with a given message **/
inline Status Status_ConsolidatorError(const std::string& msg) {
  return Status(StatusCode::Consolidator, msg);
}
/** Return a LRUCache error class Status with a given message **/
inline Status Status_LRUCacheError(const std::string& msg) {
  return Status(StatusCode::LRUCache, msg);
}
/** Return a Config error class Status with a given message **/
inline Status Status_ConfigError(const std::string& msg) {
  return Status(StatusCode::Config, msg);
}
/** Return a Utils error class Status with a given message **/
inline Status Status_UtilsError(const std::string& msg) {
  return Status(StatusCode::Utils, msg);
}
/** Return a FS_S3 error class Status with a given message **/
inline Status Status_S3Error(const std::string& msg) {
  return Status(StatusCode::FS_S3, msg);
}
/** Return a FS_AZURE error class Status with a given message **/
inline Status Status_AzureError(const std::string& msg) {
  return Status(StatusCode::FS_AZURE, msg);
}
/** Return a FS_GCS error class Status with a given message **/
inline Status Status_GCSError(const std::string& msg) {
  return Status(StatusCode::FS_GCS, msg);
}
/** Return a FS_HDFS error class Status with a given message **/
inline Status Status_HDFSError(const std::string& msg) {
  return Status(StatusCode::FS_HDFS, msg);
}
/** Return a FS_MEM error class Status with a given message **/
inline Status Status_MemFSError(const std::string& msg) {
  return Status(StatusCode::FS_MEM, msg);
}
/** Return a Attribute error class Status with a given message **/
inline Status Status_AttributeError(const std::string& msg) {
  return Status(StatusCode::Attribute, msg);
}
/** Return a Status_SparseGlobalOrderReaderError error class Status with a
 * given message **/
inline Status Status_SparseGlobalOrderReaderError(const std::string& msg) {
  return Status(StatusCode::SparseGlobalOrderReaderError, msg);
}
/** Return a Status_SparseUnorderedWithDupsReaderError error class Status with
 * a given message **/
inline Status Status_SparseUnorderedWithDupsReaderError(
    const std::string& msg) {
  return Status(StatusCode::SparseUnorderedWithDupsReaderError, msg);
}
/** Return a Status_DenseReaderError error class Status with a given message
 * **/
inline Status Status_DenseReaderError(const std::string& msg) {
  return Status(StatusCode::DenseReaderError, msg);
}
/** Return a Reader error class Status with a given message **/
inline Status Status_ReaderError(const std::string& msg) {
  return Status(StatusCode::Reader, msg);
}
/** Return a Writer error class Status with a given message **/
inline Status Status_WriterError(const std::string& msg) {
  return Status(StatusCode::Writer, msg);
}
/** Return a PreallocatedBuffer error class Status with a given message
 * **/
inline Status Status_PreallocatedBufferError(const std::string& msg) {
  return Status(StatusCode::PreallocatedBuffer, msg);
}
/** Return a Status_FilterError error class Status with a given message **/
inline Status Status_FilterError(const std::string& msg) {
  return Status(StatusCode::Filter, msg);
}
/** Return a Encryption error class Status with a given message **/
inline Status Status_EncryptionError(const std::string& msg) {
  return Status(StatusCode::Encryption, msg);
}
/** Return an Array error class Status with a given message **/
inline Status Status_ArrayError(const std::string& msg) {
  return Status(StatusCode::Array, msg);
}
/** Return a VFSFileHandle error class Status with a given message **/
inline Status Status_VFSFileHandleError(const std::string& msg) {
  return Status(StatusCode::VFSFileHandleError, msg);
}
/** Return a Status_ContextError error class Status with a given message **/
inline Status Status_ContextError(const std::string& msg) {
  return Status(StatusCode::ContextError, msg);
}
/** Return a Status_SubarrayError error class Status with a given message **/
inline Status Status_SubarrayError(const std::string& msg) {
  return Status(StatusCode::SubarrayError, msg);
}
/** Return a Status_SubarrayPartitionerError error class Status with a given
 * message
 * **/
inline Status Status_SubarrayPartitionerError(const std::string& msg) {
  return Status(StatusCode::SubarrayPartitionerError, msg);
}
/** Return a Status_RTreeError error class Status with a given message **/
inline Status Status_RTreeError(const std::string& msg) {
  return Status(StatusCode::RTreeError, msg);
}
/** Return a Status_CellSlabIterError error class Status with a given message
 * **/
inline Status Status_CellSlabIterError(const std::string& msg) {
  return Status(StatusCode::CellSlabIterError, msg);
}
/** Return a Status_RestError error class Status with a given message **/
inline Status Status_RestError(const std::string& msg) {
  return Status(StatusCode::RestError, msg);
}
/** Return a Status_SerializationError error class Status with a given message
 * **/
inline Status Status_SerializationError(const std::string& msg) {
  return Status(StatusCode::SerializationError, msg);
}
/** Return a Status_ThreadPoolError error class Status with a given message
 * **/
inline Status Status_ThreadPoolError(const std::string& msg) {
  return Status(StatusCode::ThreadPoolError, msg);
}
/** Return a Status_FragmentInfoError error class Status with a given message
 * **/
inline Status Status_FragmentInfoError(const std::string& msg) {
  return Status(StatusCode::FragmentInfoError, msg);
}
/** Return a Status_DenseTilerError error class Status with a given message
 * **/
inline Status Status_DenseTilerError(const std::string& msg) {
  return Status(StatusCode::DenseTilerError, msg);
}
/** Return a Status_QueryConditionError error class Status with a given
 * message **/
inline Status Status_QueryConditionError(const std::string& msg) {
  return Status(StatusCode::QueryConditionError, msg);
}
}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_STATUS_H
