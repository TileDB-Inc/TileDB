/**
 * @file   status.h
 *
 * @section LICENSE
 *
 * The BSD License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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

#include "tiledb/common/common-std.h"
#include "tiledb/common/heap_memory.h"

namespace tiledb::common {

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

#define RETURN_NOT_OK_TUPLE(s, ...) \
  do {                              \
    Status _s = (s);                \
    if (!_s.ok()) {                 \
      return {_s, __VA_ARGS__};     \
    }                               \
  } while (false)

/**
 * The original `Status` class, used as a ubiquitous return value to avoid
 * throwing exceptions.
 *
 * This class is in transition at present. It's in its second-to-last
 * implementation at present. The next and last transition will be to replace
 * `state_`, a class-allocated buffer-like entity with _ad hoc_ member
 * addressing (which it has been from the beginning), with a variable of type
 * `optional<StatusException>`. This will greatly simplify the class
 * implementation and make interconversion easy. The value `nullopt` will be the
 * OK status; anything else will be an error status.
 */
class [[nodiscard]] Status {
  friend class StatusException;

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * OK status has a NULL state_.  Otherwise, state_ is a new[] array
   * of the following form:
   *    state_[0..sizeof(string_view)-1] == origin
   *    state_[sizeof(string_view)..sizeof(string_view)+3] == size of message
   *    state_[sizeof(string_view)+4..]  == message
   */
  const char* state_;

  static constexpr ptrdiff_t origin_offset_ = 0;
  using origin_type = std::string_view;
  static constexpr ptrdiff_t message_size_offset_ =
      origin_offset_ + sizeof(origin_type);
  using message_size_type = uint32_t;
  static constexpr ptrdiff_t message_text_offset_ =
      message_size_offset_ + sizeof(message_size_type);
  static constexpr char Ok_text_[3]{"Ok"};

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  [[nodiscard]] inline const origin_type& origin_() const {
    return *reinterpret_cast<const origin_type*>(state_ + origin_offset_);
  }

  [[nodiscard]] inline origin_type& origin_() {
    return *reinterpret_cast<origin_type*>(
        const_cast<char*>(state_) + origin_offset_);
  }

  [[nodiscard]] inline message_size_type message_size_() const {
    return *reinterpret_cast<const message_size_type*>(
        state_ + message_size_offset_);
  }

  [[nodiscard]] inline message_size_type& message_size_() {
    return *reinterpret_cast<message_size_type*>(
        const_cast<char*>(state_) + message_size_offset_);
  }

  [[nodiscard]] inline const char* message_text_ptr_() const {
    return state_ + message_text_offset_;
  }

  [[nodiscard]] inline char* message_text_ptr_() {
    return const_cast<char*>(state_) + message_text_offset_;
  }

  /** Clones and returns the input state (allocates memory). */
  void copy_state(const Status& st);

  /**
   * The number of bytes allocated for the `state_` array, given the size of
   * the message.
   *
   * @param message_size The size of the message
   * @return
   */
  inline static size_t allocation_size(size_t message_size) {
    return message_text_offset_ + message_size;
  }

  /**
   * The vicinity of the error in the code, if present.
   *
   * This function is private. It's used only in status_exception through a
   * friend declaration. It's used there for interconversion with this class.
   *
   * @return if state_ is present its origin element; otherwise null
   */
  inline std::string_view origin() const {
    return ok() ? std::string_view{} : origin_();
  }

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
  Status(const std::string_view& vicinity, const std::string_view& message);

  /** Destructor. */
  ~Status() {
    tdb_delete_array(state_);
  }

  /** Copy the specified status. */
  Status(const Status& s);

  /** Assign status. */
  void operator=(const Status& s);

  /**  Return a success status **/
  static inline Status Ok() {
    return Status();
  }

  /** Returns true iff the status indicates success **/
  inline bool ok() const {
    return (state_ == nullptr);
  }

  /**
   * Return a std::string representation of this status object suitable for
   * printing.  Return "Ok" for success.
   */
  std::string to_string() const;

  /** Return an std::string copy of the Status message **/
  inline std::string message() const {
    return {message_text_ptr_(), message_size_()};
  }
};

inline Status::Status(const Status& s) {
  copy_state(s);
}

inline void Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and when both s and *this are ok.
  if (state_ != s.state_) {
    tdb_delete_array(state_);
    copy_state(s);
  }
}

/**  Return a success status **/
inline Status Status_Ok() {
  return {};
}
inline Status Status_Error(const std::string& msg) {
  return {"Error", msg};
};
/** Return a FragmentMetadata error class Status with a given message **/
inline Status Status_FragmentMetadataError(const std::string& msg) {
  return {"[TileDB::FragmentMetadata] Error", msg};
}
/** Return a ArraySchema error class Status with a given message **/
inline Status Status_ArraySchemaError(const std::string& msg) {
  return {"[TileDB::ArraySchema] Error", msg};
}
/** Return a ArraySchemaEvolution error class Status with a given message **/
inline Status Status_ArraySchemaEvolutionError(const std::string& msg) {
  return {"[TileDB::ArraySchemaEvolution] Error", msg};
}

/** Return a IO error class Status with a given message **/
inline Status Status_IOError(const std::string& msg) {
  return {"[TileDB::IO] Error", msg};
}
/** Return a ChecksumError error class Status with a given message **/
inline Status Status_ChecksumError(const std::string& msg) {
  return {"[TileDB::ChecksumError] Error", msg};
}
/** Return a Tile error class Status with a given message **/
inline Status Status_TileError(const std::string& msg) {
  return {"[TileDB::Tile] Error", msg};
}
/** Return a TileIO error class Status with a given message **/
inline Status Status_TileIOError(const std::string& msg) {
  return {"[TileDB::TileIO] Error", msg};
}
/** Return a Buffer error class Status with a given message **/
inline Status Status_BufferError(const std::string& msg) {
  return {"[TileDB::Buffer] Error", msg};
}
/** Return a Query error class Status with a given message **/
inline Status Status_QueryError(const std::string& msg) {
  return {"[TileDB::Query] Error", msg};
}
/** Return a Status_VFSError error class Status with a given message **/
inline Status Status_VFSError(const std::string& msg) {
  return {"[TileDB::VFS] Error", msg};
}
/** Return a Dimension error class Status with a given message **/
inline Status Status_DimensionError(const std::string& msg) {
  return {"[TileDB::Dimension] Error", msg};
}
/** Return a Domain error class Status with a given message **/
inline Status Status_DomainError(const std::string& msg) {
  return {"[TileDB::Domain] Error", msg};
}
/** Return a Consolidator error class Status with a given message **/
inline Status Status_ConsolidatorError(const std::string& msg) {
  return {"[TileDB::Consolidator] Error", msg};
}
/** Return a Utils error class Status with a given message **/
inline Status Status_UtilsError(const std::string& msg) {
  return {"[TileDB::Utils] Error", msg};
}
/** Return a FS_S3 error class Status with a given message **/
inline Status Status_S3Error(const std::string& msg) {
  return {"[TileDB::S3] Error", msg};
}
/** Return a FS_HDFS error class Status with a given message **/
inline Status Status_HDFSError(const std::string& msg) {
  return {"[TileDB::HDFS] Error", msg};
}
/** Return a FS_MEM error class Status with a given message **/
inline Status Status_MemFSError(const std::string& msg) {
  return {"[TileDB::MemFS] Error", msg};
}
/** Return a Status_SparseGlobalOrderReaderError error class Status with a
 * given message **/
inline Status Status_SparseGlobalOrderReaderError(const std::string& msg) {
  return {"[TileDB::SparseGlobalOrderReaderError] Error", msg};
}
/** Return a Status_SparseUnorderedWithDupsReaderError error class Status with
 * a given message **/
inline Status Status_SparseUnorderedWithDupsReaderError(
    const std::string& msg) {
  return {"[TileDB::SparseUnorderedWithDupsReaderError] Error", msg};
}
/** Return a Reader error class Status with a given message **/
inline Status Status_ReaderError(const std::string& msg) {
  return {"[TileDB::Reader] Error", msg};
}
/** Return a Writer error class Status with a given message **/
inline Status Status_WriterError(const std::string& msg) {
  return {"[TileDB::Writer] Error", msg};
}
/** Return a PreallocatedBuffer error class Status with a given message
 * **/
inline Status Status_PreallocatedBufferError(const std::string& msg) {
  return {"[TileDB::PreallocatedBuffer] Error", msg};
}
/** Return a Status_FilterError error class Status with a given message **/
inline Status Status_FilterError(const std::string& msg) {
  return {"[TileDB::Filter] Error", msg};
}
/** Return a Encryption error class Status with a given message **/
inline Status Status_EncryptionError(const std::string& msg) {
  return {"[TileDB::Encryption] Error", msg};
}
/** Return an Array error class Status with a given message **/
inline Status Status_ArrayError(const std::string& msg) {
  return {"[TileDB::Array] Error", msg};
}
/** Return a VFSFileHandle error class Status with a given message **/
inline Status Status_VFSFileHandleError(const std::string& msg) {
  return {"[TileDB::VFSFileHandle] Error", msg};
}
/** Return a Status_SubarrayError error class Status with a given message **/
inline Status Status_SubarrayError(const std::string& msg) {
  return {"[TileDB::Subarray] Error", msg};
}
/** Return a Status_SubarrayPartitionerError error class Status with a given
 * message
 **/
inline Status Status_SubarrayPartitionerError(const std::string& msg) {
  return {"[TileDB::SubarrayPartitioner] Error", msg};
}
/** Return a Status_RTreeError error class Status with a given message **/
inline Status Status_RTreeError(const std::string& msg) {
  return {"[TileDB::RTree] Error", msg};
}
/** Return a Status_CellSlabIterError error class Status with a given message
 * **/
inline Status Status_CellSlabIterError(const std::string& msg) {
  return {"[TileDB::CellSlabIter] Error", msg};
}
/** Return a Status_RestError error class Status with a given message **/
inline Status Status_RestError(const std::string& msg) {
  return {"[TileDB::REST] Error", msg};
}
/** Return a Status_SerializationError error class Status with a given message
 * **/
inline Status Status_SerializationError(const std::string& msg) {
  return {"[TileDB::Serialization] Error", msg};
}
/** Return a Status_ThreadPoolError error class Status with a given message
 * **/
inline Status Status_ThreadPoolError(const std::string& msg) {
  return {"[TileDB::ThreadPool] Error", msg};
}
/** Return a Status_DenseTilerError error class Status with a given message
 * **/
inline Status Status_DenseTilerError(const std::string& msg) {
  return {"[TileDB::DenseTiler] Error", msg};
}
/** Return a Status_QueryConditionError error class Status with a given
 * message **/
inline Status Status_QueryConditionError(const std::string& msg) {
  return {"[TileDB::QueryCondition] Error", msg};
}
/** Return a Status_ArrayDirectoryError error class Status with a given
 * message **/
inline Status Status_ArrayDirectoryError(const std::string& msg) {
  return {"[TileDB::ArrayDirectory] Error", msg};
}
/** Return a Status_TaskError error class Status with a given
 * message **/
inline Status Status_TaskError(const std::string& msg) {
  return {"[TileDB::Task] Error", msg};
}
/** Return a Range error class Status with a given message **/
inline Status Status_RangeError(const std::string& msg) {
  return {"[TileDB::Range] Error", msg};
}
}  // namespace tiledb::common

#endif  // TILEDB_STATUS_H
