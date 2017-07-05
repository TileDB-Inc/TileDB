/**
 * @file   status.h
 *
 * @section LICENSE
 *
 * The BSD License
 *
 * @copyright
 * Copyright (c) 2017 TileDB, Inc.
 * Copyright (c) 2011 The LevelDB Authors.  All rights reserved.
 *
 * @section description
 * A Status encapsulates the result of an operation.  It may indicate success,
 * or it may indicate an error with an associated error message.
 *
 * Multiple threads can invoke const methods on a Status without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same Status must use
 * external synchronization.
 */

#ifndef TILEDB_STATUS_H
#define TILEDB_STATUS_H

#include <cstdint>
#include <cstring>
#include <string>

namespace tiledb {

#define RETURN_NOT_OK(s) \
  do {                   \
    Status _s = (s);     \
    if (!_s.ok()) {      \
      return _s;         \
    }                    \
  } while (0);

#define RETURN_NOT_OK_ELSE(s, else_) \
  do {                               \
    Status _s = (s);                 \
    if (!_s.ok()) {                  \
      else_;                         \
      return _s;                     \
    }                                \
  } while (0);

enum class StatusCode : char {
  Ok,
  Error,
  StorageManager,
  WriteState,
  Fragment,
  Bookkeeping,
  Array,
  ArraySchema,
  ArrayIt,
  ARS,   // Array Read State
  ASRS,  // Array Sorted Read State
  ASWS,  // Array Sorted Write State
  Metadata,
  OS,
  IO,
  Mem,
  MMap,
  GZip,
  Compression,
  AIO
};

class Status {
 public:
  // Create a success status
  Status()
      : state_(nullptr) {
  }
  ~Status() {
    delete[] state_;
  }

  Status(StatusCode code, const std::string& msg)
      : Status(code, msg, -1) {
  }

  // Copy the specified status
  Status(const Status& s);
  void operator=(const Status& s);

  // Return a success status
  static Status Ok() {
    return Status();
  }

  static Status Error(const std::string& msg) {
    return Status(StatusCode::Error, msg, -1);
  }

  static Status StorageManagerError() {
    return Status(StatusCode::StorageManager, "", -1);
  }

  static Status StorageManagerError(const std::string& msg) {
    return Status(StatusCode::StorageManager, msg, -1);
  }

  static Status FragmentError(const std::string& msg) {
    return Status(StatusCode::Fragment, msg, -1);
  }

  static Status BookkeepingError(const std::string& msg) {
    return Status(StatusCode::Bookkeeping, msg, -1);
  }

  static Status ArrayError(const std::string& msg) {
    return Status(StatusCode::Array, msg, -1);
  }

  static Status ArraySchemaError(const std::string& msg) {
    return Status(StatusCode::ArraySchema, msg, -1);
  }

  static Status ArrayItError(const std::string& msg) {
    return Status(StatusCode::ArrayIt, msg, -1);
  }

  static Status ARSError(const std::string& msg) {
    return Status(StatusCode::ARS, msg, -1);
  }

  static Status ASRSError(const std::string& msg) {
    return Status(StatusCode::ASRS, msg, -1);
  }

  static Status ASWSError(const std::string& msg) {
    return Status(StatusCode::ASWS, msg, -1);
  }

  static Status MetadataError(const std::string& msg) {
    return Status(StatusCode::Metadata, msg, -1);
  }
  static Status OSError(const std::string& msg) {
    return Status(StatusCode::OS, msg, -1);
  }

  static Status IOError(const std::string& msg) {
    return Status(StatusCode::IO, msg, -1);
  }

  static Status MemError(const std::string& msg) {
    return Status(StatusCode::Mem, msg, -1);
  }

  static Status MMapError(const std::string& msg) {
    return Status(StatusCode::MMap, msg, -1);
  }

  static Status GZipError(const std::string& msg) {
    return Status(StatusCode::GZip, msg, -1);
  }

  static Status CompressionError(const std::string& msg) {
    return Status(StatusCode::Compression, msg, -1);
  }

  static Status AIOError(const std::string& msg) {
    return Status(StatusCode::AIO, msg, -1);
  }

  // Returns true iff the status indicates success
  bool ok() const {
    return (state_ == nullptr);
  }

  // Return a string representation of this status object sutible for printing
  // Return "Ok" for success.
  std::string to_string() const;

  // Return a string representation of the status code
  std::string code_to_string() const;

  // Get the POSIX code associated with this Status, -1 if None.
  int16_t posix_code() const;

  // return the status code of this Status object
  StatusCode code() const {
    return (
        (state_ == nullptr) ? StatusCode::Ok :
                              static_cast<StatusCode>(state_[4]));
  }

  std::string message() const {
    uint32_t length;
    memcpy(&length, state_, sizeof(length));
    std::string msg;
    msg.append((state_ + 7), length);
    return msg;
  }

 private:
  // OK status has a NULL state_.  Otherwise, state_ is a new[] array
  // of the following form:
  //    state_[0..3] == length of message
  //    state_[4]    == code
  //    state_[5..6] == posix_code
  //    state_[7..]  == message
  const char* state_;

  Status(StatusCode code, const std::string& msg, int16_t posix_code);
  static const char* copy_state(const char* s);
};

inline Status::Status(const Status& s) {
  state_ = (s.state_ == nullptr) ? nullptr : copy_state(s.state_);
}

inline void Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and when both s and *this are ok.
  if (state_ != s.state_) {
    delete[] state_;
    state_ = (s.state_ == nullptr) ? nullptr : copy_state(s.state_);
  }
}
};  // namespace tiledb

#endif  // TILEDB_STATUS_H
