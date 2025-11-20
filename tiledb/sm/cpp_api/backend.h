/**
 * @file   backend.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file declares the C++ API for TileDB backend identification.
 */

#ifndef TILEDB_CPP_API_BACKEND_H
#define TILEDB_CPP_API_BACKEND_H

#include "context.h"
#include "tiledb.h"  // NOLINT(misc-include-cleaner)

#include <string>

namespace tiledb {

/**
 * Represents the backend type for a TileDB URI.
 *
 * **Example:**
 *
 * @code{.cpp}
 * tiledb::Context ctx;
 * auto backend = tiledb::Backend::from_uri(ctx, "s3://bucket/array");
 * if (backend == tiledb::Backend::Type::S3) {
 *   std::cout << "This is an S3 backend" << std::endl;
 * }
 * @endcode
 */
class Backend {
 public:
  /* ********************************* */
  /*           TYPE DEFINITIONS        */
  /* ********************************* */

  /** The backend type. */
  enum class Type {
    /** Amazon S3 backend (includes HTTP/HTTPS). */
    S3,
    /** Microsoft Azure backend. */
    Azure,
    /** Google Cloud Storage backend. */
    GCS,
    /** TileDB Cloud REST backend (legacy v1). */
    TileDB_v1,
    /** TileDB Cloud REST backend (v2). */
    TileDB_v2,
    /** Invalid or unknown backend type. */
    Invalid
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  explicit Backend(const Type& type)
      : type_(type) {
  }

  explicit Backend(tiledb_backend_t type) {
    switch (type) {
      case TILEDB_BACKEND_S3:
        type_ = Type::S3;
        break;
      case TILEDB_BACKEND_AZURE:
        type_ = Type::Azure;
        break;
      case TILEDB_BACKEND_GCS:
        type_ = Type::GCS;
        break;
      case TILEDB_BACKEND_TILEDB_v1:
        type_ = Type::TileDB_v1;
        break;
      case TILEDB_BACKEND_TILEDB_v2:
        type_ = Type::TileDB_v2;
        break;
      case TILEDB_BACKEND_INVALID:
      default:
        type_ = Type::Invalid;
        break;
    }
  }

  Backend() = default;
  Backend(const Backend&) = default;
  Backend(Backend&&) = default;
  Backend& operator=(const Backend&) = default;
  Backend& operator=(Backend&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Returns a string representation of the backend type.
   *
   * @return String representation of the backend
   */
  std::string to_str() const {
    switch (type_) {
      case Type::S3:
        return "S3";
      case Type::Azure:
        return "Azure";
      case Type::GCS:
        return "GCS";
      case Type::TileDB_v1:
        return "TileDB_v1";
      case Type::TileDB_v2:
        return "TileDB_v2";
      case Type::Invalid:
        return "Invalid";
    }
    return "Unknown";
  }

  /** Returns the backend type. */
  Type type() const {
    return type_;
  }

  /** Converts to the underlying C API enum type. */
  tiledb_backend_t c_type() const {
    switch (type_) {
      case Type::S3:
        return TILEDB_BACKEND_S3;
      case Type::Azure:
        return TILEDB_BACKEND_AZURE;
      case Type::GCS:
        return TILEDB_BACKEND_GCS;
      case Type::TileDB_v1:
        return TILEDB_BACKEND_TILEDB_v1;
      case Type::TileDB_v2:
        return TILEDB_BACKEND_TILEDB_v2;
      case Type::Invalid:
      default:
        return TILEDB_BACKEND_INVALID;
    }
  }

  /** Compares backend types for equality. */
  bool operator==(const Backend& rhs) const {
    return this->type_ == rhs.type();
  }

  /** Compares backend types for inequality. */
  bool operator!=(const Backend& rhs) const {
    return !(*this == rhs);
  }

  /** Compares backend type with enum directly. */
  bool operator==(const Type& rhs) const {
    return this->type_ == rhs;
  }

  /** Compares backend type with enum directly. */
  bool operator!=(const Type& rhs) const {
    return this->type_ != rhs;
  }

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * Gets the backend type for a given URI.
   *
   * **Example:**
   *
   * @code{.cpp}
   * tiledb::Context ctx;
   * auto backend = tiledb::Backend::from_uri(ctx, "s3://bucket/array");
   * std::cout << "Backend: " << backend.to_str() << std::endl;
   * @endcode
   *
   * @param ctx The TileDB context
   * @param uri The URI to check
   * @return A Backend object containing the backend type
   */
  static Backend from_uri(const Context& ctx, const std::string& uri) {
    tiledb_backend_t type;
    ctx.handle_error(
        tiledb_uri_get_backend_name(ctx.ptr().get(), uri.c_str(), &type));
    return Backend(type);
  }

 private:
  /** The backend type. */
  Type type_ = Type::Invalid;
};

/**
 * Prints a Backend to an output stream.
 * @param os Output stream
 * @param backend Backend to print
 * @return Reference to the output stream
 */
inline std::ostream& operator<<(std::ostream& os, const Backend& backend) {
  os << backend.to_str();
  return os;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_BACKEND_H
