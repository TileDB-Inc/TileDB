/**
 * @file   uri.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file defines class URI.
 */

#ifndef TILEDB_URI_H
#define TILEDB_URI_H

#include <string>

#include "status.h"

namespace tiledb {

/** Implements functionality regarding URIs. */
class URI {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  URI();

  /**
   * Constructor.
   *
   * @param path String that gets converted into an absolute path and stored
   *     as a URI.
   */
  explicit URI(const std::string& path);

  /** Destructor. */
  ~URI();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns a C-style pointer to the URI string. */
  const char* c_str() const;

  /** Checks if the URI is invalid (empty string). */
  bool is_invalid() const;

  /**
   * Checks if the input path is posix.
   *
   * @param path The path to be checked.
   * @return The result of the check.
   */
  static bool is_posix(const std::string& path);

  /**
   * Checks if the URI is posix.
   *
   * @return The result of the check.
   */
  bool is_posix() const;

  /**
   * Checks if the input path is HDFS.
   *
   * @param path The path to be checked.
   * @return The result of the check.
   */
  static bool is_hdfs(const std::string& path);

  /**
   * Checks if the URI is HDFS.
   *
   * @return The result of the check.
   */
  bool is_hdfs() const;

  /**
   * Checks if the input path is S3.
   *
   * @param path The path to be checked.
   * @return The result of the check.
   */
  static bool is_s3(const std::string& path);

  /**
   * Checks if the URI is S3.
   *
   * @return The result of the check.
   */
  bool is_s3() const;

  /**
   * Joins the URI with the input path.
   *
   * @param path The path to append.
   * @return The resulting URI.
   */
  URI join_path(const std::string& path) const;

  /** Returns the last part of the URI (i.e., excluding the parent). */
  std::string last_path_part() const;

  /** Returns the parent of the URI. */
  URI parent() const;

  /** Returns the URI path, stripping the resource. For examples,
   *  "file:///my/path/" is the URI, this function will return
   *  "/my/path/".
   */
  std::string to_path() const;

  /** Returns the URI string. */
  std::string to_string() const;

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** The URI is stored as a string. */
  std::string uri_;
};

}  // namespace tiledb

#endif  // TILEDB_URI_H
