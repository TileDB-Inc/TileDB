/**
 * @file   uri.h
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
 * This file defines class URI.
 */

#ifndef TILEDB_URI_H
#define TILEDB_URI_H

#include <string>

#include "tiledb/common/status.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** URI functions of all kinds, involving both syntax and filesystem.
 *
 * The constructor of this class accesses the file system if the URI is a file-
 * based URI, using the current directory to resolve an absolute path. At
 * present, there's no separate URI class that offers only syntax functions.
 */
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
  explicit URI(const char* path);

  /**
   * Constructor.
   *
   * @param path String that gets converted into an absolute path and stored
   *     as a URI.
   */
  explicit URI(char* path);

  /**
   * Constructor.
   *
   * @param path String that gets converted into an absolute path and stored
   *     as a URI.
   */
  explicit URI(std::string_view path);

  /**
   * Constructor.
   *
   * @param path
   * @param get_abs should local files become absolute
   */
  explicit URI(std::string_view path, const bool& get_abs);

  /** Destructor. */
  ~URI();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Return a copy of this URI with a trailing '/' added (if it did not already
   * have one).
   */
  URI add_trailing_slash() const;

  /**
   * Return a copy of this URI without a trailing '/' (if it already
   * has one).
   */
  URI remove_trailing_slash() const;

  /**
   * Returns if the URI is empty or not.
   */
  bool empty() const;

  /** Returns a C-style pointer to the URI string. */
  const char* c_str() const;

  /** Checks if the URI is invalid (empty string). */
  bool is_invalid() const;

  /**
   * Checks if the input path is file.
   *
   * @param path The path to be checked.
   * @return The result of the check.
   */
  static bool is_file(std::string_view path);

  /**
   * Checks if the URI is file.
   *
   * @return The result of the check.
   */
  bool is_file() const;

  /**
   * Checks if the URI contains a string.
   *
   * @param str the string to search for in the URI
   * @return The result of the check.
   */
  bool contains(std::string_view str) const;

  /**
   * Checks if the input path is HDFS.
   *
   * @param path The path to be checked.
   * @return The result of the check.
   */
  static bool is_hdfs(std::string_view path);

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
  static bool is_s3(std::string_view path);

  /**
   * Checks if the URI is S3.
   *
   * @return The result of the check.
   */
  bool is_s3() const;

  /**
   * Checks if the input path is Azure.
   *
   * @param path The path to be checked.
   * @return The result of the check.
   */
  static bool is_azure(std::string_view path);

  /**
   * Checks if the URI is Azure.
   *
   * @return The result of the check.
   */
  bool is_azure() const;

  /**
   * Checks if the input path is gcs.
   *
   * @param path The path to be checked.
   * @return The result of the check.
   */
  static bool is_gcs(std::string_view path);

  /**
   * Checks if the URI is gcs.
   *
   * @return The result of the check.
   */
  bool is_gcs() const;

  /**
   * Checks if the input path is mem.
   *
   * @param path The path to be checked.
   * @return The result of the check.
   */
  static bool is_memfs(std::string_view path);

  /**
   * Checks if the URI is mem.
   *
   * @return The result of the check.
   */
  bool is_memfs() const;

  /**
   * Checks if the input path is TileDB.
   *
   * @param path The path to be checked.
   * @return The result of the check.
   */
  static bool is_tiledb(std::string_view path);

  /**
   * Checks if the URI is TileDB.
   *
   * @return The result of the check.
   */
  bool is_tiledb() const;

  /**
   * Checks the TileDB REST URI for validity and returns its components.
   *
   * @param array_namespace Set to the namespace of the input URI
   * @param array_uri Set to the array URI of the input URI.
   * @return Status
   */
  Status get_rest_components(
      std::string* array_namespace, std::string* array_uri) const;

  /**
   * Return the fragment name from the URI if one can be found.
   *
   * The logic for this parsing is that first we locate a '/__fragments/' path
   * component in the string and then take everything up to the next possibly
   * non-existent '/' separator.
   *
   * @return The fragment name URI if one is found, else std::nullopt.
   */
  std::optional<URI> get_fragment_name() const;

  /**
   * Joins the URI with the input path.
   *
   * @param path The path to append.
   * @return The resulting URI.
   */
  URI join_path(const std::string& path) const;

  /**
   * Joins the URI with the input URI.
   *
   * @param uri The URI to append.
   * @return The resulting URI.
   */
  URI join_path(const URI& uri) const;

  /** Returns the last part of the URI (i.e., excluding the parent). */
  std::string last_path_part() const;

  /** Returns the two last parts of the URI (i.e., after the penultimate `/`).
   */
  std::string last_two_path_parts() const;

  /**
   * Returns the URI path for the current platform, stripping the resource. For
   * example, if "file:///my/path/" is the URI, this function will return
   * "/my/path/" on Mac and Linux. If "file:///C:/my/path" is the URI, this
   * function will return "C:\my\path" on Windows. HDFS and S3 URIs are returned
   * unmodified.
   *
   * @param uri The URI to convert.
   * @return std::string The converted path, or empty string on error.
   */
  static std::string to_path(const std::string& uri);

  /** Returns the URI path for the current platform, stripping the resource. For
   * example, if "file:///my/path/" is the URI, this function will return
   * "/my/path/" on Mac and Linux. If "file:///C:/my/path" is the URI, this
   * function will return "C:\my\path" on Windows. HDFS and S3 URIs are returned
   * unmodified.
   */
  std::string to_path() const;

  /** Returns the URI string. */
  std::string to_string() const;

  /** Returns the parent dir URI */
  URI parent_path() const;

  /** Returns the backend name given by backend://my/path */
  std::string backend_name() const;

  /** For comparing URIs alphanumerically. */
  bool operator==(const URI& uri) const;

  /** For comparing URIs alphanumerically. */
  bool operator!=(const URI& uri) const;

  /** For comparing URIs alphanumerically. */
  bool operator<(const URI& uri) const;

  /** For comparing URIs alphanumerically. */
  bool operator>(const URI& uri) const;

  operator std::string_view() const noexcept;

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** The URI is stored as a string. */
  std::string uri_;
};

/**
 * Used to store a fragment URI, materializing its timestamp range
 * for convenience.
 */
struct TimestampedURI {
  URI uri_;
  std::pair<uint64_t, uint64_t> timestamp_range_;

  TimestampedURI(
      const URI& uri, const std::pair<uint64_t, uint64_t>& timestamp_range)
      : uri_(uri)
      , timestamp_range_(timestamp_range) {
  }

  bool operator<(const TimestampedURI& uri) const {
    return timestamp_range_.first < uri.timestamp_range_.first;
  }

  const URI& uri() const {
    return uri_;
  }

  const std::pair<uint64_t, uint64_t>& timestamp_range() const {
    return timestamp_range_;
  }

  bool operator==(const TimestampedURI& other) const {
    return (
        uri() == other.uri() && timestamp_range() == other.timestamp_range());
  }

  bool has_unary_timestamp_range() const {
    return timestamp_range_.first == timestamp_range_.second;
  }
};

/**
 * URI hash operator.
 */
struct URIHasher {
  std::size_t operator()(const URI& uri) const {
    return std::hash<std::string>()(uri.to_string());
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_URI_H
