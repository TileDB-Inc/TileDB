/**
 * @file   unit-uri.cc
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
 * Tests the `URI` class.
 */

#include <catch.hpp>
#include "tiledb/sm/filesystem/uri.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/path_win.h"
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

using namespace tiledb::sm;

#ifdef _WIN32
static const char PATH_SEPARATOR = '\\';
static std::string current_dir() {
  return Win::current_dir();
}
#else
static const char PATH_SEPARATOR = '/';
static std::string current_dir() {
  return Posix::current_dir();
}
#endif

TEST_CASE("URI: Test join", "[uri]") {
  URI uri("file:///path");
  CHECK(uri.join_path("").to_string() == "file:///path");
  CHECK(uri.join_path("abc").to_string() == "file:///path/abc");
  CHECK(uri.join_path("/abc").to_string() == "file:///path/abc");
  CHECK(uri.join_path("abc/def").to_string() == "file:///path/abc/def");
  CHECK(uri.join_path("/abc/def").to_string() == "file:///path/abc/def");

  uri = URI("file:///path/");
  CHECK(uri.join_path("").to_string() == "file:///path/");
  CHECK(uri.join_path("abc").to_string() == "file:///path/abc");
  CHECK(uri.join_path("/abc").to_string() == "file:///path/abc");
  CHECK(uri.join_path("abc/def").to_string() == "file:///path/abc/def");
  CHECK(uri.join_path("/abc/def").to_string() == "file:///path/abc/def");
}

TEST_CASE("URI: Test file URIs", "[uri]") {
  URI uri("file:///path");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  CHECK(uri.to_string() == "file:///path");
#ifndef _WIN32
  // note: "file://path" is an accepted URI form on windows for UNC
  // level share viewing, but expected .is_invalid() on *nix.
  uri = URI("file://path");
  CHECK(uri.is_invalid());
#endif
  uri =
      URI("file:///path/is/quite/long/long/long/long/long/long/long/long/long/"
          "long/long/long/long/long/long/long/long/long/long/long/long/long/"
          "long/long/long/long/long/long/long/long/long/long/long/long/long/"
          "long/long/long/long/long/long/long/long/long/long/long/long/long");
  CHECK(!uri.is_invalid());
  uri = URI("");
  CHECK(uri.is_invalid());

  // TODO: re-enable these checks if appropriate for posix_filesystem.
  // uri = URI("file:///path/../relative");
  // CHECK(!uri.is_invalid());
  // CHECK(URI::is_file(uri.to_string()));
  // CHECK(uri.to_string() == "file:///relative");
  // uri = URI("file:///path/.././relative/./path");
  // CHECK(!uri.is_invalid());
  // CHECK(URI::is_file(uri.to_string()));
  // CHECK(uri.to_string() == "file:///relative/path");
}

TEST_CASE("URI: Test relative paths", "[uri]") {
  URI uri = URI("path1");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  CHECK(uri.to_string().find("file:///") == 0);
  CHECK(uri.to_path() == current_dir() + PATH_SEPARATOR + "path1");
#ifdef _WIN32
  CHECK(
      uri.to_string() ==
      path_win::uri_from_path(Win::current_dir()) + "/path1");
#else
  CHECK(uri.to_string() == "file://" + Posix::current_dir() + "/path1");
#endif

  uri = URI(".");
  CHECK(!uri.is_invalid());
  CHECK(uri.to_path() == current_dir());
}

TEST_CASE("URI: Test URI to path", "[uri]") {
  URI uri = URI("file:///my/path");
#ifdef _WIN32
  // Absolute paths with no drive letter are relative to the current working
  // directory's drive.
  CHECK(uri.to_path() == "\\my\\path");
#else
  CHECK(uri.to_path() == "/my/path");
#endif

  uri = URI("file:///my/path/../relative/path");
#ifdef _WIN32
  CHECK(uri.to_path() == "\\my\\relative\\path");
#else
  CHECK(uri.to_path() == "/my/path/../relative/path");
#endif

  uri = URI("s3://path/on/s3");
  CHECK(uri.to_path() == "s3://path/on/s3");
  uri = URI("s3://relative/../path/on/s3");
  CHECK(uri.to_path() == "s3://relative/../path/on/s3");
  uri = URI("azure://path/on/azure");
  CHECK(uri.to_path() == "azure://path/on/azure");
  uri = URI("azure://relative/../path/on/azure");
  CHECK(uri.to_path() == "azure://relative/../path/on/azure");
  uri = URI("hdfs://path/on/hdfs");
  CHECK(uri.to_path() == "hdfs://path/on/hdfs");
  uri = URI("hdfs://relative/../path/on/hdfs");
  CHECK(uri.to_path() == "hdfs://relative/../path/on/hdfs");

  uri = URI("C:\\my\\path");
#ifdef _WIN32
  CHECK(uri.to_string() == "file:///C:/my/path");
  CHECK(uri.to_path() == "C:\\my\\path");
#else
  // Windows paths on non-Windows platforms are nonsensical, but have defined
  // behavior.
  CHECK(uri.to_string() == "file://" + current_dir() + "/" + "C:\\my\\path");
  CHECK(uri.to_path() == current_dir() + "/" + "C:\\my\\path");
#endif

  uri = URI("file:///C:/my/path");
#ifdef _WIN32
  CHECK(uri.to_path() == "C:\\my\\path");
#else
  CHECK(uri.to_path() == "/C:/my/path");
#endif
}

TEST_CASE("URI: Test schemes", "[uri]") {
  CHECK(URI("path/to/dir").is_file());
  CHECK(URI("file:///path/to/dir").is_file());

  CHECK(URI("s3://bucket/dir").is_s3());
  CHECK(URI("http://bucket/dir").is_s3());
  CHECK(URI("https://bucket/dir").is_s3());

  CHECK(URI("azure://container/dir").is_azure());

  CHECK(URI("hdfs://namenode/dir").is_hdfs());

  CHECK(URI("tiledb://namespace/array").is_tiledb());
}

TEST_CASE("URI: Test REST components", "[uri]") {
  std::string ns, array;

  CHECK(!URI("").get_rest_components(&ns, &array).ok());
  CHECK(!URI("abc").get_rest_components(&ns, &array).ok());
  CHECK(!URI("path/to/dir").get_rest_components(&ns, &array).ok());
  CHECK(!URI("/path/to/dir").get_rest_components(&ns, &array).ok());
  CHECK(!URI("file:///path/to/dir").get_rest_components(&ns, &array).ok());
  CHECK(!URI("s3://bucket/dir").get_rest_components(&ns, &array).ok());
  CHECK(!URI("azure://container/dir").get_rest_components(&ns, &array).ok());
  CHECK(!URI("http://bucket/dir").get_rest_components(&ns, &array).ok());
  CHECK(!URI("https://bucket/dir").get_rest_components(&ns, &array).ok());
  CHECK(!URI("hdfs://namenode/dir").get_rest_components(&ns, &array).ok());

  CHECK(URI("tiledb://namespace/array").get_rest_components(&ns, &array).ok());
  CHECK(ns == "namespace");
  CHECK(array == "array");
  CHECK(URI("tiledb://namespace/array/uri")
            .get_rest_components(&ns, &array)
            .ok());
  CHECK(ns == "namespace");
  CHECK(array == "array/uri");
  CHECK(URI("tiledb://namespace/s3://bucket/dir")
            .get_rest_components(&ns, &array)
            .ok());
  CHECK(ns == "namespace");
  CHECK(array == "s3://bucket/dir");
  CHECK(URI("tiledb://namespace/azure://container/dir")
            .get_rest_components(&ns, &array)
            .ok());
  CHECK(ns == "namespace");
  CHECK(array == "azure://container/dir");

  CHECK(!URI("tiledb:///array").get_rest_components(&ns, &array).ok());
  CHECK(!URI("tiledb://ns").get_rest_components(&ns, &array).ok());
  CHECK(!URI("tiledb://ns/").get_rest_components(&ns, &array).ok());
  CHECK(!URI("tiledb://").get_rest_components(&ns, &array).ok());
  CHECK(!URI("tiledb:///").get_rest_components(&ns, &array).ok());
}

#ifdef _WIN32

TEST_CASE("URI: Test Windows paths", "[uri]") {
  URI uri("C:\\path");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  // Windows file URIs keep the drive letter to remain fully qualified.
  CHECK(uri.to_string() == "file:///C:/path");
  uri = URI("g:\\path\\..\\relative\\");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  CHECK(uri.to_string() == "file:///g:/relative/");
  uri = URI("C:\\mixed/slash\\types");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  CHECK(uri.to_string() == "file:///C:/mixed/slash/types");
  uri = URI("C:/mixed/slash/types");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  CHECK(uri.to_string() == "file:///C:/mixed/slash/types");
  uri = URI("C:\\Program Files (x86)\\TileDB\\");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  CHECK(uri.to_string() == "file:///C:/Program%20Files%20(x86)/TileDB/");
  uri = URI("path1\\path2");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  CHECK(uri.to_string().find("file:///") == 0);
  CHECK(
      uri.to_string() ==
      path_win::uri_from_path(Win::current_dir()) + "/path1/path2");
}

#endif
