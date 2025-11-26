/**
 * @file   unit-uri.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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

#include <test/support/tdb_catch.h>
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

  CHECK(URI("tiledb://namespace/array").is_tiledb());
}

TEST_CASE(
    "URI: Test REST components, valid, legacy",
    "[uri][rest-components][legacy]") {
  std::string arr = GENERATE(
      "8f039466-6e90-42ea-af53-dc0ba47d00c2",
      "a",
      "array_name",
      "s3://bucket/arrays/array_name",
      "s3://b/d/a");

  URI::RESTURIComponents rest_components;
  SECTION("Legacy REST URI components") {
    std::string ns = GENERATE("demo", "d");
    tiledb::sm::URI uri("tiledb://" + ns + "/" + arr);
    REQUIRE(uri.get_rest_components(true, &rest_components).ok());
    REQUIRE(rest_components.server_namespace == ns);
    REQUIRE(rest_components.asset_storage == arr);
  }
}

TEST_CASE(
    "URI: Test REST components, valid, TileDB-Server",
    "[uri][rest-components][server]") {
  std::string ns = GENERATE(
      "workspace/teamspace",
      "ws_cvsj3li97ng28m60nhj0/ts_cvsj4ei97ng28m60nhkg",
      "w/t");

  URI::RESTURIComponents rest_components;
  SECTION("Without storage component") {
    std::string arr = "ast_d05mes5q3m0fsaikmdn0";
    tiledb::sm::URI uri("tiledb://" + ns + "/" + arr);
    REQUIRE(uri.get_rest_components(false, &rest_components).ok());
    REQUIRE(rest_components.server_namespace == ns);
    REQUIRE(rest_components.asset_storage == "");
    REQUIRE(rest_components.server_path == arr);

    arr = "a";
    uri = tiledb::sm::URI("tiledb://" + ns + "/" + arr);
    REQUIRE(uri.get_rest_components(false, &rest_components).ok());
    REQUIRE(rest_components.server_namespace == ns);
    REQUIRE(rest_components.asset_storage == "");
    REQUIRE(rest_components.server_path == arr);

    arr = "array_name";
    uri = tiledb::sm::URI("tiledb://" + ns + "/" + arr);
    REQUIRE(uri.get_rest_components(false, &rest_components).ok());
    REQUIRE(rest_components.server_namespace == ns);
    REQUIRE(rest_components.asset_storage == "");
    REQUIRE(rest_components.server_path == arr);

    arr = "fld01/array_name";
    uri = tiledb::sm::URI("tiledb://" + ns + "/" + arr);
    REQUIRE(uri.get_rest_components(false, &rest_components).ok());
    REQUIRE(rest_components.server_namespace == ns);
    REQUIRE(rest_components.asset_storage == "");
    REQUIRE(rest_components.server_path == "fld01/array_name");

    arr = "fld01/fld02/array_name";
    uri = tiledb::sm::URI("tiledb://" + ns + "/" + arr);
    REQUIRE(uri.get_rest_components(false, &rest_components).ok());
    REQUIRE(rest_components.server_namespace == ns);
    REQUIRE(rest_components.asset_storage == "");
    REQUIRE(rest_components.server_path == "fld01/fld02/array_name");

    arr = "fld-01/fld 02/array name";
    uri = tiledb::sm::URI("tiledb://" + ns + "/" + arr);
    REQUIRE(uri.get_rest_components(false, &rest_components).ok());
    REQUIRE(rest_components.server_namespace == ns);
    REQUIRE(rest_components.asset_storage == "");
    REQUIRE(rest_components.server_path == "fld-01/fld 02/array name");
  }

  SECTION("With storage component") {
    std::string arr = "s3://bucket/arrays/array_name";
    tiledb::sm::URI uri("tiledb://" + ns + "/" + arr);
    REQUIRE(uri.get_rest_components(false, &rest_components).ok());
    REQUIRE(rest_components.server_namespace == ns);
    REQUIRE(rest_components.asset_storage == "s3://bucket/arrays/array_name");
    REQUIRE(rest_components.server_path == "array_name");

    arr = "s3://b/a";
    uri = tiledb::sm::URI("tiledb://" + ns + "/" + arr);
    REQUIRE(uri.get_rest_components(false, &rest_components).ok());
    REQUIRE(rest_components.server_namespace == ns);
    REQUIRE(rest_components.asset_storage == "s3://b/a");
    REQUIRE(rest_components.server_path == "a");

    arr = "fld01/fld02/s3://bucket/arrays/array_name";
    uri = tiledb::sm::URI("tiledb://" + ns + "/" + arr);
    REQUIRE(uri.get_rest_components(false, &rest_components).ok());
    REQUIRE(rest_components.server_namespace == ns);
    REQUIRE(rest_components.asset_storage == "s3://bucket/arrays/array_name");
    REQUIRE(rest_components.server_path == "fld01/fld02/array_name");

    arr = "fld01/gs://bucket/array_name";
    uri = tiledb::sm::URI("tiledb://" + ns + "/" + arr);
    REQUIRE(uri.get_rest_components(false, &rest_components).ok());
    REQUIRE(rest_components.server_namespace == ns);
    REQUIRE(rest_components.asset_storage == "gs://bucket/array_name");
    REQUIRE(rest_components.server_path == "fld01/array_name");
  }
}
TEST_CASE("URI: Test REST components, invalid", "[uri]") {
  URI::RESTURIComponents rest_components;

  const std::string invalid_rest_uri[] = {
      "",
      "abc",
      "path/to/dir",
      "/path/to/dir",
      "file:///path/to/dir",
      "s3://bucket/dir",
      "azure://container/dir",
      "http://bucket/dir",
      "https://bucket/dir",
      "tiledb:///array",
      "tiledb://ns",
      "tiledb://ns/",
      "tiledb://",
      "tiledb:///"};
  constexpr size_t N{sizeof(invalid_rest_uri) / sizeof(std::string)};

  bool legacy = GENERATE(true, false);
  for (size_t j = 0; j < N; ++j) {
    std::string x{invalid_rest_uri[j]};
    DYNAMIC_SECTION(
        (legacy ? "legacy " : "TileDB-Server ") << "\"" << x << "\" invalid") {
      CHECK(!URI(x).get_rest_components(legacy, &rest_components).ok());
    }
  }
}

TEST_CASE("URI: Test is_timestamped_name", "[uri][is_timestamped_name]") {
  std::vector<std::pair<std::string, bool>> test_uris = {
      {"__1764100213547_1764100213550_035477e475b011ac8c2f01a13532ccad.vac",
       true},
      {"__1764100213547_1764100213550_035477e475b011ac8c2f01a13532ccad", true},
      {"__1_1_035477e475b011ac8c2f01a13532ccad", true},
      {"__1_1_035477e475b011ac8c2f01a13532ccad_22", true},
      {"__1_1_035477e475b011ac8c2f01a13532ccad_22.vac", true},
      {"__1_1_035477e475b011ac8c2f01a13532ccad_22.tmp", false},
      {"__1_1_035477e475b011ac8c2f01a13532ccad.tmp", false},
      {"__1_1_035477e475b011ac8c2f01a13532cca", false},
      {"___1_035477e475b011ac8c2f01a13532cca", false},
      {"_1_1_035477e475b011ac8c2f01a13532ccad", false},
      {"__1_1_035477e475b011ac8c2f01a13532ccad.", false},
      {"__1_1_035477e475b011a_c8c2f01a13532ccad.", false},
      {"", false},
      {"______", false},
      {"__1_2_3", false},
  };
  for (const auto& test : test_uris) {
    URI uri(test.first);
    DYNAMIC_SECTION("Checking is_timestamped_name URI: " << test.first) {
      CHECK(uri.is_timestamped_name() == test.second);
    }
  }
}

TEST_CASE("URI: Test get_fragment_name", "[uri][get_fragment_name]") {
  std::vector<std::pair<URI, std::optional<URI>>> cases = {
      {URI("a randomish string"), std::nullopt},
      {URI("file:///array_name"), std::nullopt},
      {URI("file:///array_name/__schema/__t1_t2_uuid_version"), std::nullopt},
      {URI("file:///array_name/__fragment_metadata/something_here"),
       std::nullopt},
      {URI("file:///array_name/__fragments/"), std::nullopt},
      {URI("/__fragments//"), std::nullopt},
      {URI("file:///array_name/__fragments//"), std::nullopt},
      {URI("file:///array_name/__fragments/a"),
       URI("file:///array_name/__fragments/a")},
      {URI("file:///array_name/__fragments/a/b"),
       URI("file:///array_name/__fragments/a")},
      {URI("green /__fragments/ and ham"), URI("green /__fragments/ and ham")},
      {URI("green /__fragments/ and ham/but no eggs"),
       URI("green /__fragments/ and ham")}};

  for (auto& test : cases) {
    REQUIRE(test.first.get_fragment_name() == test.second);
  }
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
