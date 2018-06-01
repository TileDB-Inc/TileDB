/**
 * @file   unit-capi-uri.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB Inc.
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
 * Tests the C API for URI.
 */

#include "catch.hpp"
#include "tiledb/sm/c_api/tiledb.h"

#include <string.h>

#ifdef _WIN32
#include <Windows.h>
static const unsigned PLATFORM_PATH_MAX = MAX_PATH;
#elif __APPLE__
#include <sys/syslimits.h>
static const unsigned PLATFORM_PATH_MAX = PATH_MAX;
#else
#include <limits.h>
static const unsigned PLATFORM_PATH_MAX = PATH_MAX;
#endif

TEST_CASE("C API: Test URI", "[capi], [uri]") {
  int rc;
  tiledb_ctx_t* ctx;
  rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);

  char path[PLATFORM_PATH_MAX];
  unsigned path_length = PLATFORM_PATH_MAX;
  rc = tiledb_uri_to_path(ctx, "file:///my/path", path, &path_length);
  CHECK(rc == TILEDB_OK);
#ifdef _WIN32
  CHECK(path_length == 8);
  CHECK(path[path_length] == '\0');
  CHECK(strlen(path) == path_length);
  CHECK(strcmp(path, "\\my\\path") == 0);
#else
  CHECK(path_length == 8);
  CHECK(path[path_length] == '\0');
  CHECK(strlen(path) == path_length);
  CHECK(strcmp(path, "/my/path") == 0);
#endif

  path_length = 9;
  rc = tiledb_uri_to_path(ctx, "file:///my/path", path, &path_length);
  CHECK(rc == TILEDB_OK);
  path_length = 8;
  rc = tiledb_uri_to_path(ctx, "file:///my/path", path, &path_length);
  CHECK(rc == TILEDB_ERR);
  path_length = 0;
  rc = tiledb_uri_to_path(ctx, "file:///my/path", path, &path_length);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_uri_to_path(ctx, "file:///my/path", nullptr, &path_length);
  CHECK(rc == TILEDB_ERR);

  path_length = PLATFORM_PATH_MAX;
  rc = tiledb_uri_to_path(ctx, "file:///C:/my/path", path, &path_length);
  CHECK(rc == TILEDB_OK);
#ifdef _WIN32
  CHECK(path_length == 10);
  CHECK(path[path_length] == '\0');
  CHECK(strlen(path) == path_length);
  CHECK(strcmp(path, "C:\\my\\path") == 0);
#else
  CHECK(path_length == 11);
  CHECK(path[path_length] == '\0');
  CHECK(strlen(path) == path_length);
  CHECK(strcmp(path, "/C:/my/path") == 0);
#endif

  path_length = PLATFORM_PATH_MAX;
  rc = tiledb_uri_to_path(ctx, "s3://my/path", path, &path_length);
  CHECK(rc == TILEDB_OK);
  CHECK(path_length == 12);
  CHECK(path[path_length] == '\0');
  CHECK(strlen(path) == path_length);
  CHECK(strcmp(path, "s3://my/path") == 0);

  path_length = PLATFORM_PATH_MAX;
  rc = tiledb_uri_to_path(ctx, "hdfs://my/path", path, &path_length);
  CHECK(rc == TILEDB_OK);
  CHECK(path_length == 14);
  CHECK(path[path_length] == '\0');
  CHECK(strlen(path) == path_length);
  CHECK(strcmp(path, "hdfs://my/path") == 0);

  tiledb_ctx_free(&ctx);
}
