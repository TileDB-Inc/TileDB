/**
 * @file   benchmark_config.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * Backend-configurable helpers for benchmarks. Reads environment variables
 * to support local filesystem (default), S3, and REST (tiledb://) backends.
 *
 * Environment variables:
 *   BENCH_URI_PREFIX                - Prepended to array name (e.g.
 * "s3://bucket/bench/") BENCH_S3_ENDPOINT              - S3 endpoint override
 *   BENCH_S3_REGION                - S3 region
 *   BENCH_S3_SCHEME                - S3 connection scheme (default "https")
 *   BENCH_S3_USE_VIRTUAL_ADDRESSING - Virtual host-style addressing
 *   BENCH_REST_SERVER              - REST server address
 *   BENCH_REST_TOKEN               - REST API token
 */

#ifndef TILEDB_BENCHMARK_CONFIG_H
#define TILEDB_BENCHMARK_CONFIG_H

#include <tiledb/tiledb>

#include <cstdlib>
#include <string>

/**
 * Returns a full array URI by prepending the BENCH_URI_PREFIX env var
 * to the given name. If unset, returns the name as-is (local filesystem).
 */
inline std::string bench_uri(const std::string& name) {
  const char* prefix = std::getenv("BENCH_URI_PREFIX");
  if (prefix)
    return std::string(prefix) + name;
  return name;
}

/**
 * Returns a tiledb::Config populated from BENCH_S3_* and BENCH_REST_*
 * environment variables. Returns a default config if none are set.
 */
inline tiledb::Config bench_config() {
  tiledb::Config cfg;

  const char* s3_endpoint = std::getenv("BENCH_S3_ENDPOINT");
  if (s3_endpoint)
    cfg["vfs.s3.endpoint_override"] = s3_endpoint;

  const char* s3_region = std::getenv("BENCH_S3_REGION");
  if (s3_region)
    cfg["vfs.s3.region"] = s3_region;

  const char* s3_scheme = std::getenv("BENCH_S3_SCHEME");
  if (s3_scheme)
    cfg["vfs.s3.scheme"] = s3_scheme;

  const char* s3_virtual = std::getenv("BENCH_S3_USE_VIRTUAL_ADDRESSING");
  if (s3_virtual)
    cfg["vfs.s3.use_virtual_addressing"] = s3_virtual;

  const char* rest_server = std::getenv("BENCH_REST_SERVER");
  if (rest_server)
    cfg["rest.server_address"] = rest_server;

  const char* rest_token = std::getenv("BENCH_REST_TOKEN");
  if (rest_token)
    cfg["rest.token"] = rest_token;

  return cfg;
}

/**
 * Cleans up a benchmark array for any backend.
 * Uses Object::remove for tiledb:// URIs, VFS::remove_dir otherwise.
 */
inline void bench_teardown(tiledb::Context& ctx, const std::string& uri) {
  if (uri.rfind("tiledb://", 0) == 0) {
    tiledb::Object::remove(ctx, uri);
  } else {
    tiledb::VFS vfs(ctx);
    if (vfs.is_dir(uri))
      vfs.remove_dir(uri);
  }
}

#endif
