/**
 * @file   tiledb_config.c
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This example shows how to use the TileDB config objects.
 *
 * Simply run:
 *
 * ```
 * $ ./tiledb_config_c
 * Default settings:
 * "sm.array_schema_cache_size" : "10000000"
 * "sm.fragment_metadata_cache_size" : "10000000"
 * "sm.tile_cache_size" : "10000000"
 * "vfs.hdfs.kerb_ticket_cache_path" : ""
 * "vfs.hdfs.name_node_uri" : ""
 * "vfs.hdfs.username" : ""
 * "vfs.s3.connect_timeout_ms" : "3000"
 * "vfs.s3.endpoint_override" : "localhost:9000"
 * "vfs.s3.file_buffer_size" : "5242880"
 * "vfs.s3.region" : ""
 * "vfs.s3.request_timeout_ms" : "3000"
 * "vfs.s3.scheme" : "http"
 * "vfs.s3.use_virtual_addressing" : "false"
 *
 * Tile cache size: 10000000
 *
 * VFS S3 settings:
 * "connect_timeout_ms" : "5000"
 * "endpoint_override" : "localhost:8888"
 * "file_buffer_size" : "5242880"
 * "region" : ""
 * "request_timeout_ms" : "3000"
 * "scheme" : "http"
 * "use_virtual_addressing" : "false"
 * ```
 */

#include <stdio.h>
#include <tiledb/tiledb.h>

int main() {
  // Create a TileDB config
  tiledb_config_t* config;
  tiledb_error_t* error = NULL;
  tiledb_config_create(&config, &error);

  // Create a TileDB config iterator
  tiledb_config_iter_t* config_iter;
  tiledb_config_iter_create(config, &config_iter, NULL, &error);

  // Upon creating a TileDB config (and before setting any values to the
  // config parameters), the config contains the default settings. We can
  // iterate over the parameter-value pairs as follows
  printf("Default settings:\n");
  int done = 0;
  const char *param, *value;
  tiledb_config_iter_done(config_iter, &done, &error);
  while (!done) {
    tiledb_config_iter_here(config_iter, &param, &value, &error);
    printf("\"%s\" : \"%s\"\n", param, value);
    tiledb_config_iter_next(config_iter, &error);
    tiledb_config_iter_done(config_iter, &done, &error);
  }

  // Set values
  tiledb_config_set(config, "vfs.s3.connect_timeout_ms", "5000", &error);
  tiledb_config_set(
      config, "vfs.s3.endpoint_override", "localhost:8888", &error);

  // Get values
  tiledb_config_get(config, "sm.tile_cache_size", &value, &error);
  printf("\nTile cache size: %s\n", value);

  // We can also iterate over parameters with a specific prefix. Here
  // we print only the S3 settings. Note that the resulting parameter names
  // have this prefix stripped.
  printf("\nVFS S3 settings:\n");
  tiledb_config_iter_reset(config, config_iter, "vfs.s3.", &error);
  tiledb_config_iter_done(config_iter, &done, &error);
  while (!done) {
    tiledb_config_iter_here(config_iter, &param, &value, &error);
    printf("\"%s\" : \"%s\"\n", param, value);
    tiledb_config_iter_next(config_iter, &error);
    tiledb_config_iter_done(config_iter, &done, &error);
  }

  // Assign a config object to a context and VFS
  tiledb_ctx_t* ctx;
  tiledb_vfs_t* vfs;
  tiledb_ctx_create(&ctx, config);
  tiledb_vfs_create(ctx, &vfs, config);

  // Clean up
  tiledb_config_free(&config);
  tiledb_config_iter_free(&config_iter);
  if (error != NULL)
    tiledb_error_free(&error);
  tiledb_vfs_free(ctx, &vfs);
  tiledb_ctx_free(&ctx);

  return 0;
}