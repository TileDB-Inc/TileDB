/**
 * @file   config.c
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
 * This program shows how to set/get the TileDB configuration parameters.
 */

#include <stdio.h>
#include <tiledb/tiledb.h>

void set_get_config_ctx_vfs() {
  // Create a TileDB config
  tiledb_config_t *config, *config_ctx, *config_vfs;
  tiledb_error_t* error = NULL;
  tiledb_config_alloc(&config, &error);

  // Set/Get config to/from ctx
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(config, &ctx);
  tiledb_ctx_get_config(ctx, &config_ctx);

  // Set/Get config to/from bfs
  tiledb_vfs_t* vfs;
  tiledb_vfs_alloc(ctx, config, &vfs);
  tiledb_vfs_get_config(ctx, vfs, &config_vfs);

  // Clean up
  tiledb_ctx_free(&ctx);
  tiledb_vfs_free(&vfs);
  tiledb_config_free(&config);
  tiledb_config_free(&config_ctx);
  tiledb_config_free(&config_vfs);
}

void set_get_config() {
  // Create a TileDB config
  tiledb_config_t* config;
  tiledb_error_t* error = NULL;
  tiledb_config_alloc(&config, &error);

  // Set value
  tiledb_config_set(config, "vfs.s3.connect_timeout_ms", "5000", &error);

  // Get value
  const char* value;
  tiledb_config_get(config, "sm.memory_budget", &value, &error);
  printf("\nMemory budget: %s\n\n", value);

  // Clean up
  tiledb_config_free(&config);
}

void print_default() {
  // Create a TileDB config
  tiledb_config_t* config;
  tiledb_error_t* error = NULL;
  tiledb_config_alloc(&config, &error);

  // Create a TileDB config iterator
  tiledb_config_iter_t* config_iter;
  tiledb_config_iter_alloc(config, NULL, &config_iter, &error);

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

  // Clean up
  tiledb_config_free(&config);
  tiledb_config_iter_free(&config_iter);
}

void iter_config_with_prefix() {
  // Create a TileDB config
  tiledb_config_t* config;
  tiledb_error_t* error = NULL;
  tiledb_config_alloc(&config, &error);

  // Create a TileDB config iterator
  tiledb_config_iter_t* config_iter;
  tiledb_config_iter_alloc(config, "vfs.s3", &config_iter, &error);

  printf("\nVFS S3 settings:\n");
  int done = 0;
  const char *param, *value;
  tiledb_config_iter_done(config_iter, &done, &error);
  while (!done) {
    tiledb_config_iter_here(config_iter, &param, &value, &error);
    printf("\"%s\" : \"%s\"\n", param, value);
    tiledb_config_iter_next(config_iter, &error);
    tiledb_config_iter_done(config_iter, &done, &error);
  }

  // Clean up
  tiledb_config_free(&config);
  tiledb_config_iter_free(&config_iter);
}

void save_load_config() {
  // Create a TileDB config
  tiledb_config_t* config;
  tiledb_error_t* error = NULL;
  tiledb_config_alloc(&config, &error);

  // Set value
  tiledb_config_set(config, "sm.memory_budget", "0", &error);

  // Save to file
  tiledb_config_save_to_file(config, "tiledb_config.txt", &error);

  // Load from file
  tiledb_config_t* config_load;
  tiledb_config_alloc(&config_load, &error);
  tiledb_config_load_from_file(config_load, "tiledb_config.txt", &error);

  // Get value
  const char* value;
  tiledb_config_get(config_load, "sm.memory_budget", &value, &error);
  printf("\nMemory budget: %s\n\n", value);

  // Clean up
  tiledb_config_free(&config);
  tiledb_config_free(&config_load);
}

int main() {
  set_get_config_ctx_vfs();
  set_get_config();
  print_default();
  iter_config_with_prefix();
  save_load_config();

  return 0;
}
