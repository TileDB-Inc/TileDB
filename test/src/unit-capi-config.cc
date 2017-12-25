/**
 * @file   unit-capi-config.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB Inc.
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
 * Tests the C API config object.
 */

#include "catch.hpp"
#include "tiledb.h"

#include <fstream>

void check_correct_file() {
  // Create a test config file
  std::ofstream ofs("test_config.txt");
  ofs << "   # comment line\n";
  ofs << "tiledb.tile_cache_size 1000\n";
  ofs << "# another comment line\n";
  ofs << "tiledb.array_metadata_cache_size 1000 # some comment\n";
  ofs << "#    last comment line\n";
  ofs.close();

  // Set config from file
  tiledb_config_t* config;
  int rc = tiledb_config_create(&config);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_config_set_from_file(config, "test_config.txt");
  CHECK(rc == TILEDB_OK);
  tiledb_ctx_t* ctx;
  rc = tiledb_ctx_create(&ctx, config);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_ctx_free(ctx);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_free(config);
  CHECK(rc == TILEDB_OK);

  // Remove file
  std::string cmd = std::string("rm -f test_config.txt");
  CHECK(system(cmd.c_str()) == 0);
}

void check_incorrect_file_cannot_open() {
  // Set config from file
  tiledb_config_t* config;
  int rc = tiledb_config_create(&config);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_config_set_from_file(config, "non_existent_file");
  CHECK(rc == TILEDB_OK);
  tiledb_ctx_t* ctx;
  rc = tiledb_ctx_create(&ctx, config);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_ctx_free(ctx);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_free(config);
  CHECK(rc == TILEDB_OK);
}

void check_incorrect_file_missing_value() {
  // Create a test config file
  std::ofstream ofs("test_config.txt");
  ofs << "   # comment line\n";
  ofs << "tiledb.tile_cache_size    \n";
  ofs << "# another comment line\n";
  ofs << "tiledb.array_metadata_cache_size 1000\n";
  ofs << "#    last comment line\n";
  ofs.close();

  // Set config from file
  tiledb_config_t* config;
  int rc = tiledb_config_create(&config);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_config_set_from_file(config, "test_config.txt");
  CHECK(rc == TILEDB_OK);
  tiledb_ctx_t* ctx;
  rc = tiledb_ctx_create(&ctx, config);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_ctx_free(ctx);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_free(config);
  CHECK(rc == TILEDB_OK);

  // Remove file
  std::string cmd = std::string("rm -f test_config.txt");
  CHECK(system(cmd.c_str()) == 0);
}

void check_incorrect_file_extra_word() {
  // Create a test config file
  std::ofstream ofs("test_config.txt");
  ofs << "   # comment line\n";
  ofs << "tiledb.tile_cache_size 1000\n";
  ofs << "# another comment line\n";
  ofs << "tiledb.array_metadata_cache_size 1000 some comment\n";
  ofs << "#    last comment line\n";
  ofs.close();

  // Set config from file
  tiledb_config_t* config;
  int rc = tiledb_config_create(&config);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_config_set_from_file(config, "test_config.txt");
  CHECK(rc == TILEDB_OK);
  tiledb_ctx_t* ctx;
  rc = tiledb_ctx_create(&ctx, config);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_ctx_free(ctx);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_free(config);
  CHECK(rc == TILEDB_OK);

  // Remove file
  std::string cmd = std::string("rm -f test_config.txt");
  CHECK(system(cmd.c_str()) == 0);
}

TEST_CASE("C API: Test config", "[capi], [config]") {
  tiledb_config_t* config;
  int rc = tiledb_config_create(&config);
  REQUIRE(rc == TILEDB_OK);

  // Check correct parameter, correct argument
  rc = tiledb_config_set(config, "tiledb.tile_cache_size", "100");
  CHECK(rc == TILEDB_OK);
  tiledb_ctx_t* ctx;
  rc = tiledb_ctx_create(&ctx, config);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_ctx_free(ctx);
  CHECK(rc == TILEDB_OK);

  // Check correct parameter, correct argument
  rc = tiledb_config_set(config, "tiledb.tile_cache_size", "+100");
  CHECK(rc == TILEDB_OK);
  rc = tiledb_ctx_create(&ctx, config);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_ctx_free(ctx);
  CHECK(rc == TILEDB_OK);

  // Check invalid argument for correct parameter
  rc = tiledb_config_set(config, "tiledb.tile_cache_size", "xadf");
  CHECK(rc == TILEDB_OK);
  rc = tiledb_ctx_create(&ctx, config);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_ctx_free(ctx);
  CHECK(rc == TILEDB_OK);

  // Check invalid argument for correct parameter
  rc = tiledb_config_set(config, "tiledb.tile_cache_size", "10xadf");
  CHECK(rc == TILEDB_OK);
  rc = tiledb_ctx_create(&ctx, config);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_ctx_free(ctx);
  CHECK(rc == TILEDB_OK);

  // Check invalid argument for correct parameter
  rc = tiledb_config_set(config, "tiledb.tile_cache_size", "-10");
  CHECK(rc == TILEDB_OK);
  rc = tiledb_ctx_create(&ctx, config);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_ctx_free(ctx);
  CHECK(rc == TILEDB_OK);

  // Check invalid parameter
  rc = tiledb_config_set(config, "tiledb.tile_cache_size", "10");
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_set(config, "slkjs", "10");
  CHECK(rc == TILEDB_OK);
  rc = tiledb_ctx_create(&ctx, config);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_ctx_free(ctx);
  CHECK(rc == TILEDB_OK);

  // Check out of range argument for correct parameter
  rc = tiledb_config_unset(config, "slkjs");
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_set(
      config, "tiledb.tile_cache_size", "100000000000000000000");
  CHECK(rc == TILEDB_OK);
  rc = tiledb_ctx_create(&ctx, config);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_ctx_free(ctx);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_config_free(config);
  CHECK(rc == TILEDB_OK);
}

TEST_CASE("C API: Test config from file", "[capi], [config]") {
  check_correct_file();
  check_incorrect_file_cannot_open();
  check_incorrect_file_missing_value();
  check_incorrect_file_extra_word();
}
