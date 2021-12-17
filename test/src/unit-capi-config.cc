/**
 * @file   unit-capi-config.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
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
#include "tiledb/sm/c_api/tiledb.h"

#include <tiledb/sm/misc/constants.h>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <thread>

void remove_file(const std::string& filename) {
  // Remove file
  tiledb_ctx_t* ctx = nullptr;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_t* vfs = nullptr;
  REQUIRE(tiledb_vfs_alloc(ctx, nullptr, &vfs) == TILEDB_OK);
  CHECK(tiledb_vfs_remove_file(ctx, vfs, filename.c_str()) == TILEDB_OK);
  tiledb_vfs_free(&vfs);
  tiledb_ctx_free(&ctx);
}

void check_load_correct_file() {
  // Create a test config file
  std::ofstream ofs("test_config.txt");
  ofs << "   # comment line\n";
  ofs << "sm.tile_cache_size 1000\n";
  ofs << "# another comment line\n";
  ofs << "sm.consolidation.steps 2 # some comment\n";
  ofs << "#    last comment line\n";
  ofs.close();

  // Set config from file
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  int rc = tiledb_config_alloc(&config, &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_load_from_file(config, "test_config.txt", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  tiledb_ctx_t* ctx = nullptr;
  rc = tiledb_ctx_alloc(config, &ctx);
  CHECK(rc == TILEDB_OK);
  tiledb_ctx_free(&ctx);
  tiledb_config_free(&config);

  remove_file("test_config.txt");
}

void check_error(tiledb_error_t* error, const std::string& msg) {
  const char* err_msg;
  int rc = tiledb_error_message(error, &err_msg);
  CHECK(rc == TILEDB_OK);
  CHECK(std::string(err_msg) == msg);
}

void check_load_incorrect_file_cannot_open() {
  // Set config from file
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  int rc = tiledb_config_alloc(&config, &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_load_from_file(config, "non_existent_file", &error);
  CHECK(rc == TILEDB_ERR);
  CHECK(error != nullptr);
  check_error(
      error,
      "[TileDB::Config] Error: Failed to open config file 'non_existent_file'");
  tiledb_error_free(&error);
  tiledb_config_free(&config);
  CHECK(config == nullptr);
}

void check_load_incorrect_file_missing_value() {
  // Create a test config file
  std::ofstream ofs("test_config.txt");
  ofs << "   # comment line\n";
  ofs << "sm.tile_cache_size    \n";
  ofs << "# another comment line\n";
  ofs << "sm.consolidation.steps 2 # some comment\n";
  ofs << "#    last comment line\n";
  ofs.close();

  // Set config from file
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  int rc = tiledb_config_alloc(&config, &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_load_from_file(config, "test_config.txt", &error);
  CHECK(rc == TILEDB_ERR);
  CHECK(error != nullptr);
  check_error(
      error,
      "[TileDB::Config] Error: Failed to parse config file 'test_config.txt'; "
      "Missing parameter value (line: 1)");
  tiledb_error_free(&error);
  CHECK(error == nullptr);
  tiledb_config_free(&config);
  CHECK(config == nullptr);
  remove_file("test_config.txt");
}

void check_load_incorrect_file_extra_word() {
  // Create a test config file
  std::ofstream ofs("test_config.txt");
  ofs << "   # comment line\n";
  ofs << "sm.tile_cache_size 1000\n";
  ofs << "# another comment line\n";
  ofs << "sm.consolidation.steps 2 some comment\n";
  ofs << "#    last comment line\n";
  ofs.close();

  // Set config from file
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  int rc = tiledb_config_alloc(&config, &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_load_from_file(config, "test_config.txt", &error);
  CHECK(rc == TILEDB_ERR);
  CHECK(error != nullptr);
  check_error(
      error,
      "[TileDB::Config] Error: Failed to parse config file 'test_config.txt'; "
      "Invalid line format (line: 3)");
  tiledb_error_free(&error);
  tiledb_config_free(&config);
  remove_file("test_config.txt");
}

void check_save_to_file() {
  tiledb_config_t* config;
  tiledb_error_t* error;
  int rc = tiledb_config_alloc(&config, &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);

  // Check that azure storage account name is not serialized.
  rc = tiledb_config_set(
      config, "vfs.azure.storage_account_name", "storagename", &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);

  // Check that azure account key is not serialized.
  rc = tiledb_config_set(
      config, "vfs.azure.storage_account_key", "secret", &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);

  // Check that azure SAS token is not serialized.
  rc = tiledb_config_set(
      config, "vfs.azure.storage_sas_token", "secret", &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);

  // Check that password is not serialized.
  rc = tiledb_config_set(config, "vfs.s3.proxy_password", "password", &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);

  // Check that aws access key id is not serialized.
  rc = tiledb_config_set(config, "vfs.s3.aws_access_key_id", "keyid", &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);

  // Check that aws secret access key is not serialized.
  rc = tiledb_config_set(
      config, "vfs.s3.aws_secret_access_key", "secret", &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);

  // Check that aws session token is not serialized.
  rc = tiledb_config_set(
      config, "vfs.s3.aws_session_token", "session_token", &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);

  rc = tiledb_config_save_to_file(config, "test_config.txt", &error);
  REQUIRE(rc == TILEDB_OK);

  std::stringstream ss;
  // Items here need to be assigned to 'ss' in alphabetical order as
  // an std::[ordered_]map is where the comparison values saved to file
  // come from.
  ss << "config.env_var_prefix TILEDB_\n";
  ss << "config.logging_format DEFAULT\n";
#ifdef TILEDB_VERBOSE
  ss << "config.logging_level 1\n";
#else
  ss << "config.logging_level 0\n";
#endif
  ss << "rest.http_compressor any\n";
  ss << "rest.retry_count 25\n";
  ss << "rest.retry_delay_factor 1.25\n";
  ss << "rest.retry_http_codes 503\n";
  ss << "rest.retry_initial_delay_ms 500\n";
  ss << "rest.server_address https://api.tiledb.com\n";
  ss << "rest.server_serialization_format CAPNP\n";
  ss << "sm.check_coord_dups true\n";
  ss << "sm.check_coord_oob true\n";
  ss << "sm.check_global_order true\n";
  ss << "sm.compute_concurrency_level " << std::thread::hardware_concurrency()
     << "\n";
  ss << "sm.consolidation.amplification 1.0\n";
  ss << "sm.consolidation.buffer_size 50000000\n";
  ss << "sm.consolidation.mode fragments\n";
  ss << "sm.consolidation.step_max_frags 4294967295\n";
  ss << "sm.consolidation.step_min_frags 4294967295\n";
  ss << "sm.consolidation.step_size_ratio 0.0\n";
  ss << "sm.consolidation.steps 4294967295\n";
  ss << "sm.consolidation.timestamp_end " << std::to_string(UINT64_MAX) << "\n";
  ss << "sm.consolidation.timestamp_start 0\n";
  ss << "sm.dedup_coords false\n";
  ss << "sm.enable_signal_handlers true\n";
  ss << "sm.encryption_key 0\n";
  ss << "sm.encryption_type NO_ENCRYPTION\n";
  ss << "sm.io_concurrency_level " << std::thread::hardware_concurrency()
     << "\n";
  ss << "sm.max_tile_overlap_size 314572800\n";
  ss << "sm.mem.malloc_trim true\n";
  ss << "sm.mem.reader.sparse_global_order.ratio_array_data 0.1\n";
  ss << "sm.mem.reader.sparse_global_order.ratio_coords 0.5\n";
  ss << "sm.mem.reader.sparse_global_order.ratio_query_condition 0.25\n";
  ss << "sm.mem.reader.sparse_global_order.ratio_rcs 0.05\n";
  ss << "sm.mem.reader.sparse_global_order.ratio_tile_ranges 0.1\n";
  ss << "sm.mem.reader.sparse_unordered_with_dups.ratio_array_data 0.1\n";
  ss << "sm.mem.reader.sparse_unordered_with_dups.ratio_coords 0.5\n";
  ss << "sm.mem.reader.sparse_unordered_with_dups.ratio_query_condition "
        "0.25\n";
  ss << "sm.mem.reader.sparse_unordered_with_dups.ratio_tile_ranges 0.1\n";
  ss << "sm.mem.total_budget 10737418240\n";
  ss << "sm.memory_budget 5368709120\n";
  ss << "sm.memory_budget_var 10737418240\n";
  ss << "sm.query.dense.reader legacy\n";
  ss << "sm.query.sparse_global_order.reader legacy\n";
  ss << "sm.query.sparse_unordered_with_dups.non_overlapping_ranges false\n";
  ss << "sm.query.sparse_unordered_with_dups.reader refactored\n";
  ss << "sm.read_range_oob warn\n";
  ss << "sm.skip_checksum_validation false\n";
  ss << "sm.skip_est_size_partitioning false\n";
  ss << "sm.tile_cache_size 10000000\n";
  ss << "sm.vacuum.mode fragments\n";
  ss << "sm.vacuum.timestamp_end " << std::to_string(UINT64_MAX) << "\n";
  ss << "sm.vacuum.timestamp_start 0\n";
  ss << "sm.var_offsets.bitsize 64\n";
  ss << "sm.var_offsets.extra_element false\n";
  ss << "sm.var_offsets.mode bytes\n";
  ss << "vfs.azure.block_list_block_size 5242880\n";
  ss << "vfs.azure.max_parallel_ops " << std::thread::hardware_concurrency()
     << "\n";
  ss << "vfs.azure.use_block_list_upload true\n";
  ss << "vfs.azure.use_https true\n";
  ss << "vfs.file.max_parallel_ops " << std::thread::hardware_concurrency()
     << "\n";
  ss << "vfs.file.posix_directory_permissions 755\n";
  ss << "vfs.file.posix_file_permissions 644\n";
  ss << "vfs.gcs.max_parallel_ops " << std::thread::hardware_concurrency()
     << "\n";
  ss << "vfs.gcs.multi_part_size 5242880\n";
  ss << "vfs.gcs.request_timeout_ms 3000\n";
  ss << "vfs.gcs.use_multi_part_upload true\n";
  ss << "vfs.max_batch_size 104857600\n";
  ss << "vfs.min_batch_gap 512000\n";
  ss << "vfs.min_batch_size 20971520\n";
  ss << "vfs.min_parallel_size 10485760\n";
  ss << "vfs.read_ahead_cache_size 10485760\n";
  ss << "vfs.read_ahead_size 102400\n";
  ss << "vfs.s3.bucket_canned_acl NOT_SET\n";
  ss << "vfs.s3.connect_max_tries 5\n";
  ss << "vfs.s3.connect_scale_factor 25\n";
  ss << "vfs.s3.connect_timeout_ms 10800\n";
  ss << "vfs.s3.logging_level Off\n";
  ss << "vfs.s3.max_parallel_ops " << std::thread::hardware_concurrency()
     << "\n";
  ss << "vfs.s3.multipart_part_size 5242880\n";
  ss << "vfs.s3.object_canned_acl NOT_SET\n";
  ss << "vfs.s3.proxy_port 0\n";
  ss << "vfs.s3.proxy_scheme http\n";
  ss << "vfs.s3.region us-east-1\n";
  ss << "vfs.s3.request_timeout_ms 3000\n";
  ss << "vfs.s3.requester_pays false\n";
  ss << "vfs.s3.scheme https\n";
  ss << "vfs.s3.skip_init false\n";
  ss << "vfs.s3.use_multipart_upload true\n";
  ss << "vfs.s3.use_virtual_addressing true\n";
  ss << "vfs.s3.verify_ssl true\n";

  std::ifstream ifs("test_config.txt");
  std::stringstream ss_file;
  for (std::string line; std::getline(ifs, line);)
    ss_file << line << "\n";
  ifs.close();

  CHECK(ss.str() == ss_file.str());
  remove_file("test_config.txt");

  tiledb_config_free(&config);
}

TEST_CASE("C API: Test config", "[capi][config]") {
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  int rc = tiledb_config_alloc(&config, &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);

  // Check correct parameter, correct argument
  rc = tiledb_config_set(config, "sm.tile_cache_size", "100", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  tiledb_ctx_t* ctx;
  rc = tiledb_ctx_alloc(config, &ctx);
  CHECK(rc == TILEDB_OK);
  tiledb_ctx_free(&ctx);
  CHECK(ctx == nullptr);

  // Check get for existing argument
  const char* value = nullptr;
  rc = tiledb_config_get(config, "sm.tile_cache_size", &value, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  CHECK(!strcmp(value, "100"));

  // Check get for non-existing argument
  rc = tiledb_config_get(config, "foo", &value, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  CHECK(value == nullptr);

  // Check get config from context
  rc = tiledb_ctx_alloc(config, &ctx);
  CHECK(rc == TILEDB_OK);
  tiledb_config_t* get_config = nullptr;
  rc = tiledb_ctx_get_config(ctx, &get_config);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_get(get_config, "sm.tile_cache_size", &value, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  CHECK(!strcmp(value, "100"));
  tiledb_config_free(&get_config);
  tiledb_ctx_free(&ctx);

  // Check correct parameter, correct argument
  rc = tiledb_config_set(config, "sm.tile_cache_size", "+100", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_ctx_alloc(config, &ctx);
  CHECK(rc == TILEDB_OK);
  tiledb_ctx_free(&ctx);

  // Check invalid argument for correct parameter
  rc = tiledb_config_set(config, "sm.tile_cache_size", "xadf", &error);
  CHECK(rc == TILEDB_ERR);
  CHECK(error != nullptr);
  check_error(
      error,
      "[TileDB::Utils] Error: Failed to convert string 'xadf' to uint64_t; "
      "Invalid argument");
  tiledb_error_free(&error);

  // Check invalid argument for correct parameter
  rc = tiledb_config_set(config, "sm.tile_cache_size", "10xadf", &error);
  CHECK(rc == TILEDB_ERR);
  CHECK(error != nullptr);
  check_error(
      error,
      "[TileDB::Utils] Error: Failed to convert string '10xadf' to uint64_t; "
      "Invalid argument");
  tiledb_error_free(&error);

  // Check invalid argument for correct parameter
  rc = tiledb_config_set(config, "sm.tile_cache_size", "-10", &error);
  CHECK(rc == TILEDB_ERR);
  CHECK(error != nullptr);
  check_error(
      error,
      "[TileDB::Utils] Error: Failed to convert string '-10' to uint64_t; "
      "Invalid argument");
  tiledb_error_free(&error);

  // Set valid
  rc = tiledb_config_set(config, "sm.tile_cache_size", "10", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);

  // Check invalid parameters are ignored
  rc = tiledb_config_set(config, "sm.unknown_config_param", "10", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);

  // Unset invalid parameter (ignore)
  rc = tiledb_config_unset(config, "slkjs", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);

  // Unset valid parameter
  rc = tiledb_config_unset(config, "sm.tile_cache_size", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_get(config, "sm.tile_cache_size", &value, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  CHECK(!strcmp(value, "10000000"));

  // Set valid, defaulting parameter
  rc = tiledb_config_set(config, "vfs.s3.region", "pluto", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_get(config, "vfs.s3.region", &value, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  CHECK(!strcmp(value, "pluto"));

  // Unset valid, defaulting parameter
  rc = tiledb_config_unset(config, "vfs.s3.region", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_get(config, "vfs.s3.region", &value, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  CHECK(!strcmp(value, "us-east-1"));

  // Set valid, non-defaulting parameter
  rc = tiledb_config_set(config, "foo", "123", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_get(config, "foo", &value, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  CHECK(!strcmp(value, "123"));

  // Unset valid, non-defaulting parameter
  rc = tiledb_config_unset(config, "foo", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_get(config, "foo", &value, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  CHECK(value == nullptr);

  // Check out of range argument for correct parameter
  rc = tiledb_config_set(
      config, "sm.tile_cache_size", "100000000000000000000", &error);
  CHECK(rc == TILEDB_ERR);
  CHECK(error != nullptr);
  check_error(
      error,
      "[TileDB::Utils] Error: Failed to convert string '100000000000000000000' "
      "to uint64_t; Value out of range");

  // Check config and config2 are the same
  tiledb_config_t* config2 = config;
  uint8_t equal = 2;
  rc = tiledb_config_compare(config, config2, &equal);
  CHECK(rc == TILEDB_OK);
  CHECK(equal == 1);

  // Check config and config3 are not the same
  tiledb_config_t* config3 = nullptr;
  tiledb_error_t* error2 = nullptr;
  rc = tiledb_config_alloc(&config3, &error2);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error2 == nullptr);

  uint8_t equal2 = 0;
  rc = tiledb_config_compare(config, config3, &equal2);
  CHECK(rc == TILEDB_OK);
  CHECK(equal2 == 0);

  tiledb_error_free(&error);
  tiledb_config_free(&config);
  tiledb_error_free(&error2);
  tiledb_config_free(&config3);
}

TEST_CASE("C API: Test config iter", "[capi][config]") {
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);

  // Populate a config
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  rc = tiledb_config_alloc(&config, &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_set(config, "config.logging_level", "2", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_set(config, "config.logging_format", "JSON", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_set(config, "sm.tile_cache_size", "100", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_set(config, "vfs.s3.scheme", "https", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_set(config, "vfs.hdfs.username", "stavros", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_set(config, "sm.var_offsets.mode", "elements", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc =
      tiledb_config_set(config, "sm.var_offsets.extra_element", "true", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_set(config, "sm.var_offsets.bitsize", "32", &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);

  // Prepare maps
  std::map<std::string, std::string> all_param_values;
  all_param_values["config.env_var_prefix"] = "TILEDB_";
  all_param_values["config.logging_level"] = "2";
  all_param_values["config.logging_format"] = "JSON";
  all_param_values["rest.server_address"] = "https://api.tiledb.com";
  all_param_values["rest.server_serialization_format"] = "CAPNP";
  all_param_values["rest.http_compressor"] = "any";
  all_param_values["rest.retry_count"] = "25";
  all_param_values["rest.retry_delay_factor"] = "1.25";
  all_param_values["rest.retry_initial_delay_ms"] = "500";
  all_param_values["rest.retry_http_codes"] = "503";
  all_param_values["sm.encryption_key"] = "0";
  all_param_values["sm.encryption_type"] = "NO_ENCRYPTION";
  all_param_values["sm.dedup_coords"] = "false";
  all_param_values["sm.check_coord_dups"] = "true";
  all_param_values["sm.check_coord_oob"] = "true";
  all_param_values["sm.check_global_order"] = "true";
  all_param_values["sm.tile_cache_size"] = "100";
  all_param_values["sm.skip_est_size_partitioning"] = "false";
  all_param_values["sm.memory_budget"] = "5368709120";
  all_param_values["sm.memory_budget_var"] = "10737418240";
  all_param_values["sm.query.dense.reader"] = "legacy";
  all_param_values["sm.query.sparse_global_order.reader"] = "legacy";
  all_param_values["sm.query.sparse_unordered_with_dups.reader"] = "refactored";
  all_param_values
      ["sm.query.sparse_unordered_with_dups.non_overlapping_ranges"] = "false";
  all_param_values["sm.mem.malloc_trim"] = "true";
  all_param_values["sm.mem.total_budget"] = "10737418240";
  all_param_values["sm.mem.reader.sparse_global_order.ratio_coords"] = "0.5";
  all_param_values["sm.mem.reader.sparse_global_order.ratio_query_condition"] =
      "0.25";
  all_param_values["sm.mem.reader.sparse_global_order.ratio_tile_ranges"] =
      "0.1";
  all_param_values["sm.mem.reader.sparse_global_order.ratio_array_data"] =
      "0.1";
  all_param_values["sm.mem.reader.sparse_global_order.ratio_rcs"] = "0.05";
  all_param_values["sm.mem.reader.sparse_unordered_with_dups.ratio_coords"] =
      "0.5";
  all_param_values
      ["sm.mem.reader.sparse_unordered_with_dups.ratio_query_condition"] =
          "0.25";
  all_param_values
      ["sm.mem.reader.sparse_unordered_with_dups.ratio_tile_ranges"] = "0.1";
  all_param_values
      ["sm.mem.reader.sparse_unordered_with_dups.ratio_array_data"] = "0.1";
  all_param_values["sm.enable_signal_handlers"] = "true";
  all_param_values["sm.compute_concurrency_level"] =
      std::to_string(std::thread::hardware_concurrency());
  all_param_values["sm.io_concurrency_level"] =
      std::to_string(std::thread::hardware_concurrency());
  all_param_values["sm.skip_checksum_validation"] = "false";
  all_param_values["sm.consolidation.amplification"] = "1.0";
  all_param_values["sm.consolidation.steps"] = "4294967295";
  all_param_values["sm.consolidation.timestamp_start"] = "0";
  all_param_values["sm.consolidation.timestamp_end"] =
      std::to_string(UINT64_MAX);
  all_param_values["sm.consolidation.step_min_frags"] = "4294967295";
  all_param_values["sm.consolidation.step_max_frags"] = "4294967295";
  all_param_values["sm.consolidation.buffer_size"] = "50000000";
  all_param_values["sm.consolidation.step_size_ratio"] = "0.0";
  all_param_values["sm.consolidation.mode"] = "fragments";
  all_param_values["sm.read_range_oob"] = "warn";
  all_param_values["sm.vacuum.mode"] = "fragments";
  all_param_values["sm.vacuum.timestamp_start"] = "0";
  all_param_values["sm.vacuum.timestamp_end"] = std::to_string(UINT64_MAX);
  all_param_values["sm.var_offsets.bitsize"] = "32";
  all_param_values["sm.var_offsets.extra_element"] = "true";
  all_param_values["sm.var_offsets.mode"] = "elements";
  all_param_values["sm.max_tile_overlap_size"] = "314572800";

  all_param_values["vfs.max_batch_size"] = "104857600";
  all_param_values["vfs.min_batch_gap"] = "512000";
  all_param_values["vfs.min_batch_size"] = "20971520";
  all_param_values["vfs.min_parallel_size"] = "10485760";
  all_param_values["vfs.read_ahead_size"] = "102400";
  all_param_values["vfs.read_ahead_cache_size"] = "10485760";
  all_param_values["vfs.gcs.project_id"] = "";
  all_param_values["vfs.gcs.max_parallel_ops"] =
      std::to_string(std::thread::hardware_concurrency());
  all_param_values["vfs.gcs.multi_part_size"] = "5242880";
  all_param_values["vfs.gcs.use_multi_part_upload"] = "true";
  all_param_values["vfs.gcs.request_timeout_ms"] = "3000";
  all_param_values["vfs.azure.storage_account_name"] = "";
  all_param_values["vfs.azure.storage_account_key"] = "";
  all_param_values["vfs.azure.storage_sas_token"] = "";
  all_param_values["vfs.azure.blob_endpoint"] = "";
  all_param_values["vfs.azure.block_list_block_size"] = "5242880";
  all_param_values["vfs.azure.max_parallel_ops"] =
      std::to_string(std::thread::hardware_concurrency());
  all_param_values["vfs.azure.use_block_list_upload"] = "true";
  all_param_values["vfs.azure.use_https"] = "true";
  all_param_values["vfs.file.posix_file_permissions"] = "644";
  all_param_values["vfs.file.posix_directory_permissions"] = "755";
  all_param_values["vfs.file.max_parallel_ops"] =
      std::to_string(std::thread::hardware_concurrency());
  all_param_values["vfs.s3.scheme"] = "https";
  all_param_values["vfs.s3.region"] = "us-east-1";
  all_param_values["vfs.s3.aws_access_key_id"] = "";
  all_param_values["vfs.s3.aws_secret_access_key"] = "";
  all_param_values["vfs.s3.aws_session_token"] = "";
  all_param_values["vfs.s3.aws_role_arn"] = "";
  all_param_values["vfs.s3.aws_external_id"] = "";
  all_param_values["vfs.s3.aws_load_frequency"] = "";
  all_param_values["vfs.s3.aws_session_name"] = "";
  all_param_values["vfs.s3.endpoint_override"] = "";
  all_param_values["vfs.s3.use_virtual_addressing"] = "true";
  all_param_values["vfs.s3.skip_init"] = "false";
  all_param_values["vfs.s3.use_multipart_upload"] = "true";
  all_param_values["vfs.s3.max_parallel_ops"] =
      std::to_string(std::thread::hardware_concurrency());
  all_param_values["vfs.s3.multipart_part_size"] = "5242880";
  all_param_values["vfs.s3.ca_file"] = "";
  all_param_values["vfs.s3.ca_path"] = "";
  all_param_values["vfs.s3.connect_timeout_ms"] = "10800";
  all_param_values["vfs.s3.connect_max_tries"] = "5";
  all_param_values["vfs.s3.connect_scale_factor"] = "25";
  all_param_values["vfs.s3.sse"] = "";
  all_param_values["vfs.s3.sse_kms_key_id"] = "";
  all_param_values["vfs.s3.logging_level"] = "Off";
  all_param_values["vfs.s3.request_timeout_ms"] = "3000";
  all_param_values["vfs.s3.requester_pays"] = "false";
  all_param_values["vfs.s3.proxy_host"] = "";
  all_param_values["vfs.s3.proxy_password"] = "";
  all_param_values["vfs.s3.proxy_port"] = "0";
  all_param_values["vfs.s3.proxy_scheme"] = "http";
  all_param_values["vfs.s3.proxy_username"] = "";
  all_param_values["vfs.s3.verify_ssl"] = "true";
  all_param_values["vfs.hdfs.username"] = "stavros";
  all_param_values["vfs.hdfs.kerb_ticket_cache_path"] = "";
  all_param_values["vfs.hdfs.name_node_uri"] = "";
  all_param_values["vfs.s3.bucket_canned_acl"] = "NOT_SET";
  all_param_values["vfs.s3.object_canned_acl"] = "NOT_SET";

  std::map<std::string, std::string> vfs_param_values;
  vfs_param_values["max_batch_size"] = "104857600";
  vfs_param_values["min_batch_gap"] = "512000";
  vfs_param_values["min_batch_size"] = "20971520";
  vfs_param_values["min_parallel_size"] = "10485760";
  vfs_param_values["read_ahead_size"] = "102400";
  vfs_param_values["read_ahead_cache_size"] = "10485760";
  vfs_param_values["gcs.project_id"] = "";
  vfs_param_values["gcs.max_parallel_ops"] =
      std::to_string(std::thread::hardware_concurrency());
  vfs_param_values["gcs.multi_part_size"] = "5242880";
  vfs_param_values["gcs.use_multi_part_upload"] = "true";
  vfs_param_values["gcs.request_timeout_ms"] = "3000";
  vfs_param_values["azure.storage_account_name"] = "";
  vfs_param_values["azure.storage_account_key"] = "";
  vfs_param_values["azure.storage_sas_token"] = "";
  vfs_param_values["azure.blob_endpoint"] = "";
  vfs_param_values["azure.block_list_block_size"] = "5242880";
  vfs_param_values["azure.max_parallel_ops"] =
      std::to_string(std::thread::hardware_concurrency());
  vfs_param_values["azure.use_block_list_upload"] = "true";
  vfs_param_values["azure.use_https"] = "true";
  vfs_param_values["file.posix_file_permissions"] = "644";
  vfs_param_values["file.posix_directory_permissions"] = "755";
  vfs_param_values["file.max_parallel_ops"] =
      std::to_string(std::thread::hardware_concurrency());
  vfs_param_values["s3.scheme"] = "https";
  vfs_param_values["s3.region"] = "us-east-1";
  vfs_param_values["s3.aws_access_key_id"] = "";
  vfs_param_values["s3.aws_secret_access_key"] = "";
  vfs_param_values["s3.aws_session_token"] = "";
  vfs_param_values["s3.aws_role_arn"] = "";
  vfs_param_values["s3.aws_external_id"] = "";
  vfs_param_values["s3.aws_load_frequency"] = "";
  vfs_param_values["s3.aws_session_name"] = "";
  vfs_param_values["s3.endpoint_override"] = "";
  vfs_param_values["s3.use_virtual_addressing"] = "true";
  vfs_param_values["s3.skip_init"] = "false";
  vfs_param_values["s3.use_multipart_upload"] = "true";
  vfs_param_values["s3.max_parallel_ops"] =
      std::to_string(std::thread::hardware_concurrency());
  vfs_param_values["s3.multipart_part_size"] = "5242880";
  vfs_param_values["s3.ca_file"] = "";
  vfs_param_values["s3.ca_path"] = "";
  vfs_param_values["s3.connect_timeout_ms"] = "10800";
  vfs_param_values["s3.connect_max_tries"] = "5";
  vfs_param_values["s3.connect_scale_factor"] = "25";
  vfs_param_values["s3.sse"] = "";
  vfs_param_values["s3.sse_kms_key_id"] = "";
  vfs_param_values["s3.logging_level"] = "Off";
  vfs_param_values["s3.request_timeout_ms"] = "3000";
  vfs_param_values["s3.requester_pays"] = "false";
  vfs_param_values["s3.proxy_host"] = "";
  vfs_param_values["s3.proxy_password"] = "";
  vfs_param_values["s3.proxy_port"] = "0";
  vfs_param_values["s3.proxy_scheme"] = "http";
  vfs_param_values["s3.proxy_username"] = "";
  vfs_param_values["s3.verify_ssl"] = "true";
  vfs_param_values["s3.bucket_canned_acl"] = "NOT_SET";
  vfs_param_values["s3.object_canned_acl"] = "NOT_SET";
  vfs_param_values["hdfs.username"] = "stavros";
  vfs_param_values["hdfs.kerb_ticket_cache_path"] = "";
  vfs_param_values["hdfs.name_node_uri"] = "";

  std::map<std::string, std::string> gcs_param_values;
  gcs_param_values["project_id"] = "";
  gcs_param_values["max_parallel_ops"] =
      std::to_string(std::thread::hardware_concurrency());
  gcs_param_values["multi_part_size"] = "5242880";
  gcs_param_values["use_multi_part_upload"] = "true";
  gcs_param_values["request_timeout_ms"] = "3000";

  std::map<std::string, std::string> azure_param_values;
  azure_param_values["storage_account_name"] = "";
  azure_param_values["storage_account_key"] = "";
  azure_param_values["storage_sas_token"] = "";
  azure_param_values["blob_endpoint"] = "";
  azure_param_values["block_list_block_size"] = "5242880";
  azure_param_values["max_parallel_ops"] =
      std::to_string(std::thread::hardware_concurrency());
  azure_param_values["use_block_list_upload"] = "true";
  azure_param_values["use_https"] = "true";

  std::map<std::string, std::string> s3_param_values;
  s3_param_values["scheme"] = "https";
  s3_param_values["region"] = "us-east-1";
  s3_param_values["aws_access_key_id"] = "";
  s3_param_values["aws_secret_access_key"] = "";
  s3_param_values["aws_session_token"] = "";
  s3_param_values["aws_role_arn"] = "";
  s3_param_values["aws_external_id"] = "";
  s3_param_values["aws_load_frequency"] = "";
  s3_param_values["aws_session_name"] = "";
  s3_param_values["endpoint_override"] = "";
  s3_param_values["use_virtual_addressing"] = "true";
  s3_param_values["skip_init"] = "false";
  s3_param_values["use_multipart_upload"] = "true";
  s3_param_values["max_parallel_ops"] =
      std::to_string(std::thread::hardware_concurrency());
  s3_param_values["multipart_part_size"] = "5242880";
  s3_param_values["ca_file"] = "";
  s3_param_values["ca_path"] = "";
  s3_param_values["connect_timeout_ms"] = "10800";
  s3_param_values["connect_max_tries"] = "5";
  s3_param_values["connect_scale_factor"] = "25";
  s3_param_values["sse"] = "";
  s3_param_values["sse_kms_key_id"] = "";
  s3_param_values["logging_level"] = "Off";
  s3_param_values["request_timeout_ms"] = "3000";
  s3_param_values["requester_pays"] = "false";
  s3_param_values["proxy_host"] = "";
  s3_param_values["proxy_password"] = "";
  s3_param_values["proxy_port"] = "0";
  s3_param_values["proxy_scheme"] = "http";
  s3_param_values["proxy_username"] = "";
  s3_param_values["verify_ssl"] = "true";
  s3_param_values["bucket_canned_acl"] = "NOT_SET";
  s3_param_values["object_canned_acl"] = "NOT_SET";

  // Create an iterator and iterate over all parameters
  tiledb_config_iter_t* config_iter = nullptr;
  rc = tiledb_config_iter_alloc(config, nullptr, &config_iter, &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);
  int done;
  rc = tiledb_config_iter_done(config_iter, &done, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  CHECK(!(bool)done);
  const char *param, *value;
  std::map<std::string, std::string> all_iter_map;
  do {
    rc = tiledb_config_iter_here(config_iter, &param, &value, &error);
    CHECK(rc == TILEDB_OK);
    CHECK(error == nullptr);
    CHECK(param != nullptr);
    CHECK(value != nullptr);
    all_iter_map[std::string(param)] = std::string(value);
    rc = tiledb_config_iter_next(config_iter, &error);
    CHECK(rc == TILEDB_OK);
    CHECK(error == nullptr);
    rc = tiledb_config_iter_done(config_iter, &done, &error);
    CHECK(rc == TILEDB_OK);
    CHECK(error == nullptr);
  } while (!done);
  // highlight any difference to aid the poor developer in event CHECK() fails.
  // If tiledb environment config variables have been set to something different
  // from default configuration (such as
  // "set/export TILEDB_VFS_S3_AWS_ACCESS_KEY_ID=minio"),
  // these can legitimately differ from the defaults expected!
  for (auto i1 = all_param_values.begin(); i1 != all_param_values.end(); ++i1) {
    if (auto i2 = all_iter_map.find(i1->first); i2 == all_iter_map.end()) {
      std::cout << "all_iter_map[\"" << i1->first << "\"] not found!"
                << std::endl;
    } else {
      if (i1->first != i2->first) {
        std::cout << "huh? i1->first != i2->first, \"" << i1->first
                  << "\" vs \"" << i2->first << std::endl;
      } else if (i2->second != i1->second) {
        std::cout << "values for key \"" << i1->first << "\", "
                  << "\"" << i2->second << "\" != "
                  << "\"" << i1->second << "\"" << std::endl;
      } else if (all_param_values[i1->first] != all_iter_map[i1->first]) {
        // if i1->first == i2->first, then should not be possible to be here,
        // but just in case...
        std::cout << " apv[k] != aim[k], k \"" << i1->first << "\", "
                  << "\"" << all_param_values[i1->first] << "\" != \""
                  << all_iter_map[i1->first] << "\"" << std::endl;
      }
    }
  }
  for (auto i1 = all_iter_map.begin(); i1 != all_iter_map.end(); ++i1) {
    if (all_param_values.find(i1->first) == all_param_values.end()) {
      std::cout << "all_param_values[\"" << i1->first << "\"] not found!"
                << std::endl;
    }
    // else, for all like keys, unlike values should have been reported in
    // previous loop.
  }
  CHECK(all_param_values == all_iter_map);
  tiledb_config_iter_free(&config_iter);
  CHECK(error == nullptr);

  // Create an iterator and iterate over vfs parameters
  rc = tiledb_config_iter_alloc(config, "vfs.", &config_iter, &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_iter_done(config_iter, &done, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  CHECK(!(bool)done);
  std::map<std::string, std::string> vfs_iter_map;
  do {
    rc = tiledb_config_iter_here(config_iter, &param, &value, &error);
    CHECK(rc == TILEDB_OK);
    CHECK(error == nullptr);
    CHECK(param != nullptr);
    CHECK(value != nullptr);
    vfs_iter_map[std::string(param)] = std::string(value);
    rc = tiledb_config_iter_next(config_iter, &error);
    CHECK(rc == TILEDB_OK);
    CHECK(error == nullptr);
    rc = tiledb_config_iter_done(config_iter, &done, &error);
    CHECK(rc == TILEDB_OK);
    CHECK(error == nullptr);
  } while (!done);
  // highlight any difference to aid the poor developer in event CHECK() fails.
  for (auto i1 = vfs_param_values.begin(); i1 != vfs_param_values.end(); ++i1) {
    if (auto i2 = vfs_iter_map.find(i1->first); i2 == vfs_iter_map.end()) {
      std::cout << "vfs_iter_map[\"" << i1->first << "\"] not found!"
                << std::endl;
    } else {
      if (i1->first != i2->first) {
        std::cout << "huh? i1->first != i2->first, \"" << i1->first
                  << "\" vs \"" << i2->first << std::endl;
      } else if (i2->second != i1->second) {
        std::cout << "values for key \"" << i1->first << "\", "
                  << "\"" << i2->second << "\" != "
                  << "\"" << i1->second << "\"" << std::endl;
      } else if (vfs_param_values[i1->first] != vfs_iter_map[i1->first]) {
        // if i1->first == i2->first, then should not be possible to be here,
        // but just in case...
        std::cout << " apv[k] != aim[k], k \"" << i1->first << "\", "
                  << "\"" << vfs_param_values[i1->first] << "\" != \""
                  << vfs_iter_map[i1->first] << "\"" << std::endl;
        // std::cout << " apv/aim [\"" << i1->first << "\"], \"" <<
        // all_param_values[i1->first] << " != \"" <<
      }
    }
  }
  for (auto i1 = vfs_iter_map.begin(); i1 != vfs_iter_map.end(); ++i1) {
    if (vfs_param_values.find(i1->first) == vfs_param_values.end()) {
      std::cout << "vfs_param_values[\"" << i1->first << "\"] not found!"
                << std::endl;
    }
  }
  CHECK(vfs_param_values == vfs_iter_map);
  tiledb_config_iter_free(&config_iter);

  // Create an iterator and iterate over gcs parameters
  rc = tiledb_config_iter_alloc(config, "vfs.gcs.", &config_iter, &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_iter_done(config_iter, &done, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  CHECK(!(bool)done);
  std::map<std::string, std::string> gcs_iter_map;
  do {
    rc = tiledb_config_iter_here(config_iter, &param, &value, &error);
    CHECK(rc == TILEDB_OK);
    CHECK(error == nullptr);
    CHECK(param != nullptr);
    CHECK(value != nullptr);
    gcs_iter_map[std::string(param)] = std::string(value);
    rc = tiledb_config_iter_next(config_iter, &error);
    CHECK(rc == TILEDB_OK);
    CHECK(error == nullptr);
    rc = tiledb_config_iter_done(config_iter, &done, &error);
    CHECK(rc == TILEDB_OK);
    CHECK(error == nullptr);
  } while (!done);
  CHECK(gcs_param_values == gcs_iter_map);
  tiledb_config_iter_free(&config_iter);
  CHECK(error == nullptr);

  // Create an iterator and iterate over azure parameters
  rc = tiledb_config_iter_alloc(config, "vfs.azure.", &config_iter, &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_iter_done(config_iter, &done, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  CHECK(!(bool)done);
  std::map<std::string, std::string> azure_iter_map;
  do {
    rc = tiledb_config_iter_here(config_iter, &param, &value, &error);
    CHECK(rc == TILEDB_OK);
    CHECK(error == nullptr);
    CHECK(param != nullptr);
    CHECK(value != nullptr);
    azure_iter_map[std::string(param)] = std::string(value);
    rc = tiledb_config_iter_next(config_iter, &error);
    CHECK(rc == TILEDB_OK);
    CHECK(error == nullptr);
    rc = tiledb_config_iter_done(config_iter, &done, &error);
    CHECK(rc == TILEDB_OK);
    CHECK(error == nullptr);
  } while (!done);
  CHECK(azure_param_values == azure_iter_map);
  tiledb_config_iter_free(&config_iter);
  CHECK(error == nullptr);

  // Create an iterator and iterate over s3 parameters
  rc = tiledb_config_iter_alloc(config, "vfs.s3.", &config_iter, &error);
  REQUIRE(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_iter_done(config_iter, &done, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  CHECK(!(bool)done);
  std::map<std::string, std::string> s3_iter_map;
  do {
    rc = tiledb_config_iter_here(config_iter, &param, &value, &error);
    CHECK(rc == TILEDB_OK);
    CHECK(error == nullptr);
    CHECK(param != nullptr);
    CHECK(value != nullptr);
    s3_iter_map[std::string(param)] = std::string(value);
    rc = tiledb_config_iter_next(config_iter, &error);
    CHECK(rc == TILEDB_OK);
    CHECK(error == nullptr);
    rc = tiledb_config_iter_done(config_iter, &done, &error);
    CHECK(rc == TILEDB_OK);
    CHECK(error == nullptr);
  } while (!done);
  // highlight any difference to aid the poor developer in event CHECK() fails.
  for (auto i1 = s3_param_values.begin(); i1 != s3_param_values.end(); ++i1) {
    if (auto i2 = s3_iter_map.find(i1->first); i2 == s3_iter_map.end()) {
      std::cout << "s3_iter_map[\"" << i1->first << "\"] not found!"
                << std::endl;
    } else {
      if (i1->first != i2->first) {
        std::cout << "huh? i1->first != i2->first, \"" << i1->first
                  << "\" vs \"" << i2->first << std::endl;
      } else if (i2->second != i1->second) {
        std::cout << "values for key \"" << i1->first << "\", "
                  << "\"" << i2->second << "\" != "
                  << "\"" << i1->second << "\"" << std::endl;
      } else if (s3_param_values[i1->first] != s3_iter_map[i1->first]) {
        // if i1->first == i2->first, then should not be possible to be here,
        // but just in case...
        std::cout << " apv[k] != aim[k], k \"" << i1->first << "\", "
                  << "\"" << s3_param_values[i1->first] << "\" != \""
                  << s3_iter_map[i1->first] << "\"" << std::endl;
        // std::cout << " apv/aim [\"" << i1->first << "\"], \"" <<
        // all_param_values[i1->first] << " != \"" <<
      }
    }
  }
  for (auto i1 = s3_iter_map.begin(); i1 != s3_iter_map.end(); ++i1) {
    if (s3_param_values.find(i1->first) == s3_param_values.end()) {
      std::cout << "s3_param_values[\"" << i1->first << "\"] not found!"
                << std::endl;
    }
  }
  CHECK(s3_param_values == s3_iter_map);
  tiledb_config_iter_free(&config_iter);
  CHECK(error == nullptr);

  // Clean up
  tiledb_config_free(&config);
  tiledb_ctx_free(&ctx);
}

TEST_CASE("C API: Test config from file", "[capi][config]") {
  check_load_correct_file();
  check_load_incorrect_file_cannot_open();
  check_load_incorrect_file_missing_value();
  check_load_incorrect_file_extra_word();
  check_save_to_file();
}

TEST_CASE(
    "C API: Test boolean config values are normalized", "[capi][config]") {
  tiledb_error_t* err;
  tiledb_config_t* config = nullptr;
  int rc = tiledb_config_alloc(&config, &err);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_config_set(config, "vfs.s3.use_virtual_addressing", "TRUE", &err);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_set(config, "vfs.s3.use_virtual_addressing", "True", &err);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_set(config, "vfs.s3.skip_init", "FALSE", &err);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_set(config, "vfs.s3.skip_init", "False", &err);
  CHECK(rc == TILEDB_OK);
  rc =
      tiledb_config_set(config, "vfs.s3.use_virtual_addressing", "FALSE", &err);
  CHECK(rc == TILEDB_OK);
  rc =
      tiledb_config_set(config, "vfs.s3.use_virtual_addressing", "False", &err);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_set(config, "vfs.s3.skit_init", "TRUE", &err);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_set(config, "vfs.s3.skip_init", "True", &err);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_config_set(config, "vfs.s3.use_multipart_upload", "TRUE", &err);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_set(config, "vfs.s3.use_multipart_upload", "True", &err);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_set(config, "vfs.s3.use_multipart_upload", "FALSE", &err);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_set(config, "vfs.s3.use_multipart_upload", "False", &err);
  CHECK(rc == TILEDB_OK);

  tiledb_config_free(&config);
}

TEST_CASE("C API: Test VFS config inheritance", "[capi][config][vfs-inherit]") {
  tiledb_error_t* err;
  tiledb_config_t* config = nullptr;
  int rc = tiledb_config_alloc(&config, &err);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_config_set(config, "sm.tile_cache_size", "100", &err);
  CHECK(rc == TILEDB_OK);
  tiledb_config_t* vfs_config = nullptr;
  rc = tiledb_config_alloc(&vfs_config, &err);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_config_set(vfs_config, "vfs.s3.ca_file", "path", &err);
  CHECK(rc == TILEDB_OK);

  tiledb_ctx_t* ctx = nullptr;
  rc = tiledb_ctx_alloc(config, &ctx);
  CHECK(rc == TILEDB_OK);

  tiledb_vfs_t* vfs = nullptr;
  tiledb_config_t* vfs_config_get = nullptr;
  rc = tiledb_vfs_alloc(ctx, vfs_config, &vfs);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_vfs_get_config(ctx, vfs, &vfs_config_get);
  CHECK(rc == TILEDB_OK);

  const char* value = nullptr;
  rc = tiledb_config_get(vfs_config_get, "sm.tile_cache_size", &value, &err);
  CHECK(rc == TILEDB_OK);
  CHECK(!strcmp(value, "100"));
  rc = tiledb_config_get(vfs_config_get, "vfs.s3.ca_file", &value, &err);
  CHECK(rc == TILEDB_OK);
  CHECK(!strcmp(value, "path"));

  tiledb_config_free(&config);
  tiledb_config_free(&vfs_config);
  tiledb_config_free(&vfs_config_get);
  tiledb_vfs_free(&vfs);
  tiledb_ctx_free(&ctx);
}
