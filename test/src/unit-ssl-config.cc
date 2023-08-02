/**
 * @file unit-ssl-config.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * Tests for S3 SSL Configuration
 */

#include <test/support/tdb_catch.h>
#include "tiledb/platform/platform.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/filesystem.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/storage_manager/context.h"

using namespace tiledb;
using namespace tiledb::sm;

const static std::string bucket_name = "ssl-config-test";

// We're telling libcurl to use a non-standard root certificate when
// evaluating whether the SSL connection is secure. On macOS where libcurl
// links against SecureTransport by default, a non-standard root certificate
// requires storing it in the system keychain which we absolutely do not
// want ever. I assume there's similar issues on Windows but I have not
// tested it myself. Regardless, the only real use case for this feature
// is in Docker containers that don't include a ca-certificates package
// so restricting to Linux should be fine.
#define REQUIRES_LINUX()          \
  do {                            \
    if (!platform::is_os_linux) { \
      return;                     \
    }                             \
  } while (0)

std::string get_test_ca_path();
std::string get_test_ca_file();
void check_failure(Filesystem fs, Config& cfg);
void check_success(Filesystem fs, Config& cfg);

Config azure_base_config() {
  Config cfg;

  std::string azure_user = "devstoreaccount1";
  std::string azure_key =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDX"
      "J1OUzFT50uSRZ6IFsuFq2UVErCz4I6"
      "tq/K1SZFPTOtr/KBHBeksoGMGw==";
  std::string azure_endpoint = "https://localhost:10001/devstoreaccount1";

  REQUIRE(cfg.set("vfs.azure.storage_account_name", azure_user).ok());
  REQUIRE(cfg.set("vfs.azure.storage_account_key", azure_key).ok());
  REQUIRE(cfg.set("vfs.azure.blob_endpoint", azure_endpoint).ok());

  return cfg;
}

TEST_CASE("Azure - Connection Error", "[ssl_config][azure]") {
  // Show that SSL connections without configuration are broken
  // so that the other tests show that setting the config values
  // actually works rather than me not realizing I accidentally
  // set an http endpoint instead of https.
  auto cfg = azure_base_config();
  check_failure(Filesystem::AZURE, cfg);
}

TEST_CASE("Azure - Verify False - ssl.verify", "[ssl_config][azure]") {
  // For some reason, Windows fails to disable SSL validation. Given
  // that this is only a test to ensure that we've got SSL turned on in
  // for testing we just disable it since we really don't want uses
  // running with verify=false in the general case.
  REQUIRES_LINUX();

  auto cfg = azure_base_config();
  REQUIRE(cfg.set("ssl.verify", "false").ok());
  check_success(Filesystem::AZURE, cfg);
}

TEST_CASE("Azure - CAINFO - ssl.ca_file", "[ssl_config][azure]") {
  REQUIRES_LINUX();

  auto cfg = azure_base_config();
  REQUIRE(cfg.set("ssl.verify", "true").ok());
  REQUIRE(cfg.set("ssl.ca_file", get_test_ca_file()).ok());
  check_success(Filesystem::AZURE, cfg);
}

TEST_CASE("Azure - CAPATH - ssl.ca_path", "[ssl_config][azure]") {
  REQUIRES_LINUX();

  auto cfg = azure_base_config();
  REQUIRE(cfg.set("ssl.verify_ssl", "true").ok());
  REQUIRE(cfg.set("ssl.ca_path", get_test_ca_path()).ok());

  // The Azure client does not support setting the CAPATH in libcurl so
  // this is an expected failure.
  check_failure(Filesystem::AZURE, cfg);
}

Config gcs_base_config() {
  Config cfg;

  REQUIRE(cfg.set("vfs.gcs.endpoint", "https://localhost:9001").ok());

  return cfg;
}

TEST_CASE("GCS - Connection Error", "[ssl_config][gcs]") {
  // Show that SSL connections without configuration are broken
  // so that the other tests show that setting the config values
  // actually works rather than me not realizing I accidentally
  // set an http endpoint instead of https.
  auto cfg = gcs_base_config();
  check_failure(Filesystem::GCS, cfg);
}

TEST_CASE("GCS - Verify False - ssl.verify", "[ssl_config][gcs]") {
  // GCS does not allow disabling SSL verification through
  // its API so we require this to be a failure as well.
  auto cfg = gcs_base_config();
  REQUIRE(cfg.set("ssl.verify", "false").ok());
  check_failure(Filesystem::GCS, cfg);
}

TEST_CASE("GCS - CAINFO - ssl.ca_file", "[ssl_config][gcs]") {
  REQUIRES_LINUX();

  auto cfg = gcs_base_config();
  REQUIRE(cfg.set("ssl.verify", "true").ok());
  REQUIRE(cfg.set("ssl.ca_file", get_test_ca_file()).ok());
  check_success(Filesystem::GCS, cfg);
}

TEST_CASE("GCS - CAPATH - ssl.ca_path", "[ssl_config][gcs]") {
  auto cfg = gcs_base_config();
  REQUIRE(cfg.set("ssl.verify_ssl", "true").ok());
  REQUIRE(cfg.set("ssl.ca_path", get_test_ca_path()).ok());

  // The GCS client does not support setting the CAPATH in libcurl so
  // this is an expected failure.
  check_failure(Filesystem::GCS, cfg);
}

Config s3_base_config() {
  Config cfg;

  REQUIRE(cfg.set("vfs.s3.endpoint_override", "localhost:9999").ok());
  REQUIRE(cfg.set("vfs.s3.scheme", "https").ok());
  REQUIRE(cfg.set("vfs.s3.use_virtual_addressing", "false").ok());
  REQUIRE(cfg.set("vfs.s3.verify_ssl", "true").ok());

  return cfg;
}

TEST_CASE("S3 - Connection Error", "[ssl_config][s3][yarps]") {
  // Show that SSL connections without configuration are broken
  // so that the other tests show that setting the config values
  // actually works rather than me not realizing I accidentally
  // set an http endpoint instead of https.
  auto cfg = s3_base_config();
  REQUIRE(cfg.set("vfs.s3.logging_level", "trace").ok());
  std::cerr << "S3 - Connection Error: " << std::this_thread::get_id()
            << std::endl;
  check_failure(Filesystem::S3, cfg);
}

TEST_CASE("S3 - Verify False - vfs.s3.verify_ssl", "[ssl_config][s3]") {
  std::cerr << "S3 - Verify False - vfs.s3.verify_ssl: "
            << std::this_thread::get_id() << std::endl;
  auto cfg = s3_base_config();
  REQUIRE(cfg.set("vfs.s3.verify_ssl", "false").ok());
  check_success(Filesystem::S3, cfg);
}

TEST_CASE("S3 - Verify False - ssl.verify", "[ssl_config][s3]") {
  auto cfg = s3_base_config();
  REQUIRE(cfg.set("ssl.verify", "false").ok());
  check_success(Filesystem::S3, cfg);
}

TEST_CASE("S3 - CAINFO - vfs.s3.ca_file", "[ssl_config][s3]") {
  REQUIRES_LINUX();

  auto cfg = s3_base_config();
  REQUIRE(cfg.set("vfs.s3.verify_ssl", "true").ok());
  REQUIRE(cfg.set("vfs.s3.ca_file", get_test_ca_file()).ok());
  check_success(Filesystem::S3, cfg);
}

TEST_CASE("S3 - CAINFO - ssl.ca_file", "[ssl_config][s3]") {
  REQUIRES_LINUX();

  auto cfg = s3_base_config();
  REQUIRE(cfg.set("ssl.verify", "true").ok());
  REQUIRE(cfg.set("ssl.ca_file", get_test_ca_file()).ok());
  check_success(Filesystem::S3, cfg);
}

TEST_CASE("S3 - CAPATH - vfs.s3.ca_path", "[ssl_config][s3]") {
  REQUIRES_LINUX();

  auto cfg = s3_base_config();
  REQUIRE(cfg.set("vfs.s3.verify_ssl", "true").ok());
  REQUIRE(cfg.set("vfs.s3.ca_path", get_test_ca_path()).ok());
  check_success(Filesystem::S3, cfg);
}

TEST_CASE("S3 - CAPATH - ssl.ca_path", "[ssl_config][s3]") {
  REQUIRES_LINUX();

  auto cfg = s3_base_config();
  REQUIRE(cfg.set("ssl.verify", "true").ok());
  REQUIRE(cfg.set("ssl.ca_path", get_test_ca_path()).ok());
  check_success(Filesystem::S3, cfg);
}

std::string get_test_ca_path() {
  return std::string(TILEDB_TEST_INPUTS_DIR) + "/test_certs/";
}

std::string get_test_ca_file() {
  return get_test_ca_path() + "public.crt";
}

void check_failure(Filesystem fs, Config& cfg) {
  std::cerr << "CHECK FAILURE: " << std::this_thread::get_id() << std::endl;
  Context ctx(cfg);
  auto& vfs = ctx.resources().vfs();

  if (!vfs.supports_fs(fs)) {
    return;
  }

  std::string scheme;

  if (fs == Filesystem::AZURE) {
    scheme = "azure";
  } else if (fs == Filesystem::GCS) {
    scheme = "gcs";
  } else if (fs == Filesystem::S3) {
    scheme = "s3";
  } else {
    throw std::invalid_argument("Invalid fs value: " + filesystem_str(fs));
  }

  URI bucket_uri = URI(scheme + "://" + bucket_name);

  Status st;
  bool is_bucket;

  try {
    st = vfs.is_bucket(bucket_uri, &is_bucket);
  } catch (...) {
    // Some backends throw exceptions to signal SSL error conditions
    // so we pass the test by returning early here.
    return;
  }

  // Otherwise, make sure we get a failure status
  REQUIRE(!st.ok());
}

void check_success(Filesystem fs, Config& cfg) {
  std::cerr << "CHECK SUCCESS: " << std::this_thread::get_id() << std::endl;
  Context ctx(cfg);
  auto& vfs = ctx.resources().vfs();

  if (!vfs.supports_fs(fs)) {
    return;
  }

  std::string scheme;

  if (fs == Filesystem::AZURE) {
    scheme = "azure";
  } else if (fs == Filesystem::GCS) {
    scheme = "gcs";
  } else if (fs == Filesystem::S3) {
    scheme = "s3";
  } else {
    throw std::invalid_argument("Invalid fs value: " + filesystem_str(fs));
  }

  URI bucket_uri = URI(scheme + "://" + bucket_name);

  bool is_bucket;
  throw_if_not_ok(vfs.is_bucket(bucket_uri, &is_bucket));
  if (is_bucket) {
    throw_if_not_ok(vfs.remove_bucket(bucket_uri));
  }
  throw_if_not_ok(vfs.create_bucket(bucket_uri));

  throw_if_not_ok(vfs.is_bucket(bucket_uri, &is_bucket));
  REQUIRE(is_bucket);
}
