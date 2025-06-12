/**
 * @file   vfs_helpers.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2023 TileDB, Inc.
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
 * This file declares some vfs-specfic test suite helper functions.
 */

#ifndef TILEDB_VFS_HELPERS_H
#define TILEDB_VFS_HELPERS_H

#include <test/support/tdb_catch_prng.h>
#include <filesystem>
#include "test/support/src/helpers.h"
#include "tiledb/sm/enums/vfs_mode.h"
#include "tiledb/sm/filesystem/vfs.h"

namespace tiledb::test {

// Forward declaration
class SupportedFs;

#ifdef TILEDB_TESTS_AWS_CONFIG
constexpr bool aws_s3_config = true;
#else
constexpr bool aws_s3_config = false;
#endif

/**
 * Generates a random temp directory URI for use in VFS tests.
 *
 * @param prefix A prefix to use for the temp directory name. Should include
 *    `s3://`, `mem://` or other URI prefix for the backend.
 * @return URI which the caller can use to create a temp directory.
 */
tiledb::sm::URI test_dir(const std::string& prefix);

/**
 * Create the vector of supported filesystems.
 */
std::vector<std::unique_ptr<SupportedFs>> vfs_test_get_fs_vec();

/**
 * Initialize the vfs test.
 *
 * @param fs_vec The vector of supported filesystems
 * @param ctx The TileDB context.
 * @param vfs The VFS object.
 * @param config An optional configuration argument.
 */
Status vfs_test_init(
    const std::vector<std::unique_ptr<SupportedFs>>& fs_vec,
    tiledb_ctx_t** ctx,
    tiledb_vfs_t** vfs,
    tiledb_config_t* config = nullptr);

/**
 * Close the vfs test.
 *
 * @param fs_vec The vector of supported filesystems
 * @param ctx The TileDB context.
 * @param vfs The VFS object.
 */
Status vfs_test_close(
    const std::vector<std::unique_ptr<SupportedFs>>& fs_vec,
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs);

void vfs_test_remove_temp_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const std::string& path);

void vfs_test_create_temp_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const std::string& path);

std::string vfs_array_uri(
    const std::unique_ptr<SupportedFs>& fs,
    const std::string& array_name,
    tiledb_ctx_t* ctx);

/**
 * This class defines and manipulates
 * a list of supported filesystems.
 */
class SupportedFs {
 public:
  virtual ~SupportedFs() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns Status upon setting up the associated
   * filesystem's configuration
   * Only for S3, Azure
   * No-op otherwise
   *
   * @param config Configuration parameters
   * @param error Error parameter
   * @return Status OK if successful
   */
  virtual Status prepare_config(
      tiledb_config_t* config, tiledb_error_t* error) = 0;

  /**
   * Creates bucket / container if does not exist
   * Only for S3, Azure
   * No-op otherwise
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) = 0;

  /**
   * Removes bucket / container if exists
   * Only for S3, Azure
   * No-op otherwise
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) = 0;

  /**
   * Get the name of the filesystem's directory
   *
   * @return string directory name
   */
  virtual std::string temp_dir() = 0;

  /**
   * Checks if the filesystem is accessed via REST
   *
   * @return true if REST, false if not
   */
  virtual bool is_rest() = 0;

  /**
   * Checks if the filesystem is local or remote
   *
   * @return true if local, false if not
   */
  virtual bool is_local() = 0;
};

/**
 * This class provides support for
 * the S3 filesystem.
 */
class SupportedFsS3 : public SupportedFs {
 public:
  SupportedFsS3(bool rest = false)
      : s3_prefix_("s3://")
      , s3_bucket_(s3_prefix_ + "tiledb-" + random_label() + "/")
      , temp_dir_(s3_bucket_ + "tiledb_test/")
      , rest_(rest) {
  }

  ~SupportedFsS3() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns Status upon setting up the associated
   * filesystem's configuration
   *
   * @param config Configuration parameters
   * @param error Error parameter
   * @return Status OK if successful
   */
  virtual Status prepare_config(tiledb_config_t* config, tiledb_error_t* error);

  /**
   * Creates bucket if does not exist
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Removes bucket if exists
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Get the name of the filesystem's directory
   *
   * @return string directory name
   */
  virtual std::string temp_dir();

  /**
   * Checks if the filesystem is accessed via REST
   *
   * @return true if REST, false if not
   */
  virtual bool is_rest();

  /**
   * Checks if the filesystem is local or remote
   *
   * @return true if local, false if not
   */
  inline bool is_local() {
    return false;
  }

 private:
  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

  /** The directory prefix of the S3 filesystem. */
  const std::string s3_prefix_;

  /** The bucket name for the S3 filesystem. */
  const std::string s3_bucket_;

  /** The directory name of the S3 filesystem. */
  std::string temp_dir_;

  /** If the filesystem is accessed via REST. */
  bool rest_;
};

/**
 * This class provides support for
 * the Azure filesystem.
 */
class SupportedFsAzure : public SupportedFs {
 public:
  SupportedFsAzure()
      : azure_prefix_("azure://")
      , container_(azure_prefix_ + "tiledb-" + random_label() + "/")
      , temp_dir_(container_ + "tiledb_test/") {
  }

  ~SupportedFsAzure() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns Status upon setting up the associated
   * filesystem's configuration
   *
   * @param config Configuration parameters
   * @param error Error parameter
   * @return Status OK if successful
   */
  virtual Status prepare_config(tiledb_config_t* config, tiledb_error_t* error);

  /**
   * Creates container if does not exist
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Removes container if exists
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Get the name of the filesystem's directory
   *
   * @return string directory name
   */
  virtual std::string temp_dir();

  /**
   * Checks if the filesystem is accessed via REST
   *
   * @return true if REST, false if not
   */
  inline bool is_rest() {
    return false;
  }

  /**
   * Checks if the filesystem is local or remote
   *
   * @return true if local, false if not
   */
  inline bool is_local() {
    return false;
  }

 private:
  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

  /** The directory prefix of the Azure filesystem. */
  const std::string azure_prefix_;

  /** The container name for the Azure filesystem. */
  const std::string container_;

  /** The directory name of the Azure filesystem. */
  std::string temp_dir_;
};

/**
 * This class provides support for the GCS filesystem.
 */
class SupportedFsGCS : public SupportedFs {
 public:
  SupportedFsGCS(std::string prefix = "gcs://")
      : prefix_(prefix)
      , bucket_(prefix_ + "tiledb-" + random_label() + "/")
      , temp_dir_(bucket_ + "tiledb_test/") {
  }

  ~SupportedFsGCS() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns Status upon setting up the associated
   * filesystem's configuration
   *
   * @param config Configuration parameters
   * @param error Error parameter
   * @return Status OK if successful
   */
  virtual Status prepare_config(tiledb_config_t* config, tiledb_error_t* error);

  /**
   * Creates bucket if does not exist
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Removes bucket if exists
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Get the name of the filesystem's directory
   *
   * @return string directory name
   */
  virtual std::string temp_dir();

  /**
   * Checks if the filesystem is accessed via REST
   *
   * @return true if REST, false if not
   */
  inline bool is_rest() {
    return false;
  }

  /**
   * Checks if the filesystem is local or remote
   *
   * @return true if local, false if not
   */
  inline bool is_local() {
    return false;
  }

 private:
  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

  /** The directory prefix of the GCS filesystem. */
  const std::string prefix_;

  /** The bucket name for the GCS filesystem. */
  const std::string bucket_;

  /** The directory name of the GCS filesystem. */
  std::string temp_dir_;
};

/**
 * This class provides support for
 * the Windows or Posix (local) filesystem.
 */
class SupportedFsLocal : public SupportedFs {
 public:
#ifdef _WIN32
  SupportedFsLocal()
      : temp_dir_(tiledb::sm::Win::current_dir() + "\\tiledb_test\\")
      , file_prefix_("") {
  }
#else
  SupportedFsLocal()
      : temp_dir_(tiledb::sm::Posix::current_dir() + "/tiledb_test/")
      , file_prefix_("file://") {
  }
#endif

  ~SupportedFsLocal() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * No-op
   *
   * @param config Configuration parameters
   * @param error Error parameter
   * @return Status OK if successful
   */
  virtual Status prepare_config(tiledb_config_t* config, tiledb_error_t* error);

  /**
   * No-op
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * No-op
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Get the name of the filesystem's directory
   *
   * @return string directory name
   */
  virtual std::string temp_dir();

  /**
   * Get the name of the filesystem's file prefix
   *
   * @return string prefix name
   */
  std::string file_prefix();

  /**
   * Checks if the filesystem is accessed via REST
   *
   * @return true if REST, false if not
   */
  inline bool is_rest() {
    return false;
  }

  /**
   * Checks if the filesystem is local or remote
   *
   * @return true if local, false if not
   */
  inline bool is_local() {
    return true;
  }

 private:
  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

#ifdef _WIN32
  /** The directory name of the Windows filesystem. */
  std::string temp_dir_;

  /** The file prefix name of the Windows filesystem. */
  std::string file_prefix_;

#else
  /** The directory name of the Posix filesystem. */
  std::string temp_dir_;

  /** The file prefix name of the Posix filesystem. */
  std::string file_prefix_;
#endif
};

/**
 * This class provides support for
 * the Mem filesystem.
 */
class SupportedFsMem : public SupportedFs {
 public:
  SupportedFsMem()
      : temp_dir_("mem://tiledb_test/") {
  }

  ~SupportedFsMem() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns Status upon setting up the associated
   * filesystem's configuration
   *
   * @param config Configuration parameters
   * @param error Error parameter
   * @return Status OK if successful
   */
  virtual Status prepare_config(tiledb_config_t* config, tiledb_error_t* error);

  /**
   * Creates container if does not exist
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Removes container if exists
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Get the name of the filesystem's directory
   *
   * @return string directory name
   */
  virtual std::string temp_dir();

  /**
   * Checks if the filesystem is accessed via REST
   *
   * @return true if REST, false if not
   */
  inline bool is_rest() {
    return false;
  }

  /**
   * Checks if the filesystem is local or remote
   *
   * @return true if local, false if not
   */
  inline bool is_local() {
    return true;
  }

 private:
  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

  /** The directory name of the Mem filesystem. */
  std::string temp_dir_;
};

/**
 * Struct which allocates a config and conditionally sets filesystem-specific
 * parameters on it.
 */
struct vfs_config {
  /** Config handle. */
  tiledb_config_handle_t* config{nullptr};

  /** Constructor. */
  vfs_config() {
    tiledb_error_t* error = nullptr;
    auto rc = tiledb_config_alloc(&config, &error);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("error creating config handle");
    }
    if (error != nullptr) {
      throw std::logic_error(
          "tiledb_config_alloc returned OK but with non-null error");
    }

    if constexpr (tiledb::sm::filesystem::s3_enabled) {
      SupportedFsS3 fs_s3;
      auto st = fs_s3.prepare_config(config, error);
      if (!st.ok()) {
        throw std::runtime_error("error preparing S3 config");
      }
    }

    if constexpr (tiledb::sm::filesystem::azure_enabled) {
      SupportedFsAzure fs_azure;
      auto st = fs_azure.prepare_config(config, error);
      if (!st.ok()) {
        throw std::runtime_error("error preparing Azure config");
      }
    }
  }

  /** Copy constructor is deleted. */
  vfs_config(const vfs_config&) = delete;

  /** Move constructor is deleted. */
  vfs_config(vfs_config&&) = delete;

  /** Copy assignment is deleted. */
  vfs_config& operator=(const vfs_config&) = delete;

  /** Move assignment is deleted. */
  vfs_config& operator=(vfs_config&&) = delete;

  /** Destructor. */
  ~vfs_config() {
    tiledb_config_free(&config);
  }
};

/**
 * Fixture for creating a temporary directory for a test case. This fixture
 * also manages the context and virtual file system for the test case.
 */
struct TemporaryDirectoryFixture {
 public:
  /** Fixture constructor. */
  TemporaryDirectoryFixture()
      : supported_filesystems_(vfs_test_get_fs_vec()) {
    // Initialize virtual filesystem and context.
    REQUIRE(vfs_test_init(supported_filesystems_, &ctx, &vfs_).ok());

    // Create temporary directory based on the supported filesystem
#ifdef _WIN32
    SupportedFsLocal windows_fs;
    temp_dir_ = windows_fs.file_prefix() + windows_fs.temp_dir();
#else
    SupportedFsLocal posix_fs;
    temp_dir_ = posix_fs.file_prefix() + posix_fs.temp_dir();
#endif
    create_dir(temp_dir_, ctx, vfs_);
  }

  /** Fixture destructor. */
  ~TemporaryDirectoryFixture() {
    remove_dir(temp_dir_, ctx, vfs_);
    tiledb_ctx_free(&ctx);
    tiledb_vfs_free(&vfs_);
  }

  /**
   * Allocate an TileDB context to use the same configuration as the context for
   * the temporary directory except for encryption settings.
   *
   * @param encryption_type Value to set on the configuration for
   * `sm.encryption_type`.
   * @param encryption_key Value to set on the configuration for
   * `sm.encryption_key`.
   * @param ctx_with_encrypt Context that will be allocated.
   */
  void alloc_encrypted_ctx(
      const std::string& encryption_type,
      const std::string& encryption_key,
      tiledb_ctx_t** ctx_with_encrypt) const;

  /**
   * Creates a new array array in the temporary directory and returns the
   * fullpath the array.
   *
   * @param name Name of the array relative to the temporary directory.
   * @param array_schema Schema for the array to be created.
   * @param array_schema Schema for the array to be created.
   * @param serialize To serialize or not the creation of the schema
   * @returns URI of the array.
   */
  std::string create_temporary_array(
      std::string&& name,
      tiledb_array_schema_t* array_schema,
      const bool serialize = false);

  /**
   * Check the return code for a TileDB C-API function is TILEDB_ERR and
   * compare the last error message from the local TileDB context to an expected
   * error message.
   *
   * @param rc Return code from a TileDB C-API function.
   * @param expected_msg The expected message from the last error.
   */
  inline void check_tiledb_error_with(
      int rc, const std::string& expected_msg) const {
    test::check_tiledb_error_with(ctx, rc, expected_msg);
  }

  /**
   * Checks the return code for a TileDB C-API function is TILEDB_OK. If not,
   * if will add a failed assert to the Catch2 test and print the last error
   * message from the local TileDB context.
   *
   * @param rc Return code from a TileDB C-API function.
   */
  inline void check_tiledb_ok(int rc) const {
    test::check_tiledb_ok(ctx, rc);
  }

  /** Create a path in the temporary directory. */
  inline std::string fullpath(std::string&& name) {
    return temp_dir_ + name;
  }

  /**
   * Returns the context pointer object.
   */
  inline tiledb_ctx_t* get_ctx() {
    return ctx;
  }

  /**
   * Require the return code for a TileDB C-API function is TILEDB_ERR and
   * compare the last error message from the local TileDB context to an expected
   * error message.
   *
   * @param rc Return code from a TileDB C-API function.
   * @param expected_msg The expected message from the last error.
   */
  inline void require_tiledb_error_with(
      int rc, const std::string& expected_msg) const {
    test::require_tiledb_error_with(ctx, rc, expected_msg);
  }

  /**
   * Requires the return code for a TileDB C-API function is TILEDB_OK. If not,
   * it will end the Catch2 test and print the last error message from the local
   * TileDB context.
   *
   * @param rc Return code from a TileDB C-API function.
   */
  inline void require_tiledb_ok(int rc) const {
    test::require_tiledb_ok(ctx, rc);
  }

 protected:
  /** TileDB context */
  tiledb_ctx_t* ctx;

  /** Name of the temporary directory to use for this test */
  std::string temp_dir_;

  /** Virtual file system */
  tiledb_vfs_t* vfs_;

 private:
  /** Vector of supported filesystems. Used to initialize ``vfs_``. */
  const std::vector<std::unique_ptr<SupportedFs>> supported_filesystems_;
};

/*
 * Class to instantiate when setting up a test case to run for all VFSs and
 * REST
 *
 * Example usage: In the beginning of the test define a VFSTestSetup variable
 * and then just use the Context and the rest of the resources it encapsulates
 * in the test. The resources will be automatically get freed when the variable
 * will get out of scope.
 *
 * {
 * tiledb::test::VFSTestSetup vfs_test_setup{"foo_array"};
 * auto ctx = vfs_test_setup.ctx();
 * auto array_uri = vfs_test_setup.array_uri("quickstart_sparse");
 * Array array(ctx, array_uri, TILEDB_WRITE);
 * ...
 * } // ctx context is destroyed and VFS directories removed
 *
 */
struct VFSTestSetup {
  VFSTestSetup(tiledb_config_t* config = nullptr, bool remove_tmpdir = true)
      : fs_vec(vfs_test_get_fs_vec())
      , ctx_c{nullptr}
      , vfs_c{nullptr}
      , cfg_c{config} {
    vfs_test_init(fs_vec, &ctx_c, &vfs_c, cfg_c).ok();
    temp_dir = fs_vec[0]->temp_dir();
    if (remove_tmpdir) {
      vfs_test_remove_temp_dir(ctx_c, vfs_c, temp_dir);
    }
    tiledb_vfs_create_dir(ctx_c, vfs_c, temp_dir.c_str());
  };

  void update_config(tiledb_config_t* config) {
    // free resources
    tiledb_ctx_free(&ctx_c);
    tiledb_vfs_free(&vfs_c);
    cfg_c = config;

    // reallocate with input config
    vfs_test_init(fs_vec, &ctx_c, &vfs_c, cfg_c).ok();
  }

  bool is_rest() {
    return fs_vec[0]->is_rest();
  }

  bool is_legacy_rest();

  bool is_local() {
    return fs_vec[0]->is_local();
  }

  std::string array_uri(
      const std::string& array_name, bool strip_tiledb_prefix = false) {
    // The order allows for stripping prefix from a REST URI.
    if (strip_tiledb_prefix || !is_rest()) {
      return temp_dir + array_name;
    }

    // Non-REST case is handled above.
    if (is_legacy_rest()) {
      return "tiledb://unit/" + temp_dir + array_name;
    } else {
      // Include a space in the URI to test URL encoding.
      return "tiledb://unit workspace/unit teamspace/" + temp_dir + array_name;
    }
  }

  Context ctx() {
    return Context(ctx_c, false);
  }

  ~VFSTestSetup() {
    vfs_test_remove_temp_dir(ctx_c, vfs_c, temp_dir);
    vfs_test_close(fs_vec, ctx_c, vfs_c).ok();

    tiledb_ctx_free(&ctx_c);
    tiledb_vfs_free(&vfs_c);
  };

  std::vector<std::unique_ptr<SupportedFs>> fs_vec;
  tiledb_ctx_handle_t* ctx_c;
  tiledb_vfs_handle_t* vfs_c;
  tiledb_config_handle_t* cfg_c;
  std::string temp_dir;
};

/**
 * Denies write access to a local filesystem path.
 *
 * Not supported on Windows. The permissions function there sets the
 * readonly bit on the path, which is not supported on directories.
 *
 * To support it on Windows we would have to add and remove Access Control
 * Lists, which is a nontrivial thing to do.
 */
class DenyWriteAccess {
 public:
  DenyWriteAccess() = delete;

  DenyWriteAccess(const std::string& path)
      : path_(path)
      , previous_perms_(std::filesystem::status(path).permissions()) {
    std::filesystem::permissions(
        path_,
        std::filesystem::perms::owner_write,
        std::filesystem::perm_options::remove);
  }

  ~DenyWriteAccess() {
    std::filesystem::permissions(
        path_, previous_perms_, std::filesystem::perm_options::replace);
  }

 private:
  /** The path. */
  const std::string path_;

  /** The previous permissions of the path. */
  const std::filesystem::perms previous_perms_;
};

/**
 * Base class use for VFS and file system test objects. Deriving classes are
 * responsible for creating a temporary directory and populating it with test
 * objects for the related file system.
 */
class VFSTestBase {
 protected:
  /**
   * Requires derived class to create a temporary directory.
   *
   * @param test_tree Vector used to build test directory and objects.
   *    For each element we create a nested directory with N objects.
   * @param prefix The URI prefix to use for the test directory.
   */
  VFSTestBase(const std::vector<size_t>& test_tree, const std::string& prefix);

 public:
  /** Type definition for objects returned from ls_recursive */
  using LsObjects = std::vector<std::pair<std::string, uint64_t>>;

  virtual ~VFSTestBase();

  /**
   * @return True if the URI prefix is supported by the build, else false.
   */
  inline bool is_supported() const {
    return is_supported_;
  }

  inline LsObjects& expected_results() {
    return expected_results_;
  }

  /**
   * Creates a config for testing VFS storage backends over local emulators.
   *
   * @return Fully initialized configuration for testing VFS storage backends.
   */
  static tiledb::sm::Config create_test_config();

  /** FilePredicate for passing to ls_filtered that accepts all files. */
  static bool accept_all_files(const std::string_view&, uint64_t) {
    return true;
  }

  std::vector<size_t> test_tree_;
  ThreadPool compute_, io_;
  tiledb::sm::VFS vfs_;
  std::string prefix_;
  tiledb::sm::URI temp_dir_;

 private:
  LsObjects expected_results_;
  bool is_supported_;
};

/**
 * Test object for tiledb::sm::VFS functionality. When constructed, this test
 * object creates a temporary directory and populates it using the test_tree
 * vector passed to the constructor. For each element in the vector, we create a
 * nested directory with N objects. The constructor also writes `10 * N` bytes
 * of data to each object created for testing returned object sizes are correct.
 *
 * This test object can be used for any valid VFS URI prefix, and is not
 * specific to any one backend.
 */
class VFSTest : public VFSTestBase {
 public:
  VFSTest(const std::vector<size_t>& test_tree, const std::string& prefix);
};

/** Test object for tiledb::sm::S3 functionality. */
class S3Test : public VFSTestBase, protected tiledb::sm::S3_within_VFS {
 public:
  explicit S3Test(const std::vector<size_t>& test_tree)
      : VFSTestBase(test_tree, "s3://")
      , S3_within_VFS(&tiledb::test::g_helper_stats, &io_, vfs_.config()) {
#ifdef HAVE_S3
    s3().create_bucket(temp_dir_);
    for (size_t i = 1; i <= test_tree_.size(); i++) {
      sm::URI path = temp_dir_.join_path("subdir_" + std::to_string(i));
      // VFS::create_dir is a no-op for S3; Just create objects.
      for (size_t j = 1; j <= test_tree_[i - 1]; j++) {
        auto object_uri = path.join_path("test_file_" + std::to_string(j));
        s3().touch(object_uri);
        std::string data(j * 10, 'a');
        s3().write(object_uri, data.data(), data.size());
        s3().flush_object(object_uri).ok();
        expected_results().emplace_back(object_uri.to_string(), data.size());
      }
    }
    std::sort(expected_results().begin(), expected_results().end());
#endif
  }

#ifdef HAVE_S3
  /** Expose protected accessor from S3_within_VFS. */
  tiledb::sm::S3& get_s3() {
    return s3();
  }

  /** Expose protected const accessor from S3_within_VFS. */
  const tiledb::sm::S3& get_s3() const {
    return s3();
  }
#endif
};

/** Stub test object for tiledb::sm::Win and Posix functionality. */
class LocalFsTest : public VFSTestBase {
 public:
  explicit LocalFsTest(const std::vector<size_t>& test_tree);
};

/** Stub test object for tiledb::sm::Azure functionality. */
class AzureTest : public VFSTestBase {
 public:
  explicit AzureTest(const std::vector<size_t>& test_tree)
      : VFSTestBase(test_tree, "azure://") {
#ifdef HAVE_AZURE
    vfs_.create_bucket(temp_dir_).ok();
    for (size_t i = 1; i <= test_tree_.size(); i++) {
      sm::URI path = temp_dir_.join_path("subdir_" + std::to_string(i));
      // VFS::create_dir is a no-op for Azure; Just create objects.
      for (size_t j = 1; j <= test_tree_[i - 1]; j++) {
        auto object_uri = path.join_path("test_file_" + std::to_string(j));
        vfs_.touch(object_uri).ok();
        std::string data(j * 10, 'a');
        vfs_.write(object_uri, data.data(), data.size()).ok();
        vfs_.close_file(object_uri).ok();
        expected_results().emplace_back(object_uri.to_string(), data.size());
      }
    }
    std::sort(expected_results().begin(), expected_results().end());
#endif
  }
};

/** Stub test object for tiledb::sm::GCS functionality. */
class GCSTest : public VFSTestBase {
 public:
  explicit GCSTest(const std::vector<size_t>& test_tree)
      : VFSTestBase(test_tree, "gcs://") {
#ifdef HAVE_GCS
    vfs_.create_bucket(temp_dir_).ok();
    for (size_t i = 1; i <= test_tree_.size(); i++) {
      sm::URI path = temp_dir_.join_path("subdir_" + std::to_string(i));
      // VFS::create_dir is a no-op for GCS; Just create objects.
      for (size_t j = 1; j <= test_tree_[i - 1]; j++) {
        auto object_uri = path.join_path("test_file_" + std::to_string(j));
        vfs_.touch(object_uri).ok();
        std::string data(j * 10, 'a');
        vfs_.write(object_uri, data.data(), data.size()).ok();
        vfs_.close_file(object_uri).ok();
        expected_results().emplace_back(object_uri.to_string(), data.size());
      }
    }
    std::sort(expected_results().begin(), expected_results().end());
#endif
  }
};

/** Stub test object for tiledb::sm::GS functionality. */
class GSTest : public VFSTestBase {
 public:
  explicit GSTest(const std::vector<size_t>& test_tree)
      : VFSTestBase(test_tree, "gs://") {
  }
};

/** Stub test object for tiledb::sm::MemFilesystem functionality. */
class MemFsTest : public VFSTestBase {
 public:
  explicit MemFsTest(const std::vector<size_t>& test_tree)
      : VFSTestBase(test_tree, "mem://") {
  }
};
}  // namespace tiledb::test

#endif
