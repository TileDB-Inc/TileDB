/**
 * @file   vfs_test_helper.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 TileDB, Inc.
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

#ifndef TILEDB_VFS_TEST_HELPER_H
#define TILEDB_VFS_TEST_HELPER_H

#include "test/src/helpers.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

using namespace tiledb::common;

namespace tiledb {

namespace sm {}

namespace test {

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

  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

  /** The directory name of the associated filesystem. */
  std::string temp_dir_;

  /** The vector of supported filesystems. */
  const std::vector<SupportedFs*> fs_vec;
};

/**
 * This class provides support for
 * the S3 filesystem.
 */
class SupportedFsS3 : public SupportedFs {
 public:
  ~SupportedFsS3() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns Status upon setting up the associated
   * filesystem's configuration
   *
   * @param config Configuration parameters
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

  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

  /** The directory prefix of the S3 filesystem. */
  const std::string s3_prefix_ = "s3://";

  /** The bucket name for the S3 filesystem. */
  const std::string s3_bucket_ = s3_prefix_ + random_name("tiledb") + "/";

  /** The directory name of the S3 filesystem. */
  const std::string s3_temp_dir_ = s3_bucket_ + "tiledb_test/";

  /** The directory name of the S3 filesystem. */
  std::string temp_dir_ = s3_temp_dir_;
};

/**
 * This class provides support for
 * the HDFS filesystem.
 */
class SupportedFsHDFS : public SupportedFs {
 public:
  ~SupportedFsHDFS() = default;
  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * No-op
   *
   * @param config Configuration parameters
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

  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

  /** The directory name of the HDFS filesystem. */
  const std::string hdfs_temp_dir_ = "hdfs:///tiledb_test/";

  /** The directory name of the HDFS filesystem. */
  std::string temp_dir_ = hdfs_temp_dir_;
};

/**
 * This class provides support for
 * the Azure filesystem.
 */
class SupportedFsAzure : public SupportedFs {
 public:
  ~SupportedFsAzure() = default;
  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns Status upon setting up the associated
   * filesystem's configuration
   *
   * @param config Configuration parameters
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

  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

  /** The directory prefix of the Azure filesystem. */
  const std::string azure_prefix_ = "azure://";

  /** The container name for the Azure filesystem. */
  const std::string container = azure_prefix_ + random_name("tiledb") + "/";

  /** The directory name of the Azure filesystem. */
  const std::string azure_temp_dir_ = container + "tiledb_test/";

  /** The directory name of the Azure filesystem. */
  std::string temp_dir_ = azure_temp_dir_;
};

/**
 * This class provides support for
 * the Windows or Posix (local) filesystem.
 */
class SupportedFsLocal : public SupportedFs {
 public:
  ~SupportedFsLocal() = default;
  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * No-op
   *
   * @param config Configuration parameters
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

  /* ********************************* */
  /*           ATTRIBUTES              */
  /* ********************************* */

#ifdef _WIN32
  /** The directory name of the Windows filesystem. */
  std::string temp_dir_ = tiledb::sm::Win::current_dir() + "\\tiledb_test\\";

  /** The file prefix name of the Windows filesystem. */
  std::string file_prefix_ = "";
#else
  /** The directory name of the Posix filesystem. */
  std::string temp_dir_ = tiledb::sm::Posix::current_dir() + "/tiledb_test/";

  /** The file prefix name of the Posix filesystem. */
  std::string file_prefix_ = "file://";

#endif
};

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
 */
Status vfs_test_init(
    const std::vector<std::unique_ptr<SupportedFs>>& fs_vec,
    tiledb_ctx_t** ctx,
    tiledb_vfs_t** vfs);

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

}  // End of namespace test
}  // End of namespace tiledb

#endif