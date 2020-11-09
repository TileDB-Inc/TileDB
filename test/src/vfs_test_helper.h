/**
 * @file   helpers.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
  virtual Status prepare_config(
      tiledb_config_t* config, tiledb_error_t* error) = 0;

  /**
   * Creates bucket / container if does not exist
   * Only for S3, Azure
   * No-op otherwise
   *
   * @param ctx The TileDB.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) = 0;

  /**
   * Removes bucket / container if exists
   * Only for S3, Azure
   * No-op otherwise
   *
   * @param ctx The TileDB.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs) = 0;

  /* ********************************* */
  /*             ATTRIBUTES            */
  /* ********************************* */

  /** The directory name of the associated filesystem. */
  std::string vfs_helper_temp_dir_;

  /** The vector of supported filesystems. */
  std::vector<SupportedFs> fs_vec;
};

/**
 * This class provides support for
 * the S3 filesystem.
 */
class SupportedFsS3 : public SupportedFs {
 public:
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
   * @param ctx The TileDB.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Removes bucket if exists
   *
   * @param ctx The TileDB.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /* ********************************* */
  /*             ATTRIBUTES            */
  /* ********************************* */

  /** The directory prefix of the S3 filesystem. */
  const std::string S3_PREFIX;

  /** The bucket name for the S3 filesystem. */
  const std::string S3_BUCKET;

  /** The directory name of the S3 filesystem. */
  const std::string S3_TEMP_DIR;

  /** The directory name of the S3 filesystem. */
  std::string vfs_helper_temp_dir = S3_TEMP_DIR;
};

/**
 * This class provides support for
 * the HDFS filesystem.
 */
class SupportedFsHDFS : public SupportedFs {
 public:
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
   * No-op
   *
   * @param ctx The TileDB.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * No-op
   *
   * @param ctx The TileDB.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /* ********************************* */
  /*             ATTRIBUTES            */
  /* ********************************* */

  /** The directory name of the HDFS filesystem. */
  const std::string HDFS_TEMP_DIR;

  /** The directory name of the HDFS filesystem. */
  std::string vfs_helper_temp_dir = HDFS_TEMP_DIR;
};

/**
 * This class provides support for
 * the Azure filesystem.
 */
class SupportedFsAzure : public SupportedFs {
 public:
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
   * @param ctx The TileDB.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * Removes container if exists
   *
   * @param ctx The TileDB.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /* ********************************* */
  /*             ATTRIBUTES            */
  /* ********************************* */

  /** The directory prefix of the Azure filesystem. */
  const std::string AZURE_PREFIX;

  /** The container name for the Azure filesystem. */
  const std::string container;

  /** The directory name of the Azure filesystem. */
  const std::string AZURE_TEMP_DIR;

  /** The directory name of the Azure filesystem. */
  std::string vfs_helper_temp_dir = AZURE_TEMP_DIR;
};

/**
 * This class provides support for
 * the Windows filesystem.
 */
class SupportedFsWindows : public SupportedFs {
 public:
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
   * No-op
   *
   * @param ctx The TileDB.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * No-op
   *
   * @param ctx The TileDB.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /* ********************************* */
  /*             ATTRIBUTES            */
  /* ********************************* */

  /** The directory name of the Windows filesystem. */
  const std::string WIN_TEMP_DIR;

  /** The directory name of the Windows filesystem. */
  std::string vfs_helper_temp_dir = WIN_TEMP_DIR;
};

/**
 * This class provides support for
 * the Posix filesystem.
 */
class SupportedFsPosix : public SupportedFs {
 public:
  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns Status upon setting up the associated
   * filesystem's configuration
   *
   * @param config Configuration parameters
   * @param error Error
   * @return Status OK if successful
   */
  virtual Status prepare_config(tiledb_config_t* config, tiledb_error_t* error);

  /**
   * No-op
   *
   * @param ctx The TileDB.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status init(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /**
   * No-op
   *
   * @param ctx The TileDB.
   * @param vfs The VFS object.
   * @return Status OK if successful
   */
  virtual Status close(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs);

  /* ********************************* */
  /*             ATTRIBUTES            */
  /* ********************************* */

  /** The directory name of the Posix filesystem. */
  const std::string POSIX_TEMP_DIR;

  /** The directory name of the Posix filesystem. */
  std::string vfs_helper_temp_dir = POSIX_TEMP_DIR;
};

/**
 * Create the vector of supported filesystems.
 */
std::vector<SupportedFs> vfs_test_get_fs_vec();

/**
 * Initialize the vfs test.
 *
 * @param ctx The TileDB.
 * @param vfs The VFS object.
 */
void vfs_test_init(tiledb_ctx_t** ctx, tiledb_vfs_t** vfs);

/**
 * Close the vfs test.
 *
 * @param fs_vec The vector of supported filesystems.
 */
Status vfs_test_close(std::vector<SupportedFs> fs_vec);

}  // End of namespace test
}  // End of namespace tiledb

#endif