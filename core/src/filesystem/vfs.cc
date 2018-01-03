/**
 * @file   vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file implements the VFS class.
 */

#include "vfs.h"
#include <config.h>
#include "hdfs_filesystem.h"
#include "logger.h"
#include "posix_filesystem.h"

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

VFS::VFS() {
#ifdef HAVE_HDFS
  supported_fs_.insert(Filesystem::HDFS);
#endif
#ifdef HAVE_S3
  supported_fs_.insert(Filesystem::S3);
#endif
}

VFS::~VFS() {
#ifdef HAVE_HDFS
  if (hdfs_ != nullptr) {
    // Do not disconnect - may lead to problems
    // Status st = hdfs::disconnect(hdfs_);
  }
#endif
#ifdef HAVE_S3
    // Do not disconnect - may lead to problems
    // Status st = s3_.disconnect();
#endif
}

/* ********************************* */
/*                API                */
/* ********************************* */

std::string VFS::abs_path(const std::string& path) {
  if (URI::is_posix(path))
    return posix::abs_path(path);
  if (URI::is_hdfs(path))
    return path;
  if (URI::is_s3(path))
    return path;
  // Certainly starts with "<resource>://" other than "file://"
  return path;
}

Status VFS::create_dir(const URI& uri) const {
  if (uri.is_posix()) {
    return posix::create_dir(uri.to_path());
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::create_dir(hdfs_, uri);
#else
    return Status::VFSError("TileDB was built without HDFS support");
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.create_dir(uri);
#else
    return Status::VFSError("TileDB was built without S3 support");
#endif
  }
  return Status::Error(
      std::string("Unsupported URI scheme: ") + uri.to_string());
}

Status VFS::create_file(const URI& uri) const {
  // Do nothing if the file already exists
  if (is_file(uri))
    return Status::Ok();

  if (uri.is_posix()) {
    return posix::create_file(uri.to_path());
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::create_file(hdfs_, uri);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.create_file(uri);
#else
    return Status::VFSError("TileDB was built without S3 support");
#endif
  }
  return LOG_STATUS(Status::VFSError(
      std::string("Unsupported URI scheme: ") + uri.to_string()));
}

Status VFS::create_bucket(const URI& uri) const {
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.create_bucket(uri);
#else
    (void)uri;
    return LOG_STATUS(Status::VFSError(std::string("S3 is not supported")));
#endif
  }
  return LOG_STATUS(Status::VFSError(
      std::string("Cannot create bucket; Unsupported URI scheme: ") +
      uri.to_string()));
}

Status VFS::remove_bucket(const URI& uri) const {
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.delete_bucket(uri);
#else
    (void)uri;
    return LOG_STATUS(Status::VFSError(std::string("S3 is not supported")));
#endif
  }
  return LOG_STATUS(Status::VFSError(
      std::string("Cannot remove bucket; Unsupported URI scheme: ") +
      uri.to_string()));
}

Status VFS::remove_path(const URI& uri) const {
  if (uri.is_posix()) {
    return posix::remove_path(uri.to_path());
  } else if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::remove_path(hdfs_, uri);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  } else if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.remove_path(uri);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  } else {
    return LOG_STATUS(
        Status::VFSError("Unsupported URI scheme: " + uri.to_string()));
  }
}

Status VFS::remove_file(const URI& uri) const {
  if (uri.is_posix()) {
    return posix::remove_file(uri.to_path());
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::remove_file(hdfs_, uri);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.remove_file(uri);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI scheme: " + uri.to_string()));
}

Status VFS::filelock_lock(const URI& uri, int* fd, bool shared) const {
  if (uri.is_posix())
    return posix::filelock_lock(uri.to_path(), fd, shared);
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return Status::Ok();
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return Status::Ok();
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI scheme: " + uri.to_string()));
}

Status VFS::filelock_unlock(const URI& uri, int fd) const {
  if (uri.is_posix()) {
    return posix::filelock_unlock(fd);
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return Status::Ok();
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return Status::Ok();
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI scheme: " + uri.to_string()));
}

Status VFS::file_size(const URI& uri, uint64_t* size) const {
  if (uri.is_posix()) {
    return posix::file_size(uri.to_path(), size);
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::file_size(hdfs_, uri, size);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.file_size(uri, size);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI scheme: " + uri.to_string()));
}

bool VFS::is_dir(const URI& uri) const {
  if (uri.is_posix()) {
    return posix::is_dir(uri.to_path());
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::is_dir(hdfs_, uri);
#else
    return false;
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.is_dir(uri);
#else
    return false;
#endif
  }
  return false;
}

bool VFS::is_file(const URI& uri) const {
  if (uri.is_posix()) {
    return posix::is_file(uri.to_path());
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::is_file(hdfs_, uri);
#else
    return false;
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.is_file(uri);
#else
    return false;
#endif
  }
  return false;
}

bool VFS::is_bucket(const URI& uri) const {
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.is_bucket(uri);
#else
    (void)uri;
    return false;
#endif
  }
  return false;
}

Status VFS::init(const Config::VFSParams& vfs_params) {
#ifdef HAVE_HDFS
  RETURN_NOT_OK(hdfs::connect(hdfs_));
#endif
#ifdef HAVE_S3
  S3::S3Config s3_config;
  s3_config.region_ = vfs_params.s3_params_.region_;
  s3_config.scheme_ = vfs_params.s3_params_.scheme_;
  s3_config.endpoint_override_ = vfs_params.s3_params_.endpoint_override_;
  s3_config.use_virtual_addressing_ =
      vfs_params.s3_params_.use_virtual_addressing_;
  s3_config.file_buffer_size_ = vfs_params.s3_params_.file_buffer_size_;
  s3_config.connect_timeout_ms_ = vfs_params.s3_params_.connect_timeout_ms_;
  s3_config.request_timeout_ms_ = vfs_params.s3_params_.request_timeout_ms_;
  RETURN_NOT_OK(s3_.connect(s3_config));
#endif

  (void)vfs_params;

  return Status::Ok();
}

Status VFS::ls(const URI& parent, std::vector<URI>* uris) const {
  std::vector<std::string> paths;
  if (parent.is_posix()) {
    RETURN_NOT_OK(posix::ls(parent.to_path(), &paths));
  } else if (parent.is_hdfs()) {
#ifdef HAVE_HDFS
    RETURN_NOT_OK(hdfs::ls(hdfs_, parent, &paths));
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  } else if (parent.is_s3()) {
#ifdef HAVE_S3
    RETURN_NOT_OK(s3_.ls(parent, &paths));
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  } else {
    return LOG_STATUS(
        Status::VFSError("Unsupported URI scheme: " + parent.to_string()));
  }
  std::sort(paths.begin(), paths.end());
  for (auto& path : paths) {
    uris->emplace_back(path);
  }
  return Status::Ok();
}

Status VFS::move_path(const URI& old_uri, const URI& new_uri) {
  // Posix
  if (old_uri.is_posix()) {
    if (new_uri.is_posix())
      return posix::move_path(old_uri.to_path(), new_uri.to_path());
    return LOG_STATUS(Status::VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // HDFS
  if (old_uri.is_hdfs()) {
    if (new_uri.is_hdfs())
#ifdef HAVE_HDFS
      return hdfs::move_path(hdfs_, old_uri, new_uri);
#else
      return LOG_STATUS(
          Status::VFSError("TileDB was built without HDFS support"));
#endif
    return LOG_STATUS(Status::VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // S3
  if (old_uri.is_s3()) {
    if (new_uri.is_s3())
#ifdef HAVE_S3
      return s3_.move_path(old_uri, new_uri);
#else
      return LOG_STATUS(
          Status::VFSError("TileDB was built without S3 support"));
#endif
    return LOG_STATUS(Status::VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // Unsupported filesystem
  return LOG_STATUS(Status::VFSError(
      "Unsupported URI schemes: " + old_uri.to_string() + ", " +
      new_uri.to_string()));
}

Status VFS::read(
    const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const {
  if (!is_file(uri))
    return LOG_STATUS(
        Status::VFSError("Cannot read from file; File does not exist"));

  if (uri.is_posix()) {
    return posix::read(uri.to_path(), offset, buffer, nbytes);
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::read(hdfs_, uri, offset, buffer, nbytes);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.read(uri, offset, buffer, nbytes);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI schemes: " + uri.to_string()));
}

bool VFS::supports_fs(Filesystem fs) const {
  return (supported_fs_.find(fs) != supported_fs_.end());
}

Status VFS::sync(const URI& uri) {
  if (uri.is_posix()) {
    return posix::sync(uri.to_path());
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return Status::Ok();
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.flush_file(uri);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI schemes: " + uri.to_string()));
}

Status VFS::write(const URI& uri, const void* buffer, uint64_t buffer_size) {
  if (uri.is_posix()) {
    return posix::write(uri.to_path(), buffer, buffer_size);
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::write(hdfs_, uri, buffer, buffer_size);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.write(uri, buffer, buffer_size);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI schemes: " + uri.to_string()));
}

}  // namespace tiledb
