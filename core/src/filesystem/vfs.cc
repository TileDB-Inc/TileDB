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
#include "hdfs_filesystem.h"
#include "logger.h"
#include "posix_filesystem.h"
#include "win32_filesystem.h"

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

VFS::VFS() = default;

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
#ifdef _WIN32
  if (win32::is_win32_path(path))
    return win32::uri_from_path(win32::abs_path(path));
  else if (URI::is_file(path)) 
    return win32::uri_from_path(win32::abs_path(win32::path_from_uri(path)));
#else
  if (URI::is_file(path))
    return posix::abs_path(path);
#endif
  if (URI::is_hdfs(path))
    return path;
  if (URI::is_s3(path))
    return path;
  // Certainly starts with "<resource>://" other than "file://"
  return path;
}

Status VFS::create_dir(const URI& uri) const {
  if (uri.is_file()) {
#ifdef _WIN32
    return win32::create_dir(uri.to_path());
#else
    return posix::create_dir(uri.to_path());
#endif
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
  if (uri.is_file()) {
#ifdef _WIN32
    return win32::create_file(uri.to_path());
#else
    return posix::create_file(uri.to_path());
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::create_file(hdfs_, uri);
#else
    return Status::VFSError("TileDB was built without HDFS support");
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.create_file(uri);
#else
    return Status::VFSError("TileDB was built without S3 support");
#endif
  }
  return Status::VFSError(
      std::string("Unsupported URI scheme: ") + uri.to_string());
}

Status VFS::remove_path(const URI& uri) const {
  if (uri.is_file()) {
#ifdef _WIN32
    return win32::remove_path(uri.to_path());
#else
    return posix::remove_path(uri.to_path());
#endif
  } else if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::remove_path(hdfs_, uri);
#else
    return Status::VFSError("TileDB was built without HDFS support");
#endif
  } else if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.remove_path(uri);
#else
    return Status::VFSError("TileDB was built without S3 support");
#endif
  } else {
    return Status::VFSError("Unsupported URI scheme: " + uri.to_string());
  }
}

Status VFS::remove_file(const URI& uri) const {
  if (uri.is_file()) {
#ifdef _WIN32
    return win32::remove_file(uri.to_path());
#else
    return posix::remove_file(uri.to_path());
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::remove_file(hdfs_, uri);
#else
    return Status::VFSError("TileDB was built without HDFS support");
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.remove_file(uri);
#else
    return Status::VFSError("TileDB was built without S3 support");
#endif
  }
  return Status::VFSError("Unsupported URI scheme: " + uri.to_string());
}

Status VFS::filelock_lock(const URI& uri, file_lock_t* fd, bool shared) const {
  if (uri.is_file())
#ifdef _WIN32
    return win32::filelock_lock(uri.to_path(), fd, shared);
#else
    return posix::filelock_lock(uri.to_path(), fd, shared);
#endif

  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return Status::Ok();
#else
    return Status::VFSError("TileDB was built without HDFS support");
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return Status::Ok();
#else
    return Status::VFSError("TileDB was built without S3 support");
#endif
  }
  return Status::VFSError("Unsupported URI scheme: " + uri.to_string());
}

Status VFS::filelock_unlock(const URI& uri, file_lock_t fd) const {
  if (uri.is_file()) {
#ifdef _WIN32
    return win32::filelock_unlock(fd);
#else
    return posix::filelock_unlock(fd);
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return Status::Ok();
#else
    return Status::VFSError("TileDB was built without HDFS support");
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return Status::Ok();
#else
    return Status::VFSError("TileDB was built without S3 support");
#endif
  }
  return Status::VFSError("Unsupported URI scheme: " + uri.to_string());
}

Status VFS::file_size(const URI& uri, uint64_t* size) const {
  if (uri.is_file()) {
#ifdef _WIN32
    return win32::file_size(uri.to_path(), size);
#else
    return posix::file_size(uri.to_path(), size);
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::file_size(hdfs_, uri, size);
#else
    return Status::VFSError("TileDB was built without HDFS support");
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.file_size(uri, size);
#else
    return Status::VFSError("TileDB was built without S3 support");
#endif
  }
  return Status::VFSError("Unsupported URI scheme: " + uri.to_string());
}

bool VFS::is_dir(const URI& uri) const {
  if (uri.is_file()) {
#ifdef _WIN32
    return win32::is_dir(uri.to_path());
#else
    return posix::is_dir(uri.to_path());
#endif
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
  if (uri.is_file()) {
#ifdef _WIN32
    return win32::is_file(uri.to_path());
#else
    return posix::is_file(uri.to_path());
#endif
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

#ifdef HAVE_S3
Status VFS::init(const S3::S3Config& s3_config) {
#ifdef HAVE_HDFS
  RETURN_NOT_OK(hdfs::connect(hdfs_));
#endif
  return s3_.connect(s3_config);
}
#else
Status VFS::init() {
#ifdef HAVE_HDFS
  RETURN_NOT_OK(hdfs::connect(hdfs_));
#endif
  return Status::Ok();
}
#endif

Status VFS::ls(const URI& parent, std::vector<URI>* uris) const {
  std::vector<std::string> paths;
  if (parent.is_file()) {
#ifdef _WIN32
    RETURN_NOT_OK(win32::ls(parent.to_path(), &paths));
#else
    RETURN_NOT_OK(posix::ls(parent.to_path(), &paths));
#endif
  } else if (parent.is_hdfs()) {
#ifdef HAVE_HDFS
    RETURN_NOT_OK(hdfs::ls(hdfs_, parent, &paths));
#else
    return Status::VFSError("TileDB was built without HDFS support");
#endif
  } else if (parent.is_s3()) {
#ifdef HAVE_S3
    RETURN_NOT_OK(s3_.ls(parent, &paths));
#else
    return Status::VFSError("TileDB was built without S3 support");
#endif
  } else {
    return Status::VFSError("Unsupported URI scheme: " + parent.to_string());
  }
  std::sort(paths.begin(), paths.end());
  for (auto& path : paths) {
    uris->emplace_back(path);
  }
  return Status::Ok();
}

Status VFS::move_path(const URI& old_uri, const URI& new_uri) {
  if (old_uri.is_file()) {
    if (new_uri.is_file()) {
#ifdef _WIN32
      return win32::move_path(old_uri.to_path(), new_uri.to_path());
#else
      return posix::move_path(old_uri.to_path(), new_uri.to_path());
#endif
    }
    if (new_uri.is_hdfs()) {
      return hdfs::put_path(old_uri, new_uri);
    }
  }
  if (old_uri.is_hdfs()) {
    if (new_uri.is_hdfs()) {
#ifdef HAVE_HDFS
      return hdfs::move_path(hdfs_, old_uri, new_uri);
#else
      return Status::VFSError("TileDB was built without HDFS support");
#endif
    }
    if (new_uri.is_file()) {
      return hdfs::get_path(old_uri, new_uri);
    }
  }
  if (old_uri.is_s3()) {
    if (new_uri.is_s3()) {
#ifdef HAVE_S3
      return s3_.move_path(old_uri, new_uri);
#else
      return Status::VFSError("TileDB was built without S3 support");
#endif
    }
  }
  return Status::VFSError(
      "Unsupported URI schemes: " + old_uri.to_string() + ", " +
      new_uri.to_string());
}

Status VFS::read_from_file(
    const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const {
  if (uri.is_file()) {
#ifdef _WIN32
    return win32::read_from_file(uri.to_path(), offset, buffer, nbytes);
#else
    return posix::read_from_file(uri.to_path(), offset, buffer, nbytes);
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::read_from_file(hdfs_, uri, offset, buffer, nbytes);
#else
    return Status::VFSError("TileDB was built without HDFS support");
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.read_from_file(uri, offset, buffer, nbytes);
#else
    return Status::VFSError("TileDB was built without S3 support");
#endif
  }
  return Status::VFSError("Unsupported URI schemes: " + uri.to_string());
}

Status VFS::sync(const URI& uri) {
  if (uri.is_file()) {
#ifdef _WIN32
    return win32::sync(uri.to_path());
#else
    return posix::sync(uri.to_path());
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return Status::Ok();
#else
    return Status::VFSError("TileDB was built without HDFS support");
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.flush_file(uri);
#else
    return Status::VFSError("TileDB was built without S3 support");
#endif
  }
  return Status::VFSError("Unsupported URI schemes: " + uri.to_string());
}

Status VFS::write_to_file(
  const URI& uri, const void* buffer, uint64_t buffer_size) {
  if (uri.is_file()) {
#ifdef _WIN32
    return win32::write_to_file(uri.to_path(), buffer, buffer_size);
#else
    return posix::write_to_file(uri.to_path(), buffer, buffer_size);
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs::write_to_file(hdfs_, uri, buffer, buffer_size);
#else
    return Status::VFSError("TileDB was built without HDFS support");
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.write_to_file(uri, buffer, buffer_size);
#else
    return Status::VFSError("TileDB was built without S3 support");
#endif
  }
  return Status::VFSError("Unsupported URI schemes: " + uri.to_string());
}

}  // namespace tiledb
