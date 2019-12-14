/**
 * @file   vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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

#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/filesystem/hdfs_filesystem.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"

#include <iostream>
#include <list>
#include <unordered_map>

namespace tiledb {
namespace sm {

/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/**
 * Map of file URI -> number of current locks. This is shared across the entire
 * process.
 */
static std::unordered_map<std::string, std::pair<uint64_t, filelock_t>>
    process_filelocks_;

/** Mutex protecting the filelock process and the filelock counts map. */
static std::mutex filelock_mtx_;

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

VFS::VFS() {
  STATS_FUNC_VOID_IN(vfs_constructor);

#ifdef HAVE_HDFS
  supported_fs_.insert(Filesystem::HDFS);
#endif
#ifdef HAVE_S3
  supported_fs_.insert(Filesystem::S3);
#endif

  init_ = false;

  STATS_FUNC_VOID_OUT(vfs_constructor);
}

/* ********************************* */
/*                API                */
/* ********************************* */

std::string VFS::abs_path(const std::string& path) {
  STATS_FUNC_IN(vfs_abs_path);
  // workaround for older clang (llvm 3.5) compilers (issue #828)
  std::string path_copy = path;
#ifdef _WIN32
  if (Win::is_win_path(path))
    return Win::uri_from_path(Win::abs_path(path));
  else if (URI::is_file(path))
    return Win::uri_from_path(Win::abs_path(Win::path_from_uri(path)));
#else
  if (URI::is_file(path))
    return Posix::abs_path(path);
#endif
  if (URI::is_hdfs(path))
    return path_copy;
  if (URI::is_s3(path))
    return path_copy;
  // Certainly starts with "<resource>://" other than "file://"
  return path_copy;

  STATS_FUNC_OUT(vfs_abs_path);
}

Config VFS::config() const {
  return config_;
}

Status VFS::create_dir(const URI& uri) const {
  STATS_FUNC_IN(vfs_create_dir);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot create directory; VFS not initialized"));

  if (!uri.is_s3()) {
    bool is_dir;
    RETURN_NOT_OK(this->is_dir(uri, &is_dir));
    if (is_dir)
      return Status::Ok();
  }

  if (uri.is_file()) {
#ifdef _WIN32
    return win_.create_dir(uri.to_path());
#else
    return posix_.create_dir(uri.to_path());
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs_->create_dir(uri);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    // It is a noop for S3
    return Status::Ok();
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(Status::VFSError(
      std::string("Unsupported URI scheme: ") + uri.to_string()));

  STATS_FUNC_OUT(vfs_create_dir);
}

Status VFS::dir_size(const URI& dir_name, uint64_t* dir_size) const {
  STATS_FUNC_IN(vfs_dir_size);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot get directory size; VFS not initialized"));

  // Sanity check
  bool is_dir;
  RETURN_NOT_OK(this->is_dir(dir_name, &is_dir));
  if (!is_dir)
    return LOG_STATUS(Status::VFSError(
        std::string("Cannot get directory size; Input '") +
        dir_name.to_string() + "' is not a directory"));

  // Get all files in the tree rooted at `dir_name` and add their sizes
  *dir_size = 0;
  uint64_t size;
  std::list<URI> to_ls;
  bool is_file;
  to_ls.push_front(dir_name);
  do {
    auto uri = to_ls.front();
    to_ls.pop_front();
    std::vector<URI> children;
    RETURN_NOT_OK(ls(uri, &children));
    for (const auto& child : children) {
      RETURN_NOT_OK(this->is_file(child, &is_file));
      if (is_file) {
        RETURN_NOT_OK(file_size(child, &size));
        *dir_size += size;
      } else {
        to_ls.push_back(child);
      }
    }
  } while (!to_ls.empty());

  return Status::Ok();

  STATS_FUNC_OUT(vfs_dir_size);
}

Status VFS::touch(const URI& uri) const {
  STATS_FUNC_IN(vfs_create_file);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot touch file; VFS not initialized"));

  if (uri.is_file()) {
#ifdef _WIN32
    return win_.touch(uri.to_path());
#else
    return posix_.touch(uri.to_path());
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs_->touch(uri);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.touch(uri);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(Status::VFSError(
      std::string("Unsupported URI scheme: ") + uri.to_string()));

  STATS_FUNC_OUT(vfs_create_file);
}

Status VFS::cancel_all_tasks() {
  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot cancel all tasks; VFS not initialized"));

  cancelable_tasks_.cancel_all_tasks();
  return Status::Ok();
}

Status VFS::create_bucket(const URI& uri) const {
  STATS_FUNC_IN(vfs_create_bucket);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot create bucket; VFS not initialized"));

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

  STATS_FUNC_OUT(vfs_create_bucket);
}

Status VFS::remove_bucket(const URI& uri) const {
  STATS_FUNC_IN(vfs_remove_bucket);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot remove bucket; VFS not initialized"));

  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.remove_bucket(uri);
#else
    (void)uri;
    return LOG_STATUS(Status::VFSError(std::string("S3 is not supported")));
#endif
  }
  return LOG_STATUS(Status::VFSError(
      std::string("Cannot remove bucket; Unsupported URI scheme: ") +
      uri.to_string()));

  STATS_FUNC_OUT(vfs_remove_bucket);
}

Status VFS::empty_bucket(const URI& uri) const {
  STATS_FUNC_IN(vfs_empty_bucket);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot empty bucket; VFS not "
                         "initialized"));

  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.empty_bucket(uri);
#else
    (void)uri;
    return LOG_STATUS(Status::VFSError(std::string("S3 is not supported")));
#endif
  }
  return LOG_STATUS(Status::VFSError(
      std::string("Cannot empty bucket; Unsupported URI scheme: ") +
      uri.to_string()));

  STATS_FUNC_OUT(vfs_empty_bucket);
}

Status VFS::is_empty_bucket(const URI& uri, bool* is_empty) const {
  STATS_FUNC_IN(vfs_is_empty_bucket);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot check if bucket is empty; "
                         "VFS not initialized"));

  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.is_empty_bucket(uri, is_empty);
#else
    (void)uri;
    (void)is_empty;
    return LOG_STATUS(Status::VFSError(std::string("S3 is not supported")));
#endif
  }
  return LOG_STATUS(Status::VFSError(
      std::string("Cannot remove bucket; Unsupported URI scheme: ") +
      uri.to_string()));

  STATS_FUNC_OUT(vfs_is_empty_bucket);
}

Status VFS::remove_dir(const URI& uri) const {
  STATS_FUNC_IN(vfs_remove_dir);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot remove directory; VFS not "
                         "initialized"));

  if (uri.is_file()) {
#ifdef _WIN32
    return win_.remove_dir(uri.to_path());
#else
    return posix_.remove_dir(uri.to_path());
#endif
  } else if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs_->remove_dir(uri);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  } else if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.remove_dir(uri);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  } else {
    return LOG_STATUS(
        Status::VFSError("Unsupported URI scheme: " + uri.to_string()));
  }

  STATS_FUNC_OUT(vfs_remove_dir);
}

Status VFS::remove_file(const URI& uri) const {
  STATS_FUNC_IN(vfs_remove_file);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot remove file; VFS not initialized"));

  if (uri.is_file()) {
#ifdef _WIN32
    return win_.remove_file(uri.to_path());
#else
    return posix_.remove_file(uri.to_path());
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs_->remove_file(uri);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.remove_object(uri);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI scheme: " + uri.to_string()));

  STATS_FUNC_OUT(vfs_remove_file);
}

Status VFS::filelock_lock(const URI& uri, filelock_t* lock, bool shared) const {
  STATS_FUNC_IN(vfs_filelock_lock);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot lock filelock; VFS not initialized"));

  // Get config
  bool found = false;
  bool enable_filelocks = false;
  RETURN_NOT_OK(config_.get<bool>(
      "vfs.file.enable_filelocks", &enable_filelocks, &found));
  assert(found);

  if (!enable_filelocks)
    return Status::Ok();

  // Hold the lock while updating counts and performing the lock.
  std::unique_lock<std::mutex> lck(filelock_mtx_);

  auto it = process_filelocks_.find(uri.to_string());
  if (it != process_filelocks_.end()) {
    it->second.first++;
    // we need to return the lock for the xlock semantics
    *lock = it->second.second;
    return Status::Ok();
  }

  // We must hold the fd in the global map in order to free from any context
  if (uri.is_file()) {
#ifdef _WIN32
    auto st = win_.filelock_lock(uri.to_path(), lock, shared);
#else
    auto st = posix_.filelock_lock(uri.to_path(), lock, shared);
#endif

    if (st.ok())
      process_filelocks_[uri.to_string()] = {1, *lock};
    return st;
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

  STATS_FUNC_OUT(vfs_filelock_lock);
}

Status VFS::filelock_unlock(const URI& uri) const {
  STATS_FUNC_IN(vfs_filelock_unlock);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot unlock filelock; VFS not initialized"));

  // Get config
  bool found = false;
  bool enable_filelocks = false;
  RETURN_NOT_OK(config_.get<bool>(
      "vfs.file.enable_filelocks", &enable_filelocks, &found));
  assert(found);

  if (!enable_filelocks)
    return Status::Ok();

  // Hold the lock while updating counts and performing the unlock.
  std::unique_lock<std::mutex> lck(filelock_mtx_);

  // Decrement the lock counter and return if the counter is still > 0.
  bool should_unlock = false;
  filelock_t fd = INVALID_FILELOCK;
  Status st = decr_lock_count(uri, &should_unlock, &fd);
  if (!st.ok() || !should_unlock) {
    return st;
  }

  if (uri.is_file()) {
#ifdef _WIN32
    return win_.filelock_unlock(fd);
#else
    return posix_.filelock_unlock(fd);
#endif
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

  STATS_FUNC_OUT(vfs_filelock_unlock);
}

Status VFS::decr_lock_count(
    const URI& uri, bool* is_zero, filelock_t* lock) const {
  auto it = process_filelocks_.find(uri.to_string());
  if (it == process_filelocks_.end()) {
    return LOG_STATUS(
        Status::VFSError("No lock counter for URI " + uri.to_string()));
  } else if (it->second.first == 0) {
    return LOG_STATUS(
        Status::VFSError("Invalid lock count for URI " + uri.to_string()));
  }

  it->second.first--;

  if (it->second.first == 0) {
    *is_zero = true;
    *lock = it->second.second;
    process_filelocks_.erase(it);
  } else {
    *is_zero = false;
  }
  return Status::Ok();
}

Status VFS::max_parallel_ops(const URI& uri, uint64_t* ops) const {
  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot get max parallel ops; VFS not initialized"));

  bool found;
  *ops = 0;

  if (uri.is_file()) {
    RETURN_NOT_OK(
        config_.get<uint64_t>("vfs.file.max_parallel_ops", ops, &found));
    assert(found);
  } else if (uri.is_hdfs()) {
    // HDFS backend is currently serial.
    *ops = 1;
  } else if (uri.is_s3()) {
    RETURN_NOT_OK(
        config_.get<uint64_t>("vfs.s3.max_parallel_ops", ops, &found));
    assert(found);
  } else {
    *ops = 1;
  }

  return Status::Ok();
}

Status VFS::file_size(const URI& uri, uint64_t* size) const {
  STATS_FUNC_IN(vfs_file_size);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot get file size; VFS not initialized"));

  if (uri.is_file()) {
#ifdef _WIN32
    return win_.file_size(uri.to_path(), size);
#else
    return posix_.file_size(uri.to_path(), size);
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs_->file_size(uri, size);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.object_size(uri, size);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI scheme: " + uri.to_string()));

  STATS_FUNC_OUT(vfs_file_size);
}

Status VFS::is_dir(const URI& uri, bool* is_dir) const {
  STATS_FUNC_IN(vfs_is_dir);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot check directory; VFS not initialized"));

  if (uri.is_file()) {
#ifdef _WIN32
    *is_dir = win_.is_dir(uri.to_path());
#else
    *is_dir = posix_.is_dir(uri.to_path());
#endif
    return Status::Ok();
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs_->is_dir(uri, is_dir);
#else
    *is_dir = false;
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.is_dir(uri, is_dir);
#else
    *is_dir = false;
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI scheme: " + uri.to_string()));

  STATS_FUNC_OUT(vfs_is_dir);
}

Status VFS::is_file(const URI& uri, bool* is_file) const {
  STATS_FUNC_IN(vfs_is_file);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot check file; VFS not initialized"));

  if (uri.is_file()) {
#ifdef _WIN32
    *is_file = win_.is_file(uri.to_path());
#else
    *is_file = posix_.is_file(uri.to_path());
#endif
    return Status::Ok();
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs_->is_file(uri, is_file);
#else
    *is_file = false;
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    *is_file = s3_.is_object(uri);
    return Status::Ok();
#else
    *is_file = false;
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI scheme: " + uri.to_string()));

  STATS_FUNC_OUT(vfs_is_file);
}

Status VFS::is_bucket(const URI& uri, bool* is_bucket) const {
  STATS_FUNC_IN(vfs_is_bucket);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot check bucket; VFS not initialized"));

  if (uri.is_s3()) {
#ifdef HAVE_S3
    *is_bucket = s3_.is_bucket(uri);
    return Status::Ok();
#else
    *is_bucket = false;
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }

  return LOG_STATUS(
      Status::VFSError("Unsupported URI scheme: " + uri.to_string()));

  STATS_FUNC_OUT(vfs_is_bucket);
}

Status VFS::init(const Config* ctx_config, const Config* vfs_config) {
  STATS_FUNC_IN(vfs_init);

  // Set appropriately the config
  if (ctx_config)
    config_ = *ctx_config;
  if (vfs_config)
    config_.inherit(*vfs_config);

  bool found = false;
  uint64_t nthreads = 0;
  RETURN_NOT_OK(config_.get<uint64_t>("vfs.num_threads", &nthreads, &found));
  assert(found);

  RETURN_NOT_OK(thread_pool_.init(nthreads));

#ifdef HAVE_HDFS
  hdfs_ = std::unique_ptr<hdfs::HDFS>(new (std::nothrow) hdfs::HDFS());
  if (hdfs_.get() == nullptr) {
    return LOG_STATUS(Status::VFSError("Could not create VFS HDFS backend"));
  }
  RETURN_NOT_OK(hdfs_->init(config_));
#endif

#ifdef HAVE_S3
  RETURN_NOT_OK(s3_.init(config_, &thread_pool_));
#endif

#ifdef WIN32
  win_.init(config_, &thread_pool_);
#else
  posix_.init(config_, &thread_pool_);
#endif

  init_ = true;

  return Status::Ok();

  STATS_FUNC_OUT(vfs_init);
}

Status VFS::terminate() {
  STATS_FUNC_IN(vfs_terminate);

#ifdef HAVE_S3
  return s3_.disconnect();
#endif

  return Status::Ok();

  STATS_FUNC_OUT(vfs_terminate);
}

Status VFS::ls(const URI& parent, std::vector<URI>* uris) const {
  STATS_FUNC_IN(vfs_ls);

  if (!init_)
    return LOG_STATUS(Status::VFSError("Cannot list; VFS not initialized"));

  std::vector<std::string> paths;
  if (parent.is_file()) {
#ifdef _WIN32
    RETURN_NOT_OK(win_.ls(parent.to_path(), &paths));
#else
    RETURN_NOT_OK(posix_.ls(parent.to_path(), &paths));
#endif
  } else if (parent.is_hdfs()) {
#ifdef HAVE_HDFS
    RETURN_NOT_OK(hdfs_->ls(parent, &paths));
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
  parallel_sort(paths.begin(), paths.end());
  for (const auto& path : std::vector<std::string>(paths)) {
    uris->emplace_back(path);
  }
  return Status::Ok();

  STATS_FUNC_OUT(vfs_ls);
}

Status VFS::move_file(const URI& old_uri, const URI& new_uri) {
  STATS_FUNC_IN(vfs_move_file);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot move file; VFS not initialized"));

  // If new_uri exists, delete it or raise an error based on `force`
  bool is_file;
  RETURN_NOT_OK(this->is_file(new_uri, &is_file));
  if (is_file)
    RETURN_NOT_OK(remove_file(new_uri));

  // File
  if (old_uri.is_file()) {
    if (new_uri.is_file()) {
#ifdef _WIN32
      return win_.move_path(old_uri.to_path(), new_uri.to_path());
#else
      return posix_.move_path(old_uri.to_path(), new_uri.to_path());
#endif
    }
    return LOG_STATUS(Status::VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // HDFS
  if (old_uri.is_hdfs()) {
    if (new_uri.is_hdfs())
#ifdef HAVE_HDFS
      return hdfs_->move_path(old_uri, new_uri);
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
      return s3_.move_object(old_uri, new_uri);
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

  STATS_FUNC_OUT(vfs_move_file);
}

Status VFS::move_dir(const URI& old_uri, const URI& new_uri) {
  STATS_FUNC_IN(vfs_move_dir);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot move directory; VFS not initialized"));

  // File
  if (old_uri.is_file()) {
    if (new_uri.is_file()) {
#ifdef _WIN32
      return win_.move_path(old_uri.to_path(), new_uri.to_path());
#else
      return posix_.move_path(old_uri.to_path(), new_uri.to_path());
#endif
    }
    return LOG_STATUS(Status::VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // HDFS
  if (old_uri.is_hdfs()) {
    if (new_uri.is_hdfs())
#ifdef HAVE_HDFS
      return hdfs_->move_path(old_uri, new_uri);
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
      return s3_.move_dir(old_uri, new_uri);
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

  STATS_FUNC_OUT(vfs_move_dir);
}

Status VFS::read(
    const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) {
  STATS_FUNC_IN(vfs_read);

  if (!init_)
    return LOG_STATUS(Status::VFSError("Cannot read; VFS not initialized"));

  STATS_COUNTER_ADD(vfs_read_total_bytes, nbytes);

  // Get config params
  bool found;
  uint64_t min_parallel_size = 0;
  RETURN_NOT_OK(config_.get<uint64_t>(
      "vfs.min_parallel_size", &min_parallel_size, &found));
  assert(found);
  uint64_t max_ops = 0;
  RETURN_NOT_OK(max_parallel_ops(uri, &max_ops));

  // Ensure that each thread is responsible for at least min_parallel_size
  // bytes, and cap the number of parallel operations at the configured maximum
  // number.
  uint64_t num_ops =
      std::min(std::max(nbytes / min_parallel_size, uint64_t(1)), max_ops);

  if (num_ops == 1) {
    return read_impl(uri, offset, buffer, nbytes);
  } else {
    STATS_COUNTER_ADD(vfs_read_num_parallelized, 1);
    std::vector<std::future<Status>> results;
    uint64_t thread_read_nbytes = utils::math::ceil(nbytes, num_ops);

    for (uint64_t i = 0; i < num_ops; i++) {
      uint64_t begin = i * thread_read_nbytes,
               end = std::min((i + 1) * thread_read_nbytes - 1, nbytes - 1);
      uint64_t thread_nbytes = end - begin + 1;
      uint64_t thread_offset = offset + begin;
      auto thread_buffer = reinterpret_cast<char*>(buffer) + begin;
      auto task = cancelable_tasks_.enqueue(
          &thread_pool_,
          [this, uri, thread_offset, thread_buffer, thread_nbytes]() {
            return read_impl(uri, thread_offset, thread_buffer, thread_nbytes);
          });
      results.push_back(std::move(task));
    }
    Status st = thread_pool_.wait_all(results);
    if (!st.ok()) {
      std::stringstream errmsg;
      errmsg << "VFS parallel read error '" << uri.to_string() << "'; "
             << st.message();
      return LOG_STATUS(Status::VFSError(errmsg.str()));
    }
    return st;
  }

  STATS_FUNC_OUT(vfs_read);
}

Status VFS::read_impl(
    const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) {
  if (uri.is_file()) {
#ifdef _WIN32
    return win_.read(uri.to_path(), offset, buffer, nbytes);
#else
    return posix_.read(uri.to_path(), offset, buffer, nbytes);
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs_->read(uri, offset, buffer, nbytes);
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

Status VFS::read_all(
    const URI& uri,
    const std::vector<std::tuple<uint64_t, void*, uint64_t>>& regions,
    ThreadPool* thread_pool,
    std::vector<std::future<Status>>* tasks) {
  STATS_FUNC_IN(vfs_read_all);

  if (!init_)
    return LOG_STATUS(Status::VFSError("Cannot read all; VFS not initialized"));

  STATS_COUNTER_ADD(vfs_read_all_total_regions, regions.size());

  // Ensure no deadlock due to shared threadpool
  assert(thread_pool != &thread_pool_);

  if (regions.empty())
    return Status::Ok();

  // Convert the individual regions into batched regions.
  std::vector<BatchedRead> batches;
  RETURN_NOT_OK(compute_read_batches(regions, &batches));

  // Read all the batches and copy to the original destinations.
  for (const auto& batch : batches) {
    URI uri_copy = uri;
    BatchedRead batch_copy = batch;
    auto task = thread_pool->enqueue([uri_copy, batch_copy, this]() {
      Buffer buffer;
      RETURN_NOT_OK(buffer.realloc(batch_copy.nbytes));
      RETURN_NOT_OK(
          read(uri_copy, batch_copy.offset, buffer.data(), batch_copy.nbytes));
      // Parallel copy back into the individual destinations.
      for (uint64_t i = 0; i < batch_copy.regions.size(); i++) {
        const auto& region = batch_copy.regions[i];
        uint64_t offset = std::get<0>(region);
        void* dest = std::get<1>(region);
        uint64_t nbytes = std::get<2>(region);
        std::memcpy(dest, buffer.data(offset - batch_copy.offset), nbytes);
      }

      return Status::Ok();
    });

    tasks->push_back(std::move(task));
  }

  return Status::Ok();

  STATS_FUNC_OUT(vfs_read_all);
}

Status VFS::compute_read_batches(
    const std::vector<std::tuple<uint64_t, void*, uint64_t>>& regions,
    std::vector<BatchedRead>* batches) const {
  // Get config params
  bool found;
  uint64_t min_batch_size = 0;
  RETURN_NOT_OK(
      config_.get<uint64_t>("vfs.min_batch_size", &min_batch_size, &found));
  assert(found);
  uint64_t min_batch_gap = 0;
  RETURN_NOT_OK(
      config_.get<uint64_t>("vfs.min_batch_gap", &min_batch_gap, &found));
  assert(found);

  // Ensure the regions are sorted on offset.
  std::vector<std::tuple<uint64_t, void*, uint64_t>> sorted_regions(
      regions.begin(), regions.end());
  parallel_sort(
      sorted_regions.begin(),
      sorted_regions.end(),
      [](const std::tuple<uint64_t, void*, uint64_t>& a,
         const std::tuple<uint64_t, void*, uint64_t>& b) {
        return std::get<0>(a) < std::get<0>(b);
      });

  // Start the first batch containing only the first region.
  BatchedRead curr_batch(sorted_regions.front());
  uint64_t curr_batch_useful_bytes = curr_batch.nbytes;
  for (uint64_t i = 1; i < sorted_regions.size(); i++) {
    const auto& region = sorted_regions[i];
    uint64_t offset = std::get<0>(region);
    uint64_t nbytes = std::get<2>(region);
    uint64_t new_batch_size = (offset + nbytes) - curr_batch.offset;
    uint64_t gap = offset - (curr_batch.offset + curr_batch.nbytes);
    if (new_batch_size <= min_batch_size || gap <= min_batch_gap) {
      // Extend current batch.
      curr_batch.nbytes = new_batch_size;
      curr_batch.regions.push_back(region);
      curr_batch_useful_bytes += nbytes;
    } else {
      // Push the old batch and start a new one.
      batches->push_back(curr_batch);
      curr_batch.offset = offset;
      curr_batch.nbytes = nbytes;
      curr_batch.regions.clear();
      curr_batch.regions.push_back(region);
      curr_batch_useful_bytes = nbytes;
    }
  }

  // Push the last batch
  batches->push_back(curr_batch);

  return Status::Ok();
}

bool VFS::supports_fs(Filesystem fs) const {
  STATS_FUNC_IN(vfs_supports_fs);

  return (supported_fs_.find(fs) != supported_fs_.end());

  STATS_FUNC_OUT(vfs_supports_fs);
}

bool VFS::supports_uri_scheme(const URI& uri) const {
  if (uri.is_s3()) {
    return supports_fs(Filesystem::S3);
  } else if (uri.is_hdfs()) {
    return supports_fs(Filesystem::HDFS);
  } else {
    return true;
  }
}

Status VFS::sync(const URI& uri) {
  STATS_FUNC_IN(vfs_sync);

  if (!init_)
    return LOG_STATUS(Status::VFSError("Cannot sync; VFS not initialized"));

  if (uri.is_file()) {
#ifdef _WIN32
    return win_.sync(uri.to_path());
#else
    return posix_.sync(uri.to_path());
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs_->sync(uri);
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

  STATS_FUNC_OUT(vfs_sync);
}

Status VFS::open_file(const URI& uri, VFSMode mode) {
  STATS_FUNC_IN(vfs_open_file);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot open file; VFS not initialized"));

  bool is_file;
  RETURN_NOT_OK(this->is_file(uri, &is_file));

  switch (mode) {
    case VFSMode::VFS_READ:
      if (!is_file)
        return LOG_STATUS(Status::VFSError(
            std::string("Cannot open file '") + uri.c_str() +
            "'; File does not exist"));
      break;
    case VFSMode::VFS_WRITE:
      if (is_file)
        RETURN_NOT_OK(remove_file(uri));
      break;
    case VFSMode::VFS_APPEND:
      if (uri.is_s3()) {
#ifdef HAVE_S3
        return LOG_STATUS(Status::VFSError(
            std::string("Cannot open file '") + uri.c_str() +
            "'; S3 does not support append mode"));
#else
        return LOG_STATUS(Status::VFSError(
            "Cannot open file; TileDB was built without S3 support"));
#endif
      }
      break;
  }

  return Status::Ok();

  STATS_FUNC_OUT(vfs_open_file);
}

Status VFS::close_file(const URI& uri) {
  STATS_FUNC_IN(vfs_close_file);

  if (!init_)
    return LOG_STATUS(
        Status::VFSError("Cannot close file; VFS not initialized"));

  if (uri.is_file()) {
#ifdef _WIN32
    return win_.sync(uri.to_path());
#else
    return posix_.sync(uri.to_path());
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs_->sync(uri);
#else
    return LOG_STATUS(
        Status::VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.flush_object(uri);
#else
    return LOG_STATUS(Status::VFSError("TileDB was built without S3 support"));
#endif
  }
  return LOG_STATUS(
      Status::VFSError("Unsupported URI schemes: " + uri.to_string()));

  STATS_FUNC_OUT(vfs_close_file);
}

Status VFS::write(const URI& uri, const void* buffer, uint64_t buffer_size) {
  STATS_FUNC_IN(vfs_write);

  if (!init_)
    return LOG_STATUS(Status::VFSError("Cannot write; VFS not initialized"));

  STATS_COUNTER_ADD(vfs_write_total_bytes, buffer_size);

  if (uri.is_file()) {
#ifdef _WIN32
    return win_.write(uri.to_path(), buffer, buffer_size);
#else
    return posix_.write(uri.to_path(), buffer, buffer_size);
#endif
  }
  if (uri.is_hdfs()) {
#ifdef HAVE_HDFS
    return hdfs_->write(uri, buffer, buffer_size);
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

  STATS_FUNC_OUT(vfs_write);
}

}  // namespace sm
}  // namespace tiledb
