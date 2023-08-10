/**
 * @file   vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
#include "path_win.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/filesystem.h"
#include "tiledb/sm/enums/vfs_mode.h"
#include "tiledb/sm/filesystem/hdfs_filesystem.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/tile/tile.h"

#include <iostream>
#include <list>
#include <sstream>
#include <unordered_map>

using namespace tiledb::common;
using tiledb::common::filesystem::directory_entry;

namespace tiledb::sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

VFS::VFS(
    stats::Stats* const parent_stats,
    ThreadPool* const compute_tp,
    ThreadPool* const io_tp,
    const Config& config)
    : VFSBase(parent_stats)
    , S3_within_VFS(stats_, io_tp, config)
    , config_(config)
    , compute_tp_(compute_tp)
    , io_tp_(io_tp)
    , vfs_params_(VFSParameters(config)) {
  Status st;
  assert(compute_tp);
  assert(io_tp);

  // Construct the read-ahead cache.
  read_ahead_cache_ = tdb_unique_ptr<ReadAheadCache>(
      tdb_new(ReadAheadCache, vfs_params_.read_ahead_cache_size_));

#ifdef HAVE_HDFS
  supported_fs_.insert(Filesystem::HDFS);
  hdfs_ = tdb_unique_ptr<hdfs::HDFS>(tdb_new(hdfs::HDFS));
  st = hdfs_->init(config_);
  if (!st.ok()) {
    throw std::runtime_error("[VFS::VFS] Failed to initialize HDFS backend.");
  }
#endif

#ifdef HAVE_S3
  supported_fs_.insert(Filesystem::S3);
#endif

#ifdef HAVE_AZURE
  supported_fs_.insert(Filesystem::AZURE);
  st = azure_.init(config_, io_tp_);
  if (!st.ok()) {
    throw std::runtime_error("[VFS::VFS] Failed to initialize Azure backend.");
  }
#endif

#ifdef HAVE_GCS
  supported_fs_.insert(Filesystem::GCS);
  st = gcs_.init(config_, io_tp_);
  if (!st.ok()) {
    throw std::runtime_error("[VFS::VFS] Failed to initialize GCS backend.");
  }
#endif

#ifdef _WIN32
  throw_if_not_ok(win_.init(config_));
#else
  throw_if_not_ok(posix_.init(config_));
#endif

  supported_fs_.insert(Filesystem::MEMFS);
}

/* ********************************* */
/*                API                */
/* ********************************* */

std::string VFS::abs_path(const std::string& path) {
  // workaround for older clang (llvm 3.5) compilers (issue #828)
  std::string path_copy = path;
#ifdef _WIN32
  {
    std::string norm_sep_path = path_win::slashes_to_backslashes(path);
    if (path_win::is_win_path(norm_sep_path))
      return path_win::uri_from_path(Win::abs_path(norm_sep_path));
    else if (URI::is_file(path))
      return path_win::uri_from_path(
          Win::abs_path(path_win::path_from_uri(path)));
  }
#else
  if (URI::is_file(path))
    return Posix::abs_path(path);
#endif
  if (URI::is_hdfs(path))
    return path_copy;
  if (URI::is_s3(path))
    return path_copy;
  if (URI::is_azure(path))
    return path_copy;
  if (URI::is_gcs(path))
    return path_copy;
  if (URI::is_memfs(path))
    return path_copy;
  // Certainly starts with "<resource>://" other than "file://"
  return path_copy;
}

Config VFS::config() const {
  return config_;
}

Status VFS::create_dir(const URI& uri) const {
  if (!uri.is_s3() && !uri.is_azure() && !uri.is_gcs()) {
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
    return LOG_STATUS(Status_VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    // It is a noop for S3
    return Status::Ok();
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    // It is a noop for Azure
    return Status::Ok();
#else
    return LOG_STATUS(
        Status_VFSError("TileDB was built without Azure support"));
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    // It is a noop for GCS
    return Status::Ok();
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without GCS support"));
#endif
  }
  if (uri.is_memfs()) {
    return memfs_.create_dir(uri.to_path());
  }
  return LOG_STATUS(Status_VFSError(
      std::string("Unsupported URI scheme: ") + uri.to_string()));
}

Status VFS::dir_size(const URI& dir_name, uint64_t* dir_size) const {
  // Sanity check
  bool is_dir;
  RETURN_NOT_OK(this->is_dir(dir_name, &is_dir));
  if (!is_dir)
    return LOG_STATUS(Status_VFSError(
        std::string("Cannot get directory size; Input '") +
        dir_name.to_string() + "' is not a directory"));

  // Get all files in the tree rooted at `dir_name` and add their sizes
  *dir_size = 0;
  std::list<URI> to_ls;
  to_ls.push_front(dir_name);
  do {
    auto uri = to_ls.front();
    to_ls.pop_front();
    auto&& [st, children] = ls_with_sizes(uri);
    RETURN_NOT_OK(st);

    for (const auto& child : *children) {
      if (!child.is_directory()) {
        *dir_size += child.file_size();
      } else {
        to_ls.push_back(URI(child.path().native()));
      }
    }
  } while (!to_ls.empty());

  return Status::Ok();
}

Status VFS::touch(const URI& uri) const {
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
    return LOG_STATUS(Status_VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.touch(uri);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return azure_.touch(uri);
#else
    return LOG_STATUS(
        Status_VFSError("TileDB was built without Azure support"));
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs_.touch(uri);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without GCS support"));
#endif
  }
  if (uri.is_memfs()) {
    return memfs_.touch(uri.to_path());
  }
  return LOG_STATUS(Status_VFSError(
      std::string("Unsupported URI scheme: ") + uri.to_string()));
}

Status VFS::cancel_all_tasks() {
  cancelable_tasks_.cancel_all_tasks();
  return Status::Ok();
}

Status VFS::create_bucket(const URI& uri) const {
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.create_bucket(uri);
#else
    (void)uri;
    return LOG_STATUS(Status_VFSError(std::string("S3 is not supported")));
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return azure_.create_container(uri);
#else
    (void)uri;
    return LOG_STATUS(Status_VFSError(std::string("Azure is not supported")));
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs_.create_bucket(uri);
#else
    (void)uri;
    return LOG_STATUS(Status_VFSError(std::string("GCS is not supported")));
#endif
  }
  return LOG_STATUS(Status_VFSError(
      std::string("Cannot create bucket; Unsupported URI scheme: ") +
      uri.to_string()));
}

Status VFS::remove_bucket(const URI& uri) const {
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.remove_bucket(uri);
#else
    (void)uri;
    return LOG_STATUS(Status_VFSError(std::string("S3 is not supported")));
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return azure_.remove_container(uri);
#else
    (void)uri;
    return LOG_STATUS(Status_VFSError(std::string("Azure is not supported")));
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs_.remove_bucket(uri);
#else
    (void)uri;
    return LOG_STATUS(Status_VFSError(std::string("GCS is not supported")));
#endif
  }
  return LOG_STATUS(Status_VFSError(
      std::string("Cannot remove bucket; Unsupported URI scheme: ") +
      uri.to_string()));
}

Status VFS::empty_bucket(const URI& uri) const {
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.empty_bucket(uri);
#else
    (void)uri;
    return LOG_STATUS(Status_VFSError(std::string("S3 is not supported")));
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return azure_.empty_container(uri);
#else
    (void)uri;
    return LOG_STATUS(Status_VFSError(std::string("Azure is not supported")));
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs_.empty_bucket(uri);
#else
    (void)uri;
    return LOG_STATUS(Status_VFSError(std::string("GCS is not supported")));
#endif
  }
  return LOG_STATUS(Status_VFSError(
      std::string("Cannot empty bucket; Unsupported URI scheme: ") +
      uri.to_string()));
}

Status VFS::is_empty_bucket(const URI& uri, bool* is_empty) const {
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.is_empty_bucket(uri, is_empty);
#else
    (void)uri;
    (void)is_empty;
    return LOG_STATUS(Status_VFSError(std::string("S3 is not supported")));
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return azure_.is_empty_container(uri, is_empty);
#else
    (void)uri;
    (void)is_empty;
    return LOG_STATUS(Status_VFSError(std::string("Azure is not supported")));
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs_.is_empty_bucket(uri, is_empty);
#else
    (void)uri;
    (void)is_empty;
    return LOG_STATUS(Status_VFSError(std::string("GCS is not supported")));
#endif
  }
  return LOG_STATUS(Status_VFSError(
      std::string("Cannot remove bucket; Unsupported URI scheme: ") +
      uri.to_string()));
}

Status VFS::remove_dir(const URI& uri) const {
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
    return LOG_STATUS(Status_VFSError("TileDB was built without HDFS support"));
#endif
  } else if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.remove_dir(uri);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
  } else if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return azure_.remove_dir(uri);
#else
    return LOG_STATUS(
        Status_VFSError("TileDB was built without Azure support"));
#endif
  } else if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs_.remove_dir(uri);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without GCS support"));
#endif
  } else if (uri.is_memfs()) {
    return memfs_.remove(uri.to_path(), true);
  } else {
    return LOG_STATUS(
        Status_VFSError("Unsupported URI scheme: " + uri.to_string()));
  }
}

void VFS::remove_dirs(
    ThreadPool* compute_tp, const std::vector<URI>& uris) const {
  throw_if_not_ok(parallel_for(compute_tp, 0, uris.size(), [&](size_t i) {
    bool is_dir;
    RETURN_NOT_OK(this->is_dir(uris[i], &is_dir));
    if (is_dir) {
      RETURN_NOT_OK(remove_dir(uris[i]));
    }
    return Status::Ok();
  }));
}

Status VFS::remove_file(const URI& uri) const {
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
    return LOG_STATUS(Status_VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.remove_object(uri);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return azure_.remove_blob(uri);
#else
    return LOG_STATUS(
        Status_VFSError("TileDB was built without Azure support"));
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs_.remove_object(uri);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without GCS support"));
#endif
  }
  if (uri.is_memfs()) {
    return memfs_.remove(uri.to_path(), false);
  }
  return LOG_STATUS(
      Status_VFSError("Unsupported URI scheme: " + uri.to_string()));
}

void VFS::remove_files(
    ThreadPool* compute_tp, const std::vector<URI>& uris) const {
  throw_if_not_ok(parallel_for(compute_tp, 0, uris.size(), [&](size_t i) {
    RETURN_NOT_OK(remove_file(uris[i]));
    return Status::Ok();
  }));
}

void VFS::remove_files(
    ThreadPool* compute_tp, const std::vector<TimestampedURI>& uris) const {
  throw_if_not_ok(parallel_for(compute_tp, 0, uris.size(), [&](size_t i) {
    RETURN_NOT_OK(remove_file(uris[i].uri_));
    return Status::Ok();
  }));
}

Status VFS::max_parallel_ops(const URI& uri, uint64_t* ops) const {
  bool found;
  *ops = 0;

  if (uri.is_hdfs()) {
    // HDFS backend is currently serial.
    *ops = 1;
  } else if (uri.is_s3()) {
    RETURN_NOT_OK(
        config_.get<uint64_t>("vfs.s3.max_parallel_ops", ops, &found));
    assert(found);
  } else if (uri.is_azure()) {
    RETURN_NOT_OK(
        config_.get<uint64_t>("vfs.azure.max_parallel_ops", ops, &found));
    assert(found);
  } else if (uri.is_gcs()) {
    RETURN_NOT_OK(
        config_.get<uint64_t>("vfs.gcs.max_parallel_ops", ops, &found));
    assert(found);
  } else {
    *ops = 1;
  }

  return Status::Ok();
}

Status VFS::file_size(const URI& uri, uint64_t* size) const {
  stats_->add_counter("file_size_num", 1);
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
    return LOG_STATUS(Status_VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.object_size(uri, size);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return azure_.blob_size(uri, size);
#else
    return LOG_STATUS(
        Status_VFSError("TileDB was built without Azure support"));
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs_.object_size(uri, size);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without GCS support"));
#endif
  }
  if (uri.is_memfs()) {
    return memfs_.file_size(uri.to_path(), size);
  }
  return LOG_STATUS(
      Status_VFSError("Unsupported URI scheme: " + uri.to_string()));
}

Status VFS::is_dir(const URI& uri, bool* is_dir) const {
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
    return LOG_STATUS(Status_VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.is_dir(uri, is_dir);
#else
    *is_dir = false;
    return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return azure_.is_dir(uri, is_dir);
#else
    *is_dir = false;
    return LOG_STATUS(
        Status_VFSError("TileDB was built without Azure support"));
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs_.is_dir(uri, is_dir);
#else
    *is_dir = false;
    return LOG_STATUS(Status_VFSError("TileDB was built without GCS support"));
#endif
  }
  if (uri.is_memfs()) {
    *is_dir = memfs_.is_dir(uri.to_path());
    return Status::Ok();
  }
  return LOG_STATUS(
      Status_VFSError("Unsupported URI scheme: " + uri.to_string()));
}

Status VFS::is_file(const URI& uri, bool* is_file) const {
  stats_->add_counter("is_object_num", 1);
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
    return LOG_STATUS(Status_VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    RETURN_NOT_OK(s3_.is_object(uri, is_file));
    return Status::Ok();
#else
    *is_file = false;
    return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return azure_.is_blob(uri, is_file);
#else
    *is_file = false;
    return LOG_STATUS(
        Status_VFSError("TileDB was built without Azure support"));
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs_.is_object(uri, is_file);
#else
    *is_file = false;
    return LOG_STATUS(Status_VFSError("TileDB was built without GCS support"));
#endif
  }
  if (uri.is_memfs()) {
    *is_file = memfs_.is_file(uri.to_path());
    return Status::Ok();
  }
  return LOG_STATUS(
      Status_VFSError("Unsupported URI scheme: " + uri.to_string()));
}

Status VFS::is_bucket(const URI& uri, bool* is_bucket) const {
  if (uri.is_s3()) {
#ifdef HAVE_S3
    RETURN_NOT_OK(s3_.is_bucket(uri, is_bucket));
    return Status::Ok();
#else
    *is_bucket = false;
    return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    RETURN_NOT_OK(azure_.is_container(uri, is_bucket));
    return Status::Ok();
#else
    *is_bucket = false;
    return LOG_STATUS(
        Status_VFSError("TileDB was built without Azure support"));
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    RETURN_NOT_OK(gcs_.is_bucket(uri, is_bucket));
    return Status::Ok();
#else
    *is_bucket = false;
    return LOG_STATUS(Status_VFSError("TileDB was built without GCS support"));
#endif
  }

  return LOG_STATUS(
      Status_VFSError("Unsupported URI scheme: " + uri.to_string()));
}

Status VFS::ls(const URI& parent, std::vector<URI>* uris) const {
  stats_->add_counter("ls_num", 1);
  auto&& [st, entries] = ls_with_sizes(parent);
  RETURN_NOT_OK(st);

  for (auto& fs : *entries) {
    uris->emplace_back(fs.path().native());
  }

  return Status::Ok();
}

void VFS::ls_recursive(
    const URI& parent, std::vector<URI>* uris, int64_t max_paths) const {
  Status st;
  optional<std::vector<directory_entry>> entries;

  if (parent.is_file() || parent.is_memfs()) {
    std::queue<URI> q;
    q.push(parent);
    bool done = false;
    while (!q.empty() && !done) {
      if (parent.is_memfs()) {
        std::tie(st, entries) = memfs_.ls_with_sizes(q.front());
      } else if (parent.is_file()) {
#ifdef _WIN32
        std::tie(st, entries) = win_.ls_with_sizes(q.front());
#else
        std::tie(st, entries) = posix_.ls_with_sizes(q.front());
#endif
      }
      throw_if_not_ok(st);
      // Sort the entries to avoid strange collections when pruned by max_paths.
      parallel_sort(
          io_tp_,
          entries->begin(),
          entries->end(),
          [](const directory_entry& l, const directory_entry& r) {
            return l.path().native() < r.path().native();
          });
      for (const auto& result : *entries) {
        if (result.is_directory()) {
          q.emplace(result.path().native());
        }

        uris->emplace_back(result.path().native());
        if (static_cast<int64_t>(uris->size()) == max_paths) {
          done = true;
          break;
        }
      }
      q.pop();
    }
  } else if (parent.is_s3()) {
#ifdef HAVE_S3
    std::tie(st, entries) = s3_.ls_with_sizes(parent, "", max_paths);
    throw_if_not_ok(st);
#else
    throw VFSStatusException("TileDB was built without S3 support");
#endif
  } else {
    throw VFSStatusException(
        "Recursive ls over " + parent.backend_name() +
        " storage backend is not supported.");
  }

  // LocalFS, MemFS results were sorted during traversal.
  if (!parent.is_file() && !parent.is_memfs()) {
    parallel_sort(
        compute_tp_,
        entries->begin(),
        entries->end(),
        [](const directory_entry& l, const directory_entry& r) {
          return l.path().native() < r.path().native();
        });
    for (auto& fs : *entries) {
      uris->emplace_back(fs.path().native());
    }
  }
}

tuple<Status, optional<std::vector<directory_entry>>> VFS::ls_with_sizes(
    const URI& parent) const {
  // Noop if `parent` is not a directory, do not error out.
  // For S3, GCS and Azure, `ls` on a non-directory will just
  // return an empty `uris` vector.
  if (!(parent.is_s3() || parent.is_gcs() || parent.is_azure())) {
    bool flag = false;
    RETURN_NOT_OK_TUPLE(is_dir(parent, &flag), nullopt);

    if (!flag) {
      return {Status::Ok(), std::vector<directory_entry>()};
    }
  }

  optional<std::vector<directory_entry>> entries;
  if (parent.is_file()) {
#ifdef _WIN32
    Status st;
    std::tie(st, entries) = win_.ls_with_sizes(parent);
#else
    Status st;
    std::tie(st, entries) = posix_.ls_with_sizes(parent);
#endif
    RETURN_NOT_OK_TUPLE(st, nullopt);
  } else if (parent.is_s3()) {
#ifdef HAVE_S3
    Status st;
    std::tie(st, entries) = s3_.ls_with_sizes(parent);
#else
    auto st =
        LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
    RETURN_NOT_OK_TUPLE(st, nullopt);
  } else if (parent.is_azure()) {
#ifdef HAVE_AZURE
    Status st;
    std::tie(st, entries) = azure_.ls_with_sizes(parent);
#else
    auto st =
        LOG_STATUS(Status_VFSError("TileDB was built without Azure support"));
#endif
    RETURN_NOT_OK_TUPLE(st, nullopt);
  } else if (parent.is_gcs()) {
#ifdef HAVE_GCS
    Status st;
    std::tie(st, entries) = gcs_.ls_with_sizes(parent);
#else
    auto st =
        LOG_STATUS(Status_VFSError("TileDB was built without GCS support"));
#endif
    RETURN_NOT_OK_TUPLE(st, nullopt);
  } else if (parent.is_hdfs()) {
#ifdef HAVE_HDFS
    Status st;
    std::tie(st, entries) = hdfs_->ls_with_sizes(parent);
#else
    auto st =
        LOG_STATUS(Status_VFSError("TileDB was built without HDFS support"));
#endif
    RETURN_NOT_OK_TUPLE(st, nullopt);
  } else if (parent.is_memfs()) {
    Status st;
    std::tie(st, entries) =
        memfs_.ls_with_sizes(URI("mem://" + parent.to_path()));
    RETURN_NOT_OK_TUPLE(st, nullopt);
  } else {
    auto st = LOG_STATUS(
        Status_VFSError("Unsupported URI scheme: " + parent.to_string()));
    return {st, std::nullopt};
  }
  parallel_sort(
      compute_tp_,
      entries->begin(),
      entries->end(),
      [](const directory_entry& l, const directory_entry& r) {
        return l.path().native() < r.path().native();
      });

  return {Status::Ok(), entries};
}

Status VFS::move_file(const URI& old_uri, const URI& new_uri) {
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
    return LOG_STATUS(Status_VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // HDFS
  if (old_uri.is_hdfs()) {
    if (new_uri.is_hdfs())
#ifdef HAVE_HDFS
      return hdfs_->move_path(old_uri, new_uri);
#else
      return LOG_STATUS(
          Status_VFSError("TileDB was built without HDFS support"));
#endif
    return LOG_STATUS(Status_VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // S3
  if (old_uri.is_s3()) {
    if (new_uri.is_s3())
#ifdef HAVE_S3
      return s3_.move_object(old_uri, new_uri);
#else
      return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
    return LOG_STATUS(Status_VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // Azure
  if (old_uri.is_azure()) {
    if (new_uri.is_azure())
#ifdef HAVE_AZURE
      return azure_.move_object(old_uri, new_uri);
#else
      return LOG_STATUS(
          Status_VFSError("TileDB was built without Azure support"));
#endif
    return LOG_STATUS(Status_VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // GCS
  if (old_uri.is_gcs()) {
    if (new_uri.is_gcs())
#ifdef HAVE_GCS
      return gcs_.move_object(old_uri, new_uri);
#else
      return LOG_STATUS(
          Status_VFSError("TileDB was built without GCS support"));
#endif
    return LOG_STATUS(Status_VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // In-memory filesystem
  if (old_uri.is_memfs()) {
    if (new_uri.is_memfs()) {
      return memfs_.move(old_uri.to_path(), new_uri.to_path());
    }
    return LOG_STATUS(Status_VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // Unsupported filesystem
  return LOG_STATUS(Status_VFSError(
      "Unsupported URI schemes: " + old_uri.to_string() + ", " +
      new_uri.to_string()));
}

Status VFS::move_dir(const URI& old_uri, const URI& new_uri) {
  // File
  if (old_uri.is_file()) {
    if (new_uri.is_file()) {
#ifdef _WIN32
      return win_.move_path(old_uri.to_path(), new_uri.to_path());
#else
      return posix_.move_path(old_uri.to_path(), new_uri.to_path());
#endif
    }
    return LOG_STATUS(Status_VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // HDFS
  if (old_uri.is_hdfs()) {
    if (new_uri.is_hdfs())
#ifdef HAVE_HDFS
      return hdfs_->move_path(old_uri, new_uri);
#else
      return LOG_STATUS(
          Status_VFSError("TileDB was built without HDFS support"));
#endif
    return LOG_STATUS(Status_VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // S3
  if (old_uri.is_s3()) {
    if (new_uri.is_s3())
#ifdef HAVE_S3
      return s3_.move_dir(old_uri, new_uri);
#else
      return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
    return LOG_STATUS(Status_VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // Azure
  if (old_uri.is_azure()) {
    if (new_uri.is_azure())
#ifdef HAVE_AZURE
      return azure_.move_dir(old_uri, new_uri);
#else
      return LOG_STATUS(
          Status_VFSError("TileDB was built without Azure support"));
#endif
    return LOG_STATUS(Status_VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // GCS
  if (old_uri.is_gcs()) {
    if (new_uri.is_gcs())
#ifdef HAVE_GCS
      return gcs_.move_dir(old_uri, new_uri);
#else
      return LOG_STATUS(
          Status_VFSError("TileDB was built without GCS support"));
#endif
    return LOG_STATUS(Status_VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // In-memory filesystem
  if (old_uri.is_memfs()) {
    if (new_uri.is_memfs()) {
      return memfs_.move(old_uri.to_path(), new_uri.to_path());
    }
    return LOG_STATUS(Status_VFSError(
        "Moving files across filesystems is not supported yet"));
  }

  // Unsupported filesystem
  return LOG_STATUS(Status_VFSError(
      "Unsupported URI schemes: " + old_uri.to_string() + ", " +
      new_uri.to_string()));
}

Status VFS::copy_file(const URI& old_uri, const URI& new_uri) {
  // If new_uri exists, delete it or raise an error based on `force`
  bool is_file;
  RETURN_NOT_OK(this->is_file(new_uri, &is_file));
  if (is_file)
    RETURN_NOT_OK(remove_file(new_uri));

  // File
  if (old_uri.is_file()) {
    if (new_uri.is_file()) {
#ifdef _WIN32
      return LOG_STATUS(Status_IOError(
          std::string("Copying files on Windows is not yet supported.")));
#else
      return posix_.copy_file(old_uri.to_path(), new_uri.to_path());
#endif
    }
    return LOG_STATUS(Status_VFSError(
        "Copying files across filesystems is not supported yet"));
  }

  // HDFS
  if (old_uri.is_hdfs()) {
    if (new_uri.is_hdfs())
#ifdef HAVE_HDFS
      return LOG_STATUS(Status_IOError(
          std::string("Copying files on HDFS is not yet supported.")));
#else
      return LOG_STATUS(
          Status_VFSError("TileDB was built without HDFS support"));
#endif
    return LOG_STATUS(Status_VFSError(
        "Copying files across filesystems is not supported yet"));
  }

  // S3
  if (old_uri.is_s3()) {
    if (new_uri.is_s3())
#ifdef HAVE_S3
      return s3_.copy_file(old_uri, new_uri);
#else
      return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
    return LOG_STATUS(Status_VFSError(
        "Copying files across filesystems is not supported yet"));
  }

  // Azure
  if (old_uri.is_azure()) {
    if (new_uri.is_azure())
#ifdef HAVE_AZURE
      return LOG_STATUS(Status_IOError(
          std::string("Copying files on Azure is not yet supported.")));
#else
      return LOG_STATUS(
          Status_VFSError("TileDB was built without Azure support"));
#endif
    return LOG_STATUS(Status_VFSError(
        "Copying files across filesystems is not supported yet"));
  }

  // GCS
  if (old_uri.is_gcs()) {
    if (new_uri.is_gcs())
#ifdef HAVE_GCS
      return LOG_STATUS(Status_IOError(
          std::string("Copying files on GCS is not yet supported.")));
#else
      return LOG_STATUS(
          Status_VFSError("TileDB was built without GCS support"));
#endif
    return LOG_STATUS(Status_VFSError(
        "Copying files across filesystems is not supported yet"));
  }

  // Unsupported filesystem
  return LOG_STATUS(Status_VFSError(
      "Unsupported URI schemes: " + old_uri.to_string() + ", " +
      new_uri.to_string()));
}

Status VFS::copy_dir(const URI& old_uri, const URI& new_uri) {
  // File
  if (old_uri.is_file()) {
    if (new_uri.is_file()) {
#ifdef _WIN32
      return LOG_STATUS(Status_IOError(
          std::string("Copying directories on Windows is not yet supported.")));
#else
      return posix_.copy_dir(old_uri.to_path(), new_uri.to_path());
#endif
    }
    return LOG_STATUS(Status_VFSError(
        "Copying directories across filesystems is not supported yet"));
  }

  // HDFS
  if (old_uri.is_hdfs()) {
    if (new_uri.is_hdfs())
#ifdef HAVE_HDFS
      return LOG_STATUS(Status_IOError(
          std::string("Copying directories on HDFS is not yet supported.")));
#else
      return LOG_STATUS(
          Status_VFSError("TileDB was built without HDFS support"));
#endif
    return LOG_STATUS(Status_VFSError(
        "Copying directories across filesystems is not supported yet"));
  }

  // S3
  if (old_uri.is_s3()) {
    if (new_uri.is_s3())
#ifdef HAVE_S3
      return s3_.copy_dir(old_uri, new_uri);
#else
      return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
    return LOG_STATUS(Status_VFSError(
        "Copying directories across filesystems is not supported yet"));
  }

  // Azure
  if (old_uri.is_azure()) {
    if (new_uri.is_azure())
#ifdef HAVE_AZURE
      return LOG_STATUS(Status_IOError(
          std::string("Copying directories on Azure is not yet supported.")));
#else
      return LOG_STATUS(
          Status_VFSError("TileDB was built without Azure support"));
#endif
    return LOG_STATUS(Status_VFSError(
        "Copying directories across filesystems is not supported yet"));
  }

  // GCS
  if (old_uri.is_gcs()) {
    if (new_uri.is_gcs())
#ifdef HAVE_GCS
      return LOG_STATUS(Status_IOError(
          std::string("Copying directories on GCS is not yet supported.")));
#else
      return LOG_STATUS(
          Status_VFSError("TileDB was built without GCS support"));
#endif
    return LOG_STATUS(Status_VFSError(
        "Copying directories across filesystems is not supported yet"));
  }

  // Unsupported filesystem
  return LOG_STATUS(Status_VFSError(
      "Unsupported URI schemes: " + old_uri.to_string() + ", " +
      new_uri.to_string()));
}

Status VFS::read(
    const URI& uri,
    const uint64_t offset,
    void* const buffer,
    const uint64_t nbytes,
    bool use_read_ahead) {
  stats_->add_counter("read_byte_num", nbytes);

  // Get config params
  uint64_t min_parallel_size = vfs_params_.min_parallel_size_;
  uint64_t max_ops = 0;
  RETURN_NOT_OK(max_parallel_ops(uri, &max_ops));

  // Ensure that each thread is responsible for at least min_parallel_size
  // bytes, and cap the number of parallel operations at the configured maximum
  // number.
  uint64_t num_ops =
      std::min(std::max(nbytes / min_parallel_size, uint64_t(1)), max_ops);

  if (num_ops == 1) {
    return read_impl(uri, offset, buffer, nbytes, use_read_ahead);
  } else {
    // we don't want read-ahead when performing random access reads
    use_read_ahead = false;
    std::vector<ThreadPool::Task> results;
    uint64_t thread_read_nbytes = utils::math::ceil(nbytes, num_ops);

    for (uint64_t i = 0; i < num_ops; i++) {
      uint64_t begin = i * thread_read_nbytes,
               end = std::min((i + 1) * thread_read_nbytes - 1, nbytes - 1);
      uint64_t thread_nbytes = end - begin + 1;
      uint64_t thread_offset = offset + begin;
      auto thread_buffer = reinterpret_cast<char*>(buffer) + begin;
      auto task = cancelable_tasks_.execute(
          io_tp_,
          [this,
           uri,
           thread_offset,
           thread_buffer,
           thread_nbytes,
           use_read_ahead]() {
            return read_impl(
                uri,
                thread_offset,
                thread_buffer,
                thread_nbytes,
                use_read_ahead);
          });
      results.push_back(std::move(task));
    }
    Status st = io_tp_->wait_all(results);
    if (!st.ok()) {
      std::stringstream errmsg;
      errmsg << "VFS parallel read error '" << uri.to_string() << "'; "
             << st.message();
      return LOG_STATUS(Status_VFSError(errmsg.str()));
    }
    return st;
  }
}

Status VFS::read_impl(
    const URI& uri,
    const uint64_t offset,
    void* const buffer,
    const uint64_t nbytes,
    const bool use_read_ahead) {
  stats_->add_counter("read_ops_num", 1);

  // We only check to use the read-ahead cache for cloud-storage
  // backends. No-op the `use_read_ahead` to prevent the unused
  // variable compiler warning.
  (void)use_read_ahead;

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
    return LOG_STATUS(Status_VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    const auto read_fn = std::bind(
        &S3::read,
        &s3_,
        std::placeholders::_1,
        std::placeholders::_2,
        std::placeholders::_3,
        std::placeholders::_4,
        std::placeholders::_5,
        std::placeholders::_6);
    return read_ahead_impl(
        read_fn, uri, offset, buffer, nbytes, use_read_ahead);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    const auto read_fn = std::bind(
        &Azure::read,
        &azure_,
        std::placeholders::_1,
        std::placeholders::_2,
        std::placeholders::_3,
        std::placeholders::_4,
        std::placeholders::_5,
        std::placeholders::_6);
    return read_ahead_impl(
        read_fn, uri, offset, buffer, nbytes, use_read_ahead);
#else
    return LOG_STATUS(
        Status_VFSError("TileDB was built without Azure support"));
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    const auto read_fn = std::bind(
        &GCS::read,
        &gcs_,
        std::placeholders::_1,
        std::placeholders::_2,
        std::placeholders::_3,
        std::placeholders::_4,
        std::placeholders::_5,
        std::placeholders::_6);
    return read_ahead_impl(
        read_fn, uri, offset, buffer, nbytes, use_read_ahead);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without GCS support"));
#endif
  }
  if (uri.is_memfs()) {
    return memfs_.read(uri.to_path(), offset, buffer, nbytes);
  }

  return LOG_STATUS(
      Status_VFSError("Unsupported URI schemes: " + uri.to_string()));
}

Status VFS::read_ahead_impl(
    const std::function<Status(
        const URI&, off_t, void*, uint64_t, uint64_t, uint64_t*)>& read_fn,
    const URI& uri,
    const uint64_t offset,
    void* const buffer,
    const uint64_t nbytes,
    const bool use_read_ahead) {
  // Stores the total number of bytes read.
  uint64_t nbytes_read = 0;

  // Do not use the read-ahead cache if disabled by the caller.
  if (!use_read_ahead)
    return read_fn(uri, offset, buffer, nbytes, 0, &nbytes_read);

  // Only perform a read-ahead if the requested read size
  // is smaller than the size of the buffers in the read-ahead
  // cache. This is because:
  // 1. The read-ahead is primarily beneficial for IO patterns
  //    that consist of numerous small reads.
  // 2. Large reads may evict cached buffers that would be useful
  //    to a future small read.
  // 3. It saves us a copy. We must make a copy of the buffer at
  //    some point (one for the user, one for the cache).
  if (nbytes >= vfs_params_.read_ahead_size_)
    return read_fn(uri, offset, buffer, nbytes, 0, &nbytes_read);

  // Avoid a read if the requested buffer can be read from the
  // read cache. Note that we intentionally do not use a read
  // cache for local files because we rely on the operating
  // system's file system to cache readahead data in memory.
  // Additionally, we do not perform readahead with HDFS.
  bool success;
  RETURN_NOT_OK(read_ahead_cache_->read(uri, offset, buffer, nbytes, &success));
  if (success)
    return Status::Ok();

  // We will read directly into the read-ahead buffer and then copy
  // the subrange of this buffer back to the user to satisfy the
  // read request.
  Buffer ra_buffer;
  RETURN_NOT_OK(ra_buffer.realloc(vfs_params_.read_ahead_size_));

  // Calculate the exact number of bytes to populate `ra_buffer`
  // with `vfs_params_.read_ahead_size_` bytes.
  const uint64_t ra_nbytes = vfs_params_.read_ahead_size_ - nbytes;

  // Read into `ra_buffer`.
  RETURN_NOT_OK(
      read_fn(uri, offset, ra_buffer.data(), nbytes, ra_nbytes, &nbytes_read));

  // Copy the requested read range back into the caller's output `buffer`.
  assert(nbytes_read >= nbytes);
  std::memcpy(buffer, ra_buffer.data(), nbytes);

  // Cache `ra_buffer` at `offset`.
  ra_buffer.set_size(nbytes_read);
  RETURN_NOT_OK(read_ahead_cache_->insert(uri, offset, std::move(ra_buffer)));

  return Status::Ok();
}

bool VFS::supports_fs(Filesystem fs) const {
  return (supported_fs_.find(fs) != supported_fs_.end());
}

bool VFS::supports_uri_scheme(const URI& uri) const {
  if (uri.is_s3()) {
    return supports_fs(Filesystem::S3);
  } else if (uri.is_azure()) {
    return supports_fs(Filesystem::AZURE);
  } else if (uri.is_gcs()) {
    return supports_fs(Filesystem::GCS);
  } else if (uri.is_hdfs()) {
    return supports_fs(Filesystem::HDFS);
  } else {
    return true;
  }
}

Status VFS::sync(const URI& uri) {
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
    return LOG_STATUS(Status_VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return Status::Ok();
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return Status::Ok();
#else
    return LOG_STATUS(
        Status_VFSError("TileDB was built without Azure support"));
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return Status::Ok();
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without GCS support"));
#endif
  }
  if (uri.is_memfs()) {
    return Status::Ok();
  }
  return LOG_STATUS(
      Status_VFSError("Unsupported URI scheme: " + uri.to_string()));
}

Status VFS::open_file(const URI& uri, VFSMode mode) {
  bool is_file;
  RETURN_NOT_OK(this->is_file(uri, &is_file));

  switch (mode) {
    case VFSMode::VFS_READ:
      if (!is_file)
        return LOG_STATUS(Status_VFSError(
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
        return LOG_STATUS(Status_VFSError(
            std::string("Cannot open file '") + uri.c_str() +
            "'; S3 does not support append mode"));
#else
        return LOG_STATUS(Status_VFSError(
            "Cannot open file; TileDB was built without S3 support"));
#endif
      }
      if (uri.is_azure()) {
#ifdef HAVE_AZURE
        return LOG_STATUS(Status_VFSError(
            std::string("Cannot open file '") + uri.c_str() +
            "'; Azure does not support append mode"));
#else
        return LOG_STATUS(Status_VFSError(
            "Cannot open file; TileDB was built without Azure support"));
#endif
      }
      if (uri.is_gcs()) {
#ifdef HAVE_GCS
        return LOG_STATUS(Status_VFSError(
            std::string("Cannot open file '") + uri.c_str() +
            "'; GCS does not support append mode"));
#else
        return LOG_STATUS(Status_VFSError(
            "Cannot open file; TileDB was built without GCS support"));
#endif
      }
      break;
  }

  return Status::Ok();
}

Status VFS::close_file(const URI& uri) {
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
    return LOG_STATUS(Status_VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3_.flush_object(uri);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return azure_.flush_blob(uri);
#else
    return LOG_STATUS(
        Status_VFSError("TileDB was built without Azure support"));
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs_.flush_object(uri);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without GCS support"));
#endif
  }
  if (uri.is_memfs()) {
    return Status::Ok();
  }
  return LOG_STATUS(
      Status_VFSError("Unsupported URI schemes: " + uri.to_string()));
}

void VFS::finalize_and_close_file(const URI& uri) {
  if (uri.is_s3()) {
#ifdef HAVE_S3
    s3_.finalize_and_flush_object(uri);
    return;
#else
    throw StatusException(
        Status_VFSError("TileDB was built without S3 support"));
#endif
  }
  throw_if_not_ok(close_file(uri));
}

Status VFS::write(
    const URI& uri,
    const void* buffer,
    uint64_t buffer_size,
    bool remote_global_order_write) {
  stats_->add_counter("write_byte_num", buffer_size);
  stats_->add_counter("write_ops_num", 1);

  (void)remote_global_order_write;

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
    return LOG_STATUS(Status_VFSError("TileDB was built without HDFS support"));
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    if (remote_global_order_write) {
      s3_.global_order_write_buffered(uri, buffer, buffer_size);
      return Status::Ok();
    }
    return s3_.write(uri, buffer, buffer_size);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return azure_.write(uri, buffer, buffer_size);
#else
    return LOG_STATUS(
        Status_VFSError("TileDB was built without Azure support"));
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs_.write(uri, buffer, buffer_size);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without GCS support"));
#endif
  }
  if (uri.is_memfs()) {
    return memfs_.write(uri.to_path(), buffer, buffer_size);
  }
  return LOG_STATUS(
      Status_VFSError("Unsupported URI schemes: " + uri.to_string()));
}

std::pair<Status, std::optional<VFS::MultiPartUploadState>>
VFS::multipart_upload_state(const URI& uri) {
  if (uri.is_file()) {
    return {Status::Ok(), {}};
  } else if (uri.is_s3()) {
#ifdef HAVE_S3
    VFS::MultiPartUploadState state;
    auto s3_state = s3_.multipart_upload_state(uri);
    if (!s3_state.has_value()) {
      return {Status::Ok(), nullopt};
    }
    state.upload_id = s3_state->upload_id;
    state.part_number = s3_state->part_number;
    state.status = s3_state->st;
    auto& completed_parts = s3_state->completed_parts;
    for (auto& entry : completed_parts) {
      state.completed_parts.emplace_back();
      state.completed_parts.back().e_tag = entry.second.GetETag();
      state.completed_parts.back().part_number = entry.second.GetPartNumber();
    }
    if (!s3_state->buffered_chunks.empty()) {
      state.buffered_chunks.emplace();
      for (auto& chunk : s3_state->buffered_chunks) {
        state.buffered_chunks->emplace_back(
            URI(chunk.uri).remove_trailing_slash().last_path_part(),
            chunk.size);
      }
    }

    return {Status::Ok(), state};
#else
    return {
        LOG_STATUS(Status_VFSError("TileDB was built without S3 support")),
        nullopt};
#endif
  } else if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return {
        LOG_STATUS(Status_VFSError("Not yet supported for Azure")), nullopt};
#else
    return {
        LOG_STATUS(Status_VFSError("TileDB was built without Azure support")),
        nullopt};
#endif
  } else if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return {LOG_STATUS(Status_VFSError("Not yet supported for GCS")), nullopt};
#else
    return {
        LOG_STATUS(Status_VFSError("TileDB was built without GCS support")),
        nullopt};
#endif
  }

  return {
      LOG_STATUS(
          Status_VFSError("Unsupported URI schemes: " + uri.to_string())),
      nullopt};
}

Status VFS::set_multipart_upload_state(
    const URI& uri, const MultiPartUploadState& state) {
  (void)state;
  if (uri.is_file()) {
    return Status::Ok();
  } else if (uri.is_s3()) {
#ifdef HAVE_S3
    S3::MultiPartUploadState s3_state;
    s3_state.part_number = state.part_number;
    s3_state.upload_id = *state.upload_id;
    s3_state.st = state.status;
    for (auto& part : state.completed_parts) {
      auto rv = s3_state.completed_parts.try_emplace(part.part_number);
      rv.first->second.SetETag(part.e_tag->c_str());
      rv.first->second.SetPartNumber(part.part_number);
    }

    if (state.buffered_chunks.has_value()) {
      for (auto& chunk : *state.buffered_chunks) {
        // Chunk URI gets reconstructed from the serialized chunk name
        // and the real attribute uri
        s3_state.buffered_chunks.emplace_back(
            s3_.generate_chunk_uri(uri, chunk.uri).to_string(), chunk.size);
      }
    }

    return s3_.set_multipart_upload_state(uri.to_string(), s3_state);
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without S3 support"));
#endif
  } else if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return LOG_STATUS(Status_VFSError("Not yet supported for Azure"));
#else
    return LOG_STATUS(
        Status_VFSError("TileDB was built without Azure support"));
#endif
  } else if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return LOG_STATUS(Status_VFSError("Not yet supported for GCS"));
#else
    return LOG_STATUS(Status_VFSError("TileDB was built without GCS support"));
#endif
  }

  return LOG_STATUS(
      Status_VFSError("Unsupported URI schemes: " + uri.to_string()));
}

Status VFS::flush_multipart_file_buffer(const URI& uri) {
  if (uri.is_s3()) {
#ifdef HAVE_S3
    Buffer* buff = nullptr;
    throw_if_not_ok(s3_.get_file_buffer(uri, &buff));
    s3_.global_order_write(uri, buff->data(), buff->size());
    buff->reset_size();

#else
    throw StatusException(
        Status_VFSError("TileDB was built without S3 support"));
#endif
  }

  return Status::Ok();
}

}  // namespace tiledb::sm
