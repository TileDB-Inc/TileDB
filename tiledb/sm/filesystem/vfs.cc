/**
 * @file   vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
#include "tiledb/common/assert.h"
#include "tiledb/common/log_duration_instrument.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/filesystem.h"
#include "tiledb/sm/enums/vfs_mode.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/tile/tile.h"

#include <iostream>
#include <list>
#include <sstream>
#include <unordered_map>

using namespace tiledb::common;
using namespace tiledb::sm::filesystem;

namespace tiledb::sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

VFS::VFS(
    stats::Stats* const parent_stats,
    Logger* const logger,
    ThreadPool* const compute_tp,
    ThreadPool* const io_tp,
    const Config& config)
    : VFSBase(parent_stats)
    , Azure_within_VFS(io_tp, config)
    , GCS_within_VFS(io_tp, config)
    , S3_within_VFS(stats_, io_tp, config)
    , config_(config)
    , logger_(logger)
    , compute_tp_(compute_tp)
    , io_tp_(io_tp)
    , vfs_params_(VFSParameters(config)) {
  Status st;
  passert(compute_tp);
  passert(io_tp);

  // Construct the read-ahead cache.
  read_ahead_cache_ = tdb_unique_ptr<ReadAheadCache>(
      tdb_new(ReadAheadCache, vfs_params_.read_ahead_cache_size_));

  if constexpr (s3_enabled) {
    supported_fs_.insert(Filesystem::S3);
  }

#ifdef HAVE_AZURE
  supported_fs_.insert(Filesystem::AZURE);
#endif

#ifdef HAVE_GCS
  supported_fs_.insert(Filesystem::GCS);
#endif

#ifdef _WIN32
  throw_if_not_ok(win_.init(config_));
#else
  posix_ = Posix(config_);
#endif

  supported_fs_.insert(Filesystem::MEMFS);
}

/* ********************************* */
/*                API                */
/* ********************************* */

std::string VFS::abs_path(std::string_view path) {
  // workaround for older clang (llvm 3.5) compilers (issue #828)
  std::string path_copy(path);
#ifdef _WIN32
  {
    std::string norm_sep_path =
        path_win::slashes_to_backslashes(std::string(path));
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
    posix_.create_dir(uri);
    return Status::Ok();
#endif
  }
  if (uri.is_s3()) {
    if constexpr (s3_enabled) {
      // It is a noop for S3
      return Status::Ok();
    } else {
      throw BuiltWithout("S3");
    }
  }
  if (uri.is_azure()) {
    if constexpr (azure_enabled) {
      // It is a noop for Azure
      return Status::Ok();
    } else {
      throw BuiltWithout("Azure");
    }
  }
  if (uri.is_gcs()) {
    if constexpr (gcs_enabled) {
      // It is a noop for GCS
      return Status::Ok();
    } else {
      throw BuiltWithout("GCS");
    }
  }
  if (uri.is_memfs()) {
    memfs_.create_dir(uri);
    return Status::Ok();
  }
  throw UnsupportedURI(uri.to_string());
}

Status VFS::dir_size(const URI& dir_name, uint64_t* dir_size) const {
  // Sanity check
  bool is_dir;
  RETURN_NOT_OK(this->is_dir(dir_name, &is_dir));
  if (!is_dir)
    throw VFSException(
        "Cannot get directory size; Input '" + dir_name.to_string() +
        "' is not a directory");

  // Get all files in the tree rooted at `dir_name` and add their sizes
  *dir_size = 0;
  std::list<URI> to_ls;
  to_ls.push_front(dir_name);
  do {
    auto uri = to_ls.front();
    to_ls.pop_front();

    for (const auto& child : ls_with_sizes(uri)) {
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
  auto instrument = make_log_duration_instrument(uri, "touch");
  if (uri.is_file()) {
#ifdef _WIN32
    return win_.touch(uri.to_path());
#else
    posix_.touch(uri);
    return Status::Ok();
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    s3().touch(uri);
    return Status::Ok();
#else
    throw BuiltWithout("S3");
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    azure().touch(uri);
    return Status::Ok();
#else
    throw BuiltWithout("Azure");
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs().touch(uri);
#else
    throw BuiltWithout("GCS");
#endif
  }
  if (uri.is_memfs()) {
    memfs_.touch(uri);
    return Status::Ok();
  }
  throw UnsupportedURI(uri.to_string());
}

Status VFS::cancel_all_tasks() {
  cancelable_tasks_.cancel_all_tasks();
  return Status::Ok();
}

Status VFS::create_bucket(const URI& uri) const {
  auto instrument = make_log_duration_instrument(uri, "create_bucket");
  if (uri.is_s3()) {
#ifdef HAVE_S3
    s3().create_bucket(uri);
    return Status::Ok();
#else
    throw BuiltWithout("S3");
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    azure().create_container(uri);
    return Status::Ok();
#else
    throw BuiltWithout("Azure");
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs().create_bucket(uri);
#else
    throw BuiltWithout("GCS");
#endif
  }
  throw UnsupportedURI(uri.to_string());
}

Status VFS::remove_bucket(const URI& uri) const {
  if (uri.is_s3()) {
#ifdef HAVE_S3
    s3().remove_bucket(uri);
    return Status::Ok();
#else
    throw BuiltWithout("S3");
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    azure().remove_container(uri);
    return Status::Ok();
#else
    throw BuiltWithout("Azure");
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs().remove_bucket(uri);
#else
    throw BuiltWithout("GCS");
#endif
  }
  throw UnsupportedURI(uri.to_string());
}

Status VFS::empty_bucket(const URI& uri) const {
  auto instrument = make_log_duration_instrument(uri, "empty_bucket");
  if (uri.is_s3()) {
#ifdef HAVE_S3
    s3().empty_bucket(uri);
    return Status::Ok();
#else
    throw BuiltWithout("S3");
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    azure().empty_container(uri);
    return Status::Ok();
#else
    throw BuiltWithout("Azure");
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs().empty_bucket(uri);
#else
    throw BuiltWithout("GCS");
#endif
  }
  throw UnsupportedURI(uri.to_string());
}

Status VFS::is_empty_bucket(
    const URI& uri, [[maybe_unused]] bool* is_empty) const {
  auto instrument = make_log_duration_instrument(uri, "is_empty_bucket");
  if (uri.is_s3()) {
#ifdef HAVE_S3
    *is_empty = s3().is_empty_bucket(uri);
    return Status::Ok();
#else
    throw BuiltWithout("S3");
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    *is_empty = azure().is_empty_container(uri);
    return Status::Ok();
#else
    throw BuiltWithout("Azure");
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs().is_empty_bucket(uri, is_empty);
#else
    throw BuiltWithout("GCS");
#endif
  }
  throw UnsupportedURI(uri.to_string());
}

Status VFS::remove_dir(const URI& uri) const {
  auto instrument = make_log_duration_instrument(uri, "remove_dir");
  if (uri.is_file()) {
#ifdef _WIN32
    return win_.remove_dir(uri.to_path());
#else
    posix_.remove_dir(uri);
#endif
  } else if (uri.is_s3()) {
#ifdef HAVE_S3
    s3().remove_dir(uri);
#else
    throw BuiltWithout("S3");
#endif
  } else if (uri.is_azure()) {
#ifdef HAVE_AZURE
    azure().remove_dir(uri);
#else
    throw BuiltWithout("Azure");
#endif
  } else if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs().remove_dir(uri);
#else
    throw BuiltWithout("GCS");
#endif
  } else if (uri.is_memfs()) {
    memfs_.remove_dir(uri);
    return Status::Ok();
  } else {
    throw UnsupportedURI(uri.to_string());
  }
  return Status::Ok();
}

void VFS::remove_dir_if_empty(const URI& uri) const {
  if (uri.is_file()) {
#ifdef _WIN32
    win_.remove_dir_if_empty(uri.to_path());
#else
    posix_.remove_dir_if_empty(uri.to_path());
#endif
  }
  // Object stores do not have directories.
}

void VFS::remove_dirs(
    ThreadPool* compute_tp, const std::vector<URI>& uris) const {
  throw_if_not_ok(parallel_for(compute_tp, 0, uris.size(), [&](size_t i) {
    bool is_dir;
    throw_if_not_ok(this->is_dir(uris[i], &is_dir));
    if (is_dir) {
      throw_if_not_ok(remove_dir(uris[i]));
    }
    return Status::Ok();
  }));
}

Status VFS::remove_file(const URI& uri) const {
  auto instrument = make_log_duration_instrument(uri, "remove_file");
  if (uri.is_file()) {
#ifdef _WIN32
    return win_.remove_file(uri.to_path());
#else
    posix_.remove_file(uri);
    return Status::Ok();
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3().remove_object(uri);
#else
    throw BuiltWithout("S3");
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    azure().remove_blob(uri);
    return Status::Ok();
#else
    throw BuiltWithout("Azure");
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs().remove_object(uri);
#else
    throw BuiltWithout("GCS");
#endif
  }
  if (uri.is_memfs()) {
    memfs_.remove_file(uri);
    return Status::Ok();
  }
  throw UnsupportedURI(uri.to_string());
}

void VFS::remove_files(
    ThreadPool* compute_tp, const std::vector<URI>& uris) const {
  throw_if_not_ok(parallel_for(compute_tp, 0, uris.size(), [&](size_t i) {
    throw_if_not_ok(remove_file(uris[i]));
    return Status::Ok();
  }));
}

void VFS::remove_files(
    ThreadPool* compute_tp, const std::vector<TimestampedURI>& uris) const {
  throw_if_not_ok(parallel_for(compute_tp, 0, uris.size(), [&](size_t i) {
    throw_if_not_ok(remove_file(uris[i].uri_));
    return Status::Ok();
  }));
}

Status VFS::max_parallel_ops(const URI& uri, uint64_t* ops) const {
  *ops = 0;

  if (uri.is_s3()) {
    *ops = config_.get<uint64_t>("vfs.s3.max_parallel_ops", Config::must_find);
  } else if (uri.is_azure()) {
    *ops =
        config_.get<uint64_t>("vfs.azure.max_parallel_ops", Config::must_find);
  } else if (uri.is_gcs()) {
    *ops = config_.get<uint64_t>("vfs.gcs.max_parallel_ops", Config::must_find);
  } else {
    *ops = 1;
  }

  return Status::Ok();
}

uint64_t VFS::file_size(const URI& uri) const {
  auto instrument = make_log_duration_instrument(uri, "file_size");
  stats_->add_counter("file_size_num", 1);
  if (uri.is_file()) {
#ifdef _WIN32
    uint64_t size;
    throw_if_not_ok(win_.file_size(uri.to_path(), &size));
    return size;
#else
    return posix_.file_size(uri);
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    uint64_t size;
    throw_if_not_ok(s3().object_size(uri, &size));
    return size;
#else
    throw BuiltWithout("S3");
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    return azure().blob_size(uri);
#else
    throw BuiltWithout("Azure");
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    uint64_t size;
    throw_if_not_ok(gcs().object_size(uri, &size));
    return size;
#else
    throw BuiltWithout("GCS");
#endif
  }
  if (uri.is_memfs()) {
    return memfs_.file_size(uri);
  }
  throw UnsupportedURI(uri.to_string());
}

Status VFS::is_dir(const URI& uri, bool* is_dir) const {
  auto instrument = make_log_duration_instrument(uri, "is_dir");
  if (uri.is_file()) {
#ifdef _WIN32
    *is_dir = win_.is_dir(uri.to_path());
#else
    *is_dir = posix_.is_dir(uri);
#endif
    return Status::Ok();
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    return s3().is_dir(uri, is_dir);
#else
    *is_dir = false;
    throw BuiltWithout("S3");
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    *is_dir = azure().is_dir(uri);
    return Status::Ok();
#else
    *is_dir = false;
    throw BuiltWithout("Azure");
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs().is_dir(uri, is_dir);
#else
    *is_dir = false;
    throw BuiltWithout("GCS");
#endif
  }
  if (uri.is_memfs()) {
    *is_dir = memfs_.is_dir(uri);
    return Status::Ok();
  }
  throw UnsupportedURI(uri.to_string());
}

Status VFS::is_file(const URI& uri, bool* is_file) const {
  auto instrument = make_log_duration_instrument(uri, "is_file");
  stats_->add_counter("is_object_num", 1);
  if (uri.is_file()) {
#ifdef _WIN32
    *is_file = win_.is_file(uri.to_path());
#else
    *is_file = posix_.is_file(uri);
#endif
    return Status::Ok();
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    RETURN_NOT_OK(s3().is_object(uri, is_file));
    return Status::Ok();
#else
    *is_file = false;
    throw BuiltWithout("S3");
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    *is_file = azure().is_blob(uri);
    return Status::Ok();
#else
    *is_file = false;
    throw BuiltWithout("Azure");
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs().is_object(uri, is_file);
#else
    *is_file = false;
    throw BuiltWithout("GCS");
#endif
  }
  if (uri.is_memfs()) {
    *is_file = memfs_.is_file(uri);
    return Status::Ok();
  }
  throw UnsupportedURI(uri.to_string());
}

Status VFS::is_bucket(const URI& uri, bool* is_bucket) const {
  auto instrument = make_log_duration_instrument(uri, "is_bucket");
  if (uri.is_s3()) {
#ifdef HAVE_S3
    *is_bucket = s3().is_bucket(uri);
    return Status::Ok();
#else
    *is_bucket = false;
    throw BuiltWithout("S3");
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    *is_bucket = azure().is_container(uri);
    return Status::Ok();
#else
    *is_bucket = false;
    throw BuiltWithout("Azure");
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    RETURN_NOT_OK(gcs().is_bucket(uri, is_bucket));
    return Status::Ok();
#else
    *is_bucket = false;
    throw BuiltWithout("GCS");
#endif
  }

  throw UnsupportedURI(uri.to_string());
}

Status VFS::ls(const URI& parent, std::vector<URI>* uris) const {
  stats_->add_counter("ls_num", 1);

  for (auto& fs : ls_with_sizes(parent)) {
    uris->emplace_back(fs.path().native());
  }

  return Status::Ok();
}

std::vector<directory_entry> VFS::ls_with_sizes(const URI& parent) const {
  auto instrument = make_log_duration_instrument(parent, "ls");
  // Noop if `parent` is not a directory, do not error out.
  // For S3, GCS and Azure, `ls` on a non-directory will just
  // return an empty `uris` vector.
  if (!(parent.is_s3() || parent.is_gcs() || parent.is_azure())) {
    bool flag = false;
    throw_if_not_ok(this->is_dir(parent, &flag));

    if (!flag) {
      return {};
    }
  }

  std::vector<directory_entry> entries;
  if (parent.is_file()) {
#ifdef _WIN32
    entries = win_.ls_with_sizes(parent);
#else
    entries = posix_.ls_with_sizes(parent);
#endif
  } else if (parent.is_s3()) {
#ifdef HAVE_S3
    entries = s3().ls_with_sizes(parent);
#else
    throw BuiltWithout("S3");
#endif
  } else if (parent.is_azure()) {
#ifdef HAVE_AZURE
    entries = azure().ls_with_sizes(parent);
#else
    throw BuiltWithout("Azure");
#endif
  } else if (parent.is_gcs()) {
#ifdef HAVE_GCS
    entries = gcs().ls_with_sizes(parent);
#else
    throw BuiltWithout("GCS");
#endif
  } else if (parent.is_memfs()) {
    entries = memfs_.ls_with_sizes(URI("mem://" + parent.to_path()));
  } else {
    throw UnsupportedURI(parent.to_string());
  }
  parallel_sort(
      compute_tp_,
      entries.begin(),
      entries.end(),
      [](const directory_entry& l, const directory_entry& r) {
        return l.path().native() < r.path().native();
      });

  return entries;
}

Status VFS::move_file(const URI& old_uri, const URI& new_uri) {
  auto instrument = make_log_duration_instrument(old_uri, new_uri);
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
      posix_.move_file(old_uri, new_uri);
      return Status::Ok();
#endif
    }
    throw UnsupportedOperation("Moving files");
  }

  // S3
  if (old_uri.is_s3()) {
    if (new_uri.is_s3())
#ifdef HAVE_S3
      return s3().move_object(old_uri, new_uri);
#else
      throw BuiltWithout("S3");
#endif
    throw UnsupportedOperation("Moving files");
  }

  // Azure
  if (old_uri.is_azure()) {
    if (new_uri.is_azure())
#ifdef HAVE_AZURE
      azure().move_object(old_uri, new_uri);
    return Status::Ok();
#else
      throw BuiltWithout("Azure");
#endif
    throw UnsupportedOperation("Moving files");
  }

  // GCS
  if (old_uri.is_gcs()) {
    if (new_uri.is_gcs())
#ifdef HAVE_GCS
      return gcs().move_object(old_uri, new_uri);
#else
      throw BuiltWithout("GCS");
#endif
    throw UnsupportedOperation("Moving files");
  }

  // In-memory filesystem
  if (old_uri.is_memfs()) {
    if (new_uri.is_memfs()) {
      memfs_.move_file(old_uri, new_uri);
      return Status::Ok();
    }
    throw UnsupportedOperation("Moving files");
  }

  // Unsupported filesystem
  throw UnsupportedURI(old_uri.to_string() + ", " + new_uri.to_string());
}

Status VFS::move_dir(const URI& old_uri, const URI& new_uri) {
  auto instrument = make_log_duration_instrument(old_uri, new_uri);
  // File
  if (old_uri.is_file()) {
    if (new_uri.is_file()) {
#ifdef _WIN32
      return win_.move_path(old_uri.to_path(), new_uri.to_path());
#else
      posix_.move_dir(old_uri, new_uri);
      return Status::Ok();
#endif
    }
    throw UnsupportedOperation("Moving directories");
  }

  // S3
  if (old_uri.is_s3()) {
    if (new_uri.is_s3()) {
#ifdef HAVE_S3
      s3().move_dir(old_uri, new_uri);
      return Status::Ok();
#else
      throw BuiltWithout("S3");
#endif
    }
    throw UnsupportedOperation("Moving directories");
  }

  // Azure
  if (old_uri.is_azure()) {
    if (new_uri.is_azure())
#ifdef HAVE_AZURE
      azure().move_dir(old_uri, new_uri);
    return Status::Ok();
#else
      throw BuiltWithout("Azure");
#endif
    throw UnsupportedOperation("Moving directories");
  }

  // GCS
  if (old_uri.is_gcs()) {
    if (new_uri.is_gcs())
#ifdef HAVE_GCS
      return gcs().move_dir(old_uri, new_uri);
#else
      throw BuiltWithout("GCS");
#endif
    throw UnsupportedOperation("Moving directories");
  }

  // In-memory filesystem
  if (old_uri.is_memfs()) {
    if (new_uri.is_memfs()) {
      memfs_.move_dir(old_uri, new_uri);
      return Status::Ok();
    }
    throw UnsupportedOperation("Moving directories");
  }

  // Unsupported filesystem
  throw UnsupportedURI(old_uri.to_string() + ", " + new_uri.to_string());
}

Status VFS::copy_file(const URI& old_uri, const URI& new_uri) {
  auto instrument = make_log_duration_instrument(old_uri, new_uri);
  // If new_uri exists, delete it or raise an error based on `force`
  bool is_file;
  RETURN_NOT_OK(this->is_file(new_uri, &is_file));
  if (is_file)
    RETURN_NOT_OK(remove_file(new_uri));

  // File
  if (old_uri.is_file()) {
    if (new_uri.is_file()) {
#ifdef _WIN32
      throw VFSException("Copying files on Windows is not yet supported.");
#else
      posix_.copy_file(old_uri, new_uri);
      return Status::Ok();
#endif
    }
    throw UnsupportedOperation("Copying files");
  }

  // S3
  if (old_uri.is_s3()) {
    if (new_uri.is_s3()) {
#ifdef HAVE_S3
      s3().copy_file(old_uri, new_uri);
      return Status::Ok();
#else
      throw BuiltWithout("S3");
#endif
    }
    throw UnsupportedOperation("Copying files");
  }

  // Azure
  if (old_uri.is_azure()) {
    if (new_uri.is_azure()) {
      if constexpr (azure_enabled) {
        throw VFSException("Copying files on Azure is not yet supported.");
      } else {
        throw BuiltWithout("Azure");
      }
    }
    throw UnsupportedOperation("Copying files");
  }

  // GCS
  if (old_uri.is_gcs()) {
    if (new_uri.is_gcs()) {
      if constexpr (gcs_enabled) {
        throw VFSException("Copying files on GCS is not yet supported.");
      } else {
        throw BuiltWithout("GCS");
      }
    }
    throw UnsupportedOperation("Copying files");
  }

  // Unsupported filesystem
  throw UnsupportedURI(old_uri.to_string() + ", " + new_uri.to_string());
}

Status VFS::copy_dir(const URI& old_uri, const URI& new_uri) {
  auto instrument = make_log_duration_instrument(old_uri, new_uri);
  // File
  if (old_uri.is_file()) {
    if (new_uri.is_file()) {
#ifdef _WIN32
      throw VFSException(
          "Copying directories on Windows is not yet supported.");
#else
      posix_.copy_dir(old_uri, new_uri);
      return Status::Ok();
#endif
    }
    throw UnsupportedOperation("Copying directories");
  }

  // S3
  if (old_uri.is_s3()) {
    if (new_uri.is_s3()) {
#ifdef HAVE_S3
      s3().copy_dir(old_uri, new_uri);
      return Status::Ok();
#else
      throw BuiltWithout("S3");
#endif
    }
    throw UnsupportedOperation("Copying directories");
  }

  // Azure
  if (old_uri.is_azure()) {
    if (new_uri.is_azure()) {
      if constexpr (azure_enabled) {
        throw VFSException(
            "Copying directories on Azure is not yet supported.");
      } else {
        throw BuiltWithout("Azure");
      }
    }
    throw UnsupportedOperation("Copying directories");
  }

  // GCS
  if (old_uri.is_gcs()) {
    if (new_uri.is_gcs()) {
      if constexpr (gcs_enabled) {
        throw VFSException("Copying directories on GCS is not yet supported.");
      } else {
        throw BuiltWithout("GCS");
      }
    }
    throw UnsupportedOperation("Copying directories");
  }

  // Unsupported filesystem
  throw UnsupportedURI(old_uri.to_string() + ", " + new_uri.to_string());
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
      return Status_VFSError(errmsg.str());
    }
    return st;
  }
}

Status VFS::read_impl(
    const URI& uri,
    const uint64_t offset,
    void* const buffer,
    const uint64_t nbytes,
    [[maybe_unused]] const bool use_read_ahead) {
  auto instrument = make_log_duration_instrument(uri, "read");
  stats_->add_counter("read_ops_num", 1);
  log_read(uri, offset, nbytes);

  // We only check to use the read-ahead cache for cloud-storage
  // backends.

  if (uri.is_file()) {
#ifdef _WIN32
    return win_.read(uri.to_path(), offset, buffer, nbytes);
#else
    posix_.read(uri, offset, buffer, nbytes, false);
    return Status::Ok();
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    const auto read_fn = std::bind(
        &S3::read_impl,
        &s3(),
        std::placeholders::_1,
        std::placeholders::_2,
        std::placeholders::_3,
        std::placeholders::_4,
        std::placeholders::_5,
        std::placeholders::_6);
    return read_ahead_impl(
        read_fn, uri, offset, buffer, nbytes, use_read_ahead);
#else
    throw BuiltWithout("S3");
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    const auto read_fn = std::bind(
        &Azure::read,
        &azure(),
        std::placeholders::_1,
        std::placeholders::_2,
        std::placeholders::_3,
        std::placeholders::_4,
        std::placeholders::_5,
        std::placeholders::_6);
    return read_ahead_impl(
        read_fn, uri, offset, buffer, nbytes, use_read_ahead);
#else
    throw BuiltWithout("Azure");
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    const auto read_fn = std::bind(
        &GCS::read,
        &gcs(),
        std::placeholders::_1,
        std::placeholders::_2,
        std::placeholders::_3,
        std::placeholders::_4,
        std::placeholders::_5,
        std::placeholders::_6);
    return read_ahead_impl(
        read_fn, uri, offset, buffer, nbytes, use_read_ahead);
#else
    throw BuiltWithout("GCS");
#endif
  }
  if (uri.is_memfs()) {
    memfs_.read(uri, offset, buffer, nbytes, use_read_ahead);
    return Status::Ok();
  }

  throw UnsupportedURI(uri.to_string());
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
  iassert(
      nbytes_read >= nbytes,
      "nbytes_read = {}, nbytes = {}",
      nbytes_read,
      nbytes);
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
  } else {
    return true;
  }
}

Status VFS::sync(const URI& uri) {
  auto instrument = make_log_duration_instrument(uri, "sync");
  if (uri.is_file()) {
#ifdef _WIN32
    return win_.sync(uri.to_path());
#else
    posix_.sync(uri);
    return Status::Ok();
#endif
  }
  if (uri.is_s3()) {
    if constexpr (s3_enabled) {
      return Status::Ok();
    } else {
      throw BuiltWithout("S3");
    }
  }
  if (uri.is_azure()) {
    if constexpr (azure_enabled) {
      return Status::Ok();
    } else {
      throw BuiltWithout("Azure");
    }
  }
  if (uri.is_gcs()) {
    if constexpr (gcs_enabled) {
      return Status::Ok();
    } else {
      throw BuiltWithout("GCS");
    }
  }
  if (uri.is_memfs()) {
    return Status::Ok();
  }
  throw UnsupportedURI(uri.to_string());
}

Status VFS::open_file(const URI& uri, VFSMode mode) {
  bool is_file;
  RETURN_NOT_OK(this->is_file(uri, &is_file));

  switch (mode) {
    case VFSMode::VFS_READ:
      if (!is_file)
        throw VFSException(
            "Cannot open file '" + uri.to_string() + "'; File does not exist");
      break;
    case VFSMode::VFS_WRITE:
      if (is_file)
        RETURN_NOT_OK(remove_file(uri));
      break;
    case VFSMode::VFS_APPEND:
      if (uri.is_s3()) {
        if constexpr (s3_enabled) {
          throw VFSException(
              "Cannot open file '" + uri.to_string() +
              "'; S3 does not support append mode");
        } else {
          throw BuiltWithout("S3");
        }
      }
      if (uri.is_azure()) {
        if constexpr (azure_enabled) {
          throw VFSException(
              "Cannot open file '" + uri.to_string() +
              "'; Azure does not support append mode");
        } else {
          throw BuiltWithout("Azure");
        }
      }
      if (uri.is_gcs()) {
        if constexpr (gcs_enabled) {
          throw VFSException(
              "Cannot open file '" + uri.to_string() +
              "'; GCS does not support append mode");
        } else {
          throw BuiltWithout("GCS");
        }
      }
      break;
  }

  return Status::Ok();
}

void VFS::flush(const URI& uri, bool finalize) {
  auto instrument = make_log_duration_instrument(uri, "flush");
  if (uri.is_file()) {
#ifdef _WIN32
    win_.flush(uri);
    return;
#else
    posix_.flush(uri, finalize);
    return;
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    s3().flush_object(uri, finalize);
    return;
#else
    throw BuiltWithout("S3");
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    azure().flush_blob(uri);
    return;
#else
    throw BuiltWithout("Azure");
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    throw_if_not_ok(gcs().flush_object(uri));
    return;
#else
    throw BuiltWithout("GCS");
#endif
  }
  if (uri.is_memfs()) {
    return;
  }
  throw UnsupportedURI(uri.to_string());
}

Status VFS::close_file(const URI& uri) {
  flush(uri);
  return Status::Ok();
}

Status VFS::write(
    const URI& uri,
    const void* buffer,
    uint64_t buffer_size,
    [[maybe_unused]] bool remote_global_order_write) {
  auto instrument = make_log_duration_instrument(uri, "write");
  stats_->add_counter("write_byte_num", buffer_size);
  stats_->add_counter("write_ops_num", 1);

  if (uri.is_file()) {
#ifdef _WIN32
    return win_.write(uri.to_path(), buffer, buffer_size);
#else
    posix_.write(uri, buffer, buffer_size);
    return Status::Ok();
#endif
  }
  if (uri.is_s3()) {
#ifdef HAVE_S3
    s3().write(uri, buffer, buffer_size, remote_global_order_write);
    return Status::Ok();
#else
    throw BuiltWithout("S3");
#endif
  }
  if (uri.is_azure()) {
#ifdef HAVE_AZURE
    azure().write(uri, buffer, buffer_size);
    return Status::Ok();
#else
    throw BuiltWithout("Azure");
#endif
  }
  if (uri.is_gcs()) {
#ifdef HAVE_GCS
    return gcs().write(uri, buffer, buffer_size);
#else
    throw BuiltWithout("GCS");
#endif
  }
  if (uri.is_memfs()) {
    memfs_.write(uri, buffer, buffer_size, remote_global_order_write);
    return Status::Ok();
  }
  throw UnsupportedURI(uri.to_string());
}

std::pair<Status, std::optional<VFS::MultiPartUploadState>>
VFS::multipart_upload_state(const URI& uri) {
  if (uri.is_file()) {
    return {Status::Ok(), {}};
  } else if (uri.is_s3()) {
#ifdef HAVE_S3
    VFS::MultiPartUploadState state;
    auto s3_state = s3().multipart_upload_state(uri);
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
    return {Status_VFSError("TileDB was built without S3 support"), nullopt};
#endif
  } else if (uri.is_azure()) {
    if constexpr (azure_enabled) {
      return {Status_VFSError("Not yet supported for Azure"), nullopt};
    } else {
      return {
          Status_VFSError("TileDB was built without Azure support"), nullopt};
    }
  } else if (uri.is_gcs()) {
    if constexpr (gcs_enabled) {
      return {Status_VFSError("Not yet supported for GCS"), nullopt};
    } else {
      return {Status_VFSError("TileDB was built without GCS support"), nullopt};
    }
  }

  return {
      Status_VFSError("Unsupported URI schemes: " + uri.to_string()), nullopt};
}

Status VFS::set_multipart_upload_state(
    const URI& uri, [[maybe_unused]] const MultiPartUploadState& state) {
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
            s3().generate_chunk_uri(uri, chunk.uri).to_string(), chunk.size);
      }
    }

    return s3().set_multipart_upload_state(uri.to_string(), s3_state);
#else
    throw BuiltWithout("S3");
#endif
  } else if (uri.is_azure()) {
    if constexpr (azure_enabled) {
      throw VFSException("Not yet supported for Azure");
    } else {
      throw BuiltWithout("Azure");
    }
  } else if (uri.is_gcs()) {
    if constexpr (gcs_enabled) {
      throw VFSException("Not yet supported for GCS");
    } else {
      throw BuiltWithout("GCS");
    }
  }

  throw UnsupportedURI(uri.to_string());
}

Status VFS::flush_multipart_file_buffer(const URI& uri) {
  auto instrument =
      make_log_duration_instrument(uri, "flush_multipart_file_buffer");
  if (uri.is_s3()) {
#ifdef HAVE_S3
    Buffer* buff = nullptr;
    throw_if_not_ok(s3().get_file_buffer(uri, &buff));
    s3().global_order_write(uri, buff->data(), buff->size());
    buff->reset_size();

#else
    throw BuiltWithout("S3");
#endif
  }

  return Status::Ok();
}

void VFS::log_read(const URI& uri, uint64_t offset, uint64_t nbytes) {
  std::string read_to_log;
  switch (vfs_params_.read_logging_mode_) {
    case VFSParameters::ReadLoggingMode::DISABLED: {
      return;
    }
    case VFSParameters::ReadLoggingMode::FRAGMENTS: {
      auto fragment_name = uri.get_fragment_name();
      if (!fragment_name.has_value()) {
        return;
      }
      read_to_log = fragment_name.value().to_string();
      break;
    }
    case VFSParameters::ReadLoggingMode::FRAGMENT_FILES: {
      if (!uri.get_fragment_name().has_value()) {
        return;
      }
      read_to_log = uri.to_string();
      break;
    }
    case VFSParameters::ReadLoggingMode::ALL_FILES: {
      read_to_log = uri.to_string();
      break;
    }
    case VFSParameters::ReadLoggingMode::ALL_READS:
    case VFSParameters::ReadLoggingMode::ALL_READS_ALWAYS: {
      read_to_log = uri.to_string() + ":offset:" + std::to_string(offset) +
                    ":nbytes:" + std::to_string(nbytes);
      break;
    }
    default:
      return;
  }

  if (vfs_params_.read_logging_mode_ !=
      VFSParameters::ReadLoggingMode::ALL_READS_ALWAYS) {
    if (reads_logged_.find(read_to_log) != reads_logged_.end()) {
      return;
    }
    reads_logged_.insert(read_to_log);
  }

  LOG_INFO("VFS Read: " + read_to_log);
}

optional<LogDurationInstrument> VFS::make_log_duration_instrument(
    const URI& uri, const std::string_view operation_name) const {
  if (!vfs_params_.log_operations_ || logger_ == nullptr) {
    return nullopt;
  }
  // Construct LogDurationInstrument in-place to take advantage of RVO.
  return std::make_optional<LogDurationInstrument>(
      logger_, "{} on {}", operation_name, uri.to_string());
}

optional<LogDurationInstrument> VFS::make_log_duration_instrument(
    const URI& source,
    const URI& destination,
    const std::string_view operation_name) const {
  if (!vfs_params_.log_operations_ || logger_ == nullptr) {
    return nullopt;
  }
  // Construct LogDurationInstrument in-place to take advantage of RVO.
  return std::make_optional<LogDurationInstrument>(
      logger_,
      "{} from {} to {}",
      operation_name,
      source.to_string(),
      destination.to_string());
}

}  // namespace tiledb::sm
