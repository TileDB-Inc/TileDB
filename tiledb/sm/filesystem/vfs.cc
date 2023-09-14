/**
 * @file vfs.cc
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

#include <iostream>
#include <list>
#include <sstream>
#include <unordered_map>

#include "tiledb/common/logger_public.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/filesystem.h"
#include "tiledb/sm/enums/vfs_mode.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "tiledb/sm/misc/utils.h"

namespace tiledb::sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

VFS::VFS(ContextResources& resources)
    : resources_(resources) {

  read_ahead_cache_ = tdb_unique_ptr<ReadAheadCache>(
      tdb_new(ReadAheadCache, vfs_params_.read_ahead_cache_size_));

  std::vector<FilesystemCreator> creators = {
    AzureFilesystemCreator(),
    GCSFilesystemCreator(),
    HDFSFilesystemCreator(),
    MEMFilesystemCreator(),
    PosixFilesystemCreator(),
    S3FilesystemCreator(),
    WinFilesystemCreator()
  };

  for (auto& creator : creators) {
    auto fs = creator.create(resources);
    if (!fs) {
      // This filesystem is not enabled.
      continue;
    }

    for (auto& scheme : creator.schemes()) {
      if (scheme.find(":") != std::string::npos) {
        throw VFSException("Invalid VFS Filesystem configuration. Scheme '" +
        + scheme + "' should contain a colon.");
      }

      if (filesystems_.find(scheme) != filesystems_.end()) {
        throw VFSException("Invalid VFS Filesystem configuration. Scheme '" +
          + scheme + "' is claimed by multiple filesystem implementations.");
      }

      filesystems_[scheme] = fs;
    }
  }
}

/* ********************************* */
/*                API                */
/* ********************************* */

Config VFS::config() const {
  return resources.config();
}

bool VFS::is_bucket(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  return fs->is_bucket(uri);
}

bool VFS::is_empty_bucket(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  if (!fs->is_bucket(uri)) {
    throw std::invalid_argument("URI '" + uri.to_string() + "' is not a bucket.");
  }

  return fs->is_empty_bucket(uri);
}

bool VFS::is_dir(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  return fs->is_dir(uri);
}

bool VFS::is_file(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  return fs->is_file(uri);
}

void VFS::create_bucket(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  return fs->create_bucket(uri);
}

void VFS::empty_bucket(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  fs->empty_bucket(uri);
}

void VFS::remove_bucket(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  fs->remove_bucket(uri);
}

void VFS::create_dir(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  fs->create_dir(uri);
}

uint64_t VFS::dir_size(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  return fs->dir_size(uri);
}

std::vector<FilesystemEntry> VFS::ls(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  return fs->ls(uri);
}

void VFS::copy_dir(const URI& src_uri, const URI& tgt_uri) {
  auto fs = get_filesystem_for_uri(uri);
  fs->copy_dir(src_uri, tgt_uri);
}

void VFS::remove_dir(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  fs->remove_dir(uri);
}

void VFS::remove_dirs(const std::vector<URI> uris) {
  auto fs = get_filesystem_for_uri(uri);
  fs->remove_dirs(uris);
}

void VFS::touch(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  fs->touch(uri);
}

uint64_t VFS::file_size(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  return fs->file_size(uri);
}

void VFS::write(const URI& uri, const void* buffer, uint64_t nbytes) {
  auto fs = get_filesystem_for_uri(uri);
  fs->write(uri, buffer, nbytes);
}

void VFS::read(const URI& uri, uint64_t offset, const void* buffer, uint64_t nbytes) {
  auto fs = get_filesystem_for_uri(uri);
  fs->read(uri, offset, buffer, nbytes);
}

void VFS::sync(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  fs->sync(uri);
}

void VFS::copy_file(const URI& src_uri, const URI& tgt_uri) {
  auto fs = get_filesystem_for_uri(uri);
  fs->copy_file(src_uri, tgt_uri);
}

void VFS::move_file(const URI& src_uri, const URI& tgt_uri) {
  auto fs = get_filesystem_for_uri(uri);
  fs->move_file(src_uri, tgt_uri);
}

void VFS::remove_file(const URI& src_uri, const URI& tgt_uri) {
  auto fs = get_filesystem_for_uri(uri);
  fs->remove_file(src_uri, tgt_uri);
}

void VFS::remove_files(const std::vector<URI> uris) {
  auto fs = get_filesystem_for_uri(uri);
  fs->remove_files(src_uri, tgt_uri);
}

void VFS::open_file(const URI& uri, VFSMode mode) {
  auto fs = get_filesystem_for_uri(uri);

  switch (mode) {
    case VFSMode::VFS_READ:
      if (!fs->is_file(uri)) {
        throw std::invalid_argument("Error opneing file '" + uri.c_str() +
          "'; File does not exist.");
      }
      break;
    case VFSMode::VFS_WRITE:
    case VFSMode::VFS_APPEND:
      if (fs->is_file(uri)) {
        fs->remove_file(uri);
      }
      break;
    default:
      stdx::unreachable();
  }
}

void VFS::close_file(const URI& uri, VFSMode mode) {
  auto fs = get_filesystem_for_uri(uri);
  fs->sync(uri);
}

void VFS::finalize_and_close(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  fs->finalize_and_close(uri);
}

MultiPartUploadState VFS::get_write_status(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  return fs->get_write_stats(uri);
}

void VFS::set_write_state(const URI& uri, const MultiPartUploadState& state) {
  auto fs = get_filesystem_for_uri(uri);
  fs->set_write_stats(uri, state);
}

void VFS::flush_write(const URI& uri) {
  auto fs = get_filesystem_for_uri(uri);
  fs->flush_write(uri);
}

shared_ptr<Filesystem> VFS::get_fileystem_for_uri(const URI& uri) {
  std::string scheme = "";

  auto pos = uri.to_sring().find("://");
  if (pos != std::string::npos) {
    scheme = uri.to_string().substr(0, pos);
  }

  auto& iter = fileystems_.find(scheme);
  if (iter == filesystems_.end()) {
    throw std::invalid_argument("No filesystem configured for scheme '" + scheme + "'");
  }

  return iter.second;
}

}  // namespace tiledb::sm
