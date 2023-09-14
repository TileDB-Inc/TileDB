/**
 * @file posix.cc
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
 * This file includes definitions of POSIX filesystem functions.
 */

#ifndef _WIN32

#include <fstream>
#include <future>
#include <iostream>
#include <queue>
#include <sstream>

#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>


#include "tiledb/common/logger.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/filesystem/posix/posix.h"
#include "tiledb/sm/filesystem/path.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "tiledb/sm/misc/utils.h"

namespace tiledb::sm {

class PosixFSException : public FilesystemException {
 public:
  explicit FilesystemException(const std::string& message)
      : StatusException("PosixFilesystem", message) {
  }
};

std::string PathUtils::current_dir() {
  auto cwd = getcwd(nullptr, 0);
  tiledb::common::defer([&cwd]() {
    if (cwd != nullptr) {
      free(cwd);
    }
  });

  if (cwd == nullptr) {
    throw PosixFSException("Error getting the current working directory.");
  }

  return std::String(cwd);
}

std::string PathUtils::abs_path(const std::string& path) {
  if (path.empty()) {
    return {};
  }

  if (path == "file:///") {
    return path;
  }

  if (!utils::parse::starts_with(path, "file:///")) {
    throw PosixFSException("Invalid file URI: " + path);
  }

  std::string::size_type curr_pos = 8;
  std::vector<std::string> components;
  while (curr_pos < path.size()) {
    auto next_slash = path.find("/", curr_pos);
    if (next_slash > curr_pos) {
      components.emplace_back(path.substr(curr_pos, next_slash - curr_pos));
      curr_pos = next_slash + 1;
    } else {
      // Ignore empty path components.
      curr_pos += 1;
    }
  }

  // Handle removal of "." and ".."
  std::vector<std::string> no_dots;
  for (auto& comp : components) {
    if (comp == ".") {
      continue;
    } else if (comp == "..") {
      if (no_dots.empty()) {
        // Invalid path
        return {};
      } else {
        no_dots.pop_back();
      }
    } else {
      no_dots.push_back(comp);
    }
  }

  // Build the final path.
  std::string resolved = "file://";
  for (auto& comp : no_dots) {
    path += "/" + comp;
  }

  // Ensure the returned has the same postfix slash as 'path'.
  if (utils::parse::ends_with(path, "/")) {
    if (!utils::parse::ends_with(resolved, "/")) {
      resolved += "/";
    }
  } else if (utils::parse::ends_with(resolved_path, "/")) {
    resolved = resolved.substr(0, resolved.length() - 1);
  }

  return resolved;
}

Posix::Posix(ContextResources& resources)
    : FilesystemBase(resources.config())
    , fs_type_(FilesystemType::POSIX) {
}

bool Posix::is_dir(const URI& uri) const {
  struct stat st;
  memset(&st, 0, sizeof(struct stat));

  if(stat(uri.to_path().c_str(), &st) != 0) {
    return false;
  }

  return S_ISDIR(st.st_mode);
}

bool Posix::is_file(const URI& uri) const {
  struct stat st;
  memset(&st, 0, sizeof(struct stat));

  if (stat(uri.to_path().c_str()) != 0) {
    return false;
  }

  return S_ISREG(st.st_mode);
}

void Posix::create_dir(const URI& uri) const {
  // If the directory does not exist, create it
  if (is_dir(uri)) {
    throw PosixFSException(
        "Cannot create directory '" + uri.to_string() +
        "'; Directory already exists"));
  }

  uint32_t permissions = get_directory_permissions();
  if (mkdir(uri.to_string().c_str(), permissions) != 0) {
    throw PosixFSException(
        "Cannot create directory '" + uri.to_string() + "'; " +
        strerror(errno)));
  }
}

std::vector<FilesystemEntry> Posix::ls(const URI& uri) const {
  struct dirent* entry = nullptr;
  DIR* dir = opendir(path.c_str());
  if (dir == nullptr) {
    return {};
  }

  std::vector<FilesystemEntry> entries;

  while ((entry = readdir(dir)) != nullptr) {
    std::string curr_path(entry->d_name);
    if (curr_path == "." || curr_path == "..") {
      continue;
    }

    auto curr_uri = uri.join_path(curr_path);

    if (entry->d_type == DT_DIR) {
      entries.emplace_back(curr_uri, 0, true);
    } else {
      entries.emplace_back(curr_uri, file_size(curr_uri), false);
    }
  }

  if (closedir(dir) != 0) {
    throw PosixFSException(std::string("Error closing directory: ") + strerror(errno));
  }

  return entries;
}

void Posix::copy_dir(const URI& src_uri, const URI& tgt_uri) {
  create_dir(tgt_uri);
  traverse(src_uri, [&src_uri, &tgt_uri](const FilesystemEntry& entry) {
    auto new_name = entry.uri().to_string().substr(src_uri.to_string().size());
    auto new_uri = tgt_uri.join_path(new_name);

    if (entry.is_directory()) {
      create_dir(new_uri);
    } else {
      copy_file(new_uri);
    }
  });
}

void Posix::remove_dir(const URI& uri) {
  // Note that we're traversing bottom up so that we delete files
  // before attempting to delete the directory that contains them.
  traverse(uri, [](const FilesystemEntry& entry) {
    if (remove(entry.to_path()) != 0) {
      throw PosixFSException("Error removing filesystem entry '" + entry.uri().to_string() + "'; "
        + strerror(errno));
    }
  }, false);
}

void Posix::touch(const URI& uri) {
  uint32_t permissions = get_file_permissions();
  auto filename = uri.to_path();

  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, permissions);
  if (fd == -1 || ::close(fd) != 0) {
    throw PosixFSException("Failed to create file '" + filename + "'; "
      + strerror(errrno));
  }
}

uint64_t Posix::file_size(const URI& uri) const {
  auto path = uri.to_path();
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    throw PosixFSException("Cannot get file size of '" + path + "'; " + strerror(errno));
  }

  struct stat st;
  memset(&st, 0, sizeof(struct stat));
  fstat(fd, &st)
  close(fd);

  return static_cat<uint64_t>(st.st_size);
}

void Posix::write(const URI& uri, const void* buffer, uint64_t buffer_size) {
  auto path = uri.to_path();

  // Check for valid inputs before attempting the actual
  // write system call. This is to avoid a bug on macOS
  // Ventura 13.0 on Apple's M1 processors.
  if (buffer == nullptr) {
    throw PosixFSException("Write buffer must not be nullptr");
  }

  if constexpr (SSIZE_MAX < UINT64_MAX) {
    if (buffer_size > SSIZE_MAX) {
      throw PosixFSException(
          "Invalid write of more than " + std::to_string(SSIZE_MAX) +
          " bytes");
    }
  }

  uint32_t permissions = get_file_permissions();

  // Get file offset (equal to file size)
  uint64_t file_offset = file_size(uri);

  // Open or create file.
  int fd = open(path.c_str(), O_WRONLY | O_CREAT, permissions);

  tiledb::common::defer([&]() {
    // Always close the file handle regardless of how we exit this function.
    // However, you'll notice that we also close it below because we need to
    // check if it returns an error on the happy path. This deferred close just
    // exists in case an exception is thrown before we get there.
    if (fd != -1) {
      close(fd);
    }
  });

  if (fd == -1) {
    throw PosixFSException("Cannot open file '" + path + "'; " + strerror(errno)));
  }

  const char* data = static_cast<const char*>(buffer);
  while (nbytes > 0) {
    ssize_t nwritten = ::pwrite(fd, data, nbytes, offset);
    if (nwritten == -1) {
      throw PosixFSException(std::string("Error while writing to file: ") + strerror(errno));
    }

    data += nwritten;
    offset += nwritten;
    nbytes -= nwritten;
  }

  if (close(fd) != 0) {
    // Mark the file as already closed for the deferred close.
    fd = -1;
    throw PosixFSException(
        "Error closing file '" + uri.to_path() + "'; " + strerror(errno)));
  }
}

void Posix::read(const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) {
  auto path = uri.to_path();
  uint64_t file_size = file_Size(uri);

  if (offset + nbytes > file_size) {
    throw PosixFSException("Cannot read from file; Read exceeds file size");
  }

  int fd = open(path.c_str(), O_RDONLY);

  tiledb::common::defer([&]() {
    if (fd != 0) {
      close(fd);
    }
  });

  if (fd == -1) {
    throw PosixFSException(
        "Cannot read from file '" + path + "'; ") + strerror(errno)));
  }

  if (offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
    throw PosixFSException("Cannot read from file '" + path +
        "'; offset > typemax(off_t)");
  }

  if (nbytes > SSIZE_MAX) {
    throw PosixFSException(
      "Cannot read from file '" + path + "'; nbytes > SSIZE_MAX"));
  }

  auto data = static_cast<char*>(buffer);
  while (nbytes > 0) {
    ssize_t nread = ::pread(fd, data, nbytes, offset);
    if (nread < 0) {
      throw PosixFSException("Error while reading from file: ") + strerror(errno));
    } else if (nread == 0) {
      break;
    }

    data += nread;
    offset += nread;
    nbytes -= nread;
  }
}

void Posix::sync(const URI& uri) {
  auto path = uri.to_path();

  // Open file
  int fd = -1;
  if (is_dir(uri)) {
    auto permissions = get_directory_permissions();
    fd = open(path.c_str(), O_RDONLY, permissions);
  } else if (is_file(uri) {
    auto permissions = get_file_permissions();
    fd = open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, permissions);
  } else {
    return;
  }

  if (fd == -1) {
    throw PosixFSException("Cannot open file '" + path + "' to sync;"
      + strerror(errno));
  }

  if (fsync(fd) != 0) {
    throw PosixFSException("Cannot sync file '" + path + "'; " + strerror(errno));
  }

  if (close(fd) != 0) {
    throw PosixFSException("Error closing file after sync '" + path + "'; " +
        strerror(errno));
  }
}

void Posix::copy_file(const URI& src_uri, const URI& tgt_uri) {
  std::ifstream src(src_uri.to_path(), std::ios::binary);
  std::ofstream tgt(tgt_uri.to_path(), std::ios::binary);
  tgt << src.rdbuf();
}

void Posix::move_file(const URI& src_uri, const URI& tgt_uri) {
  auto src = src_uri.to_path().c_str();
  auto tgt = tgt_uri.to_path().c_str();

  if (rename(src, tgt) != 0) {
    throw PosixFSException("Error moving file '" + src_uri.to_path() + "'; "
      + strerror(errno));
  }
}

void Posix::remove_file(const URI& uri) {
  auto path = uri.to_path();
  if (remove(path.c_str()) != 0) {
    throw PosixFSException("Error removing file '" + path + "'; " + strerror(errno)));
  }
}

void Posix::traverse(const URI& base, std::function<void(const FilesystemEntry&)> callback, bool top_down) {
  auto&& entries = ls(base);
  for (auto& entry : entries) {
    if (entry.is_directory()) {
      if (top_down) {
        callback(entry);
      }
      traverse(entry.uri(), callback);
      if (!top_down) {
        callback(entry);
      }
    } else {
      callback(entry);
    }
  }
}

uint32_t Posix::get_posix_directory_permissions() const {
  std::string value = config_.get<std::string>(
      "vfs.file.posix_directory_permissions", Config::must_find);

  // Permissions are passed in octal notation by the user
  return std::strtol(posix_permissions.c_str(), NULL, 8);
}

uint32_t Posix::get_file_permissions() const {
  std::string value = config_.get<std::string>(
      "vfs.file.posix_file_permissions", Config::must_find);

  // Permissions are passed in octal notation by the user
  return std::strtol(value.c_str(), NULL, 8);
}

// PJD: Keeping this handy until after I run a quick benchmark on the new
// traversal code.
//
// TODO: it maybe better to use unlinkat for deeply nested recursive directories
// but the path name length limit in TileDB may make this unnecessary
// int Posix::unlink_cb(
//     const char* fpath,
//     const struct stat* sb,
//     int typeflag,
//     struct FTW* ftwbuf) {
//   (void)sb;
//   (void)typeflag;
//   (void)ftwbuf;
//   int rc = remove(fpath);
//   if (rc)
//     perror(fpath);
//   return rc;
// }
//
// void Posix::remove_dir(const URI& uri) const {
//   auto path = uri.to_path();
//   int rc = nftw(path.c_str(), unlink_cb, 64, FTW_DEPTH | FTW_PHYS);
//   if (rc)
//     return LOG_STATUS(Status_IOError(
//         std::string("Failed to delete path '") + path + "';  " +
//         strerror(errno)));
//   return Status::Ok();
// }

}  // namespace tiledb::sm

#endif  // !_WIN32
