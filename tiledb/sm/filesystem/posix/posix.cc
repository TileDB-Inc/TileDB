/**
 * @file   posix.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#include "tiledb/sm/filesystem/posix.h"
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "tiledb/sm/misc/utils.h"
#include "uri.h"

namespace tiledb::sm {

class PosixFSException : public FilesystemException {
 public:
  explicit FilesystemException(const std::string& message)
      : StatusException("PosixFilesystem", message) {
  }
};

Posix::Posix(const Config& config)
    : FilesystemBase(config) {
  fs_type_ = FilesystemType::POSIX;
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

void Posix::touch(const URI& uri) const {
  uint32_t permissions = get_file_permissions();
  auto filename = uri.to_path();

  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, permissions);
  if (fd == -1 || ::close(fd) != 0) {
    throw PosixFSException("Failed to create file '" + filename + "'; "
      + strerror(errrno));
  }
}




















Status Posix::read_all(int fd, void* buffer, uint64_t nbytes, uint64_t offset) {
  auto bytes = reinterpret_cast<char*>(buffer);
  uint64_t nread = 0;
  do {
    ssize_t actual_read =
        ::pread(fd, bytes + nread, nbytes - nread, offset + nread);
    if (actual_read < 0) {
      return LOG_STATUS(
          Status_IOError(std::string("POSIX read error: ") + strerror(errno)));
    } else if (actual_read == 0) {
      break;
    }
    nread += actual_read;
  } while (nread < nbytes);

  if (nread != nbytes) {
    return LOG_STATUS(Status_IOError("POSIX incomplete read: EOF reached"));
  }
  return Status::Ok();
}

void Posix::adjacent_slashes_dedup(std::string* path) {
  assert(utils::parse::starts_with(*path, "file://"));
  path->erase(
      std::unique(
          path->begin() + std::string("file://").size(),
          path->end(),
          both_slashes),
      path->end());
}

std::string Posix::abs_path(const std::string& path) {
  std::string resolved_path = abs_path_internal(path);

  // Ensure the returned has the same postfix slash as 'path'.
  if (utils::parse::ends_with(path, "/")) {
    if (!utils::parse::ends_with(resolved_path, "/")) {
      resolved_path = resolved_path + "/";
    }
  } else {
    if (utils::parse::ends_with(resolved_path, "/")) {
      resolved_path = resolved_path.substr(0, resolved_path.length() - 1);
    }
  }

  return resolved_path;
}

std::string Posix::abs_path_internal(const std::string& path) {
  // Initialize current, home and root
  std::string current = current_dir();
  auto env_home_ptr = getenv("HOME");
  std::string home = env_home_ptr != nullptr ? env_home_ptr : current;
  std::string root = "/";
  std::string posix_prefix = "file://";

  // Easy cases
  if (path.empty() || path == "." || path == "./")
    return posix_prefix + current;
  if (path == "~")
    return posix_prefix + home;
  if (path == "/")
    return posix_prefix + root;

  // Other cases
  std::string ret_dir;
  if (utils::parse::starts_with(path, posix_prefix))
    return path;
  else if (utils::parse::starts_with(path, "/"))
    ret_dir = posix_prefix + path;
  else if (utils::parse::starts_with(path, "~/"))
    ret_dir = posix_prefix + home + path.substr(1, path.size() - 1);
  else if (utils::parse::starts_with(path, "./"))
    ret_dir = posix_prefix + current + path.substr(1, path.size() - 1);
  else
    ret_dir = posix_prefix + current + "/" + path;

  adjacent_slashes_dedup(&ret_dir);
  purge_dots_from_path(&ret_dir);

  return ret_dir;
}





std::string Posix::current_dir() {
  static std::unique_ptr<char, decltype(&free)> cwd_(getcwd(nullptr, 0), free);
  std::string dir = cwd_.get();
  return dir;
}

// TODO: it maybe better to use unlinkat for deeply nested recursive directories
// but the path name length limit in TileDB may make this unnecessary
int Posix::unlink_cb(
    const char* fpath,
    const struct stat* sb,
    int typeflag,
    struct FTW* ftwbuf) {
  (void)sb;
  (void)typeflag;
  (void)ftwbuf;
  int rc = remove(fpath);
  if (rc)
    perror(fpath);
  return rc;
}

void Posix::remove_dir(const URI& uri) const {
  auto path = uri.to_path();
  int rc = nftw(path.c_str(), unlink_cb, 64, FTW_DEPTH | FTW_PHYS);
  if (rc)
    return LOG_STATUS(Status_IOError(
        std::string("Failed to delete path '") + path + "';  " +
        strerror(errno)));
  return Status::Ok();
}

Status Posix::remove_file(const URI& uri) const {
  auto path = uri.to_path();
  if (remove(path.c_str()) != 0) {
    return LOG_STATUS(Status_IOError(
        std::string("Cannot delete file '") + path + "'; " + strerror(errno)));
  }
  return Status::Ok();
}

Status Posix::file_size(const URI& uri, uint64_t* size) const {
  auto path = uri.to_path();
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    return LOG_STATUS(Status_IOError(
        "Cannot get file size of '" + path + "'; " + strerror(errno)));
  }

  struct stat st;
  fstat(fd, &st);
  *size = (uint64_t)st.st_size;

  close(fd);
  return Status::Ok();
}




Status Posix::ls(
    const std::string& path, std::vector<std::string>* paths) const {
  auto&& [st, entries] = ls_with_sizes(URI(path));

  RETURN_NOT_OK(st);

  for (auto& fs : *entries) {
    paths->emplace_back(fs.path().native());
  }

  return Status::Ok();
}



Status Posix::move_file(const URI& old_path, const URI& new_path) {
  if (rename(old_path.to_path().c_str(), new_path.to_path().c_str()) != 0) {
    return LOG_STATUS(
        Status_IOError(std::string("Cannot move path: ") + strerror(errno)));
  }
  return Status::Ok();
}

Status Posix::copy_file(const URI& old_uri, const URI& new_uri) {
  std::ifstream src(old_uri.to_path(), std::ios::binary);
  std::ofstream dst(new_uri.to_path(), std::ios::binary);
  dst << src.rdbuf();
  return Status::Ok();
}


void Posix::purge_dots_from_path(std::string* path) {
  // Trivial case
  if (path == nullptr)
    return;

  // Trivial case
  uint64_t path_size = path->size();
  if (path_size == 0 || *path == "file:///")
    return;

  assert(utils::parse::starts_with(*path, "file:///"));

  // Tokenize
  const char* token_c_str = path->c_str() + 8;
  std::vector<std::string> tokens, final_tokens;
  std::string token;

  for (uint64_t i = 8; i < path_size; ++i) {
    if ((*path)[i] == '/') {
      (*path)[i] = '\0';
      token = token_c_str;
      if (!token.empty())
        tokens.push_back(token);
      token_c_str = path->c_str() + i + 1;
    }
  }
  token = token_c_str;
  if (!token.empty())
    tokens.push_back(token);

  // Purge dots
  for (auto& t : tokens) {
    if (t == ".")  // Skip single dots
      continue;

    if (t == "..") {
      if (final_tokens.empty()) {
        // Invalid path
        *path = "";
        return;
      }

      final_tokens.pop_back();
    } else {
      final_tokens.push_back(t);
    }
  }

  // Assemble final path
  *path = "file://";
  for (auto& t : final_tokens)
    *path += std::string("/") + t;
}

Status Posix::read(
    const URI& uri,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes,
    bool use_read_ahead) {
  // Checks
  auto path = uri.to_path();
  uint64_t file_size;
  RETURN_NOT_OK(this->file_size(URI(path), &file_size));
  if (offset + nbytes > file_size)
    return LOG_STATUS(
        Status_IOError("Cannot read from file; Read exceeds file size"));

  // Open file
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    return LOG_STATUS(Status_IOError(
        std::string("Cannot read from file; ") + strerror(errno)));
  }
  if (offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
    return LOG_STATUS(Status_IOError(
        std::string("Cannot read from file ' ") + path.c_str() +
        "'; offset > typemax(off_t)"));
  }
  if (nbytes > SSIZE_MAX) {
    return LOG_STATUS(Status_IOError(
        std::string("Cannot read from file ' ") + path +
        "'; nbytes > SSIZE_MAX"));
  }
  Status st = read_all(fd, buffer, nbytes, offset);
  // Close file
  if (close(fd)) {
    LOG_STATUS_NO_RETURN_VALUE(
        Status_IOError(std::string("Cannot close file; ") + strerror(errno)));
  }
  return st;
}

Status Posix::sync(const URI& uri) {
  auto path = uri.to_path();
  uint32_t permissions = 0;

  // Open file
  int fd = -1;
  if (is_dir(URI(path))) {  // DIRECTORY
    RETURN_NOT_OK(get_posix_directory_permissions(&permissions));
    fd = open(path.c_str(), O_RDONLY, permissions);
  } else if (is_file(URI(path))) {  // FILE
    RETURN_NOT_OK(get_posix_file_permissions(&permissions));
    fd = open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, permissions);
  } else
    return Status_Ok();  // If file does not exist, exit

  // Handle error
  if (fd == -1) {
    return LOG_STATUS(Status_IOError(
        std::string("Cannot open file '") + path + "' for syncing; " +
        strerror(errno)));
  }

  // Sync
  if (fsync(fd) != 0) {
    return LOG_STATUS(Status_IOError(
        std::string("Cannot sync file '") + path + "'; " + strerror(errno)));
  }

  // Close file
  if (close(fd) != 0) {
    return LOG_STATUS(Status_IOError(
        std::string("Cannot close synced file '") + path + "'; " +
        strerror(errno)));
  }

  // Success
  return Status::Ok();
}

Status Posix::write(
    const URI& uri,
    const void* buffer,
    uint64_t buffer_size,
    bool remote_global_order_write) {
  auto path = uri.to_path();
  // Check for valid inputs before attempting the actual
  // write system call. This is to avoid a bug on macOS
  // Ventura 13.0 on Apple's M1 processors.
  if (buffer == nullptr) {
    throw std::invalid_argument("buffer must not be nullptr");
  }
  if constexpr (SSIZE_MAX < UINT64_MAX) {
    if (buffer_size > SSIZE_MAX) {
      throw std::invalid_argument(
          "invalid write with more than " + std::to_string(SSIZE_MAX) +
          " bytes");
    }
  }

  uint32_t permissions = 0;
  RETURN_NOT_OK(get_posix_file_permissions(&permissions));

  // Get file offset (equal to file size)
  Status st;
  uint64_t file_offset = 0;
  if (is_file(URI(path))) {
    st = file_size(URI(path), &file_offset);
    if (!st.ok()) {
      std::stringstream errmsg;
      errmsg << "Cannot write to file '" << path << "'; " << st.message();
      return LOG_STATUS(Status_IOError(errmsg.str()));
    }
  }

  // Open or create file.
  int fd = open(path.c_str(), O_WRONLY | O_CREAT, permissions);
  if (fd == -1) {
    return LOG_STATUS(Status_IOError(
        std::string("Cannot open file '") + path + "'; " + strerror(errno)));
  }

  st = write_at(fd, file_offset, buffer, buffer_size);
  if (!st.ok()) {
    close(fd);
    std::stringstream errmsg;
    errmsg << "Cannot write to file '" << path << "'; " << st.message();
    return LOG_STATUS(Status_IOError(errmsg.str()));
  }
  if (close(fd) != 0) {
    return LOG_STATUS(Status_IOError(
        std::string("Cannot close file '") + path + "'; " + strerror(errno)));
  }
  return st;
}

Status Posix::write_at(
    int fd, uint64_t file_offset, const void* buffer, uint64_t buffer_size) {
  const char* buffer_bytes_ptr = static_cast<const char*>(buffer);
  while (buffer_size > 0) {
    ssize_t actual_written =
        ::pwrite(fd, buffer_bytes_ptr, buffer_size, file_offset);
    if (actual_written == -1) {
      return LOG_STATUS(
          Status_IOError(std::string("POSIX write error:") + strerror(errno)));
    }
    buffer_bytes_ptr += actual_written;
    file_offset += actual_written;
    buffer_size -= actual_written;
  }
  return Status::Ok();
}

Status Posix::get_posix_file_permissions(uint32_t* permissions) const {
  // Get config params
  std::string posix_permissions = config_.get<std::string>(
      "vfs.file.posix_file_permissions", Config::must_find);

  // Permissions are passed in octal notation by the user
  *permissions = std::strtol(posix_permissions.c_str(), NULL, 8);

  return Status::Ok();
}

Status Posix::get_posix_directory_permissions(uint32_t* permissions) const {
  // Get config params
  std::string posix_permissions = config_.get<std::string>(
      "vfs.file.posix_directory_permissions", Config::must_find);

  // Permissions are passed in octal notation by the user
  *permissions = std::strtol(posix_permissions.c_str(), NULL, 8);

  return Status::Ok();
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

}  // namespace tiledb::sm

#endif  // !_WIN32
