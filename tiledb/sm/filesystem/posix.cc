/**
 * @file   posix.cc
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
 * This file includes definitions of POSIX filesystem functions.
 */

#ifndef _WIN32

#include "tiledb/sm/filesystem/posix.h"
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "uri.h"

#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <format>
#include <fstream>
#include <future>
#include <iostream>
#include <queue>
#include <sstream>

using namespace tiledb::common;
using filesystem::directory_entry;

namespace tiledb::sm {

/**
 * A class that wraps a POSIX DIR* and closes it when it goes out of scope.
 */
class PosixDIR {
 public:
  DISABLE_COPY_AND_COPY_ASSIGN(PosixDIR);
  DISABLE_MOVE_AND_MOVE_ASSIGN(PosixDIR);

  /** Destructor. */
  ~PosixDIR() {
    if (dir_.has_value() && dir_.value() != nullptr) {
      // The only possible error is EBADF, which should not happen here.
      [[maybe_unused]] auto status = closedir(dir_.value());
      assert(status == 0);
    }
  }

  /** Returns the wrapped directory pointer. */
  DIR* get() const {
    return dir_.value();
  }

  /** Returns if the dir is empty. */
  bool empty() {
    return !dir_.has_value();
  }

  /**
   * Opens a directory and returns a UniqueDIR if it exists.
   *
   * @param path The path to the directory to open.
   * @return A UniqueDIR if the directory was opened successfully, or nullopt if
   *     the directory does not exist.
   * @throws IOError an error occurs while opening the directory.
   */
  static PosixDIR open(const std::string& path) {
    DIR* dir = opendir(path.c_str());
    if (dir == nullptr) {
      auto last_error = errno;
      if (last_error == ENOENT) {
        return {};
      }
      throw IOError(
          std::string("Cannot open directory; ") + strerror(last_error));
    }
    return {dir};
  }

 private:
  /**
   * Internal constructor that gets called from the `open` method.
   *
   * @param dir The directory pointer to wrap.
   */
  PosixDIR(optional<DIR*> dir = nullopt)
      : dir_(dir) {
    assert(dir != nullptr);
  }

  /** The wrapped directory pointer. */
  optional<DIR*> dir_;
};

Posix::Posix(const Config& config) {
  // Initialize member variables with posix config parameters.

  // File and directory permissions are set by the user in octal.
  std::string permissions = config.get<std::string>(
      "vfs.file.posix_file_permissions", Config::must_find);
  file_permissions_ = std::strtol(permissions.c_str(), nullptr, 8);
  permissions = config.get<std::string>(
      "vfs.file.posix_directory_permissions", Config::must_find);
  directory_permissions_ = std::strtol(permissions.c_str(), nullptr, 8);
}

void Posix::create_dir(const URI& uri) const {
  // If the directory does not exist, create it
  auto path = uri.to_path();
  if (is_dir(uri)) {
    throw IOError(
        std::string("Cannot create directory '") + path +
        "'; Directory already exists");
  }

  if (mkdir(path.c_str(), directory_permissions_) != 0) {
    throw IOError(
        std::string("Cannot create directory '") + path + "'; " +
        strerror(errno));
  }
}

void Posix::touch(const URI& uri) const {
  auto filename = uri.to_path();

  int fd =
      ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, file_permissions_);
  if (fd == -1 || ::close(fd) != 0) {
    throw IOError(
        std::string("Failed to create file '") + filename + "'; " +
        strerror(errno));
  }
}

bool Posix::is_dir(const URI& uri) const {
  struct stat st;
  memset(&st, 0, sizeof(struct stat));
  return stat(uri.to_path().c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

bool Posix::is_file(const URI& uri) const {
  struct stat st;
  memset(&st, 0, sizeof(struct stat));
  return (stat(uri.to_path().c_str(), &st) == 0) && !S_ISDIR(st.st_mode);
}

void Posix::remove_dir(const URI& uri) const {
  auto path = uri.to_path();
  int rc = nftw(path.c_str(), unlink_cb, 64, FTW_DEPTH | FTW_PHYS);
  if (rc) {
    throw IOError(
        std::string("Failed to delete path '") + path + "';  " +
        strerror(errno));
  }
}

bool Posix::remove_dir_if_empty(const std::string& path) const {
  if (rmdir(path.c_str()) != 0) {
    if (errno == ENOTEMPTY) {
      return false;
    }
    throw IOError(
        std::string("Failed to delete path '") + path + "';  " +
        strerror(errno));
  }
  return true;
}

void Posix::remove_file(const URI& uri) const {
  auto path = uri.to_path();
  if (remove(path.c_str()) != 0) {
    throw IOError(
        std::string("Cannot delete file '") + path + "'; " + strerror(errno));
  }
}

void Posix::file_size(const URI& uri, uint64_t* size) const {
  auto path = uri.to_path();
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    throw IOError("Cannot get file size of '" + path + "'; " + strerror(errno));
  }

  struct stat st;
  fstat(fd, &st);
  *size = (uint64_t)st.st_size;

  close(fd);
}

void Posix::move_file(const URI& old_path, const URI& new_path) const {
  if (rename(old_path.to_path().c_str(), new_path.to_path().c_str()) != 0) {
    throw IOError(std::string("Cannot move path: ") + strerror(errno));
  }
}

void Posix::move_dir(const URI& old_uri, const URI& new_uri) const {
  move_file(old_uri, new_uri);
}

void Posix::copy_file(const URI& old_uri, const URI& new_uri) const {
  std::ifstream src(old_uri.to_path(), std::ios::binary);
  std::ofstream dst(new_uri.to_path(), std::ios::binary);
  dst << src.rdbuf();
}

void Posix::copy_dir(const URI& old_uri, const URI& new_uri) const {
  auto old_path = old_uri.to_path();
  auto new_path = new_uri.to_path();
  create_dir(new_uri);
  std::vector<std::string> paths;
  throw_if_not_ok(ls(old_path, &paths));

  std::queue<std::string> path_queue;
  for (auto& path : paths)
    path_queue.emplace(std::move(path));

  while (!path_queue.empty()) {
    const std::string file_name_abs = path_queue.front();
    const std::string file_name = file_name_abs.substr(old_path.length());
    path_queue.pop();

    if (is_dir(URI(file_name_abs))) {
      create_dir(URI(new_path + "/" + file_name));
      std::vector<std::string> child_paths;
      throw_if_not_ok(ls(file_name_abs, &child_paths));
      for (auto& path : child_paths)
        path_queue.emplace(std::move(path));
    } else {
      assert(is_file(URI(file_name_abs)));
      copy_file(
          URI(old_path + "/" + file_name), URI(new_path + "/" + file_name));
    }
  }
}

void Posix::read(
    const URI& uri,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes,
    [[maybe_unused]] bool use_read_ahead) const {
  // Checks
  auto path = uri.to_path();
  uint64_t file_size;
  this->file_size(URI(path), &file_size);
  if (offset + nbytes > file_size) {
    throw IOError(std::format(
        "Cannot read from file; Read exceeds file size: offset {}, nbytes {}, "
        "file_size {}, URI {}",
        offset,
        nbytes,
        file_size,
        uri.to_path()));
  }

  // Open file
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    throw IOError(std::string("Cannot read from file; ") + strerror(errno));
  }
  if (offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
    throw IOError(
        std::string("Cannot read from file ' ") + path.c_str() +
        "'; offset > typemax(off_t)");
  }
  if (nbytes > SSIZE_MAX) {
    throw IOError(
        std::string("Cannot read from file ' ") + path +
        "'; nbytes > SSIZE_MAX");
  }
  throw_if_not_ok(read_all(fd, buffer, nbytes, offset));
  // Close file
  if (close(fd)) {
    LOG_STATUS_NO_RETURN_VALUE(
        Status_IOError(std::string("Cannot close file; ") + strerror(errno)));
  }
}

void Posix::sync(const URI& uri) const {
  auto path = uri.to_path();

  // Open file
  int fd = -1;
  if (is_dir(URI(path))) {  // DIRECTORY
    fd = open(path.c_str(), O_RDONLY, directory_permissions_);
  } else if (is_file(URI(path))) {  // FILE
    fd = open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, file_permissions_);
  } else {
    return;  // If file does not exist, exit
  }

  // Handle error
  if (fd == -1) {
    throw IOError(
        std::string("Cannot open file '") + path + "' for syncing; " +
        strerror(errno));
  }

  // Sync
  if (fsync(fd) != 0) {
    throw IOError(
        std::string("Cannot sync file '") + path + "'; " + strerror(errno));
  }

  // Close file
  if (close(fd) != 0) {
    throw IOError(
        std::string("Cannot close synced file '") + path + "'; " +
        strerror(errno));
  }
}

void Posix::write(
    const URI& uri,
    const void* buffer,
    uint64_t buffer_size,
    [[maybe_unused]] bool remote_global_order_write) {
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

  // Get file offset (equal to file size)
  Status st;
  uint64_t file_offset = 0;
  if (is_file(URI(path))) {
    file_size(URI(path), &file_offset);
  }

  // Open or create file.
  int fd = open(path.c_str(), O_WRONLY | O_CREAT, file_permissions_);
  if (fd == -1) {
    throw IOError(
        std::string("Cannot open file '") + path + "'; " + strerror(errno));
  }

  st = write_at(fd, file_offset, buffer, buffer_size);
  if (!st.ok()) {
    close(fd);
    std::stringstream errmsg;
    errmsg << "Cannot write to file '" << path << "'; " << st.message();
    throw IOError(errmsg.str());
  }
  if (close(fd) != 0) {
    throw IOError(
        std::string("Cannot close file '") + path + "'; " + strerror(errno));
  }
}

std::vector<directory_entry> Posix::ls_with_sizes(const URI& uri) const {
  std::string path = uri.to_path();
  struct dirent* next_path = nullptr;
  auto dir = PosixDIR::open(path);
  if (dir.empty()) {
    return {};
  }

  std::vector<directory_entry> entries;

  while ((next_path = readdir(dir.get())) != nullptr) {
    if (!strcmp(next_path->d_name, ".") || !strcmp(next_path->d_name, ".."))
      continue;
    std::string abspath = path + "/" + next_path->d_name;

    // Getting the file size here incurs an additional system call
    // via file_size() and ls() calls will feel this too.
    // If this penalty becomes noticeable, we should just duplicate
    // this implementation in ls() and don't get the size
    if (next_path->d_type == DT_DIR) {
      entries.emplace_back(abspath, 0, true);
    } else {
      uint64_t size;
      file_size(URI(abspath), &size);
      entries.emplace_back(abspath, size, false);
    }
  }
  return entries;
}

Status Posix::ls(
    const std::string& path, std::vector<std::string>* paths) const {
  for (auto& fs : ls_with_sizes(URI(path))) {
    paths->emplace_back(fs.path().native());
  }

  return Status::Ok();
}

std::string Posix::abs_path(std::string_view path) {
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

std::string Posix::current_dir() {
  static std::unique_ptr<char, decltype(&free)> cwd_(getcwd(nullptr, 0), free);
  std::string dir = cwd_.get();
  return dir;
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

bool Posix::both_slashes(char a, char b) {
  return a == '/' && b == '/';
}

std::string Posix::abs_path_internal(std::string_view path) {
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
    return std::string(path);
  else if (utils::parse::starts_with(path, "/"))
    ret_dir = posix_prefix + std::string(path);
  else if (utils::parse::starts_with(path, "~/"))
    ret_dir =
        posix_prefix + home + std::string(path.substr(1, path.size() - 1));
  else if (utils::parse::starts_with(path, "./"))
    ret_dir =
        posix_prefix + current + std::string(path.substr(1, path.size() - 1));
  else
    ret_dir = posix_prefix + current + "/" + std::string(path);

  adjacent_slashes_dedup(&ret_dir);
  purge_dots_from_path(&ret_dir);

  return ret_dir;
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

// TODO: it maybe better to use unlinkat for deeply nested recursive directories
// but the path name length limit in TileDB may make this unnecessary
int Posix::unlink_cb(const char* fpath, const struct stat*, int, struct FTW*) {
  int rc = remove(fpath);
  if (rc)
    perror(fpath);
  return rc;
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

}  // namespace tiledb::sm

#endif  // !_WIN32
