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

#include "tiledb/sm/filesystem/posix.h"
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "tiledb/sm/misc/utils.h"
#include "uri.h"

#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>

#include <fstream>
#include <future>
#include <iostream>
#include <queue>
#include <sstream>

using namespace tiledb::common;
using tiledb::common::filesystem::directory_entry;

namespace tiledb {
namespace sm {

Posix::Posix()
    : default_config_(Config())
    , config_(default_config_) {
}

bool Posix::both_slashes(char a, char b) {
  return a == '/' && b == '/';
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

Status Posix::create_dir(const std::string& path) const {
  // If the directory does not exist, create it
  if (is_dir(path)) {
    return LOG_STATUS(Status_IOError(
        std::string("Cannot create directory '") + path +
        "'; Directory already exists"));
  }

  uint32_t permissions = 0;
  RETURN_NOT_OK(get_posix_directory_permissions(&permissions));

  if (mkdir(path.c_str(), permissions) != 0) {
    return LOG_STATUS(Status_IOError(
        std::string("Cannot create directory '") + path + "'; " +
        strerror(errno)));
  }
  return Status::Ok();
}

Status Posix::touch(const std::string& filename) const {
  uint32_t permissions = 0;
  RETURN_NOT_OK(get_posix_file_permissions(&permissions));

  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, permissions);
  if (fd == -1 || ::close(fd) != 0) {
    return LOG_STATUS(Status_IOError(
        std::string("Failed to create file '") + filename + "'; " +
        strerror(errno)));
  }

  return Status::Ok();
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

Status Posix::remove_dir(const std::string& path) const {
  int rc = nftw(path.c_str(), unlink_cb, 64, FTW_DEPTH | FTW_PHYS);
  if (rc)
    return LOG_STATUS(Status_IOError(
        std::string("Failed to delete path '") + path + "';  " +
        strerror(errno)));
  return Status::Ok();
}

Status Posix::remove_file(const std::string& path) const {
  if (remove(path.c_str()) != 0) {
    return LOG_STATUS(Status_IOError(
        std::string("Cannot delete file '") + path + "'; " + strerror(errno)));
  }
  return Status::Ok();
}

Status Posix::file_size(const std::string& path, uint64_t* size) const {
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

Status Posix::init(const Config& config) {
  config_ = config;

  return Status::Ok();
}

bool Posix::is_dir(const std::string& path) const {
  struct stat st;
  memset(&st, 0, sizeof(struct stat));
  return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

bool Posix::is_file(const std::string& path) const {
  struct stat st;
  memset(&st, 0, sizeof(struct stat));
  return (stat(path.c_str(), &st) == 0) && !S_ISDIR(st.st_mode);
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

tuple<Status, optional<std::vector<directory_entry>>> Posix::ls_with_sizes(
    const URI& uri) const {
  std::string path = uri.to_path();
  struct dirent* next_path = nullptr;
  DIR* dir = opendir(path.c_str());
  if (dir == nullptr) {
    return {Status::Ok(), nullopt};
  }

  std::vector<directory_entry> entries;

  while ((next_path = readdir(dir)) != nullptr) {
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
      RETURN_NOT_OK_TUPLE(file_size(abspath, &size), nullopt);
      entries.emplace_back(abspath, size, false);
    }
  }
  // close parent directory
  if (closedir(dir) != 0) {
    auto st = LOG_STATUS(Status_IOError(
        std::string("Cannot close parent directory; ") + strerror(errno)));
    return {st, nullopt};
  }
  return {Status::Ok(), entries};
}

tuple<Status, optional<std::vector<filesystem::directory_entry>>>
Posix::ls_recursive(const URI& path, int64_t max_paths) const {
  std::vector<directory_entry> entries;
  std::queue<URI> q;
  q.push(path);

  while (!q.empty()) {
    auto&& [st, results] = ls_with_sizes(q.front());
    RETURN_NOT_OK_TUPLE(st, nullopt);
    // Sort the results to avoid strange collections when pruned by max_paths.
    parallel_sort(
        vfs_thread_pool_,
        results->begin(),
        results->end(),
        [](const directory_entry& l, const directory_entry& r) {
          return l.path().native() < r.path().native();
        });
    for (const auto& result : *results) {
      if (result.is_directory()) {
        q.emplace(result.path().native());
      }

      entries.push_back(result);
      if (static_cast<int64_t>(entries.size()) == max_paths) {
        return {Status::Ok(), entries};
      }
    }
    q.pop();
  }

  return {Status::Ok(), entries};
}

Status Posix::move_path(
    const std::string& old_path, const std::string& new_path) {
  if (rename(old_path.c_str(), new_path.c_str()) != 0) {
    return LOG_STATUS(
        Status_IOError(std::string("Cannot move path: ") + strerror(errno)));
  }
  return Status::Ok();
}

Status Posix::copy_file(
    const std::string& old_path, const std::string& new_path) {
  std::ifstream src(old_path, std::ios::binary);
  std::ofstream dst(new_path, std::ios::binary);
  dst << src.rdbuf();
  return Status::Ok();
}

Status Posix::copy_dir(
    const std::string& old_path, const std::string& new_path) {
  RETURN_NOT_OK(create_dir(new_path));
  std::vector<std::string> paths;
  RETURN_NOT_OK(ls(old_path, &paths));

  std::queue<std::string> path_queue;
  for (auto& path : paths)
    path_queue.emplace(std::move(path));

  while (!path_queue.empty()) {
    std::string file_name_abs = path_queue.front();
    std::string file_name = file_name_abs.substr(old_path.length());
    path_queue.pop();

    if (is_dir(file_name_abs)) {
      RETURN_NOT_OK(create_dir(new_path + "/" + file_name));
      std::vector<std::string> child_paths;
      RETURN_NOT_OK(ls(file_name_abs, &child_paths));
      for (auto& path : child_paths)
        path_queue.emplace(std::move(path));
    } else {
      assert(is_file(file_name_abs));
      RETURN_NOT_OK(
          copy_file(old_path + "/" + file_name, new_path + "/" + file_name));
    }
  }

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
    const std::string& path,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes) const {
  // Checks
  uint64_t file_size;
  RETURN_NOT_OK(this->file_size(path, &file_size));
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

Status Posix::sync(const std::string& path) {
  uint32_t permissions = 0;

  // Open file
  int fd = -1;
  if (is_dir(path)) {  // DIRECTORY
    RETURN_NOT_OK(get_posix_directory_permissions(&permissions));
    fd = open(path.c_str(), O_RDONLY, permissions);
  } else if (is_file(path)) {  // FILE
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
    const std::string& path, const void* buffer, uint64_t buffer_size) {
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
  if (is_file(path)) {
    st = file_size(path, &file_offset);
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
  bool found = false;
  std::string posix_permissions =
      config_.get().get("vfs.file.posix_file_permissions", &found);
  assert(found);

  // Permissions are passed in octal notation by the user
  *permissions = std::strtol(posix_permissions.c_str(), NULL, 8);

  return Status::Ok();
}

Status Posix::get_posix_directory_permissions(uint32_t* permissions) const {
  // Get config params
  bool found = false;
  std::string posix_permissions =
      config_.get().get("vfs.file.posix_directory_permissions", &found);
  assert(found);

  // Permissions are passed in octal notation by the user
  *permissions = std::strtol(posix_permissions.c_str(), NULL, 8);

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // !_WIN32
