/**
 * @file   posix.cc
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
 * This file includes definitions of POSIX filesystem functions.
 */

#ifndef _WIN32

#include "tiledb/sm/filesystem/posix.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"

#include <dirent.h>

#include <limits.h>

#include <ftw.h>

#include <fstream>
#include <iostream>

namespace tiledb {
namespace sm {

Posix::Posix()
    : config_(default_config_) {
}

bool Posix::both_slashes(char a, char b) {
  return a == '/' && b == '/';
}

uint64_t Posix::read_all(
    int fd, void* buffer, uint64_t nbytes, uint64_t offset) {
  auto bytes = reinterpret_cast<char*>(buffer);
  uint64_t nread = 0;
  do {
    ssize_t actual_read =
        ::pread(fd, bytes + nread, nbytes - nread, offset + nread);
    if (actual_read == -1) {
      LOG_STATUS(
          Status::Error(std::string("POSIX pread error: ") + strerror(errno)));
      return nread;
    } else {
      nread += actual_read;
    }
  } while (nread < nbytes);

  return nread;
}

uint64_t Posix::pwrite_all(
    int fd, uint64_t file_offset, const void* buffer, uint64_t nbytes) {
  auto bytes = reinterpret_cast<const char*>(buffer);
  uint64_t written = 0;
  do {
    ssize_t actual_written =
        ::pwrite(fd, bytes + written, nbytes - written, file_offset + written);
    if (actual_written == -1) {
      LOG_STATUS(
          Status::Error(std::string("POSIX write error: ") + strerror(errno)));
      return written;
    } else {
      written += actual_written;
    }
  } while (written < nbytes);

  return written;
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
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create directory '") + path +
        "'; Directory already exists"));
  }
  if (mkdir(path.c_str(), S_IRWXU) != 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create directory '") + path + "'; " +
        strerror(errno)));
  }
  return Status::Ok();
}

Status Posix::touch(const std::string& filename) const {
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if (fd == -1 || ::close(fd) != 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Failed to create file '") + filename + "'; " +
        strerror(errno)));
  }

  return Status::Ok();
}

std::string Posix::current_dir() {
  std::string dir;
  char* path = getcwd(nullptr, 0);
  if (path != nullptr) {
    dir = path;
    free(path);
  }
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
    return LOG_STATUS(Status::IOError(
        std::string("Failed to delete path '") + path + "';  " +
        strerror(errno)));
  return Status::Ok();
}

Status Posix::remove_file(const std::string& path) const {
  if (remove(path.c_str()) != 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot delete file '") + path + "'; " + strerror(errno)));
  }
  return Status::Ok();
}

Status Posix::file_size(const std::string& path, uint64_t* size) const {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    return LOG_STATUS(Status::IOError(
        "Cannot get file size of '" + path + "'; " + strerror(errno)));
  }

  struct stat st;
  fstat(fd, &st);
  *size = (uint64_t)st.st_size;

  close(fd);
  return Status::Ok();
}

Status Posix::filelock_lock(
    const std::string& filename, filelock_t* fd, bool shared) const {
  // Prepare the flock struct
  struct flock fl;
  memset(&fl, 0, sizeof(struct flock));
  if (shared)
    fl.l_type = F_RDLCK;
  else
    fl.l_type = F_WRLCK;
  fl.l_whence = SEEK_SET;
  fl.l_start = 0;
  fl.l_len = 0;
  fl.l_pid = getpid();

  // Open the file
  *fd = ::open(filename.c_str(), O_RDWR);
  if (*fd == -1) {
    return LOG_STATUS(Status::IOError(
        "Cannot open filelock '" + filename + "'; " + strerror(errno)));
  }
  // Acquire the lock
  if (fcntl(*fd, F_SETLKW, &fl) == -1) {
    return LOG_STATUS(Status::IOError(
        "Cannot lock filelock '" + filename + "'; " + strerror(errno)));
  }
  return Status::Ok();
}

Status Posix::filelock_unlock(filelock_t fd) const {
  if (::close(fd) == -1)
    return LOG_STATUS(Status::IOError(
        std::string("Cannot unlock filelock: ") + strerror(errno)));
  return Status::Ok();
}

Status Posix::init(const Config& config, ThreadPool* vfs_thread_pool) {
  if (vfs_thread_pool == nullptr) {
    return LOG_STATUS(
        Status::VFSError("Cannot initialize with null thread pool"));
  }

  config_ = config;
  vfs_thread_pool_ = vfs_thread_pool;

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
  struct dirent* next_path = nullptr;
  DIR* dir = opendir(path.c_str());
  if (dir == nullptr) {
    return Status::Ok();
  }
  while ((next_path = readdir(dir)) != nullptr) {
    if (!strcmp(next_path->d_name, ".") || !strcmp(next_path->d_name, ".."))
      continue;
    std::string abspath = path + "/" + next_path->d_name;
    paths->push_back(abspath);
  }
  // close parent directory
  if (closedir(dir) != 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot close parent directory; ") + strerror(errno)));
  }
  return Status::Ok();
}

Status Posix::move_path(
    const std::string& old_path, const std::string& new_path) {
  if (rename(old_path.c_str(), new_path.c_str()) != 0) {
    return LOG_STATUS(
        Status::IOError(std::string("Cannot move path: ") + strerror(errno)));
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
        Status::IOError("Cannot read from file; Read exceeds file size"));

  // Open file
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot read from file; ") + strerror(errno)));
  }
  if (offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot read from file ' ") + path.c_str() +
        "'; offset > typemax(off_t)"));
  }
  if (nbytes > SSIZE_MAX) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot read from file ' ") + path.c_str() +
        "'; nbytes > SSIZE_MAX"));
  }
  uint64_t bytes_read = read_all(fd, buffer, nbytes, offset);
  if (bytes_read != nbytes) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot read from file '") + path.c_str() +
        "'; File reading error"));
  }
  // Close file
  if (close(fd)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot read from file; ") + strerror(errno)));
  }
  return Status::Ok();
}

Status Posix::sync(const std::string& path) {
  // Open file
  int fd = -1;
  if (is_dir(path))  // DIRECTORY
    fd = open(path.c_str(), O_RDONLY, S_IRWXU);
  else if (is_file(path))  // FILE
    fd = open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, S_IRWXU);
  else
    return Status::Ok();  // If file does not exist, exit

  // Handle error
  if (fd == -1) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot open file '") + path + "' for syncing; " +
        strerror(errno)));
  }

  // Sync
  if (fsync(fd) != 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot sync file '") + path + "'; " + strerror(errno)));
  }

  // Close file
  if (close(fd) != 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot close synced file '") + path + "'; " +
        strerror(errno)));
  }

  // Success
  return Status::Ok();
}

Status Posix::write(
    const std::string& path, const void* buffer, uint64_t buffer_size) {
  // Get config params
  bool found = false;
  uint64_t min_parallel_size = 0;
  uint64_t max_parallel_ops = 0;
  RETURN_NOT_OK(config_.get().get<uint64_t>(
      "vfs.min_parallel_size", &min_parallel_size, &found));
  assert(found);
  RETURN_NOT_OK(config_.get().get<uint64_t>(
      "vfs.file.max_parallel_ops", &max_parallel_ops, &found));
  assert(found);

  // Get file offset (equal to file size)
  Status st;
  uint64_t file_offset = 0;
  if (is_file(path)) {
    st = file_size(path, &file_offset);
    if (!st.ok()) {
      std::stringstream errmsg;
      errmsg << "Cannot write to file '" << path << "'; " << st.message();
      return LOG_STATUS(Status::IOError(errmsg.str()));
    }
  }

  // Open or create file.
  int fd = open(path.c_str(), O_WRONLY | O_CREAT, S_IRWXU);
  if (fd == -1) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot open file '") + path + "'; " + strerror(errno)));
  }

  // Ensure that each thread is responsible for at least min_parallel_size
  // bytes, and cap the number of parallel operations at the thread pool size.
  uint64_t num_ops = std::min(
      std::max(buffer_size / min_parallel_size, uint64_t(1)), max_parallel_ops);
  if (num_ops == 1) {
    st = write_at(fd, file_offset, buffer, buffer_size);
    if (!st.ok()) {
      close(fd);
      std::stringstream errmsg;
      errmsg << "Cannot write to file '" << path << "'; " << st.message();
      return LOG_STATUS(Status::IOError(errmsg.str()));
    }
  } else {
    STATS_COUNTER_ADD(vfs_posix_write_num_parallelized, 1);
    std::vector<std::future<Status>> results;
    uint64_t thread_write_nbytes = utils::math::ceil(buffer_size, num_ops);
    for (uint64_t i = 0; i < num_ops; i++) {
      uint64_t begin = i * thread_write_nbytes,
               end =
                   std::min((i + 1) * thread_write_nbytes - 1, buffer_size - 1);
      uint64_t thread_nbytes = end - begin + 1;
      uint64_t thread_file_offset = file_offset + begin;
      auto thread_buffer = reinterpret_cast<const char*>(buffer) + begin;
      results.push_back(vfs_thread_pool_->enqueue(
          [fd, thread_file_offset, thread_buffer, thread_nbytes]() {
            return write_at(
                fd, thread_file_offset, thread_buffer, thread_nbytes);
          }));
    }
    st = vfs_thread_pool_->wait_all(results);
    if (!st.ok()) {
      close(fd);
      std::stringstream errmsg;
      errmsg << "Cannot write to file '" << path << "'; " << st.message();
      return LOG_STATUS(Status::IOError(errmsg.str()));
    }
  }
  if (close(fd) != 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot close file '") + path + "'; " + strerror(errno)));
  }
  return st;
}

Status Posix::write_at(
    int fd, uint64_t file_offset, const void* buffer, uint64_t buffer_size) {
  // Append data to the file in batches of constants::max_write_bytes
  // bytes at a time
  uint64_t buffer_bytes_written = 0;
  const char* buffer_bytes_ptr = static_cast<const char*>(buffer);
  while (buffer_size > constants::max_write_bytes) {
    uint64_t bytes_written = pwrite_all(
        fd,
        file_offset + buffer_bytes_written,
        buffer_bytes_ptr + buffer_bytes_written,
        constants::max_write_bytes);
    if (bytes_written != constants::max_write_bytes) {
      return LOG_STATUS(Status::IOError(
          std::string("Cannot write to file; File writing error")));
    }
    buffer_bytes_written += bytes_written;
    buffer_size -= bytes_written;
  }
  uint64_t bytes_written = pwrite_all(
      fd,
      file_offset + buffer_bytes_written,
      buffer_bytes_ptr + buffer_bytes_written,
      buffer_size);
  if (bytes_written != buffer_size) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot write to file; File writing error")));
  }
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // !_WIN32
