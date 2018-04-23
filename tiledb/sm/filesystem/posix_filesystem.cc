/**
 * @file   posix_filesystem.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include "tiledb/sm/filesystem/posix_filesystem.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"

#include <dirent.h>

#include <ftw.h>

#include <fstream>
#include <iostream>

namespace tiledb {
namespace sm {

namespace posix {

bool both_slashes(char a, char b) {
  return a == '/' && b == '/';
}

/**
 * Reads all nbytes from the given file descriptor, retrying as necessary.
 *
 * @param fd Open file descriptor to read from
 * @param buffer Buffer to hold read data
 * @param nbytes Number of bytes to read
 * @param offset Offset in file to start reading from.
 * @return Number of bytes actually read (< nbytes on error).
 */
uint64_t read_all(int fd, void* buffer, uint64_t nbytes, uint64_t offset) {
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

/**
 * Writes all nbytes to the given file descriptor, retrying as necessary.
 *
 * @param fd Open file descriptor to write to
 * @param buffer Buffer with data to write
 * @param nbytes Number of bytes to write
 * @return Number of bytes actually written (< nbytes on error).
 */
uint64_t write_all(int fd, const void* buffer, uint64_t nbytes) {
  auto bytes = reinterpret_cast<const char*>(buffer);
  uint64_t written = 0;
  do {
    ssize_t actual_written = ::write(fd, bytes + written, nbytes - written);
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

void adjacent_slashes_dedup(std::string* path) {
  assert(utils::starts_with(*path, "file://"));
  path->erase(
      std::unique(
          path->begin() + std::string("file://").size(),
          path->end(),
          both_slashes),
      path->end());
}

std::string abs_path(const std::string& path) {
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
  if (utils::starts_with(path, posix_prefix))
    return path;
  else if (utils::starts_with(path, "/"))
    ret_dir = posix_prefix + path;
  else if (utils::starts_with(path, "~/"))
    ret_dir = posix_prefix + home + path.substr(1, path.size() - 1);
  else if (utils::starts_with(path, "./"))
    ret_dir = posix_prefix + current + path.substr(1, path.size() - 1);
  else
    ret_dir = posix_prefix + current + "/" + path;

  adjacent_slashes_dedup(&ret_dir);
  purge_dots_from_path(&ret_dir);

  return ret_dir;
}

Status create_dir(const std::string& path) {
  // If the directory does not exist, create it
  if (posix::is_dir(path)) {
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

Status touch(const std::string& filename) {
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if (fd == -1 || ::close(fd) != 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Failed to create file '") + filename + "'; " +
        strerror(errno)));
  }

  return Status::Ok();
}

std::string current_dir() {
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
int unlink_cb(
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

Status remove_dir(const std::string& path) {
  int rc = nftw(path.c_str(), unlink_cb, 64, FTW_DEPTH | FTW_PHYS);
  if (rc)
    return LOG_STATUS(Status::IOError(
        std::string("Failed to delete path '") + path + "';  " +
        strerror(errno)));
  return Status::Ok();
}

Status remove_file(const std::string& path) {
  if (remove(path.c_str()) != 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot delete file '") + path + "'; " + strerror(errno)));
  }
  return Status::Ok();
}

Status file_size(const std::string& path, uint64_t* size) {
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

Status filelock_lock(const std::string& filename, filelock_t* fd, bool shared) {
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
    return LOG_STATUS(Status::StorageManagerError(
        "Cannot open filelock '" + filename + "'; " + strerror(errno)));
  }
  // Acquire the lock
  if (fcntl(*fd, F_SETLKW, &fl) == -1) {
    return LOG_STATUS(Status::IOError(
        "Cannot lock filelock '" + filename + "'; " + strerror(errno)));
  }
  return Status::Ok();
}

Status filelock_unlock(filelock_t fd) {
  if (::close(fd) == -1)
    return LOG_STATUS(Status::IOError(
        std::string("Cannot unlock filelock: ") + strerror(errno)));
  return Status::Ok();
}

bool is_dir(const std::string& path) {
  struct stat st;
  memset(&st, 0, sizeof(struct stat));
  return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

bool is_file(const std::string& path) {
  struct stat st;
  memset(&st, 0, sizeof(struct stat));
  return (stat(path.c_str(), &st) == 0) && !S_ISDIR(st.st_mode);
}

Status ls(const std::string& path, std::vector<std::string>* paths) {
  struct dirent* next_path = nullptr;
  DIR* dir = opendir(path.c_str());
  if (dir == nullptr) {
    return Status::Ok();
  }
  while ((next_path = readdir(dir)) != nullptr) {
    if (!strcmp(next_path->d_name, ".") || !strcmp(next_path->d_name, ".."))
      continue;
    auto abspath = path + "/" + next_path->d_name;
    paths->push_back(abspath);
  }
  // close parent directory
  if (closedir(dir) != 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot close parent directory; ") + strerror(errno)));
  }
  return Status::Ok();
}

Status move_path(const std::string& old_path, const std::string& new_path) {
  if (rename(old_path.c_str(), new_path.c_str()) != 0) {
    return LOG_STATUS(
        Status::IOError(std::string("Cannot move path: ") + strerror(errno)));
  }
  return Status::Ok();
}

void purge_dots_from_path(std::string* path) {
  // Trivial case
  if (path == nullptr)
    return;

  // Trivial case
  uint64_t path_size = path->size();
  if (path_size == 0 || *path == "file:///")
    return;

  assert(utils::starts_with(*path, "file:///"));

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

Status read(
    const std::string& path, uint64_t offset, void* buffer, uint64_t nbytes) {
  // Checks
  uint64_t file_size;
  RETURN_NOT_OK(posix::file_size(path, &file_size));
  if (offset + nbytes > file_size)
    return LOG_STATUS(
        Status::IOError("Cannot read from file; Read exceeds file size"));

  // Open file
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot read from file; ") + strerror(errno)));
  }
  if (offset > std::numeric_limits<off_t>::max()) {
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

Status sync(const std::string& path) {
  // Open file
  int fd = -1;
  if (posix::is_dir(path))  // DIRECTORY
    fd = open(path.c_str(), O_RDONLY, S_IRWXU);
  else if (posix::is_file(path))  // FILE
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

Status write(
    const std::string& path, const void* buffer, uint64_t buffer_size) {
  // Open file
  int fd = open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, S_IRWXU);
  if (fd == -1) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot write to file '") + path + "'; " +
        strerror(errno)));
  }

  // Append data to the file in batches of constants::max_write_bytes
  // bytes at a time
  uint64_t buffer_bytes_written = 0;
  const char* buffer_bytes_ptr = static_cast<const char*>(buffer);
  while (buffer_size > constants::max_write_bytes) {
    uint64_t bytes_written = write_all(
        fd,
        buffer_bytes_ptr + buffer_bytes_written,
        constants::max_write_bytes);
    if (bytes_written != constants::max_write_bytes) {
      return LOG_STATUS(Status::IOError(
          std::string("Cannot write to file '") + path +
          "'; File writing error"));
    }
    buffer_bytes_written += bytes_written;
    buffer_size -= bytes_written;
  }
  uint64_t bytes_written =
      write_all(fd, buffer_bytes_ptr + buffer_bytes_written, buffer_size);
  if (bytes_written != buffer_size) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot write to file '") + path +
        "'; File writing error"));
  }

  // Close file
  if (close(fd) != 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot close file '") + path + "'; " + strerror(errno)));
  }

  // Success
  return Status::Ok();
}

}  // namespace posix

}  // namespace sm
}  // namespace tiledb

#endif  // !_WIN32
