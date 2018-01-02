/**
 * @file   win32_filesystem.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file includes definitions of Win32 filesystem functions.
 */

#ifdef _WIN32

#include "win32_filesystem.h"
#include "constants.h"
#include "logger.h"
#include "utils.h"

#include <fstream>
#include <iostream>
#include <Windows.h>
#include <Shlwapi.h>
#include <wininet.h> // For INTERNET_MAX_URL_LENGTH

namespace tiledb {

namespace win32 {

/** Always returns a Windows path, converting from a URI if necessary. */
static std::string windows_path(const std::string &path_or_uri) {
  return utils::starts_with(path_or_uri, "file:///") ? path_from_uri(path_or_uri) : path_or_uri;
}

std::string abs_path(const std::string& path) {
  if (path.length() == 0) {
    return "";
  }
  // Convert to URI.
  std::string p = utils::starts_with(path, "file:///") ? path : uri_from_path(path);
  unsigned long result_len = p.length() + 1;
  char result[INTERNET_MAX_URL_LENGTH];
  std::string str_result;
  if (UrlCanonicalize(p.c_str(), result, &result_len, 0) != S_OK) {
    LOG_STATUS(Status::IOError(std::string("Cannot canonicalize path.")));
  } else {
    str_result = result;
  }
  return str_result;
}

Status create_dir(const std::string& path) {
  if (win32::is_dir(path)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create directory '") + path +
        "'; Directory already exists"));
  }
  std::string win_path = windows_path(path);
  CreateDirectory(win_path.c_str(), nullptr);
  return Status::Ok();
}

Status create_file(const std::string& filename) {
  if (win32::is_file(filename)) {
    return Status::Ok();
  }

  std::string win_path = windows_path(filename);
  HANDLE file_h = CreateFile(win_path.c_str(),
           GENERIC_WRITE,
           FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
           nullptr,
           CREATE_NEW,
           FILE_ATTRIBUTE_NORMAL,
           nullptr);
  if (file_h == INVALID_HANDLE_VALUE || CloseHandle(file_h) == 0) {
    return LOG_STATUS(Status::IOError(
           std::string("Failed to create file '") + win_path + "'"));
  }
  return Status::Ok();
}

std::string current_dir() {
  std::string dir;
  unsigned long length = GetCurrentDirectory(0, nullptr);
  char *path = (char*)std::malloc(length * sizeof(char));
  if (path == nullptr || GetCurrentDirectory(length, path) == 0) {
    LOG_STATUS(Status::IOError(
        std::string("Failed to get current directory.")));
  }
  dir = path;
  std::free(path);
  return dir;
}

static Status recursively_remove_directory(const std::string &path) {
  std::string win_path = windows_path(path);
  const std::string glob = win_path + "\\*";
  WIN32_FIND_DATA find_data;

  // Get first file in directory.
  HANDLE find_h = FindFirstFileEx(glob.c_str(), FindExInfoBasic, &find_data, FindExSearchNameMatch, nullptr, 0);
  if (find_h == INVALID_HANDLE_VALUE) {
    goto err;
  }

  while (true) {
    // Skip '.' and '..'
    if (strcmp(find_data.cFileName, ".") != 0 && strcmp(find_data.cFileName, "..") != 0) {
      if (PathIsDirectory(find_data.cFileName)) {
        if (!recursively_remove_directory(find_data.cFileName).ok()) {
          goto err;
        }
      } else {
        if (!remove_file(find_data.cFileName).ok()) {
          goto err;
        }
      }
    }

    // Next find result.
    if (!FindNextFile(find_h, &find_data)) {
      break;
    }
  }

  if (RemoveDirectory(win_path.c_str()) == 0) {
    goto err;
  }

  FindClose(find_h);
  return Status::Ok();

err:
  if (find_h != INVALID_HANDLE_VALUE) {
    FindClose(find_h);
  }
  return LOG_STATUS(Status::IOError(
    std::string("Failed to remove directory.")));
}

Status remove_path(const std::string& path) {
  if (win32::is_file(path)) {
    return remove_file(path);
  } else {
    return recursively_remove_directory(path);
  }
}

Status delete_dir(const std::string& path) {
  return recursively_remove_directory(path);
}

Status remove_file(const std::string& path) {
  std::string win_path = windows_path(path);
  if (!DeleteFile(win_path.c_str())) {
    return LOG_STATUS(Status::IOError(
      std::string("Failed to delete file '" + win_path + "'")));
  }
  return Status::Ok();
}

Status file_size(const std::string& path, uint64_t* size) {
  LARGE_INTEGER nbytes;
  HANDLE file_h = CreateFile(path.c_str(), GENERIC_READ,
    FILE_SHARE_READ, NULL, OPEN_EXISTING,
    FILE_ATTRIBUTE_NORMAL, NULL);
  if (file_h == INVALID_HANDLE_VALUE) {
    return LOG_STATUS(Status::IOError(
      std::string("Failed to get file size for '" + path + "'")));
  }
  if (!GetFileSizeEx(file_h, &nbytes)) {
    CloseHandle(file_h);
    return LOG_STATUS(Status::IOError(
      std::string("Failed to get file size for '" + path + "'")));
  }
  *size = nbytes.QuadPart;
  CloseHandle(file_h);
  return Status::Ok();
}

Status filelock_lock(const std::string& filename, file_lock_t* fd, bool shared) {
  HANDLE file_h = CreateFile(filename.c_str(), GENERIC_READ | GENERIC_WRITE,
    FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING,
    FILE_ATTRIBUTE_NORMAL, NULL);
  if (file_h == INVALID_HANDLE_VALUE) {
    return LOG_STATUS(Status::IOError(
      std::string("Failed to lock '" + filename + "'")));
  }
  OVERLAPPED overlapped = { 0 };
  if (LockFileEx(file_h, shared ? 0 : LOCKFILE_EXCLUSIVE_LOCK, 0, MAXDWORD, MAXDWORD, &overlapped) != 0) {
    CloseHandle(file_h);
    *fd = INVALID_FILE_LOCK;
    return LOG_STATUS(Status::IOError(
      std::string("Failed to lock '" + filename + "'")));
  }

  *fd = file_h;
  return Status::Ok();
}

Status filelock_unlock(file_lock_t fd) {
  OVERLAPPED overlapped = { 0 };
  if (UnlockFileEx(fd, 0, MAXDWORD, MAXDWORD, &overlapped) != 0) {
    CloseHandle(fd);
    return LOG_STATUS(Status::IOError(
      std::string("Failed to unlock file lock")));
  }
  CloseHandle(fd);
  return Status::Ok();
}

bool is_dir(const std::string& path) {
  std::string win_path = windows_path(path);
  return PathIsDirectory(win_path.c_str());
}

bool is_file(const std::string& path) {
  std::string win_path = windows_path(path);
  return PathFileExists(win_path.c_str()) && !PathIsDirectory(win_path.c_str());
}

Status ls(const std::string& path, std::vector<std::string>* paths) {
  const std::string glob = path + "\\*";
  WIN32_FIND_DATA find_data;

  // Get first file in directory.
  HANDLE find_h = FindFirstFileEx(glob.c_str(), FindExInfoBasic, &find_data, FindExSearchNameMatch, nullptr, 0);
  if (find_h == INVALID_HANDLE_VALUE) {
    goto err;
  }

  while (true) {
    // Skip '.' and '..'
    if (strcmp(find_data.cFileName, ".") != 0 && strcmp(find_data.cFileName, "..") != 0) {
      paths->push_back(find_data.cFileName);
    }

    // Next find result.
    if (!FindNextFile(find_h, &find_data)) {
      break;
    }
  }

  if (RemoveDirectory(path.c_str()) == 0) {
    goto err;
  }

  FindClose(find_h);
  return Status::Ok();

err:
  if (find_h != INVALID_HANDLE_VALUE) {
    FindClose(find_h);
  }
  return LOG_STATUS(Status::IOError(
    std::string("Failed to list directory.")));
}

Status move_path(const std::string& old_path, const std::string& new_path) {
  if (MoveFileEx(old_path.c_str(), new_path.c_str(), MOVEFILE_REPLACE_EXISTING) == 0) {
    return LOG_STATUS(Status::IOError(
      std::string("Failed to rename '" + old_path + "' to '" + new_path + "'.")));
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

  char result[INTERNET_MAX_URL_LENGTH];
  unsigned long result_length = 0;
  if (UrlCanonicalize(path->c_str(), result, &result_length, 0) == S_OK) {
    *path = result;
  } else {
    *path = "";
  }
}


Status read_from_file(
  const std::string& path, uint64_t offset, void* buffer, uint64_t nbytes) {
  // Open the file (OPEN_EXISTING with CreateFile() will only open, not create, the file).
  HANDLE file_h = CreateFile(path.c_str(), GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
  if (file_h == INVALID_HANDLE_VALUE) {
    return LOG_STATUS(
      Status::IOError("Cannot read from file; File opening error"));
  }

  LARGE_INTEGER offset_lg_int;
  offset_lg_int.QuadPart = offset;
  if (SetFilePointerEx(file_h, offset_lg_int, NULL, FILE_BEGIN) == 0) {
    CloseHandle(file_h);
    return LOG_STATUS(
      Status::IOError("Cannot read from file; File seek error"));
  }

  unsigned long num_bytes_read = 0;
  if (ReadFile(file_h, buffer, nbytes, &num_bytes_read, NULL) == 0 || num_bytes_read != nbytes) {
    CloseHandle(file_h);
    return LOG_STATUS(
      Status::IOError("Cannot read from file; File read error"));
  }

  if (CloseHandle(file_h) == 0) {
    return LOG_STATUS(
      Status::IOError("Cannot read from file; File closing error"));
  }

  return Status::Ok();
}

Status sync(const std::string& path) {
  // Open the file (OPEN_EXISTING with CreateFile() will only open, not create, the file).
  HANDLE file_h = CreateFile(path.c_str(), GENERIC_WRITE, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
  if (file_h == INVALID_HANDLE_VALUE) {
    return LOG_STATUS(
      Status::IOError("Cannot sync file; File opening error"));
  }

  if (FlushFileBuffers(file_h) == 0) {
    CloseHandle(file_h);
    return LOG_STATUS(
      Status::IOError("Cannot sync file; Sync error"));
  }

  if (CloseHandle(file_h) == 0) {
    return LOG_STATUS(
      Status::IOError("Cannot read from file; File closing error"));
  }

  return Status::Ok();
}

Status write_to_file(
  const std::string& path, const void* buffer, uint64_t buffer_size) {
  // Open the file for appending, creating it if it doesn't exist.
  HANDLE file_h = CreateFile(path.c_str(), GENERIC_WRITE, 0, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
  if (file_h == INVALID_HANDLE_VALUE) {
    return LOG_STATUS(
      Status::IOError("Cannot write to file; File opening error"));
  }

  LARGE_INTEGER offset_lg_int;
  offset_lg_int.QuadPart = 0;
  if (SetFilePointerEx(file_h, offset_lg_int, NULL, FILE_END) == 0) {
    CloseHandle(file_h);
    return LOG_STATUS(
      Status::IOError("Cannot write to file; File seek error"));
  }

  // Append data to the file in batches of constants::max_write_bytes
  // bytes at a time
  unsigned long bytes_written = 0;
  uint64_t byte_idx = 0;
  const char *byte_buffer = reinterpret_cast<const char *>(buffer);
  while (buffer_size > constants::max_write_bytes) {
    if (WriteFile(file_h, byte_buffer + byte_idx, constants::max_write_bytes, &bytes_written, NULL) == 0 || bytes_written != constants::max_write_bytes) {
      return LOG_STATUS(Status::IOError(
        std::string("Cannot write to file '") + path +
        "'; File writing error"));
    }
    buffer_size -= constants::max_write_bytes;
    byte_idx += constants::max_write_bytes;
  }
  if (WriteFile(file_h, byte_buffer + byte_idx, buffer_size, &bytes_written, NULL) == 0 || bytes_written != buffer_size) {
    return LOG_STATUS(Status::IOError(
      std::string("Cannot write to file '") + path +
      "'; File writing error"));
  }

  if (CloseHandle(file_h) == 0) {
    return LOG_STATUS(
      Status::IOError("Cannot write to file; File closing error"));
  }

  return Status::Ok();
}

std::string uri_from_path(const std::string &path) {
  unsigned long uri_length = INTERNET_MAX_URL_LENGTH;
  char uri[INTERNET_MAX_URL_LENGTH];
  std::string str_uri;
  if (UrlCreateFromPath(path.c_str(), uri, &uri_length, NULL) != S_OK) {
    LOG_STATUS(Status::IOError(
        std::string("Failed to convert path to URI.")));
  }
  str_uri = uri;
  return str_uri;
}

std::string path_from_uri(const std::string &uri) {
  unsigned long path_length = MAX_PATH;
  char path[MAX_PATH];
  std::string str_path;
  if (PathCreateFromUrl(uri.c_str(), path, &path_length, NULL) != S_OK) {
    LOG_STATUS(Status::IOError(
          std::string("Failed to convert URI to path.")));
  }
  str_path = path;
  return str_path;
}

}  // namespace win32

}  // namespace tiledb

#endif // _WIN32
