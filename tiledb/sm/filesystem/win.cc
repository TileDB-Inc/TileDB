/**
 * @file   win.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB, Inc.
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
 * This file includes definitions of Windows filesystem functions.
 */
#ifdef _WIN32

#if !defined(NOMINMAX)
#define NOMINMAX  // suppress definition of min/max macros in Windows headers
#endif
#include <Shlwapi.h>
#include <Windows.h>
#include <algorithm>
#include <cassert>
#include <codecvt>
#include <fstream>
#include <iostream>
#include <locale>
#include <sstream>
#include <string_view>

#include "filesystem_base.h"
#include "path_win.h"
#include "tiledb/common/common.h"
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/scoped_executor.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "uri.h"
#include "win.h"

#include <fmt/format.h>

using namespace tiledb::common;
using tiledb::common::filesystem::directory_entry;

namespace tiledb::sm {

namespace {
/** Returns the last Windows error message string. */
std::string get_last_error_msg_desc(decltype(GetLastError()) gle) {
  LPWSTR lpMsgBuf = nullptr;
  // FormatMessageW allocates a buffer that must be freed with LocalFree.
  if (FormatMessageW(
          FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM |
              FORMAT_MESSAGE_IGNORE_INSERTS,
          NULL,
          gle,
          // By passing zero as the language ID, Windows will try the following
          // languages in order:
          // * Language neutral
          // * Thread LANGID, based on the thread's locale value
          // * User default LANGID, based on the user's default locale value
          // * System default LANGID, based on the system default locale value
          // * US English
          0,
          (LPWSTR)&lpMsgBuf,
          0,
          NULL) == 0) {
    if (lpMsgBuf) {
      LocalFree(lpMsgBuf);
    }
    return "unknown error";
  }
  // Convert to UTF-8.
  std::string msg =
      std::wstring_convert<std::codecvt_utf8<wchar_t>>{}.to_bytes(lpMsgBuf);
  LocalFree(lpMsgBuf);
  return msg;
}

std::string get_last_error_msg(
    decltype(GetLastError()) gle, const std::string_view func_desc) {
  std::string gle_desc = get_last_error_msg_desc(gle);

  auto buf_len = gle_desc.length() + func_desc.size() + 60;
  auto display_buf = std::make_unique<char[]>(buf_len);
  std::snprintf(
      display_buf.get(),
      buf_len,
      "%s GetLastError %lu (0x%08lx): %s",
      func_desc.data(),
      gle,
      gle,
      gle_desc.c_str());
  return {display_buf.get()};
}
std::string get_last_error_msg(const std::string_view func_desc) {
  DWORD gle = GetLastError();
  return get_last_error_msg(gle, func_desc);
}
}  // namespace

std::string Win::abs_path(const std::string& path) {
  if (path.length() == 0) {
    return current_dir();
  }
  std::string full_path(path_win::slashes_to_backslashes(path));
  // If some problem leads here, note the following
  // PathIsRelative("/") unexpectedly returns true.
  // PathIsRelative("c:somedir\somesubdir") unexpectedly returns false
  if (PathIsRelative(full_path.c_str())) {
    full_path = current_dir() + "\\" + full_path;
  } else {
    full_path = path;
  }
  char result[MAX_PATH];
  std::string str_result;
  if (PathCanonicalize(result, full_path.c_str()) == FALSE) {
    auto gle = GetLastError();
    LOG_STATUS_NO_RETURN_VALUE(Status_IOError(std::string(
        "Cannot canonicalize path. (" +
        get_last_error_msg(gle, "PathCanonicalize") + ")")));
  } else {
    str_result = result;
  }
  return str_result;
}

void Win::create_dir(const URI& uri) const {
  auto path = uri.to_path();
  if (is_dir(uri)) {
    throw WindowsException(
        "Cannot create directory '" + path + "'; Directory already exists");
  }
  std::error_code ec;
  std::filesystem::create_directories(path, ec);
  if (ec) {
    throw WindowsException(
        std::string("Cannot create directory '" + path + "'; " + ec.message()));
  }
}

void Win::touch(const URI& uri) const {
  auto filename = uri.to_path();
  throw_if_not_ok(ensure_directory(filename));
  HANDLE file_h = CreateFile(
      filename.c_str(),
      GENERIC_WRITE,
      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
      nullptr,
      CREATE_NEW,
      FILE_ATTRIBUTE_NORMAL,
      nullptr);
  auto closefileonexit = [&]() {
    if (file_h != INVALID_HANDLE_VALUE)
      CloseHandle(file_h);
  };
  tiledb::common::ScopedExecutor onexit1(closefileonexit);
  if (file_h == INVALID_HANDLE_VALUE) {
    auto gle = GetLastError();
    if (gle == ERROR_FILE_EXISTS) {
      // Do not fail if the file already exists.
      return;
    }
    throw WindowsException(std::string(
        "Failed to create file '" + filename + " (" +
        get_last_error_msg(gle, "CreateFile") + ")'"));
  }
}

std::string Win::current_dir() {
  std::string dir;
  unsigned long length = GetCurrentDirectory(0, nullptr);
  char* path = (char*)tdb_malloc(length * sizeof(char));
  if (path == nullptr || GetCurrentDirectory(length, path) == 0) {
    LOG_STATUS_NO_RETURN_VALUE(Status_IOError(std::string(
        "Failed to get current directory. " +
        get_last_error_msg("GetCurrentDirectory"))));
  }
  dir = path;
  tdb_free(path);
  return dir;
}

Status Win::recursively_remove_directory(const std::string& path) const {
  const std::string glob = path + "\\*";
  WIN32_FIND_DATA find_data;
  const char* what_call = nullptr;
  Status ret;

  // Get first file in directory.
  HANDLE find_h = FindFirstFileEx(
      glob.c_str(),
      FindExInfoBasic,
      &find_data,
      FindExSearchNameMatch,
      nullptr,
      0);
  if (find_h == INVALID_HANDLE_VALUE) {
    what_call = "FindFirstFileEx";
    goto err;
  }

  while (true) {
    // Skip '.' and '..'
    if (strcmp(find_data.cFileName, ".") != 0 &&
        strcmp(find_data.cFileName, "..") != 0) {
      std::string file_path = path + "\\" + find_data.cFileName;
      if (PathIsDirectory(file_path.c_str())) {
        ret = recursively_remove_directory(file_path);
        if (!ret.ok()) {
          goto err;
        }
      } else {
        try {
          remove_file(URI(file_path));
        } catch (...) {
          goto err;
        }
      }
    }

    // Next find result.
    if (!FindNextFile(find_h, &find_data)) {
      break;
    }
  }

  if (RemoveDirectory(path.c_str()) == 0) {
    what_call = "RemoveDirectory";
    goto err;
  }

  FindClose(find_h);
  return Status::Ok();

err:
  auto gle = GetLastError();
  if (find_h != INVALID_HANDLE_VALUE) {
    FindClose(find_h);
  }

  // If we encountered an error while recursively
  // removing files and directories, return it.
  if (!ret.ok()) {
    return ret;
  }

  // Otherwise, create a new error status and return
  // that instead.
  std::string offender = __func__;
  if (what_call) {
    offender.append(" ");
    offender.append(what_call);
  }
  return LOG_STATUS(Status_IOError(std::string(
      "Failed to remove directory '" + path + "' " +
      get_last_error_msg(gle, offender.c_str()))));
}

void Win::remove_dir(const URI& uri) const {
  auto path = uri.to_path();
  if (is_dir(uri)) {
    throw_if_not_ok(recursively_remove_directory(path));
  } else {
    throw WindowsException(
        "Failed to delete path '" + path + "'; not a valid path.");
  }
}

bool Win::remove_dir_if_empty(const std::string& path) const {
  if (!RemoveDirectoryA(path.c_str())) {
    auto gle = GetLastError();
    if (gle == ERROR_DIR_NOT_EMPTY) {
      return false;
    }
    throw IOError(
        std::string("Failed to delete directory '") + path + "' " +
        get_last_error_msg(gle, "RemoveDirectory"));
  }
  return true;
}

void Win::remove_file(const URI& uri) const {
  auto path = uri.to_path();
  if (!DeleteFile(path.c_str())) {
    throw WindowsException(std::string(
        "Failed to delete file '" + path + "' " +
        get_last_error_msg("DeleteFile")));
  }
}

uint64_t Win::file_size(const URI& uri) const {
  auto path = uri.to_path();
  LARGE_INTEGER nbytes;
  HANDLE file_h = CreateFile(
      path.c_str(),
      GENERIC_READ,
      FILE_SHARE_READ,
      NULL,
      OPEN_EXISTING,
      FILE_ATTRIBUTE_NORMAL,
      NULL);
  if (file_h == INVALID_HANDLE_VALUE) {
    throw WindowsException(std::string(
        "Failed to get file size for '" + path + "' (" +
        get_last_error_msg("CreateFile") + ")'"));
  }
  if (!GetFileSizeEx(file_h, &nbytes)) {
    CloseHandle(file_h);
    throw WindowsException(std::string(
        "Failed to get file size for '" + path + "' (" +
        get_last_error_msg("GetFileSizeEx") + ")'"));
  }
  uint64_t size = nbytes.QuadPart;
  CloseHandle(file_h);
  return size;
}

bool Win::is_dir(const URI& uri) const {
  auto path = uri.to_path();
  return PathFileExists(path.c_str()) && PathIsDirectory(path.c_str());
}

bool Win::is_file(const URI& uri) const {
  auto path = uri.to_path();
  return PathFileExists(path.c_str()) && !PathIsDirectory(path.c_str());
}

Status Win::ls(const std::string& path, std::vector<std::string>* paths) const {
  for (auto& fs : ls_with_sizes(URI(path))) {
    paths->emplace_back(fs.path().native());
  }

  return Status::Ok();
}

std::vector<directory_entry> Win::ls_with_sizes(const URI& uri) const {
  auto path = uri.to_path();
  bool ends_with_slash = path.length() > 0 && path[path.length() - 1] == '\\';
  const std::string glob = path + (ends_with_slash ? "*" : "\\*");
  WIN32_FIND_DATA find_data;
  std::vector<directory_entry> entries;
  const char* what_call = nullptr;

  // Get first file in directory.
  HANDLE find_h = FindFirstFileEx(
      glob.c_str(),
      FindExInfoBasic,
      &find_data,
      FindExSearchNameMatch,
      nullptr,
      0);
  if (find_h == INVALID_HANDLE_VALUE) {
    what_call = "FindFirstFileEx";
    goto err;
  }

  while (true) {
    // Skip '.' and '..'
    if (strcmp(find_data.cFileName, ".") != 0 &&
        strcmp(find_data.cFileName, "..") != 0) {
      std::string file_path =
          path + (ends_with_slash ? "" : "\\") + find_data.cFileName;
      if (is_dir(URI(file_path))) {
        entries.emplace_back(file_path, 0, true);
      } else {
        ULARGE_INTEGER size;
        size.LowPart = find_data.nFileSizeLow;
        size.HighPart = find_data.nFileSizeHigh;
        entries.emplace_back(file_path, size.QuadPart, false);
      }
    }

    // Next find result.
    if (!FindNextFile(find_h, &find_data)) {
      break;
    }
  }

  FindClose(find_h);
  return entries;

err:
  auto gle = GetLastError();
  if (find_h != INVALID_HANDLE_VALUE) {
    FindClose(find_h);
  }
  std::string offender = __func__;
  if (what_call) {
    offender.append(" ");
    offender.append(what_call);
  }
  throw IOError(
      "Failed to list directory \"" + path + "\" " +
      get_last_error_msg(gle, offender.c_str()));
}

void Win::move_path(const URI& old_uri, const URI& new_uri) const {
  auto old_path = old_uri.to_path();
  auto new_path = new_uri.to_path();
  if (MoveFileEx(
          old_path.c_str(), new_path.c_str(), MOVEFILE_REPLACE_EXISTING) == 0) {
    throw WindowsException(std::string(
        "Failed to rename '" + old_path + "' to '" + new_path + "'. (" +
        get_last_error_msg("MoveFileEx") + ")"));
  }
}

void Win::move_dir(const URI& old_uri, const URI& new_uri) const {
  move_path(old_uri, new_uri);
}

void Win::move_file(const URI& old_uri, const URI& new_uri) const {
  move_path(old_uri, new_uri);
}

void Win::read(
    const URI& uri,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes,
    bool) const {
  auto path = uri.to_path();
  // Open the file (OPEN_EXISTING with CreateFile() will only open, not create,
  // the file).
  HANDLE file_h = CreateFile(
      path.c_str(),
      GENERIC_READ,
      FILE_SHARE_READ,
      NULL,
      OPEN_EXISTING,
      FILE_ATTRIBUTE_NORMAL,
      NULL);
  if (file_h == INVALID_HANDLE_VALUE) {
    throw WindowsException(
        "Cannot read from file '" + path + "'; File opening error (" +
        get_last_error_msg("CreateFile") + ")");
  }

  char* byte_buffer = reinterpret_cast<char*>(buffer);
  do {
    LARGE_INTEGER offset_lg_int;
    offset_lg_int.QuadPart = offset;
    OVERLAPPED ov = {0, 0, {{0, 0}}, 0};
    ov.Offset = offset_lg_int.LowPart;
    ov.OffsetHigh = offset_lg_int.HighPart;
    DWORD num_bytes_to_read = nbytes > std::numeric_limits<DWORD>::max() ?
                                  std::numeric_limits<DWORD>::max() :
                                  (DWORD)nbytes;
    DWORD num_bytes_read = 0;
    if (ReadFile(
            file_h, byte_buffer, num_bytes_to_read, &num_bytes_read, &ov) ==
        0) {
      auto gle = GetLastError();
      CloseHandle(file_h);

      std::string err_msg;
      if (gle != 0) {
        err_msg = get_last_error_msg(gle, "ReadFile");
      } else {
        err_msg = std::string("num_bytes_read ") +
                  std::to_string(num_bytes_read) + " != nbytes " +
                  std::to_string(nbytes);
      }

      throw WindowsException(fmt::format(
          "Cannot read from file '{}'; File read error '{}' offset {} nbytes "
          "{}",
          path,
          err_msg,
          offset,
          nbytes));
    }
    byte_buffer += num_bytes_read;
    offset += num_bytes_read;
    nbytes -= num_bytes_read;
  } while (nbytes > 0);
  if (CloseHandle(file_h) == 0) {
    throw WindowsException(
        "Cannot read from file '" + path + "'; File closing error " +
        get_last_error_msg("CloseHandle"));
  }
}

void Win::flush(const URI& uri, bool) {
  sync(uri);
}

void Win::sync(const URI& uri) const {
  if (!is_file(uri)) {
    return;
  }

  // Open the file (OPEN_EXISTING with CreateFile() will only open, not create,
  // the file).
  auto path = uri.to_path();
  HANDLE file_h = CreateFile(
      path.c_str(),
      GENERIC_WRITE,
      0,
      NULL,
      OPEN_EXISTING,
      FILE_ATTRIBUTE_NORMAL,
      NULL);
  if (file_h == INVALID_HANDLE_VALUE) {
    throw WindowsException(
        "Cannot sync file '" + path + "'; File opening error " +
        get_last_error_msg("CreateFile"));
  }

  if (FlushFileBuffers(file_h) == 0) {
    auto gle = GetLastError();
    CloseHandle(file_h);
    throw WindowsException(
        "Cannot sync file '" + path + "'; Sync error " +
        get_last_error_msg(gle, "FlushFileBuffers"));
  }

  if (CloseHandle(file_h) == 0) {
    throw WindowsException(
        "Cannot read from file '" + path + "'; File closing error " +
        get_last_error_msg("CloseHandle"));
  }
}

void Win::write(
    const URI& uri, const void* buffer, uint64_t buffer_size, bool) {
  auto path = uri.to_path();
  throw_if_not_ok(ensure_directory(path));
  // Open the file for appending, creating it if it doesn't exist.
  HANDLE file_h = CreateFile(
      path.c_str(),
      GENERIC_WRITE,
      0,
      NULL,
      OPEN_ALWAYS,
      FILE_ATTRIBUTE_NORMAL,
      NULL);
  if (file_h == INVALID_HANDLE_VALUE) {
    throw WindowsException(
        "Cannot write to file '" + path + "'; File opening error " +
        get_last_error_msg("CreateFile"));
  }
  // Get the current file size.
  LARGE_INTEGER file_size_lg_int;
  if (!GetFileSizeEx(file_h, &file_size_lg_int)) {
    auto gle = GetLastError();
    CloseHandle(file_h);
    throw WindowsException(
        "Cannot write to file '" + path + "'; File size error " +
        get_last_error_msg(gle, "GetFileSizeEx"));
  }
  uint64_t file_offset = file_size_lg_int.QuadPart;
  if (!write_at(file_h, file_offset, buffer, buffer_size).ok()) {
    CloseHandle(file_h);
    throw WindowsException("Cannot write to file '" + path);
  }
  // Always close the handle.
  if (CloseHandle(file_h) == 0) {
    throw WindowsException(
        "Cannot write to file '" + path + "'; File closing error");
  }
}

Status Win::write_at(
    HANDLE file_h,
    uint64_t file_offset,
    const void* buffer,
    uint64_t buffer_size) {
  // Write data to the file in batches of constants::max_write_bytes bytes at a
  // time. Instead of seeking the file handle we use the OVERLAPPED struct to
  // specify an offset at which to write. Note that the file handle does not
  // have to be opened in "overlapped" mode (i.e. async writes) to do this.
  uint64_t byte_idx = 0;
  const char* byte_buffer = reinterpret_cast<const char*>(buffer);
  uint64_t remaining_bytes_to_write = buffer_size;
  while (remaining_bytes_to_write > 0) {
    DWORD bytes_to_write =
        remaining_bytes_to_write > std::numeric_limits<DWORD>::max() ?
            std::numeric_limits<DWORD>::max() :
            (DWORD)remaining_bytes_to_write;
    DWORD bytes_written = 0;
    LARGE_INTEGER offset;
    offset.QuadPart = file_offset;
    OVERLAPPED ov = {0, 0, {{0, 0}}, 0};
    ov.Offset = offset.LowPart;
    ov.OffsetHigh = offset.HighPart;
    if (WriteFile(
            file_h,
            byte_buffer + byte_idx,
            bytes_to_write,
            &bytes_written,
            &ov) == 0) {
      return LOG_STATUS(Status_IOError(std::string(
          "Cannot write to file; File writing error: " +
          get_last_error_msg("WriteFile"))));
    }
    remaining_bytes_to_write -= bytes_written;
    byte_idx += bytes_written;
    file_offset += bytes_written;
  }
  return Status::Ok();
}

}  // namespace tiledb::sm

#endif  // _WIN32
