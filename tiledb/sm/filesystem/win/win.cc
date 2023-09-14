/**
 * @file   win.cc
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
 * This file includes definitions of Windows filesystem functions.
 */
#ifdef _WIN32

#if !defined(NOMINMAX)
#define NOMINMAX  // suppress definition of min/max macros in Windows headers
#endif

#include <Shlwapi.h>
#include <Windows.h>
#include <strsafe.h>
#include <wininet.h>  // For INTERNET_MAX_URL_LENGTH
#include <algorithm>
#include <cassert>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string_view>

#include "tiledb/sm/filesystem/win/win.h"
#include "tiledb/sm/filesystem/win/path_win.h"
#include "tiledb/common/common.h"
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/scoped_executor.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/filesystem/uri.h"

namespace tiledb::sm::filesystem {

// Anonymous namespace for helper functions.
namespace {

typedef decltype(GetLastError()) TDBWinErrorType

/** Returns the last Windows error message string. */
std::string get_last_error_msg_desc(TDBWinErrorType gle) {
  LPVOID lpMsgBuf = nullptr;
  if (FormatMessage(
          FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM |
              FORMAT_MESSAGE_IGNORE_INSERTS,
          NULL,
          gle,
          MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
          (LPTSTR)&lpMsgBuf,
          0,
          NULL) == 0) {
    if (lpMsgBuf) {
      LocalFree(lpMsgBuf);
    }
    return "unknown error";
  }
  std::string msg(reinterpret_cast<char*>(lpMsgBuf));
  LocalFree(lpMsgBuf);
  return msg;
}

std::string get_last_error_msg(
    TDBWinErrorType gle, const std::string_view func_desc) {
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
  auto gle = GetLastError();
  return get_last_error_msg(gle, func_desc);
}

}  // namespace

class WinFSException : public FilesystemException {
 public:
  explicit FilesystemException(const std::string& message)
      : StatusException("WindowsFilesystem", message) {
  }
};

Win::Win(const Config& config)
    : FilesystemBase(resources.config())
    , fs_type_(FilesystemType::WIN)
    , thread_pool_(resoures.io_tp()) {
}

bool Win::is_dir(const URI& uri) const {
  return PathIsDirectory(uri.to_path().c_str());
}

bool Win::is_file(const URI& uri) const {
  return PathFileExists(uri.to_path().c_str()) && !PathIsDirectory(uri.to_path().c_str());
}

void Win::create_dir(const URI& uri) const {
  if (is_dir(uri)) {
    throw WinFSException("Cannot create directory '") + uri.to_path() +
        "'; Directory already exists"));
  }

  if (CreateDirectory(uri.to_path().c_str(), nullptr) == 0) {
    throw WinFSException("Cannot create directory '" + uri.to_path() +
        "'; " + get_last_error_msg("CreateDirectory"));
  }
}

std::vector<FilesystemEntry> Win::ls(const URI& uri) const {
  auto path = uri.to_path();
  if (path.length() > 0 && path[path.length() - 1] != '\\') {
    path += "\\";
  }

  const std::string glob = path + "*";

  // Get first file in directory.
  WIN32_FIND_DATA find_data;
  HANDLE find_h = FindFirstFileEx(
      glob.c_str(),
      FindExInfoBasic,
      &find_data,
      FindExSearchNameMatch,
      nullptr,
      0);

  tiledb::common::defer([&]() {
    if (file_h != INVALID_HANDLE_VALUE) {
      FindClose(file_h);
    }
  });

  if (find_h == INVALID_HANDLE_VALUE) {
    auto gle = GetLastError();

    std::string offender = __func__ + std::string("FindFirstFileEx");
    std::string errmsg(
        "Error listing directory '" + path + "' " +
        get_last_error_msg(gle, offender.c_str()));
    throw WinFSException(errmsg);
  }

  std::vector<FilesystemEntry> entries;

  while (true) {
    // Skip '.' and '..'
    if (strcmp(find_data.cFileName, ".") != 0 &&
        strcmp(find_data.cFileName, "..") != 0) {
      std::string entry_uri = URI(path + find_data.cFileName);
      if (is_dir(entry_uri)) {
        entries.emplace_back(entry_uri, 0, true);
      } else {
        ULARGE_INTEGER size;
        size.LowPart = find_data.nFileSizeLow;
        size.HighPart = find_data.nFileSizeHigh;
        entries.emplace_back(entry_uri, size.QuadPart, false);
      }
    }

    // Next find result.
    if (!FindNextFile(find_h, &find_data)) {
      break;
    }
  }

  return entries;
}

void Win::copy_dir(const URI& src_uri, const URI& tgt_uri) {
  throw WinFSException("Implement me!");
}

void Win::remove_dir(const URI& uri) const {
  if (!is_dir(uri)) {
    throw WinFSException("Error removing directory '" + uri.to_path() +
    "' because it is not a directory.");
  }

  auto&& entries = ls(uri);

  for (auto& entry : entries) {
    if (entry.is_directory()) {
      remove_dir(entry.uri());
    } else {
      remove_file(entry.uri());
    }
  }

  if (RemoveDirectory(path.c_str())) {
    // Directory successfully removed, we're done.
    return;
  }

  // Error removing directory, report the error.
  auto gle = GetLastError();
  std::string offender = __func__ + std::string(" RemoveDirectory");
  auto errmsg = "Failed to remove directory '" + uri.to_path() + "'; " +
      get_last_error_msg(gle, offender.c_str()))));
}

void Win::touch(const URI& uri) const {
  if (is_file(uri)) {
    return;
  }

  HANDLE file_h = CreateFile(
      uri.to_path().c_str(),
      GENERIC_WRITE,
      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
      nullptr,
      CREATE_NEW,
      FILE_ATTRIBUTE_NORMAL,
      nullptr);

  defer([&]() {
    if (file_h != INVALID_HANDLE_VALUE) {
      CloseHandle(file_h);
    }
  });

  if (file_h == INVALID_HANDLE_VALUE) {
    auto gle = GetLastError();
    throw WinFSException("Failed to create file '" + uri.to_path() + "'; " +
        get_last_error_msg(gle, "CreateFile"));
  }
}

uint64_t Win::file_size(const URI& uri) const {
  LARGE_INTEGER nbytes;
  HANDLE file_h = CreateFile(
      uri.to_path().c_str(),
      GENERIC_READ,
      FILE_SHARE_READ,
      NULL,
      OPEN_EXISTING,
      FILE_ATTRIBUTE_NORMAL,
      NULL);

  tiledb::common::defer([&]() {
    if (file_h != INVALID_HANDLE_VALUE) {
      CloseHandle(file_h);
    }
  });

  if (file_h == INVALID_HANDLE_VALUE) {
    throw WinFSException("Failed to get file size for '" + uri.to_path() + "'; " +
        get_last_error_msg("CreateFile"));
  }

  if (!GetFileSizeEx(file_h, &nbytes)) {
    throw WinFSException("Failed to get file size for '" + uri.to_path() + "'; " +
      get_last_error_msg("GetFileSizeEx"));
  }

  return nbytes.QuadPart;
}

void Win::write(const URI& uri, const void* buffer, uint64_t nbytes) {
  HANDLE file_h = CreateFile(
      path.c_str(),
      GENERIC_WRITE,
      0,
      NULL,
      OPEN_ALWAYS,
      FILE_ATTRIBUTE_NORMAL,
      NULL);

  tiledb::common::defer([&]() {
    // Always close the file handle regardless of how we exit this function.
    // However, you'll notice that we also close it below because we need to
    // check if it returns an error on the happy path. This deferred close just
    // exists in case an exception is thrown before we get there.
    //
    // The reason for this weird dance in this function and not in any of the
    // others is that errors reported when closing a file after a write are
    // usually indicative of failed writes. We, being a database company,
    // are generally incentivized to avoid ignoring file corruption only to
    // discover the issue at some unknown point in the future.
    if (file_h != INVALID_HANDLE_VALUE) {
      CloseHandle(file_h);
    }
  });

  if (file_h == INVALID_HANDLE_VALUE) {
    throw WinFSException("Error writing to file '" + path + "'; " +
        get_last_error_msg("CreateFile"));
  }

  // Get the current file size.
  LARGE_INTEGER offset_struct;
  if (!GetFileSizeEx(file_h, &offset)) {
    auto gle = GetLastError();
    throw WinFSException("Error writing to file '" + path + "'; " +
        get_last_error_msg(gle, "GetFileSizeEx"));
  }

  uint64_t offset = offset_struct.QuadPart;

  // Write data to the file in batches of constants::max_write_bytes bytes at a
  // time. Because this may be called in multiple threads, we don't seek the
  // file handle. Instead, we use the OVERLAPPED struct to specify an offset at
  // which to write. Note that the file handle does not have to be opened in
  // "overlapped" mode (i.e. async writes) to do this.
  auto data = static_cast<const char*>(buffer);
  while (nbytes > 0) {
    DWORD to_write;
    if (nbytes > std::numeric_limits<DWORD>::max()) {
      to_write = std::numeric_limits<DWORD>::max();
    } else {
      to_write = static_cast<DWORD>(nbytes);
    }

    offset_struct.QuadPart = offset;
    OVERLAPPED ov = {0, 0, {{0, 0}}, 0};
    ov.Offset = offset_struct.LowPart;
    ov.OffsetHigh = offset_struct.HighPart;

    DWORD nwritten = 0;
    if (WriteFile(file_h, data, to_write, &nwritten, &ov) == 0) {
      auto gle = GetLastError();
      throw WinFSException("Error writing to file '" + uri.to_path() + "'; "
        + get_last_error_msg(gle, "WriteFile"));
    }

    data += nwritten;
    offset += nwritten;
    nbytes -= nwritten;
  }

  // Always close the handle.
  if (CloseHandle(file_h) == 0) {
    // Mark the file as already closed for the deferred close.
    file_h = INVALID_FILE_HANDLE;
    throw WinFSException("Error closing file '" + uri.to_path() + "' after write.");
  }
}

void Win::read(
    const URI& uri,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes) {
  // Open the file with OPEN_EXISTING so that we don't accidentally create
  // a missing file.
  HANDLE file_h = CreateFile(
      path.c_str(),
      GENERIC_READ,
      FILE_SHARE_READ,
      NULL,
      OPEN_EXISTING,
      FILE_ATTRIBUTE_NORMAL,
      NULL);

  tiledb::common::defer([&]() {
    if (file_h != INVALID_HANDLE_VALUE) {
      CloseHandle(file_h);
    }
  });

  if (file_h == INVALID_HANDLE_VALUE) {
    throw WinFSException("Error reading from file '" + uri.to_path() + "'; " +
        get_last_error_msg("CreateFile"));
  }

  auto data = static_cast<char*>(buffer);

  while (nbytes > 0) {
    // Setup the required OVERLAPPED structure for the Windows equivalent
    // to POSIX's pread.
    LARGE_INTEGER offset_struct;
    offset_struct.QuadPart = offset;
    OVERLAPPED ov = {0, 0, {{0, 0}}, 0};
    ov.Offset = offset_struct.LowPart;
    ov.OffsetHigh = offset_struct.HighPart;

    // Windows can only up to 4GiB in one go so make sure we limit
    // to that in an individual read request.
    DWORD to_read;
    if(nbytes > std::numeric_limits<DWORD>::max()) {
      to_read = std::numeric_limits<DWORD>::max();
    } else {
      to_read = static_cast<DWORD>(nbytes);
    }

    LPDWORD nread;
    if (ReadFile(file_h, data, to_read, &nread, &ov) == 0) {
      auto gle = GetLastError();
      if (gle != 0) {
        throw WinFSException("Error reading from file '" + uri.to_path() + "'; " +
          get_last_error_msg(gle, "ReadFile");
      } else {
        throw WinFSException("Error reading from file '" + uri.to_path() +
          "'; read " + std::to_string(nread) " bytes when " +
          std::to_string(nbytes) " bytes were requested.");
      }
    }

    data += nread;
    offset += nread;
    nbytes -= nread;
  }

  if (CloseHandle(file_h) == 0) {
    file_h = INVALID_FILE_HANDLE;
    auto gle = GetLastError();
    throw WinFSException(
        "Error closing file '" + uri.to_path() + "'; " +
        get_last_error_msg(gle, "CloseHandle"));
  }
}

void Win::sync(const URI& uri) {
  if (!is_file(uri)) {
    return;
  }

  // Open the file with OPEN_EXISTING so that we don't accidentally create
  // a missing file.
  HANDLE file_h = CreateFile(
      uri.to_path().c_str(),
      GENERIC_WRITE,
      0,
      NULL,
      OPEN_EXISTING,
      FILE_ATTRIBUTE_NORMAL,
      NULL);

  tiledb::common::defer([&]() {
    if (file_h != INVALID_HANDLE_VALUE) {
      CloseHandle(file_h);
    }
  });

  if (file_h == INVALID_HANDLE_VALUE) {
    auto gle = GetLastError();
    throw WinFSException(
        "Error syncing file '" + uri.to_path() + "'; " +
        get_last_error_msg(gle, "CreateFile")));
  }

  if (FlushFileBuffers(file_h) == 0) {
    auto gle = GetLastError();
    throw WinFSException(
        "Error syncing file '" + uri.to_path() + "'; " +
        get_last_error_msg(gle, "FlushFileBuffers")));
  }

  if (CloseHandle(file_h) == 0) {
    file_h = INVALID_FILE_HANDLE;
    auto gle = GetLastError();
    return LOG_STATUS(Status_IOError(
        "Error syncing file '" + uri.to_path() + "'; " +
        get_last_error_msg(gle, "CloseHandle"));
  }
}

void Win::copy_file(const URI& src_uri, const URI& tgt_uri) {
  throw WinFSException("Implement me!");
}

void Win::move_file(const URI& src_uri, const URI& tgt_uri) {
  auto src_path = src_uri.to_path().c_str();
  auto tgt_path = tgt_uri.to_path().c_str();
  if (MoveFileEx(src_path, tgt_path, MOVEFILE_REPLACE_EXISTING) == 0) {
    auto gle = GetLastError();
    throw WinFSException("Error moving file '" + src_uri.to_path() + "'; " +
      get_last_error_msg(gel, "MoveFileEx"));
  }
}

void Win::remove_file(const URI& uri) {
  if (!DeleteFile(uri.to_path().c_str())) {
    auto gle = GetLastError();
    throw WinFSException("Error removing file '" + uri.to_path() + "'; "
      + get_last_error_msg(gle, "DeleteFile");
  }
}

}  // namespace tiledb::sm::filesystem

#endif  // _WIN32
