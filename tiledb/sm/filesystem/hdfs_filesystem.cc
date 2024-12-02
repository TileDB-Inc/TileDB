/**
 * @file   hdfs_filesystem.cc
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
 * NOTICE: The following code has been modified / adapted from:
 *
 * - TensorFlow HDFS platform support code under the Apache 2.0 license
 *   Copyright Tensorflow AUTHORS
 *
 * - Arrow HDFS IO backend under the Apache 2.0 license
 *   Copyright Arrow AUTHORS
 *
 * @section DESCRIPTION
 *
 * This file includes implementations of HDFS filesystem functions.
 */

#ifdef HAVE_HDFS

#include "hdfs_filesystem.h"
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/misc/constants.h"
#include "uri.h"

#include "hadoop/hdfs.h"

#include <dlfcn.h>
#include <cassert>
#include <climits>
#include <fstream>
#include <functional>
#include <iostream>
#include <sstream>

using namespace tiledb::common;
using tiledb::common::filesystem::directory_entry;

namespace tiledb::sm {

Status close_library(void* handle) {
  if (dlclose(handle)) {
    throw HDFSException(dlerror());
  }
  return Status::Ok();
}

Status load_library(const char* library_filename, void** handle) {
  *handle = dlopen(library_filename, RTLD_NOW | RTLD_LOCAL);
  if (!*handle) {
    throw HDFSException(dlerror());
  }
  return Status::Ok();
}

Status library_symbol(void* handle, const char* symbol_name, void** symbol) {
  *symbol = dlsym(handle, symbol_name);
  if (!*symbol) {
    throw HDFSException(dlerror());
  }
  return Status::Ok();
}

template <typename R, typename... Args>
Status bind_func(
    void* handle, const char* name, std::function<R(Args...)>* func) {
  void* symbol_ptr = nullptr;
  RETURN_NOT_OK(library_symbol(handle, name, &symbol_ptr));
  *func = reinterpret_cast<R (*)(Args...)>(symbol_ptr);
  return Status::Ok();
}

class LibHDFS {
 public:
  static LibHDFS* load() {
    static LibHDFS* lib = []() -> LibHDFS* {
      LibHDFS* lib = new LibHDFS;
      lib->load_and_bind();
      return lib;
    }();
    return lib;
  }

  // The status, if any, from failure to load.
  Status status() {
    return status_;
  }

  Status close() {
    if (handle_) {
      return close_library(handle_);
    }
    return Status::Ok();
  }

  std::function<hdfsFS(hdfsBuilder*)> hdfsBuilderSetForceNewInstance;
  std::function<hdfsFS(hdfsBuilder*)> hdfsBuilderConnect;
  std::function<hdfsBuilder*()> hdfsNewBuilder;
  std::function<void(hdfsBuilder*, const char*)> hdfsBuilderSetNameNode;
  std::function<int(const char*, char**)> hdfsConfGetStr;
  std::function<void(hdfsBuilder*, const char* kerbTicketCachePath)>
      hdfsBuilderSetKerbTicketCachePath;
  std::function<void(hdfsBuilder*, const char* userName)>
      hdfsBuilderSetUserName;
  std::function<int(hdfsFS, hdfsFile)> hdfsCloseFile;
  std::function<tSize(hdfsFS, hdfsFile, tOffset, void*, tSize)> hdfsPread;
  std::function<tSize(hdfsFS, hdfsFile, void*, tSize)> hdfsRead;
  std::function<tSize(hdfsFS, hdfsFile, const void*, tSize)> hdfsWrite;
  std::function<int(hdfsFS, hdfsFile)> hdfsHFlush;
  std::function<int(hdfsFS, hdfsFile)> hdfsHSync;
  std::function<hdfsFile(hdfsFS, const char*, int, int, short, tSize)>
      hdfsOpenFile;
  std::function<int(hdfsFS, const char*)> hdfsExists;
  std::function<hdfsFileInfo*(hdfsFS, const char*, int*)> hdfsListDirectory;
  std::function<void(hdfsFileInfo*, int)> hdfsFreeFileInfo;
  std::function<int(hdfsFS, const char*, int recursive)> hdfsDelete;
  std::function<int(hdfsFS, const char*)> hdfsCreateDirectory;
  std::function<hdfsFileInfo*(hdfsFS, const char*)> hdfsGetPathInfo;
  std::function<int(hdfsFS, const char*, const char*)> hdfsRename;
  std::function<int(hdfsFS, hdfsFile, tOffset)> hdfsSeek;
  std::function<int(hdfsFS)> hdfsDisconnect;

 private:
  void load_and_bind() {
    auto try_load_bind = [this](const char* name, void** handle) -> Status {
      RETURN_NOT_OK(load_library(name, handle));
#define BIND_HDFS_FUNC(function) \
  RETURN_NOT_OK(bind_func(*handle, #function, &function));
      BIND_HDFS_FUNC(hdfsBuilderSetForceNewInstance);
      BIND_HDFS_FUNC(hdfsBuilderConnect);
      BIND_HDFS_FUNC(hdfsNewBuilder);
      BIND_HDFS_FUNC(hdfsBuilderSetNameNode);
      BIND_HDFS_FUNC(hdfsConfGetStr);
      BIND_HDFS_FUNC(hdfsBuilderSetKerbTicketCachePath);
      BIND_HDFS_FUNC(hdfsBuilderSetUserName);
      BIND_HDFS_FUNC(hdfsCloseFile);
      BIND_HDFS_FUNC(hdfsPread);
      BIND_HDFS_FUNC(hdfsRead);
      BIND_HDFS_FUNC(hdfsWrite);
      BIND_HDFS_FUNC(hdfsHFlush);
      BIND_HDFS_FUNC(hdfsHSync);
      BIND_HDFS_FUNC(hdfsOpenFile);
      BIND_HDFS_FUNC(hdfsExists);
      BIND_HDFS_FUNC(hdfsListDirectory);
      BIND_HDFS_FUNC(hdfsFreeFileInfo);
      BIND_HDFS_FUNC(hdfsDelete);
      BIND_HDFS_FUNC(hdfsCreateDirectory);
      BIND_HDFS_FUNC(hdfsGetPathInfo);
      BIND_HDFS_FUNC(hdfsRename);
      BIND_HDFS_FUNC(hdfsSeek);
      BIND_HDFS_FUNC(hdfsDisconnect)
#undef BIND_HDFS_FUNC
      return Status::Ok();
    };

    // libhdfs.so won't be in the standard locations.
    // Use the path as specified in the libhdfs documentation.
    const char* hdfs_home = getenv("HADOOP_HOME");
    if (hdfs_home == nullptr) {
      status_ = Status_Error(
          "[TileDB::HDFS] Error: Environment variable HADOOP_HOME not set");
      return;
    }
#if defined(__APPLE__)
    const char* PATH_SEP = "/";
    const char* libname = "libhdfs.dylib";
#else
    const char* PATH_SEP = "/";
    const char* libname = "libhdfs.so";
#endif
    std::stringstream path;
    path << hdfs_home << PATH_SEP << "lib" << PATH_SEP << "native" << PATH_SEP
         << libname;
    status_ = try_load_bind(path.str().c_str(), &handle_);
    if (!status_.ok()) {
      // try load libhdfs.so using dynamic loader's search path in case
      // libhdfs.so is installed in non-standard location
      status_ = try_load_bind(libname, &handle_);
    }
  }

  Status status_;
  void* handle_ = nullptr;
};

HDFS::HDFS(const Config& config)
    : hdfs_(nullptr)
    , libhdfs_(LibHDFS::load())
    , hdfs_params_(HDFSParameters(config)) {
  // if libhdfs does not exist, just return and fail lazily on connection
  if (!libhdfs_->status().ok()) {
    return;
  }
  struct hdfsBuilder* builder = libhdfs_->hdfsNewBuilder();
  if (builder == nullptr) {
    throw HDFSException(
        "Failed to connect to hdfs, could not create connection builder");
  }
  libhdfs_->hdfsBuilderSetForceNewInstance(builder);
  if (hdfs_params_.name_node_uri_.empty())
    hdfs_params_.name_node_uri_ = "default";
  libhdfs_->hdfsBuilderSetNameNode(
      builder, hdfs_params_.name_node_uri_.c_str());
  if (!hdfs_params_.username_.empty())
    libhdfs_->hdfsBuilderSetUserName(builder, hdfs_params_.username_.c_str());
  if (!hdfs_params_.kerb_ticket_cache_path_.empty()) {
    libhdfs_->hdfsBuilderSetKerbTicketCachePath(
        builder, hdfs_params_.kerb_ticket_cache_path_.c_str());
  }
  // TODO: Set config strings
  hdfs_ = libhdfs_->hdfsBuilderConnect(builder);
  if (hdfs_ == nullptr) {
    throw HDFSException(
        "Failed to connect to HDFS namenode: " + hdfs_params_.name_node_uri_);
  }
}

Status HDFS::disconnect() {
  RETURN_NOT_OK(libhdfs_->status());
  if (libhdfs_->hdfsDisconnect(hdfs_) != 0) {
    throw HDFSException("Failed to disconnect hdfs");
  }
  hdfs_ = nullptr;
  return Status::Ok();
}

// Stub definition, we only try to connect once to a single fs at a time
Status HDFS::connect(hdfsFS* fs) const {
  RETURN_NOT_OK(libhdfs_->status());
  if (hdfs_ == nullptr) {
    throw HDFSException("Not connected to HDFS namenode");
  }
  *fs = hdfs_;
  return Status::Ok();
}

Status HDFS::is_dir(const URI& uri, bool* is_dir) const {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  int exists = libhdfs_->hdfsExists(fs, uri.to_path().c_str());
  if (exists == 0) {  // success
    hdfsFileInfo* fileInfo =
        libhdfs_->hdfsGetPathInfo(fs, uri.to_path().c_str());
    if (fileInfo == nullptr) {
      *is_dir = false;
    } else if ((char)(fileInfo->mKind) == 'D') {
      libhdfs_->hdfsFreeFileInfo(fileInfo, 1);
      *is_dir = true;
    } else {
      libhdfs_->hdfsFreeFileInfo(fileInfo, 1);
      *is_dir = false;
    }
  } else {
    *is_dir = false;
  }
  return Status::Ok();
}

Status HDFS::create_dir(const URI& uri) const {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  bool dir_exists = false;
  RETURN_NOT_OK(is_dir(uri, &dir_exists));
  if (dir_exists) {
    throw HDFSException(
        "Cannot create directory " + uri.to_string() +
        "'; Directory already exists");
  }
  int ret = libhdfs_->hdfsCreateDirectory(fs, uri.to_path().c_str());
  if (ret < 0) {
    throw HDFSException("Cannot create directory " + uri.to_string());
  }
  return Status::Ok();
}

Status HDFS::remove_dir(const URI& uri) const {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  int rc = libhdfs_->hdfsDelete(fs, uri.to_path().c_str(), 1);
  if (rc < 0) {
    throw HDFSException("Cannot remove path: " + uri.to_string());
  }
  return Status::Ok();
}

Status HDFS::move_path(const URI& old_uri, const URI& new_uri) const {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  if (libhdfs_->hdfsExists(fs, new_uri.to_path().c_str()) == 0) {
    throw HDFSException(
        "Cannot move path " + old_uri.to_string() + " to " +
        new_uri.to_string() + "; path exists.");
  }
  int ret = libhdfs_->hdfsRename(
      fs, old_uri.to_path().c_str(), new_uri.to_path().c_str());
  if (ret < 0) {
    throw HDFSException(
        "Error moving path " + old_uri.to_string() + " to " +
        new_uri.to_string());
  }
  return Status::Ok();
}

Status HDFS::is_file(const URI& uri, bool* is_file) const {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  int ret = libhdfs_->hdfsExists(fs, uri.to_path().c_str());
  if (!ret) {
    hdfsFileInfo* fileInfo =
        libhdfs_->hdfsGetPathInfo(fs, uri.to_path().c_str());
    if (fileInfo == NULL) {
      *is_file = false;
    } else if ((char)(fileInfo->mKind) == 'F') {
      libhdfs_->hdfsFreeFileInfo(fileInfo, 1);
      *is_file = true;
    } else {
      libhdfs_->hdfsFreeFileInfo(fileInfo, 1);
      *is_file = false;
    }
  } else {
    *is_file = false;
  }
  return Status::Ok();
}

Status HDFS::touch(const URI& uri) const {
  if (uri.to_string().back() == '/') {
    throw HDFSException(
        "Cannot create file; URI is a directory: " + uri.to_string());
  }

  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  hdfsFile writeFile =
      libhdfs_->hdfsOpenFile(fs, uri.to_path().c_str(), O_WRONLY, 0, 0, 0);
  if (!writeFile) {
    throw HDFSException(
        "Cannot create file " + uri.to_string() + "; File opening error");
  }
  // Close file
  if (libhdfs_->hdfsCloseFile(fs, writeFile)) {
    throw HDFSException(
        "Cannot create file " + uri.to_string() + "; File closing error");
  }
  return Status::Ok();
}

Status HDFS::remove_file(const URI& uri) const {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  int ret = libhdfs_->hdfsDelete(fs, uri.to_path().c_str(), 0);
  if (ret < 0) {
    throw HDFSException("Cannot delete file " + uri.to_string());
  }
  return Status::Ok();
}

Status HDFS::read(
    const URI& uri, off_t offset, void* buffer, uint64_t length) const {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  hdfsFile readFile =
      libhdfs_->hdfsOpenFile(fs, uri.to_path().c_str(), O_RDONLY, length, 0, 0);
  if (!readFile) {
    throw HDFSException(
        "Cannot read file " + uri.to_string() + ": file open error");
  }
  if (offset > std::numeric_limits<tOffset>::max()) {
    throw HDFSException(
        "Cannot read from from '" + uri.to_string() +
        "'; offset > typemax(tOffset)");
  }
  tOffset off = static_cast<tOffset>(offset);
  int ret = libhdfs_->hdfsSeek(fs, readFile, off);
  if (ret < 0) {
    throw HDFSException("Cannot seek to offset " + uri.to_string());
  }
  uint64_t bytes_to_read = length;
  char* buffptr = static_cast<char*>(buffer);
  do {
    tSize nbytes = (bytes_to_read <= INT_MAX) ? bytes_to_read : INT_MAX;
    tSize bytes_read =
        libhdfs_->hdfsRead(fs, readFile, static_cast<void*>(buffptr), nbytes);
    if (bytes_read < 0) {
      throw HDFSException(
          "Cannot read from file " + uri.to_string() + "; File reading error");
    }
    bytes_to_read -= bytes_read;
    buffptr += bytes_read;
  } while (bytes_to_read > 0);

  // Close file
  if (libhdfs_->hdfsCloseFile(fs, readFile)) {
    throw HDFSException(
        "Cannot read from file " + uri.to_string() + "; File closing error");
  }
  return Status::Ok();
}

Status HDFS::write(
    const URI& uri, const void* buffer, uint64_t buffer_size) const {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  bool file_exists = false;
  RETURN_NOT_OK(is_file(uri, &file_exists));
  int flags = file_exists ? O_WRONLY | O_APPEND : O_WRONLY;
  hdfsFile write_file = libhdfs_->hdfsOpenFile(
      fs, uri.to_path().c_str(), flags, constants::max_write_bytes, 0, 0);
  if (!write_file) {
    throw HDFSException(
        "Cannot write to file " + uri.to_string() + "; File opening error");
  }
  // Append data to the file in batches of
  // constants::max_write_bytes bytes at a time
  uint64_t buffer_bytes_written = 0;
  const char* buffer_bytes_ptr = static_cast<const char*>(buffer);
  while (buffer_size > constants::max_write_bytes) {
    tSize bytes_written = libhdfs_->hdfsWrite(
        fs,
        write_file,
        buffer_bytes_ptr + buffer_bytes_written,
        constants::max_write_bytes);
    if (bytes_written < 0 ||
        static_cast<uint64_t>(bytes_written) != constants::max_write_bytes) {
      throw HDFSException(
          "Cannot write to file " + uri.to_string() + "; File writing error");
    }
    buffer_bytes_written += bytes_written;
    buffer_size -= bytes_written;
  }
  tSize bytes_written = libhdfs_->hdfsWrite(
      fs, write_file, buffer_bytes_ptr + buffer_bytes_written, buffer_size);
  if (bytes_written < 0 ||
      static_cast<uint64_t>(bytes_written) != buffer_size) {
    throw HDFSException(
        "Cannot write to file '" + uri.to_string() + "'; File writing error");
  }
  // Close file
  if (libhdfs_->hdfsCloseFile(fs, write_file)) {
    throw HDFSException(
        "Cannot write to file " + uri.to_string() + "; File closing error");
  }
  return Status::Ok();
}

Status HDFS::sync(const URI& uri) const {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));

  bool file_exists = false;
  RETURN_NOT_OK(is_file(uri, &file_exists));
  if (!file_exists)
    return Status::Ok();

  // Open file
  hdfsFile file = libhdfs_->hdfsOpenFile(
      fs, uri.to_path().c_str(), O_WRONLY | O_APPEND, 0, 0, 0);
  if (!file) {
    throw HDFSException(
        "Cannot sync file '" + uri.to_string() + "'; File open error");
  }
  // Sync
  if (libhdfs_->hdfsHFlush(fs, file)) {
    throw HDFSException("Failed syncing file '" + uri.to_string() + "'");
  }
  // Close file
  if (libhdfs_->hdfsCloseFile(fs, file)) {
    throw HDFSException(
        "Cannot sync file " + uri.to_string() + "; File closing error");
  }
  return Status::Ok();
}

Status HDFS::ls(const URI& uri, std::vector<std::string>* paths) const {
  auto entries = ls_with_sizes(uri);
  for (auto& fs : entries) {
    paths->emplace_back(fs.path().native());
  }

  return Status::Ok();
}

std::vector<directory_entry> HDFS::ls_with_sizes(const URI& uri) const {
  hdfsFS fs = nullptr;
  throw_if_not_ok(connect(&fs));

  int numEntries = 0;
  hdfsFileInfo* fileList =
      libhdfs_->hdfsListDirectory(fs, uri.to_path().c_str(), &numEntries);
  if (fileList == NULL) {
    if (errno) {
      throw HDFSException("Cannot list files in " + uri.to_string());
    }
  }

  std::vector<directory_entry> entries;
  for (int i = 0; i < numEntries; ++i) {
    auto path = std::string(fileList[i].mName);
    if (!utils::parse::starts_with(path, "hdfs://")) {
      path = std::string("hdfs://") + path;
    }
    if (fileList[i].mKind == kObjectKindDirectory) {
      entries.emplace_back(path, 0, true);
    } else {
      entries.emplace_back(path, fileList[i].mSize, false);
    }
  }
  libhdfs_->hdfsFreeFileInfo(fileList, numEntries);
  return entries;
}

Status HDFS::file_size(const URI& uri, uint64_t* nbytes) const {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  hdfsFileInfo* fileInfo = libhdfs_->hdfsGetPathInfo(fs, uri.to_path().c_str());
  if (fileInfo == nullptr) {
    throw HDFSException("Not a file " + uri.to_string());
  }
  if ((char)(fileInfo->mKind) == 'F') {
    *nbytes = static_cast<uint64_t>(fileInfo->mSize);
  } else {
    libhdfs_->hdfsFreeFileInfo(fileInfo, 1);
    throw HDFSException("Not a file " + uri.to_string());
  }
  libhdfs_->hdfsFreeFileInfo(fileInfo, 1);
  return Status::Ok();
}

}  // namespace tiledb::sm

#endif  // HAVE_HDFS
