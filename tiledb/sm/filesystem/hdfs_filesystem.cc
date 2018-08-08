/**
 * @file   hdfs_filesystem.cc
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

#include "tiledb/sm/filesystem/hdfs_filesystem.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"

#include <dlfcn.h>
#include <fstream>
#include <iostream>
#include <sstream>

namespace tiledb {
namespace sm {

namespace hdfs {

Status close_library(void* handle) {
  if (dlclose(handle)) {
    return Status::HDFSError(dlerror());
  }
  return Status::Ok();
}

Status load_library(const char* library_filename, void** handle) {
  *handle = dlopen(library_filename, RTLD_NOW | RTLD_LOCAL);
  if (!*handle) {
    return Status::HDFSError(dlerror());
  }
  return Status::Ok();
}

Status library_symbol(void* handle, const char* symbol_name, void** symbol) {
  *symbol = dlsym(handle, symbol_name);
  if (!*symbol) {
    return Status::HDFSError(dlerror());
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
      status_ = Status::HDFSError("Environment variable HADOOP_HOME not set");
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

HDFS::HDFS()
    : hdfs_(nullptr)
    , libhdfs_(LibHDFS::load()) {
}

Status HDFS::init(const Config::HDFSParams& config) {
  // if libhdfs does not exist, just return and fail lazily on connection
  if (!libhdfs_->status().ok()) {
    return Status::Ok();
  }
  struct hdfsBuilder* builder = libhdfs_->hdfsNewBuilder();
  if (builder == nullptr) {
    return LOG_STATUS(Status::HDFSError(
        "Failed to connect to hdfs, could not create connection builder"));
  }
  libhdfs_->hdfsBuilderSetForceNewInstance(builder);
  std::string namenode_uri;
  if (!config.name_node_uri_.empty()) {
    namenode_uri = config.name_node_uri_;
  } else {
    namenode_uri = "default";
  }
  libhdfs_->hdfsBuilderSetNameNode(builder, namenode_uri.c_str());
  if (!config.username_.empty()) {
    libhdfs_->hdfsBuilderSetUserName(builder, config.username_.c_str());
  }
  if (!config.kerb_ticket_cache_path_.empty()) {
    libhdfs_->hdfsBuilderSetKerbTicketCachePath(
        builder, config.kerb_ticket_cache_path_.c_str());
  }
  // TODO: Set config strings
  hdfs_ = libhdfs_->hdfsBuilderConnect(builder);
  if (hdfs_ == nullptr) {
    // TODO: errno for better options
    return LOG_STATUS(Status::HDFSError(
        std::string("Failed to connect to HDFS namenode: ") + namenode_uri));
  }
  return Status::Ok();
}

Status HDFS::disconnect() {
  RETURN_NOT_OK(libhdfs_->status());
  if (libhdfs_->hdfsDisconnect(hdfs_) != 0) {
    return LOG_STATUS(Status::HDFSError("Failed to disconnect hdfs"));
  }
  hdfs_ = nullptr;
  return Status::Ok();
}

// Stub definition, we only try to connect once to a single fs at a time
Status HDFS::connect(hdfsFS* fs) {
  RETURN_NOT_OK(libhdfs_->status());
  if (hdfs_ == nullptr) {
    return LOG_STATUS(Status::HDFSError("Not connected to HDFS namenode"));
  }
  *fs = hdfs_;
  return Status::Ok();
}

Status HDFS::is_dir(const URI& uri, bool* is_dir) {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs))
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

Status HDFS::create_dir(const URI& uri) {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs))
  bool dir_exists = false;
  RETURN_NOT_OK(is_dir(uri, &dir_exists));
  if (dir_exists) {
    return LOG_STATUS(Status::HDFSError(
        std::string("Cannot create directory ") + uri.to_string() +
        "'; Directory already exists"));
  }
  int ret = libhdfs_->hdfsCreateDirectory(fs, uri.to_path().c_str());
  if (ret < 0) {
    return LOG_STATUS(Status::HDFSError(
        std::string("Cannot create directory ") + uri.to_string()));
  }
  return Status::Ok();
}

Status HDFS::remove_dir(const URI& uri) {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs))
  int rc = libhdfs_->hdfsDelete(fs, uri.to_path().c_str(), 1);
  if (rc < 0) {
    return LOG_STATUS(
        Status::HDFSError("Cannot remove path: " + uri.to_string()));
  }
  return Status::Ok();
}

Status HDFS::move_path(const URI& old_uri, const URI& new_uri) {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  if (libhdfs_->hdfsExists(fs, new_uri.to_path().c_str()) == 0) {
    return LOG_STATUS(Status::HDFSError(
        "Cannot move path " + old_uri.to_string() + " to " +
        new_uri.to_string() + "; path exists."));
  }
  int ret = libhdfs_->hdfsRename(
      fs, old_uri.to_path().c_str(), new_uri.to_path().c_str());
  if (ret < 0) {
    return LOG_STATUS(Status::HDFSError(
        "Error moving path " + old_uri.to_string() + " to " +
        new_uri.to_string()));
  }
  return Status::Ok();
}

Status HDFS::is_file(const URI& uri, bool* is_file) {
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

Status HDFS::touch(const URI& uri) {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  hdfsFile writeFile =
      libhdfs_->hdfsOpenFile(fs, uri.to_path().c_str(), O_WRONLY, 0, 0, 0);
  if (!writeFile) {
    return LOG_STATUS(Status::HDFSError(
        std::string("Cannot create file ") + uri.to_string() +
        "; File opening error"));
  }
  // Close file
  if (libhdfs_->hdfsCloseFile(fs, writeFile)) {
    return LOG_STATUS(Status::HDFSError(
        std::string("Cannot create file ") + uri.to_string() +
        "; File closing error"));
  }
  return Status::Ok();
}

Status HDFS::remove_file(const URI& uri) {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  int ret = libhdfs_->hdfsDelete(fs, uri.to_path().c_str(), 0);
  if (ret < 0) {
    return LOG_STATUS(Status::HDFSError(
        std::string("Cannot delete file ") + uri.to_string()));
  }
  return Status::Ok();
}

Status HDFS::read(const URI& uri, off_t offset, void* buffer, uint64_t length) {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  hdfsFile readFile =
      libhdfs_->hdfsOpenFile(fs, uri.to_path().c_str(), O_RDONLY, length, 0, 0);
  if (!readFile) {
    return LOG_STATUS(Status::HDFSError(
        std::string("Cannot read file ") + uri.to_string() +
        ": file open error"));
  }
  if (offset > std::numeric_limits<tOffset>::max()) {
    return LOG_STATUS(Status::HDFSError(
        std::string("Cannot read from from '") + uri.to_string() +
        "'; offset > typemax(tOffset)"));
  }
  tOffset off = static_cast<tOffset>(offset);
  int ret = libhdfs_->hdfsSeek(fs, readFile, off);
  if (ret < 0) {
    return LOG_STATUS(Status::HDFSError(
        std::string("Cannot seek to offset ") + uri.to_string()));
  }
  uint64_t bytes_to_read = length;
  char* buffptr = static_cast<char*>(buffer);
  do {
    tSize nbytes = (bytes_to_read <= INT_MAX) ? bytes_to_read : INT_MAX;
    tSize bytes_read =
        libhdfs_->hdfsRead(fs, readFile, static_cast<void*>(buffptr), nbytes);
    if (bytes_read < 0) {
      return LOG_STATUS(Status::HDFSError(
          "Cannot read from file " + uri.to_string() + "; File reading error"));
    }
    bytes_to_read -= bytes_read;
    buffptr += bytes_read;
  } while (bytes_to_read > 0);

  // Close file
  if (libhdfs_->hdfsCloseFile(fs, readFile)) {
    return LOG_STATUS(Status::HDFSError(
        std::string("Cannot read from file ") + uri.to_string() +
        "; File closing error"));
  }
  return Status::Ok();
}

Status HDFS::write(const URI& uri, const void* buffer, uint64_t buffer_size) {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  bool file_exists = false;
  RETURN_NOT_OK(is_file(uri, &file_exists));
  int flags = file_exists ? O_WRONLY | O_APPEND : O_WRONLY;
  hdfsFile write_file = libhdfs_->hdfsOpenFile(
      fs, uri.to_path().c_str(), flags, constants::max_write_bytes, 0, 0);
  if (!write_file) {
    return LOG_STATUS(Status::HDFSError(
        std::string("Cannot write to file ") + uri.to_string() +
        "; File opening error"));
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
      return LOG_STATUS(Status::HDFSError(
          std::string("Cannot write to file ") + uri.to_string() +
          "; File writing error"));
    }
    buffer_bytes_written += bytes_written;
    buffer_size -= bytes_written;
  }
  tSize bytes_written = libhdfs_->hdfsWrite(
      fs, write_file, buffer_bytes_ptr + buffer_bytes_written, buffer_size);
  if (bytes_written < 0 ||
      static_cast<uint64_t>(bytes_written) != buffer_size) {
    return LOG_STATUS(Status::HDFSError(
        std::string("Cannot write to file '") + uri.to_string() +
        "'; File writing error"));
  }
  // Close file
  if (libhdfs_->hdfsCloseFile(fs, write_file)) {
    return LOG_STATUS(Status::HDFSError(
        std::string("Cannot write to file ") + uri.to_string() +
        "; File closing error"));
  }
  return Status::Ok();
}

Status HDFS::sync(const URI& uri) {
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
    return LOG_STATUS(Status::HDFSError(
        std::string("Cannot sync file '") + uri.to_string() +
        "'; File open error"));
  }
  // Sync
  if (libhdfs_->hdfsHFlush(fs, file)) {
    return LOG_STATUS(Status::HDFSError(
        std::string("Failed syncing file '") + uri.to_string() + "'"));
  }
  // Close file
  if (libhdfs_->hdfsCloseFile(fs, file)) {
    return LOG_STATUS(Status::HDFSError(
        std::string("Cannot sync file ") + uri.to_string() +
        "; File closing error"));
  }
  return Status::Ok();
}

Status HDFS::ls(const URI& uri, std::vector<std::string>* paths) {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs))
  int numEntries = 0;
  hdfsFileInfo* fileList =
      libhdfs_->hdfsListDirectory(fs, uri.to_path().c_str(), &numEntries);
  if (fileList == NULL) {
    if (errno) {
      return LOG_STATUS(Status::HDFSError(
          std::string("Cannot list files in ") + uri.to_string()));
    }
  }
  for (int i = 0; i < numEntries; ++i) {
    auto path = std::string(fileList[i].mName);
    if (!utils::parse::starts_with(path, "hdfs://")) {
      path = std::string("hdfs://") + path;
    }
    paths->push_back(path);
  }
  libhdfs_->hdfsFreeFileInfo(fileList, numEntries);
  return Status::Ok();
}

Status HDFS::file_size(const URI& uri, uint64_t* nbytes) {
  hdfsFS fs = nullptr;
  RETURN_NOT_OK(connect(&fs));
  hdfsFileInfo* fileInfo = libhdfs_->hdfsGetPathInfo(fs, uri.to_path().c_str());
  if (fileInfo == nullptr) {
    return LOG_STATUS(
        Status::HDFSError(std::string("Not a file ") + uri.to_string()));
  }
  if ((char)(fileInfo->mKind) == 'F') {
    *nbytes = static_cast<uint64_t>(fileInfo->mSize);
  } else {
    libhdfs_->hdfsFreeFileInfo(fileInfo, 1);
    return LOG_STATUS(
        Status::HDFSError(std::string("Not a file ") + uri.to_string()));
  }
  libhdfs_->hdfsFreeFileInfo(fileInfo, 1);
  return Status::Ok();
}

}  // namespace hdfs

}  // namespace sm
}  // namespace tiledb

#endif  // HAVE_HDFS
