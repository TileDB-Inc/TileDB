/**
 * @file   hdfs_filesystem.cc
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
 * This file includes implementations of HDFS filesystem functions.
 */

#ifdef HAVE_HDFS

#include "hdfs_filesystem.h"

#include <fstream>
#include <iostream>

#include "constants.h"
#include "logger.h"
#include "utils.h"

namespace tiledb {

namespace hdfs {

Status connect(hdfsFS& fs) {
  struct hdfsBuilder* builder = hdfsNewBuilder();
  if (builder == nullptr) {
    return LOG_STATUS(Status::IOError(
        "Failed to connect to hdfs, could not create connection builder"));
  }
  // TODO: builder options
  hdfsBuilderSetForceNewInstance(builder);
  hdfsBuilderSetNameNode(builder, "default");
  fs = hdfsBuilderConnect(builder);
  if (fs == nullptr) {
    // TODO: errno for better options
    return LOG_STATUS(
        Status::IOError(std::string("Failed to connect to hdfs")));
  }
  return Status::Ok();
}

Status disconnect(hdfsFS& fs) {
  if (hdfsDisconnect(fs) != 0) {
    return LOG_STATUS(
        Status::IOError(std::string("Failed to disconnect hdfs")));
  }
  return Status::Ok();
}

// create a directory with the given path
Status create_dir(hdfsFS fs, const URI& uri) {
  if (is_dir(fs, uri)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create directory ") + uri.to_string() +
        "'; Directory already exists"));
  }
  int ret = hdfsCreateDirectory(fs, uri.to_path().c_str());
  if (ret < 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create directory ") + uri.to_string()));
  }
  return Status::Ok();
}

// delete the directory with the given path
Status delete_dir(hdfsFS fs, const URI& uri) {
  int ret = hdfsDelete(fs, uri.to_path().c_str(), 1);
  if (ret < 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot delete directory '") + uri.to_string()));
  }
  return Status::Ok();
}

Status move_dir(hdfsFS fs, const URI& old_uri, const URI& new_uri) {
  int ret =
      hdfsRename(fs, old_uri.to_path().c_str(), new_uri.to_path().c_str());
  if (ret < 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot move directory ") + old_uri.to_string() + " to " +
        new_uri.to_string()));
  }
  return Status::Ok();
}

bool is_dir(hdfsFS fs, const URI& uri) {
  int exists = hdfsExists(fs, uri.to_path().c_str());
  if (exists == 0) {  // success
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs, uri.to_path().c_str());
    if (fileInfo == nullptr) {
      return false;
    }
    if ((char)(fileInfo->mKind) == 'D') {
      hdfsFreeFileInfo(fileInfo, 1);
      return true;
    } else {
      hdfsFreeFileInfo(fileInfo, 1);
      return false;
    }
  }
  return false;
}

// Is the given path a valid file
bool is_file(hdfsFS fs, const URI& uri) {
  int ret = hdfsExists(fs, uri.to_path().c_str());
  if (!ret) {
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs, uri.to_path().c_str());
    if (fileInfo == NULL) {
      return false;
    }
    if ((char)(fileInfo->mKind) == 'F') {
      hdfsFreeFileInfo(fileInfo, 1);
      return true;
    } else {
      hdfsFreeFileInfo(fileInfo, 1);
      return false;
    }
  }
  return false;
}

// create a file with the given path
Status create_file(hdfsFS fs, const URI& uri) {
  // Open file
  hdfsFile writeFile =
      hdfsOpenFile(fs, uri.to_path().c_str(), O_WRONLY, 0, 0, 0);
  if (!writeFile) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create file ") + uri.to_string() +
        "; File opening error"));
  }
  // Close file
  if (hdfsCloseFile(fs, writeFile)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create file ") + uri.to_string() +
        "; File closing error"));
  }
  return Status::Ok();
}

// delete a file with the given path
Status delete_file(hdfsFS fs, const URI& uri) {
  int ret = hdfsDelete(fs, uri.to_path().c_str(), 0);
  if (ret < 0) {
    return LOG_STATUS(
        Status::IOError(std::string("Cannot delete file ") + uri.to_string()));
  }
  return Status::Ok();
}

// Read length bytes from file give by path from byte offset offset into pre
// allocated buffer buffer.
Status read_from_file(
    hdfsFS fs, const URI& uri, off_t offset, void* buffer, uint64_t length) {
  hdfsFile readFile =
      hdfsOpenFile(fs, uri.to_path().c_str(), O_RDONLY, length, 0, 0);
  if (!readFile) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot read file ") + uri.to_string() +
        ": file open error"));
  }
  int ret = hdfsSeek(fs, readFile, (tOffset)offset);
  if (ret < 0) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot seek to offset ") + uri.to_string()));
  }
  uint64_t bytes_to_read = length;
  char* buffptr = static_cast<char*>(buffer);
  do {
    tSize nbytes = (bytes_to_read <= INT_MAX) ? bytes_to_read : INT_MAX;
    tSize bytes_read =
        hdfsRead(fs, readFile, static_cast<void*>(buffptr), nbytes);
    if (bytes_read < 0) {
      return LOG_STATUS(Status::IOError(
          "Cannot read from file " + uri.to_string() + "; File reading error"));
    }
    bytes_to_read -= bytes_read;
    buffptr += bytes_read;
  } while (bytes_to_read > 0);

  // Close file
  if (hdfsCloseFile(fs, readFile)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot read from file ") + uri.to_string() +
        "; File closing error"));
  }
  return Status::Ok();
}

Status read_from_file(hdfsFS fs, const URI& uri, Buffer** buff) {
  // get the file size
  uint64_t nbytes = 0;
  RETURN_NOT_OK(file_size(fs, uri, &nbytes));
  // create a new buffer
  *buff = new Buffer();
  (*buff)->realloc(nbytes);
  // Read contents
  Status st = read_from_file(fs, uri, 0, (*buff)->data(), nbytes);
  if (!st.ok()) {
    delete *buff;
    return LOG_STATUS(Status::IOError(
        "Cannot read from file " + uri.to_string() + "; File reading error"));
  }
  return st;
}

// Write length bytes of buffer to a given path
Status write_to_file(
    hdfsFS fs, const URI& uri, const void* buffer, const uint64_t length) {
  int flags = is_file(fs, uri) ? O_WRONLY | O_APPEND : O_WRONLY;
  hdfsFile writeFile = hdfsOpenFile(
      fs, uri.to_path().c_str(), flags, constants::max_write_bytes, 0, 0);
  if (!writeFile) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot write to file ") + uri.to_string() +
        "; File opening error"));
  }
  // Append data to the file in batches of Configurator::max_write_bytes()
  // bytes at a time
  // ssize_t bytes_written = 0;
  off_t nrRemaining = 0;
  tSize curSize = 0;
  tSize written = 0;
  for (nrRemaining = (off_t)length; nrRemaining > 0;
       nrRemaining -= constants::max_write_bytes) {
    curSize = (constants::max_write_bytes < nrRemaining) ?
                  constants::max_write_bytes :
                  static_cast<tSize>(nrRemaining);
    if ((written = hdfsWrite(fs, writeFile, buffer, curSize)) != curSize) {
      return LOG_STATUS(Status::IOError(
          std::string("Cannot write to file ") + uri.to_string() +
          "; File writing error"));
    }
    hdfsFlush(fs, writeFile);
  }
  // Close file
  if (hdfsCloseFile(fs, writeFile)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot write to file ") + uri.to_string() +
        "; File closing error"));
  }
  return Status::Ok();
}

// List all subdirectories and files for a given path, appending them to paths.
Status ls(hdfsFS fs, const URI& uri, std::vector<std::string>* paths) {
  int numEntries = 0;
  hdfsFileInfo* fileList =
      hdfsListDirectory(fs, uri.to_path().c_str(), &numEntries);
  if (fileList == NULL) {
    if (errno) {
      return LOG_STATUS(Status::IOError(
          std::string("Cannot list files in ") + uri.to_string()));
    }
  }
  for (int i = 0; i < numEntries; ++i) {
    auto path = std::string(fileList[i].mName);
    if (!utils::starts_with("hdfs://", path)) {
      path = std::string("hdfs://") + path;
    }
    paths->push_back(path);
  }
  hdfsFreeFileInfo(fileList, numEntries);
  return Status::Ok();
}

// File size in bytes for a given path
Status file_size(hdfsFS fs, const URI& uri, uint64_t* nbytes) {
  hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs, uri.to_path().c_str());
  if (fileInfo == NULL) {
    return LOG_STATUS(
        Status::IOError(std::string("Not a file ") + uri.to_string()));
  }
  if ((char)(fileInfo->mKind) == 'F') {
    *nbytes = static_cast<uint64_t>(fileInfo->mSize);
  } else {
    hdfsFreeFileInfo(fileInfo, 1);
    return LOG_STATUS(
        Status::IOError(std::string("Not a file ") + uri.to_string()));
  }
  hdfsFreeFileInfo(fileInfo, 1);
  return Status::Ok();
}

}  // namespace hdfs

}  // namespace tiledb

#endif
