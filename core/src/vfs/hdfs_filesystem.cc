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
#include "../../include/vfs/hdfs_filesystem.h"
#include "constants.h"
#include "logger.h"
#include "utils.h"

#include <fstream>
#include <iostream>

namespace tiledb {

namespace vfs {

namespace hdfs {

Status connect(hdfsFS& fs) {
  fs = hdfsConnect("default", 0);
  if (!fs) {
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
Status create_dir(const std::string& path, hdfsFS fs) {
  if (hdfs::is_dir(path, fs)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create directory '") + path +
        "'; Directory already exists"));
  }
  int ret = hdfsCreateDirectory(fs, path.c_str());
  if (ret < 0) {
    return LOG_STATUS(
        Status::IOError(std::string("Cannot create directory '") + path));
  }
  return Status::Ok();
}

// delete the directory with the given path
Status delete_dir(const std::string& path, hdfsFS fs) {
  int ret = hdfsDelete(fs, path.c_str(), 1);
  if (ret < 0) {
    return LOG_STATUS(
        Status::IOError(std::string("Cannot delete directory '") + path));
  }
  return Status::Ok();
}

// Is the given path a valid directory
bool is_dir(const std::string& path, hdfsFS fs) {
  hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs, path.c_str());
  if (fileInfo == NULL) {
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

// Is the given path a valid file
bool is_file(const std::string& path, hdfsFS fs) {
  hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs, path.c_str());
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

// create a file with the given path
Status create_file(const std::string& path, hdfsFS fs) {
  // Open file
  hdfsFile writeFile = hdfsOpenFile(fs, path.c_str(), O_WRONLY, 0, 0, 0);
  if (!writeFile) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create file '") + path + "'; File opening error"));
  }
  // Close file
  if (hdfsCloseFile(fs, writeFile)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create file '") + path + "'; File closing error"));
  }

  // Success
  return Status::Ok();
}

// delete a file with the given path
Status delete_file(const std::string& path, hdfsFS fs) {
  int ret = hdfsDelete(fs, path.c_str(), 0);
  if (ret < 0) {
    return LOG_STATUS(
        Status::IOError(std::string("Cannot delete file '") + path));
  }
  return Status::Ok();
}

// Read length bytes from file give by path from byte offset offset into pre
// allocated buffer buffer.
Status read_from_file(
    const std::string& path,
    off_t offset,
    void* buffer,
    size_t length,
    hdfsFS fs) {
  hdfsFile readFile = hdfsOpenFile(fs, path.c_str(), O_RDONLY, length, 0, 0);
  if (!readFile) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot read file '") + path + "': file open error"));
  }
  int ret = hdfsSeek(fs, readFile, (tOffset)offset);
  if (ret < 0) {
    return LOG_STATUS(
        Status::IOError(std::string("Cannot seek to offset '") + path));
  }

  tSize bytes_read = hdfsRead(fs, readFile, buffer, (tSize)length);
  if (bytes_read != (tSize)length) {
    return LOG_STATUS(
        Status::IOError("Cannot read from file; File reading error"));
  }

  // Close file
  if (hdfsCloseFile(fs, readFile)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot read from file '") + path +
        "'; File closing error"));
  }

  // Success
  return Status::Ok();
}

// Write length bytes of buffer to a given path
Status write_to_file(
    const std::string& path,
    const void* buffer,
    const size_t length,
    hdfsFS fs) {
  // Open file
  hdfsFile writeFile = hdfsOpenFile(
      fs, path.c_str(), O_WRONLY, constants::max_write_bytes, 0, 0);
  if (!writeFile) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot write to file '") + path +
        "'; File opening error"));
  }
  // Append data to the file in batches of Configurator::max_write_bytes()
  // bytes at a time
  ssize_t bytes_written;
  off_t nrRemaining;
  tSize curSize;
  tSize written;
  for (nrRemaining = (off_t)length; nrRemaining > 0;
       nrRemaining -= constants::max_write_bytes) {
    curSize = (constants::max_write_bytes < nrRemaining) ?
                  constants::max_write_bytes :
                  (tSize)nrRemaining;
    if ((written = hdfsWrite(fs, writeFile, (void*)buffer, curSize)) !=
        curSize) {
      return LOG_STATUS(Status::IOError(
          std::string("Cannot write to file '") + path +
          "'; File writing error"));
    }
  }

  // Close file
  if (hdfsCloseFile(fs, writeFile)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot write to file '") + path +
        "'; File closing error"));
  }

  // Success
  return Status::Ok();
}

// List all subdirectories and files for a given path, appending them to paths.
Status ls(const std::string& path, std::vector<std::string>& paths, hdfsFS fs) {
  int numEntries;

  hdfsFileInfo* fileList = hdfsListDirectory(fs, path.c_str(), &numEntries);
  if (fileList == NULL) {
    if (errno) {
      return LOG_STATUS(
          Status::IOError(std::string("Cannot list files in '") + path + "';"));
    }
  }

  for (int i = 0; i < numEntries; ++i) {
    paths.push_back(std::string(fileList[i].mName));
  }
  hdfsFreeFileInfo(fileList, numEntries);

  // Success
  return Status::Ok();
}

// List all subdirectories (1 level deep) for a given path, appending them to
// dpaths.  Ordering does not matter.
Status ls_dirs(
    const std::string& path, std::vector<std::string>& dpaths, hdfsFS fs) {
  int numEntries;

  hdfsFileInfo* fileList = hdfsListDirectory(fs, path.c_str(), &numEntries);
  if (fileList == NULL) {
    if (errno) {
      return LOG_STATUS(
          Status::IOError(std::string("Cannot list files in '") + path + "';"));
    }
  }

  for (int i = 0; i < numEntries; ++i) {
    if ((char)(fileList[i].mKind) == 'D')
      dpaths.push_back(std::string(fileList[i].mName));
  }
  hdfsFreeFileInfo(fileList, numEntries);

  // Success
  return Status::Ok();
}

// List all subfiles (1 level deep) for a given path, appending them to fpaths.
// Ordering does not matter.
Status ls_files(
    const std::string& path, std::vector<std::string>& fpaths, hdfsFS fs) {
  int numEntries;

  hdfsFileInfo* fileList = hdfsListDirectory(fs, path.c_str(), &numEntries);
  if (fileList == NULL) {
    if (errno) {
      return LOG_STATUS(
          Status::IOError(std::string("Cannot list files in '") + path + "';"));
    }
  }

  for (int i = 0; i < numEntries; ++i) {
    if ((char)(fileList[i].mKind) == 'F')
      fpaths.push_back(std::string(fileList[i].mName));
  }
  hdfsFreeFileInfo(fileList, numEntries);

  // Success
  return Status::Ok();
}

// File size in bytes for a given path
Status filesize(const std::string& path, size_t* nbytes, hdfsFS fs) {
  hdfsFileInfo* fileInfo = hdfsGetPathInfo(fs, path.c_str());
  if (fileInfo == NULL) {
    return LOG_STATUS(
        Status::IOError(std::string("Not a file '") + path + "';"));
  }
  if ((char)(fileInfo->mKind) == 'F') {
    *nbytes = fileInfo->mSize;
  } else {
    hdfsFreeFileInfo(fileInfo, 1);
    return LOG_STATUS(
        Status::IOError(std::string("Not a file '") + path + "';"));
  }
  hdfsFreeFileInfo(fileInfo, 1);
  // Success
  return Status::Ok();
}

}  // namespace hdfs

}  // namespace vfs

}  // namespace tiledb

#endif
