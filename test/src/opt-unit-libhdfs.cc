/**
 * @file   opt-unit-libhdfs.cc
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
 * Tests for primitive HDFS filesystem functions.
 */
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include "catch.hpp"
#include "hdfs.h"

static off_t totalSize;
static struct timeval tm1;

struct LibHDFSFx {
  static inline void start() {
    gettimeofday(&tm1, NULL);
  }

  static inline void stop() {
    struct timeval tm2;
    gettimeofday(&tm2, NULL);

    unsigned long long t =
        1000000 * (tm2.tv_sec - tm1.tv_sec) + (tm2.tv_usec - tm1.tv_usec);
    printf("Time: %llu us, MB/s:%f\n", t, (double)totalSize / (double)t);
  }

  LibHDFSFx() {
  }

  ~LibHDFSFx() {
  }

  int check_hdfs_write(
      const char *writeFileName, off_t fileTotalSize, tSize bufferSize) {
    hdfsFS fs;
    hdfsFile writeFile;
    char *buffer;
    int i;
    off_t nrRemaining;
    tSize curSize;
    tSize written;

    fs = hdfsConnect("default", 0);
    CHECK(fs);
    if (!fs) {
      fprintf(stderr, "Oops! Failed to connect to hdfs!\n");
      exit(-1);
    }

    // sanity check
    if (fileTotalSize == ULONG_MAX && errno == ERANGE) {
      fprintf(stderr, "invalid file size - must be <= %lu\n", ULONG_MAX);
      exit(-3);
    }

    writeFile = hdfsOpenFile(fs, writeFileName, O_WRONLY, bufferSize, 0, 0);
    CHECK(writeFile);
    if (!writeFile) {
      fprintf(stderr, "Failed to open %s for writing!\n", writeFileName);
      exit(-2);
    }

    // data to be written to the file
    buffer = (char *)malloc(sizeof(char) * bufferSize);
    if (buffer == NULL) {
      fprintf(stderr, "Could not allocate buffer of size %d\n", bufferSize);
      return -2;
    }
    for (i = 0; i < bufferSize; ++i) {
      buffer[i] = 'a' + (i % 26);
    }

    printf(
        "Test write total bytes: %d with buffer size: %d\n",
        fileTotalSize,
        bufferSize);
    start();
    // write to the file
    for (nrRemaining = fileTotalSize; nrRemaining > 0;
         nrRemaining -= bufferSize) {
      curSize = (bufferSize < nrRemaining) ? bufferSize : (tSize)nrRemaining;
      if ((written = hdfsWrite(fs, writeFile, (void *)buffer, curSize)) !=
          curSize) {
        fprintf(
            stderr,
            "ERROR: hdfsWrite returned an error on write: %d\n",
            written);
        exit(-3);
      }
    }
    stop();

    free(buffer);
    hdfsCloseFile(fs, writeFile);
    hdfsDisconnect(fs);

    return 0;
  }

  int check_hdfs_read(const char *rfile, tSize bufferSize) {
    hdfsFS fs;
    hdfsFile readFile;
    char *buffer;
    tSize curSize;
    off_t readSize = 0;

    fs = hdfsConnect("default", 0);
    CHECK(fs);
    if (!fs) {
      fprintf(stderr, "Oops! Failed to connect to hdfs!\n");
      exit(-1);
    }

    readFile = hdfsOpenFile(fs, rfile, O_RDONLY, bufferSize, 0, 0);
    CHECK(readFile);
    if (!readFile) {
      fprintf(stderr, "Failed to open %s for writing!\n", rfile);
      exit(-2);
    }

    // data to be written to the file
    buffer = (char *)malloc(sizeof(char) * bufferSize);
    if (buffer == NULL) {
      return -2;
    }

    printf("Test read with buffer size: %d\n", bufferSize);
    start();
    // read from the file
    curSize = 1;
    while (curSize > 0) {
      curSize = hdfsRead(fs, readFile, (void *)buffer, bufferSize);
      readSize += curSize;
    }
    stop();
    CHECK(readSize == totalSize);

    free(buffer);
    hdfsCloseFile(fs, readFile);
    hdfsDisconnect(fs);

    return 0;
  }

  int get_hosts(const char *fileName) {
    hdfsFS fs;
    hdfsFile readFile;
    char *buffer;
    tSize curSize;

    fs = hdfsConnect("default", 0);
    CHECK(fs);
    if (!fs) {
      fprintf(stderr, "Oops! Failed to connect to hdfs!\n");
      exit(-1);
    }

    char ***hosts = hdfsGetHosts(fs, fileName, 0, 1);
    int i, j;
    CHECK(hosts);
    if (hosts) {
      i = 0;
      while (hosts[i]) {
        j = 0;
        while (hosts[i][j]) {
          fprintf(stdout, "\thosts[%d][%d] - %s\n", i, j, hosts[i][j]);
          ++j;
        }
        ++i;
      }
    } else {
      fprintf(stdout, "waah! hdfsGetHosts - FAILED!\n");
    }

    hdfsFreeHosts(hosts);
    hdfsDisconnect(fs);

    return 0;
  }
};

TEST_CASE_METHOD(LibHDFSFx, "Test hdfs integration") {
  totalSize = 500000000;
  check_hdfs_write("testFile_rw1", totalSize, 4096);
  check_hdfs_read("testFile_rw1", 4096);

  check_hdfs_write("testFile_rw2", totalSize, 1048576);
  check_hdfs_read("testFile_rw2", 1048576);

  check_hdfs_write("testFile_rw3", totalSize, 10485760);
  check_hdfs_read("testFile_rw3", 10485760);

  check_hdfs_write("testFile_rw4", totalSize, 104857600);
  check_hdfs_read("testFile_rw4", 104857600);

  get_hosts("testFile_rw1");
}
