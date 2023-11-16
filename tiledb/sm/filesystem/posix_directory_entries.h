/**
 * @file   posix_directory_entries.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file includes a wrapper class for the `scandir` API for posix.
 */

#ifndef TILEDB_POSIX_DIRECTORY_ENTRIES_H
#define TILEDB_POSIX_DIRECTORY_ENTRIES_H

#ifndef _WIN32

#include "tiledb/common/common.h"

#include <dirent.h>

using namespace tiledb::common;

namespace tiledb::sm {

/**
 * This is a wrapper for the scandir function on posix. It ensures that all the
 * memory allocated by the call is freed when the object goes out of scope.
 */
class PosixDirectoryEntries {
 public:
  /* ********************************* */
  /*            CONSTRUCTORS           */
  /* ********************************* */

  /**
   * @brief Construct a new Posix Directory Listing object.
   *
   * @param directory_path Path to use for listing.
   */
  PosixDirectoryEntries(const std::string directory_path) {
    // Make the call to scandir.
    dirent** directory_entries;
    int num_entries =
        scandir(directory_path.c_str(), &directory_entries, NULL, alphasort);

    if (num_entries < 0) {
      throw std::runtime_error(
          std::string("Cannot list files in directory '") + directory_path +
          "'; " + strerror(errno));
    }

    // Paths contains an array that needs to be freed, put it in a unique
    // pointer that will call std::free on destruction.
    directory_entries_.reset(directory_entries);

    // Reserve can throw, so we need to clean up if it does.
    try {
      directory_entries_pointers_.reserve(num_entries);
    } catch (...) {
      // Free all memory.
      for (int i = 0; i < num_entries; i++) {
        std::free(directory_entries[i]);
      }
      std::free(directory_entries);

      throw;
    }

    // Put every pointers in `paths` in it's own unique pointer that will call
    // std::free on desctuction of this object.
    for (int i = 0; i < num_entries; i++) {
      directory_entries_pointers_.emplace_back(directory_entries[i]);
    }
  }

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /** Get the directory entry at index `idx` */
  const dirent& operator[](size_t idx) const {
    return *directory_entries_pointers_[idx].get();
  }

  /** Get the size of the listing. */
  size_t size() {
    return directory_entries_pointers_.size();
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * Unique pointer that will free the directory entries array on destruction.
   */
  tiledb_unique_c_ptr<dirent*> directory_entries_;

  /** Vector that holds unique pointers to each directory entry. */
  std::vector<tiledb_unique_c_ptr<dirent>> directory_entries_pointers_;
};

}  // namespace tiledb::sm

#endif  // !_WIN32

#endif  // TILEDB_POSIX_DIRECTORY_ENTRIES_H
