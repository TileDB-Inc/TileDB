/**
 * @file   sift_db.h
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
 */

#ifndef TDB_SIFT_DB_H
#define TDB_SIFT_DB_H

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cassert>
#include <filesystem>
#include <span>
#include <string>
#include <vector>

/**
 * See http://corpus-texmex.irisa.fr for file format
 */

template <class T>
class sift_db : public std::vector<std::span<T>> {
  using Base = std::vector<std::span<T>>;

  std::vector<T> data_;

 public:
  sift_db(const std::string& bin_file, size_t subset = 0) {
    if (!std::filesystem::exists(bin_file)) {
      throw std::runtime_error("file " + bin_file + " does not exist");
    }
    auto file_size = std::filesystem::file_size(bin_file);

    auto fd = open(bin_file.c_str(), O_RDONLY);
    if (fd == -1) {
      throw std::runtime_error("could not open " + bin_file);
    }

    uint32_t dimension{0};
    auto num_read = read(fd, &dimension, 4);
    lseek(fd, 0, SEEK_SET);

    auto max_vectors = file_size / (4 + dimension * sizeof(T));
    if (subset > max_vectors) {
      throw std::runtime_error(
          "specified subset is too large " + std::to_string(subset) + " > " +
          std::to_string(max_vectors));
    }

    auto num_vectors = subset == 0 ? max_vectors : subset;

    struct stat s;
    fstat(fd, &s);
    size_t mapped_size = s.st_size;
    assert(s.st_size == file_size);

    T* mapped_ptr = reinterpret_cast<T*>(
        mmap(0, mapped_size, PROT_READ, MAP_FILE | MAP_PRIVATE, fd, 0));
    if ((long)mapped_ptr == -1) {
      throw std::runtime_error("mmap failed");
    }

    // @todo use unique_ptr for overwrite
    data_.resize(num_vectors * dimension);

    auto data_ptr = data_.data();
    auto sift_ptr = mapped_ptr;

    // Perform strided read
    for (size_t k = 0; k < num_vectors; ++k) {
      // Check for consistency of dimensions
      decltype(dimension) dim = *reinterpret_cast<int*>(sift_ptr++);
      if (dim != dimension) {
        throw std::runtime_error(
            "dimension mismatch: " + std::to_string(dim) +
            " != " + std::to_string(dimension));
      }
      std::copy(sift_ptr, sift_ptr + dimension, data_ptr);
      this->emplace_back(std::span<T>{data_ptr, dimension});
      data_ptr += dimension;
      sift_ptr += dimension;
    }

    munmap(mapped_ptr, mapped_size);
    close(fd);
  }
};

#endif  // TDB_SIFT_DB_H
