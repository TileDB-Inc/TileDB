/**
 * @file   unique_directory.cc
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
 * This file defines class UniqueDirectory.
 */

#include "tiledb/sm/filesystem/unique_directory.h"
#include "tiledb/common/random/prng.h"

#include <filesystem>

using namespace tiledb::common;

namespace tiledb::sm {

class UniqueDirectoryException : public StatusException {
 public:
  explicit UniqueDirectoryException(const std::string& message)
      : StatusException("UniqueDirectory", message) {
  }
};

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

UniqueDirectory::UniqueDirectory(const VFS& vfs, std::string prefix)
    : vfs_(vfs) {
  // Generate random number using global PRNG
  PRNG& prng = PRNG::get();
  auto rand = prng();

  // Generate random path
  path_ = (std::filesystem::temp_directory_path() /
           (prefix.data() + std::to_string(rand)) / "")
              .string();

  // Create unique directory on the given VFS
  Status st = vfs_.create_dir(URI(path_));
  if (!st.ok()) {
    std::throw_with_nested(
        UniqueDirectoryException("Error creating unique directory. "));
  }
}

UniqueDirectory::~UniqueDirectory() {
  // Remove the unique directory from the VFS
  Status st = vfs_.remove_dir(URI(path_));
  if (!st.ok()) {
    std::throw_with_nested(
        UniqueDirectoryException("Error removing unique directory. "));
  }
}

/* ********************************* */
/*                API                */
/* ********************************* */

std::string UniqueDirectory::path() {
  return path_;
}

}  // namespace tiledb::sm
