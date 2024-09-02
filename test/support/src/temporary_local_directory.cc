/**
 * @file   temporary_local_directory.cc
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
 * This file defines class TemporaryLocalDirectory.
 */

#include "temporary_local_directory.h"
#include "tiledb/common/random/prng.h"

#include <filesystem>
#include <system_error>

using namespace tiledb::common;

namespace tiledb::sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

TemporaryLocalDirectory::TemporaryLocalDirectory(std::string prefix) {
  // Generate random number using global PRNG
  PRNG& prng = PRNG::get();
  auto rand = prng();

  // Generate random local path
  path_ = (std::filesystem::temp_directory_path() /
           (prefix + std::to_string(rand)) / "")
              .string();

  // Create a unique directory
  std::filesystem::create_directory(path_);
}

TemporaryLocalDirectory::~TemporaryLocalDirectory() {
  // Remove the unique directory
  std::error_code ec;
  auto removed = std::filesystem::remove_all(path_, ec);
  if (removed == static_cast<std::uintmax_t>(-1)) {
    LOG_ERROR(ec.message());
  }
}

/* ********************************* */
/*                API                */
/* ********************************* */

const std::string& TemporaryLocalDirectory::path() {
  return path_;
}

}  // namespace tiledb::sm
