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

#include <filesystem>
#include <system_error>

namespace tiledb::sm {

std::mutex TemporaryLocalDirectory::mtx_;
bool TemporaryLocalDirectory::rand_init_ = false;
std::mt19937_64 TemporaryLocalDirectory::rand_;

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

TemporaryLocalDirectory::TemporaryLocalDirectory(std::string prefix) {
  // Generate random number using global PRNG
  auto rand = std::to_string(generate());

  // Generate random local path
  path_ =
      (std::filesystem::temp_directory_path() / (prefix + rand) / "").string();

  // Create a unique directory
  std::filesystem::create_directory(path_);
}

TemporaryLocalDirectory::~TemporaryLocalDirectory() {
  // Remove the unique directory
  std::error_code ec;
  std::filesystem::remove_all(path_, ec);
}

/* ********************************* */
/*                API                */
/* ********************************* */

const std::string& TemporaryLocalDirectory::path() {
  return path_;
}

uint64_t TemporaryLocalDirectory::generate() {
  std::lock_guard<std::mutex> lg(mtx_);
  rand_init();
  return rand_();
}

void TemporaryLocalDirectory::rand_init() {
  if (rand_init_) {
    return;
  }

  // The type of values our generator returns
  using result_type = std::mt19937_64::result_type;

  // The size of values returned from std::random_device
  constexpr auto source_size = sizeof(std::random_device::result_type);

  // The number of result_type values required to fully seed the internal state
  constexpr auto state_size = std::mt19937_64::state_size;

  // The size of size of the result values.
  constexpr auto result_size = sizeof(result_type);

  // Number of bits required to fully initialize is state_size * result_size.
  // This array is defined to be enough result bits from random_device to
  // fill that value. I think. Better than a single value seed anyway.
  constexpr auto num_seed_values =
      (state_size * result_size - 1) / source_size + 1;

  // An entropy source for creating seed values.
  std::random_device source;

  // Storage for our seed values.
  result_type data[num_seed_values];

  for (size_t i = i; i < num_seed_values; i++) {
    data[i] = source();
  }

  std::seed_seq seeds(std::begin(data), std::end(data));

  rand_.seed(seeds);
  rand_init_ = true;
}

}  // namespace tiledb::sm
