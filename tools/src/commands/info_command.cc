/**
 * @file  info_command.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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
 * This file defines the info command.
 */

#include "commands/info_command.h"
#include "misc/common.h"

#include "tiledb/sm/storage_manager/storage_manager.h"

namespace tiledb {
namespace cli {

using namespace tiledb::sm;

clipp::group InfoCommand::get_cli() {
  using namespace clipp;
  auto cli =
      group{((option("--array").required(true) & value("uri", array_uri_)) %
             "URI of TileDB array"),
            (option("--tile-sizes").set(type_, InfoType::TileSizes) %
             "Print statistics about tile sizes in the array.")};
  return cli;
}

void InfoCommand::run() {
  switch (type_) {
    case InfoType::None:
      break;
    case InfoType::TileSizes:
      print_tile_sizes();
      break;
  }
}

void InfoCommand::print_tile_sizes() const {
  StorageManager sm;
  THROW_NOT_OK(sm.init(nullptr));

  // Open the array
  URI uri(array_uri_);
  Array array(uri, &sm);
  THROW_NOT_OK(
      array.open(QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0));

  // Compute and report mean persisted tile sizes over all attributes.
  const auto* schema = array.array_schema();
  auto fragment_metadata = array.fragment_metadata();
  auto attributes = schema->attributes();
  std::cout << "Array URI: " << uri.to_string() << std::endl;
  std::cout << "Mean persisted tile sizes: " << std::endl;
  for (const auto& attr : attributes) {
    uint64_t persisted_tile_size = 0;
    uint64_t num_tiles = 0;
    for (const auto& f : fragment_metadata) {
      uint64_t tile_num = f->tile_num();
      num_tiles += tile_num;
      for (uint64_t tile_idx = 0; tile_idx < tile_num; tile_idx++) {
        persisted_tile_size += f->persisted_tile_size(attr->name(), tile_idx);
        if (attr->var_size())
          persisted_tile_size +=
              f->persisted_tile_var_size(attr->name(), tile_idx);
      }
    }

    double mean_persisted_tile_size = persisted_tile_size / double(num_tiles);
    std::cout << "  - " << attr->name() << ": " << mean_persisted_tile_size
              << " bytes." << std::endl;
  }

  // Close the array.
  THROW_NOT_OK(array.close());
}

}  // namespace cli
}  // namespace tiledb