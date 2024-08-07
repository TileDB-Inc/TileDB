/**
 * @file compile_fragment_metadata_main.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 */

#include "../fragment_info.h"
#include "../fragment_metadata.h"
#include "../loaded_fragment_metadata.h"
#include "../ondemand_fragment_metadata.h"
#include "../v1v2preloaded_fragment_metadata.h"

#include "tiledb/common/logger.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/storage_manager/context_resources.h"

using namespace tiledb::sm;

int main() {
  Config config;
  auto logger = make_shared<Logger>(HERE(), "foo");
  ContextResources resources(config, logger, 1, 1, "");

  FragmentInfo info(URI{}, resources);
  (void)info.fragment_num();

  FragmentMetadata meta(&resources, resources.ephemeral_memory_tracker(), 22);
  (void)meta.cell_num();

  LoadedFragmentMetadata* loaded = meta.loaded_metadata();
  (void)loaded->free_tile_offsets();

  EncryptionKey key;
  OndemandFragmentMetadata ondemand(meta, resources.ephemeral_memory_tracker());
  (void)ondemand.load_rtree(key);

  V1V2PreloadedFragmentMetadata v1v2(
      meta, resources.ephemeral_memory_tracker());
  (void)v1v2.load_rtree(key);

  return 0;
}
