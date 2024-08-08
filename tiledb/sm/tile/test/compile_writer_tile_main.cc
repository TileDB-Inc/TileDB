/**
 * @file compile_writer_tile_main.cc
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

#include "../tile_metadata_generator.h"
#include "../writer_tile_tuple.h"

#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/enums/array_type.h"

using namespace tiledb::sm;

int main() {
  MemoryTrackerManager mem;
  auto mem_tracker = mem.create_tracker();
  ArraySchema schema(ArrayType::DENSE, mem_tracker);

  WriterTileTuple writer(
      schema, 0, false, false, 0, Datatype::ANY, mem_tracker);
  (void)writer.var_size();

  TileMetadataGenerator generator(Datatype::ANY, false, false, 0, 0);
  (void)generator.process_full_tile(writer);

  return 0;
}
