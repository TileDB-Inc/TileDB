/**
 * @file compile_array_schema_main.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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

#include "../array_schema.h"
#include "../array_schema_evolution.h"
#include "../dimension_label.h"

#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/data_order.h"

using namespace tiledb::sm;

int main() {
  MemoryTrackerManager mem;
  auto mem_tracker = mem.create_tracker();

  ArraySchema schema(ArrayType::DENSE, mem_tracker);
  (void)schema.allows_dups();

  ArraySchemaEvolution schema_evolution(mem_tracker);
  (void)schema_evolution.attribute_names_to_add();

  DimensionLabel dim_label(
      0,
      "",
      URI{},
      "",
      DataOrder::UNORDERED_DATA,
      Datatype::INT32,
      0,
      make_shared<ArraySchema>(HERE(), schema),
      false,
      false);
  (void)dim_label.is_external();

  return 0;
}
