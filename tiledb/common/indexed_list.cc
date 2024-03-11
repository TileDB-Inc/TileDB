/**
 * @file   indexed_list.cc
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
 *
 * @section DESCRIPTION
 *
 * This file implements specializations for the `IndexedList` class.
 */

#include "indexed_list.h"
#include "memory_tracker.h"
#include "tiledb/sm/query/readers/result_tile.h"
#include "tiledb/sm/tile/writer_tile_tuple.h"

template <>
IndexedList<tiledb::sm::WriterTileTuple>::IndexedList(
    shared_ptr<tiledb::sm::MemoryTracker> memory_tracker)
    : memory_tracker_(memory_tracker)
    , list_(memory_tracker->get_resource(sm::MemoryType::TILE_WRITER_DATA)) {
}

template <>
IndexedList<tiledb::common::IndexedList<tiledb::sm::WriterTileTuple>>::
    IndexedList(shared_ptr<tiledb::sm::MemoryTracker> memory_tracker)
    : memory_tracker_(memory_tracker)
    , list_(memory_tracker->get_resource(sm::MemoryType::TILE_WRITER_DATA)) {
}

template <>
IndexedList<tiledb::sm::ResultTile>::IndexedList(
    shared_ptr<tiledb::sm::MemoryTracker> memory_tracker)
    : memory_tracker_(memory_tracker)
    , list_(memory_tracker->get_resource(sm::MemoryType::TILE_DATA)) {
}
