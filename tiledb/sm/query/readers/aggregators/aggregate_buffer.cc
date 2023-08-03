/**
 * @file aggregate_buffer.cc
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
 * This file implements class AggregateBuffer.
 */

#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/result_tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

AggregateBuffer::AggregateBuffer(
    const std::string name,
    const bool var_sized,
    const bool nullable,
    const uint64_t min_cell,
    const uint64_t max_cell,
    const uint64_t cell_num,
    ResultTile& rt)
    : includes_last_var_cell_(var_sized && max_cell == cell_num)
    , min_cell_(min_cell)
    , max_cell_(max_cell)
    , fixed_data_(
          name == constants::count_of_rows ?
              nullptr :
              rt.tile_tuple(name)->fixed_tile().data())
    , var_data_(
          var_sized ? std::make_optional(
                          rt.tile_tuple(name)->var_tile().data_as<char>()) :
                      nullopt)
    , var_data_size_(
          includes_last_var_cell_ ? rt.tile_tuple(name)->var_tile().size() : 0)
    , validity_data_(
          nullable ?
              std::make_optional(
                  rt.tile_tuple(name)->validity_tile().data_as<uint8_t>()) :
              nullopt)
    , count_bitmap_(false)
    , bitmap_data_(nullopt) {
}

AggregateBuffer::AggregateBuffer(
    const std::string name,
    const bool var_sized,
    const bool nullable,
    const bool count_bitmap,
    const uint64_t min_cell,
    const uint64_t max_cell,
    const uint64_t cell_num,
    ResultTile& rt,
    void* bitmap_data)
    : includes_last_var_cell_(var_sized && max_cell == cell_num)
    , min_cell_(min_cell)
    , max_cell_(max_cell)
    , fixed_data_(
          name == constants::count_of_rows ?
              nullptr :
              rt.tile_tuple(name)->fixed_tile().data())
    , var_data_(
          var_sized ? std::make_optional(
                          rt.tile_tuple(name)->var_tile().data_as<char>()) :
                      nullopt)
    , var_data_size_(
          includes_last_var_cell_ ? rt.tile_tuple(name)->var_tile().size() : 0)
    , validity_data_(
          nullable ?
              std::make_optional(
                  rt.tile_tuple(name)->validity_tile().data_as<uint8_t>()) :
              nullopt)
    , count_bitmap_(count_bitmap)
    , bitmap_data_(
          bitmap_data != nullptr ? std::make_optional(bitmap_data) : nullopt) {
}

AggregateBuffer::AggregateBuffer(
    const bool var_sized,
    const bool nullable,
    const bool count_bitmap,
    const uint64_t cell_size,
    const uint64_t min_cell,
    const uint64_t max_cell,
    const uint64_t cell_num,
    ResultTile::TileTuple& tile_tuple,
    void* bitmap_data)
    : includes_last_var_cell_(var_sized && max_cell == cell_num)
    , min_cell_(0)
    , max_cell_(max_cell - min_cell)
    , fixed_data_(
          tile_tuple.fixed_tile().data_as<char>() + min_cell * cell_size)
    , var_data_(
          var_sized ?
              std::make_optional(tile_tuple.var_tile().data_as<char>()) :
              nullopt)
    , var_data_size_(includes_last_var_cell_ ? tile_tuple.var_tile().size() : 0)
    , validity_data_(
          nullable ?
              std::make_optional(
                  tile_tuple.validity_tile().data_as<uint8_t>() + min_cell) :
              nullopt)
    , count_bitmap_(count_bitmap)
    , bitmap_data_(
          bitmap_data != nullptr ? std::make_optional(bitmap_data) : nullopt) {
}

}  // namespace sm
}  // namespace tiledb
