/**
 * @file   dense_tiler.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2024 TileDB, Inc.
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
 * This file implements class DenseTiler.
 */

#include "tiledb/sm/query/writers/dense_tiler.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/rectangle.h"
#include "tiledb/sm/tile/writer_tile_tuple.h"

using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb::sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

template <class T>
DenseTiler<T>::DenseTiler(
    shared_ptr<MemoryTracker> memory_tracker,
    const std::unordered_map<std::string, QueryBuffer>* buffers,
    const Subarray* subarray,
    Stats* const parent_stats,
    const std::string& offsets_format_mode,
    uint64_t offsets_bitsize,
    bool offsets_extra_element)
    : memory_tracker_(memory_tracker)
    , stats_(parent_stats->create_child("DenseTiler"))
    , array_schema_(subarray->array()->array_schema_latest())
    , buffers_(buffers)
    , subarray_(subarray)
    , offsets_format_mode_(offsets_format_mode)
    , offsets_bytesize_(offsets_bitsize / 8)
    , offsets_extra_element_(offsets_extra_element) {
  // Assertions
  assert(subarray != nullptr);
  assert(buffers != nullptr);
  for (const auto& buff : *buffers) {
    assert(array_schema_.is_attr(buff.first));
    (void)buff;
  }

  // Initializations
  calculate_tile_num();
  calculate_subarray_tile_coord_strides();
  calculate_first_sub_tile_coords();
  calculate_tile_and_subarray_strides();
}

template <class T>
DenseTiler<T>::~DenseTiler() {
}

/* ****************************** */
/*               API              */
/* ****************************** */

template <class T>
const typename DenseTiler<T>::CopyPlan DenseTiler<T>::copy_plan(
    uint64_t id) const {
  assert(id < tile_num_);

  // For easy reference
  CopyPlan ret;
  auto dim_num = (int32_t)array_schema_.dim_num();
  auto& domain{array_schema_.domain()};
  auto subarray = subarray_->ndrange(0);  // Guaranteed to be unary
  std::vector<std::array<T, 2>> sub(dim_num);
  for (int32_t d = 0; d < dim_num; ++d)
    sub[d] = {
        *(const T*)subarray[d].start_fixed(),
        *(const T*)subarray[d].end_fixed()};
  auto tile_layout = array_schema_.cell_order();
  auto sub_layout = subarray_->layout();

  // Copy tile and subarray strides
  ret.tile_strides_el_ = tile_strides_el_;
  ret.sub_strides_el_ = sub_strides_el_;

  // Focus on the input tile
  auto tile_sub = this->tile_subarray(id);
  auto sub_in_tile = rectangle::intersection<T>(sub, tile_sub);

  // Compute the starting element to copy from in the subarray, and
  // to copy to in the tile
  ret.sub_start_el_ = 0;
  ret.tile_start_el_ = 0;
  for (int32_t d = 0; d < dim_num; ++d) {
    ret.sub_start_el_ += (sub_in_tile[d][0] - sub[d][0]) * sub_strides_el_[d];
    ret.tile_start_el_ +=
        (sub_in_tile[d][0] - tile_sub[d][0]) * tile_strides_el_[d];
  }

  // Calculate the copy elements per iteration, as well as the
  // dimension ranges to focus on
  if (dim_num == 1) {  // Special case, copy the entire subarray 1D range
    ret.dim_ranges_.push_back({0, 0});
    ret.copy_el_ = sub_in_tile[0][1] - sub_in_tile[0][0] + 1;
    ret.first_d_ = 0;
  } else if (sub_layout != tile_layout) {
    ret.copy_el_ = 1;
    ret.first_d_ = 0;
    for (int32_t d = 0; d < dim_num; ++d) {
      ret.dim_ranges_.push_back(
          {(uint64_t)0, uint64_t(sub_in_tile[d][1] - sub_in_tile[d][0])});
    }
  } else {  // dim_num > 1 && same layout of tile and subarray cells
    if (tile_layout == Layout::ROW_MAJOR) {
      ret.copy_el_ =
          sub_in_tile[dim_num - 1][1] - sub_in_tile[dim_num - 1][0] + 1;
      int32_t last_d = dim_num - 2;
      for (; last_d >= 0; --last_d) {
        auto tile_extent = *(const T*)domain.tile_extent(last_d + 1).data();
        if (sub_in_tile[last_d + 1][1] - sub_in_tile[last_d + 1][0] + 1 ==
                tile_extent &&
            sub_in_tile[last_d + 1][0] == sub[last_d + 1][0] &&
            sub_in_tile[last_d + 1][1] == sub[last_d + 1][1])
          ret.copy_el_ *= sub_in_tile[last_d][1] - sub_in_tile[last_d][0] + 1;
        else
          break;
      }
      if (last_d < 0) {
        ret.dim_ranges_.push_back({0, 0});
      } else {
        for (int32_t d = 0; d <= last_d; ++d)
          ret.dim_ranges_.push_back(
              {0, (uint64_t)(sub_in_tile[d][1] - sub_in_tile[d][0])});
      }
      ret.first_d_ = 0;
    } else {  // COL_MAJOR
      ret.copy_el_ = sub_in_tile[0][1] - sub_in_tile[0][0] + 1;
      int32_t last_d = 1;
      for (; last_d < dim_num; ++last_d) {
        auto tile_extent = *(const T*)domain.tile_extent(last_d - 1).data();
        if (sub_in_tile[last_d - 1][1] - sub_in_tile[last_d - 1][0] + 1 ==
                tile_extent &&
            sub_in_tile[last_d - 1][0] == sub[last_d - 1][0] &&
            sub_in_tile[last_d - 1][1] == sub[last_d - 1][1])
          ret.copy_el_ *= sub_in_tile[last_d][1] - sub_in_tile[last_d][0] + 1;
        else
          break;
      }
      if (last_d == dim_num) {
        ret.dim_ranges_.push_back({0, 0});
        ret.first_d_ = dim_num - 1;
      } else {
        for (int32_t d = last_d; d < dim_num; ++d)
          ret.dim_ranges_.push_back(
              {0, (uint64_t)(sub_in_tile[d][1] - sub_in_tile[d][0])});
        ret.first_d_ = last_d;
      }
    }
  }

  return ret;
}

template <class T>
Status DenseTiler<T>::get_tile(
    uint64_t id, const std::string& name, WriterTileTuple& tile) {
  auto timer_se = stats_->start_timer("get_tile");

  // Checks
  if (id >= tile_num_)
    return LOG_STATUS(
        Status_DenseTilerError("Cannot get tile; Invalid tile id"));
  if (!array_schema_.is_attr(name))
    return LOG_STATUS(Status_DenseTilerError(
        std::string("Cannot get tile; '") + name + "' is not an attribute"));

  auto& domain{array_schema_.domain()};
  auto cell_num_in_tile = domain.cell_num_per_tile();

  // For easy reference
  if (tile.var_size()) {
    auto type = array_schema_.type(name);
    auto tile_off_size = constants::cell_var_offset_size * cell_num_in_tile;
    auto buff_it = buffers_->find(name);
    assert(buff_it != buffers_->end());
    auto buff_var = (uint8_t*)buff_it->second.buffer_var_;
    auto div = (offsets_bytesize_ == 8) ? 1 : 2;
    uint64_t buff_off_size = *(buff_it->second.buffer_size_) / div;
    uint64_t buff_var_size = *(buff_it->second.buffer_var_size_);
    auto cell_size = datatype_size(type);
    std::vector<uint8_t> fill_var(sizeof(uint64_t), 0);

    // Initialize position tile
    WriterTile tile_pos(
        constants::format_version,
        constants::cell_var_offset_type,
        constants::cell_var_offset_size,
        tile_off_size,
        memory_tracker_);

    // Fill entire tile with MAX_UINT64
    std::vector<offsets_t> to_write(
        cell_num_in_tile, std::numeric_limits<offsets_t>::max());
    tile_pos.write(to_write.data(), 0, tile_off_size);
    to_write.clear();
    to_write.shrink_to_fit();

    // Get position tile
    auto cell_num_in_buff =  // TODO: fix
        (buff_off_size - (offsets_extra_element_ * offsets_bytesize_)) /
        offsets_bytesize_;
    std::vector<uint64_t> cell_pos(cell_num_in_buff);
    for (uint64_t i = 0; i < cell_num_in_buff; ++i)
      cell_pos[i] = i;
    RETURN_NOT_OK(copy_tile(
        id, constants::cell_var_offset_size, (uint8_t*)&cell_pos[0], tile_pos));

    // Copy real offsets and values to the corresponding tiles
    auto tile_pos_buff = tile_pos.data_as<offsets_t>();
    uint64_t tile_off_offset = 0, offset = 0, val_offset, val_size, pos;
    auto mul = (offsets_format_mode_ == "bytes") ? 1 : cell_size;
    auto& tile_off = tile.offset_tile();
    auto& tile_val = tile.var_tile();
    for (uint64_t i = 0; i < cell_num_in_tile; ++i) {
      pos = tile_pos_buff[i];
      tile_off.write(&offset, tile_off_offset, sizeof(offset));
      tile_off_offset += sizeof(offset);
      if (pos == std::numeric_limits<uint64_t>::max()) {  // Empty
        tile_val.write_var(&fill_var[0], offset, cell_size);
        offset += cell_size;
      } else {  // Non-empty
        val_offset = ((offsets_bytesize_ == 8) ?
                          ((uint64_t*)buff_it->second.buffer_)[pos] :
                          ((uint32_t*)buff_it->second.buffer_)[pos]) *
                     mul;
        val_size = (pos < cell_num_in_buff - 1) ?
                       (((offsets_bytesize_ == 8) ?
                             ((uint64_t*)buff_it->second.buffer_)[pos + 1] :
                             ((uint32_t*)buff_it->second.buffer_)[pos + 1]) *
                            mul -
                        val_offset) :
                       buff_var_size - val_offset;
        tile_val.write_var(&buff_var[val_offset], offset, val_size);
        offset += val_size;
      }
    }

    tile_val.set_size(offset);
  } else {
    auto cell_size = array_schema_.cell_size(name);
    auto tile_size = cell_size * cell_num_in_tile;
    auto buff = (uint8_t*)buffers_->find(name)->second.buffer_;
    assert(buff != nullptr);

    memset(tile.fixed_tile().data(), 0, tile_size);

    // Copy tile from buffer
    RETURN_NOT_OK(copy_tile(id, cell_size, buff, tile.fixed_tile()));
  }

  if (tile.nullable()) {
    auto cell_size = constants::cell_validity_size;
    auto tile_size = cell_size * cell_num_in_tile;
    auto buff =
        (uint8_t*)buffers_->find(name)->second.validity_vector_.buffer();
    assert(buff != nullptr);

    memset(tile.validity_tile().data(), 0, tile_size);

    // Copy tile from buffer
    RETURN_NOT_OK(copy_tile(id, cell_size, buff, tile.validity_tile()));
  }

  compute_tile_metadata(name, id, tile);

  return Status::Ok();
}

template <class T>
uint64_t DenseTiler<T>::tile_num() const {
  return tile_num_;
}

template <class T>
const std::vector<uint64_t>& DenseTiler<T>::tile_strides_el() const {
  return tile_strides_el_;
}

template <class T>
const std::vector<uint64_t>& DenseTiler<T>::sub_strides_el() const {
  return sub_strides_el_;
}

template <class T>
const std::vector<uint64_t>& DenseTiler<T>::sub_tile_coord_strides() const {
  return sub_tile_coord_strides_;
}

template <class T>
const std::vector<uint64_t>& DenseTiler<T>::first_sub_tile_coords() const {
  return first_sub_tile_coords_;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

template <class T>
void DenseTiler<T>::calculate_first_sub_tile_coords() {
  // For easy reference
  auto dim_num = array_schema_.dim_num();
  auto& domain{array_schema_.domain()};
  auto subarray = subarray_->ndrange(0);

  // Calculate the coordinates of the first tile in the entire
  // domain that intersects the subarray (essentially its upper
  // left cell)
  first_sub_tile_coords_.resize(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    T dom_start{*(const T*)domain.dimension_ptr(d)->domain().start_fixed()};
    T sub_start{*(const T*)subarray[d].start_fixed()};
    T tile_extent{*(const T*)domain.tile_extent(d).data()};
    first_sub_tile_coords_[d] =
        Dimension::tile_idx(sub_start, dom_start, tile_extent);
  }
}

template <class T>
void DenseTiler<T>::calculate_subarray_tile_coord_strides() {
  // For easy reference
  auto dim_num = (int32_t)array_schema_.dim_num();
  auto& domain{array_schema_.domain()};
  auto subarray = subarray_->ndrange(0);
  auto layout = array_schema_.tile_order();

  // Compute offsets
  sub_tile_coord_strides_.reserve(dim_num);
  if (layout == Layout::ROW_MAJOR) {
    sub_tile_coord_strides_.push_back(1);
    for (auto d = dim_num - 2; d >= 0; --d) {
      auto tile_num{domain.dimension_ptr(d + 1)->tile_num(subarray[d + 1])};
      sub_tile_coord_strides_.push_back(
          sub_tile_coord_strides_.back() * tile_num);
    }
    std::reverse(
        sub_tile_coord_strides_.begin(), sub_tile_coord_strides_.end());
  } else {  // COL_MAJOR
    sub_tile_coord_strides_.push_back(1);
    for (int32_t d = 1; d < dim_num; ++d) {
      auto tile_num{domain.dimension_ptr(d - 1)->tile_num(subarray[d - 1])};
      sub_tile_coord_strides_.push_back(
          sub_tile_coord_strides_.back() * tile_num);
    }
  }
}

template <class T>
void DenseTiler<T>::calculate_tile_and_subarray_strides() {
  // For easy reference
  auto sub_layout = subarray_->layout();
  assert(sub_layout == Layout::ROW_MAJOR || sub_layout == Layout::COL_MAJOR);
  auto tile_layout = array_schema_.cell_order();
  auto dim_num = (int32_t)array_schema_.dim_num();
  auto& domain{array_schema_.domain()};
  auto subarray = subarray_->ndrange(0);

  // Compute tile strides
  tile_strides_el_.resize(dim_num);
  if (tile_layout == Layout::ROW_MAJOR) {
    tile_strides_el_[dim_num - 1] = 1;
    if (dim_num > 1) {
      for (auto d = dim_num - 2; d >= 0; --d) {
        auto tile_extent = (const T*)(domain.tile_extent(d + 1).data());
        assert(tile_extent != nullptr);
        tile_strides_el_[d] = Dimension::tile_extent_mult<T>(
            tile_strides_el_[d + 1], *tile_extent);
      }
    }
  } else {  // COL_MAJOR
    tile_strides_el_[0] = 1;
    if (dim_num > 1) {
      for (auto d = 1; d < dim_num; ++d) {
        auto tile_extent = (const T*)(domain.tile_extent(d - 1).data());
        assert(tile_extent != nullptr);
        tile_strides_el_[d] = Dimension::tile_extent_mult<T>(
            tile_strides_el_[d - 1], *tile_extent);
      }
    }
  }

  // Compute subarray slides
  sub_strides_el_.resize(dim_num);
  if (sub_layout == Layout::ROW_MAJOR) {
    sub_strides_el_[dim_num - 1] = 1;
    if (dim_num > 1) {
      for (auto d = dim_num - 2; d >= 0; --d) {
        auto sub_range_start = *(const T*)subarray[d + 1].start_fixed();
        auto sub_range_end = *(const T*)subarray[d + 1].end_fixed();
        auto sub_extent = sub_range_end - sub_range_start + 1;
        sub_strides_el_[d] = sub_strides_el_[d + 1] * sub_extent;
      }
    }
  } else {  // COL_MAJOR
    sub_strides_el_[0] = 1;
    if (dim_num > 1) {
      for (auto d = 1; d < dim_num; ++d) {
        auto sub_range_start = *(const T*)subarray[d - 1].start_fixed();
        auto sub_range_end = *(const T*)subarray[d - 1].end_fixed();
        auto sub_extent = sub_range_end - sub_range_start + 1;
        sub_strides_el_[d] = sub_strides_el_[d - 1] * sub_extent;
      }
    }
  }
}

template <class T>
void DenseTiler<T>::calculate_tile_num() {
  tile_num_ = array_schema_.domain().tile_num(subarray_->ndrange(0));
}

template <class T>
std::vector<uint64_t> DenseTiler<T>::tile_coords_in_sub(uint64_t id) const {
  // For easy reference
  auto dim_num = (int32_t)array_schema_.dim_num();
  auto layout = array_schema_.tile_order();
  std::vector<uint64_t> ret;
  auto tmp_idx = id;

  if (layout == Layout::ROW_MAJOR) {
    for (int32_t d = 0; d < dim_num; ++d) {
      ret.push_back(tmp_idx / sub_tile_coord_strides_[d]);
      tmp_idx %= sub_tile_coord_strides_[d];
    }
  } else {  // COL_MAJOR
    for (auto d = dim_num - 1; d >= 0; --d) {
      ret.push_back(tmp_idx / sub_tile_coord_strides_[d]);
      tmp_idx %= sub_tile_coord_strides_[d];
    }
    std::reverse(ret.begin(), ret.end());
  }

  return ret;
}

template <class T>
std::vector<std::array<T, 2>> DenseTiler<T>::tile_subarray(uint64_t id) const {
  // For easy reference
  auto dim_num = array_schema_.dim_num();
  auto& domain{array_schema_.domain()};

  // Get tile coordinates in the subarray tile domain
  auto tile_coords_in_sub = this->tile_coords_in_sub(id);

  // Get the tile coordinates in the array tile domain
  std::vector<uint64_t> tile_coords_in_dom(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    tile_coords_in_dom[d] = tile_coords_in_sub[d] + first_sub_tile_coords_[d];
  }

  // Calculate tile subarray based on the tile coordinates in the domain
  std::vector<std::array<T, 2>> ret(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dom_start{*(const T*)domain.dimension_ptr(d)->domain().start_fixed()};
    auto tile_extent = *(const T*)domain.tile_extent(d).data();
    ret[d][0] = Dimension::tile_coord_low(
        tile_coords_in_dom[d], dom_start, tile_extent);
    ret[d][1] = Dimension::tile_coord_high(
        tile_coords_in_dom[d], dom_start, tile_extent);
  }

  return ret;
}

template <class T>
Status DenseTiler<T>::copy_tile(
    uint64_t id, uint64_t cell_size, uint8_t* buff, WriterTile& tile) const {
  // Calculate copy plan
  const CopyPlan copy_plan = this->copy_plan(id);

  // For easy reference
  auto sub_offset = copy_plan.sub_start_el_ * cell_size;
  auto tile_offset = copy_plan.tile_start_el_ * cell_size;
  auto copy_nbytes = copy_plan.copy_el_ * cell_size;
  auto sub_strides_nbytes = copy_plan.sub_strides_el_;
  for (auto& bsn : sub_strides_nbytes) {
    bsn *= cell_size;
  }
  auto tile_strides_nbytes = copy_plan.tile_strides_el_;
  for (auto& tsn : tile_strides_nbytes) {
    tsn *= cell_size;
  }
  const auto& dim_ranges = copy_plan.dim_ranges_;
  auto first_d = copy_plan.first_d_;
  auto dim_num = (int64_t)dim_ranges.size();
  assert(dim_num > 0);

  // Auxiliary information needed in the copy loop
  std::vector<uint64_t> tile_offsets(dim_num);
  for (int64_t i = 0; i < dim_num; ++i) {
    tile_offsets[i] = tile_offset;
  }
  std::vector<uint64_t> sub_offsets(dim_num);
  for (int64_t i = 0; i < dim_num; ++i) {
    sub_offsets[i] = sub_offset;
  }
  std::vector<uint64_t> cell_coords(dim_num);
  for (int64_t i = 0; i < dim_num; ++i) {
    cell_coords[i] = dim_ranges[i][0];
  }

  // Perform the tile copy (always in row-major order)
  auto d = dim_num - 1;
  while (true) {
    // Copy a slab
    tile.write(&buff[sub_offsets[d]], tile_offsets[d], copy_nbytes);

    // Advance cell coordinates, tile and buffer offsets
    auto last_dim_changed = d;
    for (; last_dim_changed >= 0; --last_dim_changed) {
      ++cell_coords[last_dim_changed];
      if (cell_coords[last_dim_changed] > dim_ranges[last_dim_changed][1]) {
        cell_coords[last_dim_changed] = dim_ranges[last_dim_changed][0];
      } else {
        break;
      }
    }

    // Check if copy loop is done
    if (last_dim_changed < 0) {
      break;
    }

    // Update the offsets
    tile_offsets[last_dim_changed] +=
        tile_strides_nbytes[last_dim_changed + first_d];
    sub_offsets[last_dim_changed] +=
        sub_strides_nbytes[last_dim_changed + first_d];
    for (auto i = last_dim_changed + 1; i < dim_num; ++i) {
      tile_offsets[i] = tile_offsets[i - 1];
      sub_offsets[i] = sub_offsets[i - 1];
    }
  }

  return Status::Ok();
}

template <class T>
void DenseTiler<T>::compute_tile_metadata(
    const std::string& name, uint64_t id, WriterTileTuple& tile) const {
  // Calculate copy plan
  const CopyPlan copy_plan = this->copy_plan(id);

  const auto type = array_schema_.type(name);
  const auto is_dim = array_schema_.is_dim(name);
  const bool var = array_schema_.var_size(name);
  const auto cell_size = array_schema_.cell_size(name);
  const auto cell_val_num = array_schema_.cell_val_num(name);
  TileMetadataGenerator md_generator(
      type, is_dim, var, cell_size, cell_val_num);

  // For easy reference
  auto tile_offset = copy_plan.tile_start_el_;
  auto sub_strides_nbytes = copy_plan.sub_strides_el_;
  auto tile_strides_nbytes = copy_plan.tile_strides_el_;
  const auto& dim_ranges = copy_plan.dim_ranges_;
  auto first_d = copy_plan.first_d_;
  auto dim_num = (int64_t)dim_ranges.size();
  assert(dim_num > 0);

  // Auxiliary information needed in the copy loop
  std::vector<uint64_t> tile_offsets(dim_num);
  for (int64_t i = 0; i < dim_num; ++i) {
    tile_offsets[i] = tile_offset;
  }
  std::vector<uint64_t> cell_coords(dim_num);
  for (int64_t i = 0; i < dim_num; ++i) {
    cell_coords[i] = dim_ranges[i][0];
  }

  // Perform the tile copy (always in row-major order)
  auto d = dim_num - 1;
  while (true) {
    // Copy a slab
    md_generator.process_cell_slab(
        tile, tile_offsets[d], tile_offsets[d] + copy_plan.copy_el_);

    // Advance cell coordinates, tile and buffer offsets
    auto last_dim_changed = d;
    for (; last_dim_changed >= 0; --last_dim_changed) {
      ++cell_coords[last_dim_changed];
      if (cell_coords[last_dim_changed] > dim_ranges[last_dim_changed][1]) {
        cell_coords[last_dim_changed] = dim_ranges[last_dim_changed][0];
      } else {
        break;
      }
    }

    // Check if copy loop is done
    if (last_dim_changed < 0) {
      break;
    }

    // Update the offsets
    tile_offsets[last_dim_changed] +=
        tile_strides_nbytes[last_dim_changed + first_d];
    for (auto i = last_dim_changed + 1; i < dim_num; ++i) {
      tile_offsets[i] = tile_offsets[i - 1];
    }
  }

  md_generator.set_tile_metadata(tile);
}

// Explicit template instantiations
template class DenseTiler<int8_t>;
template class DenseTiler<uint8_t>;
template class DenseTiler<int16_t>;
template class DenseTiler<uint16_t>;
template class DenseTiler<int32_t>;
template class DenseTiler<uint32_t>;
template class DenseTiler<int64_t>;
template class DenseTiler<uint64_t>;

}  // namespace tiledb::sm
