/**
 * @file   dimension.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file implements class Dimension.
 */

#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/misc/utils.h"

#include <cassert>
#include <iostream>

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Dimension::Dimension()
    : Dimension("", Datatype::INT32) {
}

Dimension::Dimension(const std::string& name, Datatype type)
    : name_(name)
    , type_(type) {
  cell_val_num_ = (datatype_is_string(type)) ? constants::var_num : 1;
  set_ceil_to_tile_func();
  set_check_range_func();
  set_coincides_with_tiles_func();
  set_compute_mbr_func();
  set_crop_range_func();
  set_domain_range_func();
  set_expand_range_func();
  set_expand_range_v_func();
  set_expand_to_tile_func();
  set_oob_func();
  set_covered_func();
  set_overlap_func();
  set_overlap_ratio_func();
  set_split_range_func();
  set_splitting_value_func();
  set_tile_num_func();
  set_value_in_range_func();
}

Dimension::Dimension(const Dimension* dim) {
  assert(dim != nullptr);

  cell_val_num_ = dim->cell_val_num_;
  filters_ = dim->filters_;
  name_ = dim->name();
  type_ = dim->type_;

  // Set fuctions
  ceil_to_tile_func_ = dim->ceil_to_tile_func_;
  check_range_func_ = dim->check_range_func_;
  coincides_with_tiles_func_ = dim->coincides_with_tiles_func_;
  compute_mbr_func_ = dim->compute_mbr_func_;
  crop_range_func_ = dim->crop_range_func_;
  domain_range_func_ = dim->domain_range_func_;
  expand_range_v_func_ = dim->expand_range_v_func_;
  expand_range_func_ = dim->expand_range_func_;
  expand_to_tile_func_ = dim->expand_to_tile_func_;
  oob_func_ = dim->oob_func_;
  covered_func_ = dim->covered_func_;
  overlap_func_ = dim->overlap_func_;
  overlap_ratio_func_ = dim->overlap_ratio_func_;
  split_range_func_ = dim->split_range_func_;
  splitting_value_func_ = dim->splitting_value_func_;
  tile_num_func_ = dim->tile_num_func_;
  value_in_range_func_ = dim->value_in_range_func_;

  domain_ = dim->domain();
  tile_extent_ = dim->tile_extent();
}

/* ********************************* */
/*                API                */
/* ********************************* */

unsigned int Dimension::cell_val_num() const {
  return cell_val_num_;
}

Status Dimension::set_cell_val_num(unsigned int cell_val_num) {
  // Error checkls
  if (datatype_is_string(type_) && cell_val_num != constants::var_num)
    return LOG_STATUS(
        Status::DimensionError("Cannot set non-variable number of values per "
                               "coordinate for a string dimension"));
  if (!datatype_is_string(type_) && cell_val_num != 1)
    return LOG_STATUS(Status::DimensionError(
        "Cannot set number of values per coordinate; Currently only one value "
        "per coordinate is supported"));

  cell_val_num_ = cell_val_num;

  return Status::Ok();
}

uint64_t Dimension::coord_size() const {
  return datatype_size(type_);
}

std::string Dimension::coord_to_str(const QueryBuffer& buff, uint64_t i) const {
  std::stringstream ss;

  // Fixed sized
  if (!var_size()) {
    auto cbuff = (const unsigned char*)buff.buffer_;
    auto coord = cbuff + i * coord_size();

    switch (type_) {
      case Datatype::INT32:
        ss << *((int32_t*)coord);
        break;
      case Datatype::INT64:
        ss << *((int64_t*)coord);
        break;
      case Datatype::INT8:
        ss << *((int8_t*)coord);
        break;
      case Datatype::UINT8:
        ss << *((uint8_t*)coord);
        break;
      case Datatype::INT16:
        ss << *((int16_t*)coord);
        break;
      case Datatype::UINT16:
        ss << *((uint16_t*)coord);
        break;
      case Datatype::UINT32:
        ss << *((uint32_t*)coord);
        break;
      case Datatype::UINT64:
        ss << *((uint64_t*)coord);
        break;
      case Datatype::FLOAT32:
        ss << *((float*)coord);
        break;
      case Datatype::FLOAT64:
        ss << *((double*)coord);
        break;
      case Datatype::DATETIME_YEAR:
      case Datatype::DATETIME_MONTH:
      case Datatype::DATETIME_WEEK:
      case Datatype::DATETIME_DAY:
      case Datatype::DATETIME_HR:
      case Datatype::DATETIME_MIN:
      case Datatype::DATETIME_SEC:
      case Datatype::DATETIME_MS:
      case Datatype::DATETIME_US:
      case Datatype::DATETIME_NS:
      case Datatype::DATETIME_PS:
      case Datatype::DATETIME_FS:
      case Datatype::DATETIME_AS:
        ss << *((int64_t*)coord);
        break;
      default:
        break;
    }
  } else {  // Var-sized
    assert(type_ == Datatype::STRING_ASCII);
    auto offs = (const uint64_t*)buff.buffer_;
    auto offs_size = *(buff.buffer_size_);
    auto var_size = *(buff.buffer_var_size_);
    auto vars = (const char*)buff.buffer_var_;
    auto coord = &vars[offs[i]];
    auto next_off = ((i + 1) * constants::cell_var_offset_size == offs_size) ?
                        var_size :
                        offs[i + 1];
    auto coord_size = next_off - offs[i];
    auto coord_str = std::string(coord, coord_size);
    ss << coord_str;
  }

  return ss.str();
}

// ===== FORMAT =====
// dimension_name_size (uint32_t)
// dimension_name (string)
// type (uint8_t)
// cell_val_num (uint32_t)
// filter_pipeline (see FilterPipeline::serialize)
// domain_size (uint64_t)
// domain (void* - domain_size)
// null_tile_extent (uint8_t)
// tile_extent (void* - type_size)
Status Dimension::deserialize(
    ConstBuffer* buff, uint32_t version, Datatype type) {
  // Load dimension name
  uint32_t dimension_name_size;
  RETURN_NOT_OK(buff->read(&dimension_name_size, sizeof(uint32_t)));
  name_.resize(dimension_name_size);
  RETURN_NOT_OK(buff->read(&name_[0], dimension_name_size));

  // Applicable only to version >= 5
  if (version >= 5) {
    // Load type
    uint8_t type;
    RETURN_NOT_OK(buff->read(&type, sizeof(uint8_t)));
    type_ = (Datatype)type;

    // Load cell_val_num_
    RETURN_NOT_OK(buff->read(&cell_val_num_, sizeof(uint32_t)));

    // Load filter pipeline
    RETURN_NOT_OK(filters_.deserialize(buff));
  } else {
    type_ = type;
  }

  // Load domain
  uint64_t domain_size = 0;
  if (version >= 5) {
    RETURN_NOT_OK(buff->read(&domain_size, sizeof(uint64_t)));
  } else {
    domain_size = 2 * coord_size();
  }
  if (domain_size != 0) {
    std::vector<uint8_t> tmp(domain_size);
    RETURN_NOT_OK(buff->read(&tmp[0], domain_size));
    domain_ = Range(&tmp[0], domain_size);
  }

  // Load tile extent
  tile_extent_.clear();
  uint8_t null_tile_extent;
  RETURN_NOT_OK(buff->read(&null_tile_extent, sizeof(uint8_t)));
  if (null_tile_extent == 0) {
    tile_extent_.resize(coord_size());
    RETURN_NOT_OK(buff->read(&tile_extent_[0], coord_size()));
  }

  set_ceil_to_tile_func();
  set_check_range_func();
  set_coincides_with_tiles_func();
  set_compute_mbr_func();
  set_crop_range_func();
  set_domain_range_func();
  set_expand_range_func();
  set_expand_range_v_func();
  set_expand_to_tile_func();
  set_oob_func();
  set_covered_func();
  set_overlap_func();
  set_overlap_ratio_func();
  set_split_range_func();
  set_splitting_value_func();
  set_tile_num_func();
  set_value_in_range_func();

  return Status::Ok();
}

const Range& Dimension::domain() const {
  return domain_;
}

void Dimension::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;
  // Retrieve domain and tile extent strings
  std::string domain_s = domain_str();
  std::string tile_extent_s = tile_extent_str();

  // Dump
  fprintf(out, "### Dimension ###\n");
  fprintf(out, "- Name: %s\n", name_.c_str());
  fprintf(out, "- Type: %s\n", datatype_str(type_).c_str());
  if (!var_size())
    fprintf(out, "- Cell val num: %u\n", cell_val_num_);
  else
    fprintf(out, "- Cell val num: var\n");
  fprintf(out, "- Domain: %s\n", domain_s.c_str());
  fprintf(out, "- Tile extent: %s\n", tile_extent_s.c_str());
  fprintf(out, "- Filters: %u", (unsigned)filters_.size());
  filters_.dump(out);
  fprintf(out, "\n");
}

const FilterPipeline& Dimension::filters() const {
  return filters_;
}

const std::string& Dimension::name() const {
  return name_;
}

template <class T>
void Dimension::ceil_to_tile(
    const Dimension* dim, const Range& r, uint64_t tile_num, ByteVecValue* v) {
  assert(dim != nullptr);
  assert(!r.empty());
  assert(v != nullptr);
  assert(!dim->tile_extent().empty());

  auto tile_extent = *(const T*)dim->tile_extent().data();
  auto dim_dom = (const T*)dim->domain().data();
  v->resize(sizeof(T));
  auto r_t = (const T*)r.data();

  T mid = r_t[0] + (tile_num + 1) * tile_extent;
  uint64_t div = (mid - dim_dom[0]) / tile_extent;
  auto floored_mid = (T)div * tile_extent + dim_dom[0];
  T sp = (std::numeric_limits<T>::is_integer) ?
             floored_mid - 1 :
             std::nextafter(floored_mid, std::numeric_limits<T>::lowest());
  std::memcpy(&(*v)[0], &sp, sizeof(T));
}

void Dimension::ceil_to_tile(
    const Range& r, uint64_t tile_num, ByteVecValue* v) const {
  assert(ceil_to_tile_func_ != nullptr);
  ceil_to_tile_func_(this, r, tile_num, v);
}

Status Dimension::check_range(const Range& range) const {
  // Inapplicable to string domains
  if (type_ == Datatype::STRING_ASCII)
    return Status::Ok();

  assert(check_range_func_ != nullptr);
  std::string err_msg;
  auto ret = check_range_func_(this, range, &err_msg);
  if (!ret)
    return LOG_STATUS(Status::DimensionError(err_msg));
  return Status::Ok();
}

template <class T>
bool Dimension::coincides_with_tiles(const Dimension* dim, const Range& r) {
  assert(dim != nullptr);
  assert(!r.empty());
  assert(!dim->tile_extent().empty());

  auto dim_domain = (const T*)dim->domain().data();
  auto tile_extent = *(const T*)dim->tile_extent().data();
  auto d = (const T*)r.data();
  auto norm_1 = uint64_t(d[0] - dim_domain[0]);
  auto norm_2 = (uint64_t(d[1]) - dim_domain[0]) + 1;
  return ((norm_1 / tile_extent) * tile_extent == norm_1) &&
         ((norm_2 / tile_extent) * tile_extent == norm_2);
}

bool Dimension::coincides_with_tiles(const Range& r) const {
  assert(coincides_with_tiles_func_ != nullptr);
  return coincides_with_tiles_func_(this, r);
}

template <class T>
Status Dimension::compute_mbr(const Tile& tile, Range* mbr) {
  assert(mbr != nullptr);
  ChunkedBuffer* const chunked_buffer = tile.chunked_buffer();
  auto cell_num = tile.cell_num();
  assert(cell_num > 0);

  size_t chunk_idx = 0;
  size_t chunk_offset = 0;

  // Fetch the first internal buffer.
  void* buffer;
  RETURN_NOT_OK(chunked_buffer->internal_buffer(chunk_idx, &buffer));

  for (uint64_t c = 0; c < cell_num; ++c) {
    // Move to the next internal chunk buffer if the offset exceeds the
    // current chunk buffer's size.
    uint32_t chunk_size;
    RETURN_NOT_OK(chunked_buffer->internal_buffer_size(chunk_idx, &chunk_size));
    if (chunk_offset >= chunk_size) {
      ++chunk_idx;
      chunk_offset = 0;

      // Fetch the next internal buffer and its associated size.
      RETURN_NOT_OK(chunked_buffer->internal_buffer(chunk_idx, &buffer));
      RETURN_NOT_OK(
          chunked_buffer->internal_buffer_size(chunk_idx, &chunk_size));
    }

    // Sanity check we have sufficient space to perform our random access
    // read of 'buffer'.
    if (chunk_size < (chunk_offset + sizeof(T))) {
      return Status::DimensionError(
          "Out of bounds read to internal chunk buffer of size " +
          std::to_string(chunk_size));
    }

    const T* const v = static_cast<T*>(
        static_cast<void*>(static_cast<char*>(buffer) + chunk_offset));
    chunk_offset += sizeof(T);

    if (c == 0) {
      T res[] = {*v, *v};
      mbr->set_range(res, sizeof(res));
    } else {
      expand_range_v<T>(v, mbr);
    }
  }

  return Status::Ok();
}

Status Dimension::compute_mbr(const Tile& tile, Range* mbr) const {
  assert(compute_mbr_func_ != nullptr);
  return compute_mbr_func_(tile, mbr);
}

template <>
Status Dimension::compute_mbr_var<char>(
    const Tile& tile_off, const Tile& tile_val, Range* mbr) {
  assert(mbr != nullptr);
  ChunkedBuffer* const chunked_buffer_off = tile_off.chunked_buffer();
  ChunkedBuffer* const chunked_buffer_val = tile_val.chunked_buffer();
  auto d_val_size = tile_val.size();
  auto cell_num = tile_off.cell_num();
  assert(cell_num > 0);

  size_t chunk_idx_off = 0;
  size_t chunk_offset_off = 0;
  size_t chunk_idx_val = 0;
  size_t chunk_bytes_processed_val = 0;

  // Fetch the first internal buffers.
  void* buffer_off;
  RETURN_NOT_OK(
      chunked_buffer_off->internal_buffer(chunk_idx_off, &buffer_off));
  void* buffer_val;
  RETURN_NOT_OK(
      chunked_buffer_val->internal_buffer(chunk_idx_val, &buffer_val));

  // Fetch the value in 'chunked_buffer_off' at offset 'c'.
  // 'c' starts at 0, so we're jut taking value at the
  // address of 'buffer_off'.
  uint64_t d_off_c0 = *static_cast<uint64_t*>(buffer_off);
  chunk_offset_off += sizeof(uint64_t);

  for (uint64_t c = 0; c < cell_num; ++c) {
    // Fetch the value in 'chunked_buffer_off' at offset c + 1.
    uint64_t d_off_c1;
    if (c == (cell_num - 1)) {
      d_off_c1 = d_val_size;
    } else {
      // Move to the next internal chunk buffer of 'chunked_buffer_off'
      // if the offset exceeds the current chunk buffer's size.
      uint32_t chunk_size_off;
      RETURN_NOT_OK(chunked_buffer_off->internal_buffer_size(
          chunk_idx_off, &chunk_size_off));
      if (chunk_offset_off >= chunk_size_off) {
        ++chunk_idx_off;
        chunk_offset_off = 0;

        // Fetch the next internal buffer of 'chunked_buffer_off' and its
        // associated size.
        RETURN_NOT_OK(
            chunked_buffer_off->internal_buffer(chunk_idx_off, &buffer_off));
        RETURN_NOT_OK(chunked_buffer_off->internal_buffer_size(
            chunk_idx_off, &chunk_size_off));
      }

      // Sanity check we have sufficient space to perform our random access
      // read of 'buffer_off'.
      if (chunk_size_off < (chunk_offset_off + sizeof(uint64_t))) {
        return Status::DimensionError(
            "Out of bounds read to internal chunk buffer of size " +
            std::to_string(chunk_size_off));
      }

      d_off_c1 = *static_cast<uint64_t*>(static_cast<void*>(
          static_cast<char*>(buffer_off) + chunk_offset_off));
      chunk_offset_off += sizeof(uint64_t);
    }

    if (c == 0) {
      mbr->set_range_var(buffer_val, d_off_c1, buffer_val, d_off_c1);
    } else {
      // Offset values are guaranteed to be in ascending order.
      assert(chunk_bytes_processed_val <= d_off_c0);

      // iterate until 'chunk_idx_val' contains 'd_off_c0'
      uint32_t chunk_size_val;
      while (true) {
        RETURN_NOT_OK(chunked_buffer_val->internal_buffer_size(
            chunk_idx_val, &chunk_size_val));
        if ((d_off_c0 - chunk_bytes_processed_val) < chunk_size_val) {
          break;
        }

        ++chunk_idx_val;
        chunk_bytes_processed_val += chunk_size_val;
      }

      // Fetch the next internal buffer of 'chunked_buffer_val' and its
      // associated size.
      RETURN_NOT_OK(
          chunked_buffer_val->internal_buffer(chunk_idx_val, &buffer_val));

      // Sanity check we have sufficient space to perform our random access
      // read of 'buffer_val'.
      const size_t chunk_offset_val = d_off_c0 - chunk_bytes_processed_val;
      const uint64_t size = d_off_c1 - d_off_c0;
      if (chunk_size_val < (chunk_offset_val + size)) {
        return Status::DimensionError(
            "Out of bounds read to internal chunk buffer of size " +
            std::to_string(chunk_size_val));
      }

      const char* const v = static_cast<char*>(buffer_val) + chunk_offset_val;
      expand_range_var_v(v, size, mbr);
    }

    // Save the value in 'chunked_buffer_off' at offset 'c + 1' as the
    // offset of 'c' in the next loop iteration.
    d_off_c0 = d_off_c1;
  }

  return Status::Ok();
}

Status Dimension::compute_mbr_var(
    const Tile& tile_off, const Tile& tile_val, Range* mbr) const {
  assert(compute_mbr_var_func_ != nullptr);
  return compute_mbr_var_func_(tile_off, tile_val, mbr);
}

template <class T>
void Dimension::crop_range(const Dimension* dim, Range* range) {
  assert(dim != nullptr);
  assert(!range->empty());
  auto dim_dom = (const T*)dim->domain().data();
  auto r = (const T*)range->data();
  T res[2] = {std::max(r[0], dim_dom[0]), std::min(r[1], dim_dom[1])};
  range->set_range(res, sizeof(res));
}

void Dimension::crop_range(Range* range) const {
  assert(crop_range_func_ != nullptr);
  crop_range_func_(this, range);
}

template <class T>
uint64_t Dimension::domain_range(const Range& range) {
  assert(!range.empty());

  // Inapplicable to real domains
  if (!std::is_integral<T>::value)
    return std::numeric_limits<uint64_t>::max();

  auto r = (const T*)range.data();
  uint64_t ret = r[1] - r[0];
  if (ret == std::numeric_limits<uint64_t>::max())  // overflow
    return ret;
  ++ret;

  return ret;
}

uint64_t Dimension::domain_range(const Range& range) const {
  assert(domain_range_func_ != nullptr);
  return domain_range_func_(range);
}

template <class T>
void Dimension::expand_range_v(const void* v, Range* r) {
  assert(v != nullptr);
  assert(r != nullptr);
  assert(!r->empty());
  auto rt = (const T*)r->data();
  auto vt = (const T*)v;
  T res[2] = {std::min(rt[0], *vt), std::max(rt[1], *vt)};
  r->set_range(res, sizeof(res));
}

void Dimension::expand_range_v(const void* v, Range* r) const {
  assert(expand_range_v_func_ != nullptr);
  expand_range_v_func_(v, r);
}

void Dimension::expand_range_var_v(const char* v, uint64_t v_size, Range* r) {
  assert(v != nullptr);
  assert(r != nullptr);
  assert(!r->empty());
  assert(v_size != 0);

  auto start = r->start_str();
  auto end = r->end_str();
  auto v_str = std::string(v, v_size);

  r->set_str_range(
      (v_str < start) ? v_str : start, (v_str > end) ? v_str : end);
}

template <class T>
void Dimension::expand_range(const Range& r1, Range* r2) {
  assert(!r1.empty());
  assert(!r2->empty());
  auto d1 = (const T*)r1.data();
  auto d2 = (const T*)r2->data();
  T res[2] = {std::min(d1[0], d2[0]), std::max(d1[1], d2[1])};
  r2->set_range(res, sizeof(res));
}

void Dimension::expand_range(const Range& r1, Range* r2) const {
  assert(expand_range_func_ != nullptr);
  expand_range_func_(r1, r2);
}

void Dimension::expand_range_var(const Range& r1, Range* r2) const {
  assert(type_ == Datatype::STRING_ASCII);
  assert(!r1.empty());
  assert(!r2->empty());

  auto r1_start = r1.start_str();
  auto r1_end = r1.end_str();

  auto r2_start = r2->start_str();
  auto r2_end = r2->end_str();

  auto min = (r1_start < r2_start) ? r1_start : r2_start;
  auto max = (r1_end < r2_end) ? r2_end : r1_end;

  r2->set_str_range(min, max);
}

template <class T>
void Dimension::expand_to_tile(const Dimension* dim, Range* range) {
  assert(dim != nullptr);
  assert(!range->empty());

  // Applicable only to regular tiles and integral domains
  if (dim->tile_extent().empty() || !std::is_integral<T>::value)
    return;

  auto tile_extent = *(const T*)dim->tile_extent().data();
  auto dim_dom = (const T*)dim->domain().data();
  auto r = (const T*)range->data();
  T res[2];

  res[0] = ((r[0] - dim_dom[0]) / tile_extent * tile_extent) + dim_dom[0];
  res[1] =
      ((r[1] - dim_dom[0]) / tile_extent + 1) * tile_extent - 1 + dim_dom[0];

  range->set_range(res, sizeof(res));
}

void Dimension::expand_to_tile(Range* range) const {
  assert(expand_to_tile_func_ != nullptr);
  expand_to_tile_func_(this, range);
}

template <class T>
bool Dimension::oob(
    const Dimension* dim, const void* coord, std::string* err_msg) {
  auto domain = (const T*)dim->domain().data();
  auto coord_t = (const T*)coord;
  if (*coord_t < domain[0] || *coord_t > domain[1]) {
    std::stringstream ss;
    ss << "Coordinate " << *coord_t << " is out of domain bounds [" << domain[0]
       << ", " << domain[1] << "] on dimension '" << dim->name() << "'";
    *err_msg = ss.str();
    return true;
  }

  return false;
}

Status Dimension::oob(const void* coord) const {
  // Not applicable to string dimensions
  if (datatype_is_string(type_))
    return Status::Ok();

  assert(oob_func_ != nullptr);
  std::string err_msg;
  auto ret = oob_func_(this, coord, &err_msg);
  if (ret)
    return Status::DimensionError(err_msg);
  return Status::Ok();
}

template <class T>
bool Dimension::covered(const Range& r1, const Range& r2) {
  assert(!r1.empty());
  assert(!r2.empty());

  auto d1 = (const T*)r1.data();
  auto d2 = (const T*)r2.data();
  assert(d1[0] <= d1[1]);
  assert(d2[0] <= d2[1]);

  return d1[0] >= d2[0] && d1[1] <= d2[1];
}

bool Dimension::covered(const Range& r1, const Range& r2) const {
  assert(covered_func_ != nullptr);
  return covered_func_(r1, r2);
}

template <>
bool Dimension::overlap<char>(const Range& r1, const Range& r2) {
  assert(!r1.empty());
  assert(!r2.empty());

  auto s1_start = r1.start_str();
  auto s1_end = r1.end_str();
  auto s2_start = r2.start_str();
  auto s2_end = r2.end_str();

  auto r1_after_r2 = s1_start > s2_end;
  auto r2_after_r1 = s2_start > s1_end;

  return !r1_after_r2 && !r2_after_r1;
}

template <class T>
bool Dimension::overlap(const Range& r1, const Range& r2) {
  assert(!r1.empty());
  assert(!r2.empty());

  auto d1 = (const T*)r1.data();
  auto d2 = (const T*)r2.data();
  return !(d1[0] > d2[1] || d1[1] < d2[0]);
}

bool Dimension::overlap(const Range& r1, const Range& r2) const {
  assert(overlap_func_ != nullptr);
  return overlap_func_(r1, r2);
}

template <>
double Dimension::overlap_ratio<char>(const Range& r1, const Range& r2) {
  assert(!r1.empty());
  assert(!r2.empty());

  if (!overlap<char>(r1, r2))
    return 0.0;

  auto r1_start = r1.start_str();
  auto r1_end = r1.end_str();
  auto r2_start = r2.start_str();
  auto r2_end = r2.end_str();

  // Calculate the range of r2
  uint64_t r2_range, pref_size = 0;
  if (r2.unary()) {
    r2_range = 1;
  } else {
    pref_size = utils::parse::common_prefix_size(r2_start, r2_end);
    auto r2_start_c = (r2_start.size() == pref_size) ? 0 : r2_start[pref_size];
    assert(r2_end.size() > pref_size);
    r2_range = r2_end[pref_size] - r2_start_c + 1;
  }

  // Calculate the overlap and its range
  uint64_t o_range;
  auto o_start = (r1_start > r2_start) ? r1_start : r2_start;
  auto o_end = (r1_end < r2_end) ? r1_end : r2_end;
  if (o_start == o_end) {
    o_range = 1;
  } else {
    assert(o_end.size() > pref_size);
    auto o_start_c = (o_start.size() == pref_size) ? 0 : o_start[pref_size];
    o_range = o_end[pref_size] - o_start_c + 1;
  }

  // If the overlap is not equal to r2, then the ratio cannot be
  // 1.0 - reduce it by epsilon
  auto ratio = (double)o_range / r2_range;
  if (ratio == 1.0 && (o_start != r2_start || o_end != r2_end))
    ratio = std::nextafter(ratio, std::numeric_limits<double>::min());

  return ratio;
}

template <class T>
double Dimension::overlap_ratio(const Range& r1, const Range& r2) {
  assert(!r1.empty());
  assert(!r2.empty());

  auto d1 = (const T*)r1.data();
  auto d2 = (const T*)r2.data();
  assert(d1[0] <= d1[1]);
  assert(d2[0] <= d2[1]);

  // No overlap
  if (d1[0] > d2[1] || d1[1] < d2[0])
    return 0.0;

  // Compute ratio
  auto overlap_start = std::max(d1[0], d2[0]);
  auto overlap_end = std::min(d1[1], d2[1]);
  auto overlap_range = overlap_end - overlap_start;
  auto mbr_range = d2[1] - d2[0];
  auto max = std::numeric_limits<double>::max();
  if (std::numeric_limits<T>::is_integer) {
    overlap_range += 1;
    mbr_range += 1;
  } else {
    if (overlap_range == 0)
      overlap_range = std::nextafter(overlap_range, max);
    if (mbr_range == 0)
      mbr_range = std::nextafter(mbr_range, max);
  }

  return (double)overlap_range / mbr_range;
}

double Dimension::overlap_ratio(const Range& r1, const Range& r2) const {
  assert(overlap_ratio_func_ != nullptr);
  return overlap_ratio_func_(r1, r2);
}

template <>
void Dimension::split_range<char>(
    const Range& r, const ByteVecValue& v, Range* r1, Range* r2) {
  assert(!r.empty());
  assert(!v.empty());
  assert(!r.unary());
  assert(r1 != nullptr);
  assert(r2 != nullptr);

  // First range
  auto new_r1_end = std::string((const char*)v.data(), v.size());
  r1->set_str_range(r.start_str(), new_r1_end);

  // Second range
  auto start = std::string((const char*)v.data(), v.size());
  auto end = r.end_str();
  auto split_pos = utils::parse::common_prefix_size(start, end);
  auto new_v = (char)(start[split_pos] + 1);
  auto new_r2_start = start.substr(0, split_pos) + new_v;
  assert(new_r2_start > new_r1_end);
  r2->set_str_range(new_r2_start, r.end_str());
}

template <class T>
void Dimension::split_range(
    const Range& r, const ByteVecValue& v, Range* r1, Range* r2) {
  assert(!r.empty());
  assert(!v.empty());
  assert(r1 != nullptr);
  assert(r2 != nullptr);

  auto max = std::numeric_limits<T>::max();
  bool int_domain = std::numeric_limits<T>::is_integer;
  auto r_t = (const T*)r.data();
  auto v_t = *(const T*)(&v[0]);

  T ret[2];
  ret[0] = r_t[0];
  ret[1] = v_t;
  r1->set_range(ret, sizeof(ret));
  ret[0] = (int_domain) ? (v_t + 1) : std::nextafter(v_t, max);
  ret[1] = r_t[1];
  r2->set_range(ret, sizeof(ret));
}

void Dimension::split_range(
    const Range& r, const ByteVecValue& v, Range* r1, Range* r2) const {
  assert(split_range_func_ != nullptr);
  split_range_func_(r, v, r1, r2);
}

template <>
void Dimension::splitting_value<char>(
    const Range& r, ByteVecValue* v, bool* unsplittable) {
  assert(!r.empty());
  assert(v != nullptr);
  assert(unsplittable != nullptr);

  // Check unsplittable
  if (r.unary()) {
    *unsplittable = true;
    return;
  }

  *unsplittable = false;

  // Find position to split
  auto start = r.start_str();
  auto end = r.end_str();
  auto pref_size = utils::parse::common_prefix_size(start, end);

  // Check unsplittable
  if (end[pref_size] == 0) {
    *unsplittable = true;
    return;
  }

  auto start_c = (start.size() == pref_size) ? 0 : start[pref_size];
  auto split_v = (end[pref_size] - start_c) / 2;
  auto split_str = start.substr(0, pref_size) + (char)(start_c + split_v);
  split_str = (split_str < start) ? start : split_str;

  v->resize(split_str.size());
  std::memcpy(v->data(), split_str.data(), split_str.size());
}

template <class T>
void Dimension::splitting_value(
    const Range& r, ByteVecValue* v, bool* unsplittable) {
  assert(!r.empty());
  assert(v != nullptr);
  assert(unsplittable != nullptr);

  auto r_t = (const T*)r.data();
  T sp = r_t[0] + (r_t[1] - r_t[0]) / 2;
  v->resize(sizeof(T));
  std::memcpy(&(*v)[0], &sp, sizeof(T));
  *unsplittable = !std::memcmp(&sp, &r_t[1], sizeof(T));
}

void Dimension::splitting_value(
    const Range& r, ByteVecValue* v, bool* unsplittable) const {
  assert(splitting_value_func_ != nullptr);
  splitting_value_func_(r, v, unsplittable);
}

template <>
uint64_t Dimension::tile_num<char>(const Dimension* dim, const Range& range) {
  (void)dim;
  (void)range;

  return 1;
}

template <class T>
uint64_t Dimension::tile_num(const Dimension* dim, const Range& range) {
  assert(dim != nullptr);
  assert(!range.empty());

  // Trivial cases
  if (dim->tile_extent().empty())
    return 1;

  auto tile_extent = *(const T*)dim->tile_extent().data();
  auto dim_dom = (const T*)dim->domain().data();
  auto r = (const T*)range.data();
  const uint64_t start = floor((r[0] - dim_dom[0]) / tile_extent);
  const uint64_t end = floor((r[1] - dim_dom[0]) / tile_extent);
  return end - start + 1;
}

uint64_t Dimension::tile_num(const Range& range) const {
  assert(tile_num_func_ != nullptr);
  return tile_num_func_(this, range);
}

template <class T>
bool Dimension::value_in_range(const void* value, const Range& range) {
  assert(value != nullptr);
  assert(!range.empty());
  auto v = (const T*)value;
  auto r = (const T*)(range.data());
  return *v >= r[0] && *v <= r[1];
}

bool Dimension::value_in_range(const void* value, const Range& range) const {
  assert(value_in_range_func_ != nullptr);
  return value_in_range_func_(value, range);
}

bool Dimension::value_in_range(
    const std::string& value, const Range& range) const {
  assert(type_ == Datatype::STRING_ASCII);
  assert(!value.empty());
  assert(!range.empty());

  return value >= range.start_str() && value <= range.end_str();
}

// ===== FORMAT =====
// dimension_name_size (uint32_t)
// dimension_name (string)
// type (uint8_t)
// cell_val_num (uint32_t)
// filter_pipeline (see FilterPipeline::serialize)
// domain_size (uin64_t)
// domain (void* - domain_size)
// null_tile_extent (uint8_t)
// tile_extent (void* - type_size)
Status Dimension::serialize(Buffer* buff, uint32_t version) {
  // Sanity check
  auto is_str = datatype_is_string(type_);
  assert(is_str || !domain_.empty());

  // Write dimension name
  auto dimension_name_size = (uint32_t)name_.size();
  RETURN_NOT_OK(buff->write(&dimension_name_size, sizeof(uint32_t)));
  RETURN_NOT_OK(buff->write(name_.c_str(), dimension_name_size));

  // Applicable only to version >= 5
  if (version >= 5) {
    // Write type
    auto type = (uint8_t)type_;
    RETURN_NOT_OK(buff->write(&type, sizeof(uint8_t)));

    // Write cell_val_num_
    RETURN_NOT_OK(buff->write(&cell_val_num_, sizeof(uint32_t)));

    // Write filter pipeline
    RETURN_NOT_OK(filters_.serialize(buff));
  }

  // Write domain and tile extent
  uint64_t domain_size = (is_str) ? 0 : 2 * coord_size();
  RETURN_NOT_OK(buff->write(&domain_size, sizeof(uint64_t)));
  RETURN_NOT_OK(buff->write(domain_.data(), domain_size));

  auto null_tile_extent = (uint8_t)((tile_extent_.empty()) ? 1 : 0);
  RETURN_NOT_OK(buff->write(&null_tile_extent, sizeof(uint8_t)));
  if (!tile_extent_.empty())
    RETURN_NOT_OK(buff->write(tile_extent_.data(), tile_extent_.size()));

  return Status::Ok();
}

Status Dimension::set_domain(const void* domain) {
  if (type_ == Datatype::STRING_ASCII) {
    if (domain == nullptr)
      return Status::Ok();
    return LOG_STATUS(Status::DimensionError(
        std::string("Setting the domain to a dimension with type '") +
        datatype_str(type_) + "' is not supported"));
  }

  if (domain == nullptr)
    return Status::Ok();
  return set_domain(Range(domain, 2 * coord_size()));
}

Status Dimension::set_domain(const Range& domain) {
  if (domain.empty())
    return Status::Ok();

  domain_ = domain;
  RETURN_NOT_OK_ELSE(check_domain(), domain_.clear());

  return Status::Ok();
}

Status Dimension::set_filter_pipeline(const FilterPipeline* pipeline) {
  if (pipeline == nullptr)
    return LOG_STATUS(Status::DimensionError(
        "Cannot set filter pipeline to dimension; Pipeline cannot be null"));

  for (unsigned i = 0; i < pipeline->size(); ++i) {
    if (datatype_is_real(type_) &&
        pipeline->get_filter(i)->type() == FilterType::FILTER_DOUBLE_DELTA)
      return LOG_STATUS(
          Status::DimensionError("Cannot set DOUBLE DELTA filter to a "
                                 "dimension with a real datatype"));
  }

  filters_ = *pipeline;

  return Status::Ok();
}

Status Dimension::set_tile_extent(const void* tile_extent) {
  if (type_ == Datatype::STRING_ASCII) {
    if (tile_extent == nullptr)
      return Status::Ok();
    return LOG_STATUS(Status::DimensionError(
        std::string("Setting the tile extent to a dimension with type '") +
        datatype_str(type_) + "' is not supported"));
  }

  ByteVecValue te;
  if (tile_extent != nullptr) {
    auto size = coord_size();
    te.resize(size);
    std::memcpy(&te[0], tile_extent, size);
  }

  return set_tile_extent(te);
}

Status Dimension::set_tile_extent(const ByteVecValue& tile_extent) {
  if (domain_.empty())
    return LOG_STATUS(Status::DimensionError(
        "Cannot set tile extent; Domain must be set first"));

  // Note: this check was added in release 1.6.0. Older arrays may have been
  // serialized with a null extent, and so it is still supported internally.
  // But users can not construct dimension objects with null tile extents.
  if (tile_extent.empty())
    return LOG_STATUS(Status::DimensionError(
        "Cannot set tile extent; tile extent cannot be null"));

  tile_extent_ = tile_extent;

  return check_tile_extent();
}

Status Dimension::set_null_tile_extent_to_range() {
  // Applicable only to null extents
  if (!tile_extent_.empty())
    return Status::Ok();

  if (domain_.empty())
    return LOG_STATUS(Status::DimensionError(
        "Cannot set tile extent to domain range; Domain not set"));

  // Note: this is applicable only to dense array, which are allowed
  // only integer domains

  switch (type_) {
    case Datatype::INT32:
      return set_null_tile_extent_to_range<int>();
    case Datatype::INT64:
      return set_null_tile_extent_to_range<int64_t>();
    case Datatype::INT8:
      return set_null_tile_extent_to_range<int8_t>();
    case Datatype::UINT8:
      return set_null_tile_extent_to_range<uint8_t>();
    case Datatype::INT16:
      return set_null_tile_extent_to_range<int16_t>();
    case Datatype::UINT16:
      return set_null_tile_extent_to_range<uint16_t>();
    case Datatype::UINT32:
      return set_null_tile_extent_to_range<uint32_t>();
    case Datatype::UINT64:
      return set_null_tile_extent_to_range<uint64_t>();
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      return set_null_tile_extent_to_range<int64_t>();
    default:
      return LOG_STATUS(
          Status::DimensionError("Cannot set null tile extent to domain range; "
                                 "Invalid dimension domain type"));
  }

  assert(false);
  return LOG_STATUS(
      Status::DimensionError("Cannot set null tile extent to domain range; "
                             "Unsupported dimension type"));
}

template <class T>
Status Dimension::set_null_tile_extent_to_range() {
  // Applicable only to null extents
  if (!tile_extent_.empty())
    return Status::Ok();

  // Calculate new tile extent equal to domain range
  auto domain = (const T*)domain_.data();
  T tile_extent = domain[1] - domain[0];

  // Check overflow before adding 1
  if (tile_extent == std::numeric_limits<T>::max())
    return LOG_STATUS(Status::DimensionError(
        "Cannot set null tile extent to domain range; "
        "Domain range exceeds domain type max numeric limit"));
  ++tile_extent;
  // After this, tile_extent = domain[1] - domain[0] + 1, which is the correct
  // domain range

  // Allocate space
  uint64_t type_size = sizeof(T);
  tile_extent_.resize(type_size);
  std::memcpy(&tile_extent_[0], &tile_extent, type_size);

  return Status::Ok();
}

const ByteVecValue& Dimension::tile_extent() const {
  return tile_extent_;
}

Datatype Dimension::type() const {
  return type_;
}

bool Dimension::var_size() const {
  return cell_val_num_ == constants::var_num;
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

Status Dimension::check_domain() const {
  switch (type_) {
    case Datatype::INT32:
      return check_domain<int>();
    case Datatype::INT64:
      return check_domain<int64_t>();
    case Datatype::INT8:
      return check_domain<int8_t>();
    case Datatype::UINT8:
      return check_domain<uint8_t>();
    case Datatype::INT16:
      return check_domain<int16_t>();
    case Datatype::UINT16:
      return check_domain<uint16_t>();
    case Datatype::UINT32:
      return check_domain<uint32_t>();
    case Datatype::UINT64:
      return check_domain<uint64_t>();
    case Datatype::FLOAT32:
      return check_domain<float>();
    case Datatype::FLOAT64:
      return check_domain<double>();
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      return check_domain<int64_t>();
    default:
      return LOG_STATUS(Status::DimensionError(
          "Domain check failed; Invalid dimension domain type"));
  }
}

Status Dimension::check_tile_extent() const {
  switch (type_) {
    case Datatype::INT32:
      return check_tile_extent<int>();
    case Datatype::INT64:
      return check_tile_extent<int64_t>();
    case Datatype::INT8:
      return check_tile_extent<int8_t>();
    case Datatype::UINT8:
      return check_tile_extent<uint8_t>();
    case Datatype::INT16:
      return check_tile_extent<int16_t>();
    case Datatype::UINT16:
      return check_tile_extent<uint16_t>();
    case Datatype::UINT32:
      return check_tile_extent<uint32_t>();
    case Datatype::UINT64:
      return check_tile_extent<uint64_t>();
    case Datatype::FLOAT32:
      return check_tile_extent<float>();
    case Datatype::FLOAT64:
      return check_tile_extent<double>();
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      return check_tile_extent<int64_t>();
    default:
      return LOG_STATUS(Status::DimensionError(
          "Tile extent check failed; Invalid dimension domain type"));
  }
}

template <class T>
Status Dimension::check_tile_extent() const {
  if (domain_.empty())
    return LOG_STATUS(
        Status::DimensionError("Tile extent check failed; Domain not set"));

  if (tile_extent_.empty())
    return Status::Ok();

  auto tile_extent = (const T*)tile_extent_.data();
  auto domain = (const T*)domain_.data();
  bool is_int = std::is_integral<T>::value;

  // Check if tile extent is negative or 0
  if (*tile_extent <= 0)
    return LOG_STATUS(Status::DimensionError(
        "Tile extent check failed; Tile extent must be greater than 0"));

  // Check if tile extent exceeds domain
  if (!is_int) {
    if (*tile_extent > (domain[1] - domain[0] + 1))
      return LOG_STATUS(
          Status::DimensionError("Tile extent check failed; Tile extent "
                                 "exceeds dimension domain range"));
  } else {
    // Check if tile extent exceeds domain
    uint64_t range = domain[1] - domain[0] + 1;
    if (uint64_t(*tile_extent) > range)
      return LOG_STATUS(
          Status::DimensionError("Tile extent check failed; Tile extent "
                                 "exceeds dimension domain range"));

    // In the worst case one tile extent will be added to the upper domain
    // for the dense case, so check if the expanded domain will exceed type
    // T's max limit.
    if (range % uint64_t(*tile_extent)) {
      auto upper_floor =
          ((range - 1) / (*tile_extent)) * (*tile_extent) + domain[0];
      bool exceeds =
          (upper_floor >
           std::numeric_limits<uint64_t>::max() - (*tile_extent - 1));
      exceeds =
          (exceeds ||
           uint64_t(upper_floor) > uint64_t(std::numeric_limits<T>::max()));
      if (exceeds)
        return LOG_STATUS(Status::DimensionError(
            "Tile extent check failed; domain max expanded to multiple of tile "
            "extent exceeds max value representable by domain type. Reduce "
            "domain max by 1 tile extent to allow for expansion."));
    }
  }

  return Status::Ok();
}

std::string Dimension::domain_str() const {
  std::stringstream ss;

  if (domain_.empty())
    return constants::null_str;

  const int* domain_int32;
  const int64_t* domain_int64;
  const float* domain_float32;
  const double* domain_float64;
  const int8_t* domain_int8;
  const uint8_t* domain_uint8;
  const int16_t* domain_int16;
  const uint16_t* domain_uint16;
  const uint32_t* domain_uint32;
  const uint64_t* domain_uint64;

  switch (type_) {
    case Datatype::INT32:
      domain_int32 = (const int32_t*)domain_.data();
      ss << "[" << domain_int32[0] << "," << domain_int32[1] << "]";
      return ss.str();
    case Datatype::INT64:
      domain_int64 = (const int64_t*)domain_.data();
      ss << "[" << domain_int64[0] << "," << domain_int64[1] << "]";
      return ss.str();
    case Datatype::FLOAT32:
      domain_float32 = (const float*)domain_.data();
      ss << "[" << domain_float32[0] << "," << domain_float32[1] << "]";
      return ss.str();
    case Datatype::FLOAT64:
      domain_float64 = (const double*)domain_.data();
      ss << "[" << domain_float64[0] << "," << domain_float64[1] << "]";
      return ss.str();
    case Datatype::INT8:
      domain_int8 = (const int8_t*)domain_.data();
      ss << "[" << int(domain_int8[0]) << "," << int(domain_int8[1]) << "]";
      return ss.str();
    case Datatype::UINT8:
      domain_uint8 = (const uint8_t*)domain_.data();
      ss << "[" << int(domain_uint8[0]) << "," << int(domain_uint8[1]) << "]";
      return ss.str();
    case Datatype::INT16:
      domain_int16 = (const int16_t*)domain_.data();
      ss << "[" << domain_int16[0] << "," << domain_int16[1] << "]";
      return ss.str();
    case Datatype::UINT16:
      domain_uint16 = (const uint16_t*)domain_.data();
      ss << "[" << domain_uint16[0] << "," << domain_uint16[1] << "]";
      return ss.str();
    case Datatype::UINT32:
      domain_uint32 = (const uint32_t*)domain_.data();
      ss << "[" << domain_uint32[0] << "," << domain_uint32[1] << "]";
      return ss.str();
    case Datatype::UINT64:
      domain_uint64 = (const uint64_t*)domain_.data();
      ss << "[" << domain_uint64[0] << "," << domain_uint64[1] << "]";
      return ss.str();
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      domain_int64 = (const int64_t*)domain_.data();
      ss << "[" << domain_int64[0] << "," << domain_int64[1] << "]";
      return ss.str();

    case Datatype::CHAR:
    case Datatype::STRING_ASCII:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::ANY:
      // Not supported domain type
      assert(false);
      return "";
  }

  assert(false);
  return "";
}

std::string Dimension::tile_extent_str() const {
  std::stringstream ss;

  if (tile_extent_.empty())
    return constants::null_str;

  const int* tile_extent_int32;
  const int64_t* tile_extent_int64;
  const float* tile_extent_float32;
  const double* tile_extent_float64;
  const int8_t* tile_extent_int8;
  const uint8_t* tile_extent_uint8;
  const int16_t* tile_extent_int16;
  const uint16_t* tile_extent_uint16;
  const uint32_t* tile_extent_uint32;
  const uint64_t* tile_extent_uint64;

  switch (type_) {
    case Datatype::INT32:
      tile_extent_int32 = (const int32_t*)tile_extent_.data();
      ss << *tile_extent_int32;
      return ss.str();
    case Datatype::INT64:
      tile_extent_int64 = (const int64_t*)tile_extent_.data();
      ss << *tile_extent_int64;
      return ss.str();
    case Datatype::FLOAT32:
      tile_extent_float32 = (const float*)tile_extent_.data();
      ss << *tile_extent_float32;
      return ss.str();
    case Datatype::FLOAT64:
      tile_extent_float64 = (const double*)tile_extent_.data();
      ss << *tile_extent_float64;
      return ss.str();
    case Datatype::INT8:
      tile_extent_int8 = (const int8_t*)tile_extent_.data();
      ss << int(*tile_extent_int8);
      return ss.str();
    case Datatype::UINT8:
      tile_extent_uint8 = (const uint8_t*)tile_extent_.data();
      ss << int(*tile_extent_uint8);
      return ss.str();
    case Datatype::INT16:
      tile_extent_int16 = (const int16_t*)tile_extent_.data();
      ss << *tile_extent_int16;
      return ss.str();
    case Datatype::UINT16:
      tile_extent_uint16 = (const uint16_t*)tile_extent_.data();
      ss << *tile_extent_uint16;
      return ss.str();
    case Datatype::UINT32:
      tile_extent_uint32 = (const uint32_t*)tile_extent_.data();
      ss << *tile_extent_uint32;
      return ss.str();
    case Datatype::UINT64:
      tile_extent_uint64 = (const uint64_t*)tile_extent_.data();
      ss << *tile_extent_uint64;
      return ss.str();
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      tile_extent_int64 = (const int64_t*)tile_extent_.data();
      ss << *tile_extent_int64;
      return ss.str();

    case Datatype::CHAR:
    case Datatype::STRING_ASCII:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::ANY:
      // Not supported domain type
      assert(false);
      return "";
  }

  assert(false);
  return "";
}

void Dimension::set_crop_range_func() {
  switch (type_) {
    case Datatype::INT32:
      crop_range_func_ = crop_range<int32_t>;
      break;
    case Datatype::INT64:
      crop_range_func_ = crop_range<int64_t>;
      break;
    case Datatype::INT8:
      crop_range_func_ = crop_range<int8_t>;
      break;
    case Datatype::UINT8:
      crop_range_func_ = crop_range<uint8_t>;
      break;
    case Datatype::INT16:
      crop_range_func_ = crop_range<int16_t>;
      break;
    case Datatype::UINT16:
      crop_range_func_ = crop_range<uint16_t>;
      break;
    case Datatype::UINT32:
      crop_range_func_ = crop_range<uint32_t>;
      break;
    case Datatype::UINT64:
      crop_range_func_ = crop_range<uint64_t>;
      break;
    case Datatype::FLOAT32:
      crop_range_func_ = crop_range<float>;
      break;
    case Datatype::FLOAT64:
      crop_range_func_ = crop_range<double>;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      crop_range_func_ = crop_range<int64_t>;
      break;
    default:
      crop_range_func_ = nullptr;
      break;
  }
}

void Dimension::set_domain_range_func() {
  switch (type_) {
    case Datatype::INT32:
      domain_range_func_ = domain_range<int32_t>;
      break;
    case Datatype::INT64:
      domain_range_func_ = domain_range<int64_t>;
      break;
    case Datatype::INT8:
      domain_range_func_ = domain_range<int8_t>;
      break;
    case Datatype::UINT8:
      domain_range_func_ = domain_range<uint8_t>;
      break;
    case Datatype::INT16:
      domain_range_func_ = domain_range<int16_t>;
      break;
    case Datatype::UINT16:
      domain_range_func_ = domain_range<uint16_t>;
      break;
    case Datatype::UINT32:
      domain_range_func_ = domain_range<uint32_t>;
      break;
    case Datatype::UINT64:
      domain_range_func_ = domain_range<uint64_t>;
      break;
    case Datatype::FLOAT32:
      domain_range_func_ = domain_range<float>;
      break;
    case Datatype::FLOAT64:
      domain_range_func_ = domain_range<double>;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      domain_range_func_ = domain_range<int64_t>;
      break;
    default:
      domain_range_func_ = nullptr;
      break;
  }
}

void Dimension::set_ceil_to_tile_func() {
  switch (type_) {
    case Datatype::INT32:
      ceil_to_tile_func_ = ceil_to_tile<int32_t>;
      break;
    case Datatype::INT64:
      ceil_to_tile_func_ = ceil_to_tile<int64_t>;
      break;
    case Datatype::INT8:
      ceil_to_tile_func_ = ceil_to_tile<int8_t>;
      break;
    case Datatype::UINT8:
      ceil_to_tile_func_ = ceil_to_tile<uint8_t>;
      break;
    case Datatype::INT16:
      ceil_to_tile_func_ = ceil_to_tile<int16_t>;
      break;
    case Datatype::UINT16:
      ceil_to_tile_func_ = ceil_to_tile<uint16_t>;
      break;
    case Datatype::UINT32:
      ceil_to_tile_func_ = ceil_to_tile<uint32_t>;
      break;
    case Datatype::UINT64:
      ceil_to_tile_func_ = ceil_to_tile<uint64_t>;
      break;
    case Datatype::FLOAT32:
      ceil_to_tile_func_ = ceil_to_tile<float>;
      break;
    case Datatype::FLOAT64:
      ceil_to_tile_func_ = ceil_to_tile<double>;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      ceil_to_tile_func_ = ceil_to_tile<int64_t>;
      break;
    default:
      ceil_to_tile_func_ = nullptr;
      break;
  }
}

void Dimension::set_check_range_func() {
  switch (type_) {
    case Datatype::INT32:
      check_range_func_ = check_range<int32_t>;
      break;
    case Datatype::INT64:
      check_range_func_ = check_range<int64_t>;
      break;
    case Datatype::INT8:
      check_range_func_ = check_range<int8_t>;
      break;
    case Datatype::UINT8:
      check_range_func_ = check_range<uint8_t>;
      break;
    case Datatype::INT16:
      check_range_func_ = check_range<int16_t>;
      break;
    case Datatype::UINT16:
      check_range_func_ = check_range<uint16_t>;
      break;
    case Datatype::UINT32:
      check_range_func_ = check_range<uint32_t>;
      break;
    case Datatype::UINT64:
      check_range_func_ = check_range<uint64_t>;
      break;
    case Datatype::FLOAT32:
      check_range_func_ = check_range<float>;
      break;
    case Datatype::FLOAT64:
      check_range_func_ = check_range<double>;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      check_range_func_ = check_range<int64_t>;
      break;
    default:
      check_range_func_ = nullptr;
      break;
  }
}

void Dimension::set_coincides_with_tiles_func() {
  switch (type_) {
    case Datatype::INT32:
      coincides_with_tiles_func_ = coincides_with_tiles<int32_t>;
      break;
    case Datatype::INT64:
      coincides_with_tiles_func_ = coincides_with_tiles<int64_t>;
      break;
    case Datatype::INT8:
      coincides_with_tiles_func_ = coincides_with_tiles<int8_t>;
      break;
    case Datatype::UINT8:
      coincides_with_tiles_func_ = coincides_with_tiles<uint8_t>;
      break;
    case Datatype::INT16:
      coincides_with_tiles_func_ = coincides_with_tiles<int16_t>;
      break;
    case Datatype::UINT16:
      coincides_with_tiles_func_ = coincides_with_tiles<uint16_t>;
      break;
    case Datatype::UINT32:
      coincides_with_tiles_func_ = coincides_with_tiles<uint32_t>;
      break;
    case Datatype::UINT64:
      coincides_with_tiles_func_ = coincides_with_tiles<uint64_t>;
      break;
    case Datatype::FLOAT32:
      coincides_with_tiles_func_ = coincides_with_tiles<float>;
      break;
    case Datatype::FLOAT64:
      coincides_with_tiles_func_ = coincides_with_tiles<double>;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      coincides_with_tiles_func_ = coincides_with_tiles<int64_t>;
      break;
    default:
      coincides_with_tiles_func_ = nullptr;
      break;
  }
}

void Dimension::set_compute_mbr_func() {
  if (!var_size()) {  // Fixed-sized
    compute_mbr_var_func_ = nullptr;
    switch (type_) {
      case Datatype::INT32:
        compute_mbr_func_ = compute_mbr<int32_t>;
        break;
      case Datatype::INT64:
        compute_mbr_func_ = compute_mbr<int64_t>;
        break;
      case Datatype::INT8:
        compute_mbr_func_ = compute_mbr<int8_t>;
        break;
      case Datatype::UINT8:
        compute_mbr_func_ = compute_mbr<uint8_t>;
        break;
      case Datatype::INT16:
        compute_mbr_func_ = compute_mbr<int16_t>;
        break;
      case Datatype::UINT16:
        compute_mbr_func_ = compute_mbr<uint16_t>;
        break;
      case Datatype::UINT32:
        compute_mbr_func_ = compute_mbr<uint32_t>;
        break;
      case Datatype::UINT64:
        compute_mbr_func_ = compute_mbr<uint64_t>;
        break;
      case Datatype::FLOAT32:
        compute_mbr_func_ = compute_mbr<float>;
        break;
      case Datatype::FLOAT64:
        compute_mbr_func_ = compute_mbr<double>;
        break;
      case Datatype::DATETIME_YEAR:
      case Datatype::DATETIME_MONTH:
      case Datatype::DATETIME_WEEK:
      case Datatype::DATETIME_DAY:
      case Datatype::DATETIME_HR:
      case Datatype::DATETIME_MIN:
      case Datatype::DATETIME_SEC:
      case Datatype::DATETIME_MS:
      case Datatype::DATETIME_US:
      case Datatype::DATETIME_NS:
      case Datatype::DATETIME_PS:
      case Datatype::DATETIME_FS:
      case Datatype::DATETIME_AS:
        compute_mbr_func_ = compute_mbr<int64_t>;
        break;
      default:
        compute_mbr_func_ = nullptr;
        break;
    }
  } else {  // Var-sized
    assert(type_ == Datatype::STRING_ASCII);
    compute_mbr_func_ = nullptr;
    compute_mbr_var_func_ = compute_mbr_var<char>;
  }
}

void Dimension::set_expand_range_func() {
  switch (type_) {
    case Datatype::INT32:
      expand_range_func_ = expand_range<int32_t>;
      break;
    case Datatype::INT64:
      expand_range_func_ = expand_range<int64_t>;
      break;
    case Datatype::INT8:
      expand_range_func_ = expand_range<int8_t>;
      break;
    case Datatype::UINT8:
      expand_range_func_ = expand_range<uint8_t>;
      break;
    case Datatype::INT16:
      expand_range_func_ = expand_range<int16_t>;
      break;
    case Datatype::UINT16:
      expand_range_func_ = expand_range<uint16_t>;
      break;
    case Datatype::UINT32:
      expand_range_func_ = expand_range<uint32_t>;
      break;
    case Datatype::UINT64:
      expand_range_func_ = expand_range<uint64_t>;
      break;
    case Datatype::FLOAT32:
      expand_range_func_ = expand_range<float>;
      break;
    case Datatype::FLOAT64:
      expand_range_func_ = expand_range<double>;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      expand_range_func_ = expand_range<int64_t>;
      break;
    default:
      expand_range_func_ = nullptr;
      break;
  }
}

void Dimension::set_expand_range_v_func() {
  switch (type_) {
    case Datatype::INT32:
      expand_range_v_func_ = expand_range_v<int32_t>;
      break;
    case Datatype::INT64:
      expand_range_v_func_ = expand_range_v<int64_t>;
      break;
    case Datatype::INT8:
      expand_range_v_func_ = expand_range_v<int8_t>;
      break;
    case Datatype::UINT8:
      expand_range_v_func_ = expand_range_v<uint8_t>;
      break;
    case Datatype::INT16:
      expand_range_v_func_ = expand_range_v<int16_t>;
      break;
    case Datatype::UINT16:
      expand_range_v_func_ = expand_range_v<uint16_t>;
      break;
    case Datatype::UINT32:
      expand_range_v_func_ = expand_range_v<uint32_t>;
      break;
    case Datatype::UINT64:
      expand_range_v_func_ = expand_range_v<uint64_t>;
      break;
    case Datatype::FLOAT32:
      expand_range_v_func_ = expand_range_v<float>;
      break;
    case Datatype::FLOAT64:
      expand_range_v_func_ = expand_range_v<double>;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      expand_range_v_func_ = expand_range_v<int64_t>;
      break;
    default:
      expand_range_v_func_ = nullptr;
      break;
  }
}

void Dimension::set_expand_to_tile_func() {
  switch (type_) {
    case Datatype::INT32:
      expand_to_tile_func_ = expand_to_tile<int32_t>;
      break;
    case Datatype::INT64:
      expand_to_tile_func_ = expand_to_tile<int64_t>;
      break;
    case Datatype::INT8:
      expand_to_tile_func_ = expand_to_tile<int8_t>;
      break;
    case Datatype::UINT8:
      expand_to_tile_func_ = expand_to_tile<uint8_t>;
      break;
    case Datatype::INT16:
      expand_to_tile_func_ = expand_to_tile<int16_t>;
      break;
    case Datatype::UINT16:
      expand_to_tile_func_ = expand_to_tile<uint16_t>;
      break;
    case Datatype::UINT32:
      expand_to_tile_func_ = expand_to_tile<uint32_t>;
      break;
    case Datatype::UINT64:
      expand_to_tile_func_ = expand_to_tile<uint64_t>;
      break;
    case Datatype::FLOAT32:
      expand_to_tile_func_ = expand_to_tile<float>;
      break;
    case Datatype::FLOAT64:
      expand_to_tile_func_ = expand_to_tile<double>;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      expand_to_tile_func_ = expand_to_tile<int64_t>;
      break;
    default:
      expand_to_tile_func_ = nullptr;
      break;
  }
}

void Dimension::set_oob_func() {
  switch (type_) {
    case Datatype::INT32:
      oob_func_ = oob<int32_t>;
      break;
    case Datatype::INT64:
      oob_func_ = oob<int64_t>;
      break;
    case Datatype::INT8:
      oob_func_ = oob<int8_t>;
      break;
    case Datatype::UINT8:
      oob_func_ = oob<uint8_t>;
      break;
    case Datatype::INT16:
      oob_func_ = oob<int16_t>;
      break;
    case Datatype::UINT16:
      oob_func_ = oob<uint16_t>;
      break;
    case Datatype::UINT32:
      oob_func_ = oob<uint32_t>;
      break;
    case Datatype::UINT64:
      oob_func_ = oob<uint64_t>;
      break;
    case Datatype::FLOAT32:
      oob_func_ = oob<float>;
      break;
    case Datatype::FLOAT64:
      oob_func_ = oob<double>;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      oob_func_ = oob<int64_t>;
      break;
    default:
      oob_func_ = nullptr;
      break;
  }
}

void Dimension::set_covered_func() {
  switch (type_) {
    case Datatype::INT32:
      covered_func_ = covered<int32_t>;
      break;
    case Datatype::INT64:
      covered_func_ = covered<int64_t>;
      break;
    case Datatype::INT8:
      covered_func_ = covered<int8_t>;
      break;
    case Datatype::UINT8:
      covered_func_ = covered<uint8_t>;
      break;
    case Datatype::INT16:
      covered_func_ = covered<int16_t>;
      break;
    case Datatype::UINT16:
      covered_func_ = covered<uint16_t>;
      break;
    case Datatype::UINT32:
      covered_func_ = covered<uint32_t>;
      break;
    case Datatype::UINT64:
      covered_func_ = covered<uint64_t>;
      break;
    case Datatype::FLOAT32:
      covered_func_ = covered<float>;
      break;
    case Datatype::FLOAT64:
      covered_func_ = covered<double>;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      covered_func_ = covered<int64_t>;
      break;
    default:
      covered_func_ = nullptr;
      break;
  }
}

void Dimension::set_overlap_func() {
  switch (type_) {
    case Datatype::INT32:
      overlap_func_ = overlap<int32_t>;
      break;
    case Datatype::INT64:
      overlap_func_ = overlap<int64_t>;
      break;
    case Datatype::INT8:
      overlap_func_ = overlap<int8_t>;
      break;
    case Datatype::UINT8:
      overlap_func_ = overlap<uint8_t>;
      break;
    case Datatype::INT16:
      overlap_func_ = overlap<int16_t>;
      break;
    case Datatype::UINT16:
      overlap_func_ = overlap<uint16_t>;
      break;
    case Datatype::UINT32:
      overlap_func_ = overlap<uint32_t>;
      break;
    case Datatype::UINT64:
      overlap_func_ = overlap<uint64_t>;
      break;
    case Datatype::FLOAT32:
      overlap_func_ = overlap<float>;
      break;
    case Datatype::FLOAT64:
      overlap_func_ = overlap<double>;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      overlap_func_ = overlap<int64_t>;
      break;
    case Datatype::STRING_ASCII:
      assert(var_size());
      overlap_func_ = overlap<char>;
      break;
    default:
      overlap_func_ = nullptr;
      break;
  }
}

void Dimension::set_overlap_ratio_func() {
  switch (type_) {
    case Datatype::INT32:
      overlap_ratio_func_ = overlap_ratio<int32_t>;
      break;
    case Datatype::INT64:
      overlap_ratio_func_ = overlap_ratio<int64_t>;
      break;
    case Datatype::INT8:
      overlap_ratio_func_ = overlap_ratio<int8_t>;
      break;
    case Datatype::UINT8:
      overlap_ratio_func_ = overlap_ratio<uint8_t>;
      break;
    case Datatype::INT16:
      overlap_ratio_func_ = overlap_ratio<int16_t>;
      break;
    case Datatype::UINT16:
      overlap_ratio_func_ = overlap_ratio<uint16_t>;
      break;
    case Datatype::UINT32:
      overlap_ratio_func_ = overlap_ratio<uint32_t>;
      break;
    case Datatype::UINT64:
      overlap_ratio_func_ = overlap_ratio<uint64_t>;
      break;
    case Datatype::FLOAT32:
      overlap_ratio_func_ = overlap_ratio<float>;
      break;
    case Datatype::FLOAT64:
      overlap_ratio_func_ = overlap_ratio<double>;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      overlap_ratio_func_ = overlap_ratio<int64_t>;
      break;
    case Datatype::STRING_ASCII:
      assert(var_size());
      overlap_ratio_func_ = overlap_ratio<char>;
      break;
    default:
      overlap_ratio_func_ = nullptr;
      break;
  }
}

void Dimension::set_split_range_func() {
  switch (type_) {
    case Datatype::INT32:
      split_range_func_ = split_range<int32_t>;
      break;
    case Datatype::INT64:
      split_range_func_ = split_range<int64_t>;
      break;
    case Datatype::INT8:
      split_range_func_ = split_range<int8_t>;
      break;
    case Datatype::UINT8:
      split_range_func_ = split_range<uint8_t>;
      break;
    case Datatype::INT16:
      split_range_func_ = split_range<int16_t>;
      break;
    case Datatype::UINT16:
      split_range_func_ = split_range<uint16_t>;
      break;
    case Datatype::UINT32:
      split_range_func_ = split_range<uint32_t>;
      break;
    case Datatype::UINT64:
      split_range_func_ = split_range<uint64_t>;
      break;
    case Datatype::FLOAT32:
      split_range_func_ = split_range<float>;
      break;
    case Datatype::FLOAT64:
      split_range_func_ = split_range<double>;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      split_range_func_ = split_range<int64_t>;
      break;
    case Datatype::STRING_ASCII:
      split_range_func_ = split_range<char>;
      break;
    default:
      split_range_func_ = nullptr;
      break;
  }
}

void Dimension::set_splitting_value_func() {
  switch (type_) {
    case Datatype::INT32:
      splitting_value_func_ = splitting_value<int32_t>;
      break;
    case Datatype::INT64:
      splitting_value_func_ = splitting_value<int64_t>;
      break;
    case Datatype::INT8:
      splitting_value_func_ = splitting_value<int8_t>;
      break;
    case Datatype::UINT8:
      splitting_value_func_ = splitting_value<uint8_t>;
      break;
    case Datatype::INT16:
      splitting_value_func_ = splitting_value<int16_t>;
      break;
    case Datatype::UINT16:
      splitting_value_func_ = splitting_value<uint16_t>;
      break;
    case Datatype::UINT32:
      splitting_value_func_ = splitting_value<uint32_t>;
      break;
    case Datatype::UINT64:
      splitting_value_func_ = splitting_value<uint64_t>;
      break;
    case Datatype::FLOAT32:
      splitting_value_func_ = splitting_value<float>;
      break;
    case Datatype::FLOAT64:
      splitting_value_func_ = splitting_value<double>;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      splitting_value_func_ = splitting_value<int64_t>;
      break;
    case Datatype::STRING_ASCII:
      assert(var_size());
      splitting_value_func_ = splitting_value<char>;
      break;
    default:
      splitting_value_func_ = nullptr;
      break;
  }
}

void Dimension::set_tile_num_func() {
  switch (type_) {
    case Datatype::INT32:
      tile_num_func_ = tile_num<int32_t>;
      break;
    case Datatype::INT64:
      tile_num_func_ = tile_num<int64_t>;
      break;
    case Datatype::INT8:
      tile_num_func_ = tile_num<int8_t>;
      break;
    case Datatype::UINT8:
      tile_num_func_ = tile_num<uint8_t>;
      break;
    case Datatype::INT16:
      tile_num_func_ = tile_num<int16_t>;
      break;
    case Datatype::UINT16:
      tile_num_func_ = tile_num<uint16_t>;
      break;
    case Datatype::UINT32:
      tile_num_func_ = tile_num<uint32_t>;
      break;
    case Datatype::UINT64:
      tile_num_func_ = tile_num<uint64_t>;
      break;
    case Datatype::FLOAT32:
      tile_num_func_ = tile_num<float>;
      break;
    case Datatype::FLOAT64:
      tile_num_func_ = tile_num<double>;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      tile_num_func_ = tile_num<int64_t>;
      break;
    case Datatype::STRING_ASCII:
      tile_num_func_ = tile_num<char>;
      break;
    default:
      tile_num_func_ = nullptr;
      break;
  }
}

void Dimension::set_value_in_range_func() {
  switch (type_) {
    case Datatype::INT32:
      value_in_range_func_ = value_in_range<int32_t>;
      break;
    case Datatype::INT64:
      value_in_range_func_ = value_in_range<int64_t>;
      break;
    case Datatype::INT8:
      value_in_range_func_ = value_in_range<int8_t>;
      break;
    case Datatype::UINT8:
      value_in_range_func_ = value_in_range<uint8_t>;
      break;
    case Datatype::INT16:
      value_in_range_func_ = value_in_range<int16_t>;
      break;
    case Datatype::UINT16:
      value_in_range_func_ = value_in_range<uint16_t>;
      break;
    case Datatype::UINT32:
      value_in_range_func_ = value_in_range<uint32_t>;
      break;
    case Datatype::UINT64:
      value_in_range_func_ = value_in_range<uint64_t>;
      break;
    case Datatype::FLOAT32:
      value_in_range_func_ = value_in_range<float>;
      break;
    case Datatype::FLOAT64:
      value_in_range_func_ = value_in_range<double>;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      value_in_range_func_ = value_in_range<int64_t>;
      break;
    default:
      value_in_range_func_ = nullptr;
      break;
  }
}

}  // namespace sm

}  // namespace tiledb
