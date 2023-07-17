/**
 * @file   dimension.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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

#include "dimension.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/type/range/range.h"

#include <bitset>
#include <cassert>
#include <cmath>
#include <iostream>

using namespace tiledb::common;
using namespace tiledb::type;

tiledb::common::blank<tiledb::sm::Dimension>::blank()
    : tiledb::sm::Dimension{"", tiledb::sm::Datatype::INT32} {
}

namespace tiledb {
namespace sm {

class DimensionException : public StatusException {
 public:
  explicit DimensionException(const std::string& message)
      : StatusException("Dimension", message) {
  }
};

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Dimension::Dimension(const std::string& name, Datatype type)
    : name_(name)
    , type_(type) {
  ensure_datatype_is_supported(type_);
  cell_val_num_ = (datatype_is_string(type)) ? constants::var_num : 1;
  set_ceil_to_tile_func();
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
  set_relevant_ranges_func();
  set_covered_vec_func();
  set_split_range_func();
  set_splitting_value_func();
  set_tile_num_func();
  set_map_to_uint64_2_func();
  set_map_from_uint64_func();
  set_smaller_than_func();
}

Dimension::Dimension(
    const std::string& name,
    Datatype type,
    uint32_t cell_val_num,
    const Range& domain,
    const FilterPipeline& filter_pipeline,
    const ByteVecValue& tile_extent)
    : cell_val_num_(cell_val_num)
    , domain_(domain)
    , filters_(filter_pipeline)
    , name_(name)
    , tile_extent_(tile_extent)
    , type_(type) {
  ensure_datatype_is_supported(type_);
  set_ceil_to_tile_func();
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
  set_relevant_ranges_func();
  set_covered_vec_func();
  set_split_range_func();
  set_splitting_value_func();
  set_tile_num_func();
  set_map_to_uint64_2_func();
  set_map_from_uint64_func();
  set_smaller_than_func();
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
        Status_DimensionError("Cannot set non-variable number of values per "
                              "coordinate for a string dimension"));
  if (!datatype_is_string(type_) && cell_val_num != 1)
    return LOG_STATUS(Status_DimensionError(
        "Cannot set number of values per coordinate; Currently only one value "
        "per coordinate is supported"));

  cell_val_num_ = cell_val_num;

  return Status::Ok();
}

shared_ptr<Dimension> Dimension::deserialize(
    Deserializer& deserializer, uint32_t version, Datatype type) {
  Status st;
  // Load dimension name
  auto dimension_name_size = deserializer.read<uint32_t>();
  std::string name(
      deserializer.get_ptr<char>(dimension_name_size), dimension_name_size);

  Datatype datatype;
  uint32_t cell_val_num;
  FilterPipeline filter_pipeline;
  // Applicable only to version >= 5
  if (version >= 5) {
    // Load type
    auto type = deserializer.read<uint8_t>();
    datatype = (Datatype)type;

    // Load cell_val_num_
    cell_val_num = deserializer.read<uint32_t>();

    // Load filter pipeline
    filter_pipeline =
        FilterPipeline::deserialize(deserializer, version, datatype);
  } else {
    datatype = type;
    cell_val_num = (datatype_is_string(datatype)) ? constants::var_num : 1;
  }

  // Load domain
  uint64_t domain_size = 0;
  if (version >= 5) {
    domain_size = deserializer.read<uint64_t>();
  } else {
    domain_size = 2 * datatype_size(datatype);
  }
  Range domain;
  if (domain_size != 0) {
    domain = Range(deserializer.get_ptr<uint8_t>(domain_size), domain_size);
  }

  ByteVecValue tile_extent;
  // Load tile extent
  tile_extent.assign_as_void();
  auto null_tile_extent = deserializer.read<uint8_t>();
  if (null_tile_extent == 0) {
    tile_extent.resize(datatype_size(datatype));
    deserializer.read(tile_extent.data(), datatype_size(datatype));
  }

  return tiledb::common::make_shared<Dimension>(
      HERE(),
      name,
      datatype,
      cell_val_num,
      domain,
      filter_pipeline,
      tile_extent);
}

const Range& Dimension::domain() const {
  return domain_;
}

void Dimension::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;
  // Retrieve domain and tile extent strings
  std::string domain_s = type::range_str(domain_, type_);
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
  assert(dim->tile_extent());

  auto tile_extent = *(const T*)dim->tile_extent().data();
  auto dim_dom = (const T*)dim->domain().data();
  auto r_t = (const T*)r.data();

  T mid = tile_coord_low(tile_num + 1, r_t[0], tile_extent);
  uint64_t div = tile_idx(mid, dim_dom[0], tile_extent);
  T floored_mid = tile_coord_low(div, dim_dom[0], tile_extent);
  assert(v != nullptr);
  v->assign_as<T>(
      (std::is_integral<T>::value) ?
          floored_mid - 1 :
          static_cast<T>(
              std::nextafter(floored_mid, std::numeric_limits<T>::lowest())));
}

void Dimension::ceil_to_tile(
    const Range& r, uint64_t tile_num, ByteVecValue* v) const {
  assert(ceil_to_tile_func_ != nullptr);
  ceil_to_tile_func_(this, r, tile_num, v);
}

template <class T>
bool Dimension::coincides_with_tiles(const Dimension* dim, const Range& r) {
  assert(dim != nullptr);
  assert(!r.empty());
  assert(dim->tile_extent());

  auto dim_domain = (const T*)dim->domain().data();
  auto tile_extent = *(const T*)dim->tile_extent().data();
  auto d = (const T*)r.data();
  auto rounded_1 = Dimension::round_to_tile(d[0], dim_domain[0], tile_extent);
  auto rounded_2 =
      Dimension::round_to_tile(T(d[1] + 1), dim_domain[0], tile_extent);
  return (rounded_1 == d[0]) && (rounded_2 == T(d[1] + 1));
}

bool Dimension::coincides_with_tiles(const Range& r) const {
  assert(coincides_with_tiles_func_ != nullptr);
  return coincides_with_tiles_func_(this, r);
}

template <class T>
Range Dimension::compute_mbr(const WriterTile& tile) {
  auto cell_num = tile.cell_num();
  assert(cell_num > 0);

  void* tile_buffer = tile.data();
  assert(tile_buffer != nullptr);

  // Initialize MBR with the first tile values
  const T* const data = static_cast<T*>(tile_buffer);
  T res[] = {data[0], data[0]};
  Range mbr{res, sizeof(res)};

  // Expand the MBR with the rest tile values
  for (uint64_t c = 1; c < cell_num; ++c)
    expand_range_v<T>(&data[c], &mbr);

  return mbr;
}

Range Dimension::compute_mbr(const WriterTile& tile) const {
  assert(compute_mbr_func_ != nullptr);
  return compute_mbr_func_(tile);
}

template <>
Range Dimension::compute_mbr_var<char>(
    const WriterTile& tile_off, const WriterTile& tile_val) {
  auto d_val_size = tile_val.size();
  auto cell_num = tile_off.cell_num();
  assert(cell_num > 0);

  void* tile_buffer_off = tile_off.data();
  assert(tile_buffer_off != nullptr);

  void* tile_buffer_val = tile_val.data();
  assert(tile_buffer_val != nullptr);

  uint64_t* const d_off = static_cast<uint64_t*>(tile_buffer_off);
  char* const d_val = static_cast<char*>(tile_buffer_val);

  // Initialize MBR with the first tile values
  auto size_0 = (cell_num == 1) ? d_val_size : d_off[1];
  Range mbr{d_val, size_0, d_val, size_0};

  // Expand the MBR with the rest tile values
  for (uint64_t c = 1; c < cell_num; ++c) {
    auto size =
        (c == cell_num - 1) ? d_val_size - d_off[c] : d_off[c + 1] - d_off[c];
    expand_range_var_v(&d_val[d_off[c]], size, &mbr);
  }

  return mbr;
}

Range Dimension::compute_mbr_var(
    const WriterTile& tile_off, const WriterTile& tile_val) const {
  assert(compute_mbr_var_func_ != nullptr);
  return compute_mbr_var_func_(tile_off, tile_val);
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

  std::string start(r->start_str());
  std::string end(r->end_str());
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

  auto r1_start = r1.start_str();
  auto r1_end = r1.end_str();

  auto r2_start = r2->start_str();
  auto r2_end = r2->end_str();

  std::string min((r1_start < r2_start) ? r1_start : r2_start);
  std::string max((r1_end < r2_end) ? r2_end : r1_end);

  r2->set_str_range(min, max);
}

template <class T>
void Dimension::expand_to_tile(const Dimension* dim, Range* range) {
  assert(dim != nullptr);
  assert(!range->empty());

  // Applicable only to regular tiles and integral domains
  if (!dim->tile_extent() || !std::is_integral<T>::value)
    return;

  auto tile_extent = *(const T*)dim->tile_extent().data();
  auto dim_dom = (const T*)dim->domain().data();
  auto r = (const T*)range->data();
  auto tile_idx1 = tile_idx(r[0], dim_dom[0], tile_extent);
  auto tile_idx2 = tile_idx(r[1], dim_dom[0], tile_extent);

  T res[2];
  res[0] = tile_coord_low(tile_idx1, dim_dom[0], tile_extent);
  res[1] = tile_coord_high(tile_idx2, dim_dom[0], tile_extent);

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
    ss << "Coordinate ";
    // Account for precision in floating point types. Arbitrarily set
    // the precision to up to 2 decimal places.
    if (dim->type_ == Datatype::FLOAT32 || dim->type_ == Datatype::FLOAT64) {
      ss << std::setprecision(std::numeric_limits<T>::digits10 + 1);
    }
    ss << *coord_t << " is out of domain bounds [" << domain[0] << ", "
       << domain[1] << "] on dimension '" << dim->name() << "'";
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
    return Status_DimensionError(err_msg);
  return Status::Ok();
}

template <>
bool Dimension::covered<char>(const Range& r1, const Range& r2) {
  auto r1_start = r1.start_str();
  auto r1_end = r1.end_str();
  auto r2_start = r2.start_str();
  auto r2_end = r2.end_str();

  return r1_start >= r2_start && r1_end <= r2_end;
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
  auto r1_start = r1.start_str();
  auto r1_end = r1.end_str();
  auto r2_start = r2.start_str();
  auto r2_end = r2.end_str();

  auto r1_after_r2 = r1_start > r2_end;
  auto r2_after_r1 = r2_start > r1_end;

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
    pref_size = stdx::string::common_prefix_size(r2_start, r2_end);
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
  // Verify that we have two intervals
  assert(!r1.empty());
  assert(!r2.empty());
  auto d1 = (const T*)r1.data();
  auto d2 = (const T*)r2.data();
  const auto r1_low = d1[0];
  const auto r1_high = d1[1];
  auto r2_low = d2[0];
  auto r2_high = d2[1];
  assert(r1_low <= r1_high);
  assert(r2_low <= r2_high);

  // Special case: No overlap, intervals are disjoint
  if (r1_low > r2_high || r1_high < r2_low)
    return 0.0;
  // Special case: All overlap, interval r2 is a subset of interval r1
  if (r1_low <= r2_low && r2_high <= r1_high)
    return 1.0;
  /*
   * At this point we know that r2_low < r2_high, because we would have returned
   * by now otherwise. Note that for floating point types, however, we cannot
   * conclude that r2_high - r2_low > 0 because of the possibility of underflow.
   */
  auto intersection_low = std::max(r1_low, r2_low);
  auto intersection_high = std::min(r1_high, r2_high);
  /*
   * The intersection is a proper subset of interval r1, either the upper bounds
   * are distinct, or lower bounds are distinct, or both. This means that any
   * result has to be strictly greater than zero and strictly less than 1.
   */
  /*
   * Guard against overflow. If the outer interval has a bound with an absolute
   * value that's "too large", meaning that it might lead to an overflow, we
   * apply a shrinking transformation that preserves the ratio. We only need to
   * check the outer interval because the intersection is strictly smaller. For
   * unsigned types we only need to check the upper bound, since the lower bound
   * is zero. For signed types we need to check both.
   */
  const T safe_upper = std::nextafter(std::numeric_limits<T>::max() / 2, 0);
  bool unsafe = safe_upper < r2_high;
  if (std::numeric_limits<T>::is_signed) {
    const T safe_lower =
        std::nextafter(std::numeric_limits<T>::lowest() / 2, 0);
    unsafe = unsafe || r2_low < safe_lower;
  }
  if (unsafe) {
    r2_low /= 2;
    r2_high /= 2;
    intersection_low /= 2;
    intersection_high /= 2;
  }

  // Compute ratio
  auto numerator = intersection_high - intersection_low;
  auto denominator = r2_high - r2_low;
  if (std::numeric_limits<T>::is_integer) {
    // integer types count elements; they don't measure length
    numerator += 1;
    denominator += 1;
  } else {
    if (denominator == 0) {
      /*
       * If the variable `denominator` is zero, it's the result of rounding,
       * since checking the "disjoint" and "subset" cases above has ensured that
       * the endpoints of the denominator interval are distinct. Thus we have no
       * easy-to-access information about the true quotient value, but
       * mathematically it must be between 0 and 1, so we need either to return
       * some value between 0 and 1 or to throw an exception stating that we
       * don't support this case.
       *
       * The variable `denominator` is larger than the variable `numerator`, so
       * the numerator is also be zero. We could extract some information from
       * the floating-point representation itself, but that's a lot of work that
       * doesn't seem justified at present. Throwing an exception would be
       * better on principle, but the code in its current state now would not
       * deal with that gracefully. As a compromise, therefore, we return a
       * valid but non-computed value. It's a defect, but one that will rarely
       * if ever arise in practice.
       */
      return 0.5;
    }
    // The rounded-to-zero numerator is handled in the general case below.
  }
  auto ratio = (double)numerator / denominator;
  // Round away from the endpoints if needed.
  if (ratio == 0.0) {
    ratio = std::nextafter(0, 1);
  } else if (ratio == 1.0) {
    ratio = std::nextafter(1, 0);
  }
  assert(0.0 < ratio && ratio < 1.0);
  return ratio;
}

double Dimension::overlap_ratio(const Range& r1, const Range& r2) const {
  assert(overlap_ratio_func_ != nullptr);
  return overlap_ratio_func_(r1, r2);
}

void Dimension::relevant_ranges(
    const NDRange& ranges,
    const Range& mbr,
    std::vector<uint64_t>& relevant_ranges) const {
  assert(relevant_ranges_func_ != nullptr);
  return relevant_ranges_func_(ranges, mbr, relevant_ranges);
}

template <>
void Dimension::relevant_ranges<char>(
    const NDRange& ranges,
    const Range& mbr,
    std::vector<uint64_t>& relevant_ranges) {
  const auto& mbr_start = mbr.start_str();
  const auto& mbr_end = mbr.end_str();

  // Binary search to find the first range containing the start mbr.
  auto it = std::lower_bound(
      ranges.begin(),
      ranges.end(),
      mbr_start,
      [&](const Range& a, const std::string_view& value) {
        return a.end_str() < value;
      });

  // If we have no ranges just exit early
  if (it == ranges.end()) {
    return;
  }

  // Set start index
  const uint64_t start_range = std::distance(ranges.begin(), it);

  // Find upper bound to end comparisons. Finding this early allows avoiding the
  // conditional exit in the for loop below
  auto it2 = std::lower_bound(
      it,
      ranges.end(),
      mbr_end,
      [&](const Range& a, const std::string_view& value) {
        return a.start_str() <= value;
      });

  // Set end index
  const uint64_t end_range = std::distance(it, it2) + start_range;

  // Loop over only potential relevant ranges
  for (uint64_t r = start_range; r < end_range; ++r) {
    const auto& r1_start = ranges[r].start_str();
    const auto& r1_end = ranges[r].end_str();

    if (r1_start <= mbr_end && mbr_start <= r1_end) {
      relevant_ranges.emplace_back(r);
    }
  }
}

template <class T>
void Dimension::relevant_ranges(
    const NDRange& ranges,
    const Range& mbr,
    std::vector<uint64_t>& relevant_ranges) {
  const auto mbr_data = (const T*)mbr.start_fixed();
  const auto mbr_start = mbr_data[0];
  const auto mbr_end = mbr_data[1];

  // Binary search to find the first range containing the start mbr.
  auto it = std::lower_bound(
      ranges.begin(),
      ranges.end(),
      mbr_start,
      [&](const Range& a, const T value) {
        return ((const T*)a.start_fixed())[1] < value;
      });

  if (it == ranges.end()) {
    return;
  }

  // Set start index
  const uint64_t start_range = std::distance(ranges.begin(), it);

  // Find upper bound to end comparisons. Finding this early allows avoiding the
  // conditional exit in the for loop below
  auto it2 = std::lower_bound(
      it, ranges.end(), mbr_end, [&](const Range& a, const T value) {
        return ((const T*)a.start_fixed())[0] <= value;
      });

  // Set end index
  const uint64_t end_range = std::distance(it, it2) + start_range;

  // Loop over only potential relevant ranges
  for (uint64_t r = start_range; r < end_range; ++r) {
    const auto d1 = (const T*)ranges[r].start_fixed();

    if ((d1[0] <= mbr_end && d1[1] >= mbr_start)) {
      relevant_ranges.emplace_back(r);
    }
  }
}

std::vector<bool> Dimension::covered_vec(
    const NDRange& ranges,
    const Range& mbr,
    const std::vector<uint64_t>& relevant_ranges) const {
  assert(covered_vec_func_ != nullptr);
  return covered_vec_func_(ranges, mbr, relevant_ranges);
}

template <>
std::vector<bool> Dimension::covered_vec<char>(
    const NDRange& ranges,
    const Range& mbr,
    const std::vector<uint64_t>& relevant_ranges) {
  const auto& range_start = mbr.start_str();
  const auto& range_end = mbr.end_str();

  std::vector<bool> covered;
  covered.resize(relevant_ranges.size());
  for (uint64_t i = 0; i < relevant_ranges.size(); i++) {
    auto r = relevant_ranges[i];
    auto r2_start = ranges[r].start_str();
    auto r2_end = ranges[r].end_str();

    covered[i] = range_start >= r2_start && range_end <= r2_end;
  }

  return covered;
}

template <class T>
std::vector<bool> Dimension::covered_vec(
    const NDRange& ranges,
    const Range& mbr,
    const std::vector<uint64_t>& relevant_ranges) {
  auto d1 = (const T*)mbr.start_fixed();

  std::vector<bool> covered;
  covered.resize(relevant_ranges.size());
  for (uint64_t i = 0; i < relevant_ranges.size(); i++) {
    auto r = relevant_ranges[i];
    auto d2 = (const T*)ranges[r].start_fixed();

    covered[i] = d1[0] >= d2[0] && d1[1] <= d2[1];
  }

  return covered;
}

template <>
void Dimension::split_range<char>(
    const Range& r, const ByteVecValue& v, Range* r1, Range* r2) {
  assert(v);
  assert(r1 != nullptr);
  assert(r2 != nullptr);

  // First range
  auto min_string = std::string("\x0", 1);
  std::string new_r1_start(!r.start_str().empty() ? r.start_str() : min_string);
  std::string new_r1_end(v.rvalue_as<std::string>());
  auto new_r1_end_size = (int)new_r1_end.size();
  int pos;
  for (pos = 0; pos < new_r1_end_size; ++pos) {
    if ((int)(signed char)new_r1_end[pos] < 0) {
      new_r1_end[pos] = 127;
      new_r1_end.resize(pos + 1);
      break;
    }
  }
  assert(pos < new_r1_end_size);  // One negative char must be present
  r1->set_str_range(new_r1_start, new_r1_end);

  // Second range
  auto new_r2_start = v.rvalue_as<std::string>();
  // The following will make "a b -1 -4 0" -> "a c"
  for (pos = 0; pos < new_r1_end_size; ++pos) {
    if ((int)(signed char)new_r2_start[pos] < 0)
      break;
  }
  do {
    assert(pos != 0);
    new_r2_start[pos] = 0;
    new_r2_start[--pos]++;
  } while (pos >= 0 && (int)(signed char)new_r2_start[pos] < 0);
  new_r2_start.resize(pos + 1);

  auto max_string = std::string("\x7F", 1);
  std::string new_r2_end(!r.end_str().empty() ? r.end_str() : max_string);

  assert(new_r2_start > new_r1_end);
  assert(new_r2_start <= new_r2_end);
  r2->set_str_range(new_r2_start, new_r2_end);

  // Set the depth of the split ranges to +1 the depth of
  // the range they were split from.
  r1->set_partition_depth(r.partition_depth() + 1);
  r2->set_partition_depth(r.partition_depth() + 1);
}

template <class T>
void Dimension::split_range(
    const Range& r, const ByteVecValue& v, Range* r1, Range* r2) {
  assert(!r.empty());
  assert(v);
  assert(r1 != nullptr);
  assert(r2 != nullptr);

  auto max = std::numeric_limits<T>::max();
  bool int_domain = std::is_integral<T>::value;
  auto r_t = (const T*)r.data();
  auto v_t = *(const T*)(v.data());
  assert(v_t >= r_t[0]);
  assert(v_t < r_t[1]);

  T ret[2];
  ret[0] = r_t[0];
  ret[1] = v_t;
  r1->set_range(ret, sizeof(ret));
  ret[0] = int_domain ? (v_t + 1) : static_cast<T>(std::nextafter(v_t, max));
  ret[1] = r_t[1];
  r2->set_range(ret, sizeof(ret));

  // Set the depth of the split ranges to +1 the depth of
  // the range they were split from.
  r1->set_partition_depth(r.partition_depth() + 1);
  r2->set_partition_depth(r.partition_depth() + 1);
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
  if (!r.empty() && r.unary()) {
    *unsplittable = true;
    return;
  }

  *unsplittable = false;

  // Find position to split
  auto start = r.start_str();
  auto end = r.end_str();
  auto pref_size = stdx::string::common_prefix_size(start, end);

  // String ranges are infinitely splittable. We define a fixed
  // limit on how deep we will split a user-given range. If we
  // reach this limit, we will treat the range as unsplittable.
  if (r.partition_depth() >= constants::max_string_dim_split_depth) {
    *unsplittable = true;
    return;
  }

  // Check unsplittable
  if (!end.empty() && end[pref_size] == 0) {
    *unsplittable = true;
    return;
  }

  // 127 is the maximum ascii number
  uint8_t end_pref = end.empty() ? 127 : (uint8_t)end[pref_size];
  auto start_c = (start.size() == pref_size) ? 0 : start[pref_size];
  auto split_v = (end_pref - start_c) / 2;
  std::string split_str(start.substr(0, pref_size));
  split_str += (char)(start_c + split_v);
  split_str += "\x80";
  assert(split_str >= start);

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

  // Floating-point template specializations for this routine exist below.
  // All remaining template types are handled in this implementation,
  // where all template types are integers. We will calculate the split value
  // with the following expression: `r_t[0] + (r_t[1] - r_t[0]) / 2`. To
  // prevent overflow in the `(r_t[1] - r_t[0]) / 2` sub-expression, we will
  // perform this entire sub-expression with a 65-bit bitset.
  std::bitset<65> r_t0(r_t[0]);
  std::bitset<65> r_t1(r_t[1]);

  // If `T` is signed, respect two's complement on the high bit.
  if (std::is_signed<T>::value) {
    r_t0[64] = r_t0[63];
    r_t1[64] = r_t1[63];
  }

  // Subtract `r_t0` from `r_t1`.
  while (r_t0 != 0) {
    const std::bitset<65> carry = (~r_t1) & r_t0;
    r_t1 = r_t1 ^ r_t0;
    r_t0 = carry << 1;
  }

  // Divide by 2.
  r_t1 = r_t1 >> 1;

  // Truncate the overflow bit. This will only occur if `T` is
  // a signed integer and the result of `(r_t[1] - r_t[0]) / 2`
  // is negative. This preserves signed representation when
  // converting back to 64-bits in the following invocation
  // of `std::bitset<65>::to_ulong`.
  r_t1 = r_t1 & std::bitset<65>(UINT64_MAX);

  // Cast the intermediate result back to type `T` to calculate
  // the split value.
  const T sp = r_t[0] + static_cast<T>(r_t1.to_ullong());

  v->assign_as<T>(sp);
  *unsplittable = !std::memcmp(&sp, &r_t[1], sizeof(T));
}

template <>
void Dimension::splitting_value<float>(
    const Range& r, ByteVecValue* v, bool* unsplittable) {
  assert(!r.empty());
  assert(v != nullptr);
  assert(unsplittable != nullptr);

  auto r_t = (const float*)r.data();

  // Cast `r_t` elements to `double` to prevent overflow before
  // dividing by 2.
  const float sp = r_t[0] + ((double)r_t[1] - (double)r_t[0]) / 2;

  v->assign_as<float>(sp);
  *unsplittable = !std::memcmp(&sp, &r_t[1], sizeof(float));
}

template <>
void Dimension::splitting_value<double>(
    const Range& r, ByteVecValue* v, bool* unsplittable) {
  assert(!r.empty());
  assert(v != nullptr);
  assert(unsplittable != nullptr);

  auto r_t = (const double*)r.data();
  const double sp = r_t[0] + (r_t[1] / 2 - r_t[0] / 2);

  v->assign_as<double>(sp);
  *unsplittable = !std::memcmp(&sp, &r_t[1], sizeof(double));
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
  if (!dim->tile_extent())
    return 1;

  auto tile_extent = *(const T*)dim->tile_extent().data();
  auto dim_dom = (const T*)dim->domain().data();
  auto r = (const T*)range.data();

  uint64_t start = tile_idx(r[0], dim_dom[0], tile_extent);
  uint64_t end = tile_idx(r[1], dim_dom[0], tile_extent);
  return end - start + 1;
}

uint64_t Dimension::tile_num(const Range& range) const {
  assert(tile_num_func_ != nullptr);
  return tile_num_func_(this, range);
}

uint64_t Dimension::map_to_uint64(
    const void* coord,
    uint64_t coord_size,
    int bits,
    uint64_t max_bucket_val) const {
  assert(map_to_uint64_2_func_ != nullptr);
  return map_to_uint64_2_func_(this, coord, coord_size, bits, max_bucket_val);
}

template <class T>
uint64_t Dimension::map_to_uint64_2(
    const Dimension* dim,
    const void* coord,
    uint64_t,  // coord_size
    int,       // bits
    uint64_t max_bucket_val) {
  assert(dim != nullptr);
  assert(coord != nullptr);
  assert(!dim->domain().empty());

  double dom_start_T = *(const T*)dim->domain().start_fixed();
  double dom_end_T = *(const T*)dim->domain().end_fixed();
  auto dom_range_T = dom_end_T - dom_start_T;
  auto norm_coord_T = *(const T*)coord - dom_start_T;
  return (norm_coord_T / dom_range_T) * max_bucket_val;
}

template <>
uint64_t Dimension::map_to_uint64_2<char>(
    const Dimension*,
    const void* coord,
    uint64_t coord_size,
    int bits,
    uint64_t) {
  assert(coord != nullptr);

  auto v_str = (const char*)coord;
  auto v_str_size = coord_size;

  // The following will place up to the first 8 characters of the string
  // in the uint64 value to be returned. For instance, "cat" will be
  // "cat\0\0\0\0\0" inside the 8-byte uint64 value `ret`.
  uint64_t ret = 0;
  for (uint64_t i = 0; i < 8; ++i) {
    ret <<= 8;  // Shift by one byte
    if (i < v_str_size)
      ret |= (uint64_t)v_str[i];  // Add next character (if exists)
  }

  // Shift to fit in given number of bits
  return ret >> (64 - bits);
}

ByteVecValue Dimension::map_from_uint64(
    uint64_t value, int bits, uint64_t max_bucket_val) const {
  assert(map_from_uint64_func_ != nullptr);
  return map_from_uint64_func_(this, value, bits, max_bucket_val);
}

template <class T>
ByteVecValue Dimension::map_from_uint64(
    const Dimension* dim, uint64_t value, int bits, uint64_t max_bucket_val) {
  assert(dim != nullptr);
  assert(!dim->domain().empty());
  (void)bits;  // Not needed here

  ByteVecValue ret(sizeof(T));

  // Add domain start
  auto value_T = static_cast<T>(value);
  value_T += *(const T*)dim->domain().start_fixed();

  auto dom_start_T = *(const T*)dim->domain().start_fixed();
  auto dom_end_T = *(const T*)dim->domain().end_fixed();
  double dom_range_T = dom_end_T - dom_start_T;

  // Essentially take the largest value in the bucket
  if (std::is_integral<T>::value) {  // Integers
    T norm_coord_T =
        std::ceil(((value + 1) / (double)max_bucket_val) * dom_range_T - 1);
    T coord_T = norm_coord_T + dom_start_T;
    std::memcpy(ret.data(), &coord_T, sizeof(T));
  } else {  // Floating point types
    T norm_coord_T = ((value + 1) / (double)max_bucket_val) * dom_range_T;
    norm_coord_T =
        std::nextafter(norm_coord_T, std::numeric_limits<T>::lowest());
    T coord_T = norm_coord_T + dom_start_T;
    std::memcpy(ret.data(), &coord_T, sizeof(T));
  }

  return ret;
}

template <>
ByteVecValue Dimension::map_from_uint64<char>(
    const Dimension* dim, uint64_t value, int bits, uint64_t max_bucket_val) {
  assert(dim != nullptr);
  (void)dim;
  (void)max_bucket_val;  // Not needed here

  std::vector<uint8_t> ret(sizeof(uint64_t));  // 8 bytes

  uint64_t ret_uint64 = (value << (64 - bits));
  int ret_c;
  for (size_t i = 0; i < 8; ++i) {  // Reverse ret_uint64
    ret_c = (int)((const char*)&ret_uint64)[7 - i];
    if (ret_c < 0) {
      ret[i] = 128;
      ret.resize(i + 1);
      break;
    } else {
      ret[i] = ret_c;
    }
  }
  if (ret.back() != 128)
    ret.push_back(128);

  return ByteVecValue(std::move(ret));
}

bool Dimension::smaller_than(
    const ByteVecValue& value, const Range& range) const {
  assert(smaller_than_func_ != nullptr);
  return smaller_than_func_(this, value, range);
}

template <class T>
bool Dimension::smaller_than(
    const Dimension* dim, const ByteVecValue& value, const Range& range) {
  assert(dim != nullptr);
  (void)dim;
  assert(value);

  auto value_T = *(const T*)(value.data());
  auto range_start_T = *(const T*)range.start_fixed();
  return value_T < range_start_T;
}

template <>
bool Dimension::smaller_than<char>(
    const Dimension* dim, const ByteVecValue& value, const Range& range) {
  assert(dim != nullptr);
  assert(value);
  (void)dim;

  auto value_str = value.rvalue_as<std::string>();
  auto range_start_str = range.start_str();

  // If the range start is empty, then it is essentially -inf
  if (range_start_str.empty())
    return false;

  return value_str < range_start_str;
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
void Dimension::serialize(Serializer& serializer, uint32_t version) const {
  // Sanity check
  auto is_str = datatype_is_string(type_);
  assert(is_str || !domain_.empty());

  // Write dimension name
  auto dimension_name_size = (uint32_t)name_.size();
  serializer.write<uint32_t>(dimension_name_size);
  serializer.write(name_.data(), dimension_name_size);

  // Applicable only to version >= 5
  if (version >= 5) {
    // Write type
    auto type = (uint8_t)type_;
    serializer.write<uint8_t>(type);

    // Write cell_val_num_
    serializer.write<uint32_t>(cell_val_num_);

    // Write filter pipeline
    filters_.serialize(serializer);
  }

  // Write domain and tile extent
  uint64_t domain_size = (is_str) ? 0 : 2 * coord_size();
  serializer.write<uint64_t>(domain_size);
  serializer.write(domain_.data(), domain_size);

  auto null_tile_extent = (uint8_t)(tile_extent_ ? 0 : 1);
  serializer.write<uint8_t>(null_tile_extent);
  if (tile_extent_) {
    serializer.write(tile_extent_.data(), tile_extent_.size());
  }
}

Status Dimension::set_domain(const void* domain) {
  if (type_ == Datatype::STRING_ASCII) {
    if (domain == nullptr)
      return Status::Ok();
    return LOG_STATUS(Status_DimensionError(
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

Status Dimension::set_domain_unsafe(const void* domain) {
  domain_ = Range(domain, 2 * coord_size());

  return Status::Ok();
}

void Dimension::set_filter_pipeline(const FilterPipeline& pipeline) {
  FilterPipeline::check_filter_types(pipeline, type_, var_size());
  filters_ = pipeline;
}

Status Dimension::set_tile_extent(const void* tile_extent) {
  if (type_ == Datatype::STRING_ASCII) {
    if (tile_extent == nullptr)
      return Status::Ok();
    return LOG_STATUS(Status_DimensionError(
        std::string("Setting the tile extent to a dimension with type '") +
        datatype_str(type_) + "' is not supported"));
  }

  ByteVecValue te;
  if (tile_extent != nullptr) {
    auto size = coord_size();
    te.resize(size);
    std::memcpy(te.data(), tile_extent, size);
  }

  return set_tile_extent(te);
}

Status Dimension::set_tile_extent(const ByteVecValue& tile_extent) {
  if (type_ == Datatype::STRING_ASCII) {
    if (!tile_extent)
      return Status::Ok();
    return LOG_STATUS(Status_DimensionError(
        std::string("Setting the tile extent to a dimension with type '") +
        datatype_str(type_) + "' is not supported"));
  }
  if (domain_.empty())
    return LOG_STATUS(Status_DimensionError(
        "Cannot set tile extent; Domain must be set first"));

  tile_extent_ = tile_extent;

  return check_tile_extent();
}

Status Dimension::set_null_tile_extent_to_range() {
  switch (type_) {
    case Datatype::INT8:
      return set_null_tile_extent_to_range<int8_t>();
    case Datatype::UINT8:
      return set_null_tile_extent_to_range<uint8_t>();
    case Datatype::INT16:
      return set_null_tile_extent_to_range<int16_t>();
    case Datatype::UINT16:
      return set_null_tile_extent_to_range<uint16_t>();
    case Datatype::INT32:
      return set_null_tile_extent_to_range<int32_t>();
    case Datatype::UINT32:
      return set_null_tile_extent_to_range<uint32_t>();
    case Datatype::INT64:
      return set_null_tile_extent_to_range<int64_t>();
    case Datatype::UINT64:
      return set_null_tile_extent_to_range<uint64_t>();
    case Datatype::FLOAT32:
      return set_null_tile_extent_to_range<float>();
    case Datatype::FLOAT64:
      return set_null_tile_extent_to_range<double>();
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return set_null_tile_extent_to_range<int64_t>();
    case Datatype::STRING_ASCII:
      return Status::Ok();  // Do nothing for strings
    default:
      return LOG_STATUS(
          Status_DimensionError("Cannot set null tile extent to domain range; "
                                "Invalid dimension domain type"));
  }

  assert(false);
  return LOG_STATUS(
      Status_DimensionError("Cannot set null tile extent to domain range; "
                            "Unsupported dimension type"));
}

template <class T>
Status Dimension::set_null_tile_extent_to_range() {
  // Applicable only to null extents
  if (tile_extent_)
    return Status::Ok();

  // Check empty domain
  if (domain_.empty())
    return LOG_STATUS(Status_DimensionError(
        "Cannot set tile extent to domain range; Domain not set"));

  // Calculate new tile extent equal to domain range
  auto domain = (const T*)domain_.data();

  // For integral domain, we need to add 1, check for overflow before doing
  // anything
  if (std::is_integral<T>::value) {
    if (domain[0] == std::numeric_limits<T>::min() &&
        domain[1] == std::numeric_limits<T>::max()) {
      return LOG_STATUS(Status_DimensionError(
          "Cannot set null tile extent to domain range; "
          "Domain range exceeds domain type max numeric limit"));
    }
  }

  T tile_extent = domain[1] - domain[0];

  // We need to add 1 for integral domains
  if (std::is_integral<T>::value) {
    ++tile_extent;
    // After this, tile_extent = domain[1] - domain[0] + 1, which is the correct
    // domain range
  }

  tile_extent_.assign_as<T>(tile_extent);
  return Status::Ok();
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return check_domain<int64_t>();
    default:
      return LOG_STATUS(Status_DimensionError(
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return check_tile_extent<int64_t>();
    default:
      throw DimensionException(
          "Tile extent check failed on dimension '" + name() +
          "'; Invalid dimension domain type");
  }
}

template <class T>
Status Dimension::check_tile_extent() const {
  if (domain_.empty())
    throw DimensionException(
        "Tile extent check failed on dimension '" + name() +
        "'; Domain not set");

  if (!tile_extent_)
    return Status::Ok();

  auto tile_extent = (const T*)tile_extent_.data();
  auto domain = (const T*)domain_.data();
  bool is_int = std::is_integral<T>::value;

  // Check if tile extent exceeds domain
  if (!is_int) {
    // Check if tile extent is negative or 0
    if (*tile_extent <= 0)
      throw DimensionException(
          "Tile extent check failed on dimension '" + name() +
          "'; Tile extent must be greater than 0");

    if (*tile_extent > (domain[1] - domain[0] + 1))
      throw DimensionException(
          "Tile extent check failed on dimension '" + name() +
          "'; Tile extent " + std::to_string(*tile_extent) +
          " exceeds range on dimension domain [" + std::to_string(domain[0]) +
          ", " + std::to_string(domain[1]) + "]");
  } else {
    // Check if tile extent is 0
    if (*tile_extent == 0)
      throw DimensionException(
          "Tile extent check failed on dimension '" + name() +
          "'; Tile extent must not be 0");

    // Check if tile extent exceeds domain
    uint64_t range = (uint64_t)domain[1] - (uint64_t)domain[0] + 1;
    if (uint64_t(*tile_extent) > range)
      throw DimensionException(
          "Tile extent check failed on dimension '" + name() +
          "'; Tile extent " + std::to_string(*tile_extent) +
          " exceeds range on dimension domain [" + std::to_string(domain[0]) +
          ", " + std::to_string(domain[1]) + "]");

    // In the worst case one tile extent will be added to the upper domain
    // for the dense case, so check if the expanded domain will exceed type
    // T's max limit.
    if (range % uint64_t(*tile_extent))
      RETURN_NOT_OK(check_tile_extent_upper_floor(domain, *tile_extent));
  }

  return Status::Ok();
}

template <typename T>
Status Dimension::check_tile_extent_upper_floor(
    const T* const domain, const T tile_extent) const {
  // The type of the upper floor must match the sign of the extent
  // type.
  return std::is_signed<T>::value ?
             check_tile_extent_upper_floor_internal<T, int64_t>(
                 domain, tile_extent) :
             check_tile_extent_upper_floor_internal<T, uint64_t>(
                 domain, tile_extent);
}

template <typename T_EXTENT, typename T_FLOOR>
Status Dimension::check_tile_extent_upper_floor_internal(
    const T_EXTENT* const domain, const T_EXTENT tile_extent) const {
  const uint64_t range = domain[1] - domain[0] + 1;
  const T_FLOOR upper_floor =
      ((range - 1) / (tile_extent)) * (tile_extent) + domain[0];
  const T_FLOOR upper_floor_max = std::numeric_limits<T_FLOOR>::max() -
                                  (static_cast<T_FLOOR>(tile_extent) - 1);
  const T_FLOOR extent_max =
      static_cast<T_FLOOR>(std::numeric_limits<T_EXTENT>::max());
  const bool exceeds =
      upper_floor > upper_floor_max || upper_floor > extent_max;
  if (exceeds) {
    return LOG_STATUS(Status_DimensionError(
        "Tile extent check failed; domain max expanded to multiple of tile "
        "extent exceeds max value representable by domain type. Reduce "
        "domain max by 1 tile extent to allow for expansion."));
  }

  return Status::Ok();
}

void Dimension::ensure_datatype_is_supported(Datatype type) const {
  try {
    ensure_datatype_is_valid(type);
  } catch (...) {
    std::throw_with_nested(
        std::logic_error("[Dimension::ensure_datatype_is_supported] "));
    return;
  }

  switch (type) {
    case Datatype::CHAR:
    case Datatype::BLOB:
    case Datatype::BOOL:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::ANY:
      throw std::logic_error(
          "Datatype::" + datatype_str(type) +
          " is not a valid Dimension Datatype");
    default:
      return;
  }
}

std::string Dimension::tile_extent_str() const {
  std::stringstream ss;

  if (!tile_extent_)
    return constants::null_str;

  const float* tile_extent_float32;
  const double* tile_extent_float64;
  const uint8_t* tile_extent_uint8;
  const uint16_t* tile_extent_uint16;
  const uint32_t* tile_extent_uint32;
  const uint64_t* tile_extent_uint64;

  switch (type_) {
    case Datatype::INT32:
      tile_extent_uint32 = (const uint32_t*)tile_extent_.data();
      ss << *tile_extent_uint32;
      return ss.str();
    case Datatype::INT64:
      tile_extent_uint64 = (const uint64_t*)tile_extent_.data();
      ss << *tile_extent_uint64;
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
      tile_extent_uint8 = (const uint8_t*)tile_extent_.data();
      ss << int(*tile_extent_uint8);
      return ss.str();
    case Datatype::UINT8:
      tile_extent_uint8 = (const uint8_t*)tile_extent_.data();
      ss << int(*tile_extent_uint8);
      return ss.str();
    case Datatype::INT16:
      tile_extent_uint16 = (const uint16_t*)tile_extent_.data();
      ss << *tile_extent_uint16;
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      tile_extent_uint64 = (const uint64_t*)tile_extent_.data();
      ss << *tile_extent_uint64;
      return ss.str();

    case Datatype::BLOB:
    case Datatype::CHAR:
    case Datatype::BOOL:
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      ceil_to_tile_func_ = ceil_to_tile<int64_t>;
      break;
    default:
      ceil_to_tile_func_ = nullptr;
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
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
      case Datatype::TIME_HR:
      case Datatype::TIME_MIN:
      case Datatype::TIME_SEC:
      case Datatype::TIME_MS:
      case Datatype::TIME_US:
      case Datatype::TIME_NS:
      case Datatype::TIME_PS:
      case Datatype::TIME_FS:
      case Datatype::TIME_AS:
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      covered_func_ = covered<int64_t>;
      break;
    case Datatype::STRING_ASCII:
      assert(var_size());
      covered_func_ = covered<char>;
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
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

void Dimension::set_relevant_ranges_func() {
  switch (type_) {
    case Datatype::INT32:
      relevant_ranges_func_ = relevant_ranges<int32_t>;
      break;
    case Datatype::INT64:
      relevant_ranges_func_ = relevant_ranges<int64_t>;
      break;
    case Datatype::INT8:
      relevant_ranges_func_ = relevant_ranges<int8_t>;
      break;
    case Datatype::UINT8:
      relevant_ranges_func_ = relevant_ranges<uint8_t>;
      break;
    case Datatype::INT16:
      relevant_ranges_func_ = relevant_ranges<int16_t>;
      break;
    case Datatype::UINT16:
      relevant_ranges_func_ = relevant_ranges<uint16_t>;
      break;
    case Datatype::UINT32:
      relevant_ranges_func_ = relevant_ranges<uint32_t>;
      break;
    case Datatype::UINT64:
      relevant_ranges_func_ = relevant_ranges<uint64_t>;
      break;
    case Datatype::FLOAT32:
      relevant_ranges_func_ = relevant_ranges<float>;
      break;
    case Datatype::FLOAT64:
      relevant_ranges_func_ = relevant_ranges<double>;
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      relevant_ranges_func_ = relevant_ranges<int64_t>;
      break;
    case Datatype::STRING_ASCII:
      assert(var_size());
      relevant_ranges_func_ = relevant_ranges<char>;
      break;
    default:
      relevant_ranges_func_ = nullptr;
      break;
  }
}

void Dimension::set_covered_vec_func() {
  switch (type_) {
    case Datatype::INT32:
      covered_vec_func_ = covered_vec<int32_t>;
      break;
    case Datatype::INT64:
      covered_vec_func_ = covered_vec<int64_t>;
      break;
    case Datatype::INT8:
      covered_vec_func_ = covered_vec<int8_t>;
      break;
    case Datatype::UINT8:
      covered_vec_func_ = covered_vec<uint8_t>;
      break;
    case Datatype::INT16:
      covered_vec_func_ = covered_vec<int16_t>;
      break;
    case Datatype::UINT16:
      covered_vec_func_ = covered_vec<uint16_t>;
      break;
    case Datatype::UINT32:
      covered_vec_func_ = covered_vec<uint32_t>;
      break;
    case Datatype::UINT64:
      covered_vec_func_ = covered_vec<uint64_t>;
      break;
    case Datatype::FLOAT32:
      covered_vec_func_ = covered_vec<float>;
      break;
    case Datatype::FLOAT64:
      covered_vec_func_ = covered_vec<double>;
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      covered_vec_func_ = covered_vec<int64_t>;
      break;
    case Datatype::STRING_ASCII:
      assert(var_size());
      covered_vec_func_ = covered_vec<char>;
      break;
    default:
      covered_vec_func_ = nullptr;
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
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

void Dimension::set_map_to_uint64_2_func() {
  switch (type_) {
    case Datatype::INT32:
      map_to_uint64_2_func_ = map_to_uint64_2<int32_t>;
      break;
    case Datatype::INT64:
      map_to_uint64_2_func_ = map_to_uint64_2<int64_t>;
      break;
    case Datatype::INT8:
      map_to_uint64_2_func_ = map_to_uint64_2<int8_t>;
      break;
    case Datatype::UINT8:
      map_to_uint64_2_func_ = map_to_uint64_2<uint8_t>;
      break;
    case Datatype::INT16:
      map_to_uint64_2_func_ = map_to_uint64_2<int16_t>;
      break;
    case Datatype::UINT16:
      map_to_uint64_2_func_ = map_to_uint64_2<uint16_t>;
      break;
    case Datatype::UINT32:
      map_to_uint64_2_func_ = map_to_uint64_2<uint32_t>;
      break;
    case Datatype::UINT64:
      map_to_uint64_2_func_ = map_to_uint64_2<uint64_t>;
      break;
    case Datatype::FLOAT32:
      map_to_uint64_2_func_ = map_to_uint64_2<float>;
      break;
    case Datatype::FLOAT64:
      map_to_uint64_2_func_ = map_to_uint64_2<double>;
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      map_to_uint64_2_func_ = map_to_uint64_2<int64_t>;
      break;
    case Datatype::STRING_ASCII:
      map_to_uint64_2_func_ = map_to_uint64_2<char>;
      break;
    default:
      map_to_uint64_2_func_ = nullptr;
      break;
  }
}

void Dimension::set_map_from_uint64_func() {
  switch (type_) {
    case Datatype::INT32:
      map_from_uint64_func_ = map_from_uint64<int32_t>;
      break;
    case Datatype::INT64:
      map_from_uint64_func_ = map_from_uint64<int64_t>;
      break;
    case Datatype::INT8:
      map_from_uint64_func_ = map_from_uint64<int8_t>;
      break;
    case Datatype::UINT8:
      map_from_uint64_func_ = map_from_uint64<uint8_t>;
      break;
    case Datatype::INT16:
      map_from_uint64_func_ = map_from_uint64<int16_t>;
      break;
    case Datatype::UINT16:
      map_from_uint64_func_ = map_from_uint64<uint16_t>;
      break;
    case Datatype::UINT32:
      map_from_uint64_func_ = map_from_uint64<uint32_t>;
      break;
    case Datatype::UINT64:
      map_from_uint64_func_ = map_from_uint64<uint64_t>;
      break;
    case Datatype::FLOAT32:
      map_from_uint64_func_ = map_from_uint64<float>;
      break;
    case Datatype::FLOAT64:
      map_from_uint64_func_ = map_from_uint64<double>;
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      map_from_uint64_func_ = map_from_uint64<int64_t>;
      break;
    case Datatype::STRING_ASCII:
      map_from_uint64_func_ = map_from_uint64<char>;
      break;
    default:
      map_from_uint64_func_ = nullptr;
      break;
  }
}

void Dimension::set_smaller_than_func() {
  switch (type_) {
    case Datatype::INT32:
      smaller_than_func_ = smaller_than<int32_t>;
      break;
    case Datatype::INT64:
      smaller_than_func_ = smaller_than<int64_t>;
      break;
    case Datatype::INT8:
      smaller_than_func_ = smaller_than<int8_t>;
      break;
    case Datatype::UINT8:
      smaller_than_func_ = smaller_than<uint8_t>;
      break;
    case Datatype::INT16:
      smaller_than_func_ = smaller_than<int16_t>;
      break;
    case Datatype::UINT16:
      smaller_than_func_ = smaller_than<uint16_t>;
      break;
    case Datatype::UINT32:
      smaller_than_func_ = smaller_than<uint32_t>;
      break;
    case Datatype::UINT64:
      smaller_than_func_ = smaller_than<uint64_t>;
      break;
    case Datatype::FLOAT32:
      smaller_than_func_ = smaller_than<float>;
      break;
    case Datatype::FLOAT64:
      smaller_than_func_ = smaller_than<double>;
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
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      smaller_than_func_ = smaller_than<int64_t>;
      break;
    case Datatype::STRING_ASCII:
      smaller_than_func_ = smaller_than<char>;
      break;
    default:
      smaller_than_func_ = nullptr;
      break;
  }
}

}  // namespace sm
}  // namespace tiledb
