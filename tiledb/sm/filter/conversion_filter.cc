/**
 * @file   conversion_filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file defines class ConversionFilter.
 */

#include "tiledb/sm/filter/conversion_filter.h"
#include <iostream>
#include <limits>
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/filter_buffer.h"
using namespace tiledb::common;

namespace tiledb {
namespace sm {

ConversionFilter::ConversionFilter(
    Datatype query_datatype, Datatype store_datatype)
    : Filter(FilterType::FILTER_CONVERSION)
    , query_datatype_(query_datatype)
    , store_datatype_(store_datatype) {
}

Datatype ConversionFilter::query_datatype() const {
  return query_datatype_;
}

/** Return the datatyoe to store on disk. */
Datatype ConversionFilter::store_datatype() const {
  return store_datatype_;
}

uint64_t ConversionFilter::calc_query_size(uint64_t orig_size) {
  uint64_t orig_type_size = datatype_size(store_datatype_);
  uint64_t query_type_size = datatype_size(query_datatype_);
  if (orig_type_size > 0) {
    return uint64_t(1.0 * query_type_size * orig_size / orig_type_size);
  } else {
    return orig_size;
  }
}

ConversionFilter* ConversionFilter::clone_impl() const {
  return tdb_new(ConversionFilter, query_datatype_, store_datatype_);
}

void ConversionFilter::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;

  fprintf(out, "Conversion");
}

Status ConversionFilter::run_forward(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  if (query_datatype_ == store_datatype_) {
    RETURN_NOT_OK(output->append_view(input));
    RETURN_NOT_OK(output_metadata->append_view(input_metadata));
    return Status::Ok();
  }

  switch (store_datatype_) {
    case Datatype::INT32:
      return run_forward<int32_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT64:
      return run_forward<int64_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::FLOAT32:
      return run_forward<float>(input_metadata, input, output_metadata, output);
    case Datatype::FLOAT64:
      return run_forward<double>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT8:
      return run_forward<int8_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT8:
      return run_forward<uint8_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT16:
      return run_forward<int16_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT16:
      return run_forward<uint16_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT32:
      return run_forward<uint32_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT64:
      return run_forward<uint64_t>(
          input_metadata, input, output_metadata, output);
    default:
      return LOG_STATUS(
          Status::FilterError("Cannot filter; Unsupported store_datatype"));
  }
}

Status ConversionFilter::run_reverse(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config& config) const {
  (void)config;

  if (query_datatype_ == store_datatype_) {
    RETURN_NOT_OK(output->append_view(input));
    RETURN_NOT_OK(output_metadata->append_view(input_metadata));
    return Status::Ok();
  }

  switch (query_datatype_) {
    case Datatype::INT32:
      return run_reverse<int32_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT64:
      return run_reverse<int64_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::FLOAT32:
      return run_reverse<float>(input_metadata, input, output_metadata, output);
    case Datatype::FLOAT64:
      return run_reverse<double>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT8:
      return run_reverse<int8_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT8:
      return run_reverse<uint8_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT16:
      return run_reverse<int16_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT16:
      return run_reverse<uint16_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT32:
      return run_reverse<uint32_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT64:
      return run_reverse<uint64_t>(
          input_metadata, input, output_metadata, output);
    default:
      return LOG_STATUS(
          Status::FilterError("Cannot filter; Unsupported query_datatype"));
  }
}

bool ConversionFilter::is_convertible(Datatype datatype) {
  switch (datatype) {
    case Datatype::INT32:
    case Datatype::INT64:
    case Datatype::FLOAT32:
    case Datatype::FLOAT64:
      //  case Datatype::CHAR:
    case Datatype::INT8:
    case Datatype::UINT8:
    case Datatype::INT16:
    case Datatype::UINT16:
    case Datatype::UINT32:
    case Datatype::UINT64:
      return true;
    default:
      return false;
  }
}

template <typename TQuery, typename TStore>
Status ConversionFilter::run_reverse(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  uint64_t query_datatype_size = datatype_size(query_datatype_);
  uint64_t store_datatype_size = datatype_size(store_datatype_);
  uint64_t n = input->size() / store_datatype_size;

  for (uint64_t i = 0; i < n; ++i) {
    TStore store_value;
    input->read(&store_value, store_datatype_size);
    TQuery query_value;
    if ((double)store_value > (double)std::numeric_limits<TQuery>::max()) {
      query_value = std::numeric_limits<TQuery>::max();
    } else if (
        (double)store_value < (double)std::numeric_limits<TQuery>::lowest()) {
      query_value = std::numeric_limits<TQuery>::lowest();
    } else {
      query_value = static_cast<TQuery>(store_value);
    }

    output->write(&query_value, query_datatype_size);
  }

  // Append metata.
  RETURN_NOT_OK(output_metadata->append_view(input_metadata));

  return Status::Ok();
}

template <typename TQuery>
Status ConversionFilter::run_reverse(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  switch (store_datatype_) {
    case Datatype::INT32:
      return run_reverse<TQuery, int32_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT64:
      return run_reverse<TQuery, int64_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::FLOAT32:
      return run_reverse<TQuery, float>(
          input_metadata, input, output_metadata, output);
    case Datatype::FLOAT64:
      return run_reverse<TQuery, double>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT8:
      return run_reverse<TQuery, int8_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT8:
      return run_reverse<TQuery, uint8_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT16:
      return run_reverse<TQuery, int16_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT16:
      return run_reverse<TQuery, uint16_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT32:
      return run_reverse<TQuery, uint32_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT64:
      return run_reverse<TQuery, uint64_t>(
          input_metadata, input, output_metadata, output);
    default:
      return LOG_STATUS(
          Status::FilterError("Cannot filter; Unsupported store_datatype"));
  }
}

template <typename TQuery, typename TStore>
Status ConversionFilter::run_forward(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  uint64_t query_datatype_size = datatype_size(query_datatype_);
  uint64_t store_datatype_size = datatype_size(store_datatype_);

  uint64_t n = input->size() / query_datatype_size;
  for (uint64_t i = 0; i < n; ++i) {
    TQuery query_value;
    input->read(&query_value, query_datatype_size);

    TStore store_value;
    // Cast to double values for comparison.
    if ((double)query_value > (double)std::numeric_limits<TStore>::max()) {
      store_value = std::numeric_limits<TStore>::max();
    } else if (
        (double)query_value < (double)std::numeric_limits<TStore>::lowest()) {
      store_value = std::numeric_limits<TStore>::lowest();
    } else {
      store_value = static_cast<TStore>(query_value);
    }

    output->write(&store_value, store_datatype_size);
  }

  // Append metata.
  RETURN_NOT_OK(output_metadata->append_view(input_metadata));

  return Status::Ok();
}

template <typename TStore>
Status ConversionFilter::run_forward(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  switch (query_datatype_) {
    case Datatype::INT32:
      return run_forward<int32_t, TStore>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT64:
      return run_forward<int64_t, TStore>(
          input_metadata, input, output_metadata, output);
    case Datatype::FLOAT32:
      return run_forward<float, TStore>(
          input_metadata, input, output_metadata, output);
    case Datatype::FLOAT64:
      return run_forward<double, TStore>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT8:
      return run_forward<int8_t, TStore>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT8:
      return run_forward<uint8_t, TStore>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT16:
      return run_forward<int16_t, TStore>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT16:
      return run_forward<uint16_t, TStore>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT32:
      return run_forward<uint32_t, TStore>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT64:
      return run_forward<uint64_t, TStore>(
          input_metadata, input, output_metadata, output);
    default:
      return LOG_STATUS(
          Status::FilterError("Cannot filter; Unsupported query_datatype"));
  }
}

}  // namespace sm
}  // namespace tiledb
