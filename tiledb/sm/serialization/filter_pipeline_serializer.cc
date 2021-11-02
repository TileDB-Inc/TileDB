/**
 * @file   filter_pipeline_serializer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * This file defines serialization functions for FilterPipeline.
 */

#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/serialize.h>
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "tiledb/sm/serialization/filter_pipeline_serializer.h"

#include <set>

using namespace tiledb::common;

namespace tiledb {
namespace sm {
namespace serialization {

#ifdef TILEDB_SERIALIZATION


Status filter_pipeline_serialize_to_capnp(
    const FilterPipeline* filter_pipeline,
    capnp::FilterPipeline::Builder* filter_pipeline_builder) {
  if (filter_pipeline == nullptr)
    return LOG_STATUS(Status::SerializationError(
        "Error serializing filter pipeline; filter pipeline is null."));

  const unsigned num_filters = filter_pipeline->size();
  if (num_filters == 0)
    return Status::Ok();

  auto filter_list_builder = filter_pipeline_builder->initFilters(num_filters);
  for (unsigned i = 0; i < num_filters; i++) {
    const auto* filter = filter_pipeline->get_filter(i);
    auto filter_builder = filter_list_builder[i];
    filter_builder.setType(filter_type_str(filter->type()));

    switch (filter->type()) {
      case FilterType::FILTER_BIT_WIDTH_REDUCTION: {
        uint32_t window;
        RETURN_NOT_OK(
            filter->get_option(FilterOption::BIT_WIDTH_MAX_WINDOW, &window));
        auto data = filter_builder.initData();
        data.setUint32(window);
        break;
      }
      case FilterType::FILTER_POSITIVE_DELTA: {
        uint32_t window;
        RETURN_NOT_OK(filter->get_option(
            FilterOption::POSITIVE_DELTA_MAX_WINDOW, &window));
        auto data = filter_builder.initData();
        data.setUint32(window);
        break;
      }
      case FilterType::FILTER_GZIP:
      case FilterType::FILTER_ZSTD:
      case FilterType::FILTER_LZ4:
      case FilterType::FILTER_RLE:
      case FilterType::FILTER_BZIP2:
      case FilterType::FILTER_DOUBLE_DELTA: {
        int32_t level;
        RETURN_NOT_OK(
            filter->get_option(FilterOption::COMPRESSION_LEVEL, &level));
        auto data = filter_builder.initData();
        data.setInt32(level);
        break;
      }
      default:
        break;
    }
  }

  return Status::Ok();
}

Status filter_pipeline_deserialize_from_capnp(
    const capnp::FilterPipeline::Reader& filter_pipeline_reader,
    tdb_unique_ptr<FilterPipeline>* filter_pipeline) {
  filter_pipeline->reset(tdb_new(FilterPipeline));
  if (!filter_pipeline_reader.hasFilters())
    return Status::Ok();

  auto filter_list_reader = filter_pipeline_reader.getFilters();
  for (auto filter_reader : filter_list_reader) {
    FilterType type = FilterType::FILTER_NONE;
    RETURN_NOT_OK(filter_type_enum(filter_reader.getType().cStr(), &type));
    tdb_unique_ptr<Filter> filter(Filter::create(type));
    if (filter == nullptr)
      return LOG_STATUS(Status::SerializationError(
          "Error deserializing filter pipeline; failed to create filter."));

    switch (filter->type()) {
      case FilterType::FILTER_BIT_WIDTH_REDUCTION: {
        auto data = filter_reader.getData();
        uint32_t window = data.getUint32();
        RETURN_NOT_OK(
            filter->set_option(FilterOption::BIT_WIDTH_MAX_WINDOW, &window));
        break;
      }
      case FilterType::FILTER_POSITIVE_DELTA: {
        auto data = filter_reader.getData();
        uint32_t window = data.getUint32();
        RETURN_NOT_OK(filter->set_option(
            FilterOption::POSITIVE_DELTA_MAX_WINDOW, &window));
        break;
      }
      case FilterType::FILTER_GZIP:
      case FilterType::FILTER_ZSTD:
      case FilterType::FILTER_LZ4:
      case FilterType::FILTER_RLE:
      case FilterType::FILTER_BZIP2:
      case FilterType::FILTER_DOUBLE_DELTA: {
        auto data = filter_reader.getData();
        int32_t level = data.getInt32();
        RETURN_NOT_OK(
            filter->set_option(FilterOption::COMPRESSION_LEVEL, &level));
        break;
      }
      default:
        break;
    }

    RETURN_NOT_OK((*filter_pipeline)->add_filter(*filter));
  }

  return Status::Ok();
}

#else 
Status filter_pipeline_serialize_to_capnp(
    const FilterPipeline* filter_pipeline,
    capnp::FilterPipeline::Builder* filter_pipeline_builder) {
  return LOG_STATUS(Status::SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status filter_pipeline_deserialize_from_capnp(
    const capnp::FilterPipeline::Reader& filter_pipeline_reader,
    tdb_unique_ptr<FilterPipeline>* filter_pipeline) {
  return LOG_STATUS(Status::SerializationError(
      "Cannot serialize; serialization not enabled."));
}




#endif  // TILEDB_SERIALIZATION


}  // namespace serialization
}  // namespace sm
}  // namespace tiledb    