/**
 * @file   array_schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2022 TileDB, Inc.
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
 * This file defines serialization functions for ArraySchema.
 */
#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/serialize.h>
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/dimension_label_reference.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/filter/bit_width_reduction_filter.h"
#include "tiledb/sm/filter/bitshuffle_filter.h"
#include "tiledb/sm/filter/byteshuffle_filter.h"
#include "tiledb/sm/filter/checksum_md5_filter.h"
#include "tiledb/sm/filter/checksum_sha256_filter.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/encryption_aes256gcm_filter.h"
#include "tiledb/sm/filter/filter_create.h"
#include "tiledb/sm/filter/float_scaling_filter.h"
#include "tiledb/sm/filter/positive_delta_filter.h"
#include "tiledb/sm/filter/xor_filter.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/serialization/array_schema.h"

#include <cstring>
#include <set>
#include <string>

using namespace tiledb::common;

namespace tiledb {
namespace sm {
namespace serialization {

#ifdef TILEDB_SERIALIZATION

Status filter_to_capnp(
    const Filter* filter, capnp::Filter::Builder* filter_builder) {
  filter_builder->setType(filter_type_str(filter->type()));
  switch (filter->type()) {
    case FilterType::FILTER_BIT_WIDTH_REDUCTION: {
      uint32_t window;
      RETURN_NOT_OK(
          filter->get_option(FilterOption::BIT_WIDTH_MAX_WINDOW, &window));
      auto data = filter_builder->initData();
      data.setUint32(window);
      break;
    }
    case FilterType::FILTER_POSITIVE_DELTA: {
      uint32_t window;
      RETURN_NOT_OK(
          filter->get_option(FilterOption::POSITIVE_DELTA_MAX_WINDOW, &window));
      auto data = filter_builder->initData();
      data.setUint32(window);
      break;
    }
    case FilterType::FILTER_GZIP:
    case FilterType::FILTER_ZSTD:
    case FilterType::FILTER_LZ4:
    case FilterType::FILTER_RLE:
    case FilterType::FILTER_BZIP2:
    case FilterType::FILTER_DOUBLE_DELTA:
    case FilterType::FILTER_DICTIONARY: {
      int32_t level;
      RETURN_NOT_OK(
          filter->get_option(FilterOption::COMPRESSION_LEVEL, &level));
      auto data = filter_builder->initData();
      data.setInt32(level);
      break;
    }
    case FilterType::FILTER_SCALE_FLOAT: {
      double scale, offset;
      uint64_t byte_width;
      RETURN_NOT_OK(
          filter->get_option(FilterOption::SCALE_FLOAT_BYTEWIDTH, &byte_width));
      RETURN_NOT_OK(
          filter->get_option(FilterOption::SCALE_FLOAT_FACTOR, &scale));
      RETURN_NOT_OK(
          filter->get_option(FilterOption::SCALE_FLOAT_OFFSET, &offset));
      auto config = filter_builder->initFloatScaleConfig();
      config.setScale(scale);
      config.setOffset(offset);
      config.setByteWidth(byte_width);
      break;
    }
    case FilterType::FILTER_NONE:
    case FilterType::FILTER_BITSHUFFLE:
    case FilterType::FILTER_BYTESHUFFLE:
    case FilterType::FILTER_CHECKSUM_MD5:
    case FilterType::FILTER_CHECKSUM_SHA256:
    case FilterType::INTERNAL_FILTER_AES_256_GCM:
    case FilterType::FILTER_XOR:
      break;
  }

  return Status::Ok();
}

Status filter_pipeline_to_capnp(
    const FilterPipeline* filter_pipeline,
    capnp::FilterPipeline::Builder* filter_pipeline_builder) {
  if (filter_pipeline == nullptr)
    return LOG_STATUS(Status_SerializationError(
        "Error serializing filter pipeline; filter pipeline is null."));

  const unsigned num_filters = filter_pipeline->size();
  if (num_filters == 0)
    return Status::Ok();

  auto filter_list_builder = filter_pipeline_builder->initFilters(num_filters);
  for (unsigned i = 0; i < num_filters; i++) {
    const auto* filter = filter_pipeline->get_filter(i);
    auto filter_builder = filter_list_builder[i];
    filter_to_capnp(filter, &filter_builder);
  }

  return Status::Ok();
}

tuple<Status, optional<shared_ptr<Filter>>> filter_from_capnp(
    const capnp::Filter::Reader& filter_reader) {
  FilterType type = FilterType::FILTER_NONE;
  RETURN_NOT_OK_TUPLE(
      filter_type_enum(filter_reader.getType().cStr(), &type), nullopt);

  switch (type) {
    case FilterType::FILTER_BIT_WIDTH_REDUCTION: {
      auto data = filter_reader.getData();
      uint32_t window = data.getUint32();
      return {
          Status::Ok(),
          tiledb::common::make_shared<BitWidthReductionFilter>(HERE(), window)};
    }
    case FilterType::FILTER_POSITIVE_DELTA: {
      auto data = filter_reader.getData();
      uint32_t window = data.getUint32();
      return {Status::Ok(),
              tiledb::common::make_shared<PositiveDeltaFilter>(HERE(), window)};
    }
    case FilterType::FILTER_GZIP:
    case FilterType::FILTER_ZSTD:
    case FilterType::FILTER_LZ4:
    case FilterType::FILTER_RLE:
    case FilterType::FILTER_BZIP2:
    case FilterType::FILTER_DOUBLE_DELTA:
    case FilterType::FILTER_DICTIONARY: {
      auto data = filter_reader.getData();
      int32_t level = data.getInt32();
      return {
          Status::Ok(),
          tiledb::common::make_shared<CompressionFilter>(HERE(), type, level)};
    }
    case FilterType::FILTER_SCALE_FLOAT: {
      if (filter_reader.hasFloatScaleConfig()) {
        auto float_scale_config = filter_reader.getFloatScaleConfig();
        double scale = float_scale_config.getScale();
        double offset = float_scale_config.getOffset();
        uint64_t byte_width = float_scale_config.getByteWidth();
        return {Status::Ok(),
                tiledb::common::make_shared<FloatScalingFilter>(
                    HERE(), byte_width, scale, offset)};
      }

      return {Status::Ok(),
              tiledb::common::make_shared<FloatScalingFilter>(HERE())};
    }
    case FilterType::FILTER_NONE:
      break;
    case FilterType::FILTER_BITSHUFFLE: {
      return {Status::Ok(),
              tiledb::common::make_shared<BitshuffleFilter>(HERE())};
    }
    case FilterType::FILTER_BYTESHUFFLE: {
      return {Status::Ok(),
              tiledb::common::make_shared<ByteshuffleFilter>(HERE())};
    }
    case FilterType::FILTER_CHECKSUM_MD5: {
      return {Status::Ok(),
              tiledb::common::make_shared<ChecksumMD5Filter>(HERE())};
    }
    case FilterType::FILTER_CHECKSUM_SHA256: {
      return {Status::Ok(),
              tiledb::common::make_shared<ChecksumSHA256Filter>(HERE())};
    }
    case FilterType::INTERNAL_FILTER_AES_256_GCM: {
      return {Status::Ok(),
              tiledb::common::make_shared<EncryptionAES256GCMFilter>(HERE())};
    }
    case FilterType::FILTER_XOR: {
      return {Status::Ok(), tiledb::common::make_shared<XORFilter>(HERE())};
    }
    default: {
      throw std::logic_error(
          "Invalid data received from filter pipeline capnp reader, unknown "
          "type");
    }
  }
  return {Status_SerializationError(
              "Invalid data received from filter pipeline capnp reader, "
              "unknown type " +
              filter_type_str(type)),
          std::nullopt};
}

tuple<Status, optional<shared_ptr<FilterPipeline>>> filter_pipeline_from_capnp(
    const capnp::FilterPipeline::Reader& filter_pipeline_reader) {
  if (!filter_pipeline_reader.hasFilters())
    return {Status::Ok(), make_shared<FilterPipeline>(HERE())};

  std::vector<shared_ptr<Filter>> filter_list;
  auto filter_list_reader = filter_pipeline_reader.getFilters();
  for (auto filter_reader : filter_list_reader) {
    auto&& [st_f, filter]{filter_from_capnp(filter_reader)};
    RETURN_NOT_OK_TUPLE(st_f, nullopt);
    filter_list.push_back(filter.value());
  }

  return {Status::Ok(),
          make_shared<FilterPipeline>(
              HERE(), constants::max_tile_chunk_size, filter_list)};
}

Status attribute_to_capnp(
    const Attribute* attribute, capnp::Attribute::Builder* attribute_builder) {
  if (attribute == nullptr)
    return LOG_STATUS(Status_SerializationError(
        "Error serializing attribute; attribute is null."));

  attribute_builder->setName(attribute->name());
  attribute_builder->setType(datatype_str(attribute->type()));
  attribute_builder->setCellValNum(attribute->cell_val_num());
  attribute_builder->setNullable(attribute->nullable());

  // Get the fill value from `attribute`.
  const void* fill_value;
  uint64_t fill_value_size;
  uint8_t fill_validity = true;
  if (!attribute->nullable())
    RETURN_NOT_OK(attribute->get_fill_value(&fill_value, &fill_value_size));
  else
    RETURN_NOT_OK(attribute->get_fill_value(
        &fill_value, &fill_value_size, &fill_validity));

  // Copy the fill value buffer into a capnp vector of bytes.
  auto capnpFillValue = kj::Vector<uint8_t>();
  capnpFillValue.addAll(kj::ArrayPtr<uint8_t>(
      const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(fill_value)),
      fill_value_size));

  // Store the fill value vector of bytes.
  attribute_builder->setFillValue(capnpFillValue.asPtr());

  // Set the validity fill value.
  attribute_builder->setFillValueValidity(fill_validity ? true : false);

  const auto& filters = attribute->filters();
  auto filter_pipeline_builder = attribute_builder->initFilterPipeline();
  RETURN_NOT_OK(filter_pipeline_to_capnp(&filters, &filter_pipeline_builder));

  return Status::Ok();
}

tuple<Status, optional<shared_ptr<Attribute>>> attribute_from_capnp(
    const capnp::Attribute::Reader& attribute_reader) {
  // Get datatype
  Datatype datatype = Datatype::ANY;
  RETURN_NOT_OK_TUPLE(
      datatype_enum(attribute_reader.getType(), &datatype), nullopt);

  // Set nullable.
  const bool nullable = attribute_reader.getNullable();

  // Filter pipelines
  shared_ptr<FilterPipeline> filters{};
  if (attribute_reader.hasFilterPipeline()) {
    auto filter_pipeline_reader = attribute_reader.getFilterPipeline();
    auto&& [st_fp, f]{filter_pipeline_from_capnp(filter_pipeline_reader)};
    RETURN_NOT_OK_TUPLE(st_fp, nullopt);
    filters = f.value();
  } else {
    filters = make_shared<FilterPipeline>(HERE());
  }

  // Fill value
  ByteVecValue fill_value_vec;
  uint8_t fill_value_validity = 0;
  if (attribute_reader.hasFillValue()) {
    // To initialize the ByteVecValue object, we do so by instantiating a
    // vector of type uint8_t that points to the data stored in the
    // byte vector.
    auto capnp_byte_vec = attribute_reader.getFillValue().asBytes();
    auto vec_ptr = capnp_byte_vec.begin();
    std::vector<uint8_t> byte_vec(vec_ptr, vec_ptr + capnp_byte_vec.size());
    fill_value_vec = ByteVecValue(move(byte_vec));
    if (nullable) {
      fill_value_validity = attribute_reader.getFillValueValidity();
    }
  } else {
    // default initialization
    fill_value_vec = Attribute::default_fill_value(
        datatype, attribute_reader.getCellValNum());
  }

  return {Status::Ok(),
          tiledb::common::make_shared<Attribute>(
              HERE(),
              attribute_reader.getName(),
              datatype,
              nullable,
              attribute_reader.getCellValNum(),
              *(filters.get()),
              fill_value_vec,
              fill_value_validity)};
}

Status dimension_to_capnp(
    const Dimension* dimension, capnp::Dimension::Builder* dimension_builder) {
  if (dimension == nullptr)
    return LOG_STATUS(Status_SerializationError(
        "Error serializing dimension; dimension is null."));

  dimension_builder->setName(dimension->name());
  dimension_builder->setType(datatype_str(dimension->type()));
  dimension_builder->setNullTileExtent(!dimension->tile_extent());

  // Only set the domain if its not empty/null. String dimensions have null
  // domains
  if (!dimension->domain().empty()) {
    auto domain_builder = dimension_builder->initDomain();
    RETURN_NOT_OK(utils::set_capnp_array_ptr(
        domain_builder, dimension->type(), dimension->domain().data(), 2));
  }

  // Only set the tile extent if its not empty
  if (dimension->tile_extent()) {
    auto tile_extent_builder = dimension_builder->initTileExtent();
    RETURN_NOT_OK(utils::set_capnp_scalar(
        tile_extent_builder,
        dimension->type(),
        dimension->tile_extent().data()));
  }

  // Set filters
  const FilterPipeline& coords_filters = dimension->filters();
  capnp::FilterPipeline::Builder filters_builder =
      dimension_builder->initFilterPipeline();
  RETURN_NOT_OK(filter_pipeline_to_capnp(&coords_filters, &filters_builder));
  return Status::Ok();
}

/**
 * Deserialize a tile extent from a cap'n proto object.
 *
 * @pre dim_type is valid.
 */
ByteVecValue tile_extent_from_capnp(
    const capnp::Dimension::TileExtent::Reader tile_extent_reader,
    Datatype dim_type) {
  auto coord_size = datatype_size(dim_type);
  ByteVecValue tile_extent(coord_size);
  switch (dim_type) {
    case Datatype::INT8: {
      auto val = tile_extent_reader.getInt8();
      std::memcpy(tile_extent.data(), &val, coord_size);
      break;
    }
    case Datatype::UINT8: {
      auto val = tile_extent_reader.getUint8();
      std::memcpy(tile_extent.data(), &val, coord_size);
      break;
    }
    case Datatype::INT16: {
      auto val = tile_extent_reader.getInt16();
      std::memcpy(tile_extent.data(), &val, coord_size);
      break;
    }
    case Datatype::UINT16: {
      auto val = tile_extent_reader.getUint16();
      std::memcpy(tile_extent.data(), &val, coord_size);
      break;
    }
    case Datatype::INT32: {
      auto val = tile_extent_reader.getInt32();
      std::memcpy(tile_extent.data(), &val, coord_size);
      break;
    }
    case Datatype::UINT32: {
      auto val = tile_extent_reader.getUint32();
      std::memcpy(tile_extent.data(), &val, coord_size);
      break;
    }
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
    case Datatype::INT64: {
      auto val = tile_extent_reader.getInt64();
      std::memcpy(tile_extent.data(), &val, coord_size);
      break;
    }
    case Datatype::UINT64: {
      auto val = tile_extent_reader.getUint64();
      std::memcpy(tile_extent.data(), &val, coord_size);
      break;
    }
    case Datatype::FLOAT32: {
      auto val = tile_extent_reader.getFloat32();
      std::memcpy(tile_extent.data(), &val, coord_size);
      break;
    }
    case Datatype::FLOAT64: {
      auto val = tile_extent_reader.getFloat64();
      std::memcpy(tile_extent.data(), &val, coord_size);
      break;
    }
    default:
      throw std::logic_error(
          "[Deserialization::tile_extent_from_capnp] Precondition violated; "
          "Input Datatype is invalid.");
  }
  return tile_extent;
}

/** Deserialize a range from a cap'n proto object.
 *
 * @pre dim_type is valid.
 */
Range range_from_capnp(
    Datatype dim_type, const capnp::Dimension::Reader& dimension_reader) {
  if (dimension_reader.hasDomain()) {
    auto domain_reader = dimension_reader.getDomain();
    Buffer domain_buffer;
    Status st = utils::copy_capnp_list(domain_reader, dim_type, &domain_buffer);
    if (!st.ok()) {
      throw std::runtime_error(
          "[Deserialization::range_from_capnp] Failed to copy " +
          std::string(dimension_reader.getType().cStr()) + " typed Capnp List");
    }
    return Range{domain_buffer.data(), datatype_size(dim_type) * 2};
  } else {
    return Range{};
  }
}

/** Deserialize a dimension from a cap'n proto object. */
shared_ptr<Dimension> dimension_from_capnp(
    const capnp::Dimension::Reader& dimension_reader) {
  Status st;

  // Deserialize datatype
  Datatype dim_type;
  st = datatype_enum(dimension_reader.getType().cStr(), &dim_type);
  if (!st.ok()) {
    throw std::runtime_error(
        "[Deserialization::dimension_from_capnp] " +
        std::string(dimension_reader.getType().cStr()) +
        " is not a valid datatype identifer.");
  }

  // Validate the dim_type, satisfying precondition of tile_extent_from_capnp
  try {
    ensure_datatype_is_valid(dim_type);
  } catch (const std::exception& e) {
    std::throw_with_nested(
        std::runtime_error("[Deserialization::dimension_from_capnp] "));
  }

  // Calculate size of coordinates and re-ensure dim_type is valid (size != 0)
  auto coord_size = datatype_size(dim_type);
  if (coord_size == 0) {
    throw std::logic_error(
        "[Deserialization::dimension_from_capnp] " +
        std::to_string(std::underlying_type_t<Datatype>(dim_type)) +
        " is not valid as a datatype; datatype_size is 0.");
  }

  // Calculate cell_val_num
  uint32_t cell_val_num = datatype_is_string(dim_type) ? constants::var_num : 1;

  // Load domain
  // Note: If there is no domain in capnp, a default one is constructed.
  Range domain{range_from_capnp(dim_type, dimension_reader)};

  // Load filters
  // Note: If there is no FilterPipeline in capnp, a default one is constructed.
  shared_ptr<FilterPipeline> filters{};
  if (dimension_reader.hasFilterPipeline()) {
    auto reader = dimension_reader.getFilterPipeline();
    auto&& [st_fp, f]{filter_pipeline_from_capnp(reader)};
    if (!st_fp.ok()) {
      throw std::runtime_error(
          "[Deserialization::dimension_from_capnp] Failed to deserialize "
          "filter pipeline.");
    }
    filters = f.value();
  } else {
    filters = make_shared<FilterPipeline>(HERE());
  }

  // Load tile extent
  // Note: If there is no tile extent in capnp, a default one is constructed.
  ByteVecValue tile_extent{};
  if (!dimension_reader.getNullTileExtent()) {
    auto tile_extent_reader = dimension_reader.getTileExtent();
    tile_extent = tile_extent_from_capnp(tile_extent_reader, dim_type);
  }

  return make_shared<Dimension>(
      HERE(),
      dimension_reader.getName(),
      dim_type,
      cell_val_num,
      domain,
      *(filters.get()),
      tile_extent);
}

Status domain_to_capnp(
    const Domain* domain, capnp::Domain::Builder* domainBuilder) {
  if (domain == nullptr)
    return LOG_STATUS(
        Status_SerializationError("Error serializing domain; domain is null."));

  // The type must be serialized for backwards compatibility with pre
  // TileDB 2.10 clients
  domainBuilder->setType(datatype_str(domain->dimension_ptr(0)->type()));
  domainBuilder->setTileOrder(layout_str(domain->tile_order()));
  domainBuilder->setCellOrder(layout_str(domain->cell_order()));

  const unsigned ndims = domain->dim_num();
  auto dimensions_builder = domainBuilder->initDimensions(ndims);
  for (unsigned i = 0; i < ndims; i++) {
    auto dim_builder = dimensions_builder[i];
    RETURN_NOT_OK(dimension_to_capnp(domain->dimension_ptr(i), &dim_builder));
  }

  return Status::Ok();
}

/* Deserialize a domain from a cap'n proto object. */
shared_ptr<Domain> domain_from_capnp(
    const capnp::Domain::Reader& domain_reader) {
  Status st;

  // Deserialize and validate cell order
  Layout cell_order = Layout::ROW_MAJOR;
  st = layout_enum(domain_reader.getCellOrder().cStr(), &cell_order);
  if (!st.ok()) {
    throw std::runtime_error(
        "[Deserialization::domain_from_capnp] " +
        std::string(domain_reader.getCellOrder().cStr()) +
        " is not a valid cell order identifer.");
  }
  try {
    ensure_cell_order_is_valid(std::underlying_type_t<Layout>(cell_order));
  } catch (std::exception& e) {
    std::throw_with_nested(
        std::runtime_error("[Deserialization::domain_from_capnp] "));
  }

  // Deserialize and validate tile order
  Layout tile_order = Layout::ROW_MAJOR;
  st = layout_enum(domain_reader.getTileOrder().cStr(), &tile_order);
  if (!st.ok()) {
    throw std::runtime_error(
        "[Deserialization::domain_from_capnp] " +
        std::string(domain_reader.getTileOrder().cStr()) +
        " is not a valid tile order identifer.");
  }
  try {
    ensure_tile_order_is_valid(std::underlying_type_t<Layout>(tile_order));
  } catch (std::exception& e) {
    std::throw_with_nested(
        std::runtime_error("[Deserialization::domain_from_capnp] "));
  }

  // Deserialize dimensions
  // Note: Security validation delegated to invoked API
  std::vector<shared_ptr<Dimension>> dims;
  auto dimensions = domain_reader.getDimensions();
  for (auto dimension : dimensions) {
    dims.emplace_back(dimension_from_capnp(dimension));
  }

  return make_shared<Domain>(HERE(), cell_order, dims, tile_order);
}

Status array_schema_to_capnp(
    const ArraySchema& array_schema,
    capnp::ArraySchema::Builder* array_schema_builder,
    const bool client_side) {
  // Only set the URI if client side
  if (client_side)
    array_schema_builder->setUri(array_schema.array_uri().to_string());

  array_schema_builder->setName(array_schema.name());
  auto v = kj::heapArray<int32_t>(1);
  v[0] = array_schema.version();
  array_schema_builder->setVersion(v);
  array_schema_builder->setArrayType(array_type_str(array_schema.array_type()));
  array_schema_builder->setTileOrder(layout_str(array_schema.tile_order()));
  array_schema_builder->setCellOrder(layout_str(array_schema.cell_order()));
  array_schema_builder->setCapacity(array_schema.capacity());
  array_schema_builder->setAllowsDuplicates(array_schema.allows_dups());

  // Set coordinate filters
  const FilterPipeline& coords_filters = array_schema.coords_filters();
  capnp::FilterPipeline::Builder coords_filters_builder =
      array_schema_builder->initCoordsFilterPipeline();
  RETURN_NOT_OK(
      filter_pipeline_to_capnp(&coords_filters, &coords_filters_builder));

  // Set offset filters
  const FilterPipeline& offsets_filters =
      array_schema.cell_var_offsets_filters();
  capnp::FilterPipeline::Builder offsets_filters_builder =
      array_schema_builder->initOffsetFilterPipeline();
  RETURN_NOT_OK(
      filter_pipeline_to_capnp(&offsets_filters, &offsets_filters_builder));

  // Set validity filters
  const FilterPipeline& validity_filters = array_schema.cell_validity_filters();
  capnp::FilterPipeline::Builder validity_filters_builder =
      array_schema_builder->initValidityFilterPipeline();
  RETURN_NOT_OK(
      filter_pipeline_to_capnp(&validity_filters, &validity_filters_builder));

  // Domain
  auto domain_builder = array_schema_builder->initDomain();
  RETURN_NOT_OK(domain_to_capnp(&array_schema.domain(), &domain_builder));

  // Attributes
  const unsigned num_attrs = array_schema.attribute_num();
  auto attributes_buidler = array_schema_builder->initAttributes(num_attrs);
  for (size_t i = 0; i < num_attrs; i++) {
    auto attribute_builder = attributes_buidler[i];
    RETURN_NOT_OK(
        attribute_to_capnp(array_schema.attribute(i), &attribute_builder));
  }

  // Set timestamp range
  auto timestamp_builder = array_schema_builder->initTimestampRange(2);
  const auto& timestamp_range = array_schema.timestamp_range();
  timestamp_builder.set(0, timestamp_range.first);
  timestamp_builder.set(1, timestamp_range.second);

  return Status::Ok();
}

// #TODO Add security validation on incoming URI
ArraySchema array_schema_from_capnp(
    const capnp::ArraySchema::Reader& schema_reader, const URI& uri) {
  // Deserialize and validate array_type
  ArrayType array_type = ArrayType::DENSE;
  Status st = array_type_enum(schema_reader.getArrayType(), &array_type);
  if (!st.ok()) {
    throw std::runtime_error(
        "[Deserialization::array_schema_from_capnp] " +
        std::string(schema_reader.getArrayType().cStr()) +
        " is not a valid ArrayType identifer.");
  }

  try {
    ensure_array_type_is_valid(std::underlying_type_t<ArrayType>(array_type));
  } catch (std::exception& e) {
    std::throw_with_nested(
        std::runtime_error("[Deserialization::array_schema_from_capnp] "));
  }

  // Deserialize and validate tile_order
  Layout tile_order = Layout::ROW_MAJOR;
  st = layout_enum(schema_reader.getTileOrder().cStr(), &tile_order);
  if (!st.ok()) {
    throw std::runtime_error(
        "[Deserialization::array_schema_from_capnp] " +
        std::string(schema_reader.getTileOrder().cStr()) +
        " is not a valid tile order identifer.");
  }

  try {
    ensure_tile_order_is_valid(std::underlying_type_t<Layout>(tile_order));
  } catch (std::exception& e) {
    std::throw_with_nested(
        std::runtime_error("[Deserialization::array_schema_from_capnp] "));
  }

  // Deserialize and validate cell_order
  Layout cell_order = Layout::ROW_MAJOR;
  st = layout_enum(schema_reader.getCellOrder().cStr(), &cell_order);
  if (!st.ok()) {
    throw std::runtime_error(
        "[Deserialization::array_schema_from_capnp] " +
        std::string(schema_reader.getCellOrder().cStr()) +
        " is not a valid cell order identifer.");
  }

  try {
    ensure_cell_order_is_valid(std::underlying_type_t<Layout>(cell_order));
  } catch (std::exception& e) {
    std::throw_with_nested(
        std::runtime_error("[Deserialization::array_schema_from_capnp] "));
  }

  // Deserialize URI
  // #TODO Add security validation
  URI uri_deserialized = URI();
  if (schema_reader.hasUri()) {
    uri_deserialized = URI(schema_reader.getUri().cStr());
  } else {
    uri_deserialized = uri;
  }

  // Deserialize capacity
  // #TODO Add security validation
  uint64_t capacity = schema_reader.getCapacity();

  // Deserialize allows_dups
  // Note: No security validation is possible.
  bool allows_dups = schema_reader.getAllowsDuplicates();

  // Pre 1.8 TileDB serialized the version as the library version
  // This would have been a list of size 3, so only set the version
  // if the list size is 1, meaning tiledb 1.8 or later
  // #TODO Add security validation
  uint32_t version = constants::format_version;
  if (schema_reader.hasVersion() && schema_reader.getVersion().size() == 1) {
    version = schema_reader.getVersion()[0];
  }

  // Deserialize domain
  // Note: Security validation delegated to invoked API
  auto domain_reader = schema_reader.getDomain();
  auto domain{domain_from_capnp(domain_reader)};

  // Set coords filter pipelines
  // Note: Security validation delegated to invoked API
  // #TODO Add security validation
  FilterPipeline coords_filters;
  if (schema_reader.hasCoordsFilterPipeline()) {
    auto reader = schema_reader.getCoordsFilterPipeline();
    auto&& [st_fp, filters]{filter_pipeline_from_capnp(reader)};
    if (!st_fp.ok()) {
      throw std::runtime_error(
          "[Deserialization::array_schema_from_capnp] Cannot deserialize "
          "coords filters.");
    }
    coords_filters = *filters.value().get();
  }

  // Set offsets filter pipelines
  // Note: Security validation delegated to invoked API
  // #TODO Add security validation
  FilterPipeline cell_var_offsets_filters;
  if (schema_reader.hasOffsetFilterPipeline()) {
    auto reader = schema_reader.getOffsetFilterPipeline();
    auto&& [st_fp, filters]{filter_pipeline_from_capnp(reader)};
    if (!st_fp.ok()) {
      throw std::runtime_error(
          "[Deserialization::array_schema_from_capnp] Cannot deserialize "
          "offset filters.");
    }
    cell_var_offsets_filters = *filters.value().get();
  }

  // Set validity filter pipelines
  // Note: Security validation delegated to invoked API
  // #TODO Add security validation
  FilterPipeline cell_validity_filters;
  if (schema_reader.hasValidityFilterPipeline()) {
    auto reader = schema_reader.getValidityFilterPipeline();
    auto&& [st_fp, filters]{filter_pipeline_from_capnp(reader)};
    if (!st_fp.ok()) {
      throw std::runtime_error(
          "[Deserialization::array_schema_from_capnp] Cannot deserialize "
          "validity filters.");
    }
    cell_validity_filters = *filters.value().get();
  }

  // Set attributes
  // Note: Security validation delegated to invoked API
  // #TODO Add security validation
  auto attributes_reader = schema_reader.getAttributes();
  std::vector<shared_ptr<const Attribute>> attributes;
  attributes.reserve(attributes_reader.size());
  for (auto attr_reader : attributes_reader) {
    auto&& [st_attr, attr]{attribute_from_capnp(attr_reader)};
    if (!st_attr.ok()) {
      throw std::runtime_error(
          "[Deserialization::array_schema_from_capnp] Cannot deserialize "
          "attributes.");
    }
    attributes.emplace_back(attr.value());
  }

  // Placeholder for deserializing dimension label references
  std::vector<shared_ptr<const DimensionLabelReference>> dimension_labels{};

  // Set the range if we have two values
  // #TODO Add security validation
  std::pair<uint64_t, uint64_t> timestamp_range;
  if (schema_reader.hasTimestampRange() &&
      schema_reader.getTimestampRange().size() >= 2) {
    const auto& range = schema_reader.getTimestampRange();
    timestamp_range = std::make_pair(range[0], range[1]);
  }

  // Deserialize the name
  // #TODO Add security validation
  std::string name;
  if (schema_reader.hasName()) {
    name = schema_reader.getName().cStr();
  }

  return ArraySchema(
      uri_deserialized,
      version,
      timestamp_range,
      name,
      array_type,
      allows_dups,
      make_shared<Domain>(HERE(), domain.get()),
      cell_order,
      tile_order,
      capacity,
      attributes,
      dimension_labels,
      cell_var_offsets_filters,
      cell_validity_filters,
      coords_filters);
}

Status array_schema_serialize(
    const ArraySchema& array_schema,
    SerializationType serialize_type,
    Buffer* serialized_buffer,
    const bool client_side) {
  try {
    ::capnp::MallocMessageBuilder message;
    capnp::ArraySchema::Builder arraySchemaBuilder =
        message.initRoot<capnp::ArraySchema>();
    RETURN_NOT_OK(
        array_schema_to_capnp(array_schema, &arraySchemaBuilder, client_side));

    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(arraySchemaBuilder);
        const auto json_len = capnp_json.size();
        const char nul = '\0';
        // size does not include needed null terminator, so add +1
        RETURN_NOT_OK(serialized_buffer->realloc(json_len + 1));
        RETURN_NOT_OK(serialized_buffer->write(capnp_json.cStr(), json_len));
        RETURN_NOT_OK(serialized_buffer->write(&nul, 1));
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();
        const auto nbytes = message_chars.size();
        RETURN_NOT_OK(serialized_buffer->realloc(nbytes));
        RETURN_NOT_OK(serialized_buffer->write(message_chars.begin(), nbytes));
        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error serializing array schema; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing array schema; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing array schema; exception " + std::string(e.what())));
  }

  return Status::Ok();
}

ArraySchema array_schema_deserialize(
    SerializationType serialize_type, const Buffer& serialized_buffer) {
  capnp::ArraySchema::Reader array_schema_reader;
  ::capnp::MallocMessageBuilder message_builder;

  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        capnp::ArraySchema::Builder array_schema_builder =
            message_builder.initRoot<capnp::ArraySchema>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            array_schema_builder);
        array_schema_reader = array_schema_builder.asReader();
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        array_schema_reader = reader.getRoot<capnp::ArraySchema>();
        break;
      }
      default: {
        throw StatusException(Status_SerializationError(
            "Error deserializing array schema; Unknown serialization type "
            "passed"));
      }
    }
  } catch (kj::Exception& e) {
    throw StatusException(Status_SerializationError(
        "Error deserializing array schema; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    throw StatusException(Status_SerializationError(
        "Error deserializing array schema; exception " +
        std::string(e.what())));
  }

  return array_schema_from_capnp(array_schema_reader, URI());
}

Status nonempty_domain_serialize(
    const Dimension* dimension,
    const void* nonempty_domain,
    bool is_empty,
    SerializationType serialize_type,
    Buffer* serialized_buffer) {
  if (!is_empty && nonempty_domain == nullptr)
    return LOG_STATUS(Status_SerializationError(
        "Error serializing nonempty domain; nonempty domain is null."));

  try {
    // Serialize
    ::capnp::MallocMessageBuilder message;
    auto builder = message.initRoot<capnp::NonEmptyDomain>();
    builder.setIsEmpty(is_empty);

    if (!is_empty) {
      auto subarray_builder = builder.initNonEmptyDomain();
      RETURN_NOT_OK(utils::serialize_coords(
          subarray_builder, dimension, nonempty_domain));
    }

    // Copy to buffer
    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(builder);
        const auto json_len = capnp_json.size();
        const char nul = '\0';
        // size does not include needed null terminator, so add +1
        RETURN_NOT_OK(serialized_buffer->realloc(json_len + 1));
        RETURN_NOT_OK(serialized_buffer->write(capnp_json.cStr(), json_len));
        RETURN_NOT_OK(serialized_buffer->write(&nul, 1));
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();
        const auto nbytes = message_chars.size();
        RETURN_NOT_OK(serialized_buffer->realloc(nbytes));
        RETURN_NOT_OK(serialized_buffer->write(message_chars.begin(), nbytes));
        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error serializing nonempty domain; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing nonempty domain; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing nonempty domain; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

Status nonempty_domain_deserialize(
    const Dimension* dimension,
    const Buffer& serialized_buffer,
    SerializationType serialize_type,
    void* nonempty_domain,
    bool* is_empty) {
  if (nonempty_domain == nullptr)
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing nonempty domain; nonempty domain is null."));

  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        auto builder = message_builder.initRoot<capnp::NonEmptyDomain>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            builder);
        auto reader = builder.asReader();

        // Deserialize
        *is_empty = reader.getIsEmpty();
        if (!*is_empty) {
          void* subarray;
          RETURN_NOT_OK(utils::deserialize_coords(
              reader.getNonEmptyDomain(), dimension, &subarray));
          std::memcpy(nonempty_domain, subarray, 2 * dimension->coord_size());
          tdb_free(subarray);
        }

        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader msg_reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        auto reader = msg_reader.getRoot<capnp::NonEmptyDomain>();

        // Deserialize
        *is_empty = reader.getIsEmpty();
        if (!*is_empty) {
          void* subarray;
          RETURN_NOT_OK(utils::deserialize_coords(
              reader.getNonEmptyDomain(), dimension, &subarray));
          std::memcpy(nonempty_domain, subarray, 2 * dimension->coord_size());
          tdb_free(subarray);
        }

        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error deserializing nonempty domain; Unknown serialization type "
            "passed"));
      }
    }
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing nonempty domain; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing nonempty domain; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

Status nonempty_domain_serialize(
    const Array* array,
    const void* nonempty_domain,
    bool is_empty,
    SerializationType serialize_type,
    Buffer* serialized_buffer) {
  if (!is_empty && nonempty_domain == nullptr)
    return LOG_STATUS(Status_SerializationError(
        "Error serializing nonempty domain; nonempty domain is null."));

  const auto& schema = array->array_schema_latest();

  try {
    // Serialize
    ::capnp::MallocMessageBuilder message;
    auto builder = message.initRoot<capnp::NonEmptyDomain>();
    builder.setIsEmpty(is_empty);

    if (!is_empty) {
      auto subarray_builder = builder.initNonEmptyDomain();
      RETURN_NOT_OK(
          utils::serialize_subarray(subarray_builder, schema, nonempty_domain));
    }

    // Copy to buffer
    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(builder);
        const auto json_len = capnp_json.size();
        const char nul = '\0';
        // size does not include needed null terminator, so add +1
        RETURN_NOT_OK(serialized_buffer->realloc(json_len + 1));
        RETURN_NOT_OK(serialized_buffer->write(capnp_json.cStr(), json_len));
        RETURN_NOT_OK(serialized_buffer->write(&nul, 1));
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();
        const auto nbytes = message_chars.size();
        RETURN_NOT_OK(serialized_buffer->realloc(nbytes));
        RETURN_NOT_OK(serialized_buffer->write(message_chars.begin(), nbytes));
        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error serializing nonempty domain; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing nonempty domain; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing nonempty domain; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

Status nonempty_domain_deserialize(
    const Array* array,
    const Buffer& serialized_buffer,
    SerializationType serialize_type,
    void* nonempty_domain,
    bool* is_empty) {
  if (nonempty_domain == nullptr)
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing nonempty domain; nonempty domain is null."));

  const auto& schema = array->array_schema_latest();

  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        auto builder = message_builder.initRoot<capnp::NonEmptyDomain>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            builder);
        auto reader = builder.asReader();

        // Deserialize
        *is_empty = reader.getIsEmpty();
        if (!*is_empty) {
          void* subarray;
          RETURN_NOT_OK(utils::deserialize_subarray(
              reader.getNonEmptyDomain(), schema, &subarray));
          std::memcpy(
              nonempty_domain,
              subarray,
              2 * schema.dimension_ptr(0)->coord_size());
          tdb_free(subarray);
        }

        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader msg_reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        auto reader = msg_reader.getRoot<capnp::NonEmptyDomain>();

        // Deserialize
        *is_empty = reader.getIsEmpty();
        if (!*is_empty) {
          void* subarray;
          RETURN_NOT_OK(utils::deserialize_subarray(
              reader.getNonEmptyDomain(), schema, &subarray));
          std::memcpy(
              nonempty_domain,
              subarray,
              2 * schema.dimension_ptr(0)->coord_size());
          tdb_free(subarray);
        }

        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error deserializing nonempty domain; Unknown serialization type "
            "passed"));
      }
    }
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing nonempty domain; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing nonempty domain; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

Status nonempty_domain_serialize(
    Array* array, SerializationType serialize_type, Buffer* serialized_buffer) {
  try {
    // Serialize
    ::capnp::MallocMessageBuilder message;
    auto builder = message.initRoot<capnp::NonEmptyDomainList>();

    RETURN_NOT_OK(utils::serialize_non_empty_domain(builder, array));

    // Copy to buffer
    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(builder);
        const auto json_len = capnp_json.size();
        const char nul = '\0';
        // size does not include needed null terminator, so add +1
        RETURN_NOT_OK(serialized_buffer->realloc(json_len + 1));
        RETURN_NOT_OK(serialized_buffer->write(capnp_json.cStr(), json_len));
        RETURN_NOT_OK(serialized_buffer->write(&nul, 1));
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();
        const auto nbytes = message_chars.size();
        RETURN_NOT_OK(serialized_buffer->realloc(nbytes));
        RETURN_NOT_OK(serialized_buffer->write(message_chars.begin(), nbytes));
        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error serializing nonempty domain; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing nonempty domain; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing nonempty domain; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

Status nonempty_domain_deserialize(
    Array* array,
    const Buffer& serialized_buffer,
    SerializationType serialize_type) {
  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        auto builder = message_builder.initRoot<capnp::NonEmptyDomainList>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            builder);
        auto reader = builder.asReader();

        // Deserialize
        RETURN_NOT_OK(utils::deserialize_non_empty_domain(reader, array));
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader msg_reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        auto reader = msg_reader.getRoot<capnp::NonEmptyDomainList>();

        // Deserialize
        RETURN_NOT_OK(utils::deserialize_non_empty_domain(reader, array));
        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error deserializing nonempty domain; Unknown serialization type "
            "passed"));
      }
    }
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing nonempty domain; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing nonempty domain; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

Status max_buffer_sizes_serialize(
    Array* array,
    const void* subarray,
    SerializationType serialize_type,
    Buffer* serialized_buffer) {
  const auto& schema = array->array_schema_latest();

  try {
    // Serialize
    ::capnp::MallocMessageBuilder message;
    auto builder = message.initRoot<capnp::MaxBufferSizes>();

    // Get all attribute names including coords
    const auto& attrs = schema.attributes();
    std::set<std::string> attr_names;
    attr_names.insert(constants::coords);
    for (const auto& a : attrs)
      attr_names.insert(a->name());

    // Get max buffer size for each attribute from the given Array instance
    // and serialize it.
    auto max_buffer_sizes_builder =
        builder.initMaxBufferSizes(attr_names.size());
    size_t i = 0;
    for (const auto& attr_name : attr_names) {
      bool var_size =
          attr_name != constants::coords && schema.var_size(attr_name);
      auto max_buffer_size_builder = max_buffer_sizes_builder[i++];
      max_buffer_size_builder.setAttribute(attr_name);

      if (var_size) {
        uint64_t offset_bytes, data_bytes;
        RETURN_NOT_OK(array->get_max_buffer_size(
            attr_name.c_str(), subarray, &offset_bytes, &data_bytes));
        max_buffer_size_builder.setOffsetBytes(offset_bytes);
        max_buffer_size_builder.setDataBytes(data_bytes);
      } else {
        uint64_t data_bytes;
        RETURN_NOT_OK(array->get_max_buffer_size(
            attr_name.c_str(), subarray, &data_bytes));
        max_buffer_size_builder.setOffsetBytes(0);
        max_buffer_size_builder.setDataBytes(data_bytes);
      }
    }

    // Copy to buffer
    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(builder);
        const auto json_len = capnp_json.size();
        const char nul = '\0';
        // size does not include needed null terminator, so add +1
        RETURN_NOT_OK(serialized_buffer->realloc(json_len + 1));
        RETURN_NOT_OK(serialized_buffer->write(capnp_json.cStr(), json_len));
        RETURN_NOT_OK(serialized_buffer->write(&nul, 1));
        break;
      }
      case SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();
        const auto nbytes = message_chars.size();
        RETURN_NOT_OK(serialized_buffer->realloc(nbytes));
        RETURN_NOT_OK(serialized_buffer->write(message_chars.begin(), nbytes));
        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error serializing max buffer sizes; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing max buffer sizes; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error serializing max buffer sizes; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

Status max_buffer_sizes_deserialize(
    const ArraySchema& schema,
    const Buffer& serialized_buffer,
    SerializationType serialize_type,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  try {
    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        auto builder = message_builder.initRoot<capnp::MaxBufferSizes>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            builder);
        auto reader = builder.asReader();

        // Deserialize
        auto max_buffer_sizes_reader = reader.getMaxBufferSizes();
        const size_t num_max_buffer_sizes = max_buffer_sizes_reader.size();
        for (size_t i = 0; i < num_max_buffer_sizes; i++) {
          auto max_buffer_size_reader = max_buffer_sizes_reader[i];
          std::string attribute = max_buffer_size_reader.getAttribute();
          uint64_t offset_size = max_buffer_size_reader.getOffsetBytes();
          uint64_t data_size = max_buffer_size_reader.getDataBytes();

          if (attribute == constants::coords || !schema.var_size(attribute)) {
            (*buffer_sizes)[attribute] = std::make_pair(data_size, 0);
          } else {
            (*buffer_sizes)[attribute] = std::make_pair(offset_size, data_size);
          }
        }

        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader msg_reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        auto reader = msg_reader.getRoot<capnp::MaxBufferSizes>();

        // Deserialize
        auto max_buffer_sizes_reader = reader.getMaxBufferSizes();
        const size_t num_max_buffer_sizes = max_buffer_sizes_reader.size();
        for (size_t i = 0; i < num_max_buffer_sizes; i++) {
          auto max_buffer_size_reader = max_buffer_sizes_reader[i];
          std::string attribute = max_buffer_size_reader.getAttribute();
          uint64_t offset_size = max_buffer_size_reader.getOffsetBytes();
          uint64_t data_size = max_buffer_size_reader.getDataBytes();

          if (attribute == constants::coords || !schema.var_size(attribute)) {
            (*buffer_sizes)[attribute] = std::make_pair(data_size, 0);
          } else {
            (*buffer_sizes)[attribute] = std::make_pair(offset_size, data_size);
          }
        }

        break;
      }
      default: {
        return LOG_STATUS(Status_SerializationError(
            "Error deserializing max buffer sizes; Unknown serialization type "
            "passed"));
      }
    }
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing max buffer sizes; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status_SerializationError(
        "Error deserializing max buffer sizes; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

#else

Status array_schema_serialize(
    const ArraySchema&, SerializationType, Buffer*, const bool) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

ArraySchema array_schema_deserialize(SerializationType, const Buffer&) {
  throw StatusException(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status nonempty_domain_serialize(Array*, SerializationType, Buffer*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status nonempty_domain_deserialize(Array*, const Buffer&, SerializationType) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status nonempty_domain_serialize(
    const Array*, const void*, bool, SerializationType, Buffer*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status nonempty_domain_deserialize(
    const Array*, const Buffer&, SerializationType, void*, bool*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status max_buffer_sizes_serialize(
    Array*, const void*, SerializationType, Buffer*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status max_buffer_sizes_deserialize(
    const ArraySchema&,
    const Buffer&,
    SerializationType,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*) {
  return LOG_STATUS(Status_SerializationError(
      "Cannot serialize; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
