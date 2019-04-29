/**
 * @file   array_schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2019 TileDB, Inc.
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

#include "tiledb/sm/serialization/array_schema.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/serialization/capnp_utils.h"

#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/serialize.h>
#endif

namespace tiledb {
namespace sm {
namespace serialization {

#ifdef TILEDB_SERIALIZATION

Status filter_pipeline_to_capnp(
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

Status filter_pipeline_from_capnp(
    const capnp::FilterPipeline::Reader& filter_pipeline_reader,
    std::unique_ptr<FilterPipeline>* filter_pipeline) {
  filter_pipeline->reset(new FilterPipeline);
  if (!filter_pipeline_reader.hasFilters())
    return Status::Ok();

  auto filter_list_reader = filter_pipeline_reader.getFilters();
  for (const auto& filter_reader : filter_list_reader) {
    FilterType type = FilterType::FILTER_NONE;
    RETURN_NOT_OK(filter_type_enum(filter_reader.getType().cStr(), &type));
    std::unique_ptr<Filter> filter(Filter::create(type));
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

Status attribute_to_capnp(
    const Attribute* attribute, capnp::Attribute::Builder* attribute_builder) {
  if (attribute == nullptr)
    return LOG_STATUS(Status::SerializationError(
        "Error serializing attribute; attribute is null."));

  attribute_builder->setName(attribute->name());
  attribute_builder->setType(datatype_str(attribute->type()));
  attribute_builder->setCellValNum(attribute->cell_val_num());

  const auto* filters = attribute->filters();
  auto filter_pipeline_builder = attribute_builder->initFilterPipeline();
  RETURN_NOT_OK(filter_pipeline_to_capnp(filters, &filter_pipeline_builder));

  return Status::Ok();
}

Status attribute_from_capnp(
    const capnp::Attribute::Reader& attribute_reader,
    std::unique_ptr<Attribute>* attribute) {
  Datatype datatype = Datatype::ANY;
  RETURN_NOT_OK(datatype_enum(attribute_reader.getType(), &datatype));

  attribute->reset(new Attribute(attribute_reader.getName(), datatype));
  RETURN_NOT_OK(
      (*attribute)->set_cell_val_num(attribute_reader.getCellValNum()));

  // Set filter pipelines
  if (attribute_reader.hasFilterPipeline()) {
    auto filter_pipeline_reader = attribute_reader.getFilterPipeline();
    std::unique_ptr<FilterPipeline> filters;
    RETURN_NOT_OK(filter_pipeline_from_capnp(filter_pipeline_reader, &filters));
    RETURN_NOT_OK((*attribute)->set_filter_pipeline(filters.get()));
  }

  return Status::Ok();
}

Status dimension_to_capnp(
    const Dimension* dimension, capnp::Dimension::Builder* dimension_builder) {
  if (dimension == nullptr)
    return LOG_STATUS(Status::SerializationError(
        "Error serializing dimension; dimension is null."));

  dimension_builder->setName(dimension->name());
  dimension_builder->setType(datatype_str(dimension->type()));
  dimension_builder->setNullTileExtent(dimension->tile_extent() == nullptr);

  auto domain_builder = dimension_builder->initDomain();
  RETURN_NOT_OK(utils::set_capnp_array_ptr(
      domain_builder, dimension->type(), dimension->domain(), 2));

  if (dimension->tile_extent() != nullptr) {
    auto tile_extent_builder = dimension_builder->initTileExtent();
    RETURN_NOT_OK(utils::set_capnp_scalar(
        tile_extent_builder, dimension->type(), dimension->tile_extent()));
  }

  return Status::Ok();
}

Status dimension_from_capnp(
    const capnp::Dimension::Reader& dimension_reader,
    std::unique_ptr<Dimension>* dimension) {
  Datatype dim_type = Datatype::ANY;
  RETURN_NOT_OK(datatype_enum(dimension_reader.getType().cStr(), &dim_type));
  dimension->reset(new Dimension(dimension_reader.getName(), dim_type));

  auto domain_reader = dimension_reader.getDomain();
  Buffer domain_buffer;
  RETURN_NOT_OK(
      utils::copy_capnp_list(domain_reader, dim_type, &domain_buffer));
  RETURN_NOT_OK((*dimension)->set_domain(domain_buffer.data()));

  if (!dimension_reader.getNullTileExtent()) {
    auto tile_extent_reader = dimension_reader.getTileExtent();
    switch (dim_type) {
      case Datatype::INT8: {
        auto val = tile_extent_reader.getInt8();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case Datatype::UINT8: {
        auto val = tile_extent_reader.getUint8();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case Datatype::INT16: {
        auto val = tile_extent_reader.getInt16();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case Datatype::UINT16: {
        auto val = tile_extent_reader.getUint16();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case Datatype::INT32: {
        auto val = tile_extent_reader.getInt32();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case Datatype::UINT32: {
        auto val = tile_extent_reader.getUint32();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case Datatype::INT64: {
        auto val = tile_extent_reader.getInt64();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case Datatype::UINT64: {
        auto val = tile_extent_reader.getUint64();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case Datatype::FLOAT32: {
        auto val = tile_extent_reader.getFloat32();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case Datatype::FLOAT64: {
        auto val = tile_extent_reader.getFloat64();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      default:
        return LOG_STATUS(Status::SerializationError(
            "Error deserializing dimension; unknown datatype."));
    }
  }

  return Status::Ok();
}

Status domain_to_capnp(
    const Domain* domain, capnp::Domain::Builder* domainBuilder) {
  if (domain == nullptr)
    return LOG_STATUS(Status::SerializationError(
        "Error serializing domain; domain is null."));

  domainBuilder->setType(datatype_str(domain->type()));
  domainBuilder->setTileOrder(layout_str(domain->tile_order()));
  domainBuilder->setCellOrder(layout_str(domain->cell_order()));

  const unsigned ndims = domain->dim_num();
  auto dimensions_builder = domainBuilder->initDimensions(ndims);
  for (unsigned i = 0; i < ndims; i++) {
    auto dim_builder = dimensions_builder[i];
    RETURN_NOT_OK(dimension_to_capnp(domain->dimension(i), &dim_builder));
  }

  return Status::Ok();
}

Status domain_from_capnp(
    const capnp::Domain::Reader& domain_reader,
    std::unique_ptr<Domain>* domain) {
  Datatype datatype = Datatype::ANY;
  RETURN_NOT_OK(datatype_enum(domain_reader.getType(), &datatype));
  domain->reset(new Domain(datatype));

  auto dimensions = domain_reader.getDimensions();
  for (const auto& dimension : dimensions) {
    std::unique_ptr<Dimension> dim;
    RETURN_NOT_OK(dimension_from_capnp(dimension, &dim));
    RETURN_NOT_OK((*domain)->add_dimension(dim.get()));
  }

  return Status::Ok();
}

Status array_schema_to_capnp(
    const ArraySchema* array_schema,
    capnp::ArraySchema::Builder* array_schema_builder) {
  if (array_schema == nullptr)
    return LOG_STATUS(Status::SerializationError(
        "Error serializing array schema; array schema is null."));

  array_schema_builder->setUri(array_schema->array_uri().to_string());
  array_schema_builder->setVersion(kj::arrayPtr(constants::library_version, 3));
  array_schema_builder->setArrayType(
      array_type_str(array_schema->array_type()));
  array_schema_builder->setTileOrder(layout_str(array_schema->tile_order()));
  array_schema_builder->setCellOrder(layout_str(array_schema->cell_order()));
  array_schema_builder->setCapacity(array_schema->capacity());

  // Set coordinate filters
  const FilterPipeline* coords_filters = array_schema->coords_filters();
  capnp::FilterPipeline::Builder coords_filters_builder =
      array_schema_builder->initCoordsFilterPipeline();
  RETURN_NOT_OK(
      filter_pipeline_to_capnp(coords_filters, &coords_filters_builder));

  // Set offset filters
  const FilterPipeline* offsets_filters =
      array_schema->cell_var_offsets_filters();
  capnp::FilterPipeline::Builder offsets_filters_builder =
      array_schema_builder->initOffsetFilterPipeline();
  RETURN_NOT_OK(
      filter_pipeline_to_capnp(offsets_filters, &offsets_filters_builder));

  // Domain
  auto domain_builder = array_schema_builder->initDomain();
  RETURN_NOT_OK(domain_to_capnp(array_schema->domain(), &domain_builder));

  // Attributes
  const unsigned num_attrs = array_schema->attribute_num();
  auto attributes_buidler = array_schema_builder->initAttributes(num_attrs);
  for (size_t i = 0; i < num_attrs; i++) {
    auto attribute_builder = attributes_buidler[i];
    RETURN_NOT_OK(
        attribute_to_capnp(array_schema->attribute(i), &attribute_builder));
  }

  return Status::Ok();
}

Status array_schema_from_capnp(
    const capnp::ArraySchema::Reader& schema_reader,
    std::unique_ptr<ArraySchema>* array_schema) {
  ArrayType array_type = ArrayType::DENSE;
  RETURN_NOT_OK(array_type_enum(schema_reader.getArrayType(), &array_type));
  array_schema->reset(new ArraySchema(array_type));

  Layout layout = Layout::ROW_MAJOR;
  RETURN_NOT_OK(layout_enum(schema_reader.getTileOrder().cStr(), &layout));
  (*array_schema)->set_tile_order(layout);
  RETURN_NOT_OK(layout_enum(schema_reader.getCellOrder().cStr(), &layout));

  (*array_schema)->set_array_uri(URI(schema_reader.getUri().cStr()));
  (*array_schema)->set_cell_order(layout);
  (*array_schema)->set_capacity(schema_reader.getCapacity());

  auto domain_reader = schema_reader.getDomain();
  std::unique_ptr<Domain> domain;
  RETURN_NOT_OK(domain_from_capnp(domain_reader, &domain));
  RETURN_NOT_OK((*array_schema)->set_domain(domain.get()));

  // Set coords filter pipelines
  if (schema_reader.hasCoordsFilterPipeline()) {
    auto reader = schema_reader.getCoordsFilterPipeline();
    std::unique_ptr<FilterPipeline> filters;
    RETURN_NOT_OK(filter_pipeline_from_capnp(reader, &filters));
    RETURN_NOT_OK((*array_schema)->set_coords_filter_pipeline(filters.get()));
  }

  // Set offsets filter pipelines
  if (schema_reader.hasOffsetFilterPipeline()) {
    auto reader = schema_reader.getOffsetFilterPipeline();
    std::unique_ptr<FilterPipeline> filters;
    RETURN_NOT_OK(filter_pipeline_from_capnp(reader, &filters));
    RETURN_NOT_OK(
        (*array_schema)->set_cell_var_offsets_filter_pipeline(filters.get()));
  }

  // Set attributes
  auto attributes_reader = schema_reader.getAttributes();
  for (const auto& attr_reader : attributes_reader) {
    std::unique_ptr<Attribute> attribute;
    RETURN_NOT_OK(attribute_from_capnp(attr_reader, &attribute));
    RETURN_NOT_OK((*array_schema)->add_attribute(attribute.get(), false));
  }

  // Initialize
  RETURN_NOT_OK((*array_schema)->init());

  return Status::Ok();
}

Status array_schema_serialize(
    ArraySchema* array_schema,
    SerializationType serialize_type,
    Buffer* serialized_buffer) {
  STATS_FUNC_IN(serialization_array_schema_serialize);

  try {
    ::capnp::MallocMessageBuilder message;
    capnp::ArraySchema::Builder arraySchemaBuilder =
        message.initRoot<capnp::ArraySchema>();
    RETURN_NOT_OK(array_schema_to_capnp(array_schema, &arraySchemaBuilder));

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
        RETURN_NOT_OK(serialized_buffer->write(capnp_json.cStr(), json_len))
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
        return LOG_STATUS(Status::SerializationError(
            "Error serializing array schema; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status::SerializationError(
        "Error serializing array schema; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status::SerializationError(
        "Error serializing array schema; exception " + std::string(e.what())));
  }

  return Status::Ok();

  STATS_FUNC_OUT(serialization_array_schema_serialize);
}

Status array_schema_deserialize(
    ArraySchema** array_schema,
    SerializationType serialize_type,
    const Buffer& serialized_buffer) {
  STATS_FUNC_IN(serialization_array_schema_deserialize);

  try {
    std::unique_ptr<ArraySchema> decoded_array_schema = nullptr;

    switch (serialize_type) {
      case SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        capnp::ArraySchema::Builder array_schema_builder =
            message_builder.initRoot<capnp::ArraySchema>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            array_schema_builder);
        capnp::ArraySchema::Reader array_schema_reader =
            array_schema_builder.asReader();
        RETURN_NOT_OK(array_schema_from_capnp(
            array_schema_reader, &decoded_array_schema));
        break;
      }
      case SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        capnp::ArraySchema::Reader array_schema_reader =
            reader.getRoot<capnp::ArraySchema>();
        RETURN_NOT_OK(array_schema_from_capnp(
            array_schema_reader, &decoded_array_schema));
        break;
      }
      default: {
        return LOG_STATUS(Status::SerializationError(
            "Error deserializing array schema; Unknown serialization type "
            "passed"));
      }
    }

    if (decoded_array_schema == nullptr)
      return LOG_STATUS(Status::SerializationError(
          "Error serializing array schema; deserialized schema is null"));

    *array_schema = decoded_array_schema.release();
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status::SerializationError(
        "Error deserializing array schema; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status::SerializationError(
        "Error deserializing array schema; exception " +
        std::string(e.what())));
  }

  return Status::Ok();

  STATS_FUNC_OUT(serialization_array_schema_deserialize);
}

Status nonempty_domain_serialize(
    const Array* array,
    const void* nonempty_domain,
    bool is_empty,
    SerializationType serialize_type,
    Buffer* serialized_buffer) {
  if (!is_empty && nonempty_domain == nullptr)
    return LOG_STATUS(Status::SerializationError(
        "Error serializing nonempty domain; nonempty domain is null."));

  const auto* schema = array->array_schema();
  if (nonempty_domain == nullptr)
    return LOG_STATUS(Status::SerializationError(
        "Error serializing nonempty domain; array schema is null."));

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
        RETURN_NOT_OK(serialized_buffer->write(capnp_json.cStr(), json_len))
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
        return LOG_STATUS(Status::SerializationError(
            "Error serializing nonempty domain; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(Status::SerializationError(
        "Error serializing nonempty domain; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status::SerializationError(
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
    return LOG_STATUS(Status::SerializationError(
        "Error deserializing nonempty domain; nonempty domain is null."));

  const auto* schema = array->array_schema();
  if (nonempty_domain == nullptr)
    return LOG_STATUS(Status::SerializationError(
        "Error deserializing nonempty domain; array schema is null."));

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
          std::memcpy(nonempty_domain, subarray, 2 * schema->coords_size());
          std::free(subarray);
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
          std::memcpy(nonempty_domain, subarray, 2 * schema->coords_size());
          std::free(subarray);
        }

        break;
      }
      default: {
        return LOG_STATUS(Status::SerializationError(
            "Error deserializing nonempty domain; Unknown serialization type "
            "passed"));
      }
    }
  } catch (kj::Exception& e) {
    return LOG_STATUS(Status::SerializationError(
        "Error deserializing nonempty domain; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(Status::SerializationError(
        "Error deserializing nonempty domain; exception " +
        std::string(e.what())));
  }

  return Status::Ok();
}

#else

Status array_schema_serialize(ArraySchema*, SerializationType, Buffer*) {
  return LOG_STATUS(Status::SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status array_schema_deserialize(
    ArraySchema**, SerializationType, const Buffer&) {
  return LOG_STATUS(Status::SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status nonempty_domain_serialize(
    const Array*, const void*, bool, SerializationType, Buffer*) {
  return LOG_STATUS(Status::SerializationError(
      "Cannot serialize; serialization not enabled."));
}

Status nonempty_domain_deserialize(
    const Array*, const Buffer&, SerializationType, void*, bool*) {
  return LOG_STATUS(Status::SerializationError(
      "Cannot serialize; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
