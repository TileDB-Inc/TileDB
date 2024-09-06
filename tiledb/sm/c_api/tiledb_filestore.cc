/**
 * @file   tiledb_filestore.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines the C API of TileDB for filestore.
 **/

#include "tiledb/api/c_api/array_schema/array_schema_api_internal.h"
#include "tiledb/api/c_api/attribute/attribute_api_internal.h"
#include "tiledb/api/c_api/config/config_api_internal.h"
#include "tiledb/api/c_api/dimension/dimension_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/common/common.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/c_api/api_argument_validator.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/compressors/zstd_compressor.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/mime_type.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/enums/vfs_mode.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/misc/mgc_dict.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/storage_manager/context.h"
#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/type/range/range.h"

namespace tiledb::api {

// Forward declarations
uint64_t compute_tile_extent_based_on_file_size(uint64_t file_size);
std::string libmagic_get_mime(void* data, uint64_t size);
std::string libmagic_get_mime_encoding(void* data, uint64_t size);
bool libmagic_file_is_compressed(void* data, uint64_t size);
void read_file_header(
    tiledb::sm::VFS& vfs, const char* uri, std::vector<char>& header);
std::pair<std::string, std::string> strip_file_extension(const char* file_uri);
uint64_t get_buffer_size_from_config(
    const tiledb::sm::Config& config, uint64_t tile_extent);

int32_t tiledb_filestore_schema_create(
    tiledb_ctx_t* ctx, const char* uri, tiledb_array_schema_t** array_schema) {
  tiledb::sm::Context& context = ctx->context();
  uint64_t tile_extent = tiledb::sm::constants::filestore_default_tile_extent;

  bool is_compressed_libmagic = true;
  if (uri) {
    // The user provided a uri, let's examine the file and get some insights
    // Get the file size, calculate a reasonable tile extent
    auto& vfs = ctx->resources().vfs();
    uint64_t file_size;
    throw_if_not_ok(vfs.file_size(tiledb::sm::URI(uri), &file_size));
    if (file_size) {
      tile_extent = compute_tile_extent_based_on_file_size(file_size);
    }

    // Detect if the file is compressed or not
    uint64_t size = std::min(file_size, static_cast<uint64_t>(1024));
    std::vector<char> header(size);

    // Don't fail if compression cannot be detected, log a message
    // and default to uncompressed array
    try {
      read_file_header(vfs, uri, header);
      is_compressed_libmagic = libmagic_file_is_compressed(header.data(), size);
    } catch (const std::exception& e) {
      LOG_STATUS_NO_RETURN_VALUE(Status_Error(
          std::string("Compression couldn't be detected - ") + e.what()));
    }
  }

  try {
    // Share ownership of the internal ArraySchema ptr
    // All other calls for adding domains, attributes, etc
    // create copies of the underlying core objects from within
    // the cpp objects constructed here
    auto memory_tracker = context.resources().create_memory_tracker();
    memory_tracker->set_type(sm::MemoryTrackerType::ARRAY_CREATE);
    *array_schema = tiledb_array_schema_t::make_handle(
        tiledb::sm::ArrayType::DENSE, memory_tracker);

    // Define the range of the dimension.
    uint64_t range_lo = 0;
    uint64_t range_hi = std::numeric_limits<uint64_t>::max() - tile_extent - 1;
    tiledb::type::Range range_obj(&range_lo, &range_hi, sizeof(uint64_t));

    // Define the tile extent as a tiledb::sm::ByteVecValue.
    std::vector<uint8_t> tile_extent_vec(sizeof(uint64_t), 0);
    memcpy(tile_extent_vec.data(), &tile_extent, sizeof(uint64_t));

    auto dim = make_shared<tiledb::sm::Dimension>(
        HERE(),
        tiledb::sm::constants::filestore_dimension_name,
        tiledb::sm::Datatype::UINT64,
        1,
        range_obj,
        tiledb::sm::FilterPipeline{},
        tiledb::sm::ByteVecValue(std::move(tile_extent_vec)),
        memory_tracker);

    auto domain = make_shared<tiledb::sm::Domain>(HERE(), memory_tracker);
    throw_if_not_ok(domain->add_dimension(dim));

    auto attr = make_shared<tiledb::sm::Attribute>(
        HERE(),
        tiledb::sm::constants::filestore_attribute_name,
        tiledb::sm::Datatype::BLOB);

    // If the input file is not compressed, add our own compression
    if (!is_compressed_libmagic) {
      tiledb::sm::FilterPipeline filter;
      filter.add_filter(tiledb::sm::CompressionFilter(
          tiledb::sm::Compressor::ZSTD,
          tiledb::sm::ZStd::default_level(),
          tiledb::sm::Datatype::ANY));
      attr->set_filter_pipeline(filter);
    }

    throw_if_not_ok((*array_schema)->set_domain(domain));
    throw_if_not_ok(
        (*array_schema)->set_tile_order(tiledb::sm::Layout::ROW_MAJOR));
    throw_if_not_ok(
        (*array_schema)->set_cell_order(tiledb::sm::Layout::ROW_MAJOR));
    throw_if_not_ok((*array_schema)->add_attribute(attr));
  } catch (const std::exception& e) {
    tiledb_array_schema_t::break_handle(*array_schema);
    throw api::CAPIStatusException(
        std::string("Internal TileDB uncaught exception; ") + e.what());
  }

  return TILEDB_OK;
}

inline void ensure_uri_is_valid(const char* uri) {
  if (uri == nullptr) {
    throw api::CAPIException("Invalid uri pointer");
  }
}

inline void ensure_buffer_is_valid(void* p) {
  if (p == nullptr) {
    throw api::CAPIException("Invalid pointer");
  }
}

int32_t tiledb_filestore_uri_import(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    const char* file_uri,
    tiledb_mime_type_t) {
  api::ensure_context_is_valid(ctx);
  ensure_uri_is_valid(filestore_array_uri);
  ensure_uri_is_valid(file_uri);

  tiledb::sm::Context& context = ctx->context();

  // Get the file size
  auto& vfs = ctx->resources().vfs();
  uint64_t file_size;
  throw_if_not_ok(vfs.file_size(tiledb::sm::URI(file_uri), &file_size));
  if (!file_size) {
    return TILEDB_OK;  // NOOP
  }

  // Sync up the fragment timestamp and metadata timestamp
  uint64_t time_now = tiledb_timestamp_now_ms();
  auto array = make_shared<tiledb::sm::Array>(
      HERE(), context.resources(), tiledb::sm::URI(filestore_array_uri));
  throw_if_not_ok(array->open(
      tiledb::sm::QueryType::WRITE,
      0,
      time_now,
      tiledb::sm::EncryptionType::NO_ENCRYPTION,
      nullptr,
      0));

  // Detect mimetype and encoding with libmagic
  uint64_t size = std::min(file_size, static_cast<uint64_t>(1024));
  std::vector<char> header(size);
  read_file_header(vfs, file_uri, header);
  auto mime = libmagic_get_mime(header.data(), header.size());
  auto mime_encoding = libmagic_get_mime_encoding(header.data(), header.size());

  // We need to dump all the relevant metadata at this point so that
  // clients have all the necessary info when consuming the array
  array->put_metadata(
      tiledb::sm::constants::filestore_metadata_size_key.c_str(),
      tiledb::sm::Datatype::UINT64,
      1,
      &file_size);
  array->put_metadata(
      tiledb::sm::constants::filestore_metadata_mime_encoding_key.c_str(),
      tiledb::sm::Datatype::STRING_UTF8,
      static_cast<uint32_t>(mime_encoding.size()),
      mime_encoding.c_str());
  array->put_metadata(
      tiledb::sm::constants::filestore_metadata_mime_type_key.c_str(),
      tiledb::sm::Datatype::STRING_UTF8,
      static_cast<uint32_t>(mime.size()),
      mime.c_str());
  auto&& [fname, fext] = strip_file_extension(file_uri);
  array->put_metadata(
      tiledb::sm::constants::filestore_metadata_original_filename_key.c_str(),
      tiledb::sm::Datatype::STRING_UTF8,
      static_cast<uint32_t>(fname.size()),
      fname.c_str());
  array->put_metadata(
      tiledb::sm::constants::filestore_metadata_file_extension_key.c_str(),
      tiledb::sm::Datatype::STRING_UTF8,
      static_cast<uint32_t>(fext.size()),
      fext.c_str());

  // Write the data in batches using the global order writer
  if (!vfs.open_file(tiledb::sm::URI(file_uri), tiledb::sm::VFSMode::VFS_READ)
           .ok()) {
    throw api::CAPIException(
        "Failed to open the file; Invalid file URI or incorrect file "
        "permissions");
  }

  // tiledb:// uri hack
  // We need to special case on tiledb uris until we implement
  // serialization for global order writes. Until then, we write
  // timestamped fragments in row-major order.
  bool is_tiledb_uri = array->is_remote();
  uint64_t tile_extent = compute_tile_extent_based_on_file_size(file_size);
  auto buffer_size =
      get_buffer_size_from_config(context.resources().config(), tile_extent);

  tiledb::sm::Query query(context, array);
  throw_if_not_ok(query.set_layout(tiledb::sm::Layout::GLOBAL_ORDER));
  std::vector<std::byte> buffer(buffer_size);

  tiledb::sm::Subarray subarray(
      array.get(), nullptr, context.resources().logger(), true);
  // We need to get the right end boundary of the last space tile.
  // The last chunk either falls exactly on the end of the file
  // or it goes beyond the end of the file so that it's equal in size
  // to the tile extent
  uint64_t last_space_tile_boundary =
      (file_size / tile_extent +
       static_cast<uint64_t>(file_size % tile_extent != 0)) *
          tile_extent -
      1;
  uint64_t range_arr[] = {static_cast<uint64_t>(0), last_space_tile_boundary};
  tiledb::type::Range subarray_range(
      static_cast<void*>(range_arr), sizeof(uint64_t) * 2);
  subarray.add_range(0, std::move(subarray_range));
  query.set_subarray(subarray);

  auto tiledb_cloud_fix = [&](uint64_t start, uint64_t end) {
    tiledb::sm::Query query(context, array);
    throw_if_not_ok(query.set_layout(tiledb::sm::Layout::ROW_MAJOR));
    tiledb::sm::Subarray subarray_cloud_fix(
        array.get(), nullptr, context.resources().logger(), true);

    uint64_t cloud_fix_range_arr[] = {start, end};
    tiledb::type::Range subarray_range_cloud_fix(
        static_cast<void*>(cloud_fix_range_arr), sizeof(uint64_t) * 2);
    subarray_cloud_fix.add_range(0, std::move(subarray_range_cloud_fix));
    query.set_subarray(subarray_cloud_fix);
    uint64_t data_buff_len = end - start + 1;
    throw_if_not_ok(query.set_data_buffer(
        tiledb::sm::constants::filestore_attribute_name,
        buffer.data(),
        &data_buff_len));
    throw_if_not_ok(query.submit());
  };

  auto read_wrapper =
      [&file_size, &vfs, &file_uri, &buffer](uint64_t start) -> uint64_t {
    if (start >= file_size)
      return 0;

    uint64_t readlen = buffer.size();
    if (start + buffer.size() > file_size) {
      readlen = file_size - start;
    }
    throw_if_not_ok(
        vfs.read(tiledb::sm::URI(file_uri), start, buffer.data(), readlen));
    return readlen;
  };

  uint64_t start_range = 0;
  uint64_t end_range = buffer_size - 1;
  uint64_t readlen = 0;
  while ((readlen = read_wrapper(start_range)) > 0) {
    uint64_t end_cloud_fix = end_range;
    uint64_t query_buffer_len = buffer_size;
    if (readlen < buffer_size) {
      end_cloud_fix = start_range + readlen;
      query_buffer_len = last_space_tile_boundary -
                         file_size / (buffer_size) * (buffer_size) + 1;
      std::memset(buffer.data() + readlen, 0, buffer_size - readlen);
    }

    if (is_tiledb_uri) {
      tiledb_cloud_fix(start_range, end_cloud_fix);
    } else {
      throw_if_not_ok(query.set_data_buffer(
          tiledb::sm::constants::filestore_attribute_name,
          buffer.data(),
          &query_buffer_len));
      throw_if_not_ok(query.submit());
    }

    start_range += readlen;
    end_range += readlen;
  }

  if (start_range < file_size) {
    // Something must have gone wrong whilst reading the file
    throw_if_not_ok(vfs.close_file(tiledb::sm::URI(file_uri)));
    throw api::CAPIStatusException("Error whilst reading the file");
  }

  if (!is_tiledb_uri) {
    // Dump the fragment on disk
    throw_if_not_ok(query.finalize());
  }
  throw_if_not_ok(vfs.close_file(tiledb::sm::URI(file_uri)));

  throw_if_not_ok(array->close());

  return TILEDB_OK;
}

int32_t tiledb_filestore_uri_export(
    tiledb_ctx_t* ctx, const char* file_uri, const char* filestore_array_uri) {
  ensure_uri_is_valid(filestore_array_uri);
  ensure_uri_is_valid(file_uri);

  tiledb::sm::Context& context = ctx->context();
  auto& vfs = ctx->resources().vfs();
  if (!vfs.open_file(tiledb::sm::URI(file_uri), tiledb::sm::VFSMode::VFS_WRITE)
           .ok()) {
    throw api::CAPIException(
        "Failed to open the file; Invalid file URI or incorrect file "
        "permissions");
  }

  auto array = make_shared<tiledb::sm::Array>(
      HERE(), context.resources(), tiledb::sm::URI(filestore_array_uri));
  throw_if_not_ok(array->open(
      tiledb::sm::QueryType::READ,
      tiledb::sm::EncryptionType::NO_ENCRYPTION,
      nullptr,
      0));

  tiledb::sm::Datatype dtype;
  uint32_t num;
  const void* file_size_ptr = nullptr;
  array->get_metadata(
      tiledb::sm::constants::filestore_metadata_size_key.c_str(),
      &dtype,
      &num,
      &file_size_ptr);
  if (!file_size_ptr) {
    throw api::CAPIException(
        "The array metadata doesn't contain the " +
        tiledb::sm::constants::filestore_metadata_size_key + "key");
  }

  uint64_t file_size = *static_cast<const uint64_t*>(file_size_ptr);
  uint64_t tile_extent = compute_tile_extent_based_on_file_size(file_size);
  auto buffer_size =
      get_buffer_size_from_config(context.resources().config(), tile_extent);

  std::vector<std::byte> data(buffer_size);
  uint64_t start_range = 0;
  uint64_t end_range = std::min(file_size, buffer_size) - 1;
  do {
    uint64_t write_size = end_range - start_range + 1;
    tiledb::sm::Subarray subarray(
        array.get(), nullptr, context.resources().logger(), true);
    uint64_t subarray_range_arr[] = {start_range, end_range};
    tiledb::type::Range subarray_range(
        static_cast<void*>(subarray_range_arr), sizeof(uint64_t) * 2);
    subarray.add_range(0, std::move(subarray_range));

    tiledb::sm::Query query(context, array);
    throw_if_not_ok(query.set_layout(tiledb::sm::Layout::ROW_MAJOR));
    query.set_subarray(subarray);

    // Cloud compatibility hack. Currently stored tiledb file arrays have a
    // TILEDB_UINT8 attribute. We should pass the right datatype here to
    // support reads from existing tiledb file arrays.
    auto&& [st, schema] = array->get_array_schema();
    throw_if_not_ok(st);
    auto attr_type =
        schema.value()
            ->attribute(tiledb::sm::constants::filestore_attribute_name)
            ->type();
    if (attr_type == tiledb::sm::Datatype::UINT8) {
      throw_if_not_ok(query.set_data_buffer(
          tiledb::sm::constants::filestore_attribute_name,
          reinterpret_cast<uint8_t*>(data.data()),
          &write_size));
    } else {
      throw_if_not_ok(query.set_data_buffer(
          tiledb::sm::constants::filestore_attribute_name,
          data.data(),
          &write_size));
    }
    throw_if_not_ok(query.submit());

    throw_if_not_ok(vfs.write(
        tiledb::sm::URI(file_uri),
        reinterpret_cast<char*>(data.data()),
        write_size));

    start_range = end_range + 1;
    end_range = std::min(file_size - 1, end_range + buffer_size);
  } while (start_range <= end_range);

  throw_if_not_ok(vfs.close_file(tiledb::sm::URI(file_uri)));

  throw_if_not_ok(array->close());

  return TILEDB_OK;
}

int32_t tiledb_filestore_buffer_import(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    void* buf,
    size_t size,
    tiledb_mime_type_t) {
  api::ensure_context_is_valid(ctx);
  ensure_uri_is_valid(filestore_array_uri);
  ensure_buffer_is_valid(buf);

  if (!size) {
    return TILEDB_OK;  // NOOP
  }

  tiledb::sm::Context& context = ctx->context();

  // Sync up the fragment timestamp and metadata timestamp
  uint64_t time_now = tiledb_timestamp_now_ms();
  auto array = make_shared<tiledb::sm::Array>(
      HERE(), context.resources(), tiledb::sm::URI(filestore_array_uri));
  throw_if_not_ok(array->open(
      tiledb::sm::QueryType::WRITE,
      0,
      time_now,
      tiledb::sm::EncryptionType::NO_ENCRYPTION,
      nullptr,
      0));

  // Detect mimetype and encoding with libmagic
  uint64_t s = std::min(size, static_cast<size_t>(1024));
  auto mime = libmagic_get_mime(buf, s);
  auto mime_encoding = libmagic_get_mime_encoding(buf, s);

  // We need to dump all the relevant metadata at this point so that
  // clients have all the necessary info when consuming the array
  array->put_metadata(
      tiledb::sm::constants::filestore_metadata_size_key.c_str(),
      tiledb::sm::Datatype::UINT64,
      1,
      &size);
  array->put_metadata(
      tiledb::sm::constants::filestore_metadata_mime_encoding_key.c_str(),
      tiledb::sm::Datatype::STRING_UTF8,
      static_cast<uint32_t>(mime_encoding.size()),
      mime_encoding.c_str());
  array->put_metadata(
      tiledb::sm::constants::filestore_metadata_mime_type_key.c_str(),
      tiledb::sm::Datatype::STRING_UTF8,
      static_cast<uint32_t>(mime.size()),
      mime.c_str());
  array->put_metadata(
      tiledb::sm::constants::filestore_metadata_original_filename_key.c_str(),
      tiledb::sm::Datatype::STRING_UTF8,
      static_cast<uint32_t>(0),
      "");
  array->put_metadata(
      tiledb::sm::constants::filestore_metadata_file_extension_key.c_str(),
      tiledb::sm::Datatype::STRING_UTF8,
      static_cast<uint32_t>(0),
      "");

  tiledb::sm::Query query(context, array);
  throw_if_not_ok(query.set_layout(tiledb::sm::Layout::ROW_MAJOR));

  tiledb::sm::Subarray subarray(
      array.get(), nullptr, context.resources().logger(), true);
  uint64_t subarray_range_arr[] = {
      static_cast<uint64_t>(0), static_cast<uint64_t>(size - 1)};
  tiledb::type::Range subarray_range(
      static_cast<void*>(subarray_range_arr), sizeof(uint64_t) * 2);
  subarray.add_range(0, std::move(subarray_range));

  query.set_subarray(subarray);
  uint64_t size_tmp = size;
  throw_if_not_ok(query.set_data_buffer(
      tiledb::sm::constants::filestore_attribute_name, buf, &size_tmp));
  throw_if_not_ok(query.submit());

  throw_if_not_ok(array->close());

  return TILEDB_OK;
}

int32_t tiledb_filestore_buffer_export(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    size_t offset,
    void* buf,
    size_t size) {
  api::ensure_context_is_valid(ctx);
  ensure_uri_is_valid(filestore_array_uri);
  ensure_buffer_is_valid(buf);

  tiledb::sm::Context& context = ctx->context();
  auto array = make_shared<tiledb::sm::Array>(
      HERE(), context.resources(), tiledb::sm::URI(filestore_array_uri));
  throw_if_not_ok(array->open(
      tiledb::sm::QueryType::READ,
      tiledb::sm::EncryptionType::NO_ENCRYPTION,
      nullptr,
      0));

  // Check whether the user requested more data than the array contains.
  // Return an error if that's the case.
  // This is valid only when the array metadata contains the file_size key
  tiledb::sm::Datatype dtype;
  uint32_t num;
  const void* file_size = nullptr;
  array->get_metadata(
      tiledb::sm::constants::filestore_metadata_size_key.c_str(),
      &dtype,
      &num,
      &file_size);
  if (!file_size) {
    throw(Status_Error(
        "The array metadata doesn't contain the " +
        tiledb::sm::constants::filestore_metadata_size_key + "key"));
  } else if (*static_cast<const uint64_t*>(file_size) < offset + size) {
    throw(
        Status_Error("The number of bytes requested is bigger than the array"));
  }

  tiledb::sm::Subarray subarray(
      array.get(), nullptr, context.resources().logger(), true);
  uint64_t subarray_range_arr[] = {
      static_cast<uint64_t>(offset), static_cast<uint64_t>(offset + size - 1)};
  tiledb::type::Range subarray_range(
      static_cast<void*>(subarray_range_arr), sizeof(uint64_t) * 2);
  subarray.add_range(0, std::move(subarray_range));

  tiledb::sm::Query query(context, array);
  throw_if_not_ok(query.set_layout(tiledb::sm::Layout::ROW_MAJOR));
  query.set_subarray(subarray);
  uint64_t size_tmp = size;
  throw_if_not_ok(query.set_data_buffer(
      tiledb::sm::constants::filestore_attribute_name, buf, &size_tmp));
  throw_if_not_ok(query.submit());

  throw_if_not_ok(array->close());

  return TILEDB_OK;
}

int32_t tiledb_filestore_size(
    tiledb_ctx_t* ctx, const char* filestore_array_uri, size_t* size) {
  api::ensure_context_is_valid(ctx);
  ensure_uri_is_valid(filestore_array_uri);
  ensure_buffer_is_valid(size);

  tiledb::sm::Context& context = ctx->context();
  tiledb::sm::Array array(
      context.resources(), tiledb::sm::URI(filestore_array_uri));

  throw_if_not_ok(array.open(
      tiledb::sm::QueryType::READ,
      tiledb::sm::EncryptionType::NO_ENCRYPTION,
      nullptr,
      0));

  std::optional<tiledb::sm::Datatype> type = array.metadata_type(
      tiledb::sm::constants::filestore_metadata_size_key.c_str());
  bool has_key = type.has_value();
  if (!has_key) {
    LOG_STATUS_NO_RETURN_VALUE(Status_Error(
        std::string("Filestore size key not found in array metadata; this "
                    "filestore may not have been imported: ") +
        filestore_array_uri));
    return TILEDB_ERR;
  }
  uint32_t num;
  const void* file_size = nullptr;
  array.get_metadata(
      tiledb::sm::constants::filestore_metadata_size_key.c_str(),
      &type.value(),
      &num,
      &file_size);

  if (!file_size) {
    throw(std::logic_error(
        "The array metadata should contain the " +
        tiledb::sm::constants::filestore_metadata_size_key + "key"));
  }
  *size = *static_cast<const uint64_t*>(file_size);

  throw_if_not_ok(array.close());

  return TILEDB_OK;
}

int32_t tiledb_mime_type_to_str(
    tiledb_mime_type_t mime_type, const char** str) {
  const auto& strval =
      tiledb::sm::mime_type_str((tiledb::sm::MimeType)mime_type);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

int32_t tiledb_mime_type_from_str(
    const char* str, tiledb_mime_type_t* mime_type) {
  tiledb::sm::MimeType val = tiledb::sm::MimeType::MIME_AUTODETECT;
  if (!tiledb::sm::mime_type_enum(str, &val).ok())
    return TILEDB_ERR;
  *mime_type = (tiledb_mime_type_t)val;
  return TILEDB_OK;
}

uint64_t compute_tile_extent_based_on_file_size(uint64_t file_size) {
  if (file_size > 1024ULL * 1024ULL * 1024ULL) {        // 1GB
    return 1024ULL * 1024ULL * 100ULL;                  // 100MB
  } else if (file_size > 1024ULL * 1024ULL * 100ULL) {  // 100MB
    return 1024ULL * 1024ULL * 1ULL;                    // 1MB
  } else if (file_size > 1024ULL * 1024ULL) {           // 1MB
    return 1024ULL * 256ULL;                            // 256KB
  } else {
    return 1024ULL;  // 1KB
  }
}

std::string libmagic_get_mime(void* data, uint64_t size) {
  magic_t magic = magic_open(MAGIC_MIME_TYPE);
  if (tiledb::sm::magic_dict::magic_mgc_embedded_load(magic)) {
    std::string errmsg(magic_error(magic));
    magic_close(magic);
    throw api::CAPIStatusException(
        std::string("Cannot load magic database - ") + errmsg);
  }
  auto rv = magic_buffer(magic, data, size);
  if (!rv) {
    std::string errmsg(magic_error(magic));
    magic_close(magic);
    throw api::CAPIStatusException(
        std::string("Cannot get the mime type - ") + errmsg);
  }
  std::string mime(rv);
  magic_close(magic);
  return mime;
}

std::string libmagic_get_mime_encoding(void* data, uint64_t size) {
  magic_t magic = magic_open(MAGIC_MIME_ENCODING);
  if (tiledb::sm::magic_dict::magic_mgc_embedded_load(magic)) {
    std::string errmsg(magic_error(magic));
    magic_close(magic);
    throw api::CAPIStatusException(
        std::string("Cannot load magic database - ") + errmsg);
  }
  auto rv = magic_buffer(magic, data, size);
  if (!rv) {
    std::string errmsg(magic_error(magic));
    magic_close(magic);
    throw api::CAPIStatusException(
        std::string("Cannot get the mime encoding - ") + errmsg);
  }
  std::string mime(rv);
  magic_close(magic);
  return mime;
}

bool libmagic_file_is_compressed(void* data, uint64_t size) {
  std::unordered_set<std::string> compressed_mime_types = {
      "application/x-bzip",
      "application/x-bzip2",
      "application/gzip",
      "application/x-7z-compressed",
      "application/zip"};

  magic_t magic = magic_open(MAGIC_MIME_TYPE);
  if (tiledb::sm::magic_dict::magic_mgc_embedded_load(magic)) {
    LOG_STATUS_NO_RETURN_VALUE(Status_Error(
        std::string("cannot load magic database - ") + magic_error(magic)));
    magic_close(magic);
    return true;
  }
  auto rv = magic_buffer(magic, data, size);
  if (!rv) {
    magic_close(magic);
    return true;
  }
  std::string mime(rv);
  magic_close(magic);

  return compressed_mime_types.find(mime) != compressed_mime_types.end();
}

void read_file_header(
    tiledb::sm::VFS& vfs, const char* uri, std::vector<char>& header) {
  const tiledb::sm::URI uri_obj = tiledb::sm::URI(uri);
  throw_if_not_ok(vfs.open_file(uri_obj, tiledb::sm::VFSMode::VFS_READ));
  throw_if_not_ok(vfs.read(uri_obj, 0, header.data(), header.size()));
  throw_if_not_ok(vfs.close_file(uri_obj));
}

std::pair<std::string, std::string> strip_file_extension(const char* file_uri) {
  std::string uri(file_uri);
  auto ext_pos = uri.find_last_of('.');
  std::string ext = uri.substr(ext_pos + 1);
  auto fname_pos = uri.find_last_of('/') + 1;

  return {uri.substr(fname_pos, ext_pos - fname_pos), ext};
}

uint64_t get_buffer_size_from_config(
    const tiledb::sm::Config& config, uint64_t tile_extent) {
  bool found = false;
  uint64_t buffer_size;
  auto st = config.get<uint64_t>("filestore.buffer_size", &buffer_size, &found);

  throw_if_not_ok(st);
  assert(found);

  if (buffer_size < tile_extent) {
    throw api::CAPIStatusException(
        "The buffer size configured via filestore.buffer_size"
        "is smaller than current " +
        std::to_string(tile_extent) + " tile extent");
  }
  // Round the buffer size down to the nearest tile
  buffer_size = buffer_size / tile_extent * tile_extent;
  return buffer_size;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_plain;
template <auto f>
constexpr auto api_entry = tiledb::api::api_entry_with_context<f>;

CAPI_INTERFACE(
    filestore_schema_create,
    tiledb_ctx_t* ctx,
    const char* uri,
    tiledb_array_schema_t** array_schema) {
  return api_entry<tiledb::api::tiledb_filestore_schema_create>(
      ctx, uri, array_schema);
}

CAPI_INTERFACE(
    filestore_uri_import,
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    const char* file_uri,
    tiledb_mime_type_t mime_type) {
  return api_entry<tiledb::api::tiledb_filestore_uri_import>(
      ctx, filestore_array_uri, file_uri, mime_type);
}

CAPI_INTERFACE(
    filestore_uri_export,
    tiledb_ctx_t* ctx,
    const char* file_uri,
    const char* filestore_array_uri) {
  return api_entry<tiledb::api::tiledb_filestore_uri_export>(
      ctx, file_uri, filestore_array_uri);
}

CAPI_INTERFACE(
    filestore_buffer_import,
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    void* buf,
    size_t size,
    tiledb_mime_type_t mime_type) {
  return api_entry<tiledb::api::tiledb_filestore_buffer_import>(
      ctx, filestore_array_uri, buf, size, mime_type);
}

CAPI_INTERFACE(
    filestore_buffer_export,
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    size_t offset,
    void* buf,
    size_t size) {
  return api_entry<tiledb::api::tiledb_filestore_buffer_export>(
      ctx, filestore_array_uri, offset, buf, size);
}

CAPI_INTERFACE(
    filestore_size,
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    size_t* size) {
  return api_entry<tiledb::api::tiledb_filestore_size>(
      ctx, filestore_array_uri, size);
}

CAPI_INTERFACE(
    mime_type_to_str, tiledb_mime_type_t mime_type, const char** str) {
  return api_entry_plain<tiledb::api::tiledb_mime_type_to_str>(mime_type, str);
}

CAPI_INTERFACE(
    mime_type_from_str, const char* str, tiledb_mime_type_t* mime_type) {
  return api_entry_plain<tiledb::api::tiledb_mime_type_from_str>(
      str, mime_type);
}
