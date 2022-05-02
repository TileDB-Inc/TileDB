/**
 * @file   tiledb_filestore.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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

// Avoid deprecation warnings for the cpp api
#define TILEDB_DEPRECATED

#include <algorithm>

#include "tiledb/sm/c_api/api_argument_validator.h"
#include "tiledb/sm/c_api/api_exception_safety.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/cpp_api/array.h"
#include "tiledb/sm/cpp_api/array_schema.h"
#include "tiledb/sm/cpp_api/attribute.h"
#include "tiledb/sm/cpp_api/context.h"
#include "tiledb/sm/cpp_api/dimension.h"
#include "tiledb/sm/cpp_api/filter_list.h"
#include "tiledb/sm/cpp_api/query.h"
#include "tiledb/sm/cpp_api/subarray.h"
#include "tiledb/sm/cpp_api/vfs.h"
#include "tiledb/sm/enums/mime_type.h"
#include "tiledb/sm/misc/mgc_dict.h"

namespace tiledb::common::detail {

// Forward declarations
uint64_t compute_tile_extent_based_on_file_size(uint64_t file_size);
std::string libmagic_get_mime(void* data, uint64_t size);
std::string libmagic_get_mime_encoding(void* data, uint64_t size);
bool libmagic_file_is_compressed(void* data, uint64_t size);
Status read_file_header(
    const VFS& vfs, const char* uri, std::vector<char>& header);

TILEDB_EXPORT int32_t tiledb_filestore_schema_create(
    tiledb_ctx_t* ctx,
    const char* uri,
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT {
  if (sanity_check(ctx) == TILEDB_ERR) {
    *array_schema = nullptr;
    return TILEDB_ERR;
  }

  Context context(ctx, false);
  uint64_t tile_extent = tiledb::sm::constants::filestore_default_tile_extent;

  bool is_compressed_libmagic = true;
  if (uri) {
    // The user provided a uri, let's examine the file and get some insights
    // Get the file size, calculate a reasonable tile extent
    VFS vfs(context);
    uint64_t file_size = vfs.file_size(std::string(uri));
    if (file_size) {
      tile_extent = compute_tile_extent_based_on_file_size(file_size);
    }

    // Detect if the file is compressed or not
    uint64_t size = std::min(file_size, 1024ULL);
    std::vector<char> header(size);
    auto st = read_file_header(vfs, uri, header);
    // Don't fail if compression cannot be detected, log a message
    // and default to uncompressed array
    if (st.ok()) {
      is_compressed_libmagic = libmagic_file_is_compressed(header.data(), size);
    } else {
      LOG_STATUS(
          Status_Error("Compression couldn't be detected - " + st.message()));
    }
  }

  *array_schema = new tiledb_array_schema_t;

  ArraySchema schema(context, TILEDB_DENSE);

  // Share ownership of the internal ArraySchema ptr
  // All other calls for adding domains, attributes, etc
  // create copies of the underlying core objects from within
  // the cpp objects constructed here
  (*array_schema)->array_schema_ = schema.ptr()->array_schema_;

  auto dim = Dimension::create<uint64_t>(
      context,
      tiledb::sm::constants::filestore_dimension_name,
      {0, std::numeric_limits<uint64_t>::max() - tile_extent - 1},
      tile_extent);

  Domain domain(context);
  domain.add_dimension(dim);

  auto attr = Attribute::create(
      context, tiledb::sm::constants::filestore_attribute_name, TILEDB_BLOB);

  // If the input file is not compressed, add our own compression
  if (!is_compressed_libmagic) {
    FilterList filter(context);
    filter.add_filter({context, TILEDB_FILTER_ZSTD});
    attr.set_filter_list(filter);
  }

  schema.set_domain(domain);
  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(attr);

  return TILEDB_OK;
}

TILEDB_EXPORT int32_t tiledb_filestore_uri_import(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    const char* file_uri,
    tiledb_mime_type_t mime_type) TILEDB_NOEXCEPT {
  (void)mime_type;  // Unused

  if (sanity_check(ctx) == TILEDB_ERR || !filestore_array_uri || !file_uri) {
    return TILEDB_ERR;
  }

  Context context(ctx, false);

  // Get the file size
  VFS vfs(context);
  uint64_t file_size = vfs.file_size(std::string(file_uri));
  if (!file_size) {
    return TILEDB_OK;  // NOOP
  }

  // Sync up the fragment timestamp and metadata timestamp
  uint64_t time_now = tiledb_timestamp_now_ms();
  Array array(
      context, std::string(filestore_array_uri), TILEDB_WRITE, time_now);

  // Detect mimetype and encoding with libmagic
  std::string mime = "";
  std::string mime_encoding = "";
  uint64_t size = std::min(file_size, 1024ULL);
  std::vector<char> header(size);
  auto st = read_file_header(vfs, file_uri, header);
  if (st.ok()) {
    mime = libmagic_get_mime(header.data(), header.size());
    mime_encoding = libmagic_get_mime_encoding(header.data(), header.size());
  } else {
    LOG_STATUS(Status_Error(
        "MIME type and encoding couldn't be detected - " + st.message()));
  }

  // We need to dump all the relevant metadata at this point so that
  // clients have all the necessary info when consuming the array
  array.put_metadata(
      tiledb::sm::constants::filestore_metadata_size_key,
      TILEDB_UINT64,
      1,
      &file_size);
  array.put_metadata(
      tiledb::sm::constants::filestore_metadata_mime_encoding_key,
      TILEDB_STRING_ASCII,
      mime_encoding.size(),
      mime_encoding.c_str());
  array.put_metadata(
      tiledb::sm::constants::filestore_metadata_mime_type_key,
      TILEDB_STRING_ASCII,
      mime.size(),
      mime.c_str());

  // Write the data in batches using the global order writer
  VFS::filebuf fb(vfs);
  if (!fb.open(std::string(file_uri), std::ios::in)) {
    auto st = Status_Error(
        "Failed to open the file; Invalid file URI or incorrect file "
        "permissions");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  std::istream input(&fb);

  uint64_t tile_extent = compute_tile_extent_based_on_file_size(file_size);
  Query query(context, array);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  std::vector<std::byte> buffer(tile_extent);

  Subarray subarray(context, array);
  // We need to get the right end boundary of the last space tile.
  // The last chunk either falls exactly on the end of the file
  // or it goes beyond the end of the file so that it's equal in size
  // to the tile extent
  uint64_t last_space_tile_boundary =
      (file_size / tile_extent +
       static_cast<uint64_t>(file_size % tile_extent != 0)) *
          tile_extent -
      1;
  subarray.add_range(0, static_cast<uint64_t>(0), last_space_tile_boundary);
  query.set_subarray(subarray);

  while (input.read(reinterpret_cast<char*>(buffer.data()), tile_extent)) {
    query.set_data_buffer(
        tiledb::sm::constants::filestore_attribute_name,
        buffer.data(),
        buffer.size());
    query.submit();
  }

  // If the end of the file was reached, but less than tile_extent bytes
  // were read, write the read bytes into the array.
  // Check input.gcount() to guard against the case when file_size is
  // a multiple of tile_extent, thus eof gets set but there are no more
  // bytes to read
  if (input.eof() && input.gcount() > 0) {
    size_t read_bytes = input.gcount();
    // Initialize the remaining empty cells to 0
    std::memset(buffer.data() + read_bytes, 0, buffer.size() - read_bytes);
    query.set_data_buffer(
        tiledb::sm::constants::filestore_attribute_name,
        buffer.data(),
        buffer.size());
    query.submit();
  } else if (!input.eof()) {
    // Something must have gone wrong whilst reading the file
    auto st = Status_Error("Error whilst reading the file");
    LOG_STATUS(st);
    save_error(ctx, st);
    fb.close();
    return TILEDB_ERR;
  }

  // Dump the fragment on disk
  query.finalize();
  fb.close();

  return TILEDB_OK;
}

TILEDB_EXPORT int32_t tiledb_filestore_uri_export(
    tiledb_ctx_t* ctx,
    const char* file_uri,
    const char* filestore_array_uri) TILEDB_NOEXCEPT {
  if (sanity_check(ctx) == TILEDB_ERR || !filestore_array_uri || !file_uri) {
    return TILEDB_ERR;
  }

  Context context(ctx, false);

  VFS vfs(context);
  VFS::filebuf fb(vfs);
  if (!fb.open(std::string(file_uri), std::ios::out)) {
    auto st = Status_Error(
        "Failed to open the file; Invalid file URI or incorrect file "
        "permissions");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  std::ostream output(&fb);

  Array array(context, std::string(filestore_array_uri), TILEDB_READ);
  tiledb_datatype_t dtype;
  uint32_t num;
  const void* size;
  array.get_metadata(
      tiledb::sm::constants::filestore_metadata_size_key, &dtype, &num, &size);
  if (!size) {
    auto st = Status_Error(
        "The array metadata doesn't contain the " +
        tiledb::sm::constants::filestore_metadata_size_key + "key");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  uint64_t file_size = *static_cast<const uint64_t*>(size);
  uint64_t tile_extent = compute_tile_extent_based_on_file_size(file_size);

  std::vector<std::byte> data(tile_extent);
  uint64_t start_range = 0;
  uint64_t end_range = std::min(file_size, tile_extent) - 1;
  do {
    uint64_t write_size = end_range - start_range + 1;
    Subarray subarray(context, array);
    subarray.add_range(0, start_range, end_range);
    Query query(context, array);
    query.set_layout(TILEDB_ROW_MAJOR);
    query.set_subarray(subarray);
    query.set_data_buffer(
        tiledb::sm::constants::filestore_attribute_name,
        data.data(),
        write_size);
    query.submit();

    output.write(reinterpret_cast<char*>(data.data()), write_size);

    start_range = end_range + 1;
    end_range = std::min(file_size - 1, end_range + tile_extent);
  } while (start_range <= end_range);

  output.flush();
  fb.close();

  return TILEDB_OK;
}

TILEDB_EXPORT int32_t tiledb_filestore_buffer_import(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    void* buf,
    size_t size,
    tiledb_mime_type_t mime_type) TILEDB_NOEXCEPT {
  (void)mime_type;  // Unused

  if (sanity_check(ctx) == TILEDB_ERR || !filestore_array_uri || !buf) {
    return TILEDB_ERR;
  }

  if (!size) {
    return TILEDB_OK;  // NOOP
  }

  Context context(ctx, false);

  // Sync up the fragment timestamp and metadata timestamp
  uint64_t time_now = tiledb_timestamp_now_ms();
  Array array(
      context, std::string(filestore_array_uri), TILEDB_WRITE, time_now);

  // Detect mimetype and encoding with libmagic
  uint64_t s = std::min(size, 1024UL);
  std::string mime = libmagic_get_mime(buf, s);
  std::string mime_encoding = libmagic_get_mime_encoding(buf, s);

  // We need to dump all the relevant metadata at this point so that
  // clients have all the necessary info when consuming the array
  array.put_metadata(
      tiledb::sm::constants::filestore_metadata_size_key,
      TILEDB_UINT64,
      1,
      &size);
  array.put_metadata(
      tiledb::sm::constants::filestore_metadata_mime_encoding_key,
      TILEDB_STRING_ASCII,
      mime_encoding.size(),
      mime_encoding.c_str());
  array.put_metadata(
      tiledb::sm::constants::filestore_metadata_mime_type_key,
      TILEDB_STRING_ASCII,
      mime.size(),
      mime.c_str());

  Query query(context, array);
  query.set_layout(TILEDB_ROW_MAJOR);

  Subarray subarray(context, array);
  subarray.add_range(
      0, static_cast<uint64_t>(0), static_cast<uint64_t>(size - 1));
  query.set_subarray(subarray);
  query.set_data_buffer(
      tiledb::sm::constants::filestore_attribute_name, buf, size);
  query.submit();

  return TILEDB_OK;
}

TILEDB_EXPORT int32_t tiledb_filestore_buffer_export(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    size_t offset,
    void* buf,
    size_t size) TILEDB_NOEXCEPT {
  if (sanity_check(ctx) == TILEDB_ERR || !filestore_array_uri || !buf) {
    return TILEDB_ERR;
  }

  Context context(ctx, false);

  Array array(context, std::string(filestore_array_uri), TILEDB_READ);

  // Check whether the user requested more data than the array contains.
  // Return an error if that's the case.
  // This is valid only when the array metadata contains the file_size key
  tiledb_datatype_t dtype;
  uint32_t num;
  const void* file_size;
  array.get_metadata(
      tiledb::sm::constants::filestore_metadata_size_key,
      &dtype,
      &num,
      &file_size);
  if (file_size && *static_cast<const uint64_t*>(file_size) < offset + size) {
    auto st =
        Status_Error("The number of bytes requested is bigger than the array");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  Subarray subarray(context, array);
  subarray.add_range(
      0,
      static_cast<uint64_t>(offset),
      static_cast<uint64_t>(offset + size - 1));
  Query query(context, array);
  query.set_layout(TILEDB_ROW_MAJOR);
  query.set_subarray(subarray);
  query.set_data_buffer(
      tiledb::sm::constants::filestore_attribute_name, buf, size);
  query.submit();

  return TILEDB_OK;
}

TILEDB_EXPORT int32_t tiledb_filestore_size(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    size_t* size) TILEDB_NOEXCEPT {
  if (sanity_check(ctx) == TILEDB_ERR || !filestore_array_uri || !size) {
    return TILEDB_ERR;
  }

  Context context(ctx, false);
  Array array(context, std::string(filestore_array_uri), TILEDB_READ);

  tiledb_datatype_t dtype;
  uint32_t num;
  const void* file_size;
  array.get_metadata(
      tiledb::sm::constants::filestore_metadata_size_key,
      &dtype,
      &num,
      &file_size);
  *size = *static_cast<const uint64_t*>(file_size);

  return TILEDB_OK;
}

TILEDB_EXPORT int32_t tiledb_mime_type_to_str(
    tiledb_mime_type_t mime_type, const char** str) TILEDB_NOEXCEPT {
  const auto& strval =
      tiledb::sm::mime_type_str((tiledb::sm::MimeType)mime_type);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

TILEDB_EXPORT int32_t tiledb_mime_type_from_str(
    const char* str, tiledb_mime_type_t* mime_type) TILEDB_NOEXCEPT {
  tiledb::sm::MimeType val = tiledb::sm::MimeType::MIME_NONE;
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
    LOG_STATUS(Status_Error(
        std::string("Cannot load magic database - ") + magic_error(magic)));
    magic_close(magic);
    return "";
  }
  auto rv = magic_buffer(magic, data, size);
  if (!rv) {
    LOG_STATUS(Status_Error(
        std::string("Cannot get the mime type - ") + magic_error(magic)));
    return "";
  }
  return rv;
}

std::string libmagic_get_mime_encoding(void* data, uint64_t size) {
  magic_t magic = magic_open(MAGIC_MIME_ENCODING);
  if (tiledb::sm::magic_dict::magic_mgc_embedded_load(magic)) {
    LOG_STATUS(Status_Error(
        std::string("Cannot load magic database - ") + magic_error(magic)));
    magic_close(magic);
    return "";
  }
  auto rv = magic_buffer(magic, data, size);
  if (!rv) {
    LOG_STATUS(Status_Error(
        std::string("Cannot get the mime encoding - ") + magic_error(magic)));
    return "";
  }
  return rv;
}

bool libmagic_file_is_compressed(void* data, uint64_t size) {
  magic_t magic = magic_open(MAGIC_MIME_ENCODING);
  if (tiledb::sm::magic_dict::magic_mgc_embedded_load(magic)) {
    LOG_STATUS(Status_Error(
        std::string("cannot load magic database - ") + magic_error(magic)));
    magic_close(magic);
    return true;
  }
  return strcmp(magic_buffer(magic, data, size), "binary") == 0;
}

Status read_file_header(
    const VFS& vfs, const char* uri, std::vector<char>& header) {
  VFS::filebuf fb(vfs);
  if (!fb.open(uri, std::ios::in)) {
    return Status_Error("the file couldn't be opened");
  }
  std::istream input(&fb);
  if (!input.read(header.data(), header.size())) {
    return Status_Error("the file couldn't be read");
  }
  fb.close();
  return Status::Ok();
}

}  // namespace tiledb::common::detail

TILEDB_EXPORT int32_t tiledb_filestore_schema_create(
    tiledb_ctx_t* ctx,
    const char* uri,
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_filestore_schema_create>(
      ctx, uri, array_schema);
}

TILEDB_EXPORT int32_t tiledb_filestore_uri_import(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    const char* file_uri,
    tiledb_mime_type_t mime_type) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_filestore_uri_import>(
      ctx, filestore_array_uri, file_uri, mime_type);
}

TILEDB_EXPORT int32_t tiledb_filestore_uri_export(
    tiledb_ctx_t* ctx,
    const char* file_uri,
    const char* filestore_array_uri) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_filestore_uri_export>(
      ctx, file_uri, filestore_array_uri);
}

TILEDB_EXPORT int32_t tiledb_filestore_buffer_import(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    void* buf,
    size_t size,
    tiledb_mime_type_t mime_type) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_filestore_buffer_import>(
      ctx, filestore_array_uri, buf, size, mime_type);
}

TILEDB_EXPORT int32_t tiledb_filestore_buffer_export(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    size_t offset,
    void* buf,
    size_t size) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_filestore_buffer_export>(
      ctx, filestore_array_uri, offset, buf, size);
}

TILEDB_EXPORT int32_t tiledb_filestore_size(
    tiledb_ctx_t* ctx,
    const char* filestore_array_uri,
    size_t* size) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_filestore_size>(
      ctx, filestore_array_uri, size);
}

TILEDB_EXPORT int32_t tiledb_mime_type_to_str(
    tiledb_mime_type_t mime_type, const char** str) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_mime_type_to_str>(mime_type, str);
}

TILEDB_EXPORT int32_t tiledb_mime_type_from_str(
    const char* str, tiledb_mime_type_t* mime_type) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_mime_type_from_str>(str, mime_type);
}
