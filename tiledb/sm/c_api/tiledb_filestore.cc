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

namespace tiledb::common::detail {

size_t compute_tile_extent_based_on_file_size(size_t file_size);

TILEDB_EXPORT int32_t tiledb_filestore_schema_create(
    tiledb_ctx_t* ctx,
    const char* uri,
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT {
  if (sanity_check(ctx) == TILEDB_ERR) {
    *array_schema = nullptr;
    return TILEDB_ERR;
  }

  Context context(ctx, false);
  size_t tile_extent = tiledb::sm::constants::filestore_default_tile_extent;

  // The user provided a uri, let's examine the file and get some insights
  bool is_compressed_libmagic = true;
  if (uri) {
    // Get the file size, calculate a reasonable tile extent
    VFS vfs(context);
    size_t file_size = vfs.file_size(std::string(uri));
    if (file_size) {
      tile_extent = compute_tile_extent_based_on_file_size(file_size);
    }

    // TODO: detect compression is_compressed_libmagic=false/true
    is_compressed_libmagic = true;
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
    const char* file_uri) TILEDB_NOEXCEPT {
  if (sanity_check(ctx) == TILEDB_ERR || !filestore_array_uri || !file_uri) {
    return TILEDB_ERR;
  }

  Context context(ctx, false);

  // Get the file size
  VFS vfs(context);
  size_t file_size = vfs.file_size(std::string(file_uri));
  if (!file_size) {
    return TILEDB_OK;  // NOOP
  }

  Array array(context, std::string(filestore_array_uri), TILEDB_WRITE);

  // TODO: detect mimetype and encoding with libmagic
  std::string mime_type = "application/pdf";
  std::string mime_encoding = "us-ascii";

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
      mime_type.size(),
      mime_type.c_str());

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

  size_t tile_extent = compute_tile_extent_based_on_file_size(file_size);
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

  if (input.eof()) {
    size_t read_bytes = input.gcount();
    // Initialize the remaining empty cells to 0
    std::memset(buffer.data() + read_bytes, 0, buffer.size() - read_bytes);
    query.set_data_buffer(
        tiledb::sm::constants::filestore_attribute_name,
        buffer.data(),
        buffer.size());
    query.submit();
  } else {
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
    tiledb_ctx_t* ctx, const char* filestore_array_uri, void* buf, size_t size)
    TILEDB_NOEXCEPT {
  if (sanity_check(ctx) == TILEDB_ERR || !filestore_array_uri || !buf) {
    return TILEDB_ERR;
  }

  if (!size) {
    return TILEDB_OK;  // NOOP
  }

  Context context(ctx, false);
  Array array(context, std::string(filestore_array_uri), TILEDB_WRITE);

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
    tiledb_ctx_t* ctx, const char* filestore_array_uri, void* buf, size_t size)
    TILEDB_NOEXCEPT {
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
  if (file_size && *static_cast<const uint64_t*>(file_size) < size) {
    auto st =
        Status_Error("The number of bytes requested is bigger than the array");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  Subarray subarray(context, array);
  subarray.add_range(
      0, static_cast<uint64_t>(0), static_cast<uint64_t>(size - 1));
  Query query(context, array);
  query.set_layout(TILEDB_ROW_MAJOR);
  query.set_subarray(subarray);
  query.set_data_buffer(
      tiledb::sm::constants::filestore_attribute_name, buf, size);
  query.submit();

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

size_t compute_tile_extent_based_on_file_size(size_t file_size) {
  if (file_size > 1024ULL * 1024ULL * 1024ULL * 10ULL) {  // 10GB
    return 1024ULL * 1024ULL * 100ULL;                    // 100MB
  } else if (file_size > 1024ULL * 1024ULL * 100ULL) {    // 100MB
    return 1024ULL * 1024ULL * 1ULL;                      // 1MB
  } else if (file_size > 1024ULL * 1024ULL) {             // 1MB
    return 1024ULL * 256ULL;                              // 256KB
  } else {
    return 1024ULL;  // 1KB
  }
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
    const char* file_uri) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_filestore_uri_import>(
      ctx, filestore_array_uri, file_uri);
}

TILEDB_EXPORT int32_t tiledb_filestore_uri_export(
    tiledb_ctx_t* ctx,
    const char* file_uri,
    const char* filestore_array_uri) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_filestore_uri_export>(
      ctx, file_uri, filestore_array_uri);
}

TILEDB_EXPORT int32_t tiledb_filestore_buffer_import(
    tiledb_ctx_t* ctx, const char* filestore_array_uri, void* buf, size_t size)
    TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_filestore_buffer_import>(
      ctx, filestore_array_uri, buf, size);
}

TILEDB_EXPORT int32_t tiledb_filestore_buffer_export(
    tiledb_ctx_t* ctx, const char* filestore_array_uri, void* buf, size_t size)
    TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_filestore_buffer_export>(
      ctx, filestore_array_uri, buf, size);
}

TILEDB_EXPORT int32_t tiledb_mime_type_to_str(
    tiledb_mime_type_t mime_type, const char** str) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_mime_type_to_str>(mime_type, str);
}

TILEDB_EXPORT int32_t tiledb_mime_type_from_str(
    const char* str, tiledb_mime_type_t* mime_type) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_mime_type_from_str>(str, mime_type);
}
