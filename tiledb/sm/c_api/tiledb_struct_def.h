/**
 * @file   tiledb_struct_def.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file contains the TileDB C API struct object definitions.
 */

#ifndef TILEDB_C_API_STRUCT_DEF_H
#define TILEDB_C_API_STRUCT_DEF_H

#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/buffer/buffer_list.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/config/config_iter.h"
#include "tiledb/sm/filesystem/vfs_file_handle.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/storage_manager/context.h"
#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"

struct tiledb_array_t {
  tiledb::sm::Array* array_ = nullptr;
};

struct tiledb_buffer_t {
  tiledb::sm::Datatype datatype_ = tiledb::sm::Datatype::UINT8;
  tiledb::sm::Buffer* buffer_ = nullptr;
};

struct tiledb_buffer_list_t {
  tiledb::sm::BufferList* buffer_list_ = nullptr;
};

struct tiledb_config_t {
  tiledb::sm::Config* config_ = nullptr;
};

struct tiledb_config_iter_t {
  tiledb::sm::ConfigIter* config_iter_ = nullptr;
};

struct tiledb_ctx_t {
  tiledb::sm::Context* ctx_ = nullptr;
};

struct tiledb_error_t {
  std::string errmsg_;
};

struct tiledb_attribute_t {
  tiledb::sm::Attribute* attr_ = nullptr;
};

struct tiledb_array_schema_t {
  tiledb::sm::ArraySchema* array_schema_ = nullptr;
};

struct tiledb_dimension_t {
  tiledb::sm::Dimension* dim_ = nullptr;
};

struct tiledb_domain_t {
  tiledb::sm::Domain* domain_ = nullptr;
};

struct tiledb_filter_t {
  tiledb::sm::Filter* filter_ = nullptr;
};

struct tiledb_filter_list_t {
  tiledb::sm::FilterPipeline* pipeline_ = nullptr;
};

struct tiledb_query_t {
  tiledb::sm::Query* query_ = nullptr;
};

struct tiledb_vfs_t {
  tiledb::sm::VFS* vfs_ = nullptr;
};

struct tiledb_vfs_fh_t {
  tiledb::sm::VFSFileHandle* vfs_fh_ = nullptr;
};

#endif