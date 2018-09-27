/**
 * @file   consolidator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file implements the Consolidator class.
 */

#include "tiledb/sm/storage_manager/consolidator.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

#include <iostream>
#include <sstream>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Consolidator::Consolidator(StorageManager* storage_manager)
    : storage_manager_(storage_manager) {
}

Consolidator::~Consolidator() = default;

/* ****************************** */
/*               API              */
/* ****************************** */

Status Consolidator::consolidate(
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  std::vector<URI> old_fragment_uris;
  URI array_uri = URI(array_name);

  // Open array for reading
  Array array_for_reads(array_uri, storage_manager_);
  RETURN_NOT_OK(array_for_reads.open(
      QueryType::READ, encryption_type, encryption_key, key_length))
  if (array_for_reads.is_empty()) {
    RETURN_NOT_OK(array_for_reads.close());
    return Status::Ok();
  }

  // Open array for writing
  Array array_for_writes(array_uri, storage_manager_);
  RETURN_NOT_OK_ELSE(
      array_for_writes.open(
          QueryType::WRITE, encryption_type, encryption_key, key_length),
      array_for_reads.close());

  // Get schema
  auto array_schema = array_for_reads.array_schema();

  // Create subarray
  void* subarray = nullptr;
  auto st = create_subarray(&array_for_reads, &subarray);
  if (!st.ok()) {
    array_for_reads.close();
    array_for_writes.close();
    return st;
  }

  // Prepare buffers
  void** buffers;
  uint64_t* buffer_sizes;
  unsigned int buffer_num;
  st = create_buffers(array_schema, &buffers, &buffer_sizes, &buffer_num);
  if (!st.ok()) {
    array_for_reads.close();
    array_for_writes.close();
    return st;
  }

  // Create queries
  unsigned int fragment_num;
  auto query_r = (Query*)nullptr;
  auto query_w = (Query*)nullptr;
  URI new_fragment_uri;
  st = create_queries(
      &query_r,
      &query_w,
      subarray,
      &array_for_reads,
      &array_for_writes,
      buffers,
      buffer_sizes,
      &fragment_num,
      &new_fragment_uri);
  if (!st.ok()) {
    storage_manager_->array_close(array_uri, QueryType::READ);
    storage_manager_->array_close(array_uri, QueryType::WRITE);
    clean_up(subarray, buffer_num, buffers, buffer_sizes, query_r, query_w);
    return st;
  }

  // Check number of fragments
  if (fragment_num <= 1) {  // Nothing to consolidate
    storage_manager_->array_close(array_uri, QueryType::READ);
    storage_manager_->array_close(array_uri, QueryType::WRITE);
    clean_up(subarray, buffer_num, buffers, buffer_sizes, query_r, query_w);
    return Status::Ok();
  }

  // Read from one array and write to the other
  st = copy_array(query_r, query_w);
  if (!st.ok()) {
    storage_manager_->array_close(array_uri, QueryType::READ);
    storage_manager_->array_close(array_uri, QueryType::WRITE);
    clean_up(subarray, buffer_num, buffers, buffer_sizes, query_r, query_w);
    return st;
  }

  // Get old fragment uris
  old_fragment_uris = query_r->fragment_uris();

  // Close array for reading
  st = storage_manager_->array_close(array_uri, QueryType::READ);
  if (!st.ok()) {
    storage_manager_->array_close(array_uri, QueryType::WRITE);
    storage_manager_->vfs()->remove_dir(new_fragment_uri);
    clean_up(subarray, buffer_num, buffers, buffer_sizes, query_r, query_w);
    return st;
  }

  // Lock the array exclusively
  st = storage_manager_->array_xlock(array_uri);
  if (!st.ok()) {
    storage_manager_->array_close(array_uri, QueryType::WRITE);
    storage_manager_->vfs()->remove_dir(new_fragment_uri);
    clean_up(subarray, buffer_num, buffers, buffer_sizes, query_r, query_w);
    return st;
  }

  // Finalize both queries
  st = query_w->finalize();
  if (!st.ok()) {
    storage_manager_->array_close(array_uri, QueryType::WRITE);
    clean_up(subarray, buffer_num, buffers, buffer_sizes, query_r, query_w);
    storage_manager_->array_xunlock(array_uri);
    storage_manager_->vfs()->remove_dir(new_fragment_uri);
    return st;
  }

  // Close array
  st = storage_manager_->array_close(array_uri, QueryType::WRITE);
  if (!st.ok()) {
    storage_manager_->array_xunlock(array_uri);
    clean_up(subarray, buffer_num, buffers, buffer_sizes, query_r, query_w);
    storage_manager_->vfs()->remove_dir(new_fragment_uri);
    return st;
  }

  // Delete old fragment metadata. This makes the old fragments invisible
  st = delete_old_fragment_metadata(old_fragment_uris);
  if (!st.ok()) {
    delete_old_fragments(old_fragment_uris);
    storage_manager_->array_xunlock(array_uri);
    clean_up(subarray, buffer_num, buffers, buffer_sizes, query_r, query_w);
    return st;
  }

  // Unlock the array
  st = storage_manager_->array_xunlock(array_uri);
  if (!st.ok()) {
    delete_old_fragments(old_fragment_uris);
    clean_up(subarray, buffer_num, buffers, buffer_sizes, query_r, query_w);
    return st;
  }

  // Delete old fragments. The array does not need to be locked.
  st = delete_old_fragments(old_fragment_uris);

  // Clean up
  clean_up(subarray, buffer_num, buffers, buffer_sizes, query_r, query_w);

  return st;
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

Status Consolidator::copy_array(Query* query_r, Query* query_w) {
  do {
    RETURN_NOT_OK(query_r->submit());
    RETURN_NOT_OK(query_w->submit());
  } while (query_r->status() == QueryStatus::INCOMPLETE);

  return Status::Ok();
}

void Consolidator::clean_up(
    void* subarray,
    unsigned buffer_num,
    void** buffers,
    uint64_t* buffer_sizes,
    Query* query_r,
    Query* query_w) const {
  std::free(subarray);
  free_buffers(buffer_num, buffers, buffer_sizes);
  delete query_r;
  delete query_w;
}

Status Consolidator::create_buffers(
    const ArraySchema* array_schema,
    void*** buffers,
    uint64_t** buffer_sizes,
    unsigned int* buffer_num) {
  // For easy reference
  auto attribute_num = array_schema->attribute_num();
  auto dense = array_schema->dense();

  // Calculate number of buffers
  *buffer_num = 0;
  for (unsigned int i = 0; i < attribute_num; ++i)
    *buffer_num += (array_schema->attributes()[i]->var_size()) ? 2 : 1;
  *buffer_num += (dense) ? 0 : 1;

  // Create buffers
  *buffers = (void**)std::malloc(*buffer_num * sizeof(void*));
  if (*buffers == nullptr) {
    return LOG_STATUS(Status::ConsolidationError(
        "Cannot create consolidation buffers; Memory allocation failed"));
  }
  *buffer_sizes = new uint64_t[*buffer_num];
  if (*buffer_sizes == nullptr) {
    return LOG_STATUS(Status::ConsolidationError(
        "Cannot create consolidation buffer sizes; Memory allocation failed"));
  }

  // Allocate space for each buffer
  bool error = false;
  for (unsigned int i = 0; i < *buffer_num; ++i) {
    (*buffers)[i] = std::malloc(constants::consolidation_buffer_size);
    if ((*buffers)[i] == nullptr)  // The loop should continue to
      error = true;                // allocate nullptr to each buffer
    (*buffer_sizes)[i] = constants::consolidation_buffer_size;
  }

  // Clean up upon error
  if (error) {
    free_buffers(*buffer_num, *buffers, *buffer_sizes);
    *buffers = nullptr;
    *buffer_sizes = nullptr;
    return LOG_STATUS(Status::ConsolidationError(
        "Cannot create consolidation buffers; Memory allocation failed"));
  }

  // Success
  return Status::Ok();
}

Status Consolidator::create_queries(
    Query** query_r,
    Query** query_w,
    void* subarray,
    Array* array_for_reads,
    Array* array_for_writes,
    void** buffers,
    uint64_t* buffer_sizes,
    unsigned int* fragment_num,
    URI* new_fragment_uri) {
  // Create read query
  *query_r = new Query(storage_manager_, array_for_reads);
  if (!(*query_r)->array_schema()->is_kv())
    RETURN_NOT_OK((*query_r)->set_layout(Layout::GLOBAL_ORDER));
  RETURN_NOT_OK(set_query_buffers(*query_r, buffers, buffer_sizes));

  // Get fragment num and terminate with success if it is <=1
  *fragment_num = (*query_r)->fragment_num();
  if (*fragment_num <= 1)
    return Status::Ok();

  // Get last fragment URI, which will be the URI of the consolidated fragment
  *new_fragment_uri = (*query_r)->last_fragment_uri();
  RETURN_NOT_OK(rename_new_fragment_uri(new_fragment_uri));

  // Create write query
  *query_w = new Query(storage_manager_, array_for_writes, *new_fragment_uri);
  if (!(*query_w)->array_schema()->is_kv())
    RETURN_NOT_OK((*query_w)->set_layout(Layout::GLOBAL_ORDER));
  RETURN_NOT_OK((*query_w)->set_subarray(subarray));
  RETURN_NOT_OK(set_query_buffers(*query_w, buffers, buffer_sizes));

  return Status::Ok();
}

Status Consolidator::create_subarray(Array* array, void** subarray) const {
  auto array_schema = array->array_schema();
  assert(array_schema != nullptr);

  // Create subarray only for the dense case
  if (array_schema->dense()) {
    *subarray = std::malloc(2 * array_schema->coords_size());
    if (*subarray == nullptr)
      return LOG_STATUS(Status::ConsolidationError(
          "Cannot create subarray; Failed to allocate memory"));

    bool is_empty;
    RETURN_NOT_OK_ELSE(
        storage_manager_->array_get_non_empty_domain(
            array, *subarray, &is_empty),
        std::free(subarray));
    if (is_empty)
      return LOG_STATUS(
          Status::ConsolidationError("Cannot create subarray; empty array."));

    array_schema->domain()->expand_domain(*subarray);
  }

  return Status::Ok();
}

Status Consolidator::delete_old_fragment_metadata(
    const std::vector<URI>& uris) {
  for (auto& uri : uris) {
    auto meta_uri = uri.join_path(constants::fragment_metadata_filename);
    RETURN_NOT_OK(storage_manager_->vfs()->remove_file(meta_uri));
  }

  return Status::Ok();
}

Status Consolidator::delete_old_fragments(const std::vector<URI>& uris) {
  for (auto& uri : uris)
    RETURN_NOT_OK(storage_manager_->vfs()->remove_dir(uri));

  return Status::Ok();
}

void Consolidator::free_buffers(
    unsigned int buffer_num, void** buffers, uint64_t* buffer_sizes) const {
  for (unsigned int i = 0; i < buffer_num; ++i) {
    std::free(buffers[i]);
  }
  std::free(buffers);
  delete[] buffer_sizes;
}

Status Consolidator::rename_new_fragment_uri(URI* uri) const {
  // Get timestamp
  std::string name = uri->last_path_part();
  auto timestamp_str = name.substr(name.find_last_of('_') + 1);

  // Get current time
  uint64_t ms = utils::time::timestamp_now_ms();

  // Get uuid
  std::string uuid;
  RETURN_NOT_OK(uuid::generate_uuid(&uuid, false));

  std::stringstream ss;
  ss << uri->parent().to_string() << "/__" << uuid << "_" << ms << "_"
     << timestamp_str;

  *uri = URI(ss.str());
  return Status::Ok();
}

Status Consolidator::set_query_buffers(
    Query* query, void** buffers, uint64_t* buffer_sizes) const {
  auto dense = query->array_schema()->dense();
  auto attributes = query->array_schema()->attributes();
  unsigned bid = 0;
  for (const auto& attr : attributes) {
    if (!attr->var_size()) {
      RETURN_NOT_OK(
          query->set_buffer(attr->name(), buffers[bid], &buffer_sizes[bid]));
      ++bid;
    } else {
      RETURN_NOT_OK(query->set_buffer(
          attr->name(),
          (uint64_t*)buffers[bid],
          &buffer_sizes[bid],
          buffers[bid + 1],
          &buffer_sizes[bid + 1]));
      bid += 2;
    }
  }
  if (!dense)
    RETURN_NOT_OK(
        query->set_buffer(constants::coords, buffers[bid], &buffer_sizes[bid]));

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
