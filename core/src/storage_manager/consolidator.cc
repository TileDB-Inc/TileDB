/**
 * @file   consolidator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include "consolidator.h"
#include "logger.h"
#include "storage_manager.h"

#include <sys/time.h>
#include <sstream>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

namespace tiledb {

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

Status Consolidator::consolidate(const char* array_name) {
  std::vector<URI> old_fragment_uris;
  URI new_fragment_uri;
  URI array_uri = URI(array_name);

  // Get array metadata
  auto array_meta = (ArrayMetadata*)nullptr;
  RETURN_NOT_OK(storage_manager_->load_array_metadata(array_uri, &array_meta));

  // Prepare buffers
  void** buffers;
  uint64_t* buffer_sizes;
  unsigned int buffer_num;
  RETURN_NOT_OK_ELSE(
      create_buffers(array_meta, &buffers, &buffer_sizes, &buffer_num),
      delete array_meta);

  // Create queries
  unsigned int fragment_num;
  auto query_r = new Query();
  auto query_w = new Query();
  Status st = create_queries(
      query_r, query_w, array_name, buffers, buffer_sizes, &fragment_num);
  if (!st.ok())
    goto clean_up;

  // Check number of fragments
  if (fragment_num <= 1)  // Nothing to consolidate
    goto clean_up;

  // Read from one array and write to the other
  st = copy_array(query_r, query_w);
  if (!st.ok())
    goto clean_up;

  // Get old fragment uris
  old_fragment_uris = query_r->fragment_uris();

  // Finalize both queries
  st = finalize_queries(query_r, query_w);
  if (!st.ok())
    goto clean_up;

  // Lock the array exclusively
  st = storage_manager_->array_lock(array_uri, false);
  if (!st.ok())
    goto clean_up;

  // Delete old fragments
  st = delete_old_fragments(old_fragment_uris);
  if (!st.ok()) {
    storage_manager_->array_unlock(array_uri, false);
    goto clean_up;
  }

  // Unlock the array
  st = storage_manager_->array_unlock(array_uri, false);

// Clean up
clean_up:
  delete array_meta;
  free_buffers(buffer_num, buffers, buffer_sizes);
  delete query_r;
  delete query_w;

  return st;
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

Status Consolidator::copy_array(Query* query_r, Query* query_w) {
  do {
    RETURN_NOT_OK(storage_manager_->query_submit(query_r));
    RETURN_NOT_OK(storage_manager_->query_submit(query_w));
  } while (query_r->status() == QueryStatus::INCOMPLETE);

  return Status::Ok();
}

Status Consolidator::create_buffers(
    ArrayMetadata* array_meta,
    void*** buffers,
    uint64_t** buffer_sizes,
    unsigned int* buffer_num) {
  // For easy reference
  auto attribute_num = array_meta->attribute_num();
  auto dense = array_meta->dense();

  // Calculate number of buffers
  *buffer_num = 0;
  for (unsigned int i = 0; i < attribute_num; ++i)
    *buffer_num += (array_meta->var_size(i)) ? 2 : 1;
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
    Query* query_r,
    Query* query_w,
    const char* array_name,
    void** buffers,
    uint64_t* buffer_sizes,
    unsigned int* fragment_num) {
  // Create read query
  RETURN_NOT_OK(storage_manager_->query_init(
      query_r,
      array_name,
      QueryType::READ,
      Layout::GLOBAL_ORDER,
      nullptr,
      nullptr,
      0,
      buffers,
      buffer_sizes));

  // Get fragment num and terminate with success if it is <=1
  *fragment_num = query_r->fragment_num();
  if (*fragment_num <= 1)
    return Status::Ok();

  // Get last fragment URI, which will be the URI of the consolidated fragment
  URI new_fragment_uri = query_r->last_fragment_uri();
  RETURN_NOT_OK(rename_new_fragment_uri(&new_fragment_uri));

  // Create write query
  RETURN_NOT_OK(storage_manager_->query_init(
      query_w,
      array_name,
      QueryType::WRITE,
      Layout::GLOBAL_ORDER,
      nullptr,
      nullptr,
      0,
      buffers,
      buffer_sizes,
      new_fragment_uri));

  return Status::Ok();
}

Status Consolidator::delete_old_fragments(const std::vector<URI>& uris) {
  for (auto& uri : uris)
    RETURN_NOT_OK(storage_manager_->delete_fragment(uri));

  return Status::Ok();
}

Status Consolidator::finalize_queries(Query* query_r, Query* query_w) {
  RETURN_NOT_OK(storage_manager_->query_finalize(query_r));
  RETURN_NOT_OK(storage_manager_->query_finalize(query_w));

  return Status::Ok();
}

void Consolidator::free_buffers(
    unsigned int buffer_num, void** buffers, uint64_t* buffer_sizes) {
  for (unsigned int i = 0; i < buffer_num; ++i) {
    if (buffers[i] != nullptr)
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
  struct timeval tp;
  memset(&tp, 0, sizeof(struct timeval));
  gettimeofday(&tp, nullptr);
  auto ms = static_cast<uint64_t>(tp.tv_sec * 1000L + tp.tv_usec / 1000);

  std::stringstream ss;
  ss << uri->parent().to_string() << "/__" << std::this_thread::get_id() << "_"
     << ms << "_" << timestamp_str;

  *uri = URI(ss.str());
  return Status::Ok();
}

}  // namespace tiledb
