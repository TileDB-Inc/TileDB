/**
 * @file   consolidator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_info.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

#include <iostream>
#include <sstream>

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
    uint32_t key_length,
    const Config* config) {
  // Set config parameters
  RETURN_NOT_OK(set_config(config));

  URI array_uri = URI(array_name);
  EncryptionKey enc_key;
  RETURN_NOT_OK(enc_key.set_key(encryption_type, encryption_key, key_length));

  // Get array schema
  auto array_schema = (ArraySchema*)nullptr;
  RETURN_NOT_OK(
      storage_manager_->load_array_schema(array_uri, enc_key, &array_schema));

  RETURN_NOT_OK_ELSE(
      consolidate(array_schema, encryption_type, encryption_key, key_length),
      delete array_schema);

  delete array_schema;

  return Status::Ok();
}

Status Consolidator::consolidate_array_metadata(
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    const Config* config) {
  // Config not necessary yet
  (void)config;

  // Open array for reading
  auto array_uri = URI(array_name);
  Array array_for_reads(array_uri, storage_manager_);
  RETURN_NOT_OK(array_for_reads.open(
      QueryType::READ, encryption_type, encryption_key, key_length));

  // Open array for writing
  Array array_for_writes(array_uri, storage_manager_);
  RETURN_NOT_OK_ELSE(
      array_for_writes.open(
          QueryType::WRITE, encryption_type, encryption_key, key_length),
      array_for_reads.close());

  // Swap the in-memory metadata between the two arrays.
  // After that, the array for writes will store the (consolidated by
  // the way metadata loading works) metadata of the array for reads
  Metadata* metadata_r;
  RETURN_NOT_OK_ELSE(
      array_for_reads.metadata(&metadata_r), array_for_reads.close());
  Metadata* metadata_w;
  RETURN_NOT_OK_ELSE(
      array_for_writes.metadata(&metadata_w), array_for_reads.close());
  metadata_r->swap(metadata_w);

  // Metadata uris to delete
  const auto to_delete = metadata_w->loaded_metadata_uris();

  // Close arrays
  RETURN_NOT_OK_ELSE(array_for_reads.close(), array_for_writes.close());
  RETURN_NOT_OK(array_for_writes.close());

  for (auto& uri : to_delete)
    RETURN_NOT_OK(storage_manager_->vfs()->remove_file(uri));

  return Status::Ok();
}

/* ****************************** */
/*        STATIC FUNCTIONS        */
/* ****************************** */

void Consolidator::remove_consolidated_uris(
    std::vector<TimestampedURI>* sorted_uris) {
  // Trivial case
  if (sorted_uris->size() <= 1)
    return;

  // Put the URIs in a list, so that it is easy to remove "covered" URIs
  std::list<TimestampedURI> uri_list;
  for (auto f : *sorted_uris)
    uri_list.push_back(f);

  // NOTE: all fragments in uri_list are sorted on start timestamps,
  // and there are no overlpas (by definition when creating the
  // names of the fragments resulting from consolidation).

  for (auto it = uri_list.begin(); it != uri_list.end(); ++it) {
    // Do nothing for unary timestamp ranges, we cannot use them
    // to remove any fragments
    if (it->has_unary_timestamp_range())
      continue;

    // Move backwards and remove "covered" URIs. A previous URI is
    // "covered" if its first timestamp is the same as that of the current
    // URI. This is due to the lexicographic sorting of the
    // URIs. For instance, if the current URI range is [5,10],
    // then URI with range [5,5] will definitely preceed that URI and will
    // be "covered".
    if (it != uri_list.begin()) {
      auto it_prev = std::prev(it);
      while (it->timestamp_range_.first == it_prev->timestamp_range_.first) {
        it_prev = uri_list.erase(it_prev);
        if (it_prev == uri_list.begin())
          break;
        it_prev = std::prev(it_prev);
      }
    }

    if (it == uri_list.end())
      break;

    // Move forward and remove "covered" URIs. A next URI is covered
    // if its timestamp range is included in the timestamp range of
    // the current fragment URI. For instance, if the current URI
    // has range [1,10] and the next URI has range [3,4], then the
    // URI with range [3,4] will be removed
    auto it_next = std::next(it);
    while (it_next != uri_list.end() &&
           it_next->timestamp_range_.first >= it->timestamp_range_.first &&
           it_next->timestamp_range_.second <= it->timestamp_range_.second) {
      it_next = uri_list.erase(it_next);
    }
  }

  // Update the sorted fragment URIs
  sorted_uris->clear();
  for (const auto& f : uri_list)
    sorted_uris->push_back(f);
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

bool Consolidator::all_sparse(
    const std::vector<FragmentInfo>& fragments,
    size_t start,
    size_t end) const {
  for (size_t i = start; i <= end; ++i) {
    if (!fragments[i].sparse())
      return false;
  }

  return true;
}

bool Consolidator::are_consolidatable(
    const Domain* domain,
    const std::vector<FragmentInfo>& fragments,
    size_t start,
    size_t end,
    const NDRange& union_non_empty_domains) const {
  // True if all fragments in [start, end] are sparse
  if (all_sparse(fragments, start, end))
    return true;

  // Check overlap of union with earlier fragments
  for (size_t i = 0; i < start; ++i) {
    if (domain->overlap(
            union_non_empty_domains, fragments[i].non_empty_domain()))
      return false;
  }

  // Check consolidation amplification factor
  auto union_cell_num = domain->cell_num(union_non_empty_domains);
  uint64_t sum_cell_num = 0;
  for (size_t i = start; i <= end; ++i) {
    sum_cell_num += domain->cell_num(fragments[i].expanded_non_empty_domain());
  }

  return (double(union_cell_num) / sum_cell_num) <= config_.amplification_;
}

Status Consolidator::consolidate(
    const ArraySchema* array_schema,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  std::vector<FragmentInfo> to_consolidate;
  auto timestamp = utils::time::timestamp_now_ms();
  auto array_uri = array_schema->array_uri();
  EncryptionKey enc_key;
  RETURN_NOT_OK(enc_key.set_key(encryption_type, encryption_key, key_length));

  // Get fragment info
  std::vector<FragmentInfo> fragment_info;
  RETURN_NOT_OK(storage_manager_->get_fragment_info(
      array_schema, timestamp, enc_key, &fragment_info));

  // First make a pass and delete any entirely overwritten fragments
  RETURN_NOT_OK(delete_overwritten_fragments(array_schema, &fragment_info));

  uint32_t step = 0;
  do {
    // No need to consolidate if no more than 1 fragment exist
    if (fragment_info.size() <= 1)
      break;

    // Find the next fragments to be consolidated
    NDRange union_non_empty_domains;
    RETURN_NOT_OK(compute_next_to_consolidate(
        array_schema,
        fragment_info,
        &to_consolidate,
        &union_non_empty_domains));

    // Check if there is anything to consolidate
    if (to_consolidate.size() <= 1)
      break;

    // Consolidate the selected fragments
    URI new_fragment_uri;
    RETURN_NOT_OK(consolidate(
        array_uri,
        to_consolidate,
        union_non_empty_domains,
        encryption_type,
        encryption_key,
        key_length,
        &new_fragment_uri));

    // Get fragment info of the consolidated fragment
    FragmentInfo new_fragment_info;
    RETURN_NOT_OK(storage_manager_->get_fragment_info(
        array_schema, enc_key, new_fragment_uri, &new_fragment_info));

    // Update fragment info
    update_fragment_info(to_consolidate, new_fragment_info, &fragment_info);

    // Advance number of steps
    ++step;

  } while (step < config_.steps_);

  return Status::Ok();
}

Status Consolidator::consolidate(
    const URI& array_uri,
    const std::vector<FragmentInfo>& to_consolidate,
    const NDRange& union_non_empty_domains,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    URI* new_fragment_uri) {
  // Open array for reading
  Array array_for_reads(array_uri, storage_manager_);
  RETURN_NOT_OK(array_for_reads.open(
      QueryType::READ,
      to_consolidate,
      encryption_type,
      encryption_key,
      key_length));

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

  // Compute if all fragments to consolidate are sparse
  assert(!to_consolidate.empty());
  bool all_sparse =
      this->all_sparse(to_consolidate, 0, to_consolidate.size() - 1);

  // Prepare buffers
  void** buffers;
  uint64_t* buffer_sizes;
  unsigned int buffer_num;
  Status st = create_buffers(
      array_schema, all_sparse, &buffers, &buffer_sizes, &buffer_num);
  if (!st.ok()) {
    array_for_reads.close();
    array_for_writes.close();
    return st;
  }

  // Create queries
  auto query_r = (Query*)nullptr;
  auto query_w = (Query*)nullptr;
  st = create_queries(
      &array_for_reads,
      &array_for_writes,
      all_sparse,
      union_non_empty_domains,
      buffers,
      buffer_sizes,
      &query_r,
      &query_w,
      new_fragment_uri);
  if (!st.ok()) {
    array_for_reads.close();
    array_for_writes.close();
    clean_up(buffer_num, buffers, buffer_sizes, query_r, query_w);
    return st;
  }

  // Read from one array and write to the other
  st = copy_array(query_r, query_w);
  if (!st.ok()) {
    array_for_reads.close();
    array_for_writes.close();
    clean_up(buffer_num, buffers, buffer_sizes, query_r, query_w);
    return st;
  }

  // Close array for reading
  st = array_for_reads.close();
  if (!st.ok()) {
    array_for_writes.close();
    storage_manager_->vfs()->remove_dir(*new_fragment_uri);
    clean_up(buffer_num, buffers, buffer_sizes, query_r, query_w);
    return st;
  }

  // Finalize write query
  st = query_w->finalize();
  if (!st.ok()) {
    array_for_writes.close();
    clean_up(buffer_num, buffers, buffer_sizes, query_r, query_w);
    bool is_dir = false;
    auto st2 = storage_manager_->vfs()->is_dir(*new_fragment_uri, &is_dir);
    (void)st2;  // Perhaps report this once we support an error stack
    if (is_dir)
      storage_manager_->vfs()->remove_dir(*new_fragment_uri);
    return st;
  }

  // Close array
  st = array_for_writes.close();
  if (!st.ok()) {
    clean_up(buffer_num, buffers, buffer_sizes, query_r, query_w);
    bool is_dir = false;
    auto st2 = storage_manager_->vfs()->is_dir(*new_fragment_uri, &is_dir);
    (void)st2;  // Perhaps report this once we support an error stack
    if (is_dir)
      storage_manager_->vfs()->remove_dir(*new_fragment_uri);
    return st;
  }

  std::vector<URI> to_delete;
  for (const auto& f : to_consolidate)
    to_delete.emplace_back(f.uri());

  // Delete old fragment metadata. This makes the old fragments invisible
  st = delete_fragment_metadata(array_uri, to_delete);
  if (!st.ok()) {
    delete_fragments(to_delete);
    clean_up(buffer_num, buffers, buffer_sizes, query_r, query_w);
    return st;
  }

  // Delete old fragments. The array does not need to be locked.
  st = delete_fragments(to_delete);

  // Clean up
  clean_up(buffer_num, buffers, buffer_sizes, query_r, query_w);

  return st;
}

Status Consolidator::copy_array(Query* query_r, Query* query_w) {
  do {
    RETURN_NOT_OK(query_r->submit());
    RETURN_NOT_OK(query_w->submit());
  } while (query_r->status() == QueryStatus::INCOMPLETE);

  return Status::Ok();
}

void Consolidator::clean_up(
    unsigned buffer_num,
    void** buffers,
    uint64_t* buffer_sizes,
    Query* query_r,
    Query* query_w) const {
  free_buffers(buffer_num, buffers, buffer_sizes);
  delete query_r;
  delete query_w;
}

Status Consolidator::create_buffers(
    const ArraySchema* array_schema,
    bool sparse_mode,
    void*** buffers,
    uint64_t** buffer_sizes,
    unsigned int* buffer_num) {
  // For easy reference
  auto attribute_num = array_schema->attribute_num();
  auto sparse = !array_schema->dense() || sparse_mode;

  // Calculate number of buffers
  *buffer_num = 0;
  for (unsigned int i = 0; i < attribute_num; ++i)
    *buffer_num += (array_schema->attributes()[i]->var_size()) ? 2 : 1;
  *buffer_num += (sparse) ? 1 : 0;

  // Create buffers
  *buffers = (void**)std::malloc(*buffer_num * sizeof(void*));
  if (*buffers == nullptr) {
    return LOG_STATUS(Status::ConsolidatorError(
        "Cannot create consolidation buffers; Memory allocation failed"));
  }
  *buffer_sizes = new uint64_t[*buffer_num];
  if (*buffer_sizes == nullptr) {
    return LOG_STATUS(Status::ConsolidatorError(
        "Cannot create consolidation buffer sizes; Memory allocation failed"));
  }

  // Allocate space for each buffer
  bool error = false;
  for (unsigned int i = 0; i < *buffer_num; ++i) {
    (*buffers)[i] = std::malloc(config_.buffer_size_);
    if ((*buffers)[i] == nullptr)  // The loop should continue to
      error = true;                // allocate nullptr to each buffer
    (*buffer_sizes)[i] = config_.buffer_size_;
  }

  // Clean up upon error
  if (error) {
    free_buffers(*buffer_num, *buffers, *buffer_sizes);
    *buffers = nullptr;
    *buffer_sizes = nullptr;
    return LOG_STATUS(Status::ConsolidatorError(
        "Cannot create consolidation buffers; Memory allocation failed"));
  }

  // Success
  return Status::Ok();
}

Status Consolidator::create_queries(
    Array* array_for_reads,
    Array* array_for_writes,
    bool sparse_mode,
    const NDRange& subarray,
    void** buffers,
    uint64_t* buffer_sizes,
    Query** query_r,
    Query** query_w,
    URI* new_fragment_uri) {
  // Note: it is safe to use `set_subarray_safe` for `subarray` below
  // because the subarray is calculated by the TileDB algorithm (it
  // is not a user input prone to errors).

  // Create read query
  *query_r = new Query(storage_manager_, array_for_reads);
  RETURN_NOT_OK((*query_r)->set_layout(Layout::GLOBAL_ORDER));
  RETURN_NOT_OK(
      set_query_buffers(*query_r, sparse_mode, buffers, buffer_sizes));
  RETURN_NOT_OK((*query_r)->set_subarray_unsafe(subarray));
  if (array_for_reads->array_schema()->dense() && sparse_mode)
    RETURN_NOT_OK((*query_r)->set_sparse_mode(true));

  // Get last fragment URI, which will be the URI of the consolidated fragment
  auto first = (*query_r)->first_fragment_uri();
  auto last = (*query_r)->last_fragment_uri();
  RETURN_NOT_OK(compute_new_fragment_uri(first, last, new_fragment_uri));

  // Create write query
  *query_w = new Query(storage_manager_, array_for_writes, *new_fragment_uri);
  RETURN_NOT_OK((*query_w)->set_layout(Layout::GLOBAL_ORDER));
  if (array_for_reads->array_schema()->dense())
    RETURN_NOT_OK((*query_w)->set_subarray_unsafe(subarray));
  RETURN_NOT_OK(
      set_query_buffers(*query_w, sparse_mode, buffers, buffer_sizes));

  return Status::Ok();
}

Status Consolidator::delete_fragment_metadata(
    const URI& array_uri, const std::vector<URI>& fragments) {
  RETURN_NOT_OK(storage_manager_->array_xlock(array_uri));

  for (auto& uri : fragments) {
    auto meta_uri = uri.join_path(constants::fragment_metadata_filename);
    RETURN_NOT_OK(storage_manager_->vfs()->remove_file(meta_uri));
  }

  RETURN_NOT_OK(storage_manager_->array_xunlock(array_uri));

  return Status::Ok();
}

Status Consolidator::delete_fragments(const std::vector<URI>& fragments) {
  for (auto& uri : fragments)
    RETURN_NOT_OK(storage_manager_->vfs()->remove_dir(uri));

  return Status::Ok();
}

Status Consolidator::delete_overwritten_fragments(
    const ArraySchema* array_schema, std::vector<FragmentInfo>* fragments) {
  // Trivial case
  if (fragments->size() == 1)
    return Status::Ok();

  // Applicable only to dense arrays
  if (!array_schema->dense())
    return Status::Ok();

  // Find which fragments to delete
  auto domain = array_schema->domain();
  std::vector<URI> to_delete;
  std::list<FragmentInfo> updated;
  for (auto f : *fragments)
    updated.emplace_back(f);

  for (auto cur = updated.rbegin(); cur != updated.rend(); ++cur) {
    if (cur->sparse())
      continue;
    for (auto check = updated.begin();
         check->uri().to_string() != cur->uri().to_string();) {
      if (domain->covered(check->non_empty_domain(), cur->non_empty_domain())) {
        to_delete.emplace_back(check->uri());
        check = updated.erase(check);
      } else {
        ++check;
      }
    }
  }

  // Delete the fragment metadata
  auto array_uri = array_schema->array_uri();
  RETURN_NOT_OK(delete_fragment_metadata(array_uri, to_delete));

  // Delete the fragments
  RETURN_NOT_OK(delete_fragments(to_delete));

  // Update the input fragments
  fragments->clear();
  for (const auto& f : updated)
    fragments->emplace_back(f);

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

Status Consolidator::compute_next_to_consolidate(
    const ArraySchema* array_schema,
    const std::vector<FragmentInfo>& fragments,
    std::vector<FragmentInfo>* to_consolidate,
    NDRange* union_non_empty_domains) const {
  // Preparation
  auto domain = array_schema->domain();
  to_consolidate->clear();
  auto min = config_.min_frags_;
  min = (uint32_t)((min > fragments.size()) ? fragments.size() : min);
  auto max = config_.max_frags_;
  max = (uint32_t)((max > fragments.size()) ? fragments.size() : max);
  auto size_ratio = config_.size_ratio_;

  // Trivial case - no fragments
  if (max == 0)
    return Status::Ok();

  // Prepare the dynamic-programming matrices. The rows are from 1 to max
  // and the columns represent the fragments in `fragments`. One matrix
  // stores the sum of fragment sizes, and the other the union of the
  // corresponding non-empty domains of the fragments.
  std::vector<std::vector<uint64_t>> m_sizes;
  std::vector<std::vector<NDRange>> m_union;
  auto col_num = fragments.size();
  auto row_num = max;
  m_sizes.resize(row_num);
  for (auto& row : m_sizes)
    row.resize(col_num);
  m_union.resize(row_num);
  for (auto& row : m_union) {
    row.resize(col_num);
  }

  // Entry m[i][j] contains the collective size of fragments
  // fragments[j], ..., fragments[j+i]. If the size ratio
  // of any adjacent pair in the above list is smaller than the
  // defined one, or the entries' corresponding fragments are no
  // consolidatable, then the size sum of that entry is infinity
  // (UINT64_MAX) and the memory from the union matrix is freed.
  // This marks this entry as invalid and it will never be selected
  // as the winner for choosing which fragments to consolidate next.
  for (size_t i = 0; i < row_num; ++i) {
    for (size_t j = 0; j < col_num; ++j) {
      if (i == 0) {  // In the first row we store the sizes of `fragments`
        m_sizes[i][j] = fragments[j].fragment_size();
        m_union[i][j] = fragments[j].non_empty_domain();
      } else if (i + j >= col_num) {  // Non-valid entries
        m_sizes[i][j] = UINT64_MAX;
        m_union[i][j].clear();
        m_union[i][j].shrink_to_fit();
      } else {  // Every other row is computed using the previous row
        auto ratio = (float)fragments[i + j - 1].fragment_size() /
                     fragments[i + j].fragment_size();
        ratio = (ratio <= 1.0f) ? ratio : 1.0f / ratio;
        if (ratio >= size_ratio && (m_sizes[i - 1][j] != UINT64_MAX)) {
          m_sizes[i][j] = m_sizes[i - 1][j] + fragments[i + j].fragment_size();
          m_union[i][j] = m_union[i - 1][j];
          domain->expand_ndrange(
              fragments[i + j].non_empty_domain(), &m_union[i][j]);
          domain->expand_to_tiles(&m_union[i][j]);
          if (!are_consolidatable(domain, fragments, j, j + i, m_union[i][j])) {
            // Mark this entry as invalid
            m_sizes[i][j] = UINT64_MAX;
            m_union[i][j].clear();
            m_union[i][j].shrink_to_fit();
          }
        } else {
          // Mark this entry as invalid
          m_sizes[i][j] = UINT64_MAX;
          m_union[i][j].clear();
          m_union[i][j].shrink_to_fit();
        }
      }
    }
  }

  // Choose the maximal set of fragments with cardinality in [min, max]
  // with the minimum size
  uint64_t min_size = UINT64_MAX;
  size_t min_col = 0;
  for (int i = row_num - 1; (i >= 0 && i >= (int)min - 1); --i) {
    min_size = UINT64_MAX;
    for (size_t j = 0; j < col_num; ++j) {
      // Update the min size if the new size is more than 25% smaller.
      // This is to give preference to earlier fragment sets, in case
      // the user writes in *approximately* equal batches. Otherwise,
      // fragment sets in the middle of the timeline may get consolidated,
      // which will hinder the next step of consolidation (which will
      // select some small and some big fragments).
      if (min_size == UINT64_MAX || m_sizes[i][j] < (min_size / 1.25)) {
        min_size = m_sizes[i][j];
        min_col = j;
      }
    }

    // Results not found
    if (min_size == UINT64_MAX)
      continue;

    // Results found
    for (size_t f = min_col; f <= min_col + i; ++f)
      to_consolidate->emplace_back(fragments[f]);
    *union_non_empty_domains = m_union[i][min_col];
    break;
  }

  return Status::Ok();
}

Status Consolidator::compute_new_fragment_uri(
    const URI& first, const URI& last, URI* new_uri) const {
  // Get uuid
  std::string uuid;
  RETURN_NOT_OK(uuid::generate_uuid(&uuid, false));

  // Get fragment names
  auto tmp_uri = first;
  tmp_uri = tmp_uri.remove_trailing_slash();
  std::string first_name = tmp_uri.last_path_part();
  tmp_uri = last;
  tmp_uri = tmp_uri.remove_trailing_slash();
  std::string last_name = tmp_uri.last_path_part();

  // For creating the new fragment URI

  // Get timestamp ranges
  uint32_t f_version;
  RETURN_NOT_OK(utils::parse::get_fragment_name_version(first, &f_version));
  auto t_first = utils::parse::get_timestamp_range(f_version, first_name);
  RETURN_NOT_OK(utils::parse::get_fragment_name_version(last, &f_version));
  auto t_last = utils::parse::get_timestamp_range(f_version, last_name);

  // Create new URI
  std::stringstream ss;
  ss << first.parent().to_string() << "/__" << t_first.first << "_"
     << t_last.second << "_" << uuid << "_" << constants::format_version;

  *new_uri = URI(ss.str());

  return Status::Ok();
}

Status Consolidator::set_query_buffers(
    Query* query,
    bool sparse_mode,
    void** buffers,
    uint64_t* buffer_sizes) const {
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
  if (!dense || sparse_mode)
    RETURN_NOT_OK(
        query->set_buffer(constants::coords, buffers[bid], &buffer_sizes[bid]));

  return Status::Ok();
}

void Consolidator::update_fragment_info(
    const std::vector<FragmentInfo>& to_consolidate,
    const FragmentInfo& new_fragment_info,
    std::vector<FragmentInfo>* fragment_info) const {
  auto to_consolidate_it = to_consolidate.begin();
  auto fragment_it = fragment_info->begin();
  std::vector<FragmentInfo> updated_fragment_info;
  bool new_fragment_added = false;

  while (fragment_it != fragment_info->end()) {
    // No match - add the fragment info and advance `fragment_it`
    if (to_consolidate_it == to_consolidate.end() ||
        fragment_it->uri().to_string() !=
            to_consolidate_it->uri().to_string()) {
      updated_fragment_info.emplace_back(*fragment_it);
      ++fragment_it;
    } else {  // Match - add new fragment only once and advance both iterators
      if (!new_fragment_added) {
        updated_fragment_info.emplace_back(new_fragment_info);
        new_fragment_added = true;
      }
      ++fragment_it;
      ++to_consolidate_it;
    }
  }

  assert(
      updated_fragment_info.size() ==
      fragment_info->size() - to_consolidate.size() + 1);

  *fragment_info = std::move(updated_fragment_info);
}

Status Consolidator::set_config(const Config* config) {
  // Set the config
  Config merged_config = storage_manager_->config();
  if (config)
    merged_config.inherit(*config);
  bool found = false;
  config_.amplification_ = 0.0f;
  RETURN_NOT_OK(merged_config.get<float>(
      "sm.consolidation.amplification", &config_.amplification_, &found));
  assert(found);
  config_.steps_ = 0;
  RETURN_NOT_OK(merged_config.get<uint32_t>(
      "sm.consolidation.steps", &config_.steps_, &found));
  assert(found);
  config_.buffer_size_ = 0;
  RETURN_NOT_OK(merged_config.get<uint64_t>(
      "sm.consolidation.buffer_size", &config_.buffer_size_, &found));
  assert(found);
  config_.size_ratio_ = 0.0f;
  RETURN_NOT_OK(merged_config.get<float>(
      "sm.consolidation.step_size_ratio", &config_.size_ratio_, &found));
  assert(found);
  config_.min_frags_ = 0;
  RETURN_NOT_OK(merged_config.get<uint32_t>(
      "sm.consolidation.step_min_frags", &config_.min_frags_, &found));
  assert(found);
  config_.max_frags_ = 0;
  RETURN_NOT_OK(merged_config.get<uint32_t>(
      "sm.consolidation.step_max_frags", &config_.max_frags_, &found));
  assert(found);

  // Sanity checks
  if (config_.min_frags_ > config_.max_frags_)
    return LOG_STATUS(Status::ConsolidatorError(
        "Invalid configuration; Minimum fragments config parameter is larger "
        "than the maximum"));
  if (config_.size_ratio_ > 1.0f || config_.size_ratio_ < 0.0f)
    return LOG_STATUS(Status::ConsolidatorError(
        "Invalid configuration; Step size ratio config parameter must be in "
        "[0.0, 1.0]"));
  if (config_.amplification_ < 0)
    return LOG_STATUS(
        Status::ConsolidatorError("Invalid configuration; Amplification config "
                                  "parameter must be non-negative"));

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
