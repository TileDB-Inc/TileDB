/**
 * @file   consolidator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
#include "tiledb/common/logger.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/single_fragment_info.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/generic_tile_io.h"
#include "tiledb/sm/tile/tile.h"

#include <iostream>
#include <sstream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Consolidator::Consolidator(StorageManager* storage_manager)
    : storage_manager_(storage_manager)
    , stats_(storage_manager_->stats()->create_child("Consolidator")) {
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

  // Consolidate based on mode
  URI array_uri = URI(array_name);
  if (config_.mode_ == "fragment_meta")
    return consolidate_fragment_meta(
        array_uri, encryption_type, encryption_key, key_length);
  else if (config_.mode_ == "fragments")
    return consolidate_fragments(
        array_name, encryption_type, encryption_key, key_length);
  else if (config_.mode_ == "array_meta")
    return consolidate_array_meta(
        array_name, encryption_type, encryption_key, key_length);

  return LOG_STATUS(Status::ConsolidatorError(
      "Cannot consolidate; Invalid consolidation mode"));
}

Status Consolidator::consolidate_array_meta(
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  auto timer_se = stats_->start_timer("consolidate_array_meta");

  // Open array for reading
  auto array_uri = URI(array_name);
  Array array_for_reads(array_uri, storage_manager_);
  RETURN_NOT_OK(array_for_reads.open(
      QueryType::READ,
      config_.timestamp_start_,
      config_.timestamp_end_,
      encryption_type,
      encryption_key,
      key_length));

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
  auto st = array_for_reads.metadata(&metadata_r);
  if (!st.ok()) {
    array_for_reads.close();
    array_for_writes.close();
    return st;
  }
  Metadata* metadata_w;
  st = array_for_writes.metadata(&metadata_w);
  if (!st.ok()) {
    array_for_reads.close();
    array_for_writes.close();
    return st;
  }
  metadata_r->swap(metadata_w);

  // Metadata uris to delete
  const auto to_vacuum = metadata_w->loaded_metadata_uris();

  // Generate new name for consolidated metadata
  st = metadata_w->generate_uri(array_uri);
  if (!st.ok()) {
    array_for_reads.close();
    array_for_writes.close();
    return st;
  }

  // Get the new URI name
  URI new_uri;
  st = metadata_w->get_uri(array_uri, &new_uri);
  if (!st.ok()) {
    array_for_reads.close();
    array_for_writes.close();
    return st;
  }

  // Close arrays
  RETURN_NOT_OK_ELSE(array_for_reads.close(), array_for_writes.close());
  RETURN_NOT_OK(array_for_writes.close());

  // Write vacuum file
  URI vac_uri = URI(new_uri.to_string() + constants::vacuum_file_suffix);

  std::stringstream ss;
  for (const auto& uri : to_vacuum)
    ss << uri.to_string() << "\n";

  auto data = ss.str();
  RETURN_NOT_OK(
      storage_manager_->vfs()->write(vac_uri, data.c_str(), data.size()));
  RETURN_NOT_OK(storage_manager_->vfs()->close_file(vac_uri));

  return Status::Ok();
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

Status Consolidator::consolidate_fragments(
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  auto timer_se = stats_->start_timer("consolidate_frags");

  // Open array for reading
  Array array_for_reads(URI(array_name), storage_manager_);
  RETURN_NOT_OK(array_for_reads.open_without_fragments(
      encryption_type, encryption_key, key_length));

  // Open array for writing
  Array array_for_writes(array_for_reads.array_uri(), storage_manager_);
  RETURN_NOT_OK(array_for_writes.open(
      QueryType::WRITE, encryption_type, encryption_key, key_length));

  // Get fragment info
  FragmentInfo fragment_info;
  auto st = storage_manager_->get_fragment_info(
      array_for_reads,
      config_.timestamp_start_,
      config_.timestamp_end_,
      &fragment_info);
  if (!st.ok()) {
    array_for_reads.close();
    array_for_writes.close();
    return st;
  }

  uint32_t step = 0;
  std::vector<TimestampedURI> to_consolidate;
  bool all_sparse;
  do {
    // No need to consolidate if no more than 1 fragment exist
    if (fragment_info.fragment_num() <= 1)
      break;

    // Find the next fragments to be consolidated
    NDRange union_non_empty_domains;
    st = compute_next_to_consolidate(
        array_for_reads.array_schema(),
        fragment_info,
        &to_consolidate,
        &all_sparse,
        &union_non_empty_domains);
    if (!st.ok()) {
      array_for_reads.close();
      array_for_writes.close();
      return st;
    }

    // Check if there is anything to consolidate
    if (to_consolidate.size() <= 1)
      break;

    // Consolidate the selected fragments
    URI new_fragment_uri;
    st = consolidate(
        array_for_reads,
        array_for_writes,
        to_consolidate,
        all_sparse,
        union_non_empty_domains,
        &new_fragment_uri);
    if (!st.ok()) {
      array_for_reads.close();
      array_for_writes.close();
      return st;
    }

    // Get fragment info of the consolidated fragment
    SingleFragmentInfo new_fragment_info;
    st = storage_manager_->get_fragment_info(
        array_for_reads, new_fragment_uri, &new_fragment_info);
    if (!st.ok()) {
      array_for_reads.close();
      array_for_writes.close();
      return st;
    }

    // Update fragment info
    update_fragment_info(to_consolidate, new_fragment_info, &fragment_info);

    // Advance number of steps
    ++step;

  } while (step < config_.steps_);

  RETURN_NOT_OK_ELSE(array_for_reads.close(), array_for_writes.close());
  RETURN_NOT_OK(array_for_writes.close());

  stats_->add_counter("consolidate_step_num", step);

  return Status::Ok();
}

bool Consolidator::all_sparse(
    const FragmentInfo& fragment_info, size_t start, size_t end) const {
  const auto& fragments = fragment_info.fragments();

  for (size_t i = start; i <= end; ++i) {
    if (!fragments[i].sparse())
      return false;
  }

  return true;
}

bool Consolidator::are_consolidatable(
    const Domain* domain,
    const FragmentInfo& fragment_info,
    size_t start,
    size_t end,
    const NDRange& union_non_empty_domains) const {
  // True if all fragments in [start, end] are sparse
  if (all_sparse(fragment_info, start, end))
    return true;

  auto anterior_ndrange = fragment_info.anterior_ndrange();
  if (anterior_ndrange.size() != 0 &&
      domain->overlap(union_non_empty_domains, anterior_ndrange))
    return false;

  // Check overlap of union with earlier fragments
  const auto& fragments = fragment_info.fragments();
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
    Array& array_for_reads,
    Array& array_for_writes,
    const std::vector<TimestampedURI>& to_consolidate,
    bool all_sparse,
    const NDRange& union_non_empty_domains,
    URI* new_fragment_uri) {
  auto timer_se = stats_->start_timer("consolidate_main");

  RETURN_NOT_OK(array_for_reads.load_fragments(to_consolidate));

  if (array_for_reads.is_empty()) {
    return Status::Ok();
  }

  // Get schema
  auto array_schema = array_for_reads.array_schema();

  // Prepare buffers
  std::vector<ByteVec> buffers;
  std::vector<uint64_t> buffer_sizes;
  RETURN_NOT_OK(
      create_buffers(array_schema, all_sparse, &buffers, &buffer_sizes));

  // Create queries
  auto query_r = (Query*)nullptr;
  auto query_w = (Query*)nullptr;
  auto st = create_queries(
      &array_for_reads,
      &array_for_writes,
      all_sparse,
      union_non_empty_domains,
      &query_r,
      &query_w,
      new_fragment_uri);
  if (!st.ok()) {
    tdb_delete(query_r);
    tdb_delete(query_w);
    return st;
  }

  // Read from one array and write to the other
  st = copy_array(query_r, query_w, &buffers, &buffer_sizes, all_sparse);
  if (!st.ok()) {
    tdb_delete(query_r);
    tdb_delete(query_w);
    return st;
  }

  // Finalize write query
  st = query_w->finalize();
  if (!st.ok()) {
    tdb_delete(query_r);
    tdb_delete(query_w);
    bool is_dir = false;
    auto st2 = storage_manager_->vfs()->is_dir(*new_fragment_uri, &is_dir);
    (void)st2;  // Perhaps report this once we support an error stack
    if (is_dir)
      storage_manager_->vfs()->remove_dir(*new_fragment_uri);
    return st;
  }

  // Write vacuum file
  st = write_vacuum_file(*new_fragment_uri, to_consolidate);
  if (!st.ok()) {
    tdb_delete(query_r);
    tdb_delete(query_w);
    bool is_dir = false;
    storage_manager_->vfs()->is_dir(*new_fragment_uri, &is_dir);
    if (is_dir)
      storage_manager_->vfs()->remove_dir(*new_fragment_uri);
    return st;
  }

  // Clean up
  tdb_delete(query_r);
  tdb_delete(query_w);

  return st;
}

Status Consolidator::consolidate_fragment_meta(
    const URI& array_uri,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  auto timer_se = stats_->start_timer("consolidate_frag_meta");

  // Open array for reading
  Array array(array_uri, storage_manager_);
  RETURN_NOT_OK(
      array.open(QueryType::READ, encryption_type, encryption_key, key_length));

  // Include only fragments with footers / separate basic metadata
  Buffer buff;
  const auto& tmp_meta = array.fragment_metadata();
  std::vector<FragmentMetadata*> meta;
  for (auto m : tmp_meta) {
    if (m->format_version() > 2)
      meta.emplace_back(m);
  }
  auto fragment_num = (unsigned)meta.size();

  // Do not consolidate if the number of fragments is not >1
  if (fragment_num < 2)
    return array.close();

  // Write number of fragments
  RETURN_NOT_OK(buff.write(&fragment_num, sizeof(uint32_t)));

  // Compute new URI
  URI uri;
  auto first = meta.front()->fragment_uri();
  auto last = meta.back()->fragment_uri();
  RETURN_NOT_OK(compute_new_fragment_uri(
      first, last, array.array_schema()->write_version(), &uri));
  uri = URI(uri.to_string() + constants::meta_file_suffix);

  // Get the consolidated fragment metadata version
  auto meta_name = uri.remove_trailing_slash().last_path_part();
  auto pos = meta_name.find_last_of('.');
  meta_name = (pos == std::string::npos) ? meta_name : meta_name.substr(0, pos);
  uint32_t meta_version = 0;
  RETURN_NOT_OK(utils::parse::get_fragment_version(meta_name, &meta_version));

  // Calculate offset of first fragment footer
  uint64_t offset = sizeof(uint32_t);  // Fragment num
  for (auto m : meta) {
    offset += sizeof(uint64_t);  // Name size
    if (meta_version >= 9) {
      offset += m->fragment_uri().last_path_part().size();  // Name
    } else {
      offset += m->fragment_uri().to_string().size();  // Name
    }
    offset += sizeof(uint64_t);  // Offset
  }

  // Serialize all fragment names and footer offsets into a single buffer
  for (auto m : meta) {
    // Write name size and name
    std::string name;
    if (meta_version >= 9) {
      name = m->fragment_uri().last_path_part();
    } else {
      name = m->fragment_uri().to_string();
    }
    auto name_size = (uint64_t)name.size();
    RETURN_NOT_OK(buff.write(&name_size, sizeof(uint64_t)));
    RETURN_NOT_OK(buff.write(name.c_str(), name_size));
    RETURN_NOT_OK(buff.write(&offset, sizeof(uint64_t)));

    offset += m->footer_size();
  }

  // Serialize all fragment metadata footers in parallel
  std::vector<Buffer> buffs(meta.size());
  auto status = parallel_for(
      storage_manager_->compute_tp(), 0, buffs.size(), [&](size_t i) {
        RETURN_NOT_OK(meta[i]->write_footer(&buffs[i]));
        return Status::Ok();
      });
  RETURN_NOT_OK(status);

  // Combine serialized fragment metadata footers into a single buffer
  for (const auto& b : buffs)
    RETURN_NOT_OK(buff.write(b.data(), b.size()));

  // Close array
  RETURN_NOT_OK(array.close());

  ChunkedBuffer chunked_buffer;
  RETURN_NOT_OK(Tile::buffer_to_contiguous_fixed_chunks(
      buff, 0, constants::generic_tile_cell_size, &chunked_buffer));
  buff.disown_data();

  // Write to file
  EncryptionKey enc_key;
  RETURN_NOT_OK(enc_key.set_key(encryption_type, encryption_key, key_length));
  buff.reset_offset();
  Tile tile(
      constants::generic_tile_datatype,
      constants::generic_tile_cell_size,
      0,
      &chunked_buffer,
      false);
  GenericTileIO tile_io(storage_manager_, uri);
  uint64_t nbytes = 0;
  RETURN_NOT_OK_ELSE(
      tile_io.write_generic(&tile, enc_key, &nbytes), chunked_buffer.free());
  (void)nbytes;
  RETURN_NOT_OK_ELSE(storage_manager_->close_file(uri), chunked_buffer.free());

  chunked_buffer.free();

  return Status::Ok();
}

Status Consolidator::copy_array(
    Query* query_r,
    Query* query_w,
    std::vector<ByteVec>* buffers,
    std::vector<uint64_t>* buffer_sizes,
    bool sparse_mode) {
  auto timer_se = stats_->start_timer("consolidate_copy_array");

  // Set the read query buffers outside the repeated submissions.
  // The Reader will reset the query buffer sizes to the original
  // sizes, not the potentially smaller sizes of the results after
  // the query submission.
  RETURN_NOT_OK(set_query_buffers(query_r, sparse_mode, buffers, buffer_sizes));

  do {
    // READ
    RETURN_NOT_OK(query_r->submit());

    // Set explicitly the write query buffers, as the sizes may have
    // been altered by the read query.
    RETURN_NOT_OK(
        set_query_buffers(query_w, sparse_mode, buffers, buffer_sizes));

    // WRITE
    RETURN_NOT_OK(query_w->submit());
  } while (query_r->status() == QueryStatus::INCOMPLETE);

  return Status::Ok();
}

Status Consolidator::create_buffers(
    const ArraySchema* array_schema,
    bool sparse_mode,
    std::vector<ByteVec>* buffers,
    std::vector<uint64_t>* buffer_sizes) {
  auto timer_se = stats_->start_timer("consolidate_create_buffers");

  // For easy reference
  auto attribute_num = array_schema->attribute_num();
  auto domain = array_schema->domain();
  auto dim_num = array_schema->dim_num();
  auto sparse = !array_schema->dense() || sparse_mode;

  // Calculate number of buffers
  size_t buffer_num = 0;
  for (unsigned i = 0; i < attribute_num; ++i) {
    buffer_num += (array_schema->attributes()[i]->var_size()) ? 2 : 1;
    buffer_num += (array_schema->attributes()[i]->nullable()) ? 1 : 0;
  }
  if (sparse) {
    for (unsigned i = 0; i < dim_num; ++i)
      buffer_num += (domain->dimension(i)->var_size()) ? 2 : 1;
  }

  // Create buffers
  buffers->resize(buffer_num);
  buffer_sizes->resize(buffer_num);

  // Allocate space for each buffer
  for (unsigned i = 0; i < buffer_num; ++i) {
    (*buffers)[i].resize(config_.buffer_size_);
    (*buffer_sizes)[i] = config_.buffer_size_;
  }

  // Success
  return Status::Ok();
}

Status Consolidator::create_queries(
    Array* array_for_reads,
    Array* array_for_writes,
    bool sparse_mode,
    const NDRange& subarray,
    Query** query_r,
    Query** query_w,
    URI* new_fragment_uri) {
  auto timer_se = stats_->start_timer("consolidate_create_queries");

  // Note: it is safe to use `set_subarray_safe` for `subarray` below
  // because the subarray is calculated by the TileDB algorithm (it
  // is not a user input prone to errors).

  // Create read query
  *query_r = tdb_new(Query, storage_manager_, array_for_reads);
  RETURN_NOT_OK((*query_r)->set_layout(Layout::GLOBAL_ORDER));

  // Refactored reader optimizes for no subarray.
  if (!config_.use_refactored_readers_ ||
      array_for_reads->array_schema()->dense())
    RETURN_NOT_OK((*query_r)->set_subarray_unsafe(subarray));

  if (array_for_reads->array_schema()->dense() && sparse_mode)
    RETURN_NOT_OK((*query_r)->set_sparse_mode(true));

  // Get last fragment URI, which will be the URI of the consolidated fragment
  auto first = (*query_r)->first_fragment_uri();
  auto last = (*query_r)->last_fragment_uri();

  RETURN_NOT_OK(compute_new_fragment_uri(
      first,
      last,
      array_for_reads->array_schema()->write_version(),
      new_fragment_uri));

  // Create write query
  *query_w =
      tdb_new(Query, storage_manager_, array_for_writes, *new_fragment_uri);
  RETURN_NOT_OK((*query_w)->set_layout(Layout::GLOBAL_ORDER));
  RETURN_NOT_OK((*query_w)->disable_check_global_order());
  if (array_for_reads->array_schema()->dense())
    RETURN_NOT_OK((*query_w)->set_subarray_unsafe(subarray));

  return Status::Ok();
}

Status Consolidator::compute_next_to_consolidate(
    const ArraySchema* array_schema,
    const FragmentInfo& fragment_info,
    std::vector<TimestampedURI>* to_consolidate,
    bool* all_sparse,
    NDRange* union_non_empty_domains) const {
  auto timer_se = stats_->start_timer("consolidate_compute_next");

  // Preparation
  *all_sparse = true;
  const auto& fragments = fragment_info.fragments();
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
          if (!are_consolidatable(
                  domain, fragment_info, j, j + i, m_union[i][j])) {
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
    for (size_t f = min_col; f <= min_col + i; ++f) {
      to_consolidate->emplace_back(
          fragments[f].uri(), fragments[f].timestamp_range());
      *all_sparse &= fragments[f].sparse();
    }
    *union_non_empty_domains = m_union[i][min_col];
    break;
  }

  return Status::Ok();
}

Status Consolidator::compute_new_fragment_uri(
    const URI& first,
    const URI& last,
    uint32_t format_version,
    URI* new_uri) const {
  // Get uuid
  std::string uuid;
  RETURN_NOT_OK(uuid::generate_uuid(&uuid, false));

  // For creating the new fragment URI

  // Get timestamp ranges
  std::pair<uint64_t, uint64_t> t_first, t_last;
  RETURN_NOT_OK(utils::parse::get_timestamp_range(first, &t_first));
  RETURN_NOT_OK(utils::parse::get_timestamp_range(last, &t_last));

  // Create new URI
  std::stringstream ss;
  ss << first.parent().to_string() << "/__" << t_first.first << "_"
     << t_last.second << "_" << uuid << "_" << format_version;

  *new_uri = URI(ss.str());

  return Status::Ok();
}

Status Consolidator::set_query_buffers(
    Query* query,
    bool sparse_mode,
    std::vector<ByteVec>* buffers,
    std::vector<uint64_t>* buffer_sizes) const {
  auto array_schema = query->array_schema();
  auto dim_num = array_schema->dim_num();
  auto dense = array_schema->dense();
  auto attributes = array_schema->attributes();
  unsigned bid = 0;
  for (const auto& attr : attributes) {
    if (!attr->var_size()) {
      if (!attr->nullable()) {
        RETURN_NOT_OK(query->set_buffer(
            attr->name(), (void*)&(*buffers)[bid][0], &(*buffer_sizes)[bid]));
        ++bid;
      } else {
        RETURN_NOT_OK(query->set_buffer_vbytemap(
            attr->name(),
            (void*)&(*buffers)[bid][0],
            &(*buffer_sizes)[bid],
            (uint8_t*)&(*buffers)[bid + 1][0],
            &(*buffer_sizes)[bid + 1]));
        bid += 2;
      }
    } else {
      if (!attr->nullable()) {
        RETURN_NOT_OK(query->set_buffer(
            attr->name(),
            (uint64_t*)&(*buffers)[bid][0],
            &(*buffer_sizes)[bid],
            (void*)&(*buffers)[bid + 1][0],
            &(*buffer_sizes)[bid + 1]));
        bid += 2;
      } else {
        RETURN_NOT_OK(query->set_buffer_vbytemap(
            attr->name(),
            (uint64_t*)&(*buffers)[bid][0],
            &(*buffer_sizes)[bid],
            (void*)&(*buffers)[bid + 1][0],
            &(*buffer_sizes)[bid + 1],
            (uint8_t*)&(*buffers)[bid + 2][0],
            &(*buffer_sizes)[bid + 2]));
        bid += 3;
      }
    }
  }
  if (!dense || sparse_mode) {
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim = array_schema->dimension(d);
      auto dim_name = dim->name();
      if (!dim->var_size()) {
        RETURN_NOT_OK(query->set_buffer(
            dim_name, (void*)&(*buffers)[bid][0], &(*buffer_sizes)[bid]));
        ++bid;
      } else {
        RETURN_NOT_OK(query->set_buffer(
            dim_name,
            (uint64_t*)&(*buffers)[bid][0],
            &(*buffer_sizes)[bid],
            (void*)&(*buffers)[bid + 1][0],
            &(*buffer_sizes)[bid + 1]));
        bid += 2;
      }
    }
  }

  return Status::Ok();
}

void Consolidator::update_fragment_info(
    const std::vector<TimestampedURI>& to_consolidate,
    const SingleFragmentInfo& new_fragment_info,
    FragmentInfo* fragment_info) const {
  auto to_consolidate_it = to_consolidate.begin();
  auto fragment_it = fragment_info->fragments().begin();
  FragmentInfo updated_fragment_info;
  bool new_fragment_added = false;

  while (fragment_it != fragment_info->fragments().end()) {
    // No match - add the fragment info and advance `fragment_it`
    if (to_consolidate_it == to_consolidate.end() ||
        fragment_it->uri().to_string() != to_consolidate_it->uri_.to_string()) {
      updated_fragment_info.append(*fragment_it);
      ++fragment_it;
    } else {  // Match - add new fragment only once and advance both iterators
      if (!new_fragment_added) {
        updated_fragment_info.append(new_fragment_info);
        new_fragment_added = true;
      }
      ++fragment_it;
      ++to_consolidate_it;
    }
  }

  assert(
      updated_fragment_info.fragment_num() ==
      fragment_info->fragment_num() - to_consolidate.size() + 1);

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
  const std::string mode = merged_config.get("sm.consolidation.mode", &found);
  if (!found)
    return LOG_STATUS(Status::ConsolidatorError(
        "Cannot consolidate; Consolidation mode cannot be null"));
  config_.mode_ = mode;
  RETURN_NOT_OK(merged_config.get<uint64_t>(
      "sm.consolidation.timestamp_start", &config_.timestamp_start_, &found));
  assert(found);
  RETURN_NOT_OK(merged_config.get<uint64_t>(
      "sm.consolidation.timestamp_end", &config_.timestamp_end_, &found));
  assert(found);
  RETURN_NOT_OK(merged_config.get<bool>(
      "sm.use_refactored_readers", &config_.use_refactored_readers_, &found));
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

Status Consolidator::write_vacuum_file(
    const URI& new_uri,
    const std::vector<TimestampedURI>& to_consolidate) const {
  URI vac_uri = URI(new_uri.to_string() + constants::vacuum_file_suffix);

  std::stringstream ss;
  for (const auto& timestampedURI : to_consolidate)
    ss << timestampedURI.uri_.to_string() << "\n";

  auto data = ss.str();
  RETURN_NOT_OK(
      storage_manager_->vfs()->write(vac_uri, data.c_str(), data.size()));
  RETURN_NOT_OK(storage_manager_->vfs()->close_file(vac_uri));

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
