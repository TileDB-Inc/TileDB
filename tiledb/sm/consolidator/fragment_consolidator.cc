/**
 * @file   fragment_consolidator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2023 TileDB, Inc.
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

#include "tiledb/sm/consolidator/fragment_consolidator.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/storage_format/uri/generate_uri.h"
#include "tiledb/storage_format/uri/parse_uri.h"

#include <iostream>
#include <numeric>
#include <sstream>

using namespace tiledb::common;

namespace tiledb::sm {

class FragmentConsolidatorException : public StatusException {
 public:
  explicit FragmentConsolidatorException(const std::string& message)
      : StatusException("FragmentConsolidator", message) {
  }
};

/* ****************************** */
/*          CONSTRUCTOR           */
/* ****************************** */

FragmentConsolidator::FragmentConsolidator(
    const Config& config, StorageManager* storage_manager)
    : Consolidator(storage_manager) {
  auto st = set_config(config);
  if (!st.ok()) {
    throw std::logic_error(st.message());
  }
}

/* ****************************** */
/*               API              */
/* ****************************** */

Status FragmentConsolidator::consolidate(
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  auto timer_se = stats_->start_timer("consolidate_frags");

  check_array_uri(array_name);

  // Open array for reading
  auto array_for_reads{
      make_shared<Array>(HERE(), URI(array_name), storage_manager_)};
  RETURN_NOT_OK(array_for_reads->open_without_fragments(
      encryption_type, encryption_key, key_length));

  // Open array for writing
  auto array_for_writes{make_shared<Array>(
      HERE(), array_for_reads->array_uri(), storage_manager_)};
  RETURN_NOT_OK(array_for_writes->open(
      QueryType::WRITE, encryption_type, encryption_key, key_length));

  // Disable consolidation with timestamps on older arrays.
  if (array_for_reads->array_schema_latest().write_version() <
      constants::consolidation_with_timestamps_min_version) {
    config_.with_timestamps_ = false;
  }

  // Get fragment info
  // For dense arrays, we need to pass the last parameter to the
  // `load` function to indicate that all fragment metadata
  // must be fetched (even before `config_.timestamp_start_`),
  // to compute the anterior ND range that can help determine
  // which dense fragments are consolidatable.
  FragmentInfo fragment_info(URI(array_name), storage_manager_->resources());
  auto st = fragment_info.load(
      array_for_reads->array_directory(),
      config_.timestamp_start_,
      config_.timestamp_end_,
      encryption_type,
      encryption_key,
      key_length);
  if (!st.ok()) {
    throw_if_not_ok(array_for_reads->close());
    throw_if_not_ok(array_for_writes->close());
    return st;
  }

  uint32_t step = 0;
  std::vector<TimestampedURI> to_consolidate;
  do {
    // No need to consolidate if no more than 1 fragment exist
    if (fragment_info.fragment_num() <= 1)
      break;

    // Find the next fragments to be consolidated
    NDRange union_non_empty_domains;
    st = compute_next_to_consolidate(
        array_for_reads->array_schema_latest(),
        fragment_info,
        &to_consolidate,
        &union_non_empty_domains);
    if (!st.ok()) {
      throw_if_not_ok(array_for_reads->close());
      throw_if_not_ok(array_for_writes->close());
      return st;
    }

    // Check if there is anything to consolidate
    if (to_consolidate.size() <= 1) {
      break;
    }

    // Consolidate the selected fragments
    URI new_fragment_uri;
    st = consolidate_internal(
        array_for_reads,
        array_for_writes,
        to_consolidate,
        union_non_empty_domains,
        &new_fragment_uri);
    if (!st.ok()) {
      throw_if_not_ok(array_for_reads->close());
      throw_if_not_ok(array_for_writes->close());
      return st;
    }

    // Load info of the consolidated fragment and add it
    // to the fragment info, replacing the fragments that it
    // consolidated.
    st = fragment_info.load_and_replace(new_fragment_uri, to_consolidate);
    if (!st.ok()) {
      throw_if_not_ok(array_for_reads->close());
      throw_if_not_ok(array_for_writes->close());
      return st;
    }

    // Advance number of steps
    ++step;

  } while (step < config_.steps_);

  RETURN_NOT_OK_ELSE(
      array_for_reads->close(), throw_if_not_ok(array_for_writes->close()));
  RETURN_NOT_OK(array_for_writes->close());

  stats_->add_counter("consolidate_step_num", step);

  return Status::Ok();
}

Status FragmentConsolidator::consolidate_fragments(
    const char* array_name,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    const std::vector<std::string>& fragment_uris) {
  auto timer_se = stats_->start_timer("consolidate_frags");

  // Open array for reading
  auto array_for_reads{
      make_shared<Array>(HERE(), URI(array_name), storage_manager_)};
  RETURN_NOT_OK(array_for_reads->open_without_fragments(
      encryption_type, encryption_key, key_length));

  // Open array for writing
  auto array_for_writes{make_shared<Array>(
      HERE(), array_for_reads->array_uri(), storage_manager_)};
  RETURN_NOT_OK(array_for_writes->open(
      QueryType::WRITE, encryption_type, encryption_key, key_length));

  // Disable consolidation with timestamps on older arrays.
  if (array_for_reads->array_schema_latest().write_version() <
      constants::consolidation_with_timestamps_min_version) {
    config_.with_timestamps_ = false;
  }

  // Check if there is anything to consolidate
  if (fragment_uris.size() <= 1) {
    return Status::Ok();
  }

  // Get all fragment info
  FragmentInfo fragment_info(URI(array_name), storage_manager_->resources());
  auto st = fragment_info.load(
      array_for_reads->array_directory(),
      0,
      utils::time::timestamp_now_ms(),
      encryption_type,
      encryption_key,
      key_length);
  if (!st.ok()) {
    throw_if_not_ok(array_for_reads->close());
    throw_if_not_ok(array_for_writes->close());
    return st;
  }

  // Make a set of the uris to consolidate
  NDRange union_non_empty_domains;
  std::unordered_set<std::string> to_consolidate_set;
  for (auto& uri : fragment_uris) {
    to_consolidate_set.emplace(uri);
  }

  // Make sure all fragments to consolidate are present
  // Compute union of non empty domains as we go
  uint64_t count = 0;
  auto& domain{array_for_reads->array_schema_latest().domain()};
  std::vector<TimestampedURI> to_consolidate;
  to_consolidate.reserve(fragment_uris.size());
  auto& frag_info_vec = fragment_info.single_fragment_info_vec();
  for (auto& frag_info : frag_info_vec) {
    auto uri = frag_info.uri().last_path_part();
    if (to_consolidate_set.count(uri) != 0) {
      count++;
      domain.expand_ndrange(
          frag_info.non_empty_domain(), &union_non_empty_domains);
      to_consolidate.emplace_back(frag_info.uri(), frag_info.timestamp_range());
    }
  }

  if (count != fragment_uris.size()) {
    return logger_->status(Status_ConsolidatorError(
        "Cannot consolidate; Not all fragments could be found"));
  }

  // Consolidate the selected fragments
  URI new_fragment_uri;
  st = consolidate_internal(
      array_for_reads,
      array_for_writes,
      to_consolidate,
      union_non_empty_domains,
      &new_fragment_uri);
  if (!st.ok()) {
    throw_if_not_ok(array_for_reads->close());
    throw_if_not_ok(array_for_writes->close());
    return st;
  }

  // Load info of the consolidated fragment and add it
  // to the fragment info, replacing the fragments that it
  // consolidated.
  st = fragment_info.load_and_replace(new_fragment_uri, to_consolidate);
  if (!st.ok()) {
    throw_if_not_ok(array_for_reads->close());
    throw_if_not_ok(array_for_writes->close());
    return st;
  }

  RETURN_NOT_OK_ELSE(
      array_for_reads->close(), RETURN_NOT_OK(array_for_writes->close()));
  throw_if_not_ok(array_for_writes->close());

  return Status::Ok();
}

void FragmentConsolidator::vacuum(const char* array_name) {
  if (array_name == nullptr) {
    throw FragmentConsolidatorException(
        "Cannot vacuum fragments; Array name cannot be null");
  }

  // Get the fragment URIs and vacuum file URIs to be vacuumed
  ArrayDirectory array_dir(
      storage_manager_->resources(),
      URI(array_name),
      0,
      std::numeric_limits<uint64_t>::max(),
      ArrayDirectoryMode::VACUUM_FRAGMENTS);

  auto filtered_fragment_uris = array_dir.filtered_fragment_uris(true);
  const auto& fragment_uris_to_vacuum =
      filtered_fragment_uris.fragment_uris_to_vacuum();
  const auto& commit_uris_to_ignore =
      filtered_fragment_uris.commit_uris_to_ignore();

  if (commit_uris_to_ignore.size() > 0) {
    array_dir.write_commit_ignore_file(commit_uris_to_ignore);
  }

  // Delete the vacuum files.
  auto vfs = storage_manager_->vfs();
  auto compute_tp = storage_manager_->compute_tp();
  vfs->remove_files(
      compute_tp, filtered_fragment_uris.fragment_vac_uris_to_vacuum());

  // Delete fragment directories
  throw_if_not_ok(parallel_for(
      compute_tp, 0, fragment_uris_to_vacuum.size(), [&](size_t i) {
        RETURN_NOT_OK(vfs->remove_dir(fragment_uris_to_vacuum[i]));

        // Remove the commit file, if present.
        auto commit_uri = array_dir.get_commit_uri(fragment_uris_to_vacuum[i]);
        bool is_file = false;
        RETURN_NOT_OK(vfs->is_file(commit_uri, &is_file));
        if (is_file) {
          RETURN_NOT_OK(vfs->remove_file(commit_uri));
        }

        return Status::Ok();
      }));
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

bool FragmentConsolidator::are_consolidatable(
    const Domain& domain,
    const FragmentInfo& fragment_info,
    size_t start,
    size_t end,
    const NDRange& union_non_empty_domains) const {
  const auto& anterior_ndrange = fragment_info.anterior_ndrange();
  if (anterior_ndrange.size() != 0 &&
      domain.overlap(union_non_empty_domains, anterior_ndrange))
    return false;

  // Check overlap of union with earlier fragments
  const auto& fragments = fragment_info.single_fragment_info_vec();
  for (size_t i = 0; i < start; ++i) {
    if (domain.overlap(
            union_non_empty_domains, fragments[i].non_empty_domain()))
      return false;
  }

  // Check consolidation amplification factor
  auto union_cell_num = domain.cell_num(union_non_empty_domains);
  uint64_t sum_cell_num = 0;
  for (size_t i = start; i <= end; ++i) {
    sum_cell_num += domain.cell_num(fragments[i].expanded_non_empty_domain());
  }

  return (double(union_cell_num) / sum_cell_num) <= config_.amplification_;
}

Status FragmentConsolidator::consolidate_internal(
    shared_ptr<Array> array_for_reads,
    shared_ptr<Array> array_for_writes,
    const std::vector<TimestampedURI>& to_consolidate,
    const NDRange& union_non_empty_domains,
    URI* new_fragment_uri) {
  auto timer_se = stats_->start_timer("consolidate_internal");

  array_for_reads->load_fragments(to_consolidate);

  if (array_for_reads->is_empty()) {
    return Status::Ok();
  }

  // Get schema
  const auto& array_schema = array_for_reads->array_schema_latest();

  // If there are any delete conditions coming after the first fragment or if
  // there are any fragments with delete meta, the new fragment will include
  // delete meta.
  if (!config_.purge_deleted_cells_ &&
      array_schema.write_version() >= constants::deletes_min_version) {
    // Get the first fragment first timestamp.
    std::pair<uint64_t, uint64_t> timestamps;
    RETURN_NOT_OK(
        utils::parse::get_timestamp_range(to_consolidate[0].uri_, &timestamps));

    for (auto& delete_and_update_tile_location :
         array_for_reads->array_directory()
             .delete_and_update_tiles_location()) {
      if (delete_and_update_tile_location.timestamp() >= timestamps.first) {
        config_.with_delete_meta_ = true;
        break;
      }
    }

    if (!config_.with_delete_meta_) {
      for (auto& frag_md : array_for_reads->fragment_metadata()) {
        if (frag_md->has_delete_meta()) {
          config_.with_delete_meta_ = true;
          break;
        }
      }
    }
  }

  // Prepare buffers
  auto average_var_cell_sizes = array_for_reads->get_average_var_cell_sizes();
  FragmentConsolidationWorkspace cw{create_buffers(stats_, config_, array_schema, average_var_cell_sizes)};

  // Create queries
  auto query_r = (Query*)nullptr;
  auto query_w = (Query*)nullptr;
  auto st = create_queries(
      array_for_reads,
      array_for_writes,
      union_non_empty_domains,
      &query_r,
      &query_w,
      new_fragment_uri);
  if (!st.ok()) {
    tdb_delete(query_r);
    tdb_delete(query_w);
    return st;
  }

  // Get the vacuum URI
  URI vac_uri;
  try {
    vac_uri =
        array_for_reads->array_directory().get_vacuum_uri(*new_fragment_uri);
  } catch (std::exception& e) {
    tdb_delete(query_r);
    tdb_delete(query_w);
    std::throw_with_nested(
        std::logic_error("[FragmentConsolidator::consolidate_internal] "));
  }

  // Read from one array and write to the other
  try {
    copy_array(query_r, query_w, cw);
  } catch (...) {
    tdb_delete(query_r);
    tdb_delete(query_w);
    throw;
  };

  // Finalize write query
  st = query_w->finalize();
  if (!st.ok()) {
    tdb_delete(query_r);
    tdb_delete(query_w);
    bool is_dir = false;
    auto st2 = storage_manager_->vfs()->is_dir(*new_fragment_uri, &is_dir);
    (void)st2;  // Perhaps report this once we support an error stack
    if (is_dir)
      throw_if_not_ok(storage_manager_->vfs()->remove_dir(*new_fragment_uri));
    return st;
  }

  // Write vacuum file
  st = write_vacuum_file(
      array_for_reads->array_schema_latest().write_version(),
      array_for_reads->array_uri(),
      vac_uri,
      to_consolidate);
  if (!st.ok()) {
    tdb_delete(query_r);
    tdb_delete(query_w);
    bool is_dir = false;
    throw_if_not_ok(
        storage_manager_->vfs()->is_dir(*new_fragment_uri, &is_dir));
    if (is_dir)
      throw_if_not_ok(storage_manager_->vfs()->remove_dir(*new_fragment_uri));
    return st;
  }

  // Clean up
  tdb_delete(query_r);
  tdb_delete(query_w);

  return st;
}

void FragmentConsolidator::copy_array(
    Query* query_r, Query* query_w, FragmentConsolidationWorkspace& cw) {
  auto timer_se = stats_->start_timer("consolidate_copy_array");

  // Set the read query buffers outside the repeated submissions.
  // The Reader will reset the query buffer sizes to the original
  // sizes, not the potentially smaller sizes of the results after
  // the query submission.
  set_query_buffers(query_r, cw);

  do {
    // READ
    throw_if_not_ok(query_r->submit());

    // If Consolidation cannot make any progress, throw. The first buffer will
    // always contain fixed size data, whether it is tile offsets for var size
    // attribute/dimension or the actual fixed size data so we can use its size
    // to know if any cells were written or not.
    if (cw.sizes().at(0) == 0) {
      throw FragmentConsolidatorException(
          "Consolidation read 0 cells, no progress can be made");
    }

    // Set explicitly the write query buffers, as the sizes may have
    // been altered by the read query.
    set_query_buffers(query_w, cw);

    // WRITE
    throw_if_not_ok(query_w->submit());
  } while (query_r->status() == QueryStatus::INCOMPLETE);
}

FragmentConsolidationWorkspace&& FragmentConsolidator::create_buffers(
    stats::Stats* stats,
    const FragmentConsolidationConfig& config,
    const ArraySchema& array_schema,
    std::unordered_map<std::string, uint64_t>& avg_cell_sizes) {
  auto timer_se = stats->start_timer("consolidate_create_buffers");

  // For easy reference
  auto attribute_num = array_schema.attribute_num();
  auto& domain{array_schema.domain()};
  auto dim_num = array_schema.dim_num();
  auto sparse = !array_schema.dense();

  // Calculate buffer weights. We reserve the maximum possible number of buffers
  // to make only one allocation. If an attribute is var size and nullable, it
  // has 3 buffers, dimensions only have 2 as they cannot be nullable. Then one
  // buffer for timestamps, and 2 for delete metadata.
  std::vector<size_t> buffer_weights;
  buffer_weights.reserve(attribute_num * 3 + dim_num * 2 + 3);
  for (unsigned i = 0; i < attribute_num; ++i) {
    const auto attr = array_schema.attributes()[i];
    const auto var_size = attr->var_size();

    // First buffer is either the var size offsets or the fixed size data.
    buffer_weights.emplace_back(
        var_size ? constants::cell_var_offset_size : attr->cell_size());

    // For var size attributes, add the data buffer weight.
    if (var_size) {
      buffer_weights.emplace_back(avg_cell_sizes[attr->name()]);
    }

    // For nullable attributes, add the validity buffer weight.
    if (attr->nullable()) {
      buffer_weights.emplace_back(constants::cell_validity_size);
    }
  }

  if (sparse) {
    for (unsigned i = 0; i < dim_num; ++i) {
      const auto dim = domain.dimension_ptr(i);
      const auto var_size = dim->var_size();

      // First buffer is either the var size offsets or the fixed size data.
      buffer_weights.emplace_back(
          var_size ? constants::cell_var_offset_size : dim->coord_size());

      // For var size attributes, add the data buffer weight.
      if (var_size) {
        buffer_weights.emplace_back(avg_cell_sizes[dim->name()]);
      }
    }
  }

  if (config.with_timestamps_ && sparse) {
    buffer_weights.emplace_back(constants::timestamp_size);
  }

  // Adding buffers for delete meta, one for timestamp and one for condition
  // index.
  if (config.with_delete_meta_) {
    buffer_weights.emplace_back(constants::timestamp_size);
    buffer_weights.emplace_back(sizeof(uint64_t));
  }

  // Use the old buffer size setting to see how much memory we would use.
  auto buffer_num = buffer_weights.size();
  uint64_t total_budget = config.total_buffer_size_;

  // If a user set the per-attribute buffer size configuration, we override
  // the use of the total_budget_size config setting for backwards
  // compatible behavior.
  if (config.buffer_size_ != 0) {
    total_budget = config.buffer_size_ * buffer_num;
  }

  // Create buffers.
  FragmentConsolidationWorkspace cw{buffer_num};
  std::vector<ByteVec>& buffers{cw.buffers()};
  std::vector<uint64_t>& buffer_sizes{cw.sizes()};
  auto total_weights = std::accumulate(
      buffer_weights.begin(), buffer_weights.end(), static_cast<size_t>(0));

  // Allocate space for each buffer.
  uint64_t adjusted_budget = total_budget / total_weights * total_weights;
  for (unsigned i = 0; i < buffer_num; ++i) {
    buffer_sizes[i] = std::max<uint64_t>(
        1, adjusted_budget * buffer_weights[i] / total_weights);
    buffers[i].resize(buffer_sizes[i]);
  }

  // Success
  return std::move(cw);
}

Status FragmentConsolidator::create_queries(
    shared_ptr<Array> array_for_reads,
    shared_ptr<Array> array_for_writes,
    const NDRange& subarray,
    Query** query_r,
    Query** query_w,
    URI* new_fragment_uri) {
  auto timer_se = stats_->start_timer("consolidate_create_queries");

  const auto dense = array_for_reads->array_schema_latest().dense();

  // Note: it is safe to use `set_subarray_safe` for `subarray` below
  // because the subarray is calculated by the TileDB algorithm (it
  // is not a user input prone to errors).

  // Create read query
  *query_r = tdb_new(Query, storage_manager_, array_for_reads);
  RETURN_NOT_OK((*query_r)->set_layout(Layout::GLOBAL_ORDER));

  // Dense consolidation will do a tile aligned read.
  if (dense) {
    NDRange read_subarray = subarray;
    auto& domain{array_for_reads->array_schema_latest().domain()};
    domain.expand_to_tiles(&read_subarray);
    RETURN_NOT_OK((*query_r)->set_subarray_unsafe(read_subarray));
  }

  // Enable consolidation with timestamps on the reader, if applicable.
  if (config_.with_timestamps_ && !dense) {
    throw_if_not_ok((*query_r)->set_consolidation_with_timestamps());
  }

  // Get last fragment URI, which will be the URI of the consolidated fragment
  auto first = (*query_r)->first_fragment_uri();
  auto last = (*query_r)->last_fragment_uri();

  auto write_version = array_for_reads->array_schema_latest().write_version();
  auto fragment_name = storage_format::generate_consolidated_fragment_name(
      first, last, write_version);

  // Create write query
  *query_w = tdb_new(Query, storage_manager_, array_for_writes, fragment_name);
  RETURN_NOT_OK((*query_w)->set_layout(Layout::GLOBAL_ORDER));
  RETURN_NOT_OK((*query_w)->disable_checks_consolidation());
  (*query_w)->set_fragment_size(config_.max_fragment_size_);
  if (array_for_reads->array_schema_latest().dense()) {
    RETURN_NOT_OK((*query_w)->set_subarray_unsafe(subarray));
  }

  // Set the processed conditions on new fragment.
  const auto& delete_and_update_tiles_location =
      (*query_r)->array()->array_directory().delete_and_update_tiles_location();
  std::vector<std::string> processed_conditions;
  processed_conditions.reserve(delete_and_update_tiles_location.size());
  for (auto& location : delete_and_update_tiles_location) {
    processed_conditions.emplace_back(location.condition_marker());
  }
  (*query_w)->set_processed_conditions(processed_conditions);

  // Set the URI for the new fragment.
  auto frag_uri =
      array_for_reads->array_directory().get_fragments_dir(write_version);
  *new_fragment_uri = frag_uri.join_path(fragment_name);

  return Status::Ok();
}

Status FragmentConsolidator::compute_next_to_consolidate(
    const ArraySchema& array_schema,
    const FragmentInfo& fragment_info,
    std::vector<TimestampedURI>* to_consolidate,
    NDRange* union_non_empty_domains) const {
  auto timer_se = stats_->start_timer("consolidate_compute_next");

  // Preparation
  auto sparse = !array_schema.dense();
  const auto& fragments = fragment_info.single_fragment_info_vec();
  auto& domain{array_schema.domain()};
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
          domain.expand_ndrange(
              fragments[i + j].non_empty_domain(), &m_union[i][j]);
          if (!sparse && !are_consolidatable(
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
    }
    *union_non_empty_domains = m_union[i][min_col];
    break;
  }

  return Status::Ok();
}

void FragmentConsolidator::set_query_buffers(
    Query* query, FragmentConsolidationWorkspace& cw) const {
  std::vector<ByteVec>* buffers{&cw.buffers()};
  std::vector<uint64_t>* buffer_sizes{&cw.sizes()};

  const auto& array_schema = query->array_schema();
  auto dim_num = array_schema.dim_num();
  auto dense = array_schema.dense();
  auto attributes = array_schema.attributes();
  unsigned bid = 0;

  // Here the first buffer should always be the fixed buffer (either offsets
  // or fixed data) as we use the first buffer size to determine if any cells
  // were written or not.
  for (const auto& attr : attributes) {
    if (!attr->var_size()) {
      throw_if_not_ok(query->set_data_buffer(
          attr->name(), (void*)&(*buffers)[bid][0], &(*buffer_sizes)[bid]));
      ++bid;
      if (attr->nullable()) {
        throw_if_not_ok(query->set_validity_buffer(
            attr->name(),
            (uint8_t*)&(*buffers)[bid][0],
            &(*buffer_sizes)[bid]));
        ++bid;
      }
    } else {
      throw_if_not_ok(query->set_data_buffer(
          attr->name(),
          (void*)&(*buffers)[bid + 1][0],
          &(*buffer_sizes)[bid + 1]));
      throw_if_not_ok(query->set_offsets_buffer(
          attr->name(), (uint64_t*)&(*buffers)[bid][0], &(*buffer_sizes)[bid]));
      bid += 2;
      if (attr->nullable()) {
        throw_if_not_ok(query->set_validity_buffer(
            attr->name(),
            (uint8_t*)&(*buffers)[bid][0],
            &(*buffer_sizes)[bid]));
        ++bid;
      }
    }
  }
  if (!dense) {
    for (unsigned d = 0; d < dim_num; ++d) {
      auto dim{array_schema.dimension_ptr(d)};
      auto dim_name = dim->name();
      if (!dim->var_size()) {
        throw_if_not_ok(query->set_data_buffer(
            dim_name, (void*)&(*buffers)[bid][0], &(*buffer_sizes)[bid]));
        ++bid;
      } else {
        throw_if_not_ok(query->set_data_buffer(
            dim_name,
            (void*)&(*buffers)[bid + 1][0],
            &(*buffer_sizes)[bid + 1]));
        throw_if_not_ok(query->set_offsets_buffer(
            dim_name, (uint64_t*)&(*buffers)[bid][0], &(*buffer_sizes)[bid]));
        bid += 2;
      }
    }
  }

  if (config_.with_timestamps_ && !dense) {
    throw_if_not_ok(query->set_data_buffer(
        constants::timestamps,
        (void*)&(*buffers)[bid][0],
        &(*buffer_sizes)[bid]));
    ++bid;
  }

  if (config_.with_delete_meta_ && !dense) {
    throw_if_not_ok(query->set_data_buffer(
        constants::delete_timestamps,
        (void*)&(*buffers)[bid][0],
        &(*buffer_sizes)[bid]));
    ++bid;
    throw_if_not_ok(query->set_data_buffer(
        constants::delete_condition_index,
        (void*)&(*buffers)[bid][0],
        &(*buffer_sizes)[bid]));
    ++bid;
  }
}

Status FragmentConsolidator::set_config(const Config& config) {
  // Set the consolidation config for ease of use
  Config merged_config = storage_manager_->config();
  merged_config.inherit(config);
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
  // Only set the buffer_size_ if the user specified a value. Otherwise, we use
  // the new sm.consolidation.total_buffer_size instead.
  if (merged_config.set_params().count("sm.consolidation.buffer_size") > 0) {
    logger_->warn(
        "The `sm.consolidation.buffer_size configuration setting has been "
        "deprecated. Set consolidation buffer sizes using the newer "
        "`sm.consolidation.total_buffer_size` setting.");
    RETURN_NOT_OK(merged_config.get<uint64_t>(
        "sm.consolidation.buffer_size", &config_.buffer_size_, &found));
    assert(found);
  }
  config_.total_buffer_size_ = 0;
  RETURN_NOT_OK(merged_config.get<uint64_t>(
      "sm.consolidation.total_buffer_size",
      &config_.total_buffer_size_,
      &found));
  assert(found);
  config_.max_fragment_size_ = 0;
  RETURN_NOT_OK(merged_config.get<uint64_t>(
      "sm.consolidation.max_fragment_size",
      &config_.max_fragment_size_,
      &found));
  assert(found);
  config_.size_ratio_ = 0.0f;
  RETURN_NOT_OK(merged_config.get<float>(
      "sm.consolidation.step_size_ratio", &config_.size_ratio_, &found));
  assert(found);
  config_.purge_deleted_cells_ = false;
  RETURN_NOT_OK(merged_config.get<bool>(
      "sm.consolidation.purge_deleted_cells",
      &config_.purge_deleted_cells_,
      &found));
  assert(found);
  config_.min_frags_ = 0;
  RETURN_NOT_OK(merged_config.get<uint32_t>(
      "sm.consolidation.step_min_frags", &config_.min_frags_, &found));
  assert(found);
  config_.max_frags_ = 0;
  RETURN_NOT_OK(merged_config.get<uint32_t>(
      "sm.consolidation.step_max_frags", &config_.max_frags_, &found));
  assert(found);
  RETURN_NOT_OK(merged_config.get<uint64_t>(
      "sm.consolidation.timestamp_start", &config_.timestamp_start_, &found));
  assert(found);
  RETURN_NOT_OK(merged_config.get<uint64_t>(
      "sm.consolidation.timestamp_end", &config_.timestamp_end_, &found));
  assert(found);
  std::string reader =
      merged_config.get("sm.query.sparse_global_order.reader", &found);
  assert(found);
  config_.use_refactored_reader_ = reader.compare("refactored") == 0;
  config_.with_timestamps_ = true;
  config_.with_delete_meta_ = false;

  // Sanity checks
  if (config_.min_frags_ > config_.max_frags_)
    return logger_->status(Status_ConsolidatorError(
        "Invalid configuration; Minimum fragments config parameter is larger "
        "than the maximum"));
  if (config_.size_ratio_ > 1.0f || config_.size_ratio_ < 0.0f)
    return logger_->status(Status_ConsolidatorError(
        "Invalid configuration; Step size ratio config parameter must be in "
        "[0.0, 1.0]"));
  if (config_.amplification_ < 0)
    return logger_->status(
        Status_ConsolidatorError("Invalid configuration; Amplification config "
                                 "parameter must be non-negative"));

  return Status::Ok();
}

Status FragmentConsolidator::write_vacuum_file(
    const format_version_t write_version,
    const URI& array_uri,
    const URI& vac_uri,
    const std::vector<TimestampedURI>& to_consolidate) const {
  std::stringstream ss;
  size_t base_uri_size = 0;

  // Write vac files relative to the array URI. This was fixed for reads in
  // version 19 so only do this for arrays starting with version 19.
  if (write_version >= 19) {
    base_uri_size = array_uri.to_string().size();
  }

  for (const auto& timestampedURI : to_consolidate) {
    ss << timestampedURI.uri_.to_string().substr(base_uri_size) << "\n";
  }

  auto data = ss.str();
  RETURN_NOT_OK(
      storage_manager_->vfs()->write(vac_uri, data.c_str(), data.size()));
  RETURN_NOT_OK(storage_manager_->vfs()->close_file(vac_uri));

  return Status::Ok();
}

}  // namespace tiledb::sm
