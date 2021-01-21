/**
 * @file   stats.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2020 TileDB, Inc.
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
 * This file contains definitions of statistics-related code.
 */

#include "tiledb/sm/stats/stats.h"

#include <cassert>
#include <sstream>

namespace tiledb {
namespace sm {
namespace stats {

Stats all_stats;

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Stats::Stats() {
  enabled_ = false;
  reset();
}

/* ****************************** */
/*              API               */
/* ****************************** */

void Stats::add_counter(CounterType stat, uint64_t count) {
  std::unique_lock<std::mutex> lck(mtx_);
  auto it = counter_stats_.find(stat);
  assert(it != counter_stats_.end());
  it->second += count;
}

void Stats::add_timer(TimerType stat, double count) {
  std::unique_lock<std::mutex> lck(mtx_);
  auto it = timer_stats_.find(stat);
  assert(it != timer_stats_.end());
  it->second += count;
}

bool Stats::enabled() const {
  return enabled_;
}

void Stats::reset() {
  std::unique_lock<std::mutex> lck(mtx_);

  timer_stats_.clear();
  for (uint64_t i = 0; i != static_cast<uint64_t>(TimerType::__SIZE); ++i)
    timer_stats_[static_cast<TimerType>(i)] = 0;

  counter_stats_.clear();
  for (uint64_t i = 0; i != static_cast<uint64_t>(CounterType::__SIZE); ++i)
    counter_stats_[static_cast<CounterType>(i)] = 0;
}

void Stats::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;

  std::string output;
  Stats::dump(&output);
  fprintf(out, "%s", output.c_str());
}

void Stats::dump(std::string* out) const {
  std::stringstream ss;
  ss << dump_read();
  ss << "\n";
  ss << dump_write();
  ss << "\n";
  ss << dump_consolidate();
  ss << "\n";
  ss << dump_vfs();

  auto dbg_secs = timer_stats_.find(TimerType::DBG)->second;
  if (dbg_secs != 0)
    ss << "\nDebugging time: " << dbg_secs << " secs\n";

  *out = ss.str();
}

void Stats::raw_dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;

  std::string output;
  Stats::raw_dump(&output);
  fprintf(out, "%s", output.c_str());
}

void Stats::raw_dump(std::string* out) const {
  std::stringstream ss;

  ss << "{\n";

  // Dump timer stats
  const std::vector<std::string> timer_names =
      parse_enum_names(TimerTypeNames_);
  for (uint64_t i = 0; i != static_cast<uint64_t>(TimerType::__SIZE); ++i) {
    // The first stat is a special where we do not need to prepend a comma
    // to the end of the previous stat.
    if (i == 0) {
      ss << "  \"" << timer_names[i]
         << "\": " << timer_stats_.find(static_cast<TimerType>(i))->second;
    } else {
      ss << ",\n  \"" << timer_names[i]
         << "\": " << timer_stats_.find(static_cast<TimerType>(i))->second;
    }
  }

  // Dump counter stats
  const std::vector<std::string> counter_names =
      parse_enum_names(CounterTypeNames_);
  for (uint64_t i = 0; i != static_cast<uint64_t>(CounterType::__SIZE); ++i) {
    ss << ",\n  \"" << counter_names[i]
       << "\": " << counter_stats_.find(static_cast<CounterType>(i))->second;
  }

  ss << "\n}\n";

  *out = ss.str();
}

std::vector<std::string> Stats::parse_enum_names(
    const std::string& enum_names) const {
  std::vector<std::string> parsed_enum_names;

  std::string tmp;
  std::stringstream ss(enum_names);
  while (getline(ss, tmp, ',')) {
    if (parsed_enum_names.empty()) {
      parsed_enum_names.push_back(tmp);
    } else {
      parsed_enum_names.push_back(tmp.substr(1));
    }
  }

  return parsed_enum_names;
}

void Stats::set_enabled(bool enabled) {
  enabled_ = enabled;
}

/* ****************************** */
/*       PRIVATE FUNCTIONS        */
/* ****************************** */

std::string Stats::dump_write() const {
  auto write_time = timer_stats_.find(TimerType::WRITE)->second;
  auto write_split_coords_buff =
      timer_stats_.find(TimerType::WRITE_SPLIT_COORDS_BUFF)->second;
  auto write_check_coord_oob =
      timer_stats_.find(TimerType::WRITE_CHECK_COORD_OOB)->second;
  auto write_sort_coords =
      timer_stats_.find(TimerType::WRITE_SORT_COORDS)->second;
  auto write_check_coord_dups =
      timer_stats_.find(TimerType::WRITE_CHECK_COORD_DUPS)->second;
  auto write_compute_coord_dups =
      timer_stats_.find(TimerType::WRITE_COMPUTE_COORD_DUPS)->second;
  auto write_prepare_tiles =
      timer_stats_.find(TimerType::WRITE_PREPARE_TILES)->second;
  auto write_compute_coord_meta =
      timer_stats_.find(TimerType::WRITE_COMPUTE_COORD_META)->second;
  auto write_filter_tiles =
      timer_stats_.find(TimerType::WRITE_FILTER_TILES)->second;
  auto write_tiles = timer_stats_.find(TimerType::WRITE_TILES)->second;
  auto write_store_frag_meta =
      timer_stats_.find(TimerType::WRITE_STORE_FRAG_META)->second;
  auto write_finalize = timer_stats_.find(TimerType::WRITE_FINALIZE)->second;
  auto write_check_global_order =
      timer_stats_.find(TimerType::WRITE_CHECK_GLOBAL_ORDER)->second;
  auto write_init_tile_its =
      timer_stats_.find(TimerType::WRITE_INIT_TILE_ITS)->second;
  auto write_compute_cell_ranges =
      timer_stats_.find(TimerType::WRITE_COMPUTE_CELL_RANGES)->second;
  auto write_prepare_and_filter_tiles =
      timer_stats_.find(TimerType::WRITE_PREPARE_AND_FILTER_TILES)->second;
  auto write_array_meta =
      timer_stats_.find(TimerType::WRITE_ARRAY_META)->second;
  auto write_num = counter_stats_.find(CounterType::WRITE_NUM)->second;
  auto write_attr_num =
      counter_stats_.find(CounterType::WRITE_ATTR_NUM)->second;
  auto write_attr_fixed_num =
      counter_stats_.find(CounterType::WRITE_ATTR_FIXED_NUM)->second;
  auto write_attr_var_num =
      counter_stats_.find(CounterType::WRITE_ATTR_VAR_NUM)->second;
  auto write_attr_nullable_num =
      counter_stats_.find(CounterType::WRITE_ATTR_NULLABLE_NUM)->second;
  auto write_dim_num = counter_stats_.find(CounterType::WRITE_DIM_NUM)->second;
  auto write_dim_fixed_num =
      counter_stats_.find(CounterType::WRITE_DIM_FIXED_NUM)->second;
  auto write_dim_var_num =
      counter_stats_.find(CounterType::WRITE_DIM_VAR_NUM)->second;
  auto write_dim_zipped_num =
      counter_stats_.find(CounterType::WRITE_DIM_ZIPPED_NUM)->second;
  auto write_byte_num =
      counter_stats_.find(CounterType::WRITE_BYTE_NUM)->second;
  auto write_filtered_byte_num =
      counter_stats_.find(CounterType::WRITE_FILTERED_BYTE_NUM)->second;
  auto rtree_size = counter_stats_.find(CounterType::WRITE_RTREE_SIZE)->second;
  auto tile_offsets_size =
      counter_stats_.find(CounterType::WRITE_TILE_OFFSETS_SIZE)->second;
  auto tile_var_offsets_size =
      counter_stats_.find(CounterType::WRITE_TILE_VAR_OFFSETS_SIZE)->second;
  auto tile_var_sizes_size =
      counter_stats_.find(CounterType::WRITE_TILE_VAR_SIZES_SIZE)->second;
  auto tile_validity_offsets_size =
      counter_stats_.find(CounterType::WRITE_TILE_VALIDITY_OFFSETS_SIZE)
          ->second;
  auto frag_meta_footer_size =
      counter_stats_.find(CounterType::WRITE_FRAG_META_FOOTER_SIZE)->second;
  auto array_schema_size =
      counter_stats_.find(CounterType::WRITE_ARRAY_SCHEMA_SIZE)->second;
  auto write_tile_num =
      counter_stats_.find(CounterType::WRITE_TILE_NUM)->second;
  auto write_cell_num =
      counter_stats_.find(CounterType::WRITE_CELL_NUM)->second;
  auto write_array_meta_size =
      counter_stats_.find(CounterType::WRITE_ARRAY_META_SIZE)->second;
  auto write_ops_num = counter_stats_.find(CounterType::WRITE_OPS_NUM)->second;

  std::stringstream ss;

  if (write_num != 0) {
    ss << "==== WRITE ====\n\n";

    write(&ss, "- Number of write queries: ", write_num);
    ss << "\n";

    write(&ss, "- Number of attributes written: ", write_attr_num);
    write(
        &ss,
        "  * Number of fixed-sized attributes written: ",
        write_attr_fixed_num);
    write(
        &ss,
        "  * Number of var-sized attributes written: ",
        write_attr_var_num);
    write(&ss, "- Number of dimensions written: ", write_dim_num);
    write(
        &ss,
        "  * Number of fixed-sized dimensions written: ",
        write_dim_fixed_num);
    write(
        &ss, "  * Number of var-sized dimensions written: ", write_dim_var_num);
    write(
        &ss, "  * Number of zipped dimensions written: ", write_dim_zipped_num);
    ss << "\n";

    write_bytes(&ss, "- Number of bytes written: ", write_byte_num);
    write(&ss, "- Number of write operations: ", write_ops_num);
    write_bytes(&ss, "- Number of bytes filtered: ", write_filtered_byte_num);
    write_factor(
        &ss,
        "- Filtering deflation factor: ",
        write_filtered_byte_num,
        write_byte_num);
    ss << "\n";

    auto total_meta_size = array_schema_size + frag_meta_footer_size +
                           rtree_size + tile_offsets_size +
                           tile_var_offsets_size + tile_var_sizes_size +
                           tile_validity_offsets_size;
    write_bytes(&ss, "- Total metadata written: ", total_meta_size);
    write_bytes(&ss, "  * Array schema: ", array_schema_size);
    write_bytes(&ss, "  * Fragment metadata footer: ", frag_meta_footer_size);
    write_bytes(&ss, "  * R-tree: ", rtree_size);
    write_bytes(&ss, "  * Fixed-sized tile offsets: ", tile_offsets_size);
    write_bytes(&ss, "  * Var-sized tile offsets: ", tile_var_offsets_size);
    write_bytes(&ss, "  * Var-sized tile sizes: ", tile_var_sizes_size);
    write_bytes(&ss, "  * Validity tile sizes: ", tile_validity_offsets_size);
    ss << "\n";

    if (write_array_meta_size != 0) {
      write(&ss, "- Time to write array metadata: ", write_array_meta);
      write_bytes(&ss, "  * Array metadata size: ", write_array_meta_size);
      ss << "\n";
    }

    write(&ss, "- Number of logical cells written: ", write_cell_num);
    write(&ss, "- Number of logical tiles written: ", write_tile_num);
    auto fixed_tiles =
        write_tile_num *
        (write_attr_fixed_num + write_dim_fixed_num + write_dim_zipped_num);
    write(
        &ss, "  * Number of fixed-sized physical tiles written: ", fixed_tiles);
    auto var_tiles =
        2 * write_tile_num * (write_attr_var_num + write_dim_var_num);
    write(&ss, "  * Number of var-sized physical tiles written: ", var_tiles);
    auto validity_tiles = write_tile_num * write_attr_nullable_num;
    write(
        &ss, "  * Number of validity physical tiles written: ", validity_tiles);
    ss << "\n";

    write(&ss, "- Write time: ", write_time);
    write(
        &ss,
        "  * Time to split the coordinates buffer: ",
        write_split_coords_buff);
    write(
        &ss,
        "  * Time to check out-of-bounds coordinates: ",
        write_check_coord_oob);
    write(&ss, "  * Time to sort coordinates: ", write_sort_coords);
    write(
        &ss,
        "  * Time to check coordinate duplicates: ",
        write_check_coord_dups);
    write(
        &ss,
        "  * Time to compute coordinate duplicates: ",
        write_compute_coord_dups);
    write(&ss, "  * Time to check global order: ", write_check_global_order);
    write(
        &ss,
        "  * Time to initialize dense cell range iterators: ",
        write_init_tile_its);
    write(&ss, "  * Time to compute cell ranges: ", write_compute_cell_ranges);
    write(&ss, "  * Time to prepare tiles: ", write_prepare_tiles);
    write(
        &ss,
        "  * Time to prepare and filter tiles: ",
        write_prepare_and_filter_tiles);
    write(
        &ss,
        "  * Time to compute coordinate metadata (e.g., MBRs): ",
        write_compute_coord_meta);
    write(&ss, "  * Time to filter tiles: ", write_filter_tiles);
    write(&ss, "  * Time to write tiles: ", write_tiles);
    write(&ss, "  * Time to write fragment metadata: ", write_store_frag_meta);
    ss << "\n";

    write(&ss, "- Time to finalize write query: ", write_finalize);
  }

  return ss.str();
}

std::string Stats::dump_read() const {
  auto read_time = timer_stats_.find(TimerType::READ)->second;
  auto read_fill_dense_coords =
      timer_stats_.find(TimerType::READ_FILL_DENSE_COORDS)->second;
  auto read_array_open = timer_stats_.find(TimerType::READ_ARRAY_OPEN)->second;
  auto read_array_open_without_fragments =
      timer_stats_.find(TimerType::READ_ARRAY_OPEN_WITHOUT_FRAGMENTS)->second;
  auto read_load_array_schema =
      timer_stats_.find(TimerType::READ_LOAD_ARRAY_SCHEMA)->second;
  auto read_load_array_meta =
      timer_stats_.find(TimerType::READ_LOAD_ARRAY_META)->second;
  auto read_get_fragment_uris =
      timer_stats_.find(TimerType::READ_GET_FRAGMENT_URIS)->second;
  auto read_load_frag_meta =
      timer_stats_.find(TimerType::READ_LOAD_FRAG_META)->second;
  auto read_load_consolidated_frag_meta =
      timer_stats_.find(TimerType::READ_LOAD_CONSOLIDATED_FRAG_META)->second;
  auto read_init_state = timer_stats_.find(TimerType::READ_INIT_STATE)->second;
  auto total_read_time = read_time + read_init_state;
  auto read_compute_est_result_size =
      timer_stats_.find(TimerType::READ_COMPUTE_EST_RESULT_SIZE)->second;
  auto read_compute_tile_overlap =
      timer_stats_.find(TimerType::READ_COMPUTE_TILE_OVERLAP)->second;
  auto read_compute_tile_coords =
      timer_stats_.find(TimerType::READ_COMPUTE_TILE_COORDS)->second;
  auto read_compute_next_partition =
      timer_stats_.find(TimerType::READ_NEXT_PARTITION)->second;
  auto read_split_current_partition =
      timer_stats_.find(TimerType::READ_SPLIT_CURRENT_PARTITION)->second;
  auto read_compute_result_coords =
      timer_stats_.find(TimerType::READ_COMPUTE_RESULT_COORDS)->second;
  auto read_compute_range_coords =
      timer_stats_.find(TimerType::READ_COMPUTE_RANGE_RESULT_COORDS)->second;
  auto read_compute_sparse_result_tiles =
      timer_stats_.find(TimerType::READ_COMPUTE_SPARSE_RESULT_TILES)->second;
  auto read_compute_sparse_result_cell_slabs_sparse =
      timer_stats_
          .find(TimerType::READ_COMPUTE_SPARSE_RESULT_CELL_SLABS_SPARSE)
          ->second;
  auto read_compute_sparse_result_cell_slabs_dense =
      timer_stats_.find(TimerType::READ_COMPUTE_SPARSE_RESULT_CELL_SLABS_DENSE)
          ->second;
  auto read_copy_attr_values =
      timer_stats_.find(TimerType::READ_COPY_ATTR_VALUES)->second;
  auto read_copy_coords =
      timer_stats_.find(TimerType::READ_COPY_COORDS)->second;
  auto read_copy_fixed_attr_values =
      timer_stats_.find(TimerType::READ_COPY_FIXED_ATTR_VALUES)->second;
  auto read_copy_fixed_coords =
      timer_stats_.find(TimerType::READ_COPY_FIXED_COORDS)->second;
  auto read_copy_var_attr_values =
      timer_stats_.find(TimerType::READ_COPY_VAR_ATTR_VALUES)->second;
  auto read_copy_var_coords =
      timer_stats_.find(TimerType::READ_COPY_VAR_COORDS)->second;
  auto read_attr_tiles = timer_stats_.find(TimerType::READ_ATTR_TILES)->second;
  auto read_coord_tiles =
      timer_stats_.find(TimerType::READ_COORD_TILES)->second;
  auto read_unfilter_attr_tiles =
      timer_stats_.find(TimerType::READ_UNFILTER_ATTR_TILES)->second;
  auto read_unfilter_coord_tiles =
      timer_stats_.find(TimerType::READ_UNFILTER_COORD_TILES)->second;
  auto read_compute_relevant_tile_overlap =
      timer_stats_.find(TimerType::READ_COMPUTE_RELEVANT_TILE_OVERLAP)->second;
  auto read_load_relevant_rtrees =
      timer_stats_.find(TimerType::READ_LOAD_RELEVANT_RTREES)->second;
  auto read_compute_relevant_frags =
      timer_stats_.find(TimerType::READ_COMPUTE_RELEVANT_FRAGS)->second;

  auto array_schema_size =
      counter_stats_.find(CounterType::READ_ARRAY_SCHEMA_SIZE)->second;
  auto consolidated_frag_meta_size =
      counter_stats_.find(CounterType::CONSOLIDATED_FRAG_META_SIZE)->second;
  auto frag_meta_size =
      counter_stats_.find(CounterType::READ_FRAG_META_SIZE)->second;
  auto rtree_size = counter_stats_.find(CounterType::READ_RTREE_SIZE)->second;
  auto tile_offsets_size =
      counter_stats_.find(CounterType::READ_TILE_OFFSETS_SIZE)->second;
  auto tile_var_offsets_size =
      counter_stats_.find(CounterType::READ_TILE_VAR_OFFSETS_SIZE)->second;
  auto tile_var_sizes_size =
      counter_stats_.find(CounterType::READ_TILE_VAR_SIZES_SIZE)->second;
  auto tile_validity_offsets_size =
      counter_stats_.find(CounterType::READ_TILE_VALIDITY_OFFSETS_SIZE)->second;
  auto read_array_meta_size =
      counter_stats_.find(CounterType::READ_ARRAY_META_SIZE)->second;
  auto read_num = counter_stats_.find(CounterType::READ_NUM)->second;
  auto read_byte_num = counter_stats_.find(CounterType::READ_BYTE_NUM)->second;
  auto read_unfiltered_byte_num =
      counter_stats_.find(CounterType::READ_UNFILTERED_BYTE_NUM)->second;
  auto read_attr_fixed_num =
      counter_stats_.find(CounterType::READ_ATTR_FIXED_NUM)->second;
  auto read_attr_var_num =
      counter_stats_.find(CounterType::READ_ATTR_VAR_NUM)->second;
  auto read_attr_nullable_num =
      counter_stats_.find(CounterType::READ_ATTR_NULLABLE_NUM)->second;
  auto read_dim_fixed_num =
      counter_stats_.find(CounterType::READ_DIM_FIXED_NUM)->second;
  auto read_dim_var_num =
      counter_stats_.find(CounterType::READ_DIM_VAR_NUM)->second;
  auto read_dim_zipped_num =
      counter_stats_.find(CounterType::READ_DIM_ZIPPED_NUM)->second;
  auto read_overlap_tile_num =
      counter_stats_.find(CounterType::READ_OVERLAP_TILE_NUM)->second;
  auto read_loop_num = counter_stats_.find(CounterType::READ_LOOP_NUM)->second;
  auto read_result_num =
      counter_stats_.find(CounterType::READ_RESULT_NUM)->second;
  auto read_cell_num = counter_stats_.find(CounterType::READ_CELL_NUM)->second;
  auto read_ops_num = counter_stats_.find(CounterType::READ_OPS_NUM)->second;

  auto rest_http_retries =
      counter_stats_.find(CounterType::REST_HTTP_RETRIES)->second;
  // Treat this count as a double since we record the retry time in
  // milliseconds. Divide by 1000 to get seconds
  double rest_http_retry_time =
      counter_stats_.find(CounterType::REST_HTTP_RETRY_TIME)->second / 1000.0f;

  // Derived counters.
  auto read_dim_num =
      read_dim_var_num + read_dim_fixed_num + read_dim_zipped_num;
  auto read_attr_num = read_attr_fixed_num + read_attr_var_num;
  auto read_fixed_phys_tiles_num =
      read_overlap_tile_num *
      (read_dim_fixed_num + read_dim_zipped_num + read_attr_fixed_num);
  auto read_var_phys_tiles_num =
      2 * read_overlap_tile_num * (read_dim_var_num + read_attr_var_num);
  auto read_validity_phys_tiles_num =
      read_overlap_tile_num * read_attr_nullable_num;

  std::stringstream ss;
  if (read_num != 0) {
    ss << "==== READ ====\n\n";
    write(&ss, "- Number of read queries: ", read_num);
    write(&ss, "- Number of attempts until results are found: ", read_loop_num);
    ss << "\n";

    write(&ss, "- Number of attributes read: ", read_attr_num);
    write(
        &ss,
        "  * Number of fixed-sized attributes read: ",
        read_attr_fixed_num);
    write(&ss, "  * Number of var-sized attributes read: ", read_attr_var_num);
    write(&ss, "- Number of dimensions read: ", read_dim_num);
    write(
        &ss, "  * Number of fixed-sized dimensions read: ", read_dim_fixed_num);
    write(&ss, "  * Number of var-sized dimensions read: ", read_dim_var_num);
    write(&ss, "  * Number of zipped dimensions read: ", read_dim_zipped_num);
    ss << "\n";

    if (read_overlap_tile_num != 0) {
      write(
          &ss,
          "- Number of logical tiles overlapping the query: ",
          read_overlap_tile_num);
      write(
          &ss,
          "- Number of physical tiles read: ",
          read_fixed_phys_tiles_num + read_var_phys_tiles_num +
              read_validity_phys_tiles_num);
      write(
          &ss,
          "  * Number of physical fixed-sized tiles read: ",
          read_fixed_phys_tiles_num);
      write(
          &ss,
          "  * Number of physical var-sized tiles read: ",
          read_var_phys_tiles_num);
      write(
          &ss,
          "  * Number of physical validity tiles read: ",
          read_validity_phys_tiles_num);
      write(&ss, "- Number of cells read: ", read_cell_num);
      write(&ss, "- Number of result cells: ", read_result_num);
      write_ratio(
          &ss,
          "- Percentage of useful cells read: ",
          read_result_num,
          read_cell_num);
      ss << "\n";
    }

    write_bytes(&ss, "- Number of bytes read: ", read_byte_num);
    write(&ss, "- Number of read operations: ", read_ops_num);
    write_bytes(
        &ss, "- Number of bytes unfiltered: ", read_unfiltered_byte_num);
    write_factor(
        &ss,
        "- Unfiltering inflation factor: ",
        read_unfiltered_byte_num,
        read_byte_num);
    ss << "\n";

    if (rest_http_retries > 0) {
      write(&ss, "- Number of REST HTTP retries: ", rest_http_retries);
      write(&ss, "- Time spend in REST HTTP retries: ", rest_http_retry_time);
    }

    if (read_compute_est_result_size != 0) {
      write(
          &ss,
          "- Time to compute estimated result size: ",
          read_compute_est_result_size);
      write(
          &ss, "  * Time to compute tile overlap: ", read_compute_tile_overlap);
      write(
          &ss,
          "    > Time to compute relevant fragments: ",
          read_compute_relevant_frags);
      write(
          &ss,
          "    > Time to load relevant fragment R-trees: ",
          read_load_relevant_rtrees);
      write(
          &ss,
          "    > Time to compute relevant fragment tile overlap: ",
          read_compute_relevant_tile_overlap);
      ss << "\n";
    }

    if (read_array_open > 0) {
      write(&ss, "- Time to open array: ", read_array_open);
      write(
          &ss,
          "  * Time to open array without fragments: ",
          read_array_open_without_fragments);
      write(&ss, "  * Time to load array schema: ", read_load_array_schema);
      write(&ss, "  * Time to list fragment uris: ", read_get_fragment_uris);
      write(
          &ss,
          "  * Time to load consolidated fragment metadata: ",
          read_load_consolidated_frag_meta);
      write(&ss, "  * Time to load fragment metadata: ", read_load_frag_meta);
      ss << "\n";
    }

    auto total_meta_size = array_schema_size + consolidated_frag_meta_size +
                           frag_meta_size + rtree_size + tile_offsets_size +
                           tile_var_offsets_size + tile_var_sizes_size +
                           tile_validity_offsets_size;
    write_bytes(&ss, "- Total metadata read: ", total_meta_size);
    write_bytes(&ss, "  * Array schema: ", array_schema_size);
    write_bytes(
        &ss,
        "  * Consolidated fragment metadata: ",
        consolidated_frag_meta_size);
    write_bytes(&ss, "  * Fragment metadata: ", frag_meta_size);
    write_bytes(&ss, "  * R-tree: ", rtree_size);
    write_bytes(&ss, "  * Fixed-sized tile offsets: ", tile_offsets_size);
    write_bytes(&ss, "  * Var-sized tile offsets: ", tile_var_offsets_size);
    write_bytes(&ss, "  * Var-sized tile sizes: ", tile_var_sizes_size);
    write_bytes(&ss, "  * Validity tile offsets: ", tile_validity_offsets_size);
    ss << "\n";

    if (read_load_array_meta != 0) {
      write(&ss, "- Time to load array metadata: ", read_load_array_meta);
      write_bytes(&ss, "  * Array metadata size: ", read_array_meta_size);
      ss << "\n";
    }

    write(&ss, "- Time to initialize the read state: ", read_init_state);
    write(&ss, "  * Time to compute tile overlap: ", read_compute_tile_overlap);
    write(
        &ss,
        "    > Time to compute relevant fragments: ",
        read_compute_relevant_frags);
    write(
        &ss,
        "    > Time to load relevant fragment R-trees: ",
        read_load_relevant_rtrees);
    write(
        &ss,
        "    > Time to compute relevant fragment tile overlap: ",
        read_compute_relevant_tile_overlap);
    ss << "\n";

    write(&ss, "- Read time: ", read_time);
    write(
        &ss,
        "  * Time to compute next partition: ",
        read_compute_next_partition);
    write(
        &ss,
        "  * Time to split current partition: ",
        read_split_current_partition);
    write(
        &ss,
        "  * Time to compute tile coordinates: ",
        read_compute_tile_coords);
    write(
        &ss,
        "  * Time to compute result coordinates: ",
        read_compute_result_coords);
    write(
        &ss,
        "    > Time to compute sparse result tiles: ",
        read_compute_sparse_result_tiles);
    write(&ss, "    > Time to read coordinate tiles: ", read_coord_tiles);
    write(
        &ss,
        "    > Time to unfilter coordinate tiles: ",
        read_unfilter_coord_tiles);
    write(
        &ss,
        "    > Time to compute range result coordinates: ",
        read_compute_range_coords);
    write(
        &ss,
        "  * Time to compute sparse result cell slabs: ",
        read_compute_sparse_result_cell_slabs_sparse);
    write(
        &ss,
        "  * Time to compute dense result cell slabs: ",
        read_compute_sparse_result_cell_slabs_dense);
    write(
        &ss,
        "  * Time to copy result attribute values: ",
        read_copy_attr_values);
    write(&ss, "    > Time to read attribute tiles: ", read_attr_tiles);
    write(
        &ss,
        "    > Time to unfilter attribute tiles: ",
        read_unfilter_attr_tiles);
    write(
        &ss,
        "    > Time to copy fixed-sized attribute values: ",
        read_copy_fixed_attr_values);
    write(
        &ss,
        "    > Time to copy var-sized attribute values: ",
        read_copy_var_attr_values);
    write(&ss, "  * Time to copy result coordinates: ", read_copy_coords);
    write(
        &ss,
        "    > Time to copy fixed-sized coordinates: ",
        read_copy_fixed_coords);
    write(
        &ss,
        "    > Time to copy var-sized coordinates: ",
        read_copy_var_coords);
    write(&ss, "  * Time to fill dense coordinates: ", read_fill_dense_coords);
    ss << "\n";

    write(
        &ss,
        "- Total read query time (array open + init state + read): ",
        total_read_time);
  }

  return ss.str();
}

std::string Stats::dump_consolidate() const {
  auto consolidate_steps =
      counter_stats_.find(CounterType::CONSOLIDATE_STEP_NUM)->second;
  auto consolidate_frags =
      timer_stats_.find(TimerType::CONSOLIDATE_FRAGS)->second;
  auto consolidate_main =
      timer_stats_.find(TimerType::CONSOLIDATE_MAIN)->second;
  auto consolidate_compute_next =
      timer_stats_.find(TimerType::CONSOLIDATE_COMPUTE_NEXT)->second;
  auto consolidate_array_meta =
      timer_stats_.find(TimerType::CONSOLIDATE_ARRAY_META)->second;
  auto consolidate_frag_meta =
      timer_stats_.find(TimerType::CONSOLIDATE_FRAG_META)->second;
  auto consolidate_create_buffers =
      timer_stats_.find(TimerType::CONSOLIDATE_CREATE_BUFFERS)->second;
  auto consolidate_create_queries =
      timer_stats_.find(TimerType::CONSOLIDATE_CREATE_QUERIES)->second;
  auto consolidate_copy_array =
      timer_stats_.find(TimerType::CONSOLIDATE_COPY_ARRAY)->second;
  std::stringstream ss;

  auto consolidate =
      consolidate_frags + consolidate_frag_meta + consolidate_array_meta;
  if (consolidate != 0) {
    ss << "==== CONSOLIDATION ====\n\n";
    write(&ss, "- Consolidation time: ", consolidate);
    write(&ss, "  * Number of consolidation steps: ", consolidate_steps);
    write(&ss, "  * Time to consolidate fragments: ", consolidate_frags);
    write(
        &ss,
        "    > Time to compute next to consolidate: ",
        consolidate_compute_next);
    write(&ss, "    > Time for main consolidation: ", consolidate_main);
    write(&ss, "      # Time to create buffers: ", consolidate_create_buffers);
    write(&ss, "      # Time to create queries: ", consolidate_create_queries);
    write(&ss, "      # Time to copy the array: ", consolidate_copy_array);
    write(
        &ss,
        "  * Time to consolidate array metadata: ",
        consolidate_array_meta);
    write(
        &ss,
        "  * Time to consolidate fragment metadata: ",
        consolidate_frag_meta);
  }

  return ss.str();
}

std::string Stats::dump_vfs() const {
  auto s3_slow_down_retries =
      counter_stats_.find(CounterType::VFS_S3_SLOW_DOWN_RETRIES)->second;
  std::stringstream ss;

  if (s3_slow_down_retries > 0) {
    ss << "==== VFS ====\n\n";
    write(&ss, "- S3 SLOW_DOWN retries: ", s3_slow_down_retries);
  }

  return ss.str();
}

void Stats::write(
    std::stringstream* ss, const std::string& msg, double secs) const {
  if (secs != 0)
    (*ss) << msg << secs << " secs\n";
}

void Stats::write(
    std::stringstream* ss, const std::string& msg, uint64_t count) const {
  if (count != 0)
    (*ss) << msg << count << "\n";
}

void Stats::write_bytes(
    std::stringstream* ss, const std::string& msg, uint64_t count) const {
  if (count != 0) {
    auto gbs = (double)count / GB_BYTES;
    (*ss) << msg << count << " bytes (" << gbs << " GB) \n";
  }
}

void Stats::write_factor(
    std::stringstream* ss,
    const std::string& msg,
    uint64_t count_a,
    uint64_t count_b) const {
  if (count_a != 0 && count_b != 0)
    (*ss) << msg << (count_a / (double)count_b) << "x\n";
}

void Stats::write_ratio(
    std::stringstream* ss,
    const std::string& msg,
    uint64_t count_a,
    uint64_t count_b) const {
  if (count_a != 0 && count_b != 0)
    (*ss) << msg << (count_a / (double)count_b) * 100.0 << "%\n";
}

void Stats::report_ratio(
    FILE* out,
    const char* msg,
    const char* unit,
    uint64_t numerator,
    uint64_t denominator) const {
  std::stringstream ss;
  ss << msg << ": " << numerator << " / " << denominator << " " << unit;
  fprintf(out, "%s", ss.str().c_str());

  if (denominator > 0) {
    fprintf(out, " (%.1fx)", double(numerator) / double(denominator));
  }
  fprintf(out, "\n");
}

void Stats::report_ratio_pct(
    FILE* out,
    const char* msg,
    const char* unit,
    uint64_t numerator,
    uint64_t denominator) const {
  std::stringstream ss;
  ss << msg << ": " << numerator << " / " << denominator << " " << unit;
  fprintf(out, "%s", ss.str().c_str());

  if (denominator > 0) {
    fprintf(out, " (%.1f%%)", 100.0 * double(numerator) / double(denominator));
  }
  fprintf(out, "\n");
}

}  // namespace stats
}  // namespace sm
}  // namespace tiledb
