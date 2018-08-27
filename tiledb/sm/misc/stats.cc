#include <thread>

#include "tiledb/sm/misc/stats.h"

namespace tiledb {
namespace sm {
namespace stats {

Statistics all_stats;

Statistics::Statistics() {
  enabled_ = false;
  reset();
}

void Statistics::dump(FILE* out) const {
  fprintf(
      out,
      "===================================== TileDB Statistics Report "
      "=======================================\n");
  fprintf(out, "\nIndividual function statistics:\n");
  fprintf(
      out, "%-60s%20s%22s\n", "  Function name", "# calls", "Total time (ns)");
  fprintf(
      out,
      "  "
      "------------------------------------------------------------------------"
      "----------------------------"
      "\n");
  dump_all_func_stats(out);

  fprintf(out, "\nIndividual counter statistics:\n");
  fprintf(out, "%-60s%20s\n", "  Counter name", "Value");
  fprintf(
      out,
      "  "
      "------------------------------------------------------------------------"
      "------"
      "\n");
  dump_all_counter_stats(out);

  fprintf(out, "\nSummary:\n");
  fprintf(out, "--------\n");
  fprintf(
      out, "Hardware concurrency: %d\n", std::thread::hardware_concurrency());

  fprintf(out, "Reads:\n");
  dump_read_summary(out);

  fprintf(out, "Writes:\n");
  dump_write_summary(out);
}

void Statistics::dump_read_summary(FILE* out) const {
  fprintf(
      out,
      "  Read query submits: %" PRIu64 "\n",
      uint64_t(counter_sm_query_submit_read));

  report_ratio_pct(
      out,
      "  Tile cache hit ratio",
      "",
      counter_reader_attr_tile_cache_hits,
      counter_reader_num_attr_tiles_touched);

  report_ratio_pct(
      out,
      "  Fixed-length tile data copy-to-read ratio",
      "bytes",
      counter_reader_num_fixed_cell_bytes_copied,
      counter_reader_num_fixed_cell_bytes_read);

  report_ratio_pct(
      out,
      "  Var-length tile data copy-to-read ratio",
      "bytes",
      counter_reader_num_var_cell_bytes_copied,
      counter_reader_num_var_cell_bytes_read);

  report_ratio_pct(
      out,
      "  Total tile data copy-to-read ratio",
      "bytes",
      counter_reader_num_fixed_cell_bytes_copied +
          counter_reader_num_var_cell_bytes_copied,
      counter_reader_num_fixed_cell_bytes_read +
          counter_reader_num_var_cell_bytes_read);

  report_ratio(
      out,
      "  Read compression ratio",
      "bytes",
      counter_reader_num_fixed_cell_bytes_read +
          counter_reader_num_var_cell_bytes_read +
          counter_tileio_read_num_resulting_bytes,
      counter_reader_num_tile_bytes_read + counter_tileio_read_num_bytes_read);
}

void Statistics::dump_write_summary(FILE* out) const {
  fprintf(
      out,
      "  Write query submits: %" PRIu64 "\n",
      uint64_t(counter_sm_query_submit_write));

  fprintf(
      out,
      "  Tiles written: %" PRIu64 "\n",
      uint64_t(counter_writer_num_attr_tiles_written));

  report_ratio(
      out,
      "  Write compression ratio",
      "bytes",
      counter_writer_num_input_bytes + counter_tileio_write_num_input_bytes,
      counter_writer_num_bytes_written +
          counter_tileio_write_num_bytes_written);
}

void Statistics::report_ratio(
    FILE* out,
    const char* msg,
    const char* unit,
    uint64_t numerator,
    uint64_t denominator) const {
  fprintf(
      out,
      "%s: %" PRIu64 " / %" PRIu64 " %s",
      msg,
      numerator,
      denominator,
      unit);
  if (denominator > 0) {
    fprintf(out, " (%.1fx)", double(numerator) / double(denominator));
  }
  fprintf(out, "\n");
}

void Statistics::report_ratio_pct(
    FILE* out,
    const char* msg,
    const char* unit,
    uint64_t numerator,
    uint64_t denominator) const {
  fprintf(
      out,
      "%s: %" PRIu64 " / %" PRIu64 " %s",
      msg,
      numerator,
      denominator,
      unit);
  if (denominator > 0) {
    fprintf(out, " (%.1f%%)", 100.0 * double(numerator) / double(denominator));
  }
  fprintf(out, "\n");
}

bool Statistics::enabled() const {
  return enabled_;
}

void Statistics::set_enabled(bool enabled) {
  enabled_ = enabled;
}

}  // namespace stats
}  // namespace sm
}  // namespace tiledb