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
      "======================= TileDB Statistics Report "
      "=======================\n");
  fprintf(out, "Function statistics:\n");
  fprintf(
      out, "%-30s%20s%22s\n", "  Function name", "# calls", "Total time (ns)");
  fprintf(
      out,
      "  "
      "----------------------------------------------------------------------"
      "\n");
  dump_all_func_stats(out);

  fprintf(out, "\nCounter statistics:\n");
  fprintf(out, "%-30s%20s\n", "  Counter name", "Value");
  fprintf(
      out,
      "  "
      "------------------------------------------------"
      "\n");
  dump_all_counter_stats(out);
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