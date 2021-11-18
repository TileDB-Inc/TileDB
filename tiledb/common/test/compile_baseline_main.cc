#include "../dynamic_memory/dynamic_memory.h"
#include "../logger.h"
#include "../status.h"
#include "../governor/governor.h"
#include "../heap_memory.h"
#include "../heap_profiler.h"

using namespace tiledb::common;

int main()
{
  auto n = sizeof(Logger) + sizeof(Status) + sizeof(Governor) + sizeof(HeapProfiler);
  tdb_free(tdb_malloc(n));
  auto p = make_shared<int>(HERE(), n);
  return 0;
}
