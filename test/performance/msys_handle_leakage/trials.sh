# run from root of build tree
if [ ! -f ./tiledb/test/performance/tiledb_explore_msys_handle_leakage.exe ]; then
  echo "failed to find `pwd`/tiledb/test/performance/tiledb_explore_msys_handle_leakage.exe"
  return 1
fi
set -x
# ./tiledb/test/performance/tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=1 --perform-query=1 --consolidate-sparse-iters=1 --wait-for-keypress both
./tiledb/test/performance/tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=1 --perform-query=0 --consolidate-sparse-iters=1
./tiledb/test/performance/tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=1 --perform-query=1 --consolidate-sparse-iters=1
./tiledb/test/performance/tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=150 --perform-query=1 --consolidate-sparse-iters=1
./tiledb/test/performance/tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=300 --perform-query=1 --consolidate-sparse-iters=1
