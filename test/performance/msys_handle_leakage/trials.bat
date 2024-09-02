rem run from root of build tree...
if not exist .\test\performance\RelWithDebInfo\tiledb_explore_msys_handle_leakage.exe goto err
rem .\test\performance\RelWithDebInfo\tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=1 --perform-query=0 --consolidate-sparse-iters=1 --wait-for-keypress both
.\test\performance\RelWithDebInfo\tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=1 --perform-query=0 --consolidate-sparse-iters=1
.\test\performance\RelWithDebInfo\tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=1 --perform-query=1 --consolidate-sparse-iters=1
.\test\performance\RelWithDebInfo\tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=150 --perform-query=1 --consolidate-sparse-iters=1
.\test\performance\RelWithDebInfo\tiledb_explore_msys_handle_leakage.exe --read-sparse-iters=300 --perform-query=1 --consolidate-sparse-iters=1
goto done

:err
echo "Failed to find %cd%\test\performance\RelWithDebInfo\tiledb_explore_msys_handle_leakage.exe
goto done

:done
