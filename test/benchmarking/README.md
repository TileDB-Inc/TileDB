# TileDB benchmarking

This directory contains simple benchmarking programs intended for quick apples-apples performance measurement of common TileDB operations.

## How to run

1. Build TileDB and install a release version locally.

    ```bash
    $ cd TileDB/build
    $ ../bootstrap && make -j4 && make install-tiledb
    $ cd ../dist
    ```

2. Run the benchmarking script.

    ```bash
    $ cd TileDB/test/benchmarking
    $ ./benchmark.py
    ```

The Python script is just a harness for building and executing the standalone benchmark programs, which use the C++ API.

By default the benchmark programs will be linked against the TileDB library in the `TileDB/dist` directory (from step 1 above), so make sure you have a release version installed there.

## Running benchmarks manually

Make sure you have installed TileDB locally as above. Then build the benchmarks:

```bash
$ cd TileDB/test/benchmarking
$ mkdir build && cd build
$ cmake -DCMAKE_PREFIX_PATH=/path/to/TileDB/dist ../src
$ make
```

To run an individual benchmark, just execute it directly in the `build` directory, e.g.:

```bash
$ ./bench_dense_read_small_tile
{ "phase": "setup", "ms": 3492 }
{ "phase": "run", "ms": 3156 }
{ "phase": "teardown", "ms": 3 }
```

The `run` phase time given in milliseconds is the time for a single run of the benchmark. Setup and teardown are typically things like creating an array and removing an array respectively.

You can also run multiple runs of the benchmark, e.g.:

```bash
$ ./bench_dense_read_small_tile setup
{ "phase": "setup", "ms": 3524 }
$ for i in `seq 1 3`; do sudo purge; ./bench_dense_read_small_tile run; done
{ "phase": "run", "ms": 3287 }
{ "phase": "run", "ms": 3206 }
{ "phase": "run", "ms": 3246 }
$ ./bench_dense_read_small_tile teardown
{ "phase": "teardown", "ms": 7 }
```

The above is essentially what the Python benchmark harness script does.

## Adding benchmarks

1. Create a new file `src/bench_<name>.cc`.
2. Subclass from the `BenchmarkBase` class and implement the desired methods.
3. In the `main` function, call the `BenchmarkBase::main` function of an instance of your subclass.
4. Add `bench_<name>` to the `BENCHMARKS` list in `src/CMakeLists.txt`.

When you next run `benchmark.py` it will build and run the added benchmark.