# TileDB benchmarking

This directory contains simple benchmarking programs intended for quick apples-apples performance measurement of common TileDB operations.

## How to run

1. Build TileDB and install a release version locally.

    ```bash
    $ cd TileDB/build
    $ ../bootstrap && make -j4 install
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

## Running with S3 backend

Benchmarks can target S3 storage instead of local filesystem. Pass the
`--backend s3` flag along with `--uri-prefix` and any required S3 options:

```bash
$ ./benchmark.py --backend s3 \
    --uri-prefix "s3://my-bucket/benchmarks/" \
    --s3-region us-east-1
```

For a local S3-compatible endpoint such as MinIO or LocalStack:

```bash
$ ./benchmark.py --backend s3 \
    --uri-prefix "s3://my-bucket/benchmarks/" \
    --s3-endpoint localhost:9999 \
    --s3-scheme https \
    --s3-virtual-addressing false
```

When running manually, set the corresponding environment variables:

```bash
$ BENCH_URI_PREFIX=s3://my-bucket/benchmarks/ \
  BENCH_S3_REGION=us-east-1 \
  ./bench_dense_read_large_tile
```

## Running with REST server (tiledb:// URIs)

Benchmarks can target a TileDB REST server using `tiledb://` URIs:

```bash
$ ./benchmark.py --backend rest \
    --uri-prefix "tiledb://my-namespace/" \
    --rest-server "https://api.tiledb.com" \
    --rest-token "YOUR_API_TOKEN"
```

Or manually with environment variables:

```bash
$ BENCH_URI_PREFIX=tiledb://my-namespace/ \
  BENCH_REST_SERVER=https://api.tiledb.com \
  BENCH_REST_TOKEN=YOUR_API_TOKEN \
  ./bench_dense_read_large_tile
```

When using S3 or REST backends, the harness skips local filesystem cache
operations (`sync` and `purge`) since they are not relevant for remote storage.

## Environment variables reference

| Variable                          | Purpose                                      |
| --------------------------------- | -------------------------------------------- |
| `BENCH_URI_PREFIX`                | Prepended to array name to form full URI     |
| `BENCH_S3_ENDPOINT`               | S3 endpoint override (for MinIO, LocalStack) |
| `BENCH_S3_REGION`                 | S3 region                                    |
| `BENCH_S3_SCHEME`                 | S3 connection scheme                         |
| `BENCH_S3_USE_VIRTUAL_ADDRESSING` | Virtual host-style addressing (true/false)   |
| `BENCH_REST_SERVER`               | REST server address                          |
| `BENCH_REST_TOKEN`                | REST API token                               |

## Adding benchmarks

1. Create a new file `src/bench_<name>.cc`.
2. Subclass from the `BenchmarkBase` class and implement the desired methods.
3. Include `benchmark_config.h` and use `bench_uri("name")` for the array URI,
   `bench_config()` for the `Context` config, and `bench_teardown(ctx, uri)`
   in teardown.
4. In the `main` function, call the `BenchmarkBase::main` function of an instance of your subclass.
5. Add `bench_<name>` to the `BENCHMARKS` list in `src/CMakeLists.txt`.

When you next run `benchmark.py` it will build and run the added benchmark.