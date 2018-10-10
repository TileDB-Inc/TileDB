<a href="https://tiledb.io"><img src="https://github.com/TileDB-Inc/TileDB/raw/dev/doc/source/_static/tileDB_uppercase_600_112.png" alt="TileDB logo" width="400"></a>

[![Travis](https://travis-ci.org/TileDB-Inc/TileDB.svg?branch=dev)](https://travis-ci.org/TileDB-Inc/TileDB)
[![Appveyor](https://ci.appveyor.com/api/projects/status/kb9h9r5xulk6j53c/branch/dev?svg=true)](https://ci.appveyor.com/project/StavrosPapadopoulos/tiledb/branch/dev)
[![Documentation Status](https://readthedocs.com/projects/tiledb-inc-tiledb/badge/?version=latest)](https://docs.tiledb.io/en/latest/?badge=latest)

**Array data management made fast and easy.**

TileDB allows you to manage the massive dense and sparse multi-dimensional array data that frequently arise in many important scientific applications.

## What is TileDB?

TileDB is an efficient multi-dimensional array management system which introduces a novel format that can effectively store **dense** and **sparse** array data with support for fast updates and reads. It features excellent **compression**, an efficient parallel I/O system for **high scalability**, and high-level APIs including Python, R, Golang and more.

TileDB stores your array data on persistent storage locally or in the cloud, with built-in support for **S3** and **HDFS** storage backends.

TileDB works on Linux, macOS, and Windows, and is open-sourced under the permissive MIT License.

Learn more and see examples and tutorials in the [official documentation](https://docs.tiledb.io/en/latest).

## Features

* **Novel Format.** TileDB introduces a novel multi-dimensional array format that effectively handles both dense and sparse data with fast updates. Contrary to other popular systems like HDF5 that are optimized mostly for dense arrays, TileDB is optimized for both dense and sparse arrays, exposing a unified API. TileDB enables efficient updates through its concept of immutable, append-only "fragments."
* **Multiple Backends.** Transparently store and access your arrays on AWS S3 (or other S3 compatiable store) or HDFS with a single API.
* **Compression.** Achieve high compression ratios with TileDB's tile-based compression approach. TileDB can compress array data with a growing number of compressors, such as GZIP, BZIP2, LZ4, ZStandard, double-delta and run-length encoding.
* **Parallelism.** Use every core with TileDB's parallelized I/O and compression systems (using Intel TBB), and build powerful parallel analytics on top of the TileDB array storage manager (e.g., using OpenMP or MPI) leveraging TileDB's thread-/process-safety.
* **Portability.** TileDB works on Linux, macOS and Windows, offering easy installation packages, binaries and Docker containerization.
* **Language Bindings.** Enable your NumPy data science applications to work with immense amounts of data using TileDB's Python API. Other APIs include Golang, R and Java.
* **Key-value Store.** Store any persistent metadata with TileDB's key-value storage functionality. A TileDB key-value store inherits all the benefits of TileDB arrays such as compression, parallelism, and multiple backend support.
* **Virtual Filesystem.** Add general file management and I/O to your applications for any supported storage backend using TileDB's unified "virtual filesystem" (VFS) API.

## Quickstart

First, grab a TileDB release for your system.

* **Homebrew** (macOS): `brew install tiledb-inc/stable/tiledb`
* **Docker** (Linux/macOS): `docker pull tiledb/tiledb && docker run -it tiledb/tiledb`
* **Conda** (Linux/macOS/Windows): `conda install -c conda-forge tiledb`

For Windows, you can also download the pre-built binaries from a release .zip file: https://github.com/TileDB-Inc/TileDB/releases. For more in-depth installation information, see the full [Installation](https://docs.tiledb.io/en/latest/installation.html) doc page.

Next, save the [quickstart example program](https://github.com/TileDB-Inc/TileDB/blob/dev/examples/cpp_api/quickstart_sparse.cc) to a file `quickstart_sparse.cc`.

The example program illustrates the three main TileDB operations: array creation (`create_array()` in the example code), writing data into the array (`write_array()`), and reading from the array (`read_array()`).

It will first create the array with a simple sparse 2D schema where each cell can store a single character of data. Then, it will write data to 3 cells of the array. Finally, it will read back the cells using a spatial slice.

Compile the example program: `g++ -std=c++11 quickstart_sparse.cc -o quickstart_sparse -ltiledb`.

If you run into compilation issues, see the [Usage](https://docs.tiledb.io/en/latest/usage.html) page for more complete instructions on how to compile and link against TileDB. If you are on Windows, use the [Windows Usage](https://docs.tiledb.io/en/latest/usage.html#windows) instructions to create a Visual Studio project instead.

Run the example, and you should see the following output:

```
$ ./quickstart_sparse
Created array my_array
Cell (0,0) has data 'a'
Cell (1,1) has data 'b'
Cell (2,3) has data 'c'
```

For a full walkthrough of the quickstart program, check out the [Quickstart documentation](https://docs.tiledb.io/en/latest/quickstart.html). Also check out the corresponding [dense](https://docs.tiledb.io/en/latest/quickstart.html#a-simple-dense-array-example) quickstart and [key-value store](https://docs.tiledb.io/en/latest/quickstart.html#a-simple-key-value-example) quickstart programs on the main documentation site [docs.tiledb.io](https://docs.tiledb.io).

## Further reading

Start with the beginner tutorial series on [dense](https://docs.tiledb.io/en/latest/tutorials/dense-arrays.html) and [sparse](https://docs.tiledb.io/en/latest/tutorials/sparse-arrays.html) arrays to learn the basic TileDB concepts.

The full TileDB documentation can be found at [docs.tiledb.io](https://docs.tiledb.io/en/latest/) and includes many more tutorials and examples to get you started.

## Get involved

TileDB is an open source project and welcomes all forms of contributions. Contributors to the project should read over the [contribution docs](https://github.com/TileDB-Inc/TileDB/blob/dev/CONTRIBUTING.md) for more information.

We'd love to hear from you. Drop us a line at [hello@tiledb.io](mailto:hello@tiledb.io), visit our [forum](https://forum.tiledb.io/) or [contact form](https://tiledb.io/contact-us), or [follow us on Twitter](https://twitter.com/tiledb) to stay informed of updates and news.
