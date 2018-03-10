<a href="https://tiledb.io"><img src="https://github.com/TileDB-Inc/TileDB/raw/dev/doc/source/_static/tileDB_uppercase_600_112.png" alt="TileDB logo" width="400"></a>

[![Travis](https://travis-ci.org/TileDB-Inc/TileDB.svg?branch=dev)](https://travis-ci.org/TileDB-Inc/TileDB)
[![Appveyor](https://ci.appveyor.com/api/projects/status/kb9h9r5xulk6j53c/branch/dev?svg=true)](https://ci.appveyor.com/project/StavrosPapadopoulos/tiledb/branch/dev)
[![Documentation Status](https://readthedocs.com/projects/tiledb-inc-tiledb/badge/?version=latest)](https://docs.tiledb.io/en/latest/?badge=latest)

**Array data management made fast and easy.**

TileDB allows you to manage the massive dense and sparse multi-dimensional array data that frequently arise in many important scientific applications.

## What is TileDB?

TileDB is an efficient multi-dimensional array management system which introduces a novel on-disk format that can effectively store dense and sparse array data with support for fast updates and reads. It features excellent compression, an efficient parallel I/O system with high scalability, and a Python API.

TileDB works on Linux, macOS, and Windows, and is open-sourced under the permissive MIT License.

Learn more and see examples and tutorials in the [official documentation](https://docs.tiledb.io).

## Features

* **Novel Format.** TileDB introduces a novel multi-dimensional array format that effectively handles both dense and sparse data with fast updates. Contrary to other popular systems (e.g., HDF5) that are optimized mostly for dense arrays, TileDB is optimized for both dense and sparse arrays, exposing a unified array API. In addition, TileDB's concept of immutable, append-only fragments allows for efficient updates.
* **Compression.** Experience fast slicing and dicing of your arrays while achieving high compression ratios with TileDB's tile-based approach. TileDB can compress array data with a growing number of compressors, such as GZIP, BZIP2, LZ4, ZStandard, Blosc, double-delta and run-length encoding.
* **Parallelism.** Build powerful parallel analytics on top of the TileDB array storage manager (e.g., using OpenMP or MPI), leveraging TileDB's thread-/process-safety and asynchronous writes and reads.
* **Portability.** TileDB works on Linux, macOS and Windows, offering easy installation packages, binaries and Docker containerization. Integrate TileDB with the tools of your favorite platform to manage massive multi-dimensional array data.
* **Language Bindings.** Enable your Python and NumPy data science applications to work with immense amounts of data, beyond what can be stored in main memory. TileDB is built in C and C++ for performance and provides a Python API for interoperability and ease of use.
* **Multiple Backends.** Transparently store your arrays across multiple backends such as HDFS or S3-compliant object stores (like AWS S3, minio, or Ceph). TileDB's API is the same regardless of where the array is stored.
* **Key-value Store.** Store any persistent metadata with TileDB's key-value storage functionality. A TileDB key-value store is implemented as a TileDB sparse array and inherits all its benefits (such as compression, parallelism, and multiple backend support).
* **Virtual Filesystem.** Add general file management and IO to your applications for any supported storage backend using TileDB's unified "virtual filesystem" (VFS) API.

## Installation

The [installation guide](https://docs.tiledb.io/en/latest/installation.html) will walk you through installing TileDB on your machine.

The full TileDB documentation can be found at [docs.tiledb.io](https://docs.tiledb.io) and includes many tutorials and examples to get you started.

## Get involved

We'd love to hear from you. Drop us a line at [hello@tiledb.io](mailto:hello@tiledb.io), visit our [contact form](https://tiledb.io/contact-us), or [follow us on Twitter](https://twitter.com/tiledb) to stay informed of updates and news.
