<a href="https://tiledb.com"><img src="https://github.com/TileDB-Inc/TileDB/raw/dev/doc/source/_static/tiledb-logo_color_no_margin_@4x.png" alt="TileDB logo" width="400"></a>

[![Azure Pipelines](https://dev.azure.com/TileDB-Inc/CI/_apis/build/status/TileDB-Inc.TileDB?branchName=dev)](https://dev.azure.com/TileDB-Inc/CI/_build/latest?definitionId=2&branchName=dev) ![](doc/anaconda.svg)[![Anaconda download count badge](https://anaconda.org/conda-forge/TileDB/badges/downloads.svg)](https://anaconda.org/conda-forge/TileDB)

# The Universal Storage Engine

TileDB is a powerful engine for storing and accessing **dense and sparse multi-dimensional arrays**, which can help you model any complex data efficiently. It is an embeddable C++ library that works on Linux, macOS, and Windows. It is open-sourced under the permissive MIT License, developed and maintained by [TileDB, Inc](https://tiledb.com/). To distinguish this project from other TileDB offerings, we often refer to it as *TileDB Embedded*. 

TileDB includes the following features:

* Support for both **dense** and **sparse arrays**
* Support for **dataframes** and **key-value stores** (via sparse arrays)
* **Cloud storage** (AWS S3, Google Cloud Storage, Azure Blob Storage)
* **Chunked** (tiled) arrays
* Multiple **compression**, **encryption** and **checksum** filters
* Fully **multi-threaded** implementation
* **Parallel IO**
* **Data versioning** (rapid updates, time traveling)
* Array **metadata**
* Array **groups**
* Numerous **APIs** on top of the C++ library
* Numerous **integrations** (Spark, Dask, MariaDB, GDAL, etc.)
 
You can use TileDB to store data in a variety of applications, such as Genomics, Geospatial, Finance and more. The power of TileDB stems from the fact that any data can be modeled efficiently as either a dense or a sparse multi-dimensional array, which is the format used internally by most data science tooling. By storing your data and metadata in TileDB arrays, you abstract all the data storage and management pains, while efficiently accessing the data with your favorite data science tool.

## Quickstart

You can install the TileDB library as follows:

```bash
# Homebrew (macOS):
$ brew update
$ brew install tiledb-inc/stable/tiledb

# Or Conda (macOS, Linux, Windows):
$ conda install -c conda-forge tiledb
```

Alternatively, you can use the Docker image we provide:

```bash
$ docker pull tiledb/tiledb
$ docker run -it tiledb/tiledb
```

We include several [examples](https://github.com/TileDB-Inc/TileDB/tree/dev/examples). You can start with the following:

* [Dense array example](https://github.com/TileDB-Inc/TileDB/blob/dev/examples/cpp_api/quickstart_dense.cc)
* [Sparse array example](https://github.com/TileDB-Inc/TileDB/blob/dev/examples/cpp_api/quickstart_sparse.cc)

## Documentation

You can find the detailed TileDB documentation at [https://docs.tiledb.com](https://docs.tiledb.com).

## Building from source

Please see [building from source](https://docs.tiledb.com/main/how-to/installation/building-from-source) in
the documentation.

## Format Specification

The TileDB data format is open-source and can be found [here](format_spec/FORMAT_SPEC.md).

## APIs

The TileDB team maintains a variety of APIs built on top of the C++ library:

* [Python](https://github.com/TileDB-Inc/TileDB-Py)
* [R](https://github.com/TileDB-Inc/TileDB-R)
* [Java](https://github.com/TileDB-Inc/TileDB-Java)
* [Go](https://github.com/TileDB-Inc/TileDB-Go)

## Integrations

TileDB is also integrated with several popular databases and data science tools:

* [Spark](https://docs.tiledb.com/main/integrations-and-extensions/distributed-computing/spark)
* [Dask](https://docs.tiledb.com/main/integrations-and-extensions/distributed-computing/dask)
* [MariaDB](https://docs.tiledb.com/main/integrations-and-extensions/sql/mariadb)
* [PrestoDB](https://docs.tiledb.com/main/integrations-and-extensions/sql/prestodb)
* [Geospatial](https://docs.tiledb.com/main/integrations-and-extensions/geospatial) (GDAL, PDAL, Rasterio)
* [Genomics](https://docs.tiledb.com/main/integrations-and-extensions/population-genomics) (gVCF)

## Get involved

TileDB Embedded is an open-source project and welcomes all forms of contributions. Contributors to the project should read over the [contribution docs](https://github.com/TileDB-Inc/TileDB/blob/dev/CONTRIBUTING.md) for more information.

We'd love to hear from you. Drop us a line at [hello@tiledb.com](mailto:hello@tiledb.com), visit our [forum](https://forum.tiledb.com/) or [contact form](https://tiledb.com/contact), or [follow us on Twitter](https://twitter.com/tiledb) to stay informed of updates and news.
