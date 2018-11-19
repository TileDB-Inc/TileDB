# TileDB v1.4.1 Release Notes

## Bug fixes

* Fixed bug in incomplete queries, which should always return partial results. An incomplete status with 0 returned results must always mean that the buffers cannot even fit a single cell value. [#1056](https://github.com/TileDB-Inc/TileDB/pull/1056)
* Fixed performance bug during global order write finalization. [#1065](https://github.com/TileDB-Inc/TileDB/pull/1065)
* Fixed error in linking against static TileDB on Windows. [#1058](https://github.com/TileDB-Inc/TileDB/pull/1058)
* Fixed build error when building without TBB. [#1051](https://github.com/TileDB-Inc/TileDB/pull/1051)

## Improvements

* Set LZ4, Zlib and Zstd compressors to build in release mode. [#1034](https://github.com/TileDB-Inc/TileDB/pull/1034)
* Changed coordinates to always be split before filtering. [#1054](https://github.com/TileDB-Inc/TileDB/pull/1054) 
* Added type-safe filter option methods to C++ API. [#1062](https://github.com/TileDB-Inc/TileDB/pull/1062)

# TileDB v1.4.0 Release Notes

The 1.4.0 release brings two new major features, attribute filter lists and at-rest array encryption, along with bugfixes and performance improvements.

**Note:** TileDB v1.4.0 changes the on-disk array format. Existing arrays should be re-written using TileDB v1.4.0 before use. Starting from v1.4.0 and on, backwards compatibility for reading old-versioned arrays will be fully supported.

## New features

* All array data can now be encrypted at rest using AES-256-GCM symmetric encryption. [#968](https://github.com/TileDB-Inc/TileDB/pull/968)
* Negative and real-valued domain types are now fully supported. [#885](https://github.com/TileDB-Inc/TileDB/pull/885)
* New filter API for transforming attribute data with an ordered list of filters. [#912](https://github.com/TileDB-Inc/TileDB/pull/912)
* Current filters include: previous compressors, bit width reduction, bitshuffle, byteshuffle, and positive-delta encoding.
    * The bitshuffle filter uses an implementation by [Kiyoshi Masui](https://github.com/kiyo-masui/bitshuffle).
    * The byteshuffle filter uses an implementation by [Francesc Alted](https://github.com/Blosc/c-blosc) (from the Blosc project).
* Arrays can now be opened at specific timestamps. [#984](https://github.com/TileDB-Inc/TileDB/pull/984)

## Deprecations

* The C and C++ APIs for compression have been deprecated. The corresponding filter API should be used instead. The compression API will be removed in a future TileDB version. [#1008](https://github.com/TileDB-Inc/TileDB/pull/1008)
* Removed Blosc compressors (obviated by byteshuffle -> compressor filter list).

## Bug fixes

* Fix issue where performing a read query with empty result could cause future reads to return empty [#882](https://github.com/TileDB-Inc/TileDB/pull/882)
* Fix TBB initialization bug with multiple contexts [#898](https://github.com/TileDB-Inc/TileDB/pull/898) 
* Fix bug in max buffer sizes estimation [#903](https://github.com/TileDB-Inc/TileDB/pull/903)
* Fix Buffer allocation size being incorrectly set on realloc [#911](https://github.com/TileDB-Inc/TileDB/pull/911)

## Improvements

* Added check if the coordinates fall out-of-bounds (i.e., outside the array domain) during sparse writes, and added config param `sm.check_coord_oob` to enable/disable the check (enabled by default). [#996](https://github.com/TileDB-Inc/TileDB/pull/996)
* Add config params `sm.num_reader_threads` and `sm.num_writer_threads` for separately controlling I/O parallelism from compression parallelism.
* Added contribution guidelines [#899](https://github.com/TileDB-Inc/TileDB/pull/899)
* Enable building TileDB in Cygwin environment on Windows [#890](https://github.com/TileDB-Inc/TileDB/pull/890)
* Added a simple benchmarking script and several benchmark programs [#889](https://github.com/TileDB-Inc/TileDB/pull/889)
* Changed C API and disk format integer types to have explicit bit widths. [#981](https://github.com/TileDB-Inc/TileDB/pull/981)

## API additions

### C API

* Added `tiledb_{array,kv}_open_at`, `tiledb_{array,kv}_open_at_with_key` and `tiledb_{array,kv}_reopen_at`.
* Added `tiledb_{array,kv}_get_timestamp()`.
* Added `tiledb_kv_is_open`
* Added `tiledb_filter_t` `tiledb_filter_type_t`, `tiledb_filter_option_t`, and `tiledb_filter_list_t` types
* Added `tiledb_filter_*` and `tiledb_filter_list_*` functions.
* Added `tiledb_attribute_{set,get}_filter_list`, `tiledb_array_schema_{set,get}_coords_filter_list`, `tiledb_array_schema_{set,get}_offsets_filter_list` functions.
* Added `tiledb_query_get_buffer` and `tiledb_query_get_buffer_var`.
* Added `tiledb_array_get_uri`
* Added `tiledb_encryption_type_t`
* Added `tiledb_array_create_with_key`, `tiledb_array_open_with_key`, `tiledb_array_schema_load_with_key`, `tiledb_array_consolidate_with_key`
* Added `tiledb_kv_create_with_key`, `tiledb_kv_open_with_key`, `tiledb_kv_schema_load_with_key`, `tiledb_kv_consolidate_with_key`

### C++ API

* Added encryption overloads for `Array()`, `Array::open()`, `Array::create()`, `ArraySchema()`, `Map()`, `Map::open()`, `Map::create()` and `MapSchema()`.
* Added `Array::timestamp()` and `Array::reopen_at()` methods.
* Added `Filter` and `FilterList` classes
* Added `Attribute::filter_list()`, `Attribute::set_filter_list()`, `ArraySchema::coords_filter_list()`, `ArraySchema::set_coords_filter_list()`, `ArraySchema::offsets_filter_list()`, `ArraySchema::set_offsets_filter_list()` functions.
* Added overloads for `Array()`, `Array::open()`, `Map()`, `Map::open()` for handling timestamps.

## Breaking changes

### C API

* Removed Blosc compressors.
* Removed `tiledb_kv_set_max_buffered_items`.
* Modified `tiledb_kv_open` to not take an attribute subselection, but instead take as input the 
query type (similar to arrays). This makes the key-value store behave similarly to arrays,
which means that the key-value store does not support interleaved reads/writes any more
(an opened key-value store is used either for reads or writes, but not both).   
* `tiledb_kv_close` does not flush the written items. Instead, `tiledb_kv_flush` must be
invoked explicitly.

### C++ API

* Removed Blosc compressors.
* Removed `Map::set_max_buffered_items`.
* Modified `Map::Map` and `Map::open` to not take an attribute subselection, but instead take as input the 
query type (similar to arrays). This makes the key-value store behave similarly to arrays,
which means that the key-value store does not support interleaved reads/writes any more
(an opened key-value store is used either for reads or writes, but not both).   
* `Map::close` does not flush the written items. Instead, `Map::flush` must be
invoked explicitly.

# TileDB v1.3.2 Release Notes

## Bug fixes

* Fix read query bug from multiple fragments when query layout differs from array layout [#869](https://github.com/TileDB-Inc/TileDB/pull/869)
* Fix error when consolidating empty arrays [#861](https://github.com/TileDB-Inc/TileDB/pull/861)
* Fix bzip2 external project URL [#875](https://github.com/TileDB-Inc/TileDB/pull/875)
* Invalidate cached buffer sizes when query subarray changes [#882](https://github.com/TileDB-Inc/TileDB/pull/882)

## Improvements

* Add check to ensure tile extent greater than zero [#866](https://github.com/TileDB-Inc/TileDB/pull/866)
* Add `TILEDB_INSTALL_LIBDIR` CMake option [#858](https://github.com/TileDB-Inc/TileDB/pull/858)
* Remove `TILEDB_USE_STATIC_*` CMake variables from build [#871](https://github.com/TileDB-Inc/TileDB/pull/871)
* Allow HDFS init to succeed even if libhdfs is not found [#873](https://github.com/TileDB-Inc/TileDB/pull/873)

# TileDB v1.3.1 Release Notes

## Bug fixes
* Add missing checks when setting subarray for sparse writes [#843](https://github.com/TileDB-Inc/TileDB/pull/843)
* Fix dl linking build issue for C/C++ examples on Linux [#844](https://github.com/TileDB-Inc/TileDB/pull/844)
* Add missing type checks for C++ api Query object [#845](https://github.com/TileDB-Inc/TileDB/pull/845)
* Add missing check that coordinates are provided for sparse writes [#846](https://github.com/TileDB-Inc/TileDB/pull/846)

## Improvements

* Fixes to compile on llvm v3.5 [#831](https://github.com/TileDB-Inc/TileDB/pull/831)
* Add option disable building unittests [#836](https://github.com/TileDB-Inc/TileDB/pull/836)

# TileDB v1.3.0 Release Notes

Version 1.3.0 focused on performance, stability, documentation and API improvements/enhancements.

## New features

* New guided tutorial series added to documentation.
* Query times improved dramatically with internal parallelization using TBB (multiple PRs)
* Optional deduplication pass on writes can be enabled [#636](https://github.com/TileDB-Inc/TileDB/pull/636)
* New internal statistics reporting system to aid in performance optimization [#736](https://github.com/TileDB-Inc/TileDB/pull/736)
* Added string type support: ASCII, UTF-8, UTF-16, UTF-32, UCS-2, UCS-4 [#415](https://github.com/TileDB-Inc/TileDB/pull/415)
* Added `TILEDB_ANY` datatype [#446](https://github.com/TileDB-Inc/TileDB/pull/446)
* Added parallelized VFS read operations, enabled by default [#499](https://github.com/TileDB-Inc/TileDB/pull/499)
* SIGINT signals will cancel in-progress queries [#578](https://github.com/TileDB-Inc/TileDB/pull/578)

## Improvements

* Arrays must now be open and closed before issuing queries, which clarifies the TileDB consistency model.
* Improved handling of incomplete queries and variable-length attribute data.
* Added parallel S3, POSIX, and Win32 reads and writes, enabled by default.
* Query performance improvements with parallelism (using TBB as a dependency).
* Got rid of special S3 "directory objects."
* Refactored sparse reads, making them simpler and more amenable to parallelization.
* Refactored dense reads, making them simpler and more amenable to parallelization.
* Refactored dense ordered writes, making them simpler and more amenable to parallelization.
* Refactored unordered writes, making them simpler and more amenable to parallelization.
* Refactored global writes, making them simpler and more amenable to parallelization.
* Added ability to cancel pending background/async tasks. SIGINT signals now cancel pending tasks.
* Async queries now use a configurable number of background threads (default number of threads is 1).
* Added checks for duplicate coordinates and option for coordinate deduplication.
* Map usage via the C++ API `operator[]` is faster, similar to the `MapItem` path.

## Bug Fixes

* Fixed bugs with reads/writes of variable-sized attributes.
* Fixed file locking issue with simultaneous queries.
* Fixed S3 issues with simultaneous queries within the same context.

## API additions

### C API

* Added `tiledb_array_alloc`
* Added `tiledb_array_{open, close, free}`
* Added `tiledb_array_reopen`
* Added `tiledb_array_is_open`
* Added `tiledb_array_get_query_type`
* Added `tiledb_array_get_schema`
* Added `tiledb_array_max_buffer_size` and `tiledb_array_max_buffer_size_var`
* Added `tiledb_query_finalize` function.
* Added `tiledb_ctx_cancel_tasks` function.
* Added `tiledb_query_set_buffer` and `tiledb_query_set_buffer_var` which sets a single attribute buffer
* Added `tiledb_query_get_type`
* Added `tiledb_query_has_results`
* Added `tiledb_vfs_get_config` function.
* Added `tiledb_stats_{enable,disable,reset,dump}` functions.
* Added `tiledb_kv_alloc`
* Added `tiledb_kv_reopen`
* Added `tiledb_kv_has_key` to check if a key exists in the key-value store.
* Added `tiledb_kv_free`.
* Added `tiledb_kv_iter_alloc` which takes as input a kv object
* Added `tiledb_kv_schema_{set,get}_capacity`.
* Added `tiledb_kv_is_dirty`
* Added `tiledb_kv_iter_reset`
* Added `sm.num_async_threads`, `sm.num_tbb_threads`, and `sm.enable_signal_handlers` config parameters.
* Added `sm.check_dedup_coords` and `sm.dedup_coords` config parameters.
* Added `vfs.num_threads` and `vfs.min_parallel_size` config parameters.
* Added `vfs.{s3,file}.max_parallel_ops` config parameters.
* Added `vfs.s3.multipart_part_size` config parameter.
* Added `vfs.s3.proxy_{scheme,host,port,username,password}` config parameters.

### C++ API

* Added `Array::{open, close}`
* Added `Array::reopen`
* Added `Array::is_open`
* Added `Array::query_type`
* Added `Context::cancel_tasks()` function.
* Added `Query::finalize()` function.
* Added `Query::query_type`
* Added `Query::has_results`
* Changed the return type of the `Query` setters to return the object reference.
* Added an extra `Query` constructor that omits the query type (this is inherited from the input array).
* Added `Map::{open, close}`
* Added `Map::reopen`
* Added `Map::is_dirty`
* Added `Map::has_key` to check for key presence.
* A `tiledb::Map` defined with only one attribute will allow implicit usage, e.x. `map[key] = val` instead of `map[key][attr] = val`.
* Added optional attributes argument in `Map::Map` and `Map::open`
* `MapIter` can be used to create iterators for a map.
* Added `MapIter::reset`
* Added `MapSchema::set_capacity` and `MapSchema::capacity`
* Support for trivially copyable objects, such as a custom data struct, was added. They will be backed by an `sizeof(T)` sized `char` attribute.
* `Attribute::create<T>` can now be used with compound `T`, such as `std::string` and `std::vector<T>`, and other
  objects such as a simple data struct.
* Added a `Dimension::create` factory function that does not take tile extent,
  which sets the tile extent to `NULL`.
* `tiledb::Attribute` can now be constructed with an enumerated type (e.x. `TILEDB_CHAR`).
* Added `Stats` class (wraps C API `tiledb_stats_*` functions)
* Added `Config::save_to_file`

## Breaking changes

### C API

* `tiledb_query_finalize` must **always** be called before `tiledb_query_free` after global-order writes.
* Removed `tiledb_vfs_move` and added `tiledb_vfs_move_file` and `tiledb_vfs_move_dir` instead.
* Removed `force` argument from `tiledb_vfs_move_*` and `tiledb_object_move`.
* Removed `vfs.s3.file_buffer_size` config parameter.
* Removed `tiledb_query_get_attribute_status`.
* All `tiledb_*_free` functions now return `void` and do not take `ctx` as input (except for `tiledb_ctx_free`).
* Changed signature of `tiledb_kv_close` to take a `tiledb_kv_t*` argument instead of `tiledb_kv_t**`.
* Renamed `tiledb_domain_get_rank` to `tiledb_domain_get_ndim` to avoid confusion with matrix def of rank.
* Changed signature of `tiledb_array_get_non_empty_domain`.
* Removed `tiledb_array_compute_max_read_buffer_sizes`.
* Changed signature of `tiledb_{array,kv}_open`.
* Removed `tiledb_kv_iter_create`
* Renamed all C API functions that create TileDB objects from `tiledb_*_create` to `tiledb_*_alloc`.
* Removed `tiledb_query_set_buffers`
* Removed `tiledb_query_reset_buffers`
* Added query type argument to `tiledb_array_open`
* Changed argument order in `tiledb_config_iter_alloc`, `tiledb_ctx_alloc`, `tiledb_attribute_alloc`, `tiledb_dimension_alloc`, `tiledb_array_schema_alloc`, `tiledb_kv_schema_load`, `tiledb_kv_get_item`, `tiledb_vfs_alloc`

### C++ API

* Fixes with `Array::max_buffer_elements` and `Query::result_buffer_elements` to comply with the API docs. `pair.first` is the number of elements of the offsets buffer. If `pair.first` is 0, it is a fixed-sized attribute or coordinates.
* `std::array<T, N>` is backed by a `char` tiledb attribute since the size is not guaranteed.
* Headers have the `tiledb_cpp_api_` prefix removed. For example, the include is now `#include <tiledb/attribute.h>`
* Removed `VFS::move` and added `VFS::move_file` and `VFS::move_dir` instead.
* Removed `force` argument from `VFS::move_*` and `Object::move`.
* Removed `vfs.s3.file_buffer_size` config parameter.
* `Query::finalize` must **always** be called before going out of scope after global-order writes.
* Removed `Query::attribute_status`.
* The API was made header only to improve cross-platform compatibility. `config_iter.h`, `filebuf.h`, `map_item.h`, `map_iter.h`, and `map_proxy.h` are no longer available, but grouped into the headers of the objects they support.
* Previously a `tiledb::Map` could be created from a `std::map`, an anonymous attribute name was defined. This must now be explicitly defined: `tiledb::Map::create(tiledb::Context, std::string uri, std::map, std::string attr_name)`
* Removed `tiledb::Query::reset_buffers`. Any previous usages can safely be removed.
* `Map::begin` refers to the same iterator object. For multiple concurrent iterators, a `MapIter` should be manually constructed instead of using `Map::begin()` more than once.
* Renamed `Domain::rank` to `Domain::ndim` to avoid confusion with matrix def of rank.
* Added query type argument to `Array` constructor
* Removed iterator functionality from `Map`.
* Removed `Array::parition_subarray`.

# TileDB v1.2.2 Release Notes

## Bug fixes

* Fix I/O bug on POSIX systems with large reads/writes ([#467](https://github.com/TileDB-Inc/TileDB/pull/467))
* Memory overflow error handling (moved from constructors to init functions) ([#472](https://github.com/TileDB-Inc/TileDB/pull/472))
* Memory leaks with realloc in case of error ([#472](https://github.com/TileDB-Inc/TileDB/pull/472))
* Handle non-existent config param in C++ API ([#475](https://github.com/TileDB-Inc/TileDB/pull/475))
* Read query overflow handling ([#485](https://github.com/TileDB-Inc/TileDB/pull/485))

## Improvements

* Changed S3 default config so that AWS S3 just works ([#455](https://github.com/TileDB-Inc/TileDB/pull/455))
* Minor S3 optimizations and error message fixes ([#462](https://github.com/TileDB-Inc/TileDB/pull/462))
* Documentation additions including S3 usage ([#456](https://github.com/TileDB-Inc/TileDB/pull/456), [#458](https://github.com/TileDB-Inc/TileDB/pull/458), [#459](https://github.com/TileDB-Inc/TileDB/pull/459))
* Various CI improvements ([#449](https://github.com/TileDB-Inc/TileDB/pull/449))

# TileDB v1.2.1 Release Notes

## Bug fixes

* Fixed TileDB header includes for all examples ([#409](https://github.com/TileDB-Inc/TileDB/pull/409))
* Fixed TileDB library dynamic linking problem for C++ API ([#412](https://github.com/TileDB-Inc/TileDB/pull/412))
* Fixed VS2015 build errors ([#424](https://github.com/TileDB-Inc/TileDB/pull/424))
* Bug fix in the sparse case ([#434](https://github.com/TileDB-Inc/TileDB/pull/434))
* Bug fix for 1D vector query layout ([#438](https://github.com/TileDB-Inc/TileDB/pull/438))

## Improvements

* Added documentation to API and examples ([#410](https://github.com/TileDB-Inc/TileDB/pull/410), [#414](https://github.com/TileDB-Inc/TileDB/pull/414))
* Migrated docs to Readthedocs ([#418](https://github.com/TileDB-Inc/TileDB/pull/418), [#420](https://github.com/TileDB-Inc/TileDB/pull/420), [#422](https://github.com/TileDB-Inc/TileDB/pull/422), [#423](https://github.com/TileDB-Inc/TileDB/pull/423), [#425](https://github.com/TileDB-Inc/TileDB/pull/425))
* Added dimension domain/tile extent checks ([#429](https://github.com/TileDB-Inc/TileDB/pull/429))


# TileDB v1.2.0 Release Notes
The 1.2.0 release of TileDB includes many new features, improvements in stability and performance, and two new language interfaces (Python and C++). There are also several breaking changes in the C API and on-disk format, documented below.

**Important Note**: due to several improvements and changes in the underlying array storage mechanism, you will need to recreate any existing arrays in order to use them with TileDB v1.2.0.

## New features
* Windows support. TileDB is now fully supported on Windows systems (64-bit Windows 7 and newer).
* Python API. We are very excited to announce the initial release of a Python API for TileDB. The Python API makes TileDB accessible to a much broader audience, allowing seamless integration with existing Python libraries such as NumPy, Pandas and the scientific Python ecosystem.
* C++ API. We've included a C++ API, which allows TileDB to integrate into modern C++ applications without having to write code towards the C API.  The C++ API is more concise and provides additional compile time type safety.
* S3 object store support. You can now easily store, query, and manipulate your TileDB arrays on S3 API compatibile object stores, including Amazon's AWS S3 service.
* Virtual filesystem interface. The TileDB API now exposes a virtual filesystem (or VFS) interface, allowing you to perform tasks such as file creation, deletion, reads, and appends without worrying about whether your files are stored on S3, HDFS, a POSIX or Windows filesystem, etc.
* Key-value store. TileDB now provides a key-value (meta) data storage abstraction.  Its implementation is built upon TileDB's sparse arrays and inherits all the properties of TileDB sparse arrays.

## Improvements
* Homebrew formula added for easier installation on macOS.  Homebrew is now the perferred method for installing TileDB and its dependencies on macOS.
* Docker images updated to include stable/unstable/dev options, and easy configuration of additional components (e.g. S3 support).
* Tile cache implemented, which will greatly speed up repeated queries on overlapping regions of the same array.
* Ability to pass runtime configuration arguments to TileDB/VFS backends.
* Unnamed (or "anonymous") dimensions are now supported; having a single anonymous attribute is also supported.
* Concurrency bugfixes for several compressors.
* Correctness issue fixed in double-delta compressor for some datatypes.
* Better build behavior on systems with older GCC or CMake versions.
* Several memory leaks and overruns fixed with help of sanitizers.
* Many improved error condition checks and messages for easier debugging.
* Many other small bugs and API inconsistencies fixed.

## C API additions
* `tiledb_config_*`:  Types and functions related to the new configuration object and functionality.
* `tiledb_config_iter_*`: Iteration functionality for retieving parameters/values from the new configuration object.
* `tiledb_ctx_get_config()`: Function to get a configuration from a context.
* `tiledb_filesystem_t`: Filesystem type enum.
* `tiledb_ctx_is_supported_fs()`: Function to check for support for a given filesystem backend.
* `tiledb_error_t`, `tiledb_error_message()` and `tiledb_error_free()`: Type and functions for TileDB error messages.
* `tiledb_ctx_get_last_error()`: Function to get last error from context.
* `tiledb_domain_get_rank()`: Function to retrieve number of dimensions in a domain.
* `tiledb_domain_get_dimension_from_index()` and `tiledb_domain_get_dimension_from_name()`: Replaces dimension iterators.
* `tiledb_dimension_{create,free,get_name,get_type,get_domain,get_tile_extent}()`: Functions related to creation and manipulation of `tiledb_dimension_t` objects.
* `tiledb_array_schema_set_coords_compressor()`: Function to set the coordinates compressor.
* `tiledb_array_schema_set_offsets_compressor()`: Function to set the offsets compressor.
* `tiledb_array_schema_get_attribute_{num,from_index,from_name}()`: Replaces attribute iterators.
* `tiledb_query_create()`: Replaced many arguments with new `tiledb_query_set_*()` setter functions.
* `tiledb_array_get_non_empty_domain()`: Function to retrieve the non-empty domain from an array.
* `tiledb_array_compute_max_read_buffer_sizes()`: Function to compute an upper bound on the buffer sizes required for a read query.
* `tiledb_object_ls()`: Function to visit the children of a path.
* `tiledb_uri_to_path()`: Function to convert a file:// URI to a platform-native path.
* `TILEDB_MAX_PATH` and `tiledb_max_path()`: The maximum length for tiledb resource paths.
* `tiledb_kv_*`: Types and functions related to the new key-value store functionality.
* `tiledb_vfs_*`: Types and functions related to the new virtual filesystem (VFS) functionality.

## Breaking changes

### C API
* Rename `tiledb_array_metadata_t` -> `tiledb_array_schema_t`, and associated `tiledb_array_metadata_*` functions to `tiledb_array_schema_*`.
* Remove `tiledb_attribute_iter_t`.
* Remove `tiledb_dimension_iter_t`.
* Rename `tiledb_delete()`, `tiledb_move()`, `tiledb_walk()` to `tiledb_object_{delete,move,walk}()`.
* `tiledb_ctx_create`: Config argument added.
* `tiledb_domain_create`: Datatype argument removed.
* `tiledb_domain_add_dimension`: Name, domain and tile extent arguments replaced with single `tiledb_dimension_t` argument.
* `tiledb_query_create()`: Replaced many arguments with new `tiledb_query_set_*()` setter functions.
* `tiledb_array_create()`: Added array URI argument.
* `tiledb_*_free()`: All free functions now take a pointer to the object pointer instead of simply object pointer.
* The include files are now installed into a `tiledb` folder. The correct path is now `#include <tiledb/tiledb.h>` (or `#include <tiledb/tiledb>` for the C++ API).

### Resource Management
* Support for moving resources across previous VFS backends (local fs <-> HDFS) has been removed.  A more generic implementation for this functionality with improved performance is planned for the next version of TileDB.
