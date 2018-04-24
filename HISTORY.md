# In Progress

## New features

* Added string type support: ASCII, UTF-8, UTF-16, UTF-32, UCS-2, UCS-4 (PR #415)
* Added `TILEDB_ANY` datatype (PR #446)
* Added parallelized VFS read operations, enabled by default (PR #499)

## Improvements

* Added parallel S3 writes, enabled by default.
* Minor S3 optimizations and error message fixes (PR #462)
* Changed S3 default config so that AWS S3 just works (PR #455)
* Got rid of special S3 "directory objects"
* Refactored sparse reads, making them simpler and more amenable to parallelization.
* Refactored dense reads, making them simpler and more amenable to parallelization.
* Refactored dense ordered writes, making them simpler and more amenable to parallelization.
* Refactored unordered writes, making them simpler and more amenable to parallelization.
* Refactored global writes, making them simpler and more amenable to parallelization.

## Bug Fixes

* Memory overflow error handling (moved from constructors to init functions)
* Memory leaks with realloc in case of error
* Handle non-existent config param in C++ API.
* Fixed bugs with reads/writes of variable-sized attributes.
* Fixed file locking issue with simultaneous queries.
* Fixed S3 issues with simultaneous queries within the same context.

## API additions

### C API

* Added `tiledb_query_finalize` function. 
* Added `tiledb_vfs_get_config` function.
* Added `vfs.max_parallel_ops` and `vfs.min_parallel_size` config parameters.
* Added `vfs.s3.multipart_part_size` config parameter.

### C++ API
* Support for trivially copyable objects, such as a custom data struct, was added. They will be backed by an `sizeof(T)` sized `char` attribute.
* `Attribute::create<T>` can now be used with compound `T`, such as `std::string` and `std::vector<T>`, and other
  objects such as a simple data struct.
* Added a `Dimension::create` factory function that does not take tile extent,
  which sets the tile extent to `NULL`.
* Added `Query::finalize()` function.

## Breaking changes

### C API

* `tiledb_query_finalize` must **always** be called before `tiledb_query_free` after global-order writes.
* Removed `tiledb_vfs_move` and added `tiledb_vfs_move_file` and `tiledb_vfs_move_dir` instead.
* Removed `force` argument from `tiledb_vfs_move_*` and `tiledb_object_move`.
* Removed `vfs.s3.file_buffer_size` config parameter.

### C++ API
* Fixes with `Array::max_buffer_elements` and `Query::result_buffer_elements` to comply with the API docs. `pair.first` is the number of elements of the offsets buffer. If `pair.first` is 0, it is a fixed-sized attribute or coordinates.
* `std::array<T, N>` is backed by a `char` tiledb attribute since the size is not guaranteed.
* `Attribute::set_cell_val_num()` is deprecated. This is now deduced from the Attribute type.
* Headers have the `tiledb_cpp_api_` prefix removed. For example, the include is now `#include <tiledb/attribute.h>`
* Removed `VFS::move` and added `VFS::move_file` and `VFS::move_dir` instead.
* Removed `force` argument from `VFS::move_*` and `Object::move`.
* Removed `vfs.s3.file_buffer_size` config parameter.
* `Query::finalize` must **always** be called before going out of scope after global-order writes.

# TileDB v1.2.1 Release Notes

## Bug fixes

* Fixed TileDB header includes for all examples (#409)
* Fixed TileDB library dynamic linking problem for C++ API (#412)
* Fixed VS2015 build errors (#424)
* Bug fix in the sparse case (#434)
* Bug fix for 1D vector query layout (#438)

## Improvements

* Added documentation to API and examples (#410, #414)
* Migrated docs to Readthedocs (#418, #420, #422, #423, #425)
* Added dimension domain/tile extent checks (#429)


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
