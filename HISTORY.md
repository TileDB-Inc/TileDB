# In Progress

## Disk Format

## Breaking C API changes

## Breaking behavior

* Empty dense arrays now return cells with fill values.

## New features

## Improvements

## Deprecations

## Bug fixes

* Fixed bug in setting a fill value for var-sized attributes.

## API additions

### C API

* Added functions `tiledb_attribute_{set,get}_fill_value` to get/set default fill values

### C++ API

* Added functions `Attribute::{set,get}_fill_value` to get/set default fill values

# TileDB v2.0.4 Release Notes

## Improvements

* Fix typo in GCS cmake file for superbuild [#1665](https://github.com/TileDB-Inc/TileDB/pull/1665)

# TileDB v2.0.3 Release Notes

## Improvements

* Add robust retries for S3 SLOW_DOWN errors [#1651](https://github.com/TileDB-Inc/TileDB/pull/1651)
* Improve GCS build process [#1655](https://github.com/TileDB-Inc/TileDB/pull/1655)
* Add generation of pkg-config file [#1656](https://github.com/TileDB-Inc/TileDB/pull/1656)
* S3 should use HEADObject for file size [#1657](https://github.com/TileDB-Inc/TileDB/pull/1657)
* Improvements to stats [#1652](https://github.com/TileDB-Inc/TileDB/pull/1652)
* Add artifacts to releases from CI [#1663](https://github.com/TileDB-Inc/TileDB/pull/1663)

## Bug fixes

* Remove to unneeded semicolons noticed by the -pedantic flag [#1653](https://github.com/TileDB-Inc/TileDB/pull/1653)
* Fix cases were TILEDB_FORCE_ALL_DEPS picked up system builds [#1654](https://github.com/TileDB-Inc/TileDB/pull/1654)
* Allow errors to be show in cmake super build [#1658](https://github.com/TileDB-Inc/TileDB/pull/1658)
* Properly check vacuum files and limit fragment loading [#1661](https://github.com/TileDB-Inc/TileDB/pull/1661)
* Fix edge case where consolidated but unvacuumed array can have coordinates report twice [#1662](https://github.com/TileDB-Inc/TileDB/pull/1662)

## API additions

* Add c-api tiledb_stats_raw_dump[_str] function for raw stats dump [#1660](https://github.com/TileDB-Inc/TileDB/pull/1660)
* Add c++-api Stats::raw_dump function for raw stats dump [#1660](https://github.com/TileDB-Inc/TileDB/pull/1660)

# TileDB v2.0.2 Release Notes

## Bug fixes
* Fix hang on open array v1.6 [#1645](https://github.com/TileDB-Inc/TileDB/pull/1645)

## Improvements
* Allow empty values for variable length attributes [#1646](https://github.com/TileDB-Inc/TileDB/pull/1646)


# TileDB v2.0.1 Release Notes

## Improvements

* Remove deprecated max buffer size APIs from unit tests [#1625](https://github.com/TileDB-Inc/TileDB/pull/1625)
* Remove deprecated max buffer API from examples [#1626](https://github.com/TileDB-Inc/TileDB/pull/1626)
* Remove zipped coords from examples [#1632](https://github.com/TileDB-Inc/TileDB/pull/1632)
* Allow AWSSDK_ROOT_DIR override [#1637](https://github.com/TileDB-Inc/TileDB/pull/1637)

## Deprecations

## Bug fixes

* Allow setting zipped coords multiple times [#1627](https://github.com/TileDB-Inc/TileDB/pull/1627)
* Fix overflow in check_tile_extent [#1635](https://github.com/TileDB-Inc/TileDB/pull/1635)
* Fix C++ Dimension API `{tile_extent,domain}_to_str`. [#1638](https://github.com/TileDB-Inc/TileDB/pull/1638)
* Remove xlock in FragmentMetadata::store [#1639](https://github.com/TileDB-Inc/TileDB/pull/1639)

## API additions

# TileDB v2.0.0 Release Notes

## Disk Format

* Removed file `__coords.tdb` that stored the zipped coordinates in sparse fragments
* Now storing the coordinate tiles on each dimension in separate files
* Changed fragment name format from `__t1_t2_uuid` to `__t1_t2_uuid_<format_version>`. That was necessary for backwards compatibility

## Breaking C API changes

* Changed `domain` input of `tiledb_dimension_get_domain` to `const void**` (from `void**`).
* Changed `tile_extent` input of `tiledb_dimension_get_tile_extent` to `const void**` (from `void**`).
* Anonymous attribute and dimensions (i.e., empty strings for attribute/dimension names) is no longer supported. This is because now the user can set separate dimension buffers to the query and, therefore, supporting anonymous attributes and dimensions creates ambiguity in the current API. 

## Breaking behavior

* Now the TileDB consolidation process does not clean up the fragments or array metadata it consolidates. This is (i) to avoid exclusively locking at any point during consolidation, and (ii) to enable fine-grained time traveling even in the presence of consolidated fragments or array metadata. Instead, we added a special vacuuming API which explicitly cleans up consolidated fragments or array metadata (with appropriate configuration parameters). The vacuuming functions briefly exclusively lock the array.

## New features

* Added string dimension support (currently only `TILEDB_STRING_ASCII`).
* The user can now set separate coordinate buffers to the query. Also any subset of the dimensions is supported.
* The user can set separate filter lists per dimension, as well as the number of values per coordinate.

## Improvements

* Added support for AWS Security Token Service session tokens via configuration option `vfs.s3.session_token`. [#1472](https://github.com/TileDB-Inc/TileDB/pull/1472)
* Added support for indicating zero-value metadata by returning `value_num` == 1 from the `_get_metadatata` and `Array::get_metadata` APIs [#1438](https://github.com/TileDB-Inc/TileDB/pull/1438) (this is a non-breaking change, as the documented return of `value == nullptr` to indicate missing keys does not change)`
* User can set coordinate buffers separately for write queries.
* Added option to enable duplicate coordinates for sparse arrays [#1504](https://github.com/TileDB-Inc/TileDB/pull/1504) 
* Added support for writing at a timestamp by allowing opening an array at a timestamp (previously disabled).
* Added special files with the same name as a fragment directory and an added suffix ".ok", to indicate a committed fragment. This improved the performance of opening an array on object stores significantly, as it avoids an extra REST request per fragment.
* Added functionality to consolidation, which allows consolidating the fragment metadata footers in a single file by toggling a new config parameter. This leads to a huge performance boost when opening an array, as it avoids fetching a separate footer per fragment from storage.
* Various reader parallelizations that boosted read performance significantly.
* Configuration parameters can now be read from environmental variables. `vfs.s3.session_token` -> `TILEDB_VFS_S3_SESSION_TOKEN`. The prefix of `TILEDB_` is configurable via `config.env_var_prefix`. [#1600](https://github.com/TileDB-Inc/TileDB/pull/1600)

## Deprecations
* The TileDB tiledb_array_consolidate_metadata and tiledb_array_consolidate_metadata_with_key C-API routines have been deprecated and will be [removed entirely](https://github.com/TileDB-Inc/TileDB/issues/1591) in a future release. The tiledb_array_consolidate and tiledb_array_consolidate_with_key routines achieve the same behavior when the "sm.consolidation.mode" parameter of the configuration argument is equivalent to "array_meta".
* The TileDB Array::consolidate_metadata CPP-API routine has been deprecated and will be [removed entirely](https://github.com/TileDB-Inc/TileDB/issues/1591) in a future release. The Array::consolidate routine achieves the same behavior when the "sm.consolidation.mode" parameter of the configuration argument is equivalent to "array_meta".

## Bug fixes

* Fixed bug in dense consolidation when the array domain is not divisible by the tile extents.

## API additions

* Added C API function `tiledb_array_has_metadata_key` and C++ API function `Array::has_metadata_key` [#1439](https://github.com/TileDB-Inc/TileDB/pull/1439)
* Added C API functions `tiledb_array_schema_{set,get}_allows_dups` and C++ API functions `Array::set_allows_dups` and `Array::allows_dups`
* Added C API functions `tiledb_dimension_{set,get}_filter_list` and `tiledb_dimension_{set,get}_cell_val_num`
* Added C API functions `tiledb_array_get_non_empty_domain_from_{index,name}`
* Added C API function `tiledb_array_vacuum`
* Added C API functions `tiledb_array_get_non_empty_domain_var_size_from_{index,name}`
* Added C API functions `tiledb_array_get_non_empty_domain_var_from_{index,name}`
* Added C API function `tiledb_array_add_range_var`
* Added C API function `tiledb_array_get_range_var_size`
* Added C API function `tiledb_array_get_range_var`
* Added C++ API functions `Dimension::set_cell_val_num` and `Dimension::cell_val_num`.
* Added C++ API functions `Dimension::set_filter_list` and `Dimension::filter_list`.
* Added C++ API functions `Array::non_empty_domain(unsigned idx)` and `Array::non_empty_domain(const std::string& name)`.
* Added C++ API functions `Domain::dimension(unsigned idx)` and `Domain::dimension(const std::string& name)`.
* Added C++ API function `Array::load_schema(ctx, uri)` and `Array::load_schema(ctx, uri, key_type, key, key_len)`.
* Added C++ API function `Array::vacuum`.
* Added C++ API functions `Array::non_empty_domain_var` (from index and name).
* Added C++ API function `add_range` with string inputs.
* Added C++ API function `range` with string outputs.
* Added C++ API functions `Array` and `Context` constructors which take a c_api object to wrap. [#1623](https://github.com/TileDB-Inc/TileDB/pull/1623)

## API removals

# TileDB v1.7.7 Release Notes

## Bug fixes

* Fix expanded domain consolidation [#1572](https://github.com/TileDB-Inc/TileDB/pull/1572)

# TileDB v1.7.6 Release Notes

## New features

* Add MD5 and SHA256 checksum filters [#1515](https://github.com/TileDB-Inc/TileDB/pull/1515)

## Improvements

* Added support for AWS Security Token Service session tokens via configuration option `vfs.s3.session_token`. [#1472](https://github.com/TileDB-Inc/TileDB/pull/1472)

## Deprecations

## Bug fixes

* Fix new SHA1 for intel TBB in superbuild due to change in repository name [#1551](https://github.com/TileDB-Inc/TileDB/pull/1551)

## API additions

## API removals

# TileDB v1.7.5 Release Notes

## New features

## Improvements

* Avoid useless serialization of Array Metadata on close [#1485](https://github.com/TileDB-Inc/TileDB/pull/1485)
* Update CONTRIBUTING and Code of Conduct [#1487](https://github.com/TileDB-Inc/TileDB/pull/1487)

## Deprecations

## Bug fixes

* Fix deadlock in writes of TileDB Cloud Arrays [#1486](https://github.com/TileDB-Inc/TileDB/pull/1486)

## API additions

## API removals


# TileDB v1.7.4 Release Notes

## New features

## Improvements

* REST requests now will use http compression if available [#1479](https://github.com/TileDB-Inc/TileDB/pull/1479)

## Deprecations

## Bug fixes

## API additions

## API removals

# TileDB v1.7.3 Release Notes

## New features

## Improvements

* Array metadata fetching is now lazy (fetch on use) to improve array open performance [#1466](https://github.com/TileDB-Inc/TileDB/pull/1466)
* libtiledb on Linux will no longer re-export symbols from statically linked dependencies [#1461](https://github.com/TileDB-Inc/TileDB/pull/1461)

## Deprecations

## Bug fixes

## API additions

## API removals

# TileDB v1.7.2 Release Notes

TileDB 1.7.2 contains bug fixes and several internal optimizations.

## New features

## Improvements

* Added support for getting/setting array metadata via REST. [#1449](https://github.com/TileDB-Inc/TileDB/pull/1449)

## Deprecations

## Bug fixes

* Fixed several REST query and deserialization bugs. [#1433](https://github.com/TileDB-Inc/TileDB/pull/1433), [#1437](https://github.com/TileDB-Inc/TileDB/pull/1437), [#1440](https://github.com/TileDB-Inc/TileDB/pull/1440), [#1444](https://github.com/TileDB-Inc/TileDB/pull/1444)
* Fixed bug in setting certificate path on Linux for the REST client. [#1452](https://github.com/TileDB-Inc/TileDB/pull/1452)

## API additions

## API removals

# TileDB v1.7.1 Release Notes

TileDB 1.7.1 contains build system and bug fixes, and one non-breaking API update.

## New features

## Improvements

## Deprecations

## Bug fixes

* Fixed bug in dense consolidation when the array domain is not divisible by the tile extents. [#1442](https://github.com/TileDB-Inc/TileDB/pull/1442)

## API additions

* Added C API function `tiledb_array_has_metadata_key` and C++ API function `Array::has_metadata_key` [#1439](https://github.com/TileDB-Inc/TileDB/pull/1439)
* Added support for indicating zero-value metadata by returning `value_num` == 1 from the `_get_metadatata` and `Array::get_metadata` APIs [#1438](https://github.com/TileDB-Inc/TileDB/pull/1438) (this is a non-breaking change, as the documented return of `value == nullptr` to indicate missing keys does not change)`

## API removals

# TileDB v1.7.0 Release Notes

TileDB 1.7.0 contains the new feature of array metadata, and numerous bugfixes.

## New features

* Added array metadata. [#1377](https://github.com/TileDB-Inc/TileDB/pull/1377)

## Improvements

* Allow writes to older-versioned arrays. [#1417](https://github.com/TileDB-Inc/TileDB/pull/1417)
* Added overseen optimization to check the fragment non-empty domain before loading the fragment R-Tree. [#1395](https://github.com/TileDB-Inc/TileDB/pull/1395)
* Use `major.minor` for SOVERSION instead of full `major.minor.rev`. [#1398](https://github.com/TileDB-Inc/TileDB/pull/1398)

## Bug fixes

* Numerous query serialization bugfixes and performance improvements.
* Numerous tweaks to build strategy for libcurl dependency.
* Fix crash in StorageManager destructor when GlobalState init fails. [#1393](https://github.com/TileDB-Inc/TileDB/pull/1393)
* Fix Windows destructor crash due to missing unlock (mutex/refcount). [#1400](https://github.com/TileDB-Inc/TileDB/pull/1400)
* Normalize attribute names in multi-range size estimation C API. [#1408](https://github.com/TileDB-Inc/TileDB/pull/1408)

## API additions

* Added C API functions `tiledb_query_get_{fragment_num,fragment_uri,fragment_timestamp_range}`. [#1396](https://github.com/TileDB-Inc/TileDB/pull/1396)
* Added C++ API functions `Query::{fragment_num,fragment_uri,fragment_timestamp_range}`. [#1396](https://github.com/TileDB-Inc/TileDB/pull/1396)
* Added C API function `tiledb_ctx_set_tag` and C++ API `Context::set_tag()`. [#1406](https://github.com/TileDB-Inc/TileDB/pull/1406)
* Add config support for S3 ca_path, ca_file, and verify_ssl options. [#1376](https://github.com/TileDB-Inc/TileDB/pull/1376)

## API removals

* Removed key-value functionality, `tiledb_kv_*` functions from the C API and Map and MapSchema from the C++ API. [#1415](https://github.com/TileDB-Inc/TileDB/pull/1415)

# TileDB v1.6.3 Release Notes

## Additions

* Added config param `vfs.s3.logging_level`. [#1236](https://github.com/TileDB-Inc/TileDB/pull/1236)

## Bug fixes

* Fixed FP slice point-query with small (eps) gap coordinates. [#1384](https://github.com/TileDB-Inc/TileDB/pull/1384)
* Fixed several unused variable warnings in unit tests. [#1385](https://github.com/TileDB-Inc/TileDB/pull/1385)
* Fixed missing include in `subarray.h`. [#1374](https://github.com/TileDB-Inc/TileDB/pull/1374)
* Fixed missing virtual destructor in C++ API `schema.h`. [#1391](https://github.com/TileDB-Inc/TileDB/pull/1391)
* Fixed C++ API build error with clang regarding deleted default constructors. [#1394](https://github.com/TileDB-Inc/TileDB/pull/1394)

# TileDB v1.6.2 Release Notes

## Bug fixes

* Fix incorrect version number listed in `tiledb_version.h` header file and doc page.
* Fix issue with release notes from 1.6.0 release. [#1359](https://github.com/TileDB-Inc/TileDB/pull/1359)

# TileDB v1.6.1 Release Notes

## Bug fixes

* Bug fix in incomplete query behavior. [#1358](https://github.com/TileDB-Inc/TileDB/pull/1358)

# TileDB v1.6.0 Release Notes

The 1.6.0 release adds the major new feature of non-continuous range slicing, as well as a number of stability and usability improvements. Support is also introduced for datetime dimensions and attributes.

## New features

* Added support for multi-range reads (non-continuous range slicing) for dense and sparse arrays.
* Added support for datetime domains and attributes.

## Improvements

* Removed fragment metadata caching. [#1197](https://github.com/TileDB-Inc/TileDB/pull/1197)
* Removed array schema caching. [#1197](https://github.com/TileDB-Inc/TileDB/pull/1197)
* The tile MBR in the in-memory fragment metadata are organized into an R-Tree, speeding up tile overlap operations during subarray reads. [#1197](https://github.com/TileDB-Inc/TileDB/pull/1197)
* Improved encryption key validation process when opening already open arrays. Fixes issue with indefinite growing of the URI to encryption key mapping in `StorageManager` (the mapping is no longer needed).  [#1197](https://github.com/TileDB-Inc/TileDB/pull/1197)
* Improved dense write performance in some benchmarks. [#1229](https://github.com/TileDB-Inc/TileDB/pull/1229)
* Support for direct writes without using the S3 multi-part API. Allows writing to
  Google Cloud Storage S3 compatibility mode. [#1219](https://github.com/TileDB-Inc/TileDB/pull/1219)
* Removed 256-character length limit from URIs. [#1288](https://github.com/TileDB-Inc/TileDB/pull/1288)
* Dense reads and writes now always require a subarray to be set, to avoid confusion. [#1320](https://github.com/TileDB-Inc/TileDB/pull/1320)
* Added query and array schema serialization API. [#1262](https://github.com/TileDB-Inc/TileDB/pull/1262)

## Deprecations

* The TileDB KV API has been deprecated and will be [removed entirely](https://github.com/TileDB-Inc/TileDB/issues/1258) in a future release. The KV mechanism will be removed when full support for string-valued dimensions has been added.

## Bug fixes

* Bug fix with amplification factor in consolidation. [#1275](https://github.com/TileDB-Inc/TileDB/pull/1275)
* Fixed thread safety issue in opening arrays. [#1252](https://github.com/TileDB-Inc/TileDB/pull/1252)
* Fixed floating point exception when writing fixed-length attributes with a large cell value number. [#1289](https://github.com/TileDB-Inc/TileDB/pull/1289)
* Fixed off-by-one limitation with floating point dimension tile extents. [#1314](https://github.com/TileDB-Inc/TileDB/pull/1314)

## API additions

### C API

* Added functions `tiledb_query_{get_est_result_size, get_est_result_size_var, add_range, get_range_num, get_range}`.
* Added function `tiledb_query_get_layout`
* Added datatype `tiledb_buffer_t` and functions `tiledb_buffer_{alloc,free,get_type,set_type,get_data,set_data}`.
* Added datatype `tiledb_buffer_list_t` and functions `tiledb_buffer_list_{alloc,free,get_num_buffers,get_total_size,get_buffer,flatten}`.
* Added string conversion functions `tiledb_*_to_str()` and `tiledb_*_from_str()` for all public enum types.
* Added config param `vfs.file.enable_filelocks`
* Added datatypes `TILEDB_DATETIME_*`

### C++ API

* Added functions `Query::{query_layout, add_range, range, range_num}`.

## Breaking changes

### C API

* Removed ability to set `null` tile extents on dimensions. All dimensions must now have an explicit tile extent.

### C++ API

* Removed cast operators of C++ API objects to their underlying C API objects. This helps prevent inadvertent memory issues such as double-frees.
* Removed ability to set `null` tile extents on dimensions. All dimensions must now have an explicit tile extent.
* Changed argument `config` in `Array::consolidate()` from a const-ref to a pointer.
* Removed default includes of `Map` and `MapSchema`. To use the deprecated KV API temporarily, include `<tiledb/map.h>` explicitly.

# TileDB v1.5.1 Release Notes

## Improvements

* Better handling of `{C,CXX}FLAGS` during the build. [#1209](https://github.com/TileDB-Inc/TileDB/pull/1209)
* Update libcurl dependency to v7.64.1 for S3 builds. [#1240](https://github.com/TileDB-Inc/TileDB/pull/1240)

## Bug fixes

* S3 SDK build error fix. [#1201](https://github.com/TileDB-Inc/TileDB/pull/1201)
* Fixed thread safety issue with ZStd compressor. [#1208](https://github.com/TileDB-Inc/TileDB/pull/1208)
* Fixed crash in consolidation due to accessing invalid entry [#1213](https://github.com/TileDB-Inc/TileDB/pull/1213)
* Fixed memory leak in C++ KV API. [#1247](https://github.com/TileDB-Inc/TileDB/pull/1247)
* Fixed minor bug when writing in global order with empty buffers. [#1248](https://github.com/TileDB-Inc/TileDB/pull/1248)

# TileDB v1.5.0 Release Notes

The 1.5.0 release focuses on stability, performance, and usability improvements, as well a new consolidation algorithm.

## New features

* Added an advanced, tunable consolidation algorithm. [#1101](https://github.com/TileDB-Inc/TileDB/pull/1101)

## Improvements

* Small tiles are now batched for larger VFS read operations, improving read performance in some cases. [#1093](https://github.com/TileDB-Inc/TileDB/pull/1093)
* POSIX error messages are included in log messages. [#1076](https://github.com/TileDB-Inc/TileDB/pull/1076)
* Added `tiledb` command-line tool with several supported actions. [#1081](https://github.com/TileDB-Inc/TileDB/pull/1081)
* Added build flag to disable internal statistics. [#1111](https://github.com/TileDB-Inc/TileDB/pull/1111)
* Improved memory overhead slightly by lazily allocating memory before checking the tile cache. [#1141](https://github.com/TileDB-Inc/TileDB/pull/1141)
* Improved tile cache utilization by removing erroneous use of cache for metadata. [#1151](https://github.com/TileDB-Inc/TileDB/pull/1151)
* S3 multi-part uploads are aborted on error. [#1166](https://github.com/TileDB-Inc/TileDB/pull/1166)

## Bug fixes

* Bug fix when reading from a sparse array with real domain. Also added some checks on NaN and INF. [#1100](https://github.com/TileDB-Inc/TileDB/pull/1100)
* Fixed C++ API functions `group_by_cell` and `ungroup_var_buffer` to treat offsets in units of bytes. [#1047](https://github.com/TileDB-Inc/TileDB/pull/1047)
* Several HDFS test build errors fixed. [#1092](https://github.com/TileDB-Inc/TileDB/pull/1092)
* Fixed incorrect indexing in `parallel_for`. [#1105](https://github.com/TileDB-Inc/TileDB/pull/1105)
* Fixed incorrect filter statistics counters. [#1112](https://github.com/TileDB-Inc/TileDB/pull/1112)
* Preserve anonymous attributes in `ArraySchema` copy constructor. [#1144](https://github.com/TileDB-Inc/TileDB/pull/1144)
* Fix non-virtual destructors in C++ API. [#1153](https://github.com/TileDB-Inc/TileDB/pull/1153)
* Added zlib dependency to AWS SDK EP. [#1165](https://github.com/TileDB-Inc/TileDB/pull/1165)
* Fixed a hang in the 'S3::ls()'. [#1183](https://github.com/TileDB-Inc/TileDB/pull/1182)
* Many other small and miscellaneous bug fixes.

## API additions

### C API

* Added function `tiledb_vfs_dir_size`.
* Added function `tiledb_vfs_ls`.
* Added config params `vfs.max_batch_read_size` and `vfs.max_batch_read_amplification`.
* Added functions `tiledb_{array,kv}_encryption_type`.
* Added functions `tiledb_stats_{dump,free}_str`.
* Added function `tiledb_{array,kv}_schema_has_attribute`.
* Added function `tiledb_domain_has_dimension`.

### C++ API

* `{Array,Map}::consolidate{_with_key}` now takes a `Config` as an optional argument.
* Added function `VFS::dir_size`.
* Added function `VFS::ls`.
* Added `{Array,Map}::encryption_type()`.
* Added `{ArraySchema,MapSchema}::has_attribute()`
* Added `Domain::has_dimension()`
* Added constructor overloads for `Array` and `Map` to take a `std::string` encryption key.
* Added overloads for `{Array,Map}::{open,create,consolidate}` to take a `std::string` encryption key.
* Added untyped overloads for `Query::set_buffer()`.

## Breaking changes

### C API

* Deprecated `tiledb_compressor_t` APIs from v1.3.x have been removed, replaced by the `tiledb_filter_list` API. [#1128](https://github.com/TileDB-Inc/TileDB/pull/1128)
* `tiledb_{array,kv}_consolidate{_with_key}` now takes a `tiledb_config_t*` as argument.

### C++ API

* Deprecated `tiledb::Compressor` APIs from v1.3.x have been removed, replaced by the `FilterList` API. [#1128](https://github.com/TileDB-Inc/TileDB/pull/1128)

# TileDB v1.4.2 Release Notes

## Bug fixes

* Fixed support for config parameter values `sm.num_reader_threads` and `sm.num_writer_threads. User-specified values had been ignored for these parameters. [#1096](https://github.com/TileDB-Inc/TileDB/pull/1096)
* Fixed GCC 7 linker errors. [#1091](https://github.com/TileDB-Inc/TileDB/pull/1091)
* Bug fix in the case of dense reads in the presence of both dense and sparse fragments. [#1079](https://github.com/TileDB-Inc/TileDB/pull/1074)
* Fixed double-delta decompression bug on reads for uncompressible chunks. [#1074](https://github.com/TileDB-Inc/TileDB/pull/1074)
* Fixed unnecessary linking of shared zlib when using TileDB superbuild. [#1125](https://github.com/TileDB-Inc/TileDB/pull/1125)

## Improvements

* Added lazy creation of S3 client instance on first request. [#1084](https://github.com/TileDB-Inc/TileDB/pull/1084)
* Added config params `vfs.s3.aws_access_key_id` and `vfs.s3.aws_secret_access_key` for configure s3 access at runtime. [#1036](https://github.com/TileDB-Inc/TileDB/pull/1036)
* Added missing check if coordinates obey the global order in global order sparse writes. [#1039](https://github.com/TileDB-Inc/TileDB/pull/1039)

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
