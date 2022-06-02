# TileDB v2.9.3 Release Notes

## Improvements
* Make filestore api get configurable buffer sizes [#3223](https://github.com/TileDB-Inc/TileDB/pull/3223)
* Special case tiledb cloud uris to use row_major writes 3232]([https://github.com/TileDB-Inc/TileDB/pull/3232)

### C++ API
* Apply TILEDB_NO_API_DEPRECATION_WARNINGS to C++ API [#3236](https://github.com/TileDB-Inc/TileDB/pull/3236)

# TileDB v2.9.2 Release Notes

## Bug fixes
* Update Zlib Download URL [#3200](https://github.com/TileDB-Inc/TileDB/pull/3200)

# TileDB v2.9.1 Release Notes

## Bug fixes

* [Backport release-2.9] Sparse global order reader: refactor merge algorithm. (#3173) by @KiterLuc in https://github.com/TileDB-Inc/TileDB/pull/3182
* [Backport release-2.9] Sparse unordered w/ dups reader: fixing overflow on int value. by @github-actions in https://github.com/TileDB-Inc/TileDB/pull/3184
* [Backport release-2.9] Sparse global order reader: Check correct incomplete reason in case of too small user buffer by @github-actions in https://github.com/TileDB-Inc/TileDB/pull/3186
* [Backport release-2.9] relocate magic_mgc_gzipped.bin for build by @github-actions in https://github.com/TileDB-Inc/TileDB/pull/3185

# TileDB v2.9.0 Release Notes

## Disk Format
* Update on-disk format because of the new available compressor for Dictionary-encoding of strings [#3042](https://github.com/TileDB-Inc/TileDB/pull/3042)

## New features
* RLE compression support for var-length string dimensions [#2938](https://github.com/TileDB-Inc/TileDB/pull/2938)
* Add virtual filesystem ls_with_sizes function [#2971](https://github.com/TileDB-Inc/TileDB/pull/2971)
* Add new CMake build option for `TILEDB_EXPERIMENTAL_FEATURES` to compile time protect experimental features. [#2748](https://github.com/TileDB-Inc/TileDB/pull/2748)
* Forwardport Group API [#3058](https://github.com/TileDB-Inc/TileDB/pull/3058)
* Support Dictionary-encoding filter for string dimensions and attributes [#3077](https://github.com/TileDB-Inc/TileDB/pull/3077)
* Use legacy sparse global order reader for 2.9 [#3096](https://github.com/TileDB-Inc/TileDB/pull/3096)
* Add libmagic to build process. [#3088](https://github.com/TileDB-Inc/TileDB/pull/3088)
* New file storage APIs (tiledb_filestore_...) [#3121](https://github.com/TileDB-Inc/TileDB/pull/3121)

## Improvements
* Convert FilterPipeline deserialize function to static factory function [#2799](https://github.com/TileDB-Inc/TileDB/pull/2799)
* Convert array metadata deserialize function to factory function [#2784](https://github.com/TileDB-Inc/TileDB/pull/2784)
* A new thread pool with modern C++ compatible API and exception-safe behavior. [#2944](https://github.com/TileDB-Inc/TileDB/pull/2944)
* Global order writer: fixing multi writes for var size attributes. [#2963](https://github.com/TileDB-Inc/TileDB/pull/2963)
* Smart pointer conversion: ArraySchema Domain [#2948](https://github.com/TileDB-Inc/TileDB/pull/2948)
* VFS: adding configuration for vfs.max_batch_size. [#2960](https://github.com/TileDB-Inc/TileDB/pull/2960)
* Declare all C API functions `noexcept`. Put existing C API functions inside exception safety wrappers to meet the declaration change. [#2961](https://github.com/TileDB-Inc/TileDB/pull/2961)
* Add support for compile-time assertion configuration [#2962](https://github.com/TileDB-Inc/TileDB/pull/2962)
* Refactored tiledb::sm::serialization::attribute_from_capnp to be C41 compliant [#2937](https://github.com/TileDB-Inc/TileDB/pull/2937)
* Support reading all consolidated fragment metadata files. [#2973](https://github.com/TileDB-Inc/TileDB/pull/2973)
* Fixing build errors using MacOSX12.3.sdk. [#2981](https://github.com/TileDB-Inc/TileDB/pull/2981)
* Fixing compute_results_count_sparse_string for multiple range threads. [#2983](https://github.com/TileDB-Inc/TileDB/pull/2983)
* Global writes: fixing OOM on write continuation. [#2993](https://github.com/TileDB-Inc/TileDB/pull/2993)
* Do not store offsets when RLE is used on string dimensions [#2969](https://github.com/TileDB-Inc/TileDB/pull/2969)
* Dynamically infer bytesizes for run length and strings for strings RLE compression [#2984](https://github.com/TileDB-Inc/TileDB/pull/2984)
* Smart pointer conversion: ArraySchema Dimension [#2926](https://github.com/TileDB-Inc/TileDB/pull/2926)
* Refactored tiledb::sm::serialization::filter_pipeline_from_capnp to be C41 compliant [#2943](https://github.com/TileDB-Inc/TileDB/pull/2943)
* Enable sparse global order reader by default. [#2997](https://github.com/TileDB-Inc/TileDB/pull/2997)
* Add API for FragmentInfo::get_fragment_name [#2977](https://github.com/TileDB-Inc/TileDB/pull/2977)
* add validity file format specification [#2998](https://github.com/TileDB-Inc/TileDB/pull/2998)
* Convert Domain class deserialize function to factory function [#2800](https://github.com/TileDB-Inc/TileDB/pull/2800)
* Dense reader: fix user buffer offset computation for multi-index queries. [#3002](https://github.com/TileDB-Inc/TileDB/pull/3002)
* Sparse readers: using zipped coords buffers for fragment version < 5. [#3016](https://github.com/TileDB-Inc/TileDB/pull/3016)
* Extra UTs on string RLEs [#3024](https://github.com/TileDB-Inc/TileDB/pull/3024)
* Bump Catch2 version to 2.13.8 [#3027](https://github.com/TileDB-Inc/TileDB/pull/3027)
* Split consolidator in multiple classes. [#3004](https://github.com/TileDB-Inc/TileDB/pull/3004)
* HTML-render the existing format-spec Markdown docs. [#3043](https://github.com/TileDB-Inc/TileDB/pull/3043)
* Add more detailed doc for schema evolution timestamp range functions. [#3029](https://github.com/TileDB-Inc/TileDB/pull/3029)
* Run doc-render job on doc-only PRs, and not on non-doc PRs [#3045](https://github.com/TileDB-Inc/TileDB/pull/3045)
* Support curl POSTing >2GB data to REST [#3048](https://github.com/TileDB-Inc/TileDB/pull/3048)
* Dense reader: do not sort input ranges. [#3036](https://github.com/TileDB-Inc/TileDB/pull/3036)
* Support consolidating non-contiguous fragments. [#3037](https://github.com/TileDB-Inc/TileDB/pull/3037)
* Introduce dictionary-encoding as an enum option for filters [#3042](https://github.com/TileDB-Inc/TileDB/pull/3042)
* Move `Range` to new `tiledb::type` namespace [#3059](https://github.com/TileDB-Inc/TileDB/pull/3059)
* Convert tdb shared to shared [#2965](https://github.com/TileDB-Inc/TileDB/pull/2965)
* Add `StatusException`, an exception class to be thrown instead of returning `Status` [#3050](https://github.com/TileDB-Inc/TileDB/pull/3050)
* Global writer: fixing write continuation for fixed sized attributes. [#3062](https://github.com/TileDB-Inc/TileDB/pull/3062)
* Cherry-pick #3061 [#3064](https://github.com/TileDB-Inc/TileDB/pull/3064)
* Add ability to store (optional) name with group member [#3068](https://github.com/TileDB-Inc/TileDB/pull/3068)
* Typo fix in group.cc [#3078](https://github.com/TileDB-Inc/TileDB/pull/3078)
* All ranges tile overlap: skip computation for default dimensions. [#3080](https://github.com/TileDB-Inc/TileDB/pull/3080)
* Add golang annotation to capnp spec file [#3089](https://github.com/TileDB-Inc/TileDB/pull/3089)
* Rename tiledb time.h/math.h to avoid possible conflicts with standard header files. [#3087](https://github.com/TileDB-Inc/TileDB/pull/3087)
* Convert ArraySchema's deserialize to a factory function [#3012](https://github.com/TileDB-Inc/TileDB/pull/3012)
* varying_size_datum_at: fixing comparison error. [#3127](https://github.com/TileDB-Inc/TileDB/pull/3127)
* Global writes: check global order on write continuation. [#3109](https://github.com/TileDB-Inc/TileDB/pull/3109)

## Deprecations

## Bug fixes
* [bug] Fix SC-17415: segfault due to underflow in for loop [#3143](https://github.com/TileDB-Inc/TileDB/pull/3143)
* Unordered writer: fixing segfault for empty writes. [#3161](https://github.com/TileDB-Inc/TileDB/pull/3161)
* Sparse global order reader: prevent dims from being unfiltered twice. [#3150](https://github.com/TileDB-Inc/TileDB/pull/3150)
* Filter pipeline: fixing empty pipeline, multi chunk, refactored queries. [#3149](https://github.com/TileDB-Inc/TileDB/pull/3149)
* compare nullptr, avoid catch2 comparison warning failure [#2970](https://github.com/TileDB-Inc/TileDB/pull/2970)
* Check that array is open before getting non_empty_domain [#2980](https://github.com/TileDB-Inc/TileDB/pull/2980)
* Fix assertion failure in GCS, debug build [#3001](https://github.com/TileDB-Inc/TileDB/pull/3001)
* Fix missing stats on cloud queries. [#3009](https://github.com/TileDB-Inc/TileDB/pull/3009)
* Sparse unordered w/ dups reader: coord tiles management fix. [#3023](https://github.com/TileDB-Inc/TileDB/pull/3023)
* Incorrect validity result count in REST query [#3015](https://github.com/TileDB-Inc/TileDB/pull/3015)
* use different API approach to avoid possible file sharing violation [#3056](https://github.com/TileDB-Inc/TileDB/pull/3056)
* `tiledb_serialize_array_metadata` should load metadata if its not loaded before serializing [#3065](https://github.com/TileDB-Inc/TileDB/pull/3065)
* `tiledb_serialize_group_metadata` should load group metadata if its not loaded. [#3070](https://github.com/TileDB-Inc/TileDB/pull/3070)
* avoid some potentially invalid vector references [#2932](https://github.com/TileDB-Inc/TileDB/pull/2932)
* Sparse Index Reader Fix: Check For Empty Buffer [#3051](https://github.com/TileDB-Inc/TileDB/pull/3051)
* Sparse Global Order Reader Fix: Decrement Total Cells [#3046](https://github.com/TileDB-Inc/TileDB/pull/3046)
* Reset group metadata only based on end timestamp to ensure its always reset to now [#3091](https://github.com/TileDB-Inc/TileDB/pull/3091)
* Sparse unordered w/ dups reader: fix incomplete reason for cloud reads. [#3104](https://github.com/TileDB-Inc/TileDB/pull/3104)

## API additions

### C++ API
* Add function to check if Config contains a parameter [#3082](https://github.com/TileDB-Inc/TileDB/pull/3082)

# TileDB v2.8.2 Release Notes

## Bug fixes

* Sparse unordered w/ dups reader: fix incomplete reason for cloud reads. [#3104](https://github.com/TileDB-Inc/TileDB/pull/3104)

# TileDB v2.8.1 Release Notes

## Improvements

* Add golang annotation to capnp spec file [#3089](https://github.com/TileDB-Inc/TileDB/pull/3089)
* Update group metadata REST request to standardize cap'n proto class usage [#3095](https://github.com/TileDB-Inc/TileDB/pull/3095)

## Bug fixes

* Sparse Index Reader Fix: Check For Empty Buffer [#3051](https://github.com/TileDB-Inc/TileDB/pull/3051)
* Reset group metadata only based on end timestamp to ensure its always reset to now [#3091](https://github.com/TileDB-Inc/TileDB/pull/3091)

# TileDB v2.8.0 Release Notes

## Disk Format

* Add Metadata to groups [#2966](https://github.com/TileDB-Inc/TileDB/pull/2966)
* Add Group on disk structure for members [#2966](https://github.com/TileDB-Inc/TileDB/pull/2966)


## New features

* Support gs:// as an alias for gcs:// [#2864](https://github.com/TileDB-Inc/TileDB/pull/2864)
* Eliminate LOG_FATAL use from codebase [#2897](https://github.com/TileDB-Inc/TileDB/pull/2897)
* Collect missing docs [#2922](https://github.com/TileDB-Inc/TileDB/pull/2922)
* Support `tiledb://` objects in the Object API [#2954](https://github.com/TileDB-Inc/TileDB/pull/2954)
* RLE compression support for var-length string dimensions [#2938](https://github.com/TileDB-Inc/TileDB/pull/2938)
* Add Metadata to groups [#2966](https://github.com/TileDB-Inc/TileDB/pull/2966)
* Add robust API to groups for adding and removing members of a group [#2966](https://github.com/TileDB-Inc/TileDB/pull/2966)

## Improvements

* Support top-level cap'n proto array object [#2844](https://github.com/TileDB-Inc/TileDB/pull/2844)
* Nicer error message for tiledb fragment listing [#2872](https://github.com/TileDB-Inc/TileDB/pull/2872)
* Removing Buffer from Tile. [#2852](https://github.com/TileDB-Inc/TileDB/pull/2852)
* Splitting Writer class into 3 separate classes. [#2884](https://github.com/TileDB-Inc/TileDB/pull/2884)
* Adding a compressor algorithm for RLE encoding of strings [#2857](https://github.com/TileDB-Inc/TileDB/pull/2857)
* Reader: treating empty string range as expected. [#2883](https://github.com/TileDB-Inc/TileDB/pull/2883)
* Add a compression algorithm for dictionary encoding of strings [#2880](https://github.com/TileDB-Inc/TileDB/pull/2880)
* Adds an ArrayDirectory class to manage all URIs within the array directory. [#2909](https://github.com/TileDB-Inc/TileDB/pull/2909)
* Remove accidental addition of writer.cc. [#2917](https://github.com/TileDB-Inc/TileDB/pull/2917)
* Tile metadata generator: code cleanup. [#2919](https://github.com/TileDB-Inc/TileDB/pull/2919)
* Listing improvements: new directory structure for array. [#2918](https://github.com/TileDB-Inc/TileDB/pull/2918)
* ArraySchema's Attribute smart pointer conversion [#2887](https://github.com/TileDB-Inc/TileDB/pull/2887)
* Add option for tile level filtering [#2906](https://github.com/TileDB-Inc/TileDB/pull/2906)
* Switch to smart pointers and const references for `ArraySchema`, and avoid fetching the latest array schema twice. [#2923](https://github.com/TileDB-Inc/TileDB/pull/2923)
* Move vfs_helpers.cc and helper.cc into separate library with target that can be referenced elsewhere. [#2929](https://github.com/TileDB-Inc/TileDB/pull/2929)
* Avoid calling `generate_uri` in `ArraySchema` accessors [#2928](https://github.com/TileDB-Inc/TileDB/pull/2928)
* Global order writer: initialize last_var_offsets_. [#2930](https://github.com/TileDB-Inc/TileDB/pull/2930)
* Wrap some C API functions with exception handlers. [#2650](https://github.com/TileDB-Inc/TileDB/pull/2650)
* Fragment metadata: add min/max/sun/null count. [#2934](https://github.com/TileDB-Inc/TileDB/pull/2934)
* Filter pipeline: incorrect stopping point during chunk parallellization. [#2955](https://github.com/TileDB-Inc/TileDB/pull/2955)
* Adding support to consolidate ok/wrt files. [#2933](https://github.com/TileDB-Inc/TileDB/pull/2933)
* Tile medatada: treating TILEDB_CHAR as TILEDB_STRING_ASCII. [#2953](https://github.com/TileDB-Inc/TileDB/pull/2953)
* Fragment metadata: treating TILEDB_CHAR as TILEDB_STRING_ASCII. [#2958](https://github.com/TileDB-Inc/TileDB/pull/2958)
* Global order writer: fixing multi writes for var size attributes. [#2963](https://github.com/TileDB-Inc/TileDB/pull/2963)
* Convert FilterPipeline deserialize function to static factory function [#2799](https://github.com/TileDB-Inc/TileDB/pull/2799)
* VFS: adding configuration for vfs.max_batch_size. [#2960](https://github.com/TileDB-Inc/TileDB/pull/2960)
* Fixing build errors using MacOSX12.3.sdk. [#2981](https://github.com/TileDB-Inc/TileDB/pull/2981)
* Support reading all consolidated fragment metadata files. [#2973](https://github.com/TileDB-Inc/TileDB/pull/2973)
* Fixing compute_results_count_sparse_string for multiple range threads. [#2983](https://github.com/TileDB-Inc/TileDB/pull/2983)
* Global writes: fixing OOM on write continuation. [#2993](https://github.com/TileDB-Inc/TileDB/pull/2993)
* Do not store offsets when RLE is used on string dimensions [#2969](https://github.com/TileDB-Inc/TileDB/pull/2969)
* Dynamically infer bytesizes for run length and strings for strings RLE compression [#2984](https://github.com/TileDB-Inc/TileDB/pull/2984)
* Add ability to store (optional) name with group member [#3068](https://github.com/TileDB-Inc/TileDB/pull/3068)

## Bug fixes

* Avoid thread starvation by removing std::future usage in S3 multipart upload [#2851](https://github.com/TileDB-Inc/TileDB/pull/2851)
* windows_sanity fix [#2870](https://github.com/TileDB-Inc/TileDB/pull/2870)
* Adds missing pthreads link to dynamic memory unit test [#2888](https://github.com/TileDB-Inc/TileDB/pull/2888)
* Remove `common.h` from `arrow_io_impl.h` [#2915](https://github.com/TileDB-Inc/TileDB/pull/2915)
* Range::set_start and set_end should throw instead of empty returning [#2913](https://github.com/TileDB-Inc/TileDB/pull/2913)
* Global writer: fixing write continuation for fixed sized attributes. [#3062](https://github.com/TileDB-Inc/TileDB/pull/3062)
* `tiledb_serialize_array_metadata` should load metadata if its not loaded before serializing [#3065](https://github.com/TileDB-Inc/TileDB/pull/3065)
* `tiledb_serialize_group_metadata` should load group metadata if its not loaded. [#3070](https://github.com/TileDB-Inc/TileDB/pull/3070)

## API additions

### C API

* Introduce experimental `tiledb_ctx_alloc_with_error` to return error when context alloc fails [#2905](https://github.com/TileDB-Inc/TileDB/pull/2905)
* Add `tiledb_group_*` APIs for robust group support [#2966](https://github.com/TileDB-Inc/TileDB/pull/2966)

### C++ API

* Add missing cstddef include to fix compile w/ GCC 7 [#2885](https://github.com/TileDB-Inc/TileDB/pull/2885)
* Add `Group::*` APIs for robust group support [#2966](https://github.com/TileDB-Inc/TileDB/pull/2966)

# TileDB v2.7.1 Release Notes

## Bug fixes

* Sparse unordered w/ dups reader: fixing memory management for tiles. [#2924](https://github.com/TileDB-Inc/TileDB/pull/2924)

# TileDB v2.7.0 Release Notes

## Disk Format

* Removed file `__lock.tdb` from the array folder and updated the format spec. Also removed config `vfs.file.enable_filelocks`. [#2692](https://github.com/TileDB-Inc/TileDB/pull/2692)

## New features

* Publish Subarray access/functionality for use outside of core. [#2214](https://github.com/TileDB-Inc/TileDB/pull/2214)
* Add TILEDB_BLOB datatype [#2721](https://github.com/TileDB-Inc/TileDB/pull/2721)
* Expose array schema libtiledb version information [#2863](https://github.com/TileDB-Inc/TileDB/pull/2863)

## Improvements

* Convert loose files `thread_pool.*` into a unit [#2520](https://github.com/TileDB-Inc/TileDB/pull/2520)
* Bump cmake_minimum_required to 3.21 [#2532](https://github.com/TileDB-Inc/TileDB/pull/2532)
* dynamic_memory unit, including allocator [#2542](https://github.com/TileDB-Inc/TileDB/pull/2542)
* implement windows CI functionality with github actions [#2498](https://github.com/TileDB-Inc/TileDB/pull/2498)
* implement windows crash dump file processing for windows [#2657](https://github.com/TileDB-Inc/TileDB/pull/2657)
* Added object libraries: baseline, buffer, thread_pool [#2629](https://github.com/TileDB-Inc/TileDB/pull/2629)
* ubuntu core dump processing (GA) CI [#2642](https://github.com/TileDB-Inc/TileDB/pull/2642)
* Avoid copy and set of config in `tiledb_query_add_range` as a performance optimization from the new subarray APIs [#2740](https://github.com/TileDB-Inc/TileDB/pull/2740)
* core file stack backtracing and artifact uploading for mac CI builds [#2632](https://github.com/TileDB-Inc/TileDB/pull/2632)
* Move `Status::*Error` to `Status_*Error` [#2610](https://github.com/TileDB-Inc/TileDB/pull/2610)
* ZStd Compressor: allocate one context per thread and re-use. [#2701](https://github.com/TileDB-Inc/TileDB/pull/2701)
* Include offset index in oversize error message [#2757](https://github.com/TileDB-Inc/TileDB/pull/2757)
* Print resource pool contained type on error [#2757](https://github.com/TileDB-Inc/TileDB/pull/2757)
* Using RETURN_NOT_OK_TUPLE in attribute.cc. [#2787](https://github.com/TileDB-Inc/TileDB/pull/2787)
* Add timestamp range to schema evolution to avoid race conditions based on schema timestamp [#2776](https://github.com/TileDB-Inc/TileDB/pull/2776)
* Convert dimension deserialize to factory [#2763](https://github.com/TileDB-Inc/TileDB/pull/2763)
* Improve object type detection performance [#2792](https://github.com/TileDB-Inc/TileDB/pull/2792)
* Query condition: differentiate between nullptr and empty string. [#2802](https://github.com/TileDB-Inc/TileDB/pull/2802)
* patch git+git: to git+https: to avoid GH access failure [#2805](https://github.com/TileDB-Inc/TileDB/pull/2805)
* Remove examples writing sparse fragments to dense arrays. [#2804](https://github.com/TileDB-Inc/TileDB/pull/2804)
* Change the ZStd filter compression level range and add defined compression level default values [#2623](https://github.com/TileDB-Inc/TileDB/pull/2623)
* Change the ZStd filter compression level range and add defined compression level default values [#2811](https://github.com/TileDB-Inc/TileDB/pull/2811)
* changes to augment dbg output on build failures [#2801](https://github.com/TileDB-Inc/TileDB/pull/2801)
* Sparse unordered w/ dups reader: process queries until full user buffers [#2816](https://github.com/TileDB-Inc/TileDB/pull/2816)
* Support var-length CHAR QueryConditions [#2814](https://github.com/TileDB-Inc/TileDB/pull/2814)
* Sparse global order reader: merge smaller cell slabs. [#2818](https://github.com/TileDB-Inc/TileDB/pull/2818)
* Fixing build errors in query condition. [#2820](https://github.com/TileDB-Inc/TileDB/pull/2820)
* Enabling refactored dense reader by default. [#2808](https://github.com/TileDB-Inc/TileDB/pull/2808)
* Switching TUPLE macros to be variadic. [#2822](https://github.com/TileDB-Inc/TileDB/pull/2822)
* Read tiles: no buffer pre-allocation for no-opt filter. [#2819](https://github.com/TileDB-Inc/TileDB/pull/2819)
* Refactored dense reader: resetting the unsplittable flag on completion. [#2832](https://github.com/TileDB-Inc/TileDB/pull/2832)
* Refactored dense reader: code cleanup. [#2833](https://github.com/TileDB-Inc/TileDB/pull/2833)
* Fixing Subarray::crop_to_tile to crop default dimensions. [#2831](https://github.com/TileDB-Inc/TileDB/pull/2831)
* Dense reader: fixing src cell offset for different cell order. [#2835](https://github.com/TileDB-Inc/TileDB/pull/2835)
* Support storing integral cells in a chunk. [#2821](https://github.com/TileDB-Inc/TileDB/pull/2821)
* Improve error handling for Array non empty domain calls. [#2845](https://github.com/TileDB-Inc/TileDB/pull/2845)
* Remove OpenArray and refactor StorageManager and FragmentInfo [#2839](https://github.com/TileDB-Inc/TileDB/pull/2839)
* Adding min/max/sum/null count to fragment info. [#2830](https://github.com/TileDB-Inc/TileDB/pull/2830)
* Fine-tune unfiltering parallelization [#2842](https://github.com/TileDB-Inc/TileDB/pull/2842)
* Removing ch9473 comments in unit-cppapi-string-dims.cc. [#2837](https://github.com/TileDB-Inc/TileDB/pull/2837)
* Global writes: writting bad validity values when coordinates are dups. [#2848](https://github.com/TileDB-Inc/TileDB/pull/2848)
* Fixing issues in VS2019. [#2853](https://github.com/TileDB-Inc/TileDB/pull/2853)
* Writer: processing var tiles before offset tiles. [#2854](https://github.com/TileDB-Inc/TileDB/pull/2854)
* Writer: moving unordered_map::emplace outside of parallel_for. [#2860](https://github.com/TileDB-Inc/TileDB/pull/2860)
* Dense reader: removing unnecessary loop around read_attributes. [#2859](https://github.com/TileDB-Inc/TileDB/pull/2859)
* Convert deserialize function of Attribute to factory function [#2566](https://github.com/TileDB-Inc/TileDB/pull/2566)
* Convert Filter class deserialize and create to factory functions [#2655](https://github.com/TileDB-Inc/TileDB/pull/2655)
* Tile metadata test: reducing amount of spew in verbose mode. [#2882](https://github.com/TileDB-Inc/TileDB/pull/2882)
* compute_results_count_sparse_string: fixing incorect memcmp. [#2892](https://github.com/TileDB-Inc/TileDB/pull/2892)
* Sparse rindex readers: fixing query resume on TileDB cloud. [#2900](https://github.com/TileDB-Inc/TileDB/pull/2900)

## Deprecations

* Deprecate `TILEDB_CHAR` in favor of users using `TILEDB_BLOB` or `TILEDB_STRING_ASCII` for attribute datatypes. [#2742](https://github.com/TileDB-Inc/TileDB/pull/2742) [#2797](https://github.com/TileDB-Inc/TileDB/pull/2797)
* Deprecate `TILEDB_ANY` datatype [#2807](https://github.com/TileDB-Inc/TileDB/pull/2807)
* Deprecate `TILEDB_STRING_USC2` and `TILEDB_STRING_USC4` datatypes. [#2812](https://github.com/TileDB-Inc/TileDB/pull/2812)
* Drop support for Visual Studio 2017 compiler [#2847](https://github.com/TileDB-Inc/TileDB/pull/2847)

## Bug fixes

* Better windows relative path support with '/' [#2607](https://github.com/TileDB-Inc/TileDB/pull/2607)
* Fixed dangling links in README. [#2712](https://github.com/TileDB-Inc/TileDB/pull/2712)
* Handle multiple core files for stack traces and archiving [#2723](https://github.com/TileDB-Inc/TileDB/pull/2723)
* restore lost cmake code necessary for clean build on windows with -EnableAzure [#2656](https://github.com/TileDB-Inc/TileDB/pull/2656)
* Return error for nonempty_domain access regardless of local/remote status [#2766](https://github.com/TileDB-Inc/TileDB/pull/2766)
* Fix segfault in new sparse null `QueryCondition` code [#2794](https://github.com/TileDB-Inc/TileDB/pull/2794)
* ReaderBase needs to load var sizes [#2809](https://github.com/TileDB-Inc/TileDB/pull/2809)
* Only initialize REST query strategies once [#2836](https://github.com/TileDB-Inc/TileDB/pull/2836)
* Logger json format output is not valid [#2850](https://github.com/TileDB-Inc/TileDB/pull/2850)
* Closing a non-opened array should be a no-op instead of error. [#2889](https://github.com/TileDB-Inc/TileDB/pull/2889)
* patch (unsupported) 3rd party azure cpp lite sdk used by TileDB to avoid memory faults [#2881](https://github.com/TileDB-Inc/TileDB/pull/2881)
* Free allocated latest array schema when on error of loading all array schemas [#2907](https://github.com/TileDB-Inc/TileDB/pull/2907)

## API additions

### C API

* Add `tiledb_array_schema_evolution_set_timestamp_range` to avoid race conditions based on schema timestamp [#2776](https://github.com/TileDB-Inc/TileDB/pull/2776)
* Deprecate `TILEDB_CHAR` in favor of users using `TILEDB_BLOB` or `TILEDB_STRING_ASCII` for attribute datatypes. [#2742](https://github.com/TileDB-Inc/TileDB/pull/2742) [#2797](https://github.com/TileDB-Inc/TileDB/pull/2797)
* Add {set,get}_validity_filter_list [#2798](https://github.com/TileDB-Inc/TileDB/pull/2798)
* Deprecate `TILEDB_ANY` datatype [#2807](https://github.com/TileDB-Inc/TileDB/pull/2807)
* Deprecate `TILEDB_STRING_USC2` and `TILEDB_STRING_USC4` datatypes. [#2812](https://github.com/TileDB-Inc/TileDB/pull/2812)
* Add `tiledb_array_schema_get_version` for fetching array schema version [#2863](https://github.com/TileDB-Inc/TileDB/pull/2863)
* Introduce experimental `tiledb_ctx_alloc_with_error` to return error when context alloc fails [#2905](https://github.com/TileDB-Inc/TileDB/pull/2905)

### C++ API

* Add `ArraySchemaEvolution::set_timestamp_range` to avoid race conditions based on schema timestamp [#2776](https://github.com/TileDB-Inc/TileDB/pull/2776)
* Deprecate `TILEDB_CHAR` in favor of users using `TILEDB_BLOB` or `TILEDB_STRING_ASCII` for attribute datatypes. [#2742](https://github.com/TileDB-Inc/TileDB/pull/2742) [#2797](https://github.com/TileDB-Inc/TileDB/pull/2797)
* Add validity_filter_list set/get and missing get tests [#2798](https://github.com/TileDB-Inc/TileDB/pull/2798)
* Deprecate `TILEDB_ANY` datatype [#2807](https://github.com/TileDB-Inc/TileDB/pull/2807)
* Deprecate `TILEDB_STRING_USC2` and `TILEDB_STRING_USC4` datatypes. [#2812](https://github.com/TileDB-Inc/TileDB/pull/2812)
* Add `ArraySchema::version()` for fetching array schema version [#2863](https://github.com/TileDB-Inc/TileDB/pull/2863)
* Add missing cstddef include to fix compile w/ GCC 7 [#2885](https://github.com/TileDB-Inc/TileDB/pull/2885)

# TileDB v2.6.4 Release Notes

## Bug fixes

* Sparse unordered w/ dups reader: all empty string attribute fix. [#2874](https://github.com/TileDB-Inc/TileDB/pull/2874)
* Update Location of Zlib Download URL [#2945](https://github.com/TileDB-Inc/TileDB/pull/2945)

# TileDB v2.6.3 Release Notes

## Bug fixes

* compute_results_count_sparse_string: fixing incorect memcmp. [#2892](https://github.com/TileDB-Inc/TileDB/pull/2892)
* Sparse rindex readers: fixing query resume on TileDB cloud. [#2900](https://github.com/TileDB-Inc/TileDB/pull/2900)

# TileDB v2.6.2 Release Notes

## Bug fixes

* Only initialize REST query strategies once [#2836](https://github.com/TileDB-Inc/TileDB/pull/2836)
* Sparse unordered w dups reader: fixing max pos calculation in tile copy. [#2840](https://github.com/TileDB-Inc/TileDB/pull/2840)


# TileDB v2.6.1 Release Notes

## Bug fixes

* Sparse unordered w/ dups reader: off by one error in query continuation. [#2815](https://github.com/TileDB-Inc/TileDB/pull/2815)
* Sparse unordered w dups reader: fixing query continuation with subarray. [#2824](https://github.com/TileDB-Inc/TileDB/pull/2824)

## API additions

### C++ API

* tiledb::Array destructor no longer calls ::close for non-owned C ptr [#2823](https://github.com/TileDB-Inc/TileDB/pull/2823)


# TileDB v2.6.0 Release Notes

## Improvements

* Sparse unordered with dups reader: removing result cell slabs. [#2606](https://github.com/TileDB-Inc/TileDB/pull/2606)
* Use as-installed path for TileDBConfig CMake static library imports [#2669](https://github.com/TileDB-Inc/TileDB/pull/2669)
* Check error message variable for nullptr before further use [#2634](https://github.com/TileDB-Inc/TileDB/pull/2634)
* Fixing str_coord_intersects to use std::basic_string_view. [#2654](https://github.com/TileDB-Inc/TileDB/pull/2654)
* Sparse unordered with duplicates reader: cell num fix. [#2636](https://github.com/TileDB-Inc/TileDB/pull/2636)
* Sparse unordered with dups reader: fixing initial bound calculation. [#2638](https://github.com/TileDB-Inc/TileDB/pull/2638)
* Read_tiles parallelization improvements. [#2644](https://github.com/TileDB-Inc/TileDB/pull/2644)
* Sparse global order reader: memory management unit tests. [#2645](https://github.com/TileDB-Inc/TileDB/pull/2645)
* Reduce scope of `open_array_for_reads_mtx_` locks [#2681](https://github.com/TileDB-Inc/TileDB/pull/2681)
* Sparse refactored readers: better parallelization for tile bitmaps. [#2643](https://github.com/TileDB-Inc/TileDB/pull/2643)
* ZStd compressor: allocate one context per thread and re-use. [#2691](https://github.com/TileDB-Inc/TileDB/pull/2691)
* Sparse refactored readers: disable filtered buffer tile cache. [#2651](https://github.com/TileDB-Inc/TileDB/pull/2651)
* Moving coord_string from returning a std::string to std::basic_string_view. [#2704](https://github.com/TileDB-Inc/TileDB/pull/2704)
* Sparse unordered w/ dups reader: tracking cell progress. [#2668](https://github.com/TileDB-Inc/TileDB/pull/2668)
* Sparse unordered w/ dups reader: fixing var size overflow adjustment. [#2713](https://github.com/TileDB-Inc/TileDB/pull/2713)
* Enable memfs tests that were disabled by mistake [#2648](https://github.com/TileDB-Inc/TileDB/pull/2648)
* Add helpful details to memory limit error strings. [#2729](https://github.com/TileDB-Inc/TileDB/pull/2729)
* Sparse refactored readers: Better vectorization for tile bitmaps calculations. [#2711](https://github.com/TileDB-Inc/TileDB/pull/2711)
* Sort ranges for unordered with duplicate reader and exit comparisons early [#2736](https://github.com/TileDB-Inc/TileDB/pull/2736)
* Sparse refactored readers: better vectorization for query condition. [#2737](https://github.com/TileDB-Inc/TileDB/pull/2737)
* Use correct frag index in tiles creation for compute_result_space_tiles. [#2741](https://github.com/TileDB-Inc/TileDB/pull/2741)
* Use a single uint64 for cell counts [#2749](https://github.com/TileDB-Inc/TileDB/pull/2749)
* Array Schema name should be included with cap'n proto serialization [#2696](https://github.com/TileDB-Inc/TileDB/pull/2696)
* Add and use blocking resource pool [#2735](https://github.com/TileDB-Inc/TileDB/pull/2735)
* Making the allocation part of read_tiles single threaded. [#2753](https://github.com/TileDB-Inc/TileDB/pull/2753)
* Sparse unordered w/ dups reader: remove invalid assert. [#2778](https://github.com/TileDB-Inc/TileDB/pull/2778)
* Read tiles: fixing preallocation size for var and validity buffers. [#2781](https://github.com/TileDB-Inc/TileDB/pull/2781)
* Sparse unordered w/ dups: var buffer overflow on tile continuation fix. [#2777](https://github.com/TileDB-Inc/TileDB/pull/2777)
* Determine non overlapping ranges automatically. [#2780](https://github.com/TileDB-Inc/TileDB/pull/2780)

## Deprecations

* eliminate usage of std::iterator due to c++17 deprecation [#2675](https://github.com/TileDB-Inc/TileDB/pull/2675)

## Bug fixes

* upgrade to blosc 1.21.0 from 1.14.x [#2422](https://github.com/TileDB-Inc/TileDB/pull/2422)
* Guard ZStd resource pool to fix initialization race [#2699](https://github.com/TileDB-Inc/TileDB/pull/2699)
* [C API] Add missing save_error calls in vfs_ls [#2714](https://github.com/TileDB-Inc/TileDB/pull/2714)
* Use fragment array schema for applying query condition to account for schema evolution [#2698](https://github.com/TileDB-Inc/TileDB/pull/2698)
* Don't try to read config from uninitialize storage manager [#2771](https://github.com/TileDB-Inc/TileDB/pull/2771)

## API additions

### C API

* Add bulk point-range setter tiledb_query_add_point_ranges [#2765](https://github.com/TileDB-Inc/TileDB/pull/2765)
* Add experimental query status details API [#2770](https://github.com/TileDB-Inc/TileDB/pull/2770)

### C++ API

* Backport Query::ctx and Query::array getters from 2.7 [#2754](https://github.com/TileDB-Inc/TileDB/pull/2754)


# TileDB v2.5.3 Release Notes

## Improvements

* Removing unnecessary openssl callback function. [#2705](https://github.com/TileDB-Inc/TileDB/pull/2705)
* openssl3 md5 deprecation mitigation [#2716](https://github.com/TileDB-Inc/TileDB/pull/2716)
* Sparse refactored reader: change all_tiles_loaded_ to vector of uint8_t. [#2724](https://github.com/TileDB-Inc/TileDB/pull/2724)

## Bug fixes

* Properly check and use legacy readers instead of refactored in serialized query. [#2667](https://github.com/TileDB-Inc/TileDB/pull/2667)
* Set array URI in cap'n proto object for compatibility with repeated opened array usage in TileDB 2.4 and older. [#2676](https://github.com/TileDB-Inc/TileDB/pull/2676)
* Add the compute_mbr_var_func_pointer assignment in Dimension constructor [#2730](https://github.com/TileDB-Inc/TileDB/pull/2730)

# TileDB v2.5.2 Release Notes

## Improvements

* Provide non-AVX2 build artifact on Linux [#2649](https://github.com/TileDB-Inc/TileDB/pull/2649)
* Error out when setting multiple ranges for global layout [#2658](https://github.com/TileDB-Inc/TileDB/pull/2658)

## Bug fixes

* Patch AWS sdk for cmake 3.22 support [#2639](https://github.com/TileDB-Inc/TileDB/pull/2639)
* Remove assert on memory_used_result_tile_ranges_ in SparseUnorderedWithDupsReader [#2652](https://github.com/TileDB-Inc/TileDB/pull/2652)
* Remove tiles that are empty through being filtered with a query condition [#2659](https://github.com/TileDB-Inc/TileDB/pull/2659)
* Always load array schemas during array open to find any new array schemas created from array schema evolution [#2613](https://github.com/TileDB-Inc/TileDB/pull/2613)

# TileDB v2.5.1 Release Notes

## New features

* Disable AVX2 for MSys2 builds used by CRAN [#2614](https://github.com/TileDB-Inc/TileDB/pull/2614)

## Improvements

* Clarify error messages in `check_buffers_correctness()` [#2580](https://github.com/TileDB-Inc/TileDB/pull/2580)

## Bug fixes

* Fix schema evolution calls on all pre-TileDB 2.4 arrays [#2611](https://github.com/TileDB-Inc/TileDB/pull/2611)
* Unordered reads should be allowed for dense arrays [#2608](https://github.com/TileDB-Inc/TileDB/pull/2608)
* Fix logger creation on context to be threadsafe [#2625](https://github.com/TileDB-Inc/TileDB/pull/2625)

## API additions

### C++ API

* Add C++ API for `Context::last_error()` [#2609](https://github.com/TileDB-Inc/TileDB/pull/2609)

# TileDB v2.5.0 Release Notes

## Breaking C API changes

* Remove deprecated c-api `tiledb_array_max_buffer_size` and `tiledb_array_max_buffer_size_var` [#2579](https://github.com/TileDB-Inc/TileDB/pull/2579)
* Remove deprecated cpp-api `Array::max_buffer_elements` [#2579](https://github.com/TileDB-Inc/TileDB/pull/2579)

## New features

* Support upgrading an older version array to the latest version [#2513](https://github.com/TileDB-Inc/TileDB/pull/2513)
* Add improved logging support to classes [#2565](https://github.com/TileDB-Inc/TileDB/pull/2565)

## Improvements

* Replace Buffer key_ with char key_[32] per shortcut story id 9561 [#2502](https://github.com/TileDB-Inc/TileDB/pull/2502)
* Remove support for sparse writes in dense arrays. [#2504](https://github.com/TileDB-Inc/TileDB/pull/2504)
* Initial dense refactor. [#2503](https://github.com/TileDB-Inc/TileDB/pull/2503)
* More concise cmake output during build [#2512](https://github.com/TileDB-Inc/TileDB/pull/2512)
* Sparse refactored readers: fixing looping behavior on large arrays. [#2530](https://github.com/TileDB-Inc/TileDB/pull/2530)
* Use sparse global order reader for unordered without duplicates queries. [#2526](https://github.com/TileDB-Inc/TileDB/pull/2526)
* Add CMakeUserPresets.json to .gitignore [#2534](https://github.com/TileDB-Inc/TileDB/pull/2534)
* Sparse unordered with duplicates reader: support multiple ranges. [#2537](https://github.com/TileDB-Inc/TileDB/pull/2537)
* Refactored sparse readers: tile overlap refactor. [#2547](https://github.com/TileDB-Inc/TileDB/pull/2547)
* Refactored dense reader: fixing output buffer offsets with multi-ranges. [#2553](https://github.com/TileDB-Inc/TileDB/pull/2553)
* Refactored sparse readers: serialization fixes. [#2558](https://github.com/TileDB-Inc/TileDB/pull/2558)
* Refactored sparse readers: proper lifetime for tile bitmaps. [#2563](https://github.com/TileDB-Inc/TileDB/pull/2563)
* REST scratch buffer is now owned by the query to allow reuse [#2555](https://github.com/TileDB-Inc/TileDB/pull/2555)
* Remove default constructor from `Dimension` [#2561](https://github.com/TileDB-Inc/TileDB/pull/2561)
* Resource pool: fixing off by one error. [#2567](https://github.com/TileDB-Inc/TileDB/pull/2567)
* Splitting config for refactored readers. [#2569](https://github.com/TileDB-Inc/TileDB/pull/2569)
* Sparse refactored readers: memory management unit tests. [#2568](https://github.com/TileDB-Inc/TileDB/pull/2568)
* Removed all aspects of posix_code from Status [#2571](https://github.com/TileDB-Inc/TileDB/pull/2571)
* Fixing pre-loading for tile offsets in various readers. [#2570](https://github.com/TileDB-Inc/TileDB/pull/2570)
* Use the new logger in Subarray, SubarrayPartitioner and Consolidator classes. [#2574](https://github.com/TileDB-Inc/TileDB/pull/2574)
* Add tiledb_fragment_info_get_schema_name [#2581](https://github.com/TileDB-Inc/TileDB/pull/2581)
* Enable CMake AVX2 check [#2591](https://github.com/TileDB-Inc/TileDB/pull/2591)
* Adding logging for sparse refactored readers. [#2575](https://github.com/TileDB-Inc/TileDB/pull/2575)
* use ROW_MAJOR read paths for unordered reads of Hilbert layout array [#2551](https://github.com/TileDB-Inc/TileDB/pull/2551)

## Bug fixes

* Fix the memory leak in store_array_schema in the StorageManager class. [#2480](https://github.com/TileDB-Inc/TileDB/pull/2480)
* Fix curl/REST query scratch size to reset after each query is processed. [#2535](https://github.com/TileDB-Inc/TileDB/pull/2535)
* Sparse refactored readers: segfault with dimension only reads. [#2539](https://github.com/TileDB-Inc/TileDB/pull/2539)
* REST array metadata writes should post with timestamps [#2545](https://github.com/TileDB-Inc/TileDB/pull/2545)
* Fix bug in Arrow schema construction [#2554](https://github.com/TileDB-Inc/TileDB/pull/2554)
* Replaced `auto& path` with `auto path` [#2560](https://github.com/TileDB-Inc/TileDB/pull/2560)

## API additions

### C API

* Expose MBR in Fragment Info API [#2222](https://github.com/TileDB-Inc/TileDB/pull/2222)
* Add `tiledb_fragment_info_get_array_schema_name` for fetching array name used by fragment [#2581](https://github.com/TileDB-Inc/TileDB/pull/2581)

### C++ API

* Expose MBR in Fragment Info API [#2222](https://github.com/TileDB-Inc/TileDB/pull/2222)
* Add `FragmentInfo::array_schema_name` for fetching array name used by fragment [#2581](https://github.com/TileDB-Inc/TileDB/pull/2581)


# TileDB v2.4.3 Release Notes

## Bug fixes

* Fix segfault in result `ResultTile::coord_string` and `ResultTile::compute_results_sparse<char>` due empty chunk buffer [#2531](https://github.com/TileDB-Inc/TileDB/pull/2531)
* Fix memory corruption with empty result set in extra_element mode [#2540](https://github.com/TileDB-Inc/TileDB/pull/2540)
* REST array metadata writes should post with timestamps [#2545](https://github.com/TileDB-Inc/TileDB/pull/2545)
* Backport fixes for new Sparse Unordered with Duplicate readers from [#2530](https://github.com/TileDB-Inc/TileDB/pull/2530) and [#2538](https://github.com/TileDB-Inc/TileDB/pull/2538)

## API additions

# TileDB v2.4.2 Release Notes

## New features

* Add support for empty string as query condition value. [#2507](https://github.com/TileDB-Inc/TileDB/pull/2507)

## Improvements

* Support writing empty strings for dimensions [#2501](https://github.com/TileDB-Inc/TileDB/pull/2501)
* Refactored readers can segfault when multiple contexts are used. [#2525](https://github.com/TileDB-Inc/TileDB/pull/2525)

## Bug fixes

* Fix ch10191: check cell_val_num for varlen status instead of result count [#2505](https://github.com/TileDB-Inc/TileDB/pull/2505)
* Do not access variables after moving them [#2522](https://github.com/TileDB-Inc/TileDB/pull/2522)
* Add try/catch to `tiledb_ctx_alloc` for exception safety [#2527](https://github.com/TileDB-Inc/TileDB/pull/2527)

# TileDB v2.4.0 Release Notes

## Disk Format

* Store array schemas under `__schema` directory [#2258](https://github.com/TileDB-Inc/TileDB/pull/2258)

## New features

* Perform early audit for acceptable aws sdk windows path length [#2260](https://github.com/TileDB-Inc/TileDB/pull/2260)
* Support setting via config s3 BucketCannedACL and ObjectCannedACL via SetACL() methods [#2383](https://github.com/TileDB-Inc/TileDB/pull/2383)
* Update spdlog dependency to 1.9.0 fixing c++17 compatibility and general improvements [#1973](https://github.com/TileDB-Inc/TileDB/pull/1973)
* Added Azure SAS token config support and new config option [#2420](https://github.com/TileDB-Inc/TileDB/pull/2420)
* Load all array schemas in storage manager and pass the appropriate schema pointer to each fragment [#2415](https://github.com/TileDB-Inc/TileDB/pull/2415)
* First revision of the Interval class [#2417](https://github.com/TileDB-Inc/TileDB/pull/2417)
* Add `tiledb_schema_evolution_t` and new apis for schema evolution [#2426](https://github.com/TileDB-Inc/TileDB/pull/2426)
* Add `ArraySchemaEvolution` to cpp_api and its unit tests are also added. [#2462](https://github.com/TileDB-Inc/TileDB/pull/2462)
* Add c and cpp api functions for getting the array schema of a fragment [#2468](https://github.com/TileDB-Inc/TileDB/pull/2468)
* Add capnp serialization and rest support for array schema evolution objects [#2467](https://github.com/TileDB-Inc/TileDB/pull/2467)

## Improvements

* `encryption_key` and `encryption_type` parameters have been added to the config; internal APIs now use these parameters to set the key. [#2245](https://github.com/TileDB-Inc/TileDB/pull/2245)
* Initial read refactor [#2374](https://github.com/TileDB-Inc/TileDB/pull/2374)
* Create class ByteVecValue from typedef [#2368](https://github.com/TileDB-Inc/TileDB/pull/2368)
* Encapsulate spdlog.h [#2396](https://github.com/TileDB-Inc/TileDB/pull/2396)
* Update OSX target to 10.14 for release artifacts [#2401](https://github.com/TileDB-Inc/TileDB/pull/2401)
* Add nullable (and unordered, nullable) support to the smoke test. [#2405](https://github.com/TileDB-Inc/TileDB/pull/2405)
* Initial sparse global order reader [#2395](https://github.com/TileDB-Inc/TileDB/pull/2395)
* Remove sm.sub_partitioner_memory_budget [#2402](https://github.com/TileDB-Inc/TileDB/pull/2402)
* Update the markdown documents for our new version of array schemas [#2416](https://github.com/TileDB-Inc/TileDB/pull/2416)
* Sparse global order reader: no more result cell slab copy. [#2411](https://github.com/TileDB-Inc/TileDB/pull/2411)
* Sparse global order reader: initial memory budget improvements. [#2413](https://github.com/TileDB-Inc/TileDB/pull/2413)
* Optimization of result cell slabs generation for sparse global order reader. [#2414](https://github.com/TileDB-Inc/TileDB/pull/2414)
* Remove selective unfiltering. [#2410](https://github.com/TileDB-Inc/TileDB/pull/2410)
* Updated Azure Storage Lite SDK to 0.3.0 [#2419](https://github.com/TileDB-Inc/TileDB/pull/2419)
* Respect memory budget for sparse global order reader. [#2425](https://github.com/TileDB-Inc/TileDB/pull/2425)
* Use newer Azure patch for all platforms to solve missing header error [#2433](https://github.com/TileDB-Inc/TileDB/pull/2433)
* increased diag output for differences reported by tiledb_unit (some of which may be reasonable) [#2437](https://github.com/TileDB-Inc/TileDB/pull/2437)
* Adjustments to schema evolution new attribute reads [#2484](https://github.com/TileDB-Inc/TileDB/pull/2484)
* Change `Quickstart` link in readthedocs/doxygen `index.rst` [#2448](https://github.com/TileDB-Inc/TileDB/pull/2448)
* Initial sparse unordered with duplicates reader. [#2441](https://github.com/TileDB-Inc/TileDB/pull/2441)
* Add calls to `malloc_trim` on context and query destruction linux to potentially reduce idle memory usage [#2443](https://github.com/TileDB-Inc/TileDB/pull/2443)
* Add logger internals for `std::string` and `std::stringstream` for developer convenience [#2454](https://github.com/TileDB-Inc/TileDB/pull/2454)
* Allow empty attribute writes. [#2461](https://github.com/TileDB-Inc/TileDB/pull/2461)
* Refactored readers: serialization. [#2458](https://github.com/TileDB-Inc/TileDB/pull/2458)
* Allow null data pointers for writes. [#2481](https://github.com/TileDB-Inc/TileDB/pull/2481)
* Update backwards compatibility arrays for 2.3.0 [#2487](https://github.com/TileDB-Inc/TileDB/pull/2487)

## Deprecations

* Deprecate all `*_with_key` APIs. [#2245](https://github.com/TileDB-Inc/TileDB/pull/2245) [#2308](https://github.com/TileDB-Inc/TileDB/pull/2308) [#2412](https://github.com/TileDB-Inc/TileDB/pull/2412)

## Bug fixes

* Fix to correctly apply capnproto create_symlink avoidance patch [#2264](https://github.com/TileDB-Inc/TileDB/pull/2264)
* The bug for calculating max_size_validity for var_size attribute caused incomplete query [#2266](https://github.com/TileDB-Inc/TileDB/pull/2266)
* Always run ASAN with matching compiler versions [#2277](https://github.com/TileDB-Inc/TileDB/pull/2277)
* Fix some loop bounds that reference non-existent elements [#2282](https://github.com/TileDB-Inc/TileDB/pull/2282)
* Treating `std::vector` like an array; accessing an element that is not present to get its address. [#2276](https://github.com/TileDB-Inc/TileDB/pull/2276)
* Fix buffer arguments in unit-curl.cc [#2287](https://github.com/TileDB-Inc/TileDB/pull/2287)
* Stop loop iterations within limits of vector being initialized. [#2320](https://github.com/TileDB-Inc/TileDB/pull/2320)
* Modify FindCurl_EP.cmake to work for WIN32 -EnableDebug builds [#2319](https://github.com/TileDB-Inc/TileDB/pull/2319)
* Fixing test failure because of an uninitialized buffer. [#2386](https://github.com/TileDB-Inc/TileDB/pull/2386)
* Change a condition that assumed MSVC was the only compiler for WIN32 [#2388](https://github.com/TileDB-Inc/TileDB/pull/2388)
* Fix defects in buffer classes: read, set_offset, advance_offset [#2342](https://github.com/TileDB-Inc/TileDB/pull/2342)
* Use CHECK_SAFE() to avoid multi-threaded conflict [#2394](https://github.com/TileDB-Inc/TileDB/pull/2394)
* Use tiledb _SAFE() items when overlapping threads may invoke code [#2418](https://github.com/TileDB-Inc/TileDB/pull/2418)
* Changes to address issues with default string dimension ranges in query [#2436](https://github.com/TileDB-Inc/TileDB/pull/2436)
* Only set cmake policy CMP0076 if cmake version in use knows about it [#2463](https://github.com/TileDB-Inc/TileDB/pull/2463)
* Fix handling curl REST request having all data in single call back [#2485](https://github.com/TileDB-Inc/TileDB/pull/2485)
* Write queries should post start/end timestamps for REST arrays [#2492](https://github.com/TileDB-Inc/TileDB/pull/2492)

## API additions

* Introduce new `tiledb_experimental.h` c-api header for new feature that don't have a stabilized api yet [#2453](https://github.com/TileDB-Inc/TileDB/pull/2453)
* Introduce new `tiledb_experimental` cpp-api header for new feature that don't have a stabilized api yet [#2453](https://github.com/TileDB-Inc/TileDB/pull/2462)

### C API

* Refactoring [get/set]_buffer APIs [#2315](https://github.com/TileDB-Inc/TileDB/pull/2315)
* Add `tiledb_fragment_info_get_array_schema` functions for getting the array schema of a fragment [#2468](https://github.com/TileDB-Inc/TileDB/pull/2468)
* Add `tiledb_schema_evolution_t` and new apis for schema evolution [#2426](https://github.com/TileDB-Inc/TileDB/pull/2426)

### C++ API

* Refactoring [get/set]_buffer APIs [#2399](https://github.com/TileDB-Inc/TileDB/pull/2399)
* Add `FragmentInfo::array_schema` functions for getting the array schema of a fragment [#2468](https://github.com/TileDB-Inc/TileDB/pull/2468)
* Add `ArraySchemaEvolution` to cpp_api and its unit tests are also added. [#2462](https://github.com/TileDB-Inc/TileDB/pull/2462)

# TileDB v2.3.4 Release Notes

## Improvements

* `Query::set_layout`: setting the layout on the subarray. [#2451](https://github.com/TileDB-Inc/TileDB/pull/2451)
* Allow empty attribute writes. [#2461](https://github.com/TileDB-Inc/TileDB/pull/2461)

## Bug fixes

* Fix deserialization of buffers in write queries with nullable var-length attributes [#2442](https://github.com/TileDB-Inc/TileDB/pull/2442)

# TileDB v2.3.3 Release Notes

## Improvements

* Increase REST (TileDB Cloud) retry count from 3 to 25 to be inline with S3/GCS retry times [#2421](https://github.com/TileDB-Inc/TileDB/pull/2421)
* Avoid unnecessary `est_result_size` computation in `must_split` [#2431](https://github.com/TileDB-Inc/TileDB/pull/2431)
* Use newer Azure patch for all platforms to solve missing header error [#2433](https://github.com/TileDB-Inc/TileDB/pull/2433)

## Bug fixes

* Fix c-api error paths always resetting any alloced pointers to nullptr in-addition to deleting [#2427](https://github.com/TileDB-Inc/TileDB/pull/2427)

# TileDB v2.3.2 Release Notes

## Improvements
* Support more env selectable options in both azure-windows.yml and azure-windows-release.yml [#2384](https://github.com/TileDB-Inc/TileDB/pull/2384)
* Enable Azure/Serialization for windows CI artifacts [#2400](https://github.com/TileDB-Inc/TileDB/pull/2400)

## Bug fixes
* Correct check for last offset position so that undefined memory is not accessed. [#2390](https://github.com/TileDB-Inc/TileDB/pull/2390)
* Fix ch8416, failure to read array written with tiledb 2.2 via REST [#2404](https://github.com/TileDB-Inc/TileDB/pull/2404)
* Fix ch7582: use the correct buffer for validity deserialization [#2407](https://github.com/TileDB-Inc/TileDB/pull/2407)

# TileDB v2.3.1 Release Notes

## Improvements
* Update bzip2 in windows build to 1.0.8 [#2332](https://github.com/TileDB-Inc/TileDB/pull/2332)
* Fixing S3 build for OSX11 [#2339](https://github.com/TileDB-Inc/TileDB/pull/2339)
* Fixing possible overflow in Dimension::tile_num [#2265](https://github.com/TileDB-Inc/TileDB/pull/2265)
* Fixing tile extent calculations for signed integer domains [#2303](https://github.com/TileDB-Inc/TileDB/pull/2303)
* Add support for cross compilation on OSX in superbuild [#2354](https://github.com/TileDB-Inc/TileDB/pull/2354)
* Remove curl link args for cross compilation [#2359](https://github.com/TileDB-Inc/TileDB/pull/2359)
* Enable MacOS arm64 release artifacts [#2360](https://github.com/TileDB-Inc/TileDB/pull/2360)
* Add more stats for \`compute_result_coords\` path [#2366](https://github.com/TileDB-Inc/TileDB/pull/2366)
* Support credentials refresh for AWS [#2376](https://github.com/TileDB-Inc/TileDB/pull/2376)

## Bug fixes
* Fixing intermittent metadata test failure [#2338](https://github.com/TileDB-Inc/TileDB/pull/2338)
* Fix query condition validation check for nullable attributes with null conditions [#2344](https://github.com/TileDB-Inc/TileDB/pull/2344)
* Multi-range single dimension query fix [#2347](https://github.com/TileDB-Inc/TileDB/pull/2347)
* Rewrite `Dimension::overlap_ratio` [#2304](https://github.com/TileDB-Inc/TileDB/pull/2304)
* Follow up fixes to floating point calculations for tile extents [#2341](https://github.com/TileDB-Inc/TileDB/pull/2341)
* Fix for set_null_tile_extent_to_range [#2361](https://github.com/TileDB-Inc/TileDB/pull/2361)
* Subarray partitioner, unordered should be unordered, even for Hilbert. [#2377](https://github.com/TileDB-Inc/TileDB/pull/2377)

# TileDB v2.3.0 Release Notes

## Disk Format
* Format version incremented to 9. [#2108](https://github.com/TileDB-Inc/TileDB/pull/2108)

## Breaking behavior
* The setting of \`sm.read_range_oob\` now defaults to \`warn\`, allowing queries to run with bounded ranges that errored before. [#2176](https://github.com/TileDB-Inc/TileDB/pull/2176)
* Removes TBB as an optional dependency [#2181](https://github.com/TileDB-Inc/TileDB/pull/2181)

## New features
* Support TILEDB_DATETIME_{SEC,MS,US,NS} in arrow_io_impl.h [#2228](https://github.com/TileDB-Inc/TileDB/pull/2228)
* Adds support for filtering query results on attribute values [#2141](https://github.com/TileDB-Inc/TileDB/pull/2141)
* Adding support for time datatype dimension and attribute [#2140](https://github.com/TileDB-Inc/TileDB/pull/2140)
* Add support for serialization of config objects [#2164](https://github.com/TileDB-Inc/TileDB/pull/2164)
* Add C and C++ examples to the `examples/` directory for the `tiledb_fragment_info_t` APIs. [#2160](https://github.com/TileDB-Inc/TileDB/pull/2160)
* supporting serialization (using capnproto) build on windows [#2100](https://github.com/TileDB-Inc/TileDB/pull/2100)
* Config option "vfs.s3.sse" for S3 server-side encryption support [#2130](https://github.com/TileDB-Inc/TileDB/pull/2130)
* Name attribute/dimension files by index. This is fragment-specific and updates the format version to version 9. [#2107](https://github.com/TileDB-Inc/TileDB/pull/2107)
* Smoke Test, remove nullable structs from global namespace. [#2078](https://github.com/TileDB-Inc/TileDB/pull/2078)

## Improvements
* replace ReadFromOffset with ReadRange in GCS::read() to avoid excess gcs egress traffic [#2307](https://github.com/TileDB-Inc/TileDB/pull/2307)
* Hilbert partitioning fixes [#2269](https://github.com/TileDB-Inc/TileDB/pull/2269)
* Stats refactor [#2267](https://github.com/TileDB-Inc/TileDB/pull/2267)
* Improve Cap'n Proto cmake setup for system installations [#2263](https://github.com/TileDB-Inc/TileDB/pull/2263)
* Runtime check for minimum validity buffer size [#2261](https://github.com/TileDB-Inc/TileDB/pull/2261)
* Enable partial vacuuming when vacuuming with timestamps [#2251](https://github.com/TileDB-Inc/TileDB/pull/2251)
* Consolidation: de-dupe FragmentInfo [#2250](https://github.com/TileDB-Inc/TileDB/pull/2250)
* Consolidation: consider non empty domain before start timestamp [#2248](https://github.com/TileDB-Inc/TileDB/pull/2248)
* Add size details to s3 read error [#2249](https://github.com/TileDB-Inc/TileDB/pull/2249)
* Consolidation: do not re-open array for each fragment [#2243](https://github.com/TileDB-Inc/TileDB/pull/2243)
* Support back compat writes [#2230](https://github.com/TileDB-Inc/TileDB/pull/2230)
* Serialization support for query conditions [#2240](https://github.com/TileDB-Inc/TileDB/pull/2240)
* Make SubarrayPartitioner's member functions to return Status after calling Subarray::get_range_num. [#2235](https://github.com/TileDB-Inc/TileDB/pull/2235)
* Update bzip2 super build version to 1.0.8 to address [CVE-2019-12900](https://nvd.nist.gov/vuln/detail/CVE-2019-12900) in libbzip2 [#2233](https://github.com/TileDB-Inc/TileDB/pull/2233)
* Timestamp start and end for vacuuming and consolidation [#2227](https://github.com/TileDB-Inc/TileDB/pull/2227)
* Fix memory leaks reported on ASAN when running with leak-detection. [#2223](https://github.com/TileDB-Inc/TileDB/pull/2223)
* Use relative paths in consolidated fragment metadata [#2215](https://github.com/TileDB-Inc/TileDB/pull/2215)
* Optimize `Subarray::compute_relevant_fragments` [#2216](https://github.com/TileDB-Inc/TileDB/pull/2216)
* AWS S3: improve is_dir [#2209](https://github.com/TileDB-Inc/TileDB/pull/2209)
* Add nullable string to nullable attribute example [#2212](https://github.com/TileDB-Inc/TileDB/pull/2212)
* AWS S3: adding option to skip Aws::InitAPI [#2204](https://github.com/TileDB-Inc/TileDB/pull/2204)
* Added additional stats for subarrays and subarray partitioners [#2200](https://github.com/TileDB-Inc/TileDB/pull/2200)
* Introduces config parameter "sm.skip_est_size_partitioning" [#2203](https://github.com/TileDB-Inc/TileDB/pull/2203)
* Add config to query serialization. [#2177](https://github.com/TileDB-Inc/TileDB/pull/2177)
* Consolidation support for nullable attributes [#2196](https://github.com/TileDB-Inc/TileDB/pull/2196)
* Adjust unit tests to reduce memory leaks inside the tests. [#2179](https://github.com/TileDB-Inc/TileDB/pull/2179)
* Reduces memory usage in multi-range range reads [#2165](https://github.com/TileDB-Inc/TileDB/pull/2165)
* Add config option \`sm.read_range_oob\` to toggle bounding read ranges to domain or erroring [#2162](https://github.com/TileDB-Inc/TileDB/pull/2162)
* Windows msys2 build artifacts are no longer uploaded [#2159](https://github.com/TileDB-Inc/TileDB/pull/2159)
* Add internal log functions to log at different log levels [#2161](https://github.com/TileDB-Inc/TileDB/pull/2161)
* Parallelize Writer::filter_tiles [#2156](https://github.com/TileDB-Inc/TileDB/pull/2156)
* Added config option "vfs.gcs.request_timeout_ms" [#2148](https://github.com/TileDB-Inc/TileDB/pull/2148)
* Improve fragment info loading by parallelizing fragment_size requests [#2143](https://github.com/TileDB-Inc/TileDB/pull/2143)
* Allow open array stats to be printed without read query [#2131](https://github.com/TileDB-Inc/TileDB/pull/2131)
* Cleanup the GHA CI scripts - put common code into external shell scripts. [#2124](https://github.com/TileDB-Inc/TileDB/pull/2124)
* Reduced memory consumption in the read path for multi-range reads. [#2118](https://github.com/TileDB-Inc/TileDB/pull/2118)
* The latest version of `dev` was leaving behind a `test/empty_string3/`. This ensures that the directory is removed when `make check` is run. [#2113](https://github.com/TileDB-Inc/TileDB/pull/2113)
* Migrating AZP CI to GA [#2111](https://github.com/TileDB-Inc/TileDB/pull/2111)
* Cache non_empty_domain for REST arrays like all other arrays [#2105](https://github.com/TileDB-Inc/TileDB/pull/2105)
* Add additional stats printing to breakdown read state initialization timings [#2095](https://github.com/TileDB-Inc/TileDB/pull/2095)
* Places the in-memory filesystem under unit test [#1961](https://github.com/TileDB-Inc/TileDB/pull/1961)
* Adds a Github Action to automate the HISTORY.md [#2075](https://github.com/TileDB-Inc/TileDB/pull/2075)
* Change printfs in C++ examples to cout, edit C print statements to fix format warnings [#2226](https://github.com/TileDB-Inc/TileDB/pull/2226)

## Deprecations
* The following APIs have been deprecated: `tiledb_array_open_at`, `tiledb_array_open_at_with_key`, `tiledb_array_reopen_at`. [#2142](https://github.com/TileDB-Inc/TileDB/pull/2142)

## Bug fixes
* Fix a segfault on `VFS::ls` for the in-memory filesystem [#2255](https://github.com/TileDB-Inc/TileDB/pull/2255)
* Fix rare read corruption in S3 [#2253](https://github.com/TileDB-Inc/TileDB/pull/2253)
* Update some union initializers to use strict syntax [#2242](https://github.com/TileDB-Inc/TileDB/pull/2242)
* Fix race within `S3::init_client` [#2247](https://github.com/TileDB-Inc/TileDB/pull/2247)
* Expand accepted windows URIs. [#2237](https://github.com/TileDB-Inc/TileDB/pull/2237)
* Write fix for unordered writes on nullable, fixed attributes. [#2241](https://github.com/TileDB-Inc/TileDB/pull/2241)
* Fix tile extent to be reported as domain extent for sparse arrays with Hilbert ordering [#2231](https://github.com/TileDB-Inc/TileDB/pull/2231)
* Do not consider option sm.read_range_oob for set_subarray() on Write queries [#2211](https://github.com/TileDB-Inc/TileDB/pull/2211)
* Change avoiding generation of multiple, concatenated, subarray flattened data. [#2190](https://github.com/TileDB-Inc/TileDB/pull/2190)
* Change mutex from basic to recursive [#2180](https://github.com/TileDB-Inc/TileDB/pull/2180)
* Fixes a memory leak in the S3 read path [#2189](https://github.com/TileDB-Inc/TileDB/pull/2189)
* Fixes a potential memory leak in the filter pipeline [#2185](https://github.com/TileDB-Inc/TileDB/pull/2185)
* Fixes misc memory leaks in the unit tests [#2183](https://github.com/TileDB-Inc/TileDB/pull/2183)
* Fix memory leak of \`tiledb_config_t\` in error path of \`tiledb_config_alloc\`. [#2178](https://github.com/TileDB-Inc/TileDB/pull/2178)
* Fix check for null pointer in query deserialization [#2163](https://github.com/TileDB-Inc/TileDB/pull/2163)
* Fixes a potential crash when retrying incomplete reads [#2137](https://github.com/TileDB-Inc/TileDB/pull/2137)
* Fixes a potential crash when opening an array with consolidated fragment metadata [#2135](https://github.com/TileDB-Inc/TileDB/pull/2135)
* Corrected a bug where sparse cells may be incorrectly returned using string dimensions. [#2125](https://github.com/TileDB-Inc/TileDB/pull/2125)
* Fix segfault in serialized queries when partition is unsplittable [#2120](https://github.com/TileDB-Inc/TileDB/pull/2120)
* Always use original buffer size in serialized read queries serverside. [#2115](https://github.com/TileDB-Inc/TileDB/pull/2115)
* Fix an edge-case where a read query may hang on array with string dimensions [#2089](https://github.com/TileDB-Inc/TileDB/pull/2089)

## API additions

### C API
* Added tiledb_array_set_open_timestamp_start and tiledb_array_get_open_timestamp_start [#2285](https://github.com/TileDB-Inc/TileDB/pull/2285)
* Added tiledb_array_set_open_timestamp_end and tiledb_array_get_open_timestamp_end [#2285](https://github.com/TileDB-Inc/TileDB/pull/2285)
* Addition of `tiledb_array_set_config` to directly assign a config to an array. [#2142](https://github.com/TileDB-Inc/TileDB/pull/2142)
* tiledb_query_get_array now returns a deep-copy [#2184](https://github.com/TileDB-Inc/TileDB/pull/2184)
* Added \`tiledb_serialize_config\` and \`tiledb_deserialize_config\` [#2164](https://github.com/TileDB-Inc/TileDB/pull/2164)
* Add new api, `tiledb_query_get_config` to get a query's config. [#2167](https://github.com/TileDB-Inc/TileDB/pull/2167)
* Removes non-default parameter in "tiledb_config_unset". [#2099](https://github.com/TileDB-Inc/TileDB/pull/2099)

### C++ API
* Added Array::set_open_timestamp_start and Array::open_timestamp_start [#2285](https://github.com/TileDB-Inc/TileDB/pull/2285)
* Added Array::set_open_timestamp_end and Array::open_timestamp_end [#2285](https://github.com/TileDB-Inc/TileDB/pull/2285)
* add Query::result_buffer_elements_nullable support for dims [#2238](https://github.com/TileDB-Inc/TileDB/pull/2238)
* Addition of `tiledb_array_set_config` to directly assign a config to an array. [#2142](https://github.com/TileDB-Inc/TileDB/pull/2142)
* Add new api, `Query.config()` to get a query's config. [#2167](https://github.com/TileDB-Inc/TileDB/pull/2167)
* Removes non-default parameter in "Config::unset". [#2099](https://github.com/TileDB-Inc/TileDB/pull/2099)
* Add support for a string-typed, variable-sized, nullable attribute in the C++ API. [#2090](https://github.com/TileDB-Inc/TileDB/pull/2090)

# TileDB v2.2.9 Release Notes

## Bug fixes
* Fix rare read corruption in S3 [#2254](https://github.com/TileDB-Inc/TileDB/pull/2254)
* Write fix for unordered writes on nullable, fixed attributes [#2241](https://github.com/TileDB-Inc/TileDB/pull/2241)

# TileDB v2.2.8 Release Notes

## Disk Format

## Breaking C API changes

## Breaking behavior

## New features
* Support TILEDB_DATETIME_{SEC,MS,US,NS} in arrow_io_impl.h [#2229](https://github.com/TileDB-Inc/TileDB/pull/2229)
* Add support for serialization of config objects [#2164](https://github.com/TileDB-Inc/TileDB/pull/2164)
* Add support for serialization of query config [#2177](https://github.com/TileDB-Inc/TileDB/pull/2177)

## Improvements
* Optimize `Subarray::compute_relevant_fragments` [#2218](https://github.com/TileDB-Inc/TileDB/pull/2218)
* Reduces memory usage in multi-range range reads [#2165](https://github.com/TileDB-Inc/TileDB/pull/2165)
* Add config option `sm.read_range_oob` to toggle bounding read ranges to domain or erroring [#2162](https://github.com/TileDB-Inc/TileDB/pull/2162)
* Updates bzip2 to v1.0.8 on Linux/OSX [#2233](https://github.com/TileDB-Inc/TileDB/pull/2233)

## Deprecations

## Bug fixes
* Fixes a potential memory leak in the filter pipeline [#2185](https://github.com/TileDB-Inc/TileDB/pull/2185)
* Fixes misc memory leaks in the unit tests [#2183](https://github.com/TileDB-Inc/TileDB/pull/2183)
* Fix memory leak of `tiledb_config_t` in error path of `tiledb_config_alloc`. [#2178](https://github.com/TileDB-Inc/TileDB/pull/2178)

## API additions

### C API
* tiledb_query_get_array now returns a deep-copy [#2188](https://github.com/TileDB-Inc/TileDB/pull/2188)
* Add new api,`tiledb_query_get_config` to get a query's config. [#2167](https://github.com/TileDB-Inc/TileDB/pull/2167)
* Added `tiledb_serialize_config` and `tiledb_deserialize_config` [#2164](https://github.com/TileDB-Inc/TileDB/pull/2164)

### C++ API
* Add new api, `Query.config()`  to get a query's config. [#2167](https://github.com/TileDB-Inc/TileDB/pull/2167)

# TileDB v2.2.7 Release Notes

## Improvements

* Added config option vfs.gcs.request_timeout_ms [#2148](https://github.com/TileDB-Inc/TileDB/pull/2148)
* Improve fragment info loading by parallelizing fragment_size requests [#2143](https://github.com/TileDB-Inc/TileDB/pull/2143)
* Apply 'var_offsets.extra_element' mode to string dimension offsets too [#2145](https://github.com/TileDB-Inc/TileDB/pull/2145)

# TileDB v2.2.6 Release Notes

## Bug fixes

* Fixes a potential crash when retrying incomplete reads [#2137](https://github.com/TileDB-Inc/TileDB/pull/2137)

# TileDB v2.2.5 Release Notes

## New features

* Config option vfs.s3.sse for S3 server-side encryption support [#2130](https://github.com/TileDB-Inc/TileDB/pull/2130)

## Improvements

* Reduced memory consumption in the read path for multi-range reads. [#2118](https://github.com/TileDB-Inc/TileDB/pull/2118)
* Cache non_empty_domain for REST arrays like all other arrays [#2105](https://github.com/TileDB-Inc/TileDB/pull/2105)
* Add additional timer statistics for openning array for reads [#2027](https://github.com/TileDB-Inc/TileDB/pull/2027)
* Allow open array stats to be printed without read query [#2131](https://github.com/TileDB-Inc/TileDB/pull/2131)

## Bug fixes
* Fixes a potential crash when opening an array with consolidated fragment metadata [#2135](https://github.com/TileDB-Inc/TileDB/pull/2135)
* Corrected a bug where sparse cells may be incorrectly returned using string dimensions. [#2125](https://github.com/TileDB-Inc/TileDB/pull/2125)
* Always use original buffer size in serialized read queries serverside. [#2115](https://github.com/TileDB-Inc/TileDB/pull/2115)
* Fix segfault in serialized queries when partition is unsplittable [#2120](https://github.com/TileDB-Inc/TileDB/pull/2120)

# TileDB v2.2.4 Release Notes

## Improvements

* Add additional stats printing to breakdown read state initialization timings [#2095](https://github.com/TileDB-Inc/TileDB/pull/2095)
* Improve GCS multipart locking [#2087](https://github.com/TileDB-Inc/TileDB/pull/2087)

## Bug fixes
* Fix an edge-case where a read query may hang on array with string dimensions [#2089](https://github.com/TileDB-Inc/TileDB/pull/2089)
* Fix mutex locking bugs on Windows due to unlocking on different thread and missing task join [#2077](https://github.com/TileDB-Inc/TileDB/pull/2077)

### C++ API
* Add support for a string-typed, variable-sized, nullable attribute in the C++ API. [#2090](https://github.com/TileDB-Inc/TileDB/pull/2090)

# TileDB v2.2.3 Release Notes

## New features

* Add support for retrying REST requests that fail with certain http status code such as 503 [#2060](https://github.com/TileDB-Inc/TileDB/pull/2060)

## Improvements

* Parallelize across attributes when closing a write [#2048](https://github.com/TileDB-Inc/TileDB/pull/2048)
* Support for dimension/attribute names that contain commonly reserved filesystem characters [#2047](https://github.com/TileDB-Inc/TileDB/pull/2047)
* Remove unnecessary `is_dir` in `FragmentMetadata::store`, this can increase performance for s3 writes [#2050](https://github.com/TileDB-Inc/TileDB/pull/2050)
* Improve S3 multipart locking [#2055](https://github.com/TileDB-Inc/TileDB/pull/2055)
* Parallize loading fragments and array schema [#2061](https://github.com/TileDB-Inc/TileDB/pull/2061)

# TileDB v2.2.2 Release Notes

## New features

* REST client support for caching redirects [#1919](https://github.com/TileDB-Inc/TileDB/pull/1919)

## Improvements

* Add `rest.creation_access_credentials_name` configuration parameter [#2025](https://github.com/TileDB-Inc/TileDB/pull/2025)

## Bug fixes

* Fixed ArrowAdapter export of string arrays with 64-bit offsets [#2037](https://github.com/TileDB-Inc/TileDB/pull/2037)
* Fixed ArrowAdapter export of TILEDB_CHAR arrays with 64-bit offsets [#2039](https://github.com/TileDB-Inc/TileDB/pull/2039)

## API additions

### C API
* Add `tiledb_query_set_config` to apply a `tiledb_config_t` to query-level parameters [#2030](https://github.com/TileDB-Inc/TileDB/pull/2030)

### C++ API
* Added `Query::set_config` to apply a `tiledb::Config` to query-level parameters [#2030](https://github.com/TileDB-Inc/TileDB/pull/2030)

# TileDB v2.2.1 Release Notes

## Breaking behavior

* The tile extent can now be set to null, in which case internally TileDB sets the extent to the dimension domain range. [#1880](https://github.com/TileDB-Inc/TileDB/pull/1880)
* The C++ API `std::pair<uint64_t, uint64_t> Query::est_result_size_var` has been changed to 1) a return type of `std::array<uint64_t, 2>` and 2) returns the offsets as a size in bytes rather than elements. [#1946](https://github.com/TileDB-Inc/TileDB/pull/1946)

## New features

* Support for nullable attributes. [#1895](https://github.com/TileDB-Inc/TileDB/pull/1895) [#1938](https://github.com/TileDB-Inc/TileDB/pull/1938) [#1948](https://github.com/TileDB-Inc/TileDB/pull/1948) [#1945](https://github.com/TileDB-Inc/TileDB/pull/1945)
* Support for Hilbert order sorting for sparse arrays. [#1880](https://github.com/TileDB-Inc/TileDB/pull/1880)
* Support for AWS S3 "AssumeRole" temporary credentials [#1882](https://github.com/TileDB-Inc/TileDB/pull/1882)
* Support for zero-copy import/export with the Apache Arrow adapter [#2001](https://github.com/TileDB-Inc/TileDB/pull/2001)
* Experimental support for an in-memory backend used with bootstrap option "--enable-memfs" [#1873](https://github.com/TileDB-Inc/TileDB/pull/1873)
* Support for element offsets when reading var-sized attributes. [#1897] (https://github.com/TileDB-Inc/TileDB/pull/1897)
* Support for an extra offset indicating the size of the returned data when reading var-sized attributes. [#1932] (https://github.com/TileDB-Inc/TileDB/pull/1932)
* Support for 32-bit offsets when reading var-sized attributes. [#1950] (https://github.com/TileDB-Inc/TileDB/pull/1950)

## Improvements

* Optimized string dimension performance. 
* Added functionality to get fragment information from an array. [#1900](https://github.com/TileDB-Inc/TileDB/pull/1900)
* Prevented unnecessary sorting when (1) there is a single fragment and (i) either the query layout is global order, or (ii) the number of dimensions is 1, and (2) when there is a single range for which the result coordinates have already been sorted. [#1880](https://github.com/TileDB-Inc/TileDB/pull/1880)
* Added extra stats for consolidation. [#1880](https://github.com/TileDB-Inc/TileDB/pull/1880)
* Disabled checking if cells are written in global order when consolidating, as it was redundant (the cells are already being read in global order during consolidation). [#1880](https://github.com/TileDB-Inc/TileDB/pull/1880) 
* Optimize consolidated fragment metadata loading [#1975](https://github.com/TileDB-Inc/TileDB/pull/1975)

## Bug fixes

* Fix tiledb_dimension_alloc returning a non-null pointer after error [#1959]((https://github.com/TileDB-Inc/TileDB/pull/1859)
* Fixed issue with string dimensions and non-set subarray (which implies spanning the whole domain). There was an assertion being triggered. Now it works properly.
* Fixed bug when checking the dimension domain for infinity or NaN values. [#1880](https://github.com/TileDB-Inc/TileDB/pull/1880)
* Fixed bug with string dimension partitioning. [#1880](https://github.com/TileDB-Inc/TileDB/pull/1880)

## API additions

### C API

* Added functions for getting fragment information. [#1900](https://github.com/TileDB-Inc/TileDB/pull/1900)
* Added APIs for getting and setting ranges of queries using a dimension name. [#1920](https://github.com/TileDB-Inc/TileDB/pull/1920)

### C++ API

* Added class `FragmentInfo` and functions for getting fragment information. [#1900](https://github.com/TileDB-Inc/TileDB/pull/1900)
* Added function `Dimension::create` that allows not setting a space tile extent. [#1880](https://github.com/TileDB-Inc/TileDB/pull/1880)
* Added APIs for getting and setting ranges of queries using a dimension name. [#1920](https://github.com/TileDB-Inc/TileDB/pull/1920)
* Changed `std::pair<uint64_t, uint64_t> Query::est_result_size_var` to `std::array<uint64_t, 2> Query::est_result_size_var`. Additionally, the size estimate for the offsets have been changed from elements to bytes. [#1946](https://github.com/TileDB-Inc/TileDB/pull/1946)

# TileDB v2.2.0 Release Notes

* This release was skipped due to an erroneous `2.2.0` git tag. 

# TileDB v2.1.6 Release Notes

## Bug fixes
* Fix deadlock in `ThreadPool::wait_or_work` [#1994](https://github.com/TileDB-Inc/TileDB/pull/1994)
* Fix "[TileDB::ChunkedBuffer] Error: Cannot init chunk buffers; Total size must be non-zero." in read path [#1992](https://github.com/TileDB-Inc/TileDB/pull/1992)

# TileDB v2.1.5 Release Notes

## Improvements

* Optimize consolidated fragment metadata loading [#1975](https://github.com/TileDB-Inc/TileDB/pull/1975)
* Optimize `Reader::load_tile_offsets` for loading only relevant fragments [#1976](https://github.com/TileDB-Inc/TileDB/pull/1976) [#1983](https://github.com/TileDB-Inc/TileDB/pull/1983)
* Optimize locking in `FragmentMetadata::load_tile_offsets` and `FragmentMetadata::load_tile_var_offsets` [#1979](https://github.com/TileDB-Inc/TileDB/pull/1979)
* Exit early in `Reader::copy_coordinates`/`Reader::copy_attribute_values` when no results [#1984](https://github.com/TileDB-Inc/TileDB/pull/1984)

## Bug fixes

* Fix segfault in optimized `compute_results_sparse<char>` [#1969](https://github.com/TileDB-Inc/TileDB/pull/1969)
* Fix GCS "Error:: Read object failed"[#1966](https://github.com/TileDB-Inc/TileDB/pull/1966)
* Fix segfault in `ResultTile::str_coords_intersects` [#1981](https://github.com/TileDB-Inc/TileDB/pull/1981)

# TileDB v2.1.4 Release Notes

## Improvements

* Optimize `ResultTile::compute_results_sparse<char>` resulting in significant performance increases in certain cases with string dimensions [#1963](https://github.com/TileDB-Inc/TileDB/pull/1963)

# TileDB v2.1.3 Release Notes

## Improvements

* Optimized string dimension performance. [#1922](https://github.com/TileDB-Inc/TileDB/pull/1922)

## Bug fixes

* Updated the AWS SDK to v1.8.84 to fix an uncaught exception when using S3 [#1899](https://github.com/TileDB-Inc/TileDB/pull/1899)[TileDB-Py #409](https://github.com/TileDB-Inc/TileDB-Py/issues/409)
* Fixed bug where a read on a sparse array may return duplicate values. [#1905](https://github.com/TileDB-Inc/TileDB/pull/1905)
* Fixed bug where an array could not be opened if created with an array schema from an older version [#1889](https://github.com/TileDB-Inc/TileDB/pull/1889)
* Fix compilation of TileDB Tools [#1926](https://github.com/TileDB-Inc/TileDB/pull/1926)

# TileDB v2.1.2 Release Notes

## Bug fixes

* Fix ArraySchema not write protecting fill values for only schema version 6 or newer [#1868](https://github.com/TileDB-Inc/TileDB/pull/1868)
* Fix segfault that may occur in the VFS read-ahead cache [#1871](https://github.com/TileDB-Inc/TileDB/pull/1871)

# TileDB v2.1.1 Release Notes

## Bug fixes

* The result size estimatation routines will no longer return non-zero sizes that can not contain a single value. [#1849](https://github.com/TileDB-Inc/TileDB/pull/1849)
* Fix serialization of dense writes that use ranges [#1860](https://github.com/TileDB-Inc/TileDB/pull/1860)
* Fixed a crash from an unhandled SIGPIPE signal that may raise when using S3 [#1856](https://github.com/TileDB-Inc/TileDB/pull/1856)

# TileDB v2.1.0 Release Notes

## Breaking behavior

* Empty dense arrays now return cells with fill values. Also the result estimator is adjusted to work properly with this new behavior.

## New features

* Added configuration option "sm.compute_concurrency_level" [#1766](https://github.com/TileDB-Inc/TileDB/pull/1766)
* Added configuration option "sm.io_concurrency_level" [#1766](https://github.com/TileDB-Inc/TileDB/pull/1766)
* Added configuration option "sm.sub_partitioner_memory_budget" [#1729](https://github.com/TileDB-Inc/TileDB/pull/1729)
* Added configuration option "vfs.read_ahead_size" [#1785](https://github.com/TileDB-Inc/TileDB/pull/1785)
* Added configuration option "vfs.read_ahead_cache_size" [#1785](https://github.com/TileDB-Inc/TileDB/pull/1785)
* Added support for getting/setting Apache Arrow buffers from a Query [#1816](https://github.com/TileDB-Inc/TileDB/pull/1816)

## Improvements

* Source built curl only need HTTP support [#1712](https://github.com/TileDB-Inc/TileDB/pull/1712)
* AWS SDK version bumped to 1.8.6 [#1718](https://github.com/TileDB-Inc/TileDB/pull/1718)
* Split posix permissions into files and folers permissions [#1719](https://github.com/TileDB-Inc/TileDB/pull/1719)
* Support seeking for CURL to allow redirects for posting to REST [#1728](https://github.com/TileDB-Inc/TileDB/pull/1728)
* Changed default setting for `vfs.s3.proxy_scheme` from `https` to `http` to match common usage needs [#1759](https://github.com/TileDB-Inc/TileDB/pull/1759)
* Enabled parallelization with native system threads when TBB is disabled [#1760](https://github.com/TileDB-Inc/TileDB/pull/1760)
* Subarray ranges will be automatically coalesced as they are added [#1755](https://github.com/TileDB-Inc/TileDB/pull/1755)
* Update GCS SDK to v1.16.0 to fixes multiple bugs reported [#1768](https://github.com/TileDB-Inc/TileDB/pull/1768)
* Read-ahead cache for cloud-storage backends [#1785](https://github.com/TileDB-Inc/TileDB/pull/1785)
* Allow multiple empty values at the end of a variable-length write [#1805](https://github.com/TileDB-Inc/TileDB/pull/1805)
* Build system will raise overridable error if important paths contain regex character [#1808](https://github.com/TileDB-Inc/TileDB/pull/1808)
* Lazily create AWS ClientConfiguration to avoid slow context creations for non S3 usage after the AWS SDK version bump [#1821](https://github.com/TileDB-Inc/TileDB/pull/1821)
* Moved `Status`, `ThreadPool`, and `Logger` classes from folder `tiledb/sm` to `tiledb/common` [#1843](https://github.com/TileDB-Inc/TileDB/pull/1843)

## Deprecations

* Deprecated config option "sm.num_async_threads" [#1766](https://github.com/TileDB-Inc/TileDB/pull/1766)
* Deprecated config option "sm.num_reader_threads" [#1766](https://github.com/TileDB-Inc/TileDB/pull/1766)
* Deprecated config option "sm.num_writer_threads" [#1766](https://github.com/TileDB-Inc/TileDB/pull/1766)
* Deprecated config option "vfs.num_threads" [#1766](https://github.com/TileDB-Inc/TileDB/pull/1766)
* Support for MacOS older than 10.13 is being dropped when using the AWS SDK. Prebuilt Binaries now target 10.13 [#1753](https://github.com/TileDB-Inc/TileDB/pull/1753)
* Use of Intel's Thread Building Blocks (TBB) will be discontinued in the future. It is now disabled by default [#1762](https://github.com/TileDB-Inc/TileDB/pull/1762)
* No longer building release artifacts with Intel's Thread Building Blocks (TBB) enabled [#1825](https://github.com/TileDB-Inc/TileDB/pull/1825)

## Bug fixes

* Fixed bug in setting a fill value for var-sized attributes.
* Fixed a bug where the cpp headers would always produce compile-time warnings about using the deprecated c-api "tiledb_coords()" [#1765](https://github.com/TileDB-Inc/TileDB/pull/1765)
* Only serialize the Array URI in the array schema client side. [#1806](https://github.com/TileDB-Inc/TileDB/pull/1806)
* Fix C++ api `consolidate_metadata` function uses incorrect config [#1841](https://github.com/TileDB-Inc/TileDB/pull/1841) [#1844](https://github.com/TileDB-Inc/TileDB/pull/1844)

## API additions

### C API

* Added functions `tiledb_attribute_{set,get}_fill_value` to get/set default fill values

### C++ API

* Added functions `Attribute::{set,get}_fill_value` to get/set default fill values

# TileDB v2.0.8 Release Notes

## Improvements

* Lazy initialization for GCS backend [#1752](https://github.com/TileDB-Inc/TileDB/pull/1752)
* Add additional release artifacts which include disabling TBB [#1753](https://github.com/TileDB-Inc/TileDB/pull/1753)

## Bug fixes

* Fix crash during GCS backend initialization due to upstream bug. [#1752](https://github.com/TileDB-Inc/TileDB/pull/1752)

# TileDB v2.0.7 Release Notes

## Improvements

* Various performance optimizations in the read path. [#1689](https://github.com/TileDB-Inc/TileDB/pull/1689) [#1692](https://github.com/TileDB-Inc/TileDB/pull/1692) [#1693](https://github.com/TileDB-Inc/TileDB/pull/1693) [#1694](https://github.com/TileDB-Inc/TileDB/pull/1694) [#1695](https://github.com/TileDB-Inc/TileDB/pull/1695)
* Google Cloud SDK bumped to 1.14. [#1687](https://github.com/TileDB-Inc/TileDB/pull/1687), [#1742](https://github.com/TileDB-Inc/TileDB/pull/1742)

## Bug fixes

* Fixed error "Error: Out of bounds read to internal chunk buffer of size 65536" that may occur when writing var-sized attributes. [#1732](https://github.com/TileDB-Inc/TileDB/pull/1732)
* Fixed error "Error: Chunk read error; chunk unallocated error" that may occur when reading arrays with more than one dimension. [#1736](https://github.com/TileDB-Inc/TileDB/pull/1736)
* Fix Catch2 detection of system install [#1733](https://github.com/TileDB-Inc/TileDB/pull/1733)
* Use libtiledb-detected certificate path for Google Cloud Storage, for better linux binary/wheel portability. [#1741](https://github.com/TileDB-Inc/TileDB/pull/1741)
* Fixed a small memory leak when opening arrays. [#1690](https://github.com/TileDB-Inc/TileDB/pull/1690)
* Fixed an overflow in the partioning path that may result in a hang or poor read performance. [#1725](https://github.com/TileDB-Inc/TileDB/pull/1725)[#1707](https://github.com/TileDB-Inc/TileDB/pull/1707)
* Fix compilation on gcc 10.1 for blosc [#1740](https://github.com/TileDB-Inc/TileDB/pull/1740)
* Fixed a rare hang in the usage of `load_tile_var_sizes`. [#1748](https://github.com/TileDB-Inc/TileDB/pull/1748)


# TileDB v2.0.6 Release Notes

## Improvements

* Add new config option `vfs.file.posix_permissions`. [#1710](https://github.com/TileDB-Inc/TileDB/pull/1710)

## Bug fixes

* Return possible env config variables in config iter [#1714](https://github.com/TileDB-Inc/TileDB/pull/1714)

# TileDB v2.0.5 Release Notes

## Improvements

* Don't include curl's linking to libz, avoids build issue with double libz linkage [#1682](https://github.com/TileDB-Inc/TileDB/pull/1682)

# TileDB v2.0.4 Release Notes

## Improvements

* Fix typo in GCS cmake file for superbuild [#1665](https://github.com/TileDB-Inc/TileDB/pull/1665)
* Don't error on GCS client init failure [#1667](https://github.com/TileDB-Inc/TileDB/pull/1667)
* Don't include curl's linking to ssl, avoids build issue on fresh macos 10.14/10.15 installs [#1671](https://github.com/TileDB-Inc/TileDB/pull/1671)
* Handle ubuntu's cap'n proto package not providing cmake targets [#1659](https://github.com/TileDB-Inc/TileDB/pull/1659)

## Bug fixes

* The C++ Attribute::create API now correctly builds from an STL array [#1670](https://github.com/TileDB-Inc/TileDB/pull/1670)
* Allow opening arrays with read-only permission on posix filesystems [#1676](https://github.com/TileDB-Inc/TileDB/pull/1676)
* Fixed build issue caused by passing std::string to an Aws method [#1678](https://github.com/TileDB-Inc/TileDB/pull/1678)

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
* Added function `tiledb_query_get_array`


### C++ API

* Added functions `Query::{query_layout, add_range, range, range_num, array}`.
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
