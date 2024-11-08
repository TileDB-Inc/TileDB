---
title: Filter Pipeline
---

## Main Structure

The filter pipeline has internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Max chunk size | `uint32_t` | Maximum chunk size within a tile |
| Num filters | `uint32_t` | Number of filters in pipeline |
| Filter 1 | [Filter](#filter) | First filter |
| … | … | … |
| Filter N | [Filter](#filter) | Nth filter |

For var size data, the filter pipeline tries to fit integral cells in a chunk. It uses the following heuristic if the cell doesn't fit:

* If the chunk is not yet at 50% capacity, add the cell to the current chunk.
* If the chunk is over 50% capacity and adding the cell would make it less than 150% of the maximum chunk size, add it to this chunk.
* Else, start a new chunk.

## Filter

The filter has internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Filter type | `uint8_t` | Type of filter \(e.g. `TILEDB_FILTER_BZIP2`\) – see below for values |
| Filter options size | `uint32_t` | Number of bytes in filter options – may be 0. |
| Filter options | [Filter Options](#filter-options) | Filter options, specific to each filter. E.g. compression level for compression filters. |

TileDB supports the following filters. Each filter has a type, which also corresponds to a constant in the C API:
* [Compression](./tile.md#compression-filters)
    * [Gzip](https://www.gnu.org/software/gzip/) – `TILEDB_FILTER_GZIP` (1)
    * [Zstandard](https://facebook.github.io/zstd/) – `TILEDB_FILTER_ZSTD` (2)
    * [LZ4](https://lz4.org/) – `TILEDB_FILTER_LZ4` (3)
    * Run-length encoding – `TILEDB_FILTER_RLE` (4)
    * [BZip2](http://sourceware.org/bzip2/) – `TILEDB_FILTER_BZIP2` (5)
    * [Double-delta](filters/double_delta.md) – `TILEDB_FILTER_DOUBLE_DELTA` (6)
    * [Dictionary](filters/dictionary_encoding.md) – `TILEDB_FILTER_DICTIONARY` (14)
    * [Delta](filters/delta.md) – `TILEDB_FILTER_DELTA` (19)
* [Bit-width Reduction](./tile.md#bit-width-reduction-filter) – `TILEDB_FILTER_BIT_WIDTH_REDUCTION` (7)
* [Bit Shuffle](./tile.md#byteshuffle-filter) – `TILEDB_FILTER_BITSHUFFLE` (8)
* [Byte Shuffle](./tile.md#bitshuffle-filter) – `TILEDB_FILTER_BYTESHUFFLE` (9)
* [Positive Delta](./tile.md#positive-delta-encoding-filter) – `TILEDB_FILTER_POSITIVE_DELTA` (10)
* [Encryption](./tile.md#encryption-filters)
    * [AES-256-GCM](https://en.wikipedia.org/wiki/Galois/Counter_Mode)
* [Checksum](./tile.md#checksum-filters)
    * [MD5](https://en.wikipedia.org/wiki/MD5) – `TILEDB_FILTER_CHECKSUM_MD5` (12)
    * [SHA-256](https://en.wikipedia.org/wiki/SHA-2) – `TILEDB_FILTER_CHECKSUM_SHA256` (13)
* [Dictionary Encoding](./filters/dictionary_encoding.md) – `TILEDB_FILTER_DICTIONARY` (14)
* [Float Scale](./filters/float_scale.md) – `TILEDB_FILTER_SCALE_FLOAT` (15)
* [XOR](./filters/xor.md) – `TILEDB_FILTER_XOR` (16)
* [WEBP](./filters/webp.md) – `TILEDB_FILTER_WEBP` (18)

> [!NOTE]
> Encryption is implemented as a filter at the end of the pipeline, but the encryption filter is not directly selectable by the user, and does not appear in filter pipelines serialized to storage.

## Filter Options

The filter options are configuration parameters for the filters that do not change once the array schema has been created.

### Main Compressor Options

For the main compression filters \(any of the filter types `TILEDB_FILTER_{GZIP,ZSTD,LZ4,RLE,BZIP2,DICTIONARY}`\) the filter options have internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Compressor type | `uint8_t` | Type of compression \(e.g. `TILEDB_FILTER_BZIP2`\) |
| Compression level | `int32_t` | Compression level used \(ignored by some compressors\). |

### Delta Compressor Options

For the `TILEDB_FILTER_DELTA` and `TILEDB_FILTER_DOUBLE_DELTA` compression filters the filter options have internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Compressor type | `uint8_t` | Type of compression \(e.g. `TILEDB_FILTER_DELTA`\) |
| Compression level | `int32_t` | Ignored |
| Reinterpret datatype | `uint8_t` | Type to reinterpret data prior to compression. |

> [!NOTE]
> Prior to version 20, the _Reinterpret datatype_ field was not present for the double delta filter. Also prior to version 19, the same field was not present for the delta filter.

### Bit-width Reduction Options

The filter options for `TILEDB_FILTER_BIT_WIDTH_REDUCTION` has internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Max window size | `uint32_t` | Maximum window size in bytes |

### Float Scale Filter Reduction Options

The filter options for `TILEDB_FILTER_SCALE_FLOAT` has internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Scale | `double` | Scale parameter used for float scaling filter conversion |
| Offset | `double` | Offset parameter used for float scaling filter conversion |
| Byte width | `uint64_t` | Width of the stored integer data in bytes |

### Positive Delta Options

The filter options for `TILEDB_FILTER_POSITIVE_DELTA` has internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Max window size | `uint32_t` | Maximum window size in bytes |

### Other Filter Options

The remaining filters \(`TILEDB_FILTER_{BITSHUFFLE,BYTESHUFFLE,CHECKSUM_MD5,CHECKSUM_256,XOR}`\) do not serialize any options.
