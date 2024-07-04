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
| Filter type | `uint8_t` | Type of filter \(e.g. `TILEDB_FILTER_BZIP2`\) |
| Filter options size | `uint32_t` | Number of bytes in filter options — may be 0. |
| Filter options | [Filter Options](#filter-options) | Filter options, specific to each filter. E.g. compression level for compression filters. |

TileDB supports the following filters:
* [Float Scaling Filter](./filters/float_scale.md)
* [XOR Filter](./filters/xor.md)
* [Dictionary Encoding Filter](./filters/dictionary_encoding.md)
* [WEBP Filter](./filters/webp.md)

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
