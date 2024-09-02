---
title: Double Delta Filter
---

## Double Delta Filter

The double delta filter compresses integer type data by first computing the delta between consecutive elements, then the delta between the deltas, and bit-packing the result.

### Filter Enum Value

The filter enum value for the double delta filter is `6` (`TILEDB_FILTER_DOUBLE_DELTA` enum).

### Input and Output Layout

The input data layout will be an array of integer numbers (each known as `in_{n}`, with `n` starting from 0). Their type (henceforth known as `input_t`) is inferred from the output type of the previous filter, or the tile's datatype if this is the first filter in the pipeline, but can be overriden by the [_Reinterpret datatype_ field](../filter_pipeline.md#delta-compressor-options) in the filter options.

The output data layout consists of the following fields:

|Field|Type|Description|
|:---|:---|:---|
|`bitsize`|`uint8_t`|Minimum number of bits required to represent any `dd_n` value.|
|`n`|`uint64_t`|Number of values in the input data.|
|`in_0`|`input_t`|First input value.|
|`in_1`|`input_t`|Second input value.|
|`sign_2`|`bit`|Sign of `(in_2 - in_1) - (in_1 - in_0)`.|
|`dd_2`|`bit[bitsize]`|Absolute value of `(in_2 - in_1) - (in_1 - in_0)`.|
|…|…|…|
|`sign_n`|`bit`|Sign of `(in_n - in_{n - 1}) - (in_{n - 1} - in_{n - 2})`.|
|`dd_n`|`bit[bitsize]`|Absolute value of `(in_n - in_{n - 1}) - (in_{n - 1} - in_{n - 2})`.|
|`pad`|`bit[((n - 2) * (bitsize + 1)) % 64]`|Padding to the next 64-bit boundary.|

If `bitsize` was computed as equal to `sizeof(input_t) * 8 - 1` (i.e. double delta compression would not have yielded any size savings), double delta compression is not applied and the input data will be added to the output stream unchanged, after `bitsize` and `n`, which are always written.
