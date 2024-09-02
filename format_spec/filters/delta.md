---
title: Delta Filter
---

## Delta Filter

The delta filter transforms integer type data by computing and storing the delta between consecutive elements.

### Filter Enum Value

The filter enum value for the delta filter is `19` (`TILEDB_FILTER_DELTA` enum).

### Input and Output Layout

The input data layout will be an array of integer numbers (each known as `in_{n}`, with `n` starting from 0). Their type (henceforth known as `input_t`) is inferred from the output type of the previous filter, or the tile's datatype if this is the first filter in the pipeline, but can be overriden by the [_Reinterpret datatype_ field](../filter_pipeline.md#delta-compressor-options) in the filter options.

The output data layout consists of the following fields:

|Field|Type|Description|
|:---|:---|:---|
|`n`|`uint64_t`|Number of values in the input data.|
|`in_0`|`input_t`|First input value.|
|`delta_1`|`input_t`|Value of `in_1 - in_0`.|
|`delta_n`|`input_t`|Value of `in_n - in_{n - 1}`.|
