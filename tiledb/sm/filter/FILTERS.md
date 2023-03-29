# Directory

This directory contains the implementations of all TileDB filters, including compressors which are a subclass of filter.

# Filter

## Filter Buffer

A `FilterBuffer` wraps one `Buffer` instance, which is ultimately a pointer to some memory block (either owned by the `Buffer`, or externally allocated).

(TBD: is the multiple part support actually used?)

## Filters

Filters operate on [`FilterBuffer`]() instances: one each for input and output representing the data and metadata for the filter. Each filter operation appends metadata which is stored in the tile header and read back in reverse to un-filter the data.



A filter has the responsibility of taking one input and output `FilterBuffer`
The filter API has two entrypoints: `run_forward` and `run_reverse`.