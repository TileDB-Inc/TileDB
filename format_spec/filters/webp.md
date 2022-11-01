---
title: WEBP Filter
---

The WebP filter compresses image data using [libwebp](https://developers.google.com/speed/webp/docs/api#headers_and_libraries). The current input formats supported are `RGB`, `RGBA`, `BGR`, and `BGRA`. The colorspace format can be configured for this filter by setting the `TILEDB_WEBP_INPUT_FORMAT` filter option. Tile-based compression is determined by dimension extents. Extents equal to dimension bounds will compress the image in one pass.

```
# Data from 2x3 pixel image
input = [[255,62,83, 149,43,67, 138,43,67]
         [255,62,83, 149,43,67, 138,43,67]]
# Ingest and read with lossless compression
output = [[255,62,83, 149,43,67, 138,43,67]
          [255,62,83, 149,43,67, 138,43,67]]
# Ingest and read with lossy compression
output = [[251,60,84, 148,51,61, 142,42,64]
          [252,63,85, 150,46,67, 139,44,68]]
```

# Filter Enum Value

The filter enum value for the WEBP filter is `17` (TILEDB_FILTER_WEBP enum).

# Input and Output Layout

Input is a single array of colorspace values in RGB, BGR, RGBA, or BGRA format. For RGB format we would expect the first
pixel to store R, G, B values at `input[0]`, `input[1]`, and `input[2]` respectively, followed by the remaining pixels 
in the image. Ingesting a 10x10 image would expect 300 total input values for 10 rows of 30 colorspace values per-row.

Output is organized the same as input. If we compress an image with lossless enabled, all outputs match inputs exactly. 
If we instead use lossy compression with a quality of 0, output colorspace values will vary due to data lost during
lossy WebP compression but the organization of the data remains the same.
