---
title: WEBP Filter
---

The WebP filter compresses image data using [libwebp](https://developers.google.com/speed/webp/docs/api#headers_and_libraries). The current input formats supported are `RGB`, `RGBA`, `BGR`, and `BGRA`. The colorspace format can be configured for this filter by setting the `TILEDB_WEBP_INPUT_FORMAT` filter option. Tile-based compression is determined by dimension extents. Extents equal to dimension bounds will compress the image in one pass.


```C++
Domain domain(ctx);
// We use `width * 4` for X dimension to allow for RGBA (4) elements per-pixel 
// Extents must be divisible by pixel depth of colorspace format
// RGB / BGR is 3; RGBA / BGRA is 4, etc
domain.add_dimension(Dimension::create<unsigned>(ctx, "y", {{1, (height)}}, height/2))
    .add_dimension(Dimension::create<unsigned>(ctx, "x", {{1, (width)*4}}, (width/2)*4));
// To compress using webp we need RGBA in a single uint8_t buffer
ArraySchema schema(ctx, TILEDB_DENSE);
Attribute rgba = Attribute::create<uint8_t>(ctx, "rgba");

// Create WebP filter and set options
Filter webp(ctx, TILEDB_FILTER_WEBP);

// TILEDB_WEBP_INPUT_FORMAT is TILEDB_WEBP_NONE by default
// One of TILEDB_WEBP_{RGB, BGR, RGBA, BGRA}
// Caller should always set this option based on colorspace preference
auto fmt = TILEDB_WEBP_RGBA;
webp.set_option(TILEDB_WEBP_INPUT_FORMAT, &fmt);

// TILEDB_WEBP_QUALITY is 100.0f by default
// Floats within the range [0, 100] are valid for this option
// Lossy compression with quality of 100.0 is not lossless
float quality = 50.0f;
webp.set_option(TILEDB_WEBP_QUALITY, &quality);

// TILEDB_WEBP_LOSSLESS is 0 by default
// This option is either enabled (1) or disabled (0)
// Enable this option for lossless compression; quality will be ignored
uint8_t lossless = 0;
webp.set_option(TILEDB_WEBP_LOSSLESS, &quality);

// Add to FilterList and set attribute filters
FilterList filterList(ctx);
filterList.add_filter(webp);
rgba.set_filter_list(filterList);
```

# Filter Enum Value

The filter enum value for the WEBP filter is `17` (TILEDB_FILTER_WEBP enum).
