---
title: WEBP Filter
---

The WebP filter compresses image data using [libwebp](https://developers.google.com/speed/webp/docs/api#headers_and_libraries). The current input formats supported are `RGB`, `RGBA`, `BGR`, and `BGRA`. The colorspace format can be configured for this filter by setting the `TILEDB_WEBP_INPUT_FORMAT` filter option


```C++
Domain domain(ctx);
// We use `width * 4` for X dimension to allow for RGBA (4) elements per-pixel 
domain.add_dimension(Dimension::create<unsigned>(ctx, "y", {{1, (height)}}, 100))
      .add_dimension(Dimension::create<unsigned>(ctx, "x", {{1, (width)*4}}, 100));

// To compress using webp we need RGBA in a single uint8_t buffer
ArraySchema schema(ctx, TILEDB_DENSE);
Attribute rgba = Attribute::create<uint8_t>(ctx, "rgba");

// Create WebP filter and set options
Filter webp(ctx, TILEDB_FILTER_WEBP);
auto fmt = TILEDB_WEBP_RGBA;
webp.set_option(TILEDB_WEBP_INPUT_FORMAT, &fmt);
float quality = 50.0f;
webp.set_option(TILEDB_WEBP_QUALITY, &quality);

// Add to FilterList and set attribute filters
FilterList filterList(ctx);
filterList.add_filter(webp);
rgba.set_filter_list(filterList);
```

# Filter Enum Value

The filter enum value for the WEBP filter is `17` (TILEDB_FILTER_WEBP enum).
