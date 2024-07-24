---
title: Format Specification
---

**Notes:**

* The current TileDB format version number is **22** (`uint32_t`).
* Data written by TileDB and referenced in this document is **little-endian**
  with the following exceptions:

  - [Dictionary filter](filters/dictionary_encoding.md)
  - RLE filter

## Table of Contents

* **Array**
   * [Format Version History](./history.md)
   * [File hierarchy](./array_file_hierarchy.md)
   * [Array Schema](./array_schema.md)
   * [Fragment](./fragment.md)
   * [Array Metadata](./metadata.md)
   * [Tile](./tile.md)
   * [Generic Tile](./generic_tile.md)
* **Group**
   * [File hierarchy](./group_file_hierarchy.md)
* **Other**
   * [Consolidated Fragment Metadata File](./consolidated_fragment_metadata_file.md)
   * [Filter Pipeline](./filter_pipeline.md)
   * [Timestamped Name](./timestamped_name.md)
   * [Vacuum File](./vacuum_file.md)
