---
title: Format Specification
---

**Notes:**

* The current TileDB array format version number is **23** (`uint32_t`).
  * Other structures might be versioned separately.
* Data written by TileDB and referenced in this document is **little-endian**
  with the following exceptions:

  - [Dictionary encoding filter](filters/dictionary_encoding.md)
  - RLE filter

## Table of Contents

* **Array**
   * [Format Version History](./array_format_history.md)
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
