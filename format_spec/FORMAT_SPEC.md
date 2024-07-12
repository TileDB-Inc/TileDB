---
title: Format Specification
---

**Notes:**

* The current TileDB format version number is **22** (`uint32_t`).
* Data written by TileDB and referenced in this document is **little-endian**
  with the following exceptions:

  - [Dictionary filter](filters/dictionary_encoding.md)
  - RLE filter

## History

|Format version|TileDB version|Description|
|-|-|-|
|1|1.4|[Decouple format and library version](https://github.com/TileDB-Inc/TileDB/commit/610f087515b6de5c3290b09dab30c6943ec77feb)|
|2|1.5|[Always split coordinate tiles](https://github.com/TileDB-Inc/TileDB/commit/9394b38bdfbacd606d673896b4ae87e7968b7c2f)|
|3|1.6|[Parallelize fragment metadata loading](https://github.com/TileDB-Inc/TileDB/commit/a2eb6237e622c3a17691dbe04c9223ba099f7466)|
|4|1.7|[Remove KV storage](https://github.com/TileDB-Inc/TileDB/commit/e733f7baa85a41e25e5834a220234397d6038401)|
|5|2.0|[Split coordinates into individual files](https://github.com/TileDB-Inc/TileDB/commit/d3543bdbc4ee7c2ed1f2de8cee42b04c6ec8eafc)|
|6|2.1|[Implement attribute fill values](https://github.com/TileDB-Inc/TileDB/commit/eaafa47c97af0ee654a0ca2e97da7b8d941e672b)|
|7|2.2|[Nullable attribute support](https://github.com/TileDB-Inc/TileDB/commit/a7fd8d6dd74bb4fa1ae25a6f995da93812f92c20)|
|8|2.3|[Percent encode attribute/dimension file names](https://github.com/TileDB-Inc/TileDB/commit/97c5c4b0aa35cfd96197558ffc1189860b4adc6f)|
|9|2.3|[Name attribute/dimension files by index](https://github.com/TileDB-Inc/TileDB/commit/9a2ed1c22242f097300c2909baf6cb671a7ee33e)|
|10|2.4|[Added array schema evolution](https://github.com/TileDB-Inc/TileDB/commit/41e5e8f4b185f49777560d637b1d61de498364ce)|
|11|2.7|[Store integral cells, aka, don't split cells across chunks](https://github.com/TileDB-Inc/TileDB/commit/beab5113526b7156c8c6492542f1681555c8ae87)|
|12|2.8|[New array directory structure](https://github.com/TileDB-Inc/TileDB/commit/ce204ad1ea5b40f006f4a6ddf240d89c08b3235b)|
|13|2.9|[Add dictionary filter](https://github.com/TileDB-Inc/TileDB/commit/5637e8c678451c9d2356ccada118b504c8ca85f0)|
|14|2.10|[Consolidation with timestamps, add has_timestamps to footer](https://github.com/TileDB-Inc/TileDB/commit/31a3dce8db254efc36f6d28249febed41bba3bcd)|
|15|2.11|[Remove consolidate with timestamps config](https://github.com/TileDB-Inc/TileDB/commit/6b49739e79d804dc56eb0a7e422823ae6f002276)|
|16|2.12|[Implement delete strategy](https://github.com/TileDB-Inc/TileDB/commit/8d64b1f38177113379fa741016136dbd2b06fcfd)|
|17|2.14|[Add dimension labels and data order](https://github.com/TileDB-Inc/TileDB/commit/bb433fcf12dc74a38c7e843808ec1e593b16ce71)|
|18|2.15|[Dimension Labels no longer experimental](https://github.com/TileDB-Inc/TileDB/commit/c3a1bb47e7237f50e8ed9e33abfaa3161e23ff64)|
|19|2.16|[Vac files now use relative URIs](https://github.com/TileDB-Inc/TileDB/commit/ef3236a526b67c50138436a16f67ad274c2ca037)|
|20|2.17|[Enumerations](https://github.com/TileDB-Inc/TileDB/commit/c0d7c6a50fdeffbcc7d8c9ba4a29230fe22baed6)|
|21|2.19|[Tile metadata are now correctly calculated for nullable fixed size strings on dense arrays](https://github.com/TileDB-Inc/TileDB/commit/081bcc5f7ce4bee576f08b97de348236ac88d429)|
|22|2.25|[Add array current domain](https://github.com/TileDB-Inc/TileDB/commit/9116d3c95a83d72545520acb9a7808fc63478963)|

## Table of Contents

* **Array**
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
   * [Vacuum Pipeline](./vacuum_file.md)
