---
title: Generic Tile
---

The generic tile is a [tile](./tile.md) prepended with some extra header data, so that it can be used stand-alone without requiring extra information from the array schema. A generic tile has the following on-disk format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Version number | `uint32_t` | Format version number of the generic tile |
| Persisted size | `uint64_t` | Persisted \(e.g. compressed\) size of the tile |
| Tile size | `uint64_t` | In-memory \(e.g. uncompressed\) size of the tile |
| Datatype | `uint8_t` | Datatype of the tile |
| Cell size | `uint64_t` | Cell size of the tile |
| Encryption type | `uint8_t` | Type of encryption used in filtering the tile |
| Filter pipeline size | `uint32_t` | Number of bytes in the serialized filter pipeline |
| Filter pipeline | [Filter Pipeline](./filter_pipeline.md) | Filter pipeline used to filter the tile |
| Tile data | [Tile](./tile.md) | The serialized tile data |
