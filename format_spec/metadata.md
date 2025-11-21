---
title: Array Metadata
---

## Main Structure

The metadata is a folder called `__meta` located here:

```
my_array                            # array folder
   |  ...
   |_ __meta                        # metadata folder
         |_ <timestamped_name>      # metadata file
         |_ ...
         |_ <timestamped_name>.vac  # vacuum file
         |_ ...
```

The metadata folder can contain any number of [timestamped](./timestamped_name.md):
* [metadata files](#metadata-file)
* [vacuum files](./vacuum_file.md)
* **Note**: the timestamped names do _not_ include the format version.

## Metadata File

Metadata files with the `.tmp` extension are used for local filesystems only, indicating the metadata is still being flushed to disk.

The metadata file consists of a single [generic tile](./generic_tile.md), containing multiple entries with the following data:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Key length | `uint32_t` | The length of the key. |
| Key | `uint8_t[]` | The key. |
| Deletion | `uint8_t` | `1`/`0` if it is a deletion/insertion. |
| Value type | `uint8_t` | The value data type. Present only if `del` is `0`. |
| Number of values | `uint32_t` | The number of values. Present only if `del` is `0`. |
| Value | `uint8_t[]` | The value. Present only if `del` is `0`. |
