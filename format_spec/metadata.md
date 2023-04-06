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

`<timestamped_name>` has format `__t1_t2_uuid_v`, where:

* `t1` and `t2` are timestamps in milliseconds elapsed since 1970-01-01 00:00:00 +0000 (UTC)
* `uuid` is a unique identifier
* `v` is the format version

The metadata folder can contain:
* Any number of [metadata files](#array-metadata-file)
* Any number of [vacuum files](./vacuum_file.md)

## Metadata File

The metadata file consists of a single [generic tile](./generic_tile.md), containing multiple entries with the following data:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Key length | `uint32_t` | The length of the key. |
| Key | `uint8_t[]` | The key. |
| Deletion | `uint8_t` | `1`/`0` if it is a deletion/insertion. |
| Value type | `uint8_t` | The value data type. Present only if `del` is `0`. |
| Number of values | `uint32_t` | The number of values. Present only if `del` is `0`. |
| Value | `uint8_t[]` | The value. Present only if `del` is `0`. |
