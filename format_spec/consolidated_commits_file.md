---
title: Consolidated Commits File
---

A consolidated commits file has name `<timestamped_name>.con` and is located here:

```
my_array                              # array folder
   |_ ....
   |_ __commits                       # array commits folder
         |_ <timestamped_name>.con    # consolidated commits file
         |_ ...
```

`<timestamped_name>` has format `__t1_t2_uuid_v`, where:

* `t1` and `t2` are timestamps in milliseconds elapsed since 1970-01-01 00:00:00 +0000 (UTC)
* `uuid` is a unique identifier
* `v` is the format version

There may be multiple such files in the array commits folder. Each consolidated commits file combines a list of fragments commits, delete or update commits.
| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Commit 1 | `uint8_t[]` | Commit 1 |
| … | … | … |
| Commit N | `uint8_t[]` | Commit N |

For fragment commits, the URIs is written delimited by a new line character:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| URI  followed by a new line character | `uint8_t[]` | URI |

For delete or update commits, the URIs is written delimited by a new line character and then followed by the delete/update condition [tile](./tile.md), preceded by its size:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| URI  followed by a new line character | `uint8_t[]` | URI |
| Delete/update condition size | `uint64_t` | Delete/update condition size |
| Delete/update condition tile | `uint8_t[]` | Delete/update condition tile |
