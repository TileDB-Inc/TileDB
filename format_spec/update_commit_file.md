---
title: Update Commit File
---

A update commit file has name `<timestamped_name>.upd` and is located here:

```
my_array                              # array folder
   |_ ....
   |_ __commits                       # array commits folder
         |_ <timestamped_name>.upd    # update commit file
         |_ ...
```

`<timestamped_name>` has format `__t1_t2_uuid_v`, where:

* `t1` and `t2` are timestamps in milliseconds elapsed since 1970-01-01 00:00:00 +0000 (UTC)
* `uuid` is a unique identifier
* `v` is the format version

There may be multiple such files in the array commits folder. Each update commit file contains a [tile](./tile.md) with a serialized update condition, which is a tree of nodes followed by update values, which is a list of values. Each node for the condition can be a value node or expression node. Expression nodes have the following on disk format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Node type | `uint8_t` | 0 for expression node |
| Combination op | `uint8_t` | AND(0), OR(1), NOT(2) |
| Num children | `uint64_t[]` | Number of child nodes |
| Children 1 | `NODE` | children 1 |
| … | … | … |
| Children N | `NODE` | Children N |

Value nodes have the following on disk format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Node type | `uint8_t` | 1 for value node |
| Op | `uint8_t` | LT(0), LE(1), GT(2), GE(3), EQ(4), NE(5) |
| Field name size | `uint32_t` | Size of the field name |
| Field name value | `uint8_t[]` | Field name value |
| Value size | `uint64_t` | Value size |
| Value content | `uint8_t[]` | Value |

Update values are serialized in the following format:
| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Number of values | `uint64_t` | Number of values N |
| Field name size 1 | `uint64_t` | Size of the field name for the first update value |
| Field name value 1 | `uint8_t[]` | Field name value for the first update value |
| Value size 1 | `uint64_t` | Value size for first update value |
| Value content 1 | `uint8_t[]` | Value for first update value |
| … | … | … |
| Field name size N | `uint64_t` | Size of the field name for update value N |
| Field name value N | `uint8_t[]` | Field name value for update value N |
| Value size N | `uint64_t` | Value size for update value N |
| Value content N | `uint8_t[]` | Value for update value N |