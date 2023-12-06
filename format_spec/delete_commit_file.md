---
title: Delete Commit File
---

A delete commit file has name [`<timestamped_name>`](./timestamped_name.md)`.del` and is located here:

```
my_array                              # array folder
   |_ ....
   |_ __commits                       # array commits folder
         |_ <timestamped_name>.del    # delete commit file
         |_ ...
```

There may be multiple such files in the array commits folder. Each delete commit file contains a [tile](./tile.md) with a serialized delete condition, which is a tree of nodes. Each node can be a value node or expression node. Expression nodes have the following on disk format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Node type | `uint8_t` | 0 for expression node |
| Combination op | `uint8_t` | AND(0), OR(1), NOT(2) |
| Num children | `uint64_t` | Number of child nodes |
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
