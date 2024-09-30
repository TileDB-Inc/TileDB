---
title: Consolidated Fragment Metadata File
---

A consolidated fragment metadata file has name [`<timestamped_name>`](./timestamped_name.md)`.meta` and is located here:

```
my_array                              # array folder
   |_ ....
   |_ __fragment_meta                 # array fragment metadata folder
         |_ <timestamped_name>.meta   # consolidated fragment metadata file
         |_ ...
```

There may be multiple such files in the array folder. Each consolidated fragment metadata file combines the metadata footers of a set of fragments. It has the following on-disk format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Fragment 1 length | `uint64_t` | Number of bytes in the string of fragment 1 |
| Fragment 1 | `uint8_t[]` | Timestamped name of fragment 1 |
| Fragment 1 offset | `uint64_t` | The offset in the file where the fragment 1 footer begins |
| … | … | … |
| Fragment N length | `uint64_t` | Number of bytes in the string of fragment N |
| Fragment N | `uint8_t[]` | Timestamped name of fragment N |
| Fragment N offset | `uint64_t` | The offset in the file where the fragment N footer begins |
| Fragment 1 footer | [Footer](./fragment.md#footer) | Serialized footer of fragment 1 |
| … | … | … |
| Fragment N footer | [Footer](./fragment.md#footer) | Serialized footer of fragment N |

> [!NOTE]
> Prior to version 9, fragments in fragment metadata files were referenced by their absolute URIs.
