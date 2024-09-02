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
| URI 1 length | `uint64_t` | Number of bytes in the string of URI 1 |
| URI 1 | `uint8_t[]` | URI 1 |
| URI 1 offset | `uint64_t` | The offset in the file where the URI 1 footer begins |
| … | … | … |
| URI N length | `uint64_t` | Number of bytes in the string of URI N |
| URI N | `uint8_t[]` | URI N |
| URI N offset | `uint64_t` | The offset in the file where the URI N footer begins |
| URI 1 footer | [Footer](./fragment.md#footer) | Serialized footer of URI (fragment) 1 |
| … | … | … |
| URI N footer | [Footer](./fragment.md#footer) | Serialized footer of URI (fragment) N |
