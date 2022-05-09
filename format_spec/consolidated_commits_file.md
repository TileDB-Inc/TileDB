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

There may be multiple such files in the array commits folder. Each consolidated commits file combines a list of fragments commits URIs delimited by a new line character. The URI is the relative URI based on the top level array URI.

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| URI 1 followed by a new line character | `uint8_t[]` | URI 1 |
| … | … | … |
| URI N followed by a new line character | `uint8_t[]` | URI N |
