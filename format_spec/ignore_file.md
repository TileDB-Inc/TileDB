---
title: Ignore File
---

A ignore file has name `__t1_t2_uuid_v.ign` and is located in the array commit folder:

```
my_array                           # array folder
   |_ ....
   |_ __commits                    # array commit folder
         |___t1_t2_uuid_v.ign      # ignore file
```

or in the array metadata folder:

In the file name:

* `t1` and `t2` are timestamps in milliseconds elapsed since 1970-01-01 00:00:00 +0000 (UTC)
* `uuid` is a unique identifier
* `v` is the format version

The ignore file is a simple text file where each line contains a URI string. The URI is the relative URI based on the top level array URI.

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| URI 1 followed by a new line character | `uint8_t[]` | URI 1 to be ignored |
| … | … | … |
| URI N followed by a new line character | `uint8_t[]` | URI N to be ignored |
