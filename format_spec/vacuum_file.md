---
title: Vacuum File
---

A vacuum file has name `__t1_t2_uuid_v.vac` and can be located either in the array commit folder:

```
my_array                        # array folder
   |_ ....
   |_ __commits                 # array commit folder
         |___t1_t2_uuid_v.vac   # vacuum file
```

or in the array metadata folder:

```
my_array                        # array folder
   |  ...            
   |_ __meta                    # array metadata folder
         |_ ...
         |_ __t1_t2_uuid_v.vac  # vacuum file
         |_ ...
```

When located in the commits folder, it will include the URI of fragments (in the __fragments folder) that can be vaccumed. When located in the array metadata folder, it will include the URI or array metadata files that can be vaccumed.

In the file name:

* `t1` and `t2` are timestamps in milliseconds elapsed since 1970-01-01 00:00:00 +0000 (UTC)
* `uuid` is a unique identifier
* `v` is the format version

The vacuum file is a simple text file where each line contains a URI string:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| URI 1 followed by a new line character | `uint8_t[]` | URI 1 to be vacuumed |
| … | … | … |
| URI N followed by a new line character | `uint8_t[]` | URI N to be vacuumed |
