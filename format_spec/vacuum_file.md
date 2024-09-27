---
title: Vacuum File
---

A vacuum file has name `[`<timestamped_name>`](./timestamped_name.md)`.vac` and can be located either in the array commit folder:

```
my_array                        # array folder
   |_ ....
   |_ __commits                 # array commit folder
         |_ <timestamped_name>.vac   # vacuum file
```

or in the array or group metadata folder:

```
my_obj                         # array/group folder
   |  ...
   | __meta                    # array metadata folder
         | ...
         | <timestamped_name>.vac  # vacuum file
         | ...
```

When located in the commits folder, it will include the name of fragments (in the `__fragments` folder) that can be vacuumed. When located in the metadata folder, it will include the URI or metadata files that can be vacuumed.

The vacuum file is a simple text file where each line contains a string:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Item 1 followed by a new line character | `uint8_t[]` | Item 1 to be vacuumed |
| … | … | … |
| Item N followed by a new line character | `uint8_t[]` | Item N to be vacuumed |

> [!NOTE]
> Prior to version 19, vacuum files contained absolute URIs.
