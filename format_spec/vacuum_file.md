---
title: Vacuum File
---

A vacuum file has name `[`<timestamped_name>`](./timestamped_name.md)`.vac` and can be located either in the array commit folder:

```
my_array                        # array folder
   | ....
   | __commits                 # array commit folder
         |<timestamped_name>.vac   # vacuum file
```

or in the array metadata folder:

```
my_array                        # array folder
   |  ...            
   | __meta                    # array metadata folder
         | ...
         | <timestamped_name>.vac  # vacuum file
         | ...
```

When located in the commits folder, it will include the URI of fragments (in the `__fragments` folder) that can be vaccumed. When located in the array metadata folder, it will include the URI or array metadata files that can be vaccumed.

The vacuum file is a simple text file where each line contains a URI string:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| URI 1 followed by a new line character | `uint8_t[]` | URI 1 to be vacuumed |
| … | … | … |
| URI N followed by a new line character | `uint8_t[]` | URI N to be vacuumed |
