---
title: Enumerations
---

## Main Structure

```
my_array                                    # array folder
   |  ...
   |_ schema                      # ArraySchema directory named `__schema`
         |_ enumerations          # Enumeration directory named `__enumerations`
               |_ enumeration     # enumeration data with names `__uuid_v`


Enumeration data is stored in a subdirectory of the [array schema][./array_schema.md]
directory. Enumerations are stored using [Generic Tiles][./generic_tile.md].

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Version number | `uint32_t` | Enumerations version number |
| Datatype | `uint8_t` | The datatype of the enumeration values |
| Cell Val Num | `uint32_t` | The cell val num of the enumeration values |
| Ordered | `bool` | Whether the enumeration values should be considered ordered |
| Data Size | `uint64_t` | The number of bytes used to store the values |
| Data | `uint8_t` * Data Size | The data for the enumeration values |
| Offsets Size | `uint64_t` | The number of bytes used to store offsets if cell_var_num is TILEDB_VAR_NUM |
| Offsets | `uint8_t` * Offsets Size | The offsets data for the enumeration if cell_var_num is TILEDB_VAR_NUM |
