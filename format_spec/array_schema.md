---
title: Array Schema
---

## Array Schema File

The array schema file consists of a single [generic tile](./generic_tile.md), with the following data:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Array version | `uint32_t` | Format version number of the array schema |
| Allows dups | `bool` | _New in version 5_ Whether or not the array allows duplicate cells |
| Array type | `uint8_t` | Dense or sparse |
| Tile order | `uint8_t` | Row or column major |
| Cell order | `uint8_t` | Row or column major |
| Capacity | `uint64_t` | For sparse fragments, the data tile capacity |
| Coords filters | [Filter Pipeline](./filter_pipeline.md) | The filter pipeline used as default for coordinate tiles |
| Offsets filters | [Filter Pipeline](./filter_pipeline.md) | The filter pipeline used for cell var-len offset tiles |
| Validity filters | [Filter Pipeline](./filter_pipeline.md) | _New in version 7_ The filter pipeline used for cell validity tiles |
| Domain | [Domain](#domain) | The array domain |
| Num attributes | `uint32_t` | Number of attributes in the array |
| Attribute 1 | [Attribute](#attribute) | First attribute |
| … | … | … |
| Attribute N | [Attribute](#attribute) | Nth attribute |
| Num labels | `uint32_t` | _New in version 18_ Number of dimension labels in the array |
| Label 1 | [Dimension Label](#dimension_label) | _New in version 18_ First dimension label |
| … | … | … |
| Label N | [Dimension Label](#dimension_label) | _New in version 18_ Nth dimension label |
| Num enumerations | `uint32_t` | Number of [enumerations](./enumeration.md) in the array |
| Enumeration name length 1 | `uint32_t` | _New in version 20_ The number of characters in the enumeration 1 name |
| Enumeration name 1 | `uint8_t[]` | _New in version 20_ The name of enumeration 1 |
| Enumeration filename length 1 | `uint32_t` | _New in version 20_ The number of characters in the enumeration 1 file |
| Enumeration filename 1 | `uint8_t[]` | _New in version 20_ The name of the file in the `__enumerations` subdirectory that contains enumeration 1's data |
| Enumeration name length N | `uint32_t` | _New in version 20_ The number of characters in the enumeration N name |
| Enumeration name N | `uint8_t[]` | _New in version 20_ The name of enumeration N |
| Enumeration filename length N | `uint32_t` | _New in version 20_ The number of characters in the enumeration N file |
| Enumeration filename N | `uint8_t[]` | _New in version 20_ The name of the file in the `__enumerations` subdirectory that contains enumeration N's data |
| Current domain | [Current Domain](#current-domain) | _New in version 22_ The array's current domain |

## Domain

The domain has internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Domain datatype | `uint8_t` | _Removed in version 5_ Datatype of all dimensions |
| Num dimensions | `uint32_t` | Dimensionality/rank of the domain |
| Dimension 1 | [Dimension](#dimension) | First dimension |
| … | … | … |
| Dimension N | [Dimension](#dimension) | Nth dimension |

## Dimension

The dimension has internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Dimension name length | `uint32_t` | Number of characters in dimension name |
| Dimension name | `uint8_t[]` | Dimension name character array |
| Dimension datatype | `uint8_t` | _New in version 5_ Datatype of the coordinate values |
| Cell val num | `uint32_t` | _New in version 5_ Number of coordinate values per cell. For variable-length dimensions, this is `std::numeric_limits<uint32_t>::max()` |
| Filters | [Filter Pipeline](./filter_pipeline.md) | _New in version 5_ The filter pipeline used on coordinate value tiles |
| Domain size | `uint64_t` | _New in version 5_ The domain size in bytes |
| Domain | `uint8_t[]` | Byte array of length equal to domain size above, storing the min, max values of the dimension. |
| Null tile extent | `uint8_t` | `1` if the dimension has a null tile extent, else `0`. |
| Tile extent | `uint8_t[]` | Byte array of length equal to the dimension datatype size, storing the space tile extent of this dimension. |

> [!NOTE]
> Prior to version 5, the size of the _Domain_ field was always equal to twice the size of the dimension's data type (which is stored in the [domain](#domain) in these versions).

## Attribute

The attribute has internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Attribute name length | `uint32_t` | Number of characters in attribute name |
| Attribute name | `uint8_t[]` | Attribute name character array |
| Attribute datatype | `uint8_t` | Datatype of the attribute values |
| Cell val num | `uint32_t` | Number of attribute values per cell. For variable-length attributes, this is `std::numeric_limits<uint32_t>::max()` |
| Filters | [Filter Pipeline](./filter_pipeline.md) | The filter pipeline used on attribute value tiles |
| Fill value size | `uint64_t` | _New in version 6_ The size in bytes of the fill value |
| Fill value | `uint8_t[]` | _New in version 6_ The fill value |
| Nullable | `bool` | _New in version 7_ Whether or not the attribute can be null |
| Fill value validity | `uint8_t` | _New in version 7_ The validity fill value |
| Order | `uint8_t` | _New in version 17_ Order of the data stored in the attribute. This may be unordered, increasing or decreasing |

## Dimension Label

The dimension label has internal format:

| **Field**                  | **Type**   | **Description** |
| :------------------------- | :--------- | :-------------- |
| Dimension index            | `uint32_t` | Index of the dimension the label is for |
| Label order                | `uint8_t`  | Order of the label data |
| Dimension label name length| `uint64_t` | Number of characters in the dimension label name |
| Dimension label name       | `uint8_t []`  | The name of the dimension label |
| Relative URI               | `uint8_t`     | If the URI of the array the label data is stored in is relative to this array |
| URI length                 | `uint64_t` | The number of characters in the URI |
| URI                        | `uint8_t []`  | The URI the label data is stored in |
| Label attribute name length| `uint32_t` | The length of the attribute name of the label data |
| Label attribute name       | `uint8_t []`  | The name of the attribute the label data is stored in |
| Label datatype             | `uint8_t`  | The datatype of the label data |
| Label cell_val_num         | `uint32_t` | The number of values per cell of the label data. For variable-length labels, this is `std::numeric_limits<uint32_t>::max()` |
| Label domain size          | `uint64_t` | The size of the label domain |
| Label domain start size    | `uint64_t` | The size of the first value of the domain for variable-length datatypes. For fixed-length labels, this is 0|
| Label domain data          | `uint8_t[]`| Byte array of length equal to domain size above, storing the min, max values of the dimension |
| Is external                | `uint8_t`     | If the URI is not stored as part of this array |

## Current Domain

If a current domain is empty, only the version number and the empty flag are serialized to storage.

The current current domain format version is `1`.

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Version number | `uint32_t` | Current domain version number |
| Empty | `uint8_t` | Whether the current domain has a representation(e.g. NDRectangle) set or not |
| Type | `uint8_t` | The type of current domain stored in this file |
| NDRectangle | [MBR](./fragment.md#mbr) | A hyperrectangle defined using [1DRange](./fragment.md#mbr) items for each dimension |
