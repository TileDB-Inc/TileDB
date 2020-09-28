#Array Schema File

The array schema file has name `__array_schema.tdb` and is located here:

```
my_array                   # array folder
   |_ ....
   |_ __array_schema.tdb   # array schema file
   |_ ...
```

## Main Structure

The array schema file consists of a single [generic tile](./generic_tile.md), with the following data:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Array version | `uint32_t` | Format version number of the array schema |
| Allows dups | `bool` | Whether or not the array allows duplicate cells |
| Array type | `uint8_t` | Dense or sparse |
| Tile order | `uint8_t` | Row or column major |
| Cell order | `uint8_t` | Row or column major |
| Capacity | `uint64_t` | For sparse fragments, the data tile capacity |
| Coords filters | [Filter Pipeline](./filter_pipeline.md) | The filter pipeline used as default for coordinate tiles |
| Offsets filters | [Filter Pipeline](./filter_pipeline.md) | The filter pipeline used for cell var-len offset tiles |
| Domain | [Domain](#domain) | The array domain |
| Num attributes | `uint32_t` | Number of attributes in the array |
| Attribute 1 | [Attribute](#attribute) | First attribute |
| … | … | … |
| Attribute N | [Attribute](#attribute) | Nth attribute |

## Domain

The domain has internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Num dimensions | `uint32_t` | Dimensionality/rank of the domain |
| Dimension 1 | [Dimension](#dimension) | First dimension |
| … | … | … |
| Dimension N | [Dimension](#dimension) | Nth dimension |

## Dimension

The dimension has internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Dimension name length | `uint32_t` | Number of characters in dimension name |
| Dimension name | `char[]` | Dimension name character array |
| Dimension datatype | `uint8_t` | Datatype of the coordinate values |
| Cell val num | `uint32_t` | Number of coordinate values per cell. For variable-length dimensions, this is `std::numeric_limits<uint32_t>::max()` |
| Filters | [Filter Pipeline](./filter_pipeline.md) | The filter pipeline used on coordinate value tiles |
| Domain size | `uint64_t[]` | The domain size in bytes |
| Domain | `uint8_t[]` | Byte array of length equal to domain size above, storing the min, max values of the dimension. |
| Null tile extent | `uint8_t` | `1` if the dimension has a null tile extent, else `0`. |
| Tile extent | `uint8_t[]` | Byte array of length equal to the dimension datatype size, storing the space tile extent of this dimension. |

## Attribute

The attribute has internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Attribute name length | `uint32_t` | Number of characters in attribute name |
| Attribute name | `char[]` | Attribute name character array |
| Attribute datatype | `uint8_t` | Datatype of the attribute values |
| Cell val num | `uint32_t` | Number of attribute values per cell. For variable-length attributes, this is `std::numeric_limits<uint32_t>::max()` |
| Filters | [Filter Pipeline](./filter_pipeline.md) | The filter pipeline used on attribute value tiles |
| Fill value size | `uint64_t` | The size in bytes of the fill value |
| Fill value | `uint8_t[]` | The fill value |
| Nullable | `bool` | Whether or not the attribute can be null |
