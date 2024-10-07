---
title: Fragment
---

## Main Structure

A fragment metadata folder is called [`<timestamped_name>`](./timestamped_name.md)` and located here:

```
my_array                                    # array folder
   |  ...
   |_ __fragments                           # array fragments folder
         |_ <timestamped_name>              # fragment folder
         |      |_ __fragment_metadata.tdb  # fragment metadata
         |      |_ a0.tdb                   # fixed-sized attribute 
         |      |_ a1.tdb                   # var-sized attribute (offsets) 
         |      |_ a1_var.tdb               # var-sized attribute (values)
         |      |_ a2.tdb                   # fixed-sized nullable attribute
         |      |_ a2_validity.tdb          # fixed-sized nullable attribute (validities)
         |      |_ ...      
         |      |_ d0.tdb                   # fixed-sized dimension 
         |      |_ d1.tdb                   # var-sized dimension (offsets) 
         |      |_ d1_var.tdb               # var-sized dimension (values)
         |      |_ ...      
         |      |_ t.tdb                    # timestamp attribute
         |      |_ ...  
         |      |_ dt.tdb                   # delete timestamp attribute
         |      |_ ...  
         |      |_ dci.tdb                  # delete condition index attribute
         |      |_ ...  
         |      |_ __coords.tdb             # legacy coordinates
         |_ ...  
```

There can be any number of fragments in an array. The fragment folder contains:

* A single [fragment metadata file](#fragment-metadata-file) named `__fragment_metadata.tdb`. 
* Any number of [data files](#data-file). For each fixed-sized attribute `foo1` (or dimension `bar1`), there is a single data file `a0.tdb` (`d0.tdb`) containing the values along this attribute (dimension). For every var-sized attribute `foo2` (or dimensions `bar2`), there are two data files; `a1_var.tdb` (`d1_var.tdb`) containing the var-sized values of the attribute (dimension) and `a1.tdb` (`d1.tdb`) containing the starting offsets of each value in `a1_var.tdb` (`d1_var.rdb`). Both fixed-sized and var-sized attributes can be nullable. A nullable attribute, `foo3`, will have an additional file `a2_validity.tdb` that contains its validity vector.
* The names of the data files are not dependent on the names of the attributes/dimensions. The file names are determined by the order of the attributes and dimensions in the array schema.
* _New in version 14_ The timestamp fixed attribute (`t.tdb`) is, for fragments consolidated with timestamps, the time at which a cell was added.
* _New in version 15_ The delete timestamp fixed attribute (`dt.tdb`) is, for fragments consolidated with delete conditions, the time at which a cell was deleted.
* _New in version 15_ The delete condition [Delete commit file](./delete_commit_file.md) index fixed attribute (`dci.tdb`) is, for fragments consolidated with delete conditions, the index of the delete condition (inside of [Tile Processed Conditions](#tile-processed-conditions)) that deleted the cell.

> [!NOTE]
> Prior to version 9, data files were named after their corresponding attributes or dimensions.
>
> In version 8 only, certain characters of the data files' name were percent-encoded. These characters are `!#$%&'()*+,/:;=?@[]`, as specified in [RFC 3986](https://tools.ietf.org/html/rfc3986), as well as `"<>\|`, which are not allowed in Windows file names.

## Fragment Metadata File 

The fragment metadata file has the following on-disk format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| R-Tree | [R-Tree](#r-tree) | The serialized R-Tree |
| Tile offsets for attribute/dimension 1 | [Tile Offsets](#tile-offsets) | The serialized _on-disk_ tile offsets for attribute/dimension 1 |
| … | … | … |
| Tile offsets for attribute/dimension N | [Tile Offsets](#tile-offsets) | The serialized _on-disk_ tile offsets for attribute/dimension N |
| Variable tile offsets for attribute/dimension 1 | [Tile Offsets](#tile-offsets) | The serialized _on-disk_ variable tile offsets for attribute/dimension 1 |
| … | … | … |
| Variable tile offsets for attribute/dimension N | [Tile Offsets](#tile-offsets) | The serialized _on-disk_ variable tile offsets for attribute/dimension N |
| Variable tile sizes for attribute/dimension 1 | [Tile Sizes](#tile-sizes) | The serialized _in-memory_ variable tile sizes for attribute/dimension 1 |
| … | … | … |
| Variable tile sizes for attribute/dimension N | [Tile Sizes](#tile-sizes) | The serialized _in-memory_ variable tile sizes for attribute/dimension N |
| Validity tile offsets for attribute/dimension 1 | [Tile Offsets](#tile-offsets) | _New in version 7_ The serialized _on-disk_ validity tile offsets for attribute/dimension 1 |
| … | … | … |
| Validity tile offsets for attribute/dimension N | [Tile Offsets](#tile-offsets) | _New in version 7_ The serialized _on-disk_ validity tile offsets for attribute/dimension N |
| Tile mins for attribute/dimension 1 | [Tile Mins/Maxes](#tile-mins-maxes) | _New in version 11_ The serialized mins for attribute/dimension 1 |
| … | … | … |
| Variable mins for attribute/dimension N | [Tile Mins/Maxes](#tile-mins-maxes) | _New in version 11_ The serialized mins for attribute/dimension N |
| Tile maxes for attribute/dimension 1 | [Tile Mins/Maxes](#tile-mins-maxes) | _New in version 11_ The serialized maxes for attribute/dimension 1 |
| … | … | … |
| Variable maxes for attribute/dimension N | [Tile Mins/Maxes](#tile-mins-maxes) | _New in version 11_ The serialized maxes for attribute/dimension N |
| Tile sums for attribute/dimension 1 | [Tile Sums](#tile-sums) | _New in version 11_ The serialized sums for attribute/dimension 1 |
| … | … | … |
| Variable sums for attribute/dimension N | [Tile Sums](#tile-sums) | _New in version 11_ The serialized sums for attribute/dimension N |
| Tile null counts for attribute/dimension 1 | [Tile Null Count](#tile-null-count) | _New in version 11_ The serialized null counts for attribute/dimension 1 |
| … | … | … |
| Tile null counts for attribute/dimension N | [Tile Null Count](#tile-null-count) | _New in version 11_ The serialized null counts for attribute/dimension N |
| Fragment min, max, sum, null count | [Tile Fragment Min Max Sum Null Count](#tile-fragment-min-max-sum-null-count) | _New in version 11_ The serialized fragment min max sum null count |
| Processed conditions | [Tile Processed Conditions](#tile-processed-conditions) | _New in version 16_ The serialized processed conditions |
| Metadata footer | [Footer](#footer) | Basic metadata gathered in the footer |

> [!NOTE]
> Prior to version 3, fragment metadata are stored with a [different structure](#legacy-fragment-metadata-file).

### R-Tree

The R-Tree is a [generic tile](./generic_tile.md) with the following internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Dimension number | `uint32_t` | _Removed in version 5_ Number of dimensions. Can also be obtained from the array schema. |
| Fanout | `uint32_t` | The tree fanout |
| Datatype | `uint8_t` | _Removed in version 5_ The domain's datatype. Dimensions are no longer guaranteed to have the same datatype since version 5. |
| Num levels | `uint32_t` | The number of levels in the tree |
| Num MBRs at level 1 | `uint64_t` | The number of MBRs at level 1 |
| MBR 1 at level 1 | [MBR](#mbr) | First MBR at level 1 |
| … | … | … |
| MBR N at level 1 | [MBR](#mbr) | N-th MBR at level 1 |
| … | … | … |
| Num MBRs at level L | `uint64_t` | The number of MBRs at level L |
| MBR 1 at level L | [MBR](#mbr) | First MBR at level L |
| … | … | … |
| MBR N at level L | [MBR](#mbr) | N-th MBR at level L |

### MBR

Each MBR entry has format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| 1D range for dimension 1 | `1DRange` | The 1-dimensional range for dimension 1 |
| … | … | … |
| 1D range for dimension D | `1DRange` | The 1-dimensional range for dimension D |

For *fixed-sized dimensions*, the `1DRange` format is:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Range minimum | `uint8_t` | The minimum value with the same datatype as the dimension |
| Range maximum | `uint8_t` | The maximum value with the same datatype as the dimension |

For *var-sized dimensions*, the `1DRange` format is:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Range length | `uint64_t` | The number of bytes of the 1D range |
| Minimum value length | `uint64_t` | The number of bytes of the minimum value |
| Range minimum | `uint8_t` | The minimum (var-sized) value with the same datatype as the dimension |
| Range maximum | `uint8_t` | The maximum (var-sized) value with the same datatype as the dimension |

### Tile Offsets

Tile offsets refer to each _on-disk_ data tile's starting byte offset. \
Tile offsets is a [generic tile](./generic_tile.md) with the following internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Num tile offsets | `uint64_t` | Number of tile offsets |
| Tile offset 1 | `uint64_t` | Offset 1 |
| … | … | … |
| Tile offset N | `uint64_t` | Offset N |

### Tile Sizes

The tile size refers to the _in-memory_ size. \
It is a [generic tile](./generic_tile.md) with the following internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Num tile sizes | `uint64_t` | Number of tile sizes |
| Tile size 1 | `uint64_t` | Size 1 |
| … | … | … |
| Tile size N | `uint64_t` | Size N |

### Tile Mins Maxes

The tile mins maxes is a [generic tile](./generic_tile.md) with the following internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Num values | `uint64_t` | Number of values |
| Value 1 | `type` | Value 1 or Offset 1 |
| … | … | … |
| Value N | `type` | Value N or Offset N |
| Var buffer size | `uint64_t` | Var buffer size |
| Var buffer | `uint8_t` | Var buffer |

### Tile Sums

The tile sums is a [generic tile](./generic_tile.md) with the following internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Num values | `uint64_t` | Number of values |
| Value 1 | `uint64_t` | Sum 1 |
| … | … | … |
| Value N | `uint64_t` | Sum N |

### Tile Null Count

The tile null count is a [generic tile](./generic_tile.md) with the following internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Num values | `uint64_t` | Number of values |
| Value 1 | `uint64_t` | Count 1 |
| … | … | … |
| Value N | `uint64_t` | Count N |

### Tile Fragment Min Max Sum Null Count

The fragment min max sum null count is a [generic tile](./generic_tile.md) with the following internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Min size | `uint64_t` | Size of the min value for attribute/dimension 1 |
| Min value | `uint8_t` | Buffer for min value for attribute/dimension 1 |
| Max size | `uint64_t` | Size of the max value for attribute/dimension 1 |
| Max value | `uint8_t` | Buffer for max value for attribute/dimension 1 |
| Sum | `uint64_t` | Sum value for attribute/dimension 1 |
| Null count | `uint64_t` | Null count value for attribute/dimension 1 |
| … | … | … |
| Min size | `uint64_t` | Size of the min value for attribute/dimension N |
| Min value | `uint8_t` | Buffer for min value for attribute/dimension N |
| Max size | `uint64_t` | Size of the max value for attribute/dimension N |
| Max value | `uint8_t` | Buffer for max value for attribute/dimension N |
| Sum | `uint64_t` | Sum value for attribute/dimension N |
| Null count | `uint64_t` | Null count value for attribute/dimension N |

Tile and fragment mins, maxes, sums and null counts are colloquially referred to as "tile metadata".

> [!NOTE]
> Prior to version 21, tile metadata for nullable fixed-size strings on dense arrays might be incorrect and implementations must not rely on them.

### Tile Processed Conditions

The processed conditions is a [generic tile](./generic_tile.md) and is the list of delete/update conditions that have already been applied for this fragment and don't need to be applied again, sorted by filename, with the following internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Num | `uint64_t` | Number of processed conditions |
| Condition size | `uint64_t` | Condition size 1 |
| Condition | `uint8_t` | Condition marker filename 1 |
| … | … | … |
| Condition size | `uint64_t` | Condition size N |
| Condition | `uint8_t` | Condition marker filename N |

### Footer

The footer is a simple blob \(i.e., _not a generic tile_\) with the following internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Version number | `uint32_t` | Format version number of the fragment |
| Array schema name size | `uint64_t` | _New in version 10_ Size of the array schema name |
| Array schema name | `string` | _New in version 10_ Array schema name |
| Dense | `uint8_t` | Whether the array is dense (1) or not (0) |
| Null non-empty domain | `uint8_t` | Indicates whether the non-empty domain is null (1) or not (0) |
| Non-empty domain | [MBR](#mbr) | An MBR denoting the non-empty domain |
| Number of sparse tiles | `uint64_t` | Number of sparse tiles |
| Last tile cell num | `uint64_t` | For sparse arrays, the number of cells in the last tile in the fragment |
| Includes timestamps | `uint8_t` | _New in version 14_ Whether the fragment includes timestamps (1) or not (0) |
| Includes delete metadata | `uint8_t` | _New in version 15_ Whether the fragment includes delete metadata (1) or not (0) |
| File sizes | `uint64_t[]` | The size in bytes of each attribute/dimension file in the fragment. For var-length attributes/dimensions, this is the size of the offsets file. |
| File var sizes | `uint64_t[]` | The size in bytes of each var-length attribute/dimension file in the fragment. |
| File validity sizes | `uint64_t[]` | The size in bytes of each attribute/dimension validity vector file in the fragment. |
| R-Tree offset | `uint64_t` | The offset to the generic tile storing the R-Tree in the metadata file. |
| Tile offset for attribute/dimension 1 | `uint64_t` | The offset to the generic tile storing the tile offsets for attribute/dimension 1. |
| … | … | … |
| Tile offset for attribute/dimension N | `uint64_t` | The offset to the generic tile storing the tile offsets for attribute/dimension N |
| Tile var offset for attribute/dimension 1 | `uint64_t` | The offset to the generic tile storing the variable tile offsets for attribute/dimension 1. |
| … | … | … |
| Tile var offset for attribute/dimension N | `uint64_t` | The offset to the generic tile storing the variable tile offsets for attribute/dimension N. |
| Tile var sizes offset for attribute/dimension 1 | `uint64_t` | The offset to the generic tile storing the variable tile sizes for attribute/dimension 1. |
| … | … | … |
| Tile var sizes offset for attribute/dimension N | `uint64_t` | The offset to the generic tile storing the variable tile sizes for attribute/dimension N. |
| Tile validity offset for attribute/dimension 1 | `uint64_t` | _New in version 7_ The offset to the generic tile storing the tile validity offsets for attribute/dimension 1. |
| … | … | … |
| Tile validity offset for attribute/dimension N | `uint64_t` | _New in version 7_ The offset to the generic tile storing the tile validity offsets for attribute/dimension N |
| Tile mins offset for attribute/dimension 1 | `uint64_t` | The offset to the generic tile storing the tile mins for attribute/dimension 1. |
| … | … | … |
| Tile mins offset for attribute/dimension N | `uint64_t` | The offset to the generic tile storing the tile mins for attribute/dimension N |
| Tile maxes offset for attribute/dimension 1 | `uint64_t` | The offset to the generic tile storing the tile maxes for attribute/dimension 1. |
| … | … | … |
| Tile maxes offset for attribute/dimension N | `uint64_t` | The offset to the generic tile storing the tile maxes for attribute/dimension N |
| Tile sums offset for attribute/dimension 1 | `uint64_t` | The offset to the generic tile storing the tile sums for attribute/dimension 1. |
| … | … | … |
| Tile sums offset for attribute/dimension N | `uint64_t` | The offset to the generic tile storing the tile sums for attribute/dimension N |
| Tile null counts offset for attribute/dimension 1 | `uint64_t` | The offset to the generic tile storing the tile null counts for attribute/dimension 1. |
| … | … | … |
| Tile null counts offset for attribute/dimension N | `uint64_t` | The offset to the generic tile storing the tile null counts for attribute/dimension N |
| Fragment min max sum null count offset | `uint64_t` | The offset to the generic tile storing the fragment min max sum null count data. |
| Processed conditions offset | `uint64_t` | _New in version 16_ The offset to the generic tile storing the processed conditions. |
| Array schema name size | `uint64_t` | The total number of characters of the array schema name. |
| Array schema name | `uint8_t[]` | The array schema name. |
| Footer length | `uint64_t` | Sum of bytes of the above fields. |

> [!NOTE]
> Prior to version 10, the _Footer length_ field was present only when the array had at least one variable-sized dimension. Implementations had to obtain the format version from the fragment folder's timestamped name.

## Data File

The on-disk format of each data file is:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Tile 1 | [Tile](./tile.md#tile) | The data of tile 1 |
| … | … | … |
| Tile N | [Tile](./tile.md#tile) | The data of tile N |

## Legacy coordinates file

Prior to version 5, dimension data for sparse cells are combined in a single tile that is stored in the `__coords.tdb` file. The tile is filtered with the filters specified in the _Coords filters_ field of the [array schema](./array_schema.md).

Coordinates of a multi-dimensional array are placed in either zipped or unzipped order. In zipped order, coordinates of a cell are placed next to each other and ordered by the cell index, while in unzipped order, all coordinates values of a dimension are placed next to each other and ordered by the dimension index.

* Since version 2, coordinates are always stored unzipped.
* In version 1, coordinates are stored unzipped if a [compression filter](./tile.md#compression-filters) exists in the filter list, otherwise they are stored zipped.

## Legacy Fragment metadata file

Prior to version 3, fragment metadata is a [generic tile](./generic_tile.md) with the following internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Version number | `uint32_t` | [Format version](./array_format_history.md) number of the fragment |
| Non-empty domain size | `uint64_t` | Size of non-empty domain |
| Non-empty domain | `uint8_t[]` | Byte array of coordinate pairs storing the non-empty domain |
| Num MBRs | `uint64_t` | Number of MBRs in fragment |
| MBR 1 | `uint8_t[]` | Byte array of coordinate pairs storing MBR 1 |
| … | … | … |
| MBR N | `uint8_t[]` | Byte array of coordinate pairs storing MBR N |
| Num bounding coords | `uint64_t` | Number of bounding coordinates |
| Bounding coords | `uint8_t[]` | Byte array of coordinate pairs storing the first/last coordinates in the fragment |
| Tile offsets | [Legacy Tile Offsets](#legacy-tile-offsetssizes) | The offsets of each tile in the attribute files |
| Tile var offsets | [Legacy Tile Offsets](#legacy-tile-offsetssizes) | The offsets of each variable tile in the attribute files |
| Variable tile sizes | [Legacy Tile Sizes](#legacy-tile-offsetssizes) | The sizes of each variable tile in the attribute files |
| Last tile cell num | For sparse arrays, the number of cells in the last time in the fragment. Ignored on dense arrays. |
| File sizes | `uint64_t[]` | The size in bytes of each attribute/dimension file in the fragment. For var-length attributes/dimensions, this is the size of the offsets file. |
| File var sizes | `uint64_t[]` | The size in bytes of each var-length attribute/dimension file in the fragment. |

### Legacy tile offsets/sizes

Legacy tile offsets and sizes is a simple blob (i.e., _not a generic tile_) with the following internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Num tile offsets/sizes, attribute 1 | `uint64_t` | Number of tile offsets/sizes for attribute 1 |
| Tile offset/size 1, attribute 1 | `uint64_t` | Offset/Size 1 for attribute 1 |
| … | … | … |
| Tile offset/size N, attribute 1 | `uint64_t` | Offset/Size N for attribute 1 |
| … | … | … |
| Num tile offsets/sizes, attribute N | `uint64_t` | Number of tile offsets/sizes for attribute N |
| Tile offset/size 1, attribute N | `uint64_t` | Offset/Size 1 for attribute N |
| … | … | … |
| Tile offset/size N, attribute N | `uint64_t` | Offset/Size N for attribute N |
