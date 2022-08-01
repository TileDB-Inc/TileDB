---
title: Fragment
---

## Main Structure

A fragment metadata folder is called `<timestamped_name>` and located here:

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
         |      |_ dcmh.tdb                 # delete condition marker hash attribute
         |      |_ ...  
        |_ ...  
```

`<timestamped_name>` has format `__t1_t2_uuid_v`, where:
* `t1` and `t2` are timestamps in milliseconds elapsed since 1970-01-01 00:00:00 +0000 (UTC)
* `uuid` is a unique identifier
* `v` is the format version

There can be any number of fragments in an array. The fragment folder contains:

* A single [fragment metadata file](#fragment-metadata-file) named `__fragment_metadata.tdb`. 
* Any number of [data files](#data-file). For each fixed-sized attribute `foo1` (or dimension `bar1`), there is a single data file `a0.tdb` (`d0.tdb`) containing the values along this attribute (dimension). For every var-sized attribute `foo2` (or dimensions `bar2`), there are two data files; `a1_var.tdb` (`d1_var.tdb`) containing the var-sized values of the attribute (dimension) and `a1.tdb` (`d1.tdb`) containing the starting offsets of each value in `a1_var.tdb` (`d1_var.rdb`). Both fixed-sized and var-sized attributes can be nullable. A nullable attribute, `foo3`, will have an additional file `a2_validity.tdb` that contains its validity vector.
* The names of the data files are not dependent on the names of the attributes/dimensions. The file names are determined by the order of the attributes and dimensions in the array schema.
* The timestamp fixed attribute (`t.tdb`) is, for fragments consolidated with timestamps, the time at which a cell was added.
* The delete timestamp fixed attribute (`dt.tdb`) is, for fragments consolidated with delete conditions, the time at which a cell was deleted.
* The delete condition marker hash fixed attribute (`dcmh.tdb`) is, for fragments consolidated with delete conditions, the hash of the delete condition marker that deleted the cell.

## Fragment Metadata File 

The fragment metadata file has the following on-disk format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| R-Tree | [R-Tree](#r-tree) | The serialized R-Tree |
| Tile offsets for attribute/dimension 1 | [Tile Offsets](#tile-offsets) | The serialized tile offsets for attribute/dimension 1 |
| … | … | … |
| Tile offsets for attribute/dimension N | [Tile Offsets](#tile-offsets) | The serialized tile offsets for attribute/dimension N |
| Variable tile offsets for attribute/dimension 1 | [Tile Offsets](#tile-offsets) | The serialized variable tile offsets for attribute/dimension 1 |
| … | … | … |
| Variable tile offsets for attribute/dimension N | [Tile Offsets](#tile-offsets) | The serialized variable tile offsets for attribute/dimension N |
| Variable tile sizes for attribute/dimension 1 | [Tile Offsets](#tile-offsets) | The serialized variable tile sizes for attribute/dimension 1 |
| … | … | … |
| Variable tile sizes for attribute/dimension N | [Tile Offsets](#tile-offsets) | The serialized variable tile sizes for attribute/dimension N |
| Validity tile offsets for attribute/dimension 1 | [Tile Offsets](#tile-offsets) | The serialized validity tile offsets for attribute/dimension 1 |
| … | … | … |
| Validity tile offsets for attribute/dimension N | [Tile Offsets](#tile-offsets) | The serialized validity tile offsets for attribute/dimension N |
| Tile mins for attribute/dimension 1 | [Tile Mins/Maxs](#tile-mins-maxs) | The serialized mins for attribute/dimension 1 |
| … | … | … |
| Variable mins for attribute/dimension N | [Tile Mins/Maxs](#tile-mins-maxs) | The serialized mins for attribute/dimension N |
| Tile maxs for attribute/dimension 1 | [Tile Mins/Maxs](#tile-mins-maxs) | The serialized maxs for attribute/dimension 1 |
| … | … | … |
| Variable maxs for attribute/dimension N | [Tile Mins/Maxs](#tile-mins-maxs) | The serialized maxs for attribute/dimension N |
| Tile sums for attribute/dimension 1 | [Tile Sums](#tile-sums) | The serialized sums for attribute/dimension 1 |
| … | … | … |
| Variable sums for attribute/dimension N | [Tile Sums](#tile-sums) | The serialized sums for attribute/dimension N |
| Tile null counts for attribute/dimension 1 | [Tile Null Count](#tile-null-count) | The serialized null counts for attribute/dimension 1 |
| … | … | … |
| Variable maxs for attribute/dimension N | [[Tile Null Count](#tile-null-count) | The serialized null counts for attribute/dimension N |
| Fragment min, max, sum, null count | [[Tile Fragment Min Max Sum Null Count](#tile-fragment-min-max-sum-null-count) | The serialized fragment min max sum null count |
| Processed conditions | [[Tile Processed Conditions](#tile-processed-conditions) | The serialized processed conditions |
| Metadata footer | [Footer](#footer) | Basic metadata gathered in the footer |

### R-Tree

The R-Tree is a [generic tile](./generic_tile.md) with the following internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Fanout | `uint32_t` | The tree fanout |
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

The tile offsets is a [generic tile](./generic_tile.md) with the following internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Num tile offsets | `uint64_t` | Number of tile offsets |
| Tile offset 1 | `uint64_t` | Offset 1 |
| … | … | … |
| Tile offset N | `uint64_t` | Offset N |

### Tile Sizes

The tile sizes is a [generic tile](./generic_tile.md) with the following internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Num tile sizes | `uint64_t` | Number of tile sizes |
| Tile size 1 | `uint64_t` | Size 1 |
| … | … | … |
| Tile size N | `uint64_t` | Size N |

### Tile Mins Maxs

The tile mins maxs is a [generic tile](./generic_tile.md) with the following internal format:

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

### Tile Processed Conditions

The processed conditions is a [generic tile](./generic_tile.md) and is the list of delete/update conditions that have already been applied for this fragment and don't need to be applied again, in no particular order, with the following internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Num | `uint64_t` | Number of processed conditions |
| Condition size | `uint64_t` | Condition size 1 |
| Condition | `char` | Condition marker filename 1 |
| … | … | … |
| Condition size | `uint64_t` | Condition size N |
| Condition | `char` | Condition marker filename N |

### Footer

The footer is a simple blob \(i.e., _not a generic tile_\) with the following internal format:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Version number | `uint32_t` | Format version number of the fragment |
| Array schema name size | `uint64_t` | Size of the array schema name |
| Array schema name | `string` | Array schema name |
| Dense | `char` | Whether the array is dense |
| Null non-empty domain | `char` | Indicates whether the non-empty domain is null or not |
| Non-empty domain | [MBR](#mbr) | An MBR denoting the non-empty domain |
| Number of sparse tiles | `uint64_t` | Number of sparse tiles |
| Last tile cell num | `uint64_t` | For sparse arrays, the number of cells in the last tile in the fragment |
| Includes timestamps | `char` | Whether the fragment includes timestamps or not |
| Includes delete metadata | `char` | Whether the fragment includes delete metadata or not |
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
| Tile validity offset for attribute/dimension 1 | `uint64_t` | The offset to the generic tile storing the tile validity offsets for attribute/dimension 1. |
| … | … | … |
| Tile validity offset for attribute/dimension N | `uint64_t` | The offset to the generic tile storing the tile validity offsets for attribute/dimension N |
| Tile mins offset for attribute/dimension 1 | `uint64_t` | The offset to the generic tile storing the tile mins for attribute/dimension 1. |
| … | … | … |
| Tile mins offset for attribute/dimension N | `uint64_t` | The offset to the generic tile storing the tile mins for attribute/dimension N |
| Tile maxs offset for attribute/dimension 1 | `uint64_t` | The offset to the generic tile storing the tile maxs for attribute/dimension 1. |
| … | … | … |
| Tile maxs offset for attribute/dimension N | `uint64_t` | The offset to the generic tile storing the tile maxs for attribute/dimension N |
| Tile sums offset for attribute/dimension 1 | `uint64_t` | The offset to the generic tile storing the tile sums for attribute/dimension 1. |
| … | … | … |
| Tile sums offset for attribute/dimension N | `uint64_t` | The offset to the generic tile storing the tile sums for attribute/dimension N |
| Tile null counts offset for attribute/dimension 1 | `uint64_t` | The offset to the generic tile storing the tile null counts for attribute/dimension 1. |
| … | … | … |
| Tile null counts offset for attribute/dimension N | `uint64_t` | The offset to the generic tile storing the tile null counts for attribute/dimension N |
| Fragment min max sum null count offset | `uint64_t` | The offset to the generic tile storing the fragment min max sum null count data. |
| Processed conditions offset | `uint64_t` | The offset to the generic tile storing the processed conditions. |
| Array schema name size | `uint64_t` | The total number of characters of the array schema name. |
| Array schema name character 1 | `char` | The first character of the array schema name. |
| … | … | … |
| Array schema name character N | `char` | The last character of the array schema name. |
| Footer length | `uint64_t` | Sum of bytes of the above fields. Only present when there is at least one var-sized dimension. |

## Data File 

The on-disk format of each data file is:

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Tile 1 | [Tile](./tile.md#tile) | The data of tile 1 |
| … | … | … |
| Tile N | [Tile](./tile.md#tile) | The data of tile N |

