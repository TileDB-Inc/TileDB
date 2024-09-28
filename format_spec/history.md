---
title: Format version history
---

# Format Version History

## Version 22

Introduced in TileDB 2.25

* The _Current domain_ field was added to [array schemas](./array_schema.md#array-schema-file).

## Version 21

Introduced in TileDB 2.19

* The TileDB implementation has been updated to fix computing [tile metadata](./fragment.md#tile-mins-maxes) for nullable fixed-size strings on dense arrays.

> [!NOTE]
> This version does not contain any changes to the storage format, but was introduced as an indicator for implementations to not rely on tile metadata for nullable fixed-size strings on dense arrays on previous versions.

## Version 20

Introduced in TileDB 2.17

* Arrays can have [enumerations](./enumeration.md).
* The bit-width reduction and positive delta encoding filters are supported on data of date or time types.
* The [filter pipeline options](./filter_pipeline.md#filter-options) for the double-delta filter contain the _Reinterpret datatype_ field.

## Version 19

Introduced in TileDB 2.16

* [Vacuum files](./vacuum_file.md) contain relative paths to the location of the array.

## Version 18

Introduced in TileDB 2.15

* Arrays can have [dimension labels](./array_schema.md#dimension-label).

## Version 17

Introduced in TileDB 2.14

* The _Order_ field was added to [attributes](./array_schema.md#attribute).
* Cell offsets in dimensions or attributes of UTF-8 string type are not written in the offset tiles, if the RLE or dictionary filter exists in the filter pipeline. They are instead encoded as part of the data tile.

## Version 16

Introduced in TileDB 2.12

* Arrays can have [delete commit files](./delete_commit_file.md).
* Arrays can have [update commit files](./update_commit_file.md).
    * The TileDB implementation currently supports writing update commit files as an experimental feature, but they are not yet considered when performing reads.
* Fragment metadata contain [tile processed conditions](./fragment.md#tile-processed-conditions).

## Version 15

Introduced in TileDB 2.11

* Consolidated fragments can have delete metadata files. The _Includes delete metadata_ field was added to the [fragment metadata footer](./fragment.md#footer).

## Version 14

Introduced in TileDB 2.10

* Consolidated fragments can have timestamp files. The _Includes timestamps_ field was added to the [fragment metadata footer](./fragment.md#footer).

## Version 13

Introduced in TileDB 2.9

* The [dictionary filter](./filters/dictionary_encoding.md) was added.

## Version 12

Introduced in TileDB 2.8

* The [array file hierarchy](./array_file_hierarchy.md) was updated to store fragments, commits and consolidated fragment metadata in separate subdirectories.
* The extension of commit files was changed to `.wrt`.
* Cell offsets in dimensions or attributes of ASCII string type are not written in the offset tiles, if the RLE filter exists in the filter pipeline. They are instead encoded as part of the data tile.

## Version 11

Introduced in TileDB 2.7

* Fragment metadata contain [metadata](./fragment.md#tile-mins-maxes) (min/max value, sum, null count) for data in the whole fragment and each tile.
* The TileDB implementation has been updated to never split cells when storing them in chunks.

## Version 10

Introduced in TileDB 2.4

* Arrays support schema evolution.
    * Array schemas are stored in a `__schema` subdirectory, and have a [timestamped name](./timestamped_name.md).
    * The _Array schema name_ field was added to the [fragment metadata footer](./fragment.md#footer).
* The _Footer length_ field of the [fragment metadata footer](./fragment.md#footer) is always written.

## Version 9

Introduced in TileDB 2.3

* [Data files](./fragment.md#data-file) are named by the index of their attribute or dimension.
* The _URI_ fields of [Consolidated fragment metadata files](./consolidated_fragment_metadata_file.md) contain relative paths to the location of fragments in the array.

## Version 8

Introduced in TileDB 2.2.3

* [Data files](./fragment.md#data-file) are named by the name of their attribute or dimension, after percent encoding certain characters. These characters are `!#$%&'()*+,/:;=?@[]`, as specified in [RFC 3986](https://tools.ietf.org/html/rfc3986), as well as `"<>\|`, which are not allowed in Windows file names.

## Version 7

Introduced in TileDB 2.2

* Attributes can be nullable.
    * The _Nullable_ and _Fill value validity_ fields were added to [attributes](./array_schema.md#attribute).
    * The _Validity filters_ field was added to [array schemas](./array_schema.md#array-schema-file).
    * Fragment metadata contain validity [tile offsets](./fragment.md#tile-offsets).

## Version 6

Introduced in TileDB 2.1

* The _Fill value_ field was added to [attributes](./array_schema.md#attribute).

## Version 5

Introduced in TileDB 2.0

* Dimensions are stored in separate [data files](./fragment.md#data-file).
* Sparse arrays can have string dimensions and dimensions with different datatypes.
    * The _Dimension datatype_, _Cell val num_ and _Filters_ fields were added to [dimensions](./array_schema.md#dimension).
    * The _Domain size_ field was added to [dimensions](./array_schema.md#dimension). The domain of a dimension can have a variable size.
    * The _Domain datatype_ field was removed from [domains](./array_schema.md#domain).
    * The [MBR](./fragment.md#mbr) structure has been updated to support variable-sized dimensions.
    * The _Dimension number_ and _R-Tree datatype_ fields have been removed from [R-Trees](./fragment.md#r-tree).
* The _Allows dups_ field was added to [array schemas](./array_schema.md#array-schema-file).
* Committed fragments are indicated by the presence of an `.ok` file in the array's directory, with the same [timestamped name](./timestamped_name.md) as the fragment.

## Version 4

Introduced in TileDB 1.7

* Support for the [key-value store](https://tiledb-inc-tiledb.readthedocs-hosted.com/en/1.6.3/tutorials/kv.html) object type was removed. Key-value stores have been superseded by sparse arrays.

## Version 3

Introduced in TileDB 1.6

* The structure of [fragment metadata files](./fragment.md#fragment-metadata-file) was overhauled.
    * The [footer](./fragment.md#footer) and [R-Tree](./fragment.md#r-tree) structures were added.
    * The _Bounding coords_ field was removed.
    * The _MBRs_ field was removed. MBRs are now stored in the R-Tree.
    * Structures other than the footer like tile offsets, sizes and metadata are wrapped in their own generic tiles. This allows loading them lazily and in parallel.

## Version 2

Introduced in TileDB 1.5

* Cell coordinate values of each dimension are always stored next to each other, regardless of whether they are filtered with a compression filter or not.

## Version 1

Introduced in TileDB 1.4

* Initial version of the TileDB storage format.
