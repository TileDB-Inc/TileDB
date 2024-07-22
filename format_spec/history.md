---
title: Format version history
---

# Format Version History

## Version 22

Introduced in TileDB 2.25

* The _Current domain_ field was added to [array schemas](./array_schema.md).

## Version 21

Introduced in TileDB 2.19

* The TileDB implementation has been updated to fix computing [tile metadata](./fragment.md#tile-mins-maxes) for nullable fixed-size strings on dense arrays.

> [!NOTE]
> This version does not contain any changes to the storage format, but was introduced as an indicator for implementations to not rely on tile metadata under certain circumstances on previous versions.

## Version 20

Introduced in TileDB 2.17

* Arrays can have [enumerations](./enumeration.md).
* The bit-width reduction and positive delta filters are supported on data of date or time types.
* The [filter pipeline options](./filter_pipeline.md#filter-options) for the double-delta filter contain the _Reinterpret datatype_ field.

## Version 19

Introduced in TileDB 2.16

* [Vacuum files](./vacuum_file.md) contain relative paths to the location of the array.
* The [filter pipeline options](./filter_pipeline.md#filter-options) for the delta filter contain the _Reinterpret datatype_ field.

## Version 18

Introduced in TileDB 2.15

* Arrays can have [dimension labels](./array_schema.md#dimension-label).

## Version 17

Introduced in TileDB 2.14

* The _Order_ field was added to [attributes](./array_schema.md#attribute).
* When filtering UTF-8 strings with the dictionary or RLE filter, offsets buffers are passed to the filter unchanged and without any filtering on themselves.

## Version 16

Introduced in TileDB 2.12

* Deleting data from arrays is supported. Arrays can have [delete commit files](./delete_commit_file.md).
* Updating data in arrays is supported. Arrays can have [update commit files](./update_commit_file.md).
* Fragment metadata contain [tile processed conditions](./fragment.md#tile-processed-conditions).

## Version 15

Introduced in TileDB 2.11

* Fragments can have delete metadata files. The _Includes delete metadata_ field was added to the [fragment metadata footer](./fragment.md#footer).

## Version 14

Introduced in TileDB 2.10

* Fragments can have timestamp files. The _Includes timestamps_ field was added to the [fragment metadata footer](./fragment.md#footer).

## Version 13

Introduced in TileDB 2.9

* The [dictionary filter](./filters/dictionary_encoding.md) was added.

## Version 12

Introduced in TileDB 2.8

* The [array file hierarchy](./array_file_hierarchy.md) was updated to store fragments, commits and consolidated fragment metadata in separate subdirectories.
* The extension of commit files was changed to `.wrt`.

## Version 11

Introduced in TileDB 2.7

* Fragment metadata contain [metadata](./fragment.md#tile-mins-maxes) (min/max value, sum, null count) for each tile.

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
