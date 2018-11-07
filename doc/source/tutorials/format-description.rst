.. _format-description:

Format description
==================

This document describes the persisted format of TileDB arrays. The first section
describes the tile-based format shared by all files written by TileDB
(including attribute data, array metadata, etc), as well as the metadata added
to each tile for filtering. The second section describes the byte format of
the tile data written in each file in a TileDB array.

The current TileDB format version number is **2** (``uint32_t``).

.. note::

    All data written by TileDB and referenced in this document is
    **little-endian**.

Tile-based format
-----------------

File format
~~~~~~~~~~~

Every file written by TileDB, including attribute data, array schema and other
metadata, is internally treated as a byte array divided into tiles. I/O
operations typically occur at tile granularity. All tiles in TileDB can pass
through a filter pipeline.

Any **file** written by TileDB therefore has the following on-disk format:

+-------------------------+----------------------+-----------------------+
| **Field**               | **Type**             | **Description**       |
+=========================+======================+=======================+
| Tile 1                  | ``Tile``             | Array of bytes        |
|                         |                      | containing the first  |
|                         |                      | tile’s (filtered)     |
|                         |                      | data.                 |
+-------------------------+----------------------+-----------------------+
| ...                     | ...                  | ...                   |
+-------------------------+----------------------+-----------------------+
| Tile N                  | ``Tile``             | Array of bytes        |
|                         |                      | containing the nth    |
|                         |                      | tile’s (filtered)     |
|                         |                      | data.                 |
+-------------------------+----------------------+-----------------------+

**Every file is at least one tile**. The current version of TileDB writes two
types of ``Tile``: **attribute tiles** and **generic tiles**. Attribute tiles
contain attribute data, offsets, or coordinates. Generic tiles contain generic
non-attribute data, of which there are two kinds: the array schema and the
fragment metadata.

The fragment metadata and array schema files consist of a single generic tile.
Attribute, offsets and coordinate files consist of one or more attribute tiles.

Each generic tile contains some additional metadata in a header structure. A
generic ``Tile`` has the on-disk format:

+-------------------------+----------------------+---------------------------------+
| **Field**               | **Type**             | **Description**                 |
+=========================+======================+=================================+
| Version number          | ``uint32_t``         | Format version number of the    |
|                         |                      | generic tile.                   |
+-------------------------+----------------------+---------------------------------+
| Persisted size          | ``uint64_t``         | Persisted (e.g. compressed)     |
|                         |                      | size of the tile.               |
+-------------------------+----------------------+---------------------------------+
| Tile size               | ``uint64_t``         | In-memory (e.g. uncompressed)   |
|                         |                      | size of the tile.               |
+-------------------------+----------------------+---------------------------------+
| Datatype                | ``uint8_t``          | Datatype of the tile.           |
+-------------------------+----------------------+---------------------------------+
| Cell size               | ``uint64_t``         | Cell size of the tile.          |
+-------------------------+----------------------+---------------------------------+
| Encryption type         | ``uint8_t``          | Type of encryption used in      |
|                         |                      | filtering the tile.             |
+-------------------------+----------------------+---------------------------------+
| Filter pipeline size    | ``uint32_t``         | Number of bytes in the          |
|                         |                      | serialized ``FilterPipeline``.  |
+-------------------------+----------------------+---------------------------------+
| Filter pipeline         | ``FilterPipeline``   | Filter pipeline used to         |
|                         |                      | filter the tile.                |
+-------------------------+----------------------+---------------------------------+
| Tile data               | ``uint8_t[]``        | Array of filtered tile data     |
|                         |                      | bytes.                          |
+-------------------------+----------------------+---------------------------------+

Attribute tiles do not store extra metadata per tile (attribute metadata is
instead stored in the array schema and fragment metadata files), so an attribute
``Tile`` has the on-disk format:

+-------------------------+----------------------+---------------------------------+
| **Field**               | **Type**             | **Description**                 |
+=========================+======================+=================================+
| Tile data               | ``uint8_t[]``        | Array of filtered tile data     |
|                         |                      | bytes.                          |
+-------------------------+----------------------+---------------------------------+

A coordinate ``Tile`` is additionally processed by "splitting" coordinate tuples
across dimensions. As an example, 3D coordinates are given by users in the form
``[x1, y1, z1, x2, y2, z2, ...]``. Before being filtered, the coordinate values
stored in the tile data are rearranged to be
``[x1, x2, ..., xN, y1, y2, ..., yN, z1, z2, ..., zN]``.

To account for filtering, some additional metadata is prepended in the tile data
bytes in each tile. This filter pipeline metadata informs TileDB how the
following tile bytes should be treated (for example, how to decompress it when
reading from disk).

The array of filtered tile data bytes (for any type of tile) has the on-disk format:

+-------------------------+----------------------+------------------------------+
| **Field**               | **Type**             | **Description**              |
+=========================+======================+==============================+
| Number of chunks        | ``uint64_t``         | Number of chunks in the Tile |
+-------------------------+----------------------+------------------------------+
| Chunk 1                 | ``TileChunk``        | First chunk in the tile      |
+-------------------------+----------------------+------------------------------+
| ...                     | ...                  | ...                          |
+-------------------------+----------------------+------------------------------+
| Chunk N                 | ``TileChunk``        | Nth chunk in the tile        |
+-------------------------+----------------------+------------------------------+

Internally tile data is divided into "chunks." Every tile is at least one chunk.
A ``TileChunk`` has the following on-disk format:

+-------------------------+----------------------+-------------------------------------------+
| **Field**               | **Type**             | **Description**                           |
+=========================+======================+===========================================+
| Original length         | ``uint32_t``         | The original (unfiltered) number of bytes |
| of chunk                |                      | of chunk data.                            |
+-------------------------+----------------------+-------------------------------------------+
| Filtered length         | ``uint32_t``         | The serialized (filtered) number of bytes |
| of chunk                |                      | of chunk data.                            |
+-------------------------+----------------------+-------------------------------------------+
| Chunk metadata          | ``uint32_t``         | Number of bytes in the chunk metadata     |
| length                  |                      |                                           |
+-------------------------+----------------------+-------------------------------------------+
| Chunk metadata          | ``uint8_t[]``        | Chunk metadata bytes                      |
+-------------------------+----------------------+-------------------------------------------+
| Chunk filtered          | ``uint8_t[]``        | Filtered chunk bytes                      |
| data                    |                      |                                           |
+-------------------------+----------------------+-------------------------------------------+

The metadata added to a chunk depends on the sequence of filters in the
pipeline used to filter the containing tile.

If a pipeline used to filter tiles is empty (contains no filters), the
tile is still divided into chunks and serialized according to the above
format. In this case there are chunk metadata bytes (since there are no
filters to add metadata), and the filtered bytes are the same as
original bytes.

Chunk metadata and filtered data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The “chunk metadata bytes” before the actual chunk data bytes depend on
the particular sequence of filters in the pipeline. In the simple case,
each filter will simply concatenate its metadata to the chunk metadata
region. Because some filters in the pipeline may wish to filter the
metadata of previous filters (e.g. compression, where it is beneficial
to compress previous filters’ metadata in addition to the actual chunk
data), the ordering of filters also impacts the metadata that is
eventually written to disk.

The “chunk filtered data” bytes contain the final bytes of the chunk
after being passed through the entire pipeline. When reading tiles from
disk, these filtered bytes are passed through the filter pipeline in the
reverse order.

Internally, any filter in a filter pipeline produces two arrays of data
as output: a metadata byte array and a filtered data byte array.
Additionally, these output byte arrays can be arbitrarily separated into
“parts” by any filter. Typically, when a next filter receives the output
of the previous filter as its input, it will filter each “part”
independently.

First we will look at the output of the filters individually.

Byteshuffle filter
~~~~~~~~~~~~~~~~~~

The byteshuffle filter does not filter input
metadata, and the output data is guaranteed to be the same length as the
input data.

The byteshuffle filter produces output metadata in the format:

+-------------------------+----------------------+--------------------------------+
| **Field**               | **Type**             | **Description**                |
+=========================+======================+================================+
| Number of parts         | ``uint32_t``         | Number of data parts           |
+-------------------------+----------------------+--------------------------------+
| Length of part 1        | ``uint32_t``         | Number of bytes in data part 1 |
+-------------------------+----------------------+--------------------------------+
| ...                     | ...                  | ...                            |
+-------------------------+----------------------+--------------------------------+
| Length of part N        | ``uint32_t``         | Number of bytes in data part N |
+-------------------------+----------------------+--------------------------------+

The byteshuffle filter produces output data in the format:

+-------------------------+----------------------+--------------------------+
| **Field**               | **Type**             | **Description**          |
+=========================+======================+==========================+
| Part 1                  | ``uint8_t[]``        | Byteshuffled data part 1 |
+-------------------------+----------------------+--------------------------+
| ...                     | ...                  | ...                      |
+-------------------------+----------------------+--------------------------+
| Part N                  | ``uint8_t[]``        | Byteshuffled data part N |
+-------------------------+----------------------+--------------------------+

Bitshuffle filter
~~~~~~~~~~~~~~~~~

The bitshuffle filter does not filter input
metadata.

The bitshuffle filter produces output metadata in the format:

+-------------------------+----------------------+--------------------------------+
| **Field**               | **Type**             | **Description**                |
+=========================+======================+================================+
| Number of parts         | ``uint32_t``         | Number of data parts           |
+-------------------------+----------------------+--------------------------------+
| Length of part 1        | ``uint32_t``         | Number of bytes in data part 1 |
+-------------------------+----------------------+--------------------------------+
| ...                     | ...                  | ...                            |
+-------------------------+----------------------+--------------------------------+
| Length of part N        | ``uint32_t``         | Number of bytes in data part N |
+-------------------------+----------------------+--------------------------------+

The bitshuffle filter produces output data in the format:

+-------------------------+----------------------+-------------------------+
| **Field**               | **Type**             | **Description**         |
+=========================+======================+=========================+
| Part 1                  | ``uint8_t[]``        | Bitshuffled data part 1 |
+-------------------------+----------------------+-------------------------+
| ...                     | ...                  | ...                     |
+-------------------------+----------------------+-------------------------+
| Part N                  | ``uint8_t[]``        | Bitshuffled data part N |
+-------------------------+----------------------+-------------------------+

Bit width reduction filter
~~~~~~~~~~~~~~~~~~~~~~~~~~

The bit width reduction filter does not
filter input metadata.

The bit width reduction filter produces output metadata in the format:

+-------------------------+----------------------+--------------------------------+
| **Field**               | **Type**             | **Description**                |
+=========================+======================+================================+
| Length of input         | ``uint32_t``         | Original input number of bytes |
+-------------------------+----------------------+--------------------------------+
| Number of windows       | ``uint32_t``         | Number of windows in output    |
+-------------------------+----------------------+--------------------------------+
| Window 1 metadata       | ``WindowMD``         | Metadata for window 1          |
+-------------------------+----------------------+--------------------------------+
| ...                     | ...                  | ...                            |
+-------------------------+----------------------+--------------------------------+
| Window N metadata       | ``WindowMD``         | Metadata for window N          |
+-------------------------+----------------------+--------------------------------+

The type ``WindowMD`` has the format:

+-------------------------+----------------------+----------------------------------------------------+
| **Field**               | **Type**             | **Description**                                    |
+=========================+======================+====================================================+
| Window                  | ``T``                | Offset applied to values in the output window,     |
| value                   |                      | where ``T`` is the original datatype of the tile   |
| offset                  |                      | values.                                            |
+-------------------------+----------------------+----------------------------------------------------+
| Bit width               | ``uint8_t``          | Number of bits in the new datatype of the values   |
| of reduced              |                      | in the output window                               |
| type                    |                      |                                                    |
+-------------------------+----------------------+----------------------------------------------------+
| Window                  | ``uint32_t``         | Number of bytes in output window data.             |
| length                  |                      |                                                    |
+-------------------------+----------------------+----------------------------------------------------+

The bit width reduction filter produces output data in the format:

+-------------------------+----------------------+-------------------------------------------------+
| **Field**               | **Type**             | **Description**                                 |
+=========================+======================+=================================================+
| Window 1                | ``uint8_t[]``        | Window 1 data (possibly-reduced width elements) |
+-------------------------+----------------------+-------------------------------------------------+
| ...                     | ...                  | ...                                             |
+-------------------------+----------------------+-------------------------------------------------+
| Window N                | ``uint8_t[]``        | Window N data (possibly-reduced width elements) |
+-------------------------+----------------------+-------------------------------------------------+

Positive delta encoding filter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The positive-delta encoding filter does not filter input metadata.

The positive-delta encoding filter produces output metadata in the
format:

+-------------------------+----------------------+--------------------------------+
| **Field**               | **Type**             | **Description**                |
+=========================+======================+================================+
| Number of windows       | ``uint32_t``         | Number of windows in output    |
+-------------------------+----------------------+--------------------------------+
| Window 1 metadata       | ``WindowMD``         | Metadata for window 1          |
+-------------------------+----------------------+--------------------------------+
| ...                     | ...                  | ...                            |
+-------------------------+----------------------+--------------------------------+
| Window N metadata       | ``WindowMD``         | Metadata for window N          |
+-------------------------+----------------------+--------------------------------+

The type ``WindowMD`` has the format:

+-------------------------+----------------------+---------------------------------------------------+
| **Field**               | **Type**             | **Description**                                   |
+=========================+======================+===================================================+
| Window                  | ``T``                | Offset applied to values in the output window,    |
| value delta             |                      | where ``T`` is the datatype of the tile values.   |
| offset                  |                      |                                                   |
+-------------------------+----------------------+---------------------------------------------------+
| Window                  | ``uint32_t``         | Number of bytes in output window data.            |
| length                  |                      |                                                   |
+-------------------------+----------------------+---------------------------------------------------+

The positive-delta encoding filter produces output data in the format:

+-------------------------+----------------------+-----------------------------+
| **Field**               | **Type**             | **Description**             |
+=========================+======================+=============================+
| Window 1                | ``T[]``              | Window 1 delta-encoded data |
+-------------------------+----------------------+-----------------------------+
| ...                     | ...                  | ...                         |
+-------------------------+----------------------+-----------------------------+
| Window N                | ``T[]``              | Window N delta-encoded data |
+-------------------------+----------------------+-----------------------------+

Compression filters
~~~~~~~~~~~~~~~~~~~

The compression filters do filter input
metadata.

The compression filters produce output metadata in the format:

+-------------------------+----------------------+--------------------------------------+
| **Field**               | **Type**             | **Description**                      |
+=========================+======================+======================================+
| Number of               | ``uint32_t``         | Number of input metadata parts that  |
| metadata parts          |                      | were compressed                      |
+-------------------------+----------------------+--------------------------------------+
| Number of data          | ``uint32_t``         | Number of input data parts that were |
| parts                   |                      | compressed                           |
+-------------------------+----------------------+--------------------------------------+
| Metadata part 1         | ``CompressedPartMD`` | Metadata about the first metadata    |
+-------------------------+----------------------+--------------------------------------+
| ...                     | ...                  | ...                                  |
+-------------------------+----------------------+--------------------------------------+
| Metadata part N         | ``CompressedPartMD`` | Metadata about the nth metadata part |
+-------------------------+----------------------+--------------------------------------+
| Data part 1             | ``CompressedPartMD`` | Metadata about the first data part   |
+-------------------------+----------------------+--------------------------------------+
| ...                     | ...                  | ...                                  |
+-------------------------+----------------------+--------------------------------------+
| Data part N             | ``CompressedPartMD`` | Metadata about the nth data part     |
+-------------------------+----------------------+--------------------------------------+

The type ``CompressedPartMD`` has the format:

+-------------------------+----------------------+-----------------------------------------+
| **Field**               | **Type**             | **Description**                         |
+=========================+======================+=========================================+
| Part original           | ``uint32_t``         | Input length of the part (before        |
| length                  |                      | compression)                            |
+-------------------------+----------------------+-----------------------------------------+
| Part compressed         | ``uint32_t``         | Compressed length of the part           |
| length                  |                      |                                         |
+-------------------------+----------------------+-----------------------------------------+

The compression filters then produce output data in the format:

+-------------------------+----------------------+-----------------------------------+
| **Field**               | **Type**             | **Description**                   |
+=========================+======================+===================================+
| Metadata part 0         | ``uint8_t[]``        | Compressed bytes of the first     |
| compressed bytes        |                      | metadata part                     |
+-------------------------+----------------------+-----------------------------------+
| ...                     | ...                  | ...                               |
+-------------------------+----------------------+-----------------------------------+
| Metadata part N         | ``uint8_t[]``        | Compressed bytes of the nth       |
| compressed bytes        |                      | metadata part                     |
+-------------------------+----------------------+-----------------------------------+
| Data part 0 compressed  | ``uint8_t[]``        | Compressed bytes of the first     |
| bytes                   |                      | data part                         |
+-------------------------+----------------------+-----------------------------------+
| ...                     | ...                  | ...                               |
+-------------------------+----------------------+-----------------------------------+
| Data part N compressed  | ``uint8_t[]``        | Compressed bytes of the nth data  |
| bytes                   |                      | part                              |
+-------------------------+----------------------+-----------------------------------+


Internal formats
----------------

As mentioned, any file written by TileDB including attribute data, array schema,
fragment metadata, coordinates or offsets, is treated as an array of bytes and
broken up into separate tiles before writing. The previous section defined the
on-disk format of files written by TileDB in terms of tiles and filter metadata.

This section describes the data contained in each file, independent of any
tiling. In other words, the format structures defined here comprise unfiltered
tile data, which is treated as an array of bytes, broken into ``TileChunks``,
filtered, and written to disk with the format described in the previous section.
We refer to the byte format of unfiltered tile data as the "internal" format.

Array schema file
~~~~~~~~~~~~~~~~~

The file ``__array_schema.tdb`` has the internal format:

+-------------------------+----------------------+------------------------------+
| **Field**               | **Type**             | **Description**              |
+=========================+======================+==============================+
| Array schema            | ``ArraySchema``      | The serialized array schema. |
+-------------------------+----------------------+------------------------------+

The type ``ArraySchema`` has the internal format:

+-------------------------+----------------------+----------------------------------------------+
| **Field**               | **Type**             | **Description**                              |
+=========================+======================+==============================================+
| Array                   | ``uint32_t``         | Format version number of the array schema    |
| version                 |                      |                                              |
+-------------------------+----------------------+----------------------------------------------+
| Array                   | ``uint8_t``          | Dense or sparse                              |
| type                    |                      |                                              |
+-------------------------+----------------------+----------------------------------------------+
| Tile                    | ``uint8_t``          | Row or column major                          |
| order                   |                      |                                              |
+-------------------------+----------------------+----------------------------------------------+
| Cell                    | ``uint8_t``          | Row or column major                          |
| order                   |                      |                                              |
+-------------------------+----------------------+----------------------------------------------+
| Capacity                | ``uint64_t``         | For sparse arrays, the data tile capacity    |
|                         |                      |                                              |
+-------------------------+----------------------+----------------------------------------------+
| Coords                  | ``FilterPipeline``   | The filter pipeline used for coordinate      |
| filters                 |                      | tiles                                        |
+-------------------------+----------------------+----------------------------------------------+
| Offsets                 | ``FilterPipeline``   | The filter pipeline used for cell var-len    |
| filters                 |                      | offset tiles                                 |
+-------------------------+----------------------+----------------------------------------------+
| Domain                  | ``Domain``           | The array domain                             |
|                         |                      |                                              |
+-------------------------+----------------------+----------------------------------------------+
| Num                     | ``uint32_t``         | Number of attributes in the array            |
| attributes              |                      |                                              |
+-------------------------+----------------------+----------------------------------------------+
| Attribute               | ``Attribute``        | First attribute                              |
| 1                       |                      |                                              |
+-------------------------+----------------------+----------------------------------------------+
| ...                     | ...                  | ...                                          |
+-------------------------+----------------------+----------------------------------------------+
| Attribute               | ``Attribute``        | Nth attribute                                |
| N                       |                      |                                              |
+-------------------------+----------------------+----------------------------------------------+

The type ``Domain`` has the internal format:

+-------------------------+----------------------+--------------------------------------------------+
| **Field**               | **Type**             | **Description**                                  |
|                         |                      |                                                  |
+=========================+======================+==================================================+
| Type                    | ``uint8_t``          | Datatype of dimension values (``TILEDB_INT32``,  |
|                         |                      | ``TILEDB_FLOAT64``, etc).                        |
+-------------------------+----------------------+--------------------------------------------------+
| Num                     | ``uint32_t``         | Dimensionality/rank of the domain                |
| dimensions              |                      |                                                  |
+-------------------------+----------------------+--------------------------------------------------+
| Dimension               | ``Dimension``        | First dimension                                  |
| 1                       |                      |                                                  |
+-------------------------+----------------------+--------------------------------------------------+
| ...                     | ...                  | ...                                              |
+-------------------------+----------------------+--------------------------------------------------+
| Dimension               | ``Dimension``        | Nth dimension                                    |
| N                       |                      |                                                  |
+-------------------------+----------------------+--------------------------------------------------+

The type ``Dimension`` has the internal format:

+-------------------------+----------------------+----------------------------------------------------+
| **Field**               | **Type**             | **Description**                                    |
|                         |                      |                                                    |
+=========================+======================+====================================================+
| Dimension               | ``uint32_t``         | Number of characters in dimension name (the        |
| name                    |                      | following array)                                   |
| length                  |                      |                                                    |
+-------------------------+----------------------+----------------------------------------------------+
| Dimension               | ``char[]``           | Dimension name character array                     |
| name                    |                      |                                                    |
+-------------------------+----------------------+----------------------------------------------------+
| Domain                  | ``uint8_t[]``        | Byte array of length ``2*sizeof(DimT)``, storing   |
|                         |                      | the min, max values of the dimension (of type      |
|                         |                      | ``DimT``).                                         |
+-------------------------+----------------------+----------------------------------------------------+
| Null                    | ``uint8_t``          | ``1`` if the dimension has a null tile extent,     |
| tile                    |                      | else ``0``.                                        |
| extent                  |                      |                                                    |
+-------------------------+----------------------+----------------------------------------------------+
| Tile                    | ``uint8_t[]``        | Byte array of length ``sizeof(DimT)``, storing the |
| extent                  |                      | space tile extent of this dimension.               |
+-------------------------+----------------------+----------------------------------------------------+


The type ``Attribute`` has the internal format:

+-------------------------+----------------------+-----------------------------------------------------+
| **Field**               | **Type**             | **Description**                                     |
|                         |                      |                                                     |
+=========================+======================+=====================================================+
| Attribute               | ``uint32_t``         | Number of characters in attribute name (the         |
| name                    |                      | following array)                                    |
| length                  |                      |                                                     |
+-------------------------+----------------------+-----------------------------------------------------+
| Attribute               | ``char[]``           | Attribute name character array                      |
| name                    |                      |                                                     |
+-------------------------+----------------------+-----------------------------------------------------+
| Attribute               | ``uint8_t``          | Datatype of the attribute values                    |
| datatype                |                      |                                                     |
+-------------------------+----------------------+-----------------------------------------------------+
| Cell val num            | ``uint32_t``         | Number of attribute values per cell. For            |
|                         |                      | variable-length attributes, this is                 |
|                         |                      | ``std::numeric_limits<uint32_t>::max()``            |
+-------------------------+----------------------+-----------------------------------------------------+
| Filters                 | ``FilterPipeline``   | The filter pipeline used on attribute value tiles   |
+-------------------------+----------------------+-----------------------------------------------------+

The type ``FilterPipeline`` has the internal format:

+-------------------------+----------------------+----------------------------------+
| **Field**               | **Type**             | **Description**                  |
+=========================+======================+==================================+
| Max chunk size          | ``uint32_t``         | Maximum chunk size within a tile |
+-------------------------+----------------------+----------------------------------+
| Num filters             | ``uint32_t``         | Number of filters in pipeline    |
+-------------------------+----------------------+----------------------------------+
| Filter 1                | ``Filter``           | First filter                     |
+-------------------------+----------------------+----------------------------------+
| ...                     | ...                  | ...                              |
+-------------------------+----------------------+----------------------------------+
| Filter N                | ``Filter``           | Nth filter                       |
+-------------------------+----------------------+----------------------------------+

The type ``Filter`` has the internal format:

+-------------------------+----------------------+---------------------------------------------------+
| **Field**               | **Type**             | **Description**                                   |
|                         |                      |                                                   |
+=========================+======================+===================================================+
| Filter                  | ``uint8_t``          | Type of filter (e.g. ``TILEDB_FILTER_BZIP2``)     |
| type                    |                      |                                                   |
+-------------------------+----------------------+---------------------------------------------------+
| Filter                  | ``uint32_t``         | Number of bytes in filter metadata (the following |
| metadata                |                      | array) — may be 0.                                |
| size                    |                      |                                                   |
+-------------------------+----------------------+---------------------------------------------------+
| Filter                  | ``uint8_t[]``        | Filter metadata, specific to each filter. E.g.    |
| metadata                |                      | compression level for compression filters.        |
+-------------------------+----------------------+---------------------------------------------------+

The filter metadata contains configuration parameters for the filters
that do not change once the array schema has been created. For the
compression filters (any of the filter types
``TILEDB_FILTER_{GZIP,ZSTD,LZ4,RLE,BZIP2,DOUBLE_DELTA }``)
the filter metadata has the internal format:

+-------------------------+----------------------+-----------------------------------------------+
| **Field**               | **Type**             | **Description**                               |
|                         |                      |                                               |
+=========================+======================+===============================================+
| Compressor              | ``uint8_t``          | Type of compression (e.g. ``TILEDB_BZIP2``)   |
| type                    |                      |                                               |
+-------------------------+----------------------+-----------------------------------------------+
| Compression             | ``int32_t``          | Compression level used (ignored by some       |
| level                   |                      | compressors).                                 |
+-------------------------+----------------------+-----------------------------------------------+

The filter metadata for ``TILEDB_FILTER_BIT_WIDTH_REDUCTION`` has the
internal format:

+-------------------------+----------------------+------------------------------+
| **Field**               | **Type**             | **Description**              |
+=========================+======================+==============================+
| Max window size         | ``uint32_t``         | Maximum window size in bytes |
+-------------------------+----------------------+------------------------------+

The filter metadata for ``TILEDB_FILTER_POSITIVE_DELTA`` has the
internal format:

+-------------------------+----------------------+------------------------------+
| **Field**               | **Type**             | **Description**              |
+=========================+======================+==============================+
| Max window size         | ``uint32_t``         | Maximum window size in bytes |
+-------------------------+----------------------+------------------------------+

The remaining filters (``TILEDB_FILTER_BITSHUFFLE`` and
``TILEDB_FILTER_BYTESHUFFLE``) do not serialize any metadata.

Array lock file
~~~~~~~~~~~~~~~

The file ``__lock.tdb`` is always an empty file on disk.

Fragment metadata file
~~~~~~~~~~~~~~~~~~~~~~

The file ``__fragment_metadata.tdb`` has the internal format:

+-------------------------+----------------------+-----------------------------------+
| **Field**               | **Type**             | **Description**                   |
+=========================+======================+===================================+
| Fragment metadata       | ``FragmentMetadata`` | The serialized fragment metadata. |
+-------------------------+----------------------+-----------------------------------+

The type ``FragmentMetadata`` has the internal format:

+-------------------------+----------------------+--------------------------------------------------------+
| **Field**               | **Type**             | **Description**                                        |
+=========================+======================+========================================================+
| Version                 | ``uint32_t``         | Format version number of the fragment.                 |
| number                  |                      |                                                        |
+-------------------------+----------------------+--------------------------------------------------------+
| Non-empty               | ``uint64_t``         | Number of bytes in two coordinate tuples (i.e. depends |
| domain                  |                      | on rank and domain type of array).                     |
| size                    |                      |                                                        |
+-------------------------+----------------------+--------------------------------------------------------+
| Non-empty               | ``uint8_t[]``        | Byte array of two coordinate tuples storing the        |
| domain                  |                      | min/max coords of a bounding box surrounding the       |
|                         |                      | non-empty domain of the fragment.                      |
+-------------------------+----------------------+--------------------------------------------------------+
| Num MBRs                | ``uint64_t``         | Number of MBRs in fragment                             |
+-------------------------+----------------------+--------------------------------------------------------+
| MBR 1                   | ``uint8_t[]``        | Byte array of two coordinate tuples storing the        |
|                         |                      | min/max coords of the first MBR                        |
+-------------------------+----------------------+--------------------------------------------------------+
| ...                     | ...                  | ...                                                    |
+-------------------------+----------------------+--------------------------------------------------------+
| MBR N                   | ``uint8_t[]``        | Byte array of two coordinate tuples storing the        |
|                         |                      | min/max coords of the nth MBR                          |
+-------------------------+----------------------+--------------------------------------------------------+
| Num                     | ``uint64_t``         | Number of bounding coordinates                         |
| bounding                |                      |                                                        |
| coords                  |                      |                                                        |
+-------------------------+----------------------+--------------------------------------------------------+
| Bounding                | ``uint8_t[]``        | Byte array of two coordinate tuples storing the        |
| coords                  |                      | first/last cell coords in the fragment.                |
+-------------------------+----------------------+--------------------------------------------------------+
| Tile                    | ``TileOffsets``      | The offsets of each tile in the attribute files. For   |
| offsets                 |                      | var-len attributes, the offsets of the offset tiles.   |
+-------------------------+----------------------+--------------------------------------------------------+
| Tile var                | ``TileOffsets``      | The offsets of each var-len tile in the attribute      |
| offsets                 |                      | files.                                                 |
+-------------------------+----------------------+--------------------------------------------------------+
| Tile var                | ``TileOffsets``      | The var-len attribute tile sizes                       |
| sizes                   |                      |                                                        |
+-------------------------+----------------------+--------------------------------------------------------+
| Last tile               | ``uint64_t``         | For sparse arrays, the number of cells in the last     |
| cell num                |                      | tile in the fragment.                                  |
+-------------------------+----------------------+--------------------------------------------------------+
| File                    | ``uint64_t[]``       | The size in bytes of each attribute file in the        |
| sizes                   |                      | fragment, including coords. For var-length attributes, |
|                         |                      | this is the size of the offsets file.                  |
+-------------------------+----------------------+--------------------------------------------------------+
| File                    | ``uint64_t[]``       | The size in bytes of each var-length attribute file in |
| var                     |                      | the fragment.                                          |
| sizes                   |                      |                                                        |
+-------------------------+----------------------+--------------------------------------------------------+

The type ``TileOffsets`` has the internal format:

+-------------------------+----------------------+-----------------------------------------+
| **Field**               | **Type**             | **Description**                         |
|                         |                      |                                         |
+=========================+======================+=========================================+
| Num tile offsets,       | ``uint64_t``         | Number of tiles in the fragment for     |
| attribute 1             |                      | attribute 1.                            |
|                         |                      |                                         |
+-------------------------+----------------------+-----------------------------------------+
| Tile offsets,           | ``uint64_t``         | Offset of each tile in the attribute    |
| attribute 1             |                      | file for attribute 1.                   |
|                         |                      |                                         |
+-------------------------+----------------------+-----------------------------------------+
| ...                     | ...                  | ...                                     |
+-------------------------+----------------------+-----------------------------------------+
| Num tile offsets,       | ``uint64_t``         | Number of tiles in the fragment for     |
| attribute N             |                      | attribute N.                            |
|                         |                      |                                         |
+-------------------------+----------------------+-----------------------------------------+
| Tile offsets,           | ``uint64_t[]``       | Offset of each tile in the attribute    |
| attribute N             |                      | file for attribute N.                   |
|                         |                      |                                         |
+-------------------------+----------------------+-----------------------------------------+

The type ``TileSizes`` is identical to ``TileOffsets`` except
``uint64_t`` size values are stored instead of ``uint64_t`` offset
values.

Coords file
~~~~~~~~~~~

Within a sparse fragment, the file ``__coords.tdb`` has the following
internal format:

+-------------------------+----------------------+----------------------------------------------------+
| **Field**               | **Type**             | **Description**                                    |
+=========================+======================+====================================================+
| Dim 1                   | ``DimT[]``           | Array of the first dimension values for all        |
| coordinate              |                      | coordinate tuples of cells in the fragment.        |
| values                  |                      |                                                    |
+-------------------------+----------------------+----------------------------------------------------+
| ...                     | ...                  |                                                    |
+-------------------------+----------------------+----------------------------------------------------+
| Dim N                   | ``DimT[]``           | Array of the nth dimension values for all          |
| coordinate              |                      | coordinate tuples of cells in the fragment.        |
| values                  |                      |                                                    |
+-------------------------+----------------------+----------------------------------------------------+

Attribute file
~~~~~~~~~~~~~~

Within a fragment, each fixed-length attribute named ``<attr>`` has a
file ``<attr>.tdb`` with the internal format:

+-------------------------+----------------------+----------------------------------------------+
| **Field**               | **Type**             | **Description**                              |
+=========================+======================+==============================================+
| Attribute values        | ``AttrT[]``          | Array of the attribute values for all cells. |
+-------------------------+----------------------+----------------------------------------------+

Each var-length attribute named ``<attr>`` has two files,
``<attr>_var.tdb`` storing the variable-length data for the attribute in
each cell, and ``<attr>.tdb`` storing the offsets in ``<attr_var>.tdb``
for the data belonging to each cell. ``<attr>.tdb`` has the internal
format:

+-------------------------+----------------------+---------------------------------------------------+
| **Field**               | **Type**             | **Description**                                   |
+=========================+======================+===================================================+
| Attribute               | ``uint64_t``         | Array of the attribute value offsets in           |
| offsets                 |                      | corresponding file ``<attr>_var.tdb``.            |
+-------------------------+----------------------+---------------------------------------------------+

And ``<attr>_var.tdb`` has the format:

+-------------------------+----------------------+----------------------------------------------+
| **Field**               | **Type**             | **Description**                              |
+=========================+======================+==============================================+
| Attribute values        | ``AttrT[]``          | Array of the attribute values for all cells. |
+-------------------------+----------------------+----------------------------------------------+