Compression
===========

Compression types
-----------------

TileDB provides attribute-based compression, and compresses each tile
separately for each attribute. TileDB suppports a wide variety of
compression methods, listed in the table below:

    -  `GZIP <http://www.zlib.net/>`__
    -  `Zstandard <http://facebook.github.io/zstd/>`__
    -  `Blosc <http://blosc.org/>`__
    -  `LZ4 <https://github.com/lz4/lz4>`__
    -  `RLE <https://en.wikipedia.org/wiki/Run-length_encoding>`__
    -  `Bzip2 <http://www.bzip.org/>`__
    -  Double-delta

Note that Blosc comes with multiple compressors; BloscLZ is the default
one, but it works also in combination with LZ4, LZ4HC, Snappy, Zlib, and
Zstandard. TileDB allows the user to specify any of these compressors
with Blosc.

TileDB implements its own version of **double-delta** compression. It is
very similar to the one presented in `Facebook’s
Gorilla <http://www.vldb.org/pvldb/vol8/p1816-teller.pdf>`__ system. The
difference is that TileDB uses a fixed bitsize for all values (in
contrast to Gorilla’s variable bitsize). This makes the implementation a
bit simpler, but also allows computing directly on the compressed data
(which we are exploring in the future).

Note that the compression method must be selected based on the
application, as well as the nature of the data for each attribute. With
support for a wide variety of compressions, TileDB provides flexibility
and enables the user to explore and choose the most appropriate one for
her use case. Moreover, note that TileDB allows the user to set the
compression level (for the compression methods that admit one, e.g.,
GZIP, Blosc, and Zstandard).

Compressing coordinates
-----------------------

Recall that TileDB writes the coordinates of all dimensions in a single
file, treating them as tuples of the form ``c_1``, ``c_2``, …, ``c_d``,
where ``c_i`` is a single coordinate and ``d`` is the number of
dimensions. To make compression more effective, prior to compressing a
coordinate tile, TileDB **splits** the coordinate values into ``d``
vectors, such that the coordinates of each dimension are stored
consecutively in a single vector. This is because the coordinates are
always written sorted in row- or column-major and, thus, it is highly
likely that the coordinates of one dimension are highly repetitive,
considerably facilitating the compressor.

Default compressors
-------------------

By default, TileDB compresses the coordinates and the offsets of the
variable-sized cells with **blosc-zstd** by default. However, TileDB
enables the user to set different compressors for coordinates and
offsets via its API.
