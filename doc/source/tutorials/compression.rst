Compression
===========

In this tutorial you will learn more about compression in TileDB. It is
highly recommended that you read the tutorials on attributes, filtering, and
tiling dense/sparse arrays first.


Basic concepts and definitions
------------------------------

.. toggle-header::
    :header: **Attribute-based compression**

    TileDB allows you to choose a different compression scheme for
    each attribute by using filter lists. Compression in TileDB always occurs
    as a separate filter in a filter list.

.. toggle-header::
    :header: **Compression tradeoff**

    There is no ideal compressor. Each compressor offers a tradeoff
    between compression ratio and compression/decompression speed,
    which highly depends on the array data of your application.

Compressing arrays
------------------

The data types and values of the array cells most likely vary across attributes.
Therefore, it is desirable to allow defining a different compression scheme per
attribute, since different compressors may be more effective for different
types of data. Compression in TileDB always occurs as a separate filter in a
filter list. You specify the compressor in the filter list when defining an
attribute upon array creation.

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        Context ctx;
        ArraySchema schema(ctx, TILEDB_DENSE);
        auto a = Attribute::create<int>(ctx, "a");
        FilterList filters(ctx);
        filters.add_filter({ctx, TILEDB_FILTER_GZIP});
        a.filters(filters);
        schema.add_attribute(a);

   .. tab-container:: python
      :title: Python

      .. code-block:: python

        ctx = tiledb.Ctx()
        attr = tiledb.Attr(ctx, name="a", dtype=np.int32,
                           filters=tiledb.FilterList(ctx, [tiledb.GzipFilter(ctx)]))
        schema = tiledb.ArraySchema(ctx, domain=dom, sparse=False, attrs=[attr])

TileDB offers a variety of compressors to choose from (see below). Moreover, you
can adjust the compression level (by setting the ``TILEDB_COMPRESSION_LEVEL``
option on the compression filter, where ``-1`` means "default compression
level"). If a compressor filter is not added to an attribute, TileDB will not
compress that attribute.

You can also specify a different compressor for the coordinates
tiles as follows (the default is ZSTD with default compression level):

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        FilterList coords_filters(ctx);
        coords_filters.add_filter({ctx, TILEDB_LZ4});
        schema.set_coords_filter_list(coords_filters);

   .. tab-container:: python
      :title: Python

      .. code-block:: python

        schema = tiledb.ArraySchema(..., coords_filters=tiledb.FilterList(ctx, [tiledb.LZ4Filter(ctx)]))

To maximize the effect of compression on coordinates, TileDB first
unzips the coordinates tuples and groups the coordinates along
the dimensions. For instance, coordinates ``(1,2)``, ``(1,3)``, ``(1,5)``
will be laid out as ``1 1 1 2 3 5`` prior to passing them into
the compressor. This layout allows for the slowly varying coordinate
values to be grouped together, which leads to better compressibility
(as these values will likely be similar).

Recall that there are two data files created for a variable-length
attribute; one that stores the actual cell values and one that
stores the *starting offsets* of the cell values in the first file.
TileDB allows you to even specify a compressor for the offsets
data tiles (the default is ZSTD with default compression level):

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        FilterList offsets_filters(ctx);
        offsets_filters.add_filter({ctx, TILEDB_BZIP2});
        schema.set_offsets_filter_list(offsets_filters);

   .. tab-container:: python
      :title: Python

      .. code-block:: python

        schema = tiledb.ArraySchema(..., offsets_filters=tiledb.FilterList(ctx, [tiledb.Bzip2Filter(ctx)]))

Choosing a compressor
---------------------

TileDB offers a variety of compressors to choose from:

    -  `GZIP <http://www.zlib.net/>`__
    -  `Zstandard <http://facebook.github.io/zstd/>`__
    -  `LZ4 <https://github.com/lz4/lz4>`__
    -  `RLE <https://en.wikipedia.org/wiki/Run-length_encoding>`__
    -  `Bzip2 <http://www.bzip.org/>`__
    -  Double-delta

TileDB implements its own version of **double-delta** compression. It is
similar to the one presented in `Facebook’s
Gorilla <http://www.vldb.org/pvldb/vol8/p1816-teller.pdf>`__ system. The
difference is that TileDB uses a fixed bitsize for all values (in
contrast to Gorilla’s variable bitsize). This makes the implementation a
bit simpler, but also allows computing directly on the compressed data
(which we are exploring in the future).

TileDB utilizes a blocking technique that divides the data in blocks that
are small enough to fit in L1 cache of modern processors and perform
compression/decompression there. This reduces the activity on the
memory bus and allows leveraging the SIMD capabilities of the processor, thus
leading to a performance speed up. TileDB also allows you to apply a shuffle
filter before compression, which can result in improved compression ratio.

Choosing the right compressor for your application is quite challenging,
as the effectiveness of a compressor heavily depends on the data being
compressed. Moreover, each compressor offers a *tradeoff between compression
ratio and compression/decompression speed*. Here are a couple of
benchmarks that demonstrate this tradeoff:

    -  `Squash Compression Benchmark <https://quixdb.github.io/squash-benchmark/>`__
    -  `Genotype Compression Benchmark <http://alimanfoo.github.io/2016/09/21/genotype-compression-benchmark.html>`__

What we recommend is to ingest a subset of your data into an array,
and test with various different compressors for each of your attributes,
in order to determine what compression ratio and speed is satisfactory for
your application.


Compression and performance
---------------------------

Compression greatly affects performance; compression/decompression impacts
the writing/reading speed, whereas the compression ratio influences
the read/write I/O time in addition of
course to storage consumption. As stated above, the choice of compressor
is important for performance, but there is always a tradeoff between
compression ratio and speed, which you need to adjust based on your
application. Luckily for you, TileDB *parallelizes* internally both
compression and decompression (and filtering in general).
However, parallelization takes effect when the data tile to be filtered is large
enough. See :ref:`performance/introduction` for more information on TileDB
performance and how to tune it.