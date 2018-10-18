.. _filters:

Filters
=======

In this tutorial we will learn how to use TileDB filters. It is recommended to
first read the tutorials on sparse arrays, multi-attribute arrays and
variable-length attributes.

.. table:: Full programs
  :widths: auto

  ====================================  =============================================================
  **Program**                           **Links**
  ------------------------------------  -------------------------------------------------------------
  ``filters``                           |filterscpp| |filterspy|
  ====================================  =============================================================

.. |filterscpp| image:: ../figures/cpp.png
   :align: middle
   :width: 30
   :target: {tiledb_src_root_url}/examples/cpp_api/filters.cc

.. |filterspy| image:: ../figures/python.png
   :align: middle
   :width: 25
   :target: {tiledb_py_src_root_url}/examples/filters.py

Basic concepts and definitions
------------------------------

.. toggle-header::
    :header: **Filter**

    A filter is used to transform attribute data before it is written to an
    array. One filter performs one type of data transformation. An example of a
    filter is the bzip2 compression filter, which transforms data by compressing
    it with bzip2.

.. toggle-header::
    :header: **Filter list**

    A filter list is an ordered list of zero or more filters. Filter lists can
    be arbitrarily long, and are used to specify an ordered sequence of data
    transformations that should be performed on attribute data. TileDB allows
    you to set a different filter list for each attribute.

.. toggle-header::
    :header: **Tile chunk**

    Recall that data tiles are the atomic unit of I/O in TileDB. Before writing,
    data tiles are internally divided into multiple chunks by TileDB. The tile
    chunk is the atomic unit of filtering, i.e., each chunk of a data tile is
    filtered separately (and in parallel).


Filter lists
------------

Filtering in TileDB allows you to specify an ordered list of data
transformations, such as compression, that should be applied to attribute data
before it is written to disk. Individual filters are added to filter lists,
which are then set on attributes in the array schema.

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      To start, create a simple sparse array schema.

      .. code-block:: c++

        Context ctx;

        // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
        Domain domain(ctx);
        domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 4}}, 4))
              .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 4}}, 4));

        // The array will be sparse.
        ArraySchema schema(ctx, TILEDB_SPARSE);
        schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

      Next create two fixed-length attributes ``a1`` and ``a2``:

      .. code-block:: c++

        auto a1 = Attribute::create<uint32_t>(ctx, "a1");
        auto a2 = Attribute::create<int32_t>(ctx, "a2");

      Attribute ``a1`` will be filtered by bit width reduction followed by Zstd
      compression. First, create ``Filter`` objects for the two filtering
      operations:

      .. code-block:: c++

        Filter bit_width_reduction(ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION);
        Filter compression_zstd(ctx, TILEDB_FILTER_ZSTD);

      Next, create a ``FilterList`` object and add the two filters. Note that
      the order you add ``Filter`` objects to a ``FilterList`` is the order that
      the filtering will occur.

      .. code-block:: c++

        FilterList a1_filters(ctx);
        a1_filters.add_filter(bit_width_reduction)
            .add_filter(compression_zstd);

      Now set the filter list on attribute ``a1``:

      .. code-block:: c++

        a1.set_filter_list(a1_filters);

      Attribute ``a2`` will be filtered just with a single gzip compression
      filter:

      .. code-block:: c++

        FilterList a2_filters(ctx);
        a2_filters.add_filter({ctx, TILEDB_FILTER_GZIP});
        a2.set_filter_list(a2_filters);

      Note that ``Filter`` and ``FilterList`` objects can be reused. If instead
      you wanted to use the same filter list for ``a2`` as was used in ``a1``
      you could simply do:

      .. code-block:: c++

        a1.set_filter_list(a1_filters);
        a2.set_filter_list(a1_filters);

      Either way, add the attributes to the array schema and create the array:

      .. code-block:: c++

        schema.add_attribute(a1).add_attribute(a2);
        Array::create(array_name, schema);

      TileDB also allows you to set filter lists to be used on the offsets data
      for variable-length attributes as well as the coordinates for sparse
      fragments. For example, to set a filter list for the offsets you could do
      the following:

      .. code-block:: c++

        FilterList offsets_filters(ctx);
        offsets_filters.add_filter({ctx, TILEDB_FILTER_POSITIVE_DELTA})
            .add_filter(bit_width_reduction)
            .add_filter(compression_zstd);
        schema.set_offsets_filter_list(offsets_filters);

   .. tab-container:: python
      :title: Python

      To start, create a simple sparse array domain.

      .. code-block:: python

        # Create a TileDB context
        ctx = tiledb.Ctx()

        # The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
        dom = tiledb.Domain(ctx,
                            tiledb.Dim(ctx, name="rows", domain=(1, 4), tile=4, dtype=np.int32),
                            tiledb.Dim(ctx, name="cols", domain=(1, 4), tile=4, dtype=np.int32))

      Attribute ``a1`` will be filtered by bit width reduction followed by Zstd
      compression. First, create filter objects for the two filtering
      operations:

      .. code-block:: python

        bit_width_reduction = tiledb.BitWidthReductionFilter(ctx)
        compression_zstd = tiledb.ZstdFilter(ctx)

      Next, create a ``FilterList`` object with the two filters. Note that
      the order you specify filter objects to a ``FilterList`` is the order that
      the filtering will occur.

      .. code-block:: python

        a1_filters = tiledb.FilterList(ctx, [bit_width_reduction, compression_zstd])

      Attribute ``a2`` will be filtered just with a single gzip compression
      filter:

      .. code-block:: python

        a2_filters = tiledb.FilterList(ctx, [tiledb.GzipFilter(ctx)])

      Add the attributes to the array schema and create the array:

      .. code-block:: python

        schema = tiledb.ArraySchema(ctx, domain=dom, sparse=True,
                                    attrs=[tiledb.Attr(ctx, name="a1", dtype=np.uint32, filters=a1_filters),
                                           tiledb.Attr(ctx, name="a2", dtype=np.int32, filters=a2_filters)])
        tiledb.SparseArray.create(array_name, schema)

      TileDB also allows you to set filter lists to be used on the offsets data
      for variable-length attributes as well as the coordinates for sparse
      fragments. For example, to set a filter list for the offsets you could do
      the following:

      .. code-block:: python

        offsets_filters = [tiledb.PositiveDeltaFilter(ctx), tiledb.BitWidthReductionFilter(ctx), tiledb.ZstdFilter(ctx)]
        schema = tiledb.ArraySchema(ctx, domain=dom, sparse=True,
                                    offsets_filters=offsets_filters,
                                    attrs=[...]])


Now when data for attributes ``a1`` and ``a2`` is written to the array, the data
will first be transformed by the filter lists you specified in the array schema.

When reading from the array, the filtered data on disk is "unfiltered" through
the same list of filters in reverse, producing the original data.

When filtering the data tiles of an attribute, TileDB stores some
necessary metadata in file ``__fragment_metadata.tdb``, such as the
starting location of each filtered tile and the original tile size
in the case of variable-length attributes (recall that the original tile
size has fixed size for fixed-length attributes in *both* dense and
sparse arrays).

Filter options
--------------

Some filters have additional options that can be configured. For example, you
can set the compression level as an option on the filters that perform
compression.

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      To set the compression level to level 5 on a bzip2 compression filter:

      .. code-block:: c++

        Filter compression_bzip2(ctx, TILEDB_FILTER_BZIP2);
        int level = 5;
        compression_bzip2.set_option(TILEDB_COMPRESSION_LEVEL, &level);

      You can also retrieve option values from filters:

      .. code-block:: c++

        int level_get;
        compression_bzip2.get_option(TILEDB_COMPRESSION_LEVEL, &level_get);
        // Now level_get == 5


   .. tab-container:: python
      :title: Python

      To set the compression level to level 5 on a bzip2 compression filter:

      .. code-block:: python

        compression_bzip2 = tiledb.Bzip2Filter(ctx, level=5)

      You can also retrieve option values from filters:

      .. code-block:: python

        level_get = compression_bzip2.level
        # Now level_get == 5

The options supported by each filter are documented below.

Available filters
-----------------

TileDB supports a number of filters, and more will continue to be added in the
future.

Compression filters
~~~~~~~~~~~~~~~~~~~

There are several filters performing generic compression, which are the following:

* ``TILEDB_FILTER_GZIP``: Compresses with `Gzip <http://www.zlib.net/>`__
* ``TILEDB_FILTER_ZSTD``: Compresses with `Zstandard <http://facebook.github.io/zstd/>`__
* ``TILEDB_FILTER_LZ4``: Compresses with `LZ4 <https://github.com/lz4/lz4>`__
* ``TILEDB_FILTER_RLE``: Compresses with `run-length encoding <https://en.wikipedia.org/wiki/Run-length_encoding>`__
* ``TILEDB_FILTER_BZIP2``: Compresses with `Bzip2 <http://www.bzip.org/>`__
* ``TILEDB_FILTER_DOUBLE_DELTA``: Compresses with double-delta encoding

All of these filters support one filter option to set the compression level,
although some compressors such as RLE currently ignore the setting. The filter
option is:

* ``TILEDB_COMPRESSION_LEVEL`` (type ``int32_t``): The compression level to
  use. Default: -1 (compressor-specific default).

Byteshuffle
~~~~~~~~~~~

The filter ``TILEDB_FILTER_BYTESHUFFLE`` performs byte shuffling of data as a
way to improve compression ratios. The
`byte shuffle implementation <{tiledb_src_root_url}/external/include/blosc/shuffle.h>`_
used by TileDB comes from the `Blosc <blosc.org>`_ project.

The byte shuffling process rearranges the bytes of the input attribute cell
values in a deterministic and reversible manner designed to result in long runs
of similar bytes that can be compressed more effectively by a generic compressor
than the original unshuffled elements. Typically this filter is not used
on its own, but rather immediately followed by a compression filter in a filter
list.

For example, consider three 32-bit unsigned integer values ``1, 2, 3``, which
have the following little-endian representation when stored adjacent in memory:

.. code-block:: none

    0x01 0x00 0x00 0x00 0x02 0x00 0x00 0x00 0x03 0x00 0x00 0x00

The byte shuffle operation will rearrange the bytes of these integer elements in
memory such that the resulting array of bytes will contain each element's first
byte, followed by each element's second byte, etc. After shuffling the bytes
would therefore be:

.. code-block:: none

    0x01 0x02 0x03 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00

Note the longer run of zero-valued bytes, which will compress more efficiently.

The byteshuffle filter does not support any options.

Bitshuffle
~~~~~~~~~~

The filter ``TILEDB_FILTER_BITSHUFFLE`` performs
`bit shuffling <https://www.sciencedirect.com/science/article/pii/S2213133715000694>`_
of data as a way to improve compression ratios. The bitshuffle implementation
used in TileDB comes from `<https://github.com/kiyo-masui/bitshuffle>`_.

Bitshuffling is conceptually very similar to byteshuffling, but operates on the
bit granularity rather than the byte granularity. Shuffling at the bit level
can increase compression ratios even further than the byteshuffle filter, at the
cost of increased computation to perform the shuffle.

Typically this filter is not used on its own, but rather immediately
followed by a compression filter in a filter list.

Positive-delta encoding
~~~~~~~~~~~~~~~~~~~~~~~

The filter ``TILEDB_FILTER_POSITIVE_DELTA`` performs positive-delta encoding.
Positive-delta encoding is a form of delta encoding that only works when the
delta value is positive. Positive-delta encoding can result in better
compression ratios on the encoded data. Typically this filter is not used
on its own, but rather immediately followed by a compression filter in a filter
list.

For example, if the data being filtered was the sequence of integers ``100, 104,
108, 112, ...``, then the resulting positive-encoded data would be ``0, 4, 4,
4, ...``. This encoding is advantageous in that producing long runs of repeated
values can result in better compression ratios, if a compression filter is added
after positive-delta encoding.

The filter operates on a "window" of values at a time, which can help in some
cases to produce longer runs of repeated delta values.

The positive-delta encoding filter supports one option:

* ``TILEDB_POSITIVE_DELTA_MAX_WINDOW`` (type ``uint32_t``): The window size in
  bytes to use. Default: 1024.

.. note::

    Positive-delta encoding is particularly useful for the offsets of
    variable-length attribute data, which by definition will always have
    positive deltas. The above example of the form ``100, 104, 108, 112`` can
    easily arise in the offsets, if for example you have a variable-length
    attribute of 4-byte values with mostly single values per cell instead of a
    variable number.


Bit width reduction
~~~~~~~~~~~~~~~~~~~

The filter ``TILEDB_FILTER_BIT_WIDTH_REDUCTION`` performs bit-width reduction,
which is a form of compression.

Bit-width reduction examines a window of attribute values, and determines if all
of the values in the window can be represented by a datatype of smaller byte size.
If so, the values in the window are rewritten as values of the smaller datatype,
potentially saving several bytes per cell.

For example, consider an attribute with datatype ``uint64_t``. Initially, each
cell of data for that attribute requires 8 bytes of storage. However, if you
know that the actual value of the attribute is often 255 or less, those cells
can be stored using just a single byte in the form of a ``uint8_t``, saving 7
bytes of storage per cell. The bit-width reduction filter performs this analysis
and compression automatically over windows of attribute data.

Additionally, each cell value in a window is treated relative to the minimum
value in that window. For example, if the window size was 3 cells, which had the
values ``300, 350, 400``, the bit-width reduction filter would first determine
that the minimum value in the window was ``300``, and the relative cell values
were ``0, 50, 100``. These relative values are now less than 255 and can be
represented by a ``uint8_t`` value.

If possible, it can be a good idea to apply positive-delta encoding before
bit-width reduction, as the positive-delta encoding may further increase the
opportunities to store windows of data with a narrower datatype.

The bit-width reduction filter supports one option:

* ``TILEDB_BIT_WIDTH_MAX_WINDOW`` (type ``uint32_t``): The window size in
  bytes to use. Default: 256.

.. note::

    Bit-width reduction only works on integral datatypes.


Tile chunks
-----------

Before filtering each data tile of an attribute, TileDB internally divides the
tile into disjoint chunks. These chunks are then filtered individually.

Chunking tiles before filtering allows for better cache behavior in terms of
temporal locality, as the chunk size can be chosen to fit within the L1 cache of
your processor cores. This helps especially with multi-stage filter lists, as
the output from the previous filter is likely to still be in L1 when used as
input for the current filter.

Chunking tiles also increases the amount of parallel compute that TileDB can
make effective use of. By breaking a tile into individual chunks, each chunk can
then be filtered in parallel, which can result in excellent CPU utilization
when combined with the cache-friendly size of the chunks.

The default chunk size used by TileDB is 64KB, which is the size of many common
processor L1 caches. You can control the chunk size by changing the option on
a filter list:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        Context ctx;
        FilterList filter_list(ctx);
        // Use a max chunk size of 10,000 bytes for this filter list:
        filter_list.set_max_chunk_size(10000);

   .. tab-container:: python
      :title: Python

      .. code-block:: python

        ctx = tiledb.Ctx()
        # Use a max chunk size of 10,000 bytes for this filter list:
        filter_list = tiledb.FilterList(ctx, [tiledb.GzipFilter(ctx)], chunksize=10000)
