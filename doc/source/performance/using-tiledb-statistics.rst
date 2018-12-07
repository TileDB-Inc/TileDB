.. _using-tiledb-statistics:

Using TileDB Statistics
=======================

A lot of performance optimization for TileDB programs involves **minimizing wasted work**.
TileDB comes with an internal statistics reporting system that can help identify potential
areas of performance improvement for your TileDB programs, including reducing wasted work.

.. table:: Full programs
  :widths: auto

  ====================================  =============================================================
  **Program**                           **Links**
  ------------------------------------  -------------------------------------------------------------
  ``using_tiledb_stats``                |statscpp| |statspy|
  ====================================  =============================================================

.. |statscpp| image:: ../figures/cpp.png
   :align: middle
   :width: 30
   :target: {tiledb_src_root_url}/examples/cpp_api/using_tiledb_stats.cc

.. |statspy| image:: ../figures/python.png
   :align: middle
   :width: 25
   :target: {tiledb_py_src_root_url}/examples/using_tiledb_stats.py

The TileDB statistics can be enabled and disabled at runtime, and a report can be dumped at
any point. A typical situation is to enable the statistics immediately before submitting a query,
submit the query, and then immediately dump the report. This can be done like so:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

         tiledb::Stats::enable();
         query.submit();
         tiledb::Stats::dump(stdout);
         tiledb::Stats::disable();


   .. tab-container:: python
      :title: Python

      .. code-block:: python

         tiledb.stats_enable()
         data = A[:]
         tiledb.stats_dump()
         tiledb.stats_disable()

With the ``dump`` call, a report containing the gathered statistics will be printed. The
report prints values of many individual counters, followed by a high-level summary printed
at the end of the report. Typically the summary contains the necessary information
to make high-level performance tuning decisions. An example summary is shown below:

.. code-block:: none

    Summary:
    --------
    Hardware concurrency: 4
    Reads:
      Read query submits: 1
      Tile cache hit ratio: 0 / 1  (0.0%)
      Fixed-length tile data copy-to-read ratio: 4 / 1000 bytes (0.0%)
      Var-length tile data copy-to-read ratio: 0 / 0 bytes
      Total tile data copy-to-read ratio: 4 / 1000 bytes (0.0%)
      Read compression ratio: 1245 / 1274 bytes (1.0x)
    Writes:
      Write query submits: 0
      Tiles written: 0
      Write compression ratio: 0 / 0 bytes

Each item is explained separately below.

Hardware concurrency
    The amount of available hardware-level concurrency (cores plus hardware threads).

Read query submits
    The number of times a read query submit call was made.

Tile cache hit ratio
    Ratio expressing utilization of the tile cache. The denominator indicates how many
    tiles in total were fetched from disk to satisfy a read query. After a tile is fetched
    once, it is placed in the internal tile cache; the numerator indicates how many tiles
    were requested in order to satisfy a read query that were hits in the tile cache.
    In the above example summary, a single tile was fetched (which missed the cache because
    it was the first access to that tile). If the same tile was accessed again for a
    subsequent query, it could hit in the cache, increasing the ratio 1/2. Higher ratios
    are better.

Fixed-length tile data copy-to-read ratio
    Ratio expressing a measurement of wasted work for reads of fixed-length attributes.
    The denominator is the total number of (uncompressed) bytes of fixed-length
    attribute data fetched from disk. The numerator is the number of those bytes that
    were actually copied into the query buffers to satisfy a read query. In the above
    example, 1000 bytes of fixed-length data were read from disk and only 4 of those
    bytes were used to satisfy a read query, indicating a large amount of wasted work.
    Higher ratios are better.

Var-length tile data copy-to-read ratio
    This ratio is the same as the previous, but for variable-length attribute data.
    This is tracked separately because variable-length attributes can often be read
    performance bottlenecks, as their size is by nature unpredictable.
    Higher ratios are better.

Total tile data copy-to-read ratio
    The overall copy-to-read ratio, where the numerator is the sum of the fixed- and
    variable-length numerators, and the denominator is the sum of the fixed- and
    variable-length denominators.

Read compression ratio
    Ratio expressing the effective compression ratio of data read from disk. The numerator
    is the total number of bytes returned from disk reads after filtering. The
    denominator is the total number of bytes read from disk, whether filtered or
    not. This is different than the tile copy-to-read ratios due to extra
    read operations for array and fragment metadata. For simplicity, this ratio currently
    counts all filters as "compressors", so the ratio may not be exactly the compression
    ratio in the case that other filters besides compressors are involved.

Write query submits
    The number of times a write query submit call was made.

Tiles written
    The total number of tiles written, over all write queries.

Write compression ratio
    Ratio expressing the effective compression ratio of data written to disk. The numerator
    is the total number of un-filtered bytes requested to be written to disk. The
    denominator is the total number of bytes written from disk, after filtering. Similarly
    to the read compression ratio, this value counts all filters as compressors.

.. note::
    The TileDB library is built by default with statistics enabled. You can disable
    statistics gathering with the ``-DTILEDB_STATS=OFF`` CMake variable.

Read example
------------

As a simple example, we will examine the effect of different dense array tiling
configurations on read queries. Intuitively, the closer the tile shape aligns with
the shape of read queries to the array, the higher performance we expect to see. This
is because TileDB fetches from storage only tiles overlapping with the subarray query.
The bigger the overlap between the subarray query and the tile(s), the less the
wasted work. We will use the statistics report to gain an exact understanding of
the wasted work.

For each of the following experiments, we will read from a 2D dense array containing about
0.5GB of data. The array will be 12,000 rows by 12,000 columns with a single uncompressed
``int32`` attribute. The array is created as follows:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

           Context ctx;
           ArraySchema schema(ctx, TILEDB_DENSE);
           Domain dom(ctx);
           dom.add_dimension(
                  Dimension::create<uint32_t>(ctx, "row", {{1, 12000}}, row_tile_extent))
               .add_dimension(
                   Dimension::create<uint32_t>(ctx, "col", {{1, 12000}}, col_tile_extent));
           schema.set_domain(dom);
           schema.add_attribute(
               Attribute::create<int32_t>(ctx, "a", {TILEDB_NO_COMPRESSION, -1}));
           Array::create(array_uri, schema);

   .. tab-container:: python
      :title: Python

      .. code-block:: python

         ctx = tiledb.Ctx()
         dom = tiledb.Domain(ctx,
                            tiledb.Dim(ctx, name="rows", domain=(1, 12000), tile=row_tile_extent, dtype=np.int32),
                            tiledb.Dim(ctx, name="cols", domain=(1, 12000), tile=col_tile_extent, dtype=np.int32))

         schema = tiledb.ArraySchema(ctx, domain=dom, sparse=False,
                                    attrs=[tiledb.Attr(ctx, name="a", dtype=np.int32)])

         # Create the (empty) array on disk.
         tiledb.DenseArray.create(array_name, schema)

The total array size on disk then is 12000 * 12000 * 4 bytes, about 550 MB.

As a first example, suppose we configured the schema such that the array is composed
of a single tile, i.e.:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

         dom.add_dimension(
               Dimension::create<uint32_t>(ctx, "row", {{1, 12000}}, 12000))
            .add_dimension(
                Dimension::create<uint32_t>(ctx, "col", {{1, 12000}}, 12000));

   .. tab-container:: python
      :title: Python

      .. code-block:: python

         dom = tiledb.Domain(ctx,
                            tiledb.Dim(ctx, name="rows", domain=(1, 12000), tile=12000, dtype=np.int32),
                            tiledb.Dim(ctx, name="cols", domain=(1, 12000), tile=12000, dtype=np.int32))

With this array schema, **the entire array is composed of a single tile**. Thus, any
read query (regardless of the subarray) will fetch the entire array from disk.
We will issue a read query of the first 3,000 rows (subarray ``[1:3000, 1:12000]``)
which is 25% of the cells in the array:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

         std::vector<uint32_t> subarray = {1, 3000, 1, 12000};
         Context ctx;
         Array array(ctx, array_name, TILEDB_READ);
         Query query(ctx, array);
         std::vector<int32_t> values(
             array.max_buffer_elements(subarray)["a"].second);
         query.set_subarray(subarray).set_buffer("a", values);

         Stats::enable();
         query.submit();
         Stats::dump(stdout);
         Stats::disable();

   .. tab-container:: python
      :title: Python

      .. code-block:: python

         ctx = tiledb.Ctx()
         with tiledb.DenseArray(ctx, array_name, mode='r') as A:
             # Read a slice of 3,000 rows.
             tiledb.stats_enable()
             data = A[1:3001, 1:12001]
             tiledb.stats_dump()
             tiledb.stats_disable()

The report printed for this experiment is:

.. code-block:: none

    Summary:
    --------
    Hardware concurrency: 4
    Reads:
      Read query submits: 1
      Tile cache hit ratio: 0 / 1  (0.0%)
      Fixed-length tile data copy-to-read ratio: 144000000 / 576105488 bytes (25.0%)
      Var-length tile data copy-to-read ratio: 0 / 0 bytes
      Total tile data copy-to-read ratio: 144000000 / 576105488 bytes (25.0%)
      Read compression ratio: 576000000 / 576105488 bytes (1.0x)
    Writes:
      Write query submits: 0
      Tiles written: 0
      Write compression ratio: 0 / 0 bytes

We can see that during the time the statistics were being gathered, there was a single read query
submitted (our read query). The denominator of the tile cache hit ratio tells us that the single
read query accessed a single tile, as expected (since the entire array is a single tile).

The "fixed-length tile data copy-to-read ratio" metric expresses the "wasted work" measurement,
namely the number of bytes copied into our query buffers to fulfill the read query, divided by
the number of bytes read from disk. In this experiment, 144,000,000 bytes (the ``int32_t``
fixed-length attribute values for the subarray ``[1:3000, 1:12000]``) were copied to the query
buffers, but we read 576,000,000 tile data bytes from disk (576,000,000 = 12000 * 12000 * 4 bytes).
This copy-to-read ratio tells us 25% of the work done by TileDB to satisfy the read query was useful.

Now let's modify the array such that **each row corresponds to a single tile**, i.e.:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

         dom.add_dimension(
               Dimension::create<uint32_t>(ctx, "row", {{1, 12000}}, 1))
            .add_dimension(
                Dimension::create<uint32_t>(ctx, "col", {{1, 12000}}, 12000));

   .. tab-container:: python
      :title: Python

      .. code-block:: python

         dom = tiledb.Domain(ctx,
                            tiledb.Dim(ctx, name="rows", domain=(1, 12000), tile=1, dtype=np.int32),
                            tiledb.Dim(ctx, name="cols", domain=(1, 12000), tile=12000, dtype=np.int32))

When reading the subarray ``[1:3000, 1:12000]`` as in the previous experiment, we see
the following statistics:

.. code-block:: none

    Reads:
      Read query submits: 1
      Tile cache hit ratio: 0 / 3000  (0.0%)
      Fixed-length tile data copy-to-read ratio: 144000000 / 144060000 bytes (100.0%)
      Var-length tile data copy-to-read ratio: 0 / 0 bytes
      Total tile data copy-to-read ratio: 144000000 / 144060000 bytes (100.0%)
      Read compression ratio: 144000000 / 144060000 bytes (1.0x)

Now the denominator of the tile cache hit ratio tells us that 3,000 tiles were accessed,
which is as expected because we requested 3,000 rows. Note also the difference in the
copy-to-read ratio. We still copy 144,000,000 bytes (since the query is the same), but the
amount of data is reduced from the entire array to only the tiles (rows) required, which is
12000 * 3000 * 4 = 144,000,000 bytes. This yields a 100% useful work (no wasted work) metric.

You may notice the "read compression ratio" metric reports more bytes read and used than just
the tile data. The difference is accounted for by the array and fragment metadata,
which TileDB must also read in order to determine which tiles should be read and decompressed,
and the serialization overhead introduced by the TileDB filter pipeline.

Finally, we will issue two overlapping queries back-to-back, first the same
``[1:3000, 1:12000]`` subarray followed by subarray ``[2000:4000, 1:12000]``, i.e.:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

         Context ctx;
         std::vector<uint32_t> subarray1 = {1, 3000, 1, 12000},
                               subarray2 = {2000, 4000, 1, 12000};
         Array array(ctx, array_name, TILEDB_READ);
         std::vector<int32_t> values1(array.max_buffer_elements(subarray1)["a"].second),
                              values2(array.max_buffer_elements(subarray2)["a"].second);
         Query query1(ctx, array), query2(ctx, array);
         query1.set_subarray(subarray1).set_buffer("a", values1);
         query2.set_subarray(subarray2).set_buffer("a", values2);
         
         Stats::enable();
         query1.submit();
         query2.submit();
         Stats::dump(stdout);
         Stats::disable();

   .. tab-container:: python
      :title: Python

      .. code-block:: python

         ctx = tiledb.Ctx()
         with tiledb.DenseArray(ctx, array_name, mode='r') as A:
             tiledb.stats_enable()
             data1 = A[1:3001, 1:12001]
             data2 = A[2000:4001, 1:12001]
             tiledb.stats_dump()
             tiledb.stats_disable()

This yields the following report:

.. code-block:: none

    Reads:
      Read query submits: 2
      Tile cache hit ratio: 198 / 5001  (4.0%)
      Fixed-length tile data copy-to-read ratio: 240048000 / 230640060 bytes (104.1%)
      Var-length tile data copy-to-read ratio: 0 / 0 bytes
      Total tile data copy-to-read ratio: 240048000 / 230640060 bytes (104.1%)
      Read compression ratio: 230544000 / 230640060 bytes (1.0x)

Several things have changed, most notably now there were several hits in the tile cache out of the
5,001 tiles accessed. However, we may have expected that 1,001 tiles would hit in the cache,
since the two queries overlapped on rows 2000--3000 (inclusive). The reason we do not see
this in the statistics is that the default tile cache configuration does not allow many tiles
to be cached. Let's increase the tile cache size to 100MB and repeat the experiment:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

         Config config;
         config["sm.tile_cache_size"] = 100 * 1024 * 1024;  // 100 MB
         Context ctx(config);
         std::vector<uint32_t> subarray1 = {1, 3000, 1, 12000},
                               subarray2 = {2000, 4000, 1, 12000};
         Array array(ctx, array_name, TILEDB_READ);
         std::vector<int32_t> values1(array.max_buffer_elements(subarray1)["a"].second),
                              values2(array.max_buffer_elements(subarray2)["a"].second);
         Query query1(ctx, array), query2(ctx, array);
         query1.set_subarray(subarray1).set_buffer("a", values1);
         query2.set_subarray(subarray2).set_buffer("a", values2);

         Stats::enable();
         query1.submit();
         query2.submit();
         Stats::dump(stdout);
         Stats::disable();

   .. tab-container:: python
      :title: Python

      .. code-block:: python

         ctx = tiledb.Ctx({'sm.tile_cache_size': 100 * 1024 * 1024})
         with tiledb.DenseArray(ctx, array_name, mode='r') as A:
             tiledb.stats_enable()
             data1 = A[1:3001, 1:12001]
             data2 = A[2000:4001, 1:12001]
             tiledb.stats_dump()
             tiledb.stats_disable()

The stats summary now reads:

.. code-block:: none

    Reads:
      Read query submits: 2
      Tile cache hit ratio: 697 / 5001  (13.9%)
      Fixed-length tile data copy-to-read ratio: 240048000 / 206678080 bytes (116.1%)
      Var-length tile data copy-to-read ratio: 0 / 0 bytes
      Total tile data copy-to-read ratio: 240048000 / 206678080 bytes (116.1%)
      Read compression ratio: 206592000 / 206678080 bytes (1.0x)

We now have more hits in the cache. Also notice that the copy-to-read ratio now exceeds
100%, because although the same number of bytes were copied into the query buffers, many
of those bytes did not have to be read from disk twice (as they were hits in the cache).


Write example
-------------

The write statistics summary is less in-depth compared to the read summary. We will take a
look at the example of writing the above 12,000 by 12,000 array with synthetic attribute
data when each row is a single tile:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

         Array array(ctx, array_name, TILEDB_WRITE);
         Query query(ctx, array);
         std::vector<int32_t> values(12000 * 12000);
         for (unsigned i = 0; i < values.size(); i++) {
           values[i] = i;
         }
         query.set_layout(TILEDB_ROW_MAJOR).set_buffer("a", values);
         Stats::enable();
         query.submit();
         Stats::dump(stdout);
         Stats::disable();


   .. tab-container:: python
      :title: Python

      .. code-block:: python

         ctx = tiledb.Ctx()
         # Open the array and write to it.
         with tiledb.DenseArray(ctx, array_name, mode='w') as A:
             data = np.arange(12000 * 12000)
             tiledb.stats_enable()
             A[:] = data
             tiledb.stats_dump()
             tiledb.stats_disable()

With attribute ``a`` uncompressed as before, this gives the following report in the summary:

.. code-block:: none

    Writes:
      Write query submits: 1
      Tiles written: 12000
      Write compression ratio: 576384160 / 576284005 bytes (1.0x)

As expected, because each row was a single tile, writing 12,000 rows causes 12,000 tiles to
be written. Because ``a`` is uncompressed, the compression ratio is nearly exactly
1.0x (the small amount of difference is due to the array and fragment metadata
being compressed independently of the attribute).

If we enable compression on the ``a`` attribute when creating the array schema,
e.g. bzip2 at its default compression level, we see the change in the report:

.. code-block:: none

    Writes:
      Write query submits: 1
      Tiles written: 12000
      Write compression ratio: 576384160 / 53136720 bytes (10.8x)

Because our synthetic array data is very predictable, bzip2 does a good job compressing
it, and this is reflected in the reported compression ratio.

Full statistics report
----------------------

In general, the summary report may be enough to reveal potential sources of large
performance flaws. In addition, accompanying every stats dump is a list of all of
the individual internal performance counters that TileDB tracks. Each of the counter
names is prefixed with the system it measures, e.g. ``vfs_*`` counters measure
details of the TileDB VFS system, ``compressor_*`` measures details of the various
compressors, etc. Some of these counters are self-explanatory, and others are intended
primarily for TileDB developers to diagnose internal performance issues.

.. toggle-header::
   :header: **Example full statistics report**

   .. code-block:: none

    ===================================== TileDB Statistics Report =======================================

    Individual function statistics:
      Function name                                                          # calls       Total time (ns)
      ----------------------------------------------------------------------------------------------------
      compressor_bzip_compress,                                                12000,         63560889145
      compressor_bzip_decompress,                                                  0,                   0
      compressor_dd_compress,                                                      0,                   0
      compressor_dd_decompress,                                                    0,                   0
      compressor_gzip_compress,                                                    6,             2988746
      compressor_gzip_decompress,                                                  0,                   0
      compressor_lz4_compress,                                                     0,                   0
      compressor_lz4_decompress,                                                   0,                   0
      compressor_rle_compress,                                                     0,                   0
      compressor_rle_decompress,                                                   0,                   0
      compressor_zstd_compress,                                                    0,                   0
      compressor_zstd_decompress,                                                  0,                   0
      encryption_encrypt_aes256gcm,                                                0,                   0
      encryption_decrypt_aes256gcm,                                                0,                   0
      filter_pipeline_run_forward,                                             12001,         63850960757
      filter_pipeline_run_reverse,                                                 0,                   0
      cache_lru_evict,                                                             0,                   0
      cache_lru_insert,                                                            0,                   0
      cache_lru_invalidate,                                                        0,                   0
      cache_lru_read,                                                              0,                   0
      cache_lru_read_partial,                                                      0,                   0
      reader_compute_cell_ranges,                                                  0,                   0
      reader_compute_dense_cell_ranges,                                            0,                   0
      reader_compute_dense_overlapping_tiles_and_cell_ranges,                      0,                   0
      reader_compute_overlapping_coords,                                           0,                   0
      reader_compute_overlapping_tiles,                                            0,                   0
      reader_compute_tile_coords,                                                  0,                   0
      reader_copy_fixed_cells,                                                     0,                   0
      reader_copy_var_cells,                                                       0,                   0
      reader_dedup_coords,                                                         0,                   0
      reader_dense_read,                                                           0,                   0
      reader_fill_coords,                                                          0,                   0
      reader_filter_tiles,                                                         0,                   0
      reader_init_tile_fragment_dense_cell_range_iters,                            0,                   0
      reader_next_subarray_partition,                                              0,                   0
      reader_read,                                                                 0,                   0
      reader_read_all_tiles,                                                       0,                   0
      reader_sort_coords,                                                          0,                   0
      reader_sparse_read,                                                          0,                   0
      writer_check_coord_dups,                                                     0,                   0
      writer_check_coord_dups_global,                                              0,                   0
      writer_check_global_order,                                                   0,                   0
      writer_compute_coord_dups,                                                   0,                   0
      writer_compute_coord_dups_global,                                            0,                   0
      writer_compute_coords_metadata,                                              0,                   0
      writer_compute_write_cell_ranges,                                        12000,            44097834
      writer_create_fragment,                                                      1,              621921
      writer_filter_tiles,                                                         1,         63885761123
      writer_global_write,                                                         0,                   0
      writer_init_global_write_state,                                              0,                   0
      writer_init_tile_dense_cell_range_iters,                                     1,            14082371
      writer_ordered_write,                                                        1,         66025258154
      writer_prepare_full_tiles_fixed,                                             0,                   0
      writer_prepare_full_tiles_var,                                               0,                   0
      writer_prepare_tiles_fixed,                                                  0,                   0
      writer_prepare_tiles_ordered,                                                1,           403377491
      writer_prepare_tiles_var,                                                    0,                   0
      writer_sort_coords,                                                          0,                   0
      writer_unordered_write,                                                      0,                   0
      writer_write,                                                                1,         66025267985
      writer_write_all_tiles,                                                      1,          1565860616
      sm_array_close_for_reads,                                                    0,                   0
      sm_array_close_for_writes,                                                   0,                   0
      sm_array_open_for_reads,                                                     0,                   0
      sm_array_open_for_writes,                                                    0,                   0
      sm_array_reopen,                                                             0,                   0
      sm_read_from_cache,                                                          0,                   0
      sm_write_to_cache,                                                           0,                   0
      sm_query_submit,                                                             1,         66025270927
      tileio_is_generic_tile,                                                      0,                   0
      tileio_read_generic,                                                         0,                   0
      tileio_write_generic,                                                        1,             1671328
      vfs_abs_path,                                                                4,              201980
      vfs_close_file,                                                              2,              104927
      vfs_constructor,                                                             0,                   0
      vfs_create_bucket,                                                           0,                   0
      vfs_create_dir,                                                              1,               94723
      vfs_create_file,                                                             0,                   0
      vfs_dir_size,                                                                0,                   0
      vfs_empty_bucket,                                                            0,                   0
      vfs_file_size,                                                               0,                   0
      vfs_filelock_lock,                                                           0,                   0
      vfs_filelock_unlock,                                                         0,                   0
      vfs_init,                                                                    0,                   0
      vfs_is_bucket,                                                               0,                   0
      vfs_is_dir,                                                                  2,               48002
      vfs_is_empty_bucket,                                                         0,                   0
      vfs_is_file,                                                                 0,                   0
      vfs_ls,                                                                      0,                   0
      vfs_move_file,                                                               0,                   0
      vfs_move_dir,                                                                0,                   0
      vfs_open_file,                                                               0,                   0
      vfs_read,                                                                    0,                   0
      vfs_read_all,                                                                0,                   0
      vfs_remove_bucket,                                                           0,                   0
      vfs_remove_file,                                                             0,                   0
      vfs_remove_dir,                                                              0,                   0
      vfs_supports_fs,                                                             0,                   0
      vfs_sync,                                                                    0,                   0
      vfs_write,                                                               12002,          1553079894
      vfs_s3_fill_file_buffer,                                                     0,                   0
      vfs_s3_write_multipart,                                                      0,                   0

    Individual counter statistics:
      Counter name                                                             Value
      ------------------------------------------------------------------------------
      cache_lru_inserts,                                                           0
      cache_lru_read_hits,                                                         0
      cache_lru_read_misses,                                                       0
      reader_attr_tile_cache_hits,                                                 0
      reader_num_attr_tiles_touched,                                               0
      reader_num_bytes_after_filtering,                                            0
      reader_num_fixed_cell_bytes_copied,                                          0
      reader_num_fixed_cell_bytes_read,                                            0
      reader_num_tile_bytes_read,                                                  0
      reader_num_var_cell_bytes_copied,                                            0
      reader_num_var_cell_bytes_read,                                              0
      writer_num_attr_tiles_written,                                           12000
      writer_num_bytes_before_filtering,                                   576000000
      writer_num_bytes_written,                                             53101395
      sm_contexts_created,                                                         0
      sm_query_submit_layout_col_major,                                            0
      sm_query_submit_layout_row_major,                                            1
      sm_query_submit_layout_global_order,                                         0
      sm_query_submit_layout_unordered,                                            0
      sm_query_submit_read,                                                        0
      sm_query_submit_write,                                                       1
      tileio_read_num_bytes_read,                                                  0
      tileio_read_num_resulting_bytes,                                             0
      tileio_write_num_bytes_written,                                          35325
      tileio_write_num_input_bytes,                                           384160
      vfs_read_total_bytes,                                                        0
      vfs_write_total_bytes,                                                53136720
      vfs_read_num_parallelized,                                                   0
      vfs_read_all_total_regions,                                                  0
      vfs_posix_write_num_parallelized,                                            0
      vfs_win32_write_num_parallelized,                                            0
      vfs_s3_num_parts_written,                                                    0
      vfs_s3_write_num_parallelized,                                               0

The "function statistics" report the number of calls and amount of time in nanoseconds for
each instrumented function. It is important to note that the time reported for these
counters is aggregated across all threads. For example, if 10 threads invoke ``vfs_write``
and each thread's call takes 100 ns, then the reported time for ``vfs_write``
will be 1000 ns, even though the average time was much less.

The "counter statistics" report the values of individual counters. The summary statistics
are directly derived from these counter statistics.