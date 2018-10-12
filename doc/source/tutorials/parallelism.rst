Parallelism
===========

TileDB is *fully parallelized* internally, i.e., it uses multiple
threads to process in parallel the most heavyweight tasks. In this
tutorial we explain how TileDB parallelizes the queries and VFS
operations, outlining the configuration parameters that you can
use to control the amount of parallelization. Note that here we
cover only the most important areas, as TileDB parallelizes
numerous other internal tasks.

Queries
-------

Internally, TileDB utilizes parallelism wherever possible to optimize read
and write query performance and provide scalability for hardware with more
available parallelism. Both read and write queries are parallelized.
The array schema, query size, and layout all impact the available
parallelization within the query. TileDB uses the
`Intel Threaded Building Blocks <https://www.threadingbuildingblocks.org/>`__
library for efficient scheduling and load balancing.

Filtering
~~~~~~~~~

For filtering (such as compression) during read queries, TileDB uses the
following nested parallelism strategy::

    parallel_for_each attribute being read:
      parallel_for_each tile of the attribute overlapping the subarray:
        parallel_for_each chunk of the tile:
          filter chunk

The "chunks" of a tile are controlled by a TileDB filter list parameter
that defaults to 64KB. See the :ref:`filters` tutorial to see how.

The nested parallelism in reads allows for maximum utilization of the available
cores for filtering (e.g. decompression), in either the case where the query
intersects few large tiles or many small tiles. For writes, TileDB uses the same
strategy as reads::


    parallel_for_each attribute being written:
      parallel_for_each tile of the attribute being written:
        parallel_for_each chunk of the tile:
          filter chunk

The configuration parameter ``sm.num_async_threads`` controls the number of
threads available for use by asynchronous queries. It does not impact the
parallelism used in the ``for`` loops illustrated above. Instead, the
``sm.num_tbb_threads`` parameter impacts the ``for`` loops above, although it is
not recommended to modify this configuration parameter from its default setting.

I/O
~~~

After the tiles are filtered during a write query (or before filtering
during read queries), I/O operations are issued
to the underlying persistent storage via TileDB's virtual filesystem (VFS)
layer. For write queries, the configuration parameter ``sm.num_writer_threads``
controls the maximum number of write operations that will be dispatched in
parallel to the VFS layer. For reads, the configuration parameter is
``sm.num_reader_threads``. Both default to 1.

For flexibility in controlling the number of threads used for these parallel
calls into the VFS layer, TileDB does not use TBB for these operations. Instead,
a list of read or write tasks is accumulated during the query (one task per tile
being read or written, per attribute) and dispatched to a thread pool with a work
queue. The size of the thread pool is determined by the above configuration
parameters.

VFS Operations
--------------

The VFS layer is additionally parallelized independently of the queries.
Read VFS operations are parallelized
internally using a private thread pool. This allows large reads to be
split up and performed in parallel. Parallel VFS IO is enabled by default,
but can be controlled with the following config parameters.

- ``vfs.max_parallel_ops``: The maximum number of parallel operations a single
  VFS read will be split into.
- ``vfs.min_parallel_size``: The minimum number of bytes per parallel VFS
  operation. TileDB will not split a single VFS read such that the
  parallel operations are responsible for less than ``vfs.min_parallel_size``
  bytes (unless the operation size is not evenly divided by
  ``vfs.min_parallel_size``, in which case the "last" thread will read
  less data).

All VFS backends are currently parallelized for reads using this strategy. In
addition, POSIX and Win32 backends are parallelized for writes, controlled
by the same config parameters.

VFS-S3
~~~~~~
The S3 backend is also parallelized internally for writes. When a VFS write
operation is issued to an S3 URI, TileDB will internally buffer the data in
memory until enough has been buffered to dispatch a write operation to the S3
service. Any buffered data is flushed when the VFS file is synced or closed.
The following config parameter controls this behavior.

- ``vfs.s3.multipart_part_size``: TileDB will buffer
  ``vfs.max_parallel_ops * vfs.s3.multipart_part_size`` bytes locally before issuing
  ``vfs.max_parallel_ops`` parallel "multipart" upload requests to S3.

VFS-HDFS
~~~~~~~~
Currently, the HDFS backend is not parallelized for writes. Reads are
parallelized using the generic VFS parallel read strategy outlined above.