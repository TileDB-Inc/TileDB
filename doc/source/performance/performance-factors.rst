.. _performance-factors:

Performance Factors
===================

One of the main advantages of storing your data in TileDB arrays is the ability to
tune performance for your particular use case. In this tutorial we list the most
important factors influencing the performance when interacting with TileDB arrays.

Arrays
------

The following factors that influence performance must be considered when the
array schema is designed. In general, designing an array schema that is effective
and results in high performance depends on the type of data that the array will
store, and the type of queries that will be issued to the array.

Dense vs. sparse arrays
    Choosing a dense or sparse array representation depends on the particular
    data and use case. Sparse arrays in TileDB have additional overhead versus
    dense arrays due to the need to explicitly store cell coordinates of the
    non-empty cells. For a particular use case, this extra overhead may be
    outweighed by the storage savings of not storing dummy values for empty cells
    in the dense alternative, in which case a sparse array would be a good choice.
    Sparse array reads also have additional runtime overhead due to the potential
    need of sorting the cells based on their coordinates.

Space tiles
    In dense arrays, the space tile shape and size determines the atomic unit of
    I/O and compression. All space tiles that intersect a read query's subarray
    are read from disk whole, even if they only partially intersect with the query.
    Matching space tile shape and size closely with the typical read query can
    greatly improve performance.
    In sparse arrays, space tiles affect the order of the non-empty cells and, hence,
    the minimum bounding rectangle (MBR) shape and size. All tiles in a sparse
    array whose MBRs intersect a read query's subarray are read from disk.
    Choosing the space tiles in a way that the shape of the resulting MBR's
    is similar to the read access patterns can improve read
    performance, since fewer MBRs may then overlap with the typical read query.

Tile capacity
    The tile capacity determines the number of cells in a
    data tile of a sparse fragment (recall that a sparse fragment can exist in both dense
    and sparse arrays), which is the atomic unit of I/O and compression. If the capacity
    is too small, the data tiles can be too small, requiring many small,
    independent read operations to fulfill a particular read query. Moreover,
    very small tiles could lead to poor compressibility. If the capacity
    is too large, I/O operations would become fewer and larger (a good thing
    in general), but the amount of wasted work may increase due to reading more d
    ata than necessary to fulfill the typical read query.

Dimensions vs. attributes
    If read queries typically access the entire domain of a particular dimension,
    consider making that dimension an attribute instead. Dimensions should be used
    when read queries will issue ranges of that dimension of cells to retrieve,
    which facilitates TileDB to internally prune cell ranges or whole tiles from
    the data that must be read. Additionally, it is not possible to subselect
    on dimensions (i.e. a read query can specify only a subset of attributes should
    be read, but it must always specify all dimensions).

Filtering
    Filtering (such as compression) of attributes can reduce persistent storage
    consumption, but at the expense of increased time to read/write tiles
    from/to the array. However, tiles on disk that are smaller due to
    compression can be read from disk in less time, which can improve
    performance. Additionally, many compressors offer different levels of
    compression, which can be used to fine-tune the tradeoff.

Queries
-------

The following factors impact query performance, i.e., when reading/writing data
from/to the array.

Number of array fragments
    Each write to a TileDB array (with the exception of consecutive global-order
    writes) creates a new fragment. When reading from the array, read queries
    consider all fragments, sorting by timestamp to ensure the latest data is
    returned. However, large numbers of fragments can slow down performance during
    read queries, as TileDB must internally sort and determine potential overlap
    of cell data across all fragments. It therefore improves performance to
    consolidate arrays with a large number of fragments; consolidation collapses
    all fragments into a single one.

Attribute subsetting
    A benefit of the column-oriented nature of attribute storage in TileDB is
    if a read query does not require data for some of the attributes in the array,
    those attributes will not be touched or read from disk at all. Therefore,
    specifying only the attributes actually needed for each read query will
    improve performance. In the current version of TileDB, write queries must
    still specify values for all attributes.

Read layout
    When an ordered layout (row- or column-major) is used for a read query,
    if that layout differs from the array physical layout, TileDB must internally
    sort and reorganize the cell data that is read from the array into the
    requested order. If you do not care about the particular ordering of cells
    in the result buffers, you can specify a global order query layout,
    allowing TileDB to skip the sorting and reorganization steps.

Write layout
    Similarly, if your specifies write layouts differs from the physcial layout
    of your array (i.e., the global cell order), then TileDB needs to internally sort
    and reorganize the cell data into global order before writing to the array.
    If your cells are already laid out in global order, issuing a write query
    with global-order allows TileDB to skip the sorting and reorganization step.
    Additionally, multiple consecutive global-order writes to an open array will
    append to a single fragment rather than creating a new fragment per write,
    which can improve later the read performance. Care must be taken for global-order
    writes, as it is an error to write data to a subarray that is not aligned with
    the space tiling of the array.

Opening/closing arrays
    Before issuing queries, an array must be "opened." When an array is opened,
    TileDB reads and decodes the array and fragment data, which may require disk
    operations (and potentially decompression). An array can be opened once,
    and many queries issued to it before closing. Minimizing the number of array
    open and close operations can improve performance.


Configuration Parameters
------------------------

The following runtime configuration parameters can tune the performance of
several internal TileDB subsystems.

Cache size -- ``sm.tile_cache_size``
    When a read query causes a tile to be read from disk, TileDB places the
    uncompressed tile in an in-memory LRU cache associated with the query's
    context. When subsequent read queries in the same context request that tile,
    it may be copied directly from the cache instead of re-fetched from disk.
    The ``sm.tile_cache_size`` config parameter determines the overall size in
    bytes of the in-memory tile cache. Increasing it can lead to a higher cache
    hit ratio, and better performance.

Coordinate deduplication -- ``sm.dedup_coords`` and ``sm.check_coord_dups``
    During sparse writes, setting ``sm.dedup_coords`` to true will cause TileDB
    to internally deduplicate cells being written so that multiple cells with the
    same coordinate tuples are not written to the array (which is an error).
    A lighter-weight check is enabled by default with ``sm.check_coord_dups``,
    meaning TileDB will simply perform the check for duplicates and return an
    error if any are found. Disabling these checks can lead to better performance
    on writes.

Coordinate out-of-bounds-check -- ``sm.check_coord_oob``
    During sparse writes, setting ``sm.check_coord_oob`` to ``true`` (default) will
    cause TileDB to internally check whether the given coordinates fall outside
    the domain or not. If you are certain that this is not possible in your
    application, you can set this param to ``false``, avoiding the check and
    thus boosting performance.

Async query concurrency -- ``sm.num_async_threads``
    By default only one thread is allocated to handle async queries. Increasing
    this parameter value can lead to better performance if you are issuing many
    async queries.

Thread pool size -- ``sm.num_tbb_threads``
    TileDB internally parallelizes many expensive operations such as coordinate
    sorting. A TBB-based thread pool is used for these operations, and changing
    this config parameter from the default (while not recommended) can lead
    to better performance in certain circumstances.

Reader thread pool size -- ``sm.num_reader_threads``
    Read operations for read queries can be issued to the VFS layer in parallel
    (the VFS layer may additionally parallelize large I/O operations). For some
    hardware configurations, increasing the number of parallel VFS read
    operations with this parameter may increase performance.

Writer thread pool size -- ``sm.num_writer_threads``
    Write operations for write queries can be issued to the VFS layer in parallel
    (the VFS layer may additionally parallelize large I/O operations). For some
    hardware configurations, increasing the number of parallel VFS write
    operations with this parameter may increase performance.

VFS thread pool size -- ``vfs.num_threads``
    The virtual filesystem (VFS) subsystem in TileDB maintains a separate thread
    pool per context for I/O operations. Reducing or increasing the VFS thread
    pool size can help control the level of concurrency used for I/O operations,
    which may lead to better performance in certain circumstances.

VFS parallelism -- ``vfs.min_parallel_size`` and ``vfs.file.max_parallel_ops``
    The ``vfs.min_parallel_size`` parameter sets the minimum number of bytes that
    can go in a parallel VFS operation. This can help ensure that I/O requests
    are not broken into too small pieces, even if there are enough threads in the
    VFS thread pool to do so. Similarly, ``vfs.file.max_parallel_ops`` controls
    the maximum number of parallel operations for ``file:///`` URIs, independently
    of the thread pool size, allowing you to over- or under-subscribe VFS threads.

S3 parallelism -- ``vfs.s3.max_parallel_ops``
    This controls the maximum number of parallel operations for ``s3://`` URIs
    independently of the VFS thread pool size, allowing you to over- or
    under-subscribe VFS threads. Oversubscription can be helpful in some
    cases with S3, to help hide I/O latency.

S3 write size -- ``vfs.s3.multipart_part_size``
    Replacing ``vfs.min_parallel_size`` for S3 objects, this parameter controls the
    minimum part size of S3 multipart writes. Note that
    ``vfs.s3.multipart_part_size * vfs.s3.max_parallel_ops`` bytes will be buffered
    in memory by TileDB before actually submitting an S3 write request, at which
    point all of the parts of the multipart write are issued in parallel.

System Parameters
-----------------

Hardware concurrency
    The number of cores and hardware threads of the machine impacts the amount
    of parallelism TileDB can use internally to accelerate reads, writes and
    compression/decompression.

Storage backend (S3, local, etc)
    The different types of storage backend (S3, local disk, etc) have different
    throughput and latency characteristics, which can impact query time.