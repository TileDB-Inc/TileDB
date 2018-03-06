Storage Backends
================

TileDB integrates with multiple storage backends. Specifically,
in addition to posix-compliant local filesystems (e.g, ext4, HFD+, etc.)
and distributed filesystems (e.g., NFS, Lustre, etc.), or Windows
filesystems, TileDB supports a variety of additional storage backends,
such as **HDFS** and **AWS S3**.

Recall that TileDB creates the array fragments with **append-only**
write operations, and each fragment becomes **immutable** after its
creation. This design is ideal for **append-only filesystems**, such as
HDFS and S3.

HDFS
----

TileDB relies on the C interface to HDFS provided by
`libhdfs <http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/LibHdfs.html>`__
to interact with the distributed filesystem. The integration of TileDB
with HDFS enables the user to create enormous arrays distributed to
multiple machines by HDFS, which can then be accessed by any machine
with **global addressing** (i.e., the program simply views the entire
array, without knowing how it is distributed across the machines). This
makes writing parallel programs with TileDB on HDFS extremely easy.

.. note::
    We are planning to add API functionality for checking the **data locality** of an array
    stored on HDFS, i.e., the user will be able to check which machines  store any part of an array.

TileDB utilizes the `AWS C++ SDK <https://github.com/aws/aws-sdk-cpp>`__
to integrate with S3. TileDB also works well with the S3-compliant
`minio <https://minio.io>`__ object store, which we use in our testing
framework. Users can create TileDB arrays in S3 buckets and access them
as any other local TileDB array by using the URI of the TileDB array on
S3, e.g., ``s3://<bucket>/path/to/array``.

Recall that a TileDB array is stored essentially as a **directory**.
Moreover, users can create groups of arrays in a hierarchical structure,
similar to the traditional filesystem directory hierarchy. We wish to
retain the TileDB hierarchical structure of groups and arrays on every
storage backend for consistency. However, there is no notion of a
"directory" on S3. We workaround this by creating a special empty S3
file for every directory ``s3://<bucket>/path/to/dir``, which is simply
called ``s3://<bucket>/path/to/dir/`` (notice the ``/`` at the end of
the name). In fact, all TileDB-generated S3 objects that end in ``/``
indicate special directory files. This enables consistent use of URI's
as array names across all storage backends.

TileDB writes the various fragment files as **append-only** objects
using the **multi-part upload** API of the AWS SDK. In addition to
enabling appends, this API renders the TileDB writes to S3 particularly
amenable to optimizations via parallelization. Since TileDB updates
arrays only by writing (appending to) new files (i.e., it never updates
a file in-place), TileDB does not need to download entire objects,
update them, and re-upload them to S3. This leads to excellent write
performance.

TileDB reads utilize the **range GET request** API of the AWS SDK, which
retrieves only the requested (contiguous) bytes from a file/object,
rather than downloading the entire file from the cloud. This results in
extremely fast subarray reads, especially because of the array
**tiling**. Recall that a tile (which groups cell values that are stored
contiguously in the file) is **the atomic unit of IO**. The range GET
API enables reading each tile from S3 in a single request.

.. note::
    In subsequent releases, we are planning to integrate TileDB with **Microsoft Azure**,
    **Google Cloud** and more storage backends.

.. warning::
    Extreme care must be taken when **creating/deleting buckets** on AWS S3.
    After its creation, a bucket may take some time to "appear" in the system.
    This will cause problems if the user creates the bucket and immediately tries to write a
    file in it. Similarly, deleting a bucket may not take effect immediately and, therefore,
    it may continue to "exist" for some time.
