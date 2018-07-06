Virtual Filesystem
==================

TileDB is architected such that all IO to/from the storage backends is
abstracted behind a **Virtual Filesystem** (VFS) module. This module supports
simple operations, such as creating a file/directory, reading/writing to
a file, etc. This abstraction enables us to easily plug in more storage
backends in the future, effectively making the storage backend opaque to
the user.

A nice positive "by-product" of this architecture is that it is possible
to expose the basic virtual filesystem functionality via the TileDB
APIs. This provides a simplified interface for file IO and directory
management (i.e., not related to TileDB objects such as array) on all the
storage backends that TileDB supports.

The following code example covers most of the TileDB VFS functionality.

.. toggle-header::
    :header: **Example Code Listing**

    .. content-tabs::

       .. tab-container:: cpp
          :title: C++

          .. literalinclude:: ../{source_examples_path}/cpp_api/vfs.cc
             :language: c++
             :linenos:

Compiling and running the above code, we get the following output:

.. code-block:: bash

   $ g++ -std=c++11 vfs.cc -o vfs_cpp -ltiledb
   $ ./vfs_cpp
   Created 'dir_A'
   Created empty file 'dir_A/file_A'
   Size of file 'dir_A/file_A': 0
   Moving file 'dir_A/file_A' to 'dir_A/file_B'
   Deleting 'dir_A/file_B' and 'dir_A'
   Binary read:
   153.1
   abcdefghijkl

Note that you cannot open an existing file for writing in append mode
for S3. It is allowed to open a file in write mode and write an arbitrary
number of times to this file (i.e., append to it). However, after you
close an S3 file, you *cannot re-open it for appends*. This is because of
the fact that S3 (and similar object stores) do not support append
operations to objects after they are created and finalized.

Currently, the virtual filesystem functionality does not support
*seekable writes* because of our integration with append-only filesystems
like HDFS and S3. Moreover, *moving files across storage backends* is not yet
implemented. We plan to add both seakable writes (for filesystems
that support it) and moving across filesystems in a future
release.

Finally, note that TileDB allows you to create/delete S3 buckets via
its VFS functionality, e.g.,

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        vfs.create_bucket("s3://my_bucket");
        vfs.remove_bucket("s3://my_bucket");

However, extreme care must be taken when creating/deleting buckets on AWS S3.
After its creation, a bucket may take some time to "appear" in the system.
This will cause problems if the user creates the bucket and immediately tries to write a
file in it. Similarly, deleting a bucket may not take effect immediately and, therefore,
it may continue to "exist" for some time.

.. warning::

   The TileDB VFS feature is experimental. Everything covered here works
   great, but the APIs might undergo changes in future versions.

