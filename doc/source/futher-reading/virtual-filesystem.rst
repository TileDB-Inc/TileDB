Virtual Filesystem
==================

TileDB is architected such that all IO to/from the storage backends is
abstracted behind a **Virtual Filesystem** module. This module supports
simple operations, such as creating a file/directory, reading/writing to
a file, etc. This abstraction enables us to easily plug in more storage
backends in the future, effectively making the storage backend opaque to
the user.

A nice positive "side-effect" of this architecture is that it possible
to expose the basic virtual filesystem functionality via the TileDB C
API :ref:`Figure 24 <figure-24>`). This provides a simplified
interface for file IO and directory management (i.e., not related to
arrays) on all storage backends TileDB supports.

.. _figure-24:

.. figure:: vfs.png
    :align: center

    Figure 24: The virtual filesystem module is exposed to the C API

.. note::
    Currently the virtual filesystem functionality **does not support
    seekable writes**, but rather only appends, due to the fact that
    this is the only write mode in TileDB and because of our
    integration with append-only filesystems like HDFS and
    S3. Moreover, **moving files across filesystems** is not yet
    implemented. We plan to add both seakable writes (for filesystems
    that support it) and moving across filesystems in the next
    release."
