Concurrency
===========

TileDB is designed specifically having **parallel computation** in mind.
In order to achieve high performance when analyzing massive scientific
data, it is important for the storage manager to enable reads and writes
to be performed **concurrently** via multi-threading (e.g., with `C++ 11
threads <http://www.cplusplus.com/reference/thread/thread/>`__ or
`OpenMP <http://www.openmp.org/>`__) or multi-processing (e.g., with
`MPI <https://www.mpich.org/>`__). If the storage manager lacks this
ability, then all IO will have to be performed sequentially, which will
have a strong adverse effect on performance.

The table below summarizes the **thread-/process-safety** ability of
various TileDB operations, which is explained in more detail in the rest
of this page. Note that all discussions regarding “arrays” below
encompass the TileDB **key-value stores** as well, since they are
implemented via sparse arrays and inherit all their properties.

Note that **locking** (wherever it is needed) is achieved via mutexes
and condition variables in multi-threading, and file locking in
multi-processing (for those storage backends that support it).

.. warning::
    All **POSIX-compliant**
    filesystems and **Windows** filesystems support file locking. Note that
    **Lustre** supports POSIX file locking semantics and exposes local-
    (mount with ``-o localflock``) and cluster- (mount with ``-o flock``)
    level locking. Currently, TileDB does not use file locking on **HDFS**
    and **S3** (these storage backends do not provide such functionality,
    but rather resource locking must be implemented as an external
    feature).

.. These correspond to custom.css rules
.. role:: red
.. role:: green

==============================    ===============     ================
**Operation**                     **Thread-safe**     **Process-safe**
------------------------------    ---------------     ----------------
Write/Read Array                    :green:`✔`        :green:`✔`
Consolidate Array                   :green:`✔`        :green:`✔`
Create Array/Group                  :green:`✔`        :red:`✘`
Move/Delete/List/Walk Array         :red:`✘`          :red:`✘`
All VFS Operations                  :red:`✘`          :red:`✘`
==============================    ===============     ================

Write/Read Array
----------------

In TileDB, each write operation is **atomic**. Concurrent writes are
achieved by having each thread or process create a **separate
fragment**. No internal state is shared and, thus, no locking is
necessary at all. Note that each thread/process creates a fragment with
a unique name, using its id and the current timestamp. Therefore, there
are no conflicts even at the filesystem level.

Reads from multiple processes are independent and no locking is
required. Every process loads its own metadata from persistent storage,
and maintains its own read state. For multi-threaded reads, TileDB
maintains a single copy of the fragment metadata, utilizing mutex
locking to protect the open arrays structure so that only one thread
modifies the metadata at a time. Reads themselves do not require
locking.

Concurrent reads and writes can be arbitrarily mixed. Fragments are not
visible unless the write query has been finalized. Fragment-based writes
make it so that reads simply see the logical view of the array without
the new (incomplete) fragment. This
**multiple-writers-multiple-readers** concurrency model of TileDB is
different (and more powerful) than competing approaches, such as HDF5’s
single-writer-multiple-readers (SWMR) model. This feature comes with a
more relaxed consistency model, called **eventual consistency**
(explained in the :doc:`Consistency <consistency>` section).

Consolidate Array
-----------------

Consolidation can be performed in the background in parallel with other
reads and writes. Locking is required only for a very brief period.
Specifically, consolidation is performed independently of reads and
writes. The new fragment that is being created is not visible to reads
before consolidation is completed. The only time when locking is
required is after the consolidation finishes, when the old fragments are
deleted and the new fragment becomes visible. TileDB enforces its own
file locking at this point. After all current reads release their shared
lock, the consolidation function gets an exclusive lock, deletes the old
fragments, makes the new fragment visible, and releases the lock. After
that point, any future reads see the consolidated fragment.

Create Array/Group
------------------

Creating an array or group is thread-safe, as this can easily be
implemented via mutexes. Process-safety is more tricky and is not
currently supported in TileDB. The reason is that both an array and a
group are implemented as **directories**. In order for the filelock to
work, the file implementing the lock must exist **before** the creation
of the array/group. However, the new array/group directory can be stored
anywhere and, as such, there must be a specific global place where the
file lock must be stored. Currently, TileDB does not recognize a default
user directory (e.g., ``~/.tiledb`` could be one candidate), where such
a lock could be stored. Nevertheless, we plan to add process-safety for
this operation in a future release.

Move/Delete/List/Walk Array
---------------------------

These operations are not thread-/process-safe.

All VFS Operations
------------------

None of the virtual filesystem operations exposed at the TileDB API
currently support guaranteed thread-/process-safety. Thread-/process-safety
guarantees are inherited from the underlying storage backend.
