Object Management
=================

TileDB organizes arrays, key-value stores and groups in a natural
**hierarchical** manner, similar to the organization of folders and
files in a traditional filesystem. We refer to TileDB arrays, key-value
stores and groups as TileDB **objects**. This is essentially to
differentiate the TileDB-related files/directories from other filesystem
files/directories (e.g., managed via our VFS functionality). TileDB
offers API functions that allow the user to perform typical filesystem
operations on the TileDB objects, such as ``move``, ``remove``, ``list``
and ``walk``.
