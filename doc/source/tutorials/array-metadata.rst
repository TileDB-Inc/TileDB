:orphan:

.. _array-metadata:

Array Metadata
==============

In this tutorial we will learn how to write, read and consolidate array 
metadata.

.. table:: Full programs
  :widths: auto

  ====================================  =============================================================
  **Program**                           **Links**
  ------------------------------------  -------------------------------------------------------------
  ``array_metadata``                    |arraymetadatacpp|
  ====================================  =============================================================

.. |arraymetadatacpp| image:: ../figures/cpp.png
   :align: middle
   :width: 30
   :target: {tiledb_src_root_url}/examples/cpp_api/array_metadata.cc

What is array metadata?
-----------------------

Array metadata is a collection of *key-value* pairs associated with an array.
Each metadata item is of the form: ``key : (value_type, value_num, value)``,
where ``key`` is a string key, ``value_type`` is the data type of the value,
``value_num`` is the number of elements that constitute the value (can be
more than one), and  ``value`` is the metadata value in binary form. 


TileDB persistently stores the array metadata inside the array directory. However,
note that TileDB loads *all* its metadata upon opening the array in read mode,
which means that it assumes all the array metadata is small enough to be 
maintained in main memory.

Writing array metadata
----------------------

To write array metadata, the array must be opened in *write* mode. Here is
an example: 

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        // Open array in write mode
        Context ctx;
        Array array(ctx, "my_array", TILEDB_WRITE);

        // Write two metadata items
        int v = 100;
        array.put_metadata("aaa", TILEDB_INT32, 1, &v);
        float f[] = {1.1f, 1.2f};
        array.put_metadata("bb", TILEDB_FLOAT32, 2, f);
        std::string str = "tiledb is awesome";
        array.put_metadata("c", TILEDB_CHAR, str.size(), str.data());

        // Close array - Important!
        array.close()

Note that the metadata gets flushed to persistent storage only upon 
*closing* the array.

Reading array metadata
----------------------

To read array metadata, the array must be opened in *read* mode. Here is
an example:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        // Open array in read mode
        Context ctx;
        Array array(ctx, "my_array", TILEDB_READ);

        // Read metadata with key
        tiledb_datatype_t value_type;
        uint32_t value_num;
        const void* value;
        array.get_metadata("aaa", &v_type, &v_num, &v);

        // Close array
        array.close()

You can also enumerate all array metadata as follows:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        // Open array in read mode
        Context ctx;
        Array array(ctx, "my_array", TILEDB_READ);

        // Get number of metadata items
        uint64_t num = array.metadata_num();

        // Loop getting metadata from index 
        std::string key;
        tiledb_datatype_t value_type;
        uint32_t value_num;
        const void* value;
        for (uint64_t i = 0; i < num; ++i) { 
          array.get_metadata_from_index(i, &key, &value_type, &value_num, &value);
          // Do something with the metadata
        }

        // Close array
        array.close()

Deleting array metadata
-----------------------

TileDB allows you to delete metadata simply as shown in the example 
below. The array must be opened in *write* mode and appropriately
closed in the end so that the change gets flushed to persistent storage.
Note also that you can mix writing/overwriting and deleting metadata
in a single write session (i.e., between opening an array in write mode
and closing it).

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        // Open array in write mode
        Context ctx;
        Array array(ctx, "my_array", TILEDB_WRITE);

        // Delete metadata
        array.delete_metadata("bb");

        // Close array - Important!
        array.close()

On-disk structure
-----------------

Every array metadata write session (i.e., between opening the array in write mode,
writing/deleting some metadata and closing the array) creates a timestamped
array metadata file inside the ``__meta`` directory in the array directory:

.. code-block:: bash

    $ ls -l my_array/__meta/
    total 8
    -rwx------  1 stavros  staff  127 Sep  7 18:27 __1567895268179_1567895268179_87a009d6b2cf46b68d74621635863b45

Notice that the file name has an identical structure to that of the fragment name
(see :ref:`fragments-consolidation`), i.e., it consists of a timestamp
range and a UUID. The same semantics for opening an array at a timestamp apply
also to metadata as well, i.e., if the array is opened at a timestamp before 
``1567895268179``, the above file will be ignored.

Multiple separate write sessions (executed either serially or in parallel) create
multiple timestamped metadata files, similar to fragments (and again no
array locking is necessary here).

.. code-block:: bash

    $ ls -l my_array/__meta/
    total 8
    -rwx------  1 stavros  staff  127 Sep  7 18:27 __1567895268179_1567895268179_87a009d6b2cf46b68d74621635863b45
    -rwx------  1 stavros  staff  127 Sep  7 19:21 __1567898509507_1567898509507_f0d9756d932540729059eabcfe6856d1

Consolidating array metadata
----------------------------

To avoid the uncontrollable creation of numerous array metadata files, TileDB
enables *consolidating* all files in one, similar again to fragment consoplidation:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        Context ctx;
        Array::consolidate_metadata(ctx, "my_array");

Continuing the above example, the result of consolidation is:

.. code-block:: bash

    $ ls -l my_array/__meta/
    total 8
    -rwx------  1 stavros  staff  127 Sep  7 19:30 __1567895268179_1567898509507_7382ed6aef65427e8cc9b076e6970c61

Notice that now the new file name contains a timestamp range that includes both the timestamps
of the consolidated array metadata file names. This is again very similar to fragment 
consolidation.

Encrypting array metatadata
---------------------------

The metadata of the array inherit the encryption filters of the array.
This means that if the array is encrypted, the array metadata will be
encrypted as well. Similar to array writes/reads, in order to write/read
array metadata the array must be opened with the encryption key 
(see :ref:`encryption`). Finally,
to consolidate the metadata of an encrypted array, you must use:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        Context ctx;
        Array::consolidate_metadata(ctx, "my_array", enc_type, key, key_len);
