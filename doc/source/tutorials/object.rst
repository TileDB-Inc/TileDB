Object Management
=================

In this tutorial you will learn about how you can organize your
TileDB arrays and key-value stores *hierarchically* into TileDB groups.
We call a TileDB array, key-value store or group as *TileDB object*.
We also discuss some auxiliary TileDB object management functions.

.. warning::

   This TileDB feature is experimental. Everything covered here works
   great, but the APIs might undergo changes in future versions.

.. table:: Full programs
  :widths: auto

  ====================================  =============================================================
  **Program**                           **Links**
  ------------------------------------  -------------------------------------------------------------
  ``object``                            |objectcpp| |objectpy|
  ====================================  =============================================================


.. |objectcpp| image:: ../figures/cpp.png
   :align: middle
   :width: 30
   :target: {tiledb_src_root_url}/examples/cpp_api/object.cc

.. |objectpy| image:: ../figures/python.png
   :align: middle
   :width: 25
   :target: {tiledb_py_src_root_url}/examples/object.py


TileDB groups
-------------

TileDB allows you to hierarchically organize your arrays and key-value stores
in *groups*. A group is practically a directory with a special (empty) TileDB
file ``__tiledb_group.tdb``. This offers an intuitive and familiar way
to store your various TileDB objects in persistent storage. You can create
a group simply as follows:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        tiledb::Context ctx;
        tiledb::create_group(ctx, "my_group");

   .. tab-container:: python
      :title: Python

      .. code-block:: python

        ctx = tiledb.Ctx()
        tiledb.group_create(ctx, "my_group")

Listing the ``my_group`` directory, you get the following:

.. code-block:: bash

   $ ls -l my_group
   total 0
   -rwx------  1 stavros  staff    0 Jul  3 10:08 __tiledb_group.tdb

Note that you can hierarchically organize TileDB groups similar to
your filesystem directories, i.e., groups can be arbitrarily *nested*
in other groups.

Getting the object type
-----------------------

TileDB also allows you to check the object type as follows. If ``path``
does not exist or is not a TileDB array, key-value store or group, it is
marked as "invalid".

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        tiledb::Context ctx;
        auto obj_type = Object::object(ctx, path).type();

   .. tab-container:: python
      :title: Python

      .. code-block:: python

       ctx = tiledb.Ctx()
       type = tiledb.object_type(ctx, path)


Listing the object hierarchy
----------------------------

TileDB offers various ways to list the contents of a group,
even recursively in pre-order or post-order traversal, optionally
passing a special *callback* function that will be invoked for
every visited object. This is demonstrated in the code snippet below:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        tiledb::Context ctx;

        // List children
        std::cout << "\nListing hierarchy: \n";
        tiledb::ObjectIter obj_iter(ctx, path);
        for (const auto& object : obj_iter)
        print_path(object.uri(), object.type());

        // Walk in a path with a pre- and post-order traversal
        std::cout << "\nPreorder traversal: \n";
        obj_iter.set_recursive();  // Default order is preorder
        for (const auto& object : obj_iter)
          print_path(object.uri(), object.type());
        std::cout << "\nPostorder traversal: \n";
        obj_iter.set_recursive(TILEDB_POSTORDER);
        for (const auto& object : obj_iter)
          print_path(object.uri(), object.type());

      where the ``print_path`` callback takes as input a string path and an object
      type argument. This is how we defined it in our code example:

      .. code-block:: c++

        void print_path(const std::string& path, tiledb::Object::Type type) {
          // Simply print the path and type
          std::cout << path << " ";
          switch (type) {
            case tiledb::Object::Type::Array:
              std::cout << "ARRAY";
              break;
            case tiledb::Object::Type::KeyValue:
              std::cout << "KEY_VALUE";
              break;
            case tiledb::Object::Type::Group:
              std::cout << "GROUP";
              break;
            default:
              std::cout << "INVALID";
          }
          std::cout << "\n";
        }

   .. tab-container:: python
      :title: Python

      .. code-block:: python

        ctx = tiledb.Ctx()

        # List children
        print("\nListing hierarchy:")
        tiledb.ls(ctx, path, lambda obj_path, obj_type: print(obj_path, obj_type))

        # Walk in a path with a pre- and post-order traversal
        print("\nPreorder traversal:")
        tiledb.walk(ctx, path, lambda obj_path, obj_type: print(obj_path, obj_type))  # Default order is preorder

        print("\nPostorder traversal:")
        tiledb.walk(ctx, path, lambda obj_path, obj_type: print(obj_path, obj_type), "postorder")

In the ``object`` code example, we initially create the following hierarchy:

.. code-block:: bash

 my_group/
 ├── dense_arrays
 │   ├── array_A
 │   ├── array_B
 │   └── kv
 └── sparse_arrays
     ├── array_C
     └── array_D

The code snippet we provided above would print out the following for this
hierarchy (where ``<cwd>`` is the full path of your current working
directory):

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: bash

        Listing hierarchy:
        file://<cwd>/my_group/dense_arrays GROUP
        file://<cwd>/my_group/sparse_arrays GROUP

        Preorder traversal:
        file://<cwd>/my_group/dense_arrays GROUP
        file://<cwd>/my_group/dense_arrays/array_A ARRAY
        file://<cwd>/my_group/dense_arrays/array_B ARRAY
        file://<cwd>/my_group/dense_arrays/kv KEY_VALUE
        file://<cwd>/my_group/sparse_arrays GROUP
        file://<cwd>/my_group/sparse_arrays/array_C ARRAY
        file://<cwd>/my_group/sparse_arrays/array_D ARRAY

        Postorder traversal:
        file://<cwd>/my_group/dense_arrays/array_A ARRAY
        file://<cwd>/my_group/dense_arrays/array_B ARRAY
        file://<cwd>/my_group/dense_arrays/kv KEY_VALUE
        file://<cwd>/my_group/dense_arrays GROUP
        file://<cwd>/my_group/sparse_arrays/array_C ARRAY
        file://<cwd>/my_group/sparse_arrays/array_D ARRAY
        file://<cwd>/my_group/sparse_arrays GROUP

   .. tab-container:: python
      :title: Python

      .. code-block:: bash

        Listing hierarchy:
        file://<cwd>/my_group/dense_arrays group
        file://<cwd>/my_group/sparse_arrays group

        Preorder traversal:
        file://<cwd>/my_group/dense_arrays group
        file://<cwd>/my_group/dense_arrays/array_A array
        file://<cwd>/my_group/dense_arrays/array_B array
        file://<cwd>/my_group/dense_arrays/kv kv
        file://<cwd>/my_group/sparse_arrays group
        file://<cwd>/my_group/sparse_arrays/array_C array
        file://<cwd>/my_group/sparse_arrays/array_D array

        Postorder traversal:
        file://<cwd>/my_group/dense_arrays/array_A array
        file://<cwd>/my_group/dense_arrays/array_B array
        file://<cwd>/my_group/dense_arrays/kv kv
        file://<cwd>/my_group/dense_arrays group
        file://<cwd>/my_group/sparse_arrays/array_C array
        file://<cwd>/my_group/sparse_arrays/array_D array
        file://<cwd>/my_group/sparse_arrays group

Move/Remove objects
-------------------

TileDB offers functions for renaming and removing TileDB objects.
Note that these functions are "safe", in the sense that they
will not have any effect on "invalid" (i.e., non-TileDB) objects.

You can rename TileDB objects as follows:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        tiledb::Object::move(ctx, "my_group", "my_group_2");

   .. tab-container:: python
      :title: Python

      .. code-block:: python

        tiledb.move(ctx, "my_group", "my_group_2")

.. note::

  Moving TileDB objects across different storage backends (e.g.,
  from S3 to local storage, or vice-versa) is currently not supported.
  However, it will be added in a future version.

You can remove TileDB objects as follows:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        tiledb::Object::remove(ctx, "my_group_2/dense_arrays");

   .. tab-container:: python
      :title: Python

      .. code-block:: python

        tiledb.remove(ctx, "my_group_2/dense_arrays")

Running the ``object`` code example, we get the
output shown below. Observe the listing after ``my_group`` got
renamed to ``my_group_2`` and ``my_group_2/dense_arrays``,
``my_group_2/sparse_arrays/array_C`` got removed.

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: bash

        $ g++ -std=c++11 object.cc -o object_cpp -ltiledb
        $ ./object_cpp

        Listing hierarchy:
        file://<cwd>/my_group/dense_arrays GROUP
        file://<cwd>/my_group/sparse_arrays GROUP

        Preorder traversal:
        file://<cwd>/my_group/dense_arrays GROUP
        file://<cwd>/my_group/dense_arrays/array_A ARRAY
        file://<cwd>/my_group/dense_arrays/array_B ARRAY
        file://<cwd>/my_group/dense_arrays/kv KEY_VALUE
        file://<cwd>/my_group/sparse_arrays GROUP
        file://<cwd>/my_group/sparse_arrays/array_C ARRAY
        file://<cwd>/my_group/sparse_arrays/array_D ARRAY

        Postorder traversal:
        file://<cwd>/my_group/dense_arrays/array_A ARRAY
        file://<cwd>/my_group/dense_arrays/array_B ARRAY
        file://<cwd>/my_group/dense_arrays/kv KEY_VALUE
        file://<cwd>/my_group/dense_arrays GROUP
        file://<cwd>/my_group/sparse_arrays/array_C ARRAY
        file://<cwd>/my_group/sparse_arrays/array_D ARRAY
        file://<cwd>/my_group/sparse_arrays GROUP

        Listing hierarchy:
        file://<cwd>/my_group_2/sparse_arrays GROUP

        Preorder traversal:
        file://<cwd>/my_group_2/sparse_arrays GROUP
        file://<cwd>/my_group_2/sparse_arrays/array_D ARRAY

        Postorder traversal:
        file://<cwd>/my_group_2/sparse_arrays/array_D ARRAY
        file://<cwd>/my_group_2/sparse_arrays GROUP

   .. tab-container:: python
      :title: Python

      .. code-block:: bash

        $ python object.py

        Listing hierarchy:
        file://<cwd>/my_group/dense_arrays group
        file://<cwd>/my_group/sparse_arrays group

        Preorder traversal:
        file://<cwd>/my_group/dense_arrays group
        file://<cwd>/my_group/dense_arrays/array_A array
        file://<cwd>/my_group/dense_arrays/array_B array
        file://<cwd>/my_group/dense_arrays/kv kv
        file://<cwd>/my_group/sparse_arrays group
        file://<cwd>/my_group/sparse_arrays/array_C array
        file://<cwd>/my_group/sparse_arrays/array_D array

        Postorder traversal:
        file://<cwd>/my_group/dense_arrays/array_A array
        file://<cwd>/my_group/dense_arrays/array_B array
        file://<cwd>/my_group/dense_arrays/kv kv
        file://<cwd>/my_group/dense_arrays group
        file://<cwd>/my_group/sparse_arrays/array_C array
        file://<cwd>/my_group/sparse_arrays/array_D array
        file://<cwd>/my_group/sparse_arrays group

        Listing hierarchy:
        file://<cwd>/my_group_2/sparse_arrays group

        Preorder traversal:
        file://<cwd>/my_group_2/sparse_arrays group
        file://<cwd>/my_group_2/sparse_arrays/array_D array

        Postorder traversal:
        file://<cwd>/my_group_2/sparse_arrays/array_D array
        file://<cwd>/my_group_2/sparse_arrays group


.. code-block:: bash

   $ ls -l my_group_2/
   total 0
   -rwx------  1 stavros  staff    0 Jul  3 11:18 __tiledb_group.tdb
   drwx------  4 stavros  staff  136 Jul  3 11:18 sparse_arrays
   $ ls -l my_group_2/sparse_arrays/
   total 0
   -rwx------  1 stavros  staff    0 Jul  3 11:18 __tiledb_group.tdb
   drwx------  4 stavros  staff  136 Jul  3 11:18 array_D
   $ ls -l my_group_2/sparse_arrays/array_D/
   total 8
   -rwx------  1 stavros  staff  115 Jul  3 11:18 __array_schema.tdb
   -rwx------  1 stavros  staff    0 Jul  3 11:18 __lock.tdb
