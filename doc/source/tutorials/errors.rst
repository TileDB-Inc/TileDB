:orphan:

Catching Errors
===============

In this tutorial we shows how to catch errors resulting from
using the TileDB API.

  ====================================  =============================================================
  **Program**                           **Links**
  ------------------------------------  -------------------------------------------------------------
  ``errors``                            |errorscpp| |errorspy|
  ====================================  =============================================================


.. |errorscpp| image:: ../figures/cpp.png
   :align: middle
   :width: 30
   :target: {tiledb_src_root_url}/examples/cpp_api/errors.cc

.. |errorspy| image:: ../figures/python.png
   :align: middle
   :width: 25
   :target: {tiledb_py_src_root_url}/examples/errors.py

TileDB provides an easy way to check for an error for every API call,
as well as retrieve the error message. We provide a simple example in
the code listing above. After compiling and running the program, you
get the following output (``<cwd>`` below stands for you current
working directory path):

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: bash

        $ g++ -std=c++11 errors.cc -o errors_cpp -ltiledb
        $ ./errors_cpp
        TileDB exception:
        [TileDB::StorageManager] Error: Cannot create group; Group '<cwd>/my_group' already exists
        Callback:
        [TileDB::StorageManager] Error: Cannot create group; Group '<cwd>/my_group' already exists

   .. tab-container:: python
      :title: Python

      .. code-block:: bash

        $ python errors.py
        TileDB exception: [TileDB::StorageManager] Error: Cannot create group; Group '<cwd>/my_group' already exists

Note that if TileDB is built from source in *debug* and *verbose* modes, then the
error messages will be printed to standard output even if you do not catch explicitly
the error as shown above.
