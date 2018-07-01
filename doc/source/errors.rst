Catching Errors
===============

In this tutorial we shows how to catch errors resulting from
using the TileDB API.

.. toggle-header::
    :header: **Example Code Listing**

    .. content-tabs::

       .. tab-container:: cpp
          :title: C++

          .. literalinclude:: {source_examples_path}/cpp_api/errors.cc
             :language: c++
             :linenos:

TileDB provides an easy way to check for an error for every API call,
as well as retrieve the error message. We provide a simple example in
the code listing above. After compiling and running the program, you
get the following output (``<cwd>`` below stands for you current
working directory path):

.. code-block:: bash

   $ g++ -std=c++11 errors.cc -o errors_cpp -ltiledb
   $ ./errors_cpp
   TileDB exception:
   [TileDB::StorageManager] Error: Cannot create group; Group '<cwd>/my_group' already exists
   Callback:
   [TileDB::StorageManager] Error: Cannot create group; Group '<cwd>/my_group' already exists

Note that if TileDB is built from source in *debug mode*, then the error messages will
be printed to standard output even if you do not catch explicitly the error as
shown above.
