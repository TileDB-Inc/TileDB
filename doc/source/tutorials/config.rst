.. _config:

Configuration Parameters
========================

This tutorial demonstrates how to set and get the TileDB
config parameters, and summarizes all current config parameters
explaining their function.

  ====================================  =============================================================
  **Program**                           **Links**
  ------------------------------------  -------------------------------------------------------------
  ``config``                            |configcpp| |configpy|
  ====================================  =============================================================


.. |configcpp| image:: ../figures/cpp.png
   :align: middle
   :width: 30
   :target: {tiledb_src_root_url}/examples/cpp_api/config.cc

.. |configpy| image:: ../figures/python.png
   :align: middle
   :width: 25
   :target: {tiledb_py_src_root_url}/examples/config.py

You can create a config object and pass it to either a TileDB
context or VFS object as follows:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        // Create config object
        Config config;

        // Set/Get config to/from ctx
        Context ctx(config);
        Config config_ctx = ctx.config();

        // Set/Get config to/from VFS
        VFS vfs(ctx, config);
        Config config_vfs = vfs.config();

   .. tab-container:: python
      :title: Python

      .. code-block:: python

        # Create config object
        config = tiledb.Config()

        # Set/get config to/from ctx
        ctx = tiledb.Ctx(config)
        config_ctx = ctx.config()

        # Set/get config to/from VFS
        vfs = tiledb.VFS(ctx, config)
        config_vfs = vfs.config()

Running the ``vfs`` code example we get the output shown below.
In the rest of the tutorial
we will discuss the various ways we used the config objects
in this program and explain the output.

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: bash

        $ g++ -std=c++11 config.cc -o config_cpp -ltiledb
        $ ./config_cpp
        Tile cache size: 10000000

        Default settings:
        "sm.array_schema_cache_size" : "10000000"
        "sm.check_coord_dups" : "true"
        "sm.check_coord_oob" : "true"
        "sm.dedup_coords" : "false"
        "sm.enable_signal_handlers" : "true"
        "sm.fragment_metadata_cache_size" : "10000000"
        "sm.num_async_threads" : "1"
        "sm.num_reader_threads" : "1"
        "sm.num_tbb_threads" : "-1"
        "sm.num_writer_threads" : "1"
        "sm.tile_cache_size" : "10000000"
        "vfs.file.max_parallel_ops" : "8"
        "vfs.hdfs.kerb_ticket_cache_path" : ""
        "vfs.hdfs.name_node_uri" : ""
        "vfs.hdfs.username" : ""
        "vfs.min_parallel_size" : "10485760"
        "vfs.num_threads" : "8"
        "vfs.s3.connect_max_tries" : "5"
        "vfs.s3.connect_scale_factor" : "25"
        "vfs.s3.connect_timeout_ms" : "3000"
        "vfs.s3.endpoint_override" : ""
        "vfs.s3.max_parallel_ops" : "8"
        "vfs.s3.multipart_part_size" : "5242880"
        "vfs.s3.proxy_host" : ""
        "vfs.s3.proxy_password" : ""
        "vfs.s3.proxy_port" : "0"
        "vfs.s3.proxy_scheme" : "https"
        "vfs.s3.proxy_username" : ""
        "vfs.s3.region" : "us-east-1"
        "vfs.s3.request_timeout_ms" : "3000"
        "vfs.s3.scheme" : "https"
        "vfs.s3.use_virtual_addressing" : "true"

        VFS S3 settings:
        "connect_max_tries" : "5"
        "connect_scale_factor" : "25"
        "connect_timeout_ms" : "3000"
        "endpoint_override" : ""
        "max_parallel_ops" : "8"
        "multipart_part_size" : "5242880"
        "proxy_host" : ""
        "proxy_password" : ""
        "proxy_port" : "0"
        "proxy_scheme" : "https"
        "proxy_username" : ""
        "region" : "us-east-1"
        "request_timeout_ms" : "3000"
        "scheme" : "https"
        "use_virtual_addressing" : "true"

        Tile cache size after loading from file: 0

   .. tab-container:: python
      :title: Python

      .. code-block:: bash

        $ python config.py
        Tile cache size: 10000000

        Default settings:
        "sm.array_schema_cache_size" : "10000000"
        "sm.check_coord_dups" : "true"
        "sm.check_coord_oob" : "true"
        "sm.dedup_coords" : "false"
        "sm.enable_signal_handlers" : "true"
        "sm.fragment_metadata_cache_size" : "10000000"
        "sm.num_async_threads" : "1"
        "sm.num_reader_threads" : "1"
        "sm.num_tbb_threads" : "-1"
        "sm.num_writer_threads" : "1"
        "sm.tile_cache_size" : "10000000"
        "vfs.file.max_parallel_ops" : "8"
        "vfs.hdfs.kerb_ticket_cache_path" : ""
        "vfs.hdfs.name_node_uri" : ""
        "vfs.hdfs.username" : ""
        "vfs.min_parallel_size" : "10485760"
        "vfs.num_threads" : "8"
        "vfs.s3.connect_max_tries" : "5"
        "vfs.s3.connect_scale_factor" : "25"
        "vfs.s3.connect_timeout_ms" : "3000"
        "vfs.s3.endpoint_override" : ""
        "vfs.s3.max_parallel_ops" : "8"
        "vfs.s3.multipart_part_size" : "5242880"
        "vfs.s3.proxy_host" : ""
        "vfs.s3.proxy_password" : ""
        "vfs.s3.proxy_port" : "0"
        "vfs.s3.proxy_scheme" : "https"
        "vfs.s3.proxy_username" : ""
        "vfs.s3.region" : "us-east-1"
        "vfs.s3.request_timeout_ms" : "3000"
        "vfs.s3.scheme" : "https"
        "vfs.s3.use_virtual_addressing" : "true"

        VFS S3 settings:
        "connect_max_tries" : "5"
        "connect_scale_factor" : "25"
        "connect_timeout_ms" : "3000"
        "endpoint_override" : ""
        "max_parallel_ops" : "8"
        "multipart_part_size" : "5242880"
        "proxy_host" : ""
        "proxy_password" : ""
        "proxy_port" : "0"
        "proxy_scheme" : "https"
        "proxy_username" : ""
        "region" : "us-east-1"
        "request_timeout_ms" : "3000"
        "scheme" : "https"
        "use_virtual_addressing" : "true"

        Tile cache size after loading from file: 0


Setting/Getting config parameters
---------------------------------

*The TileDB config object is a simplified, in-memory key-value store/map,
which accepts only string keys and values*. The code below simply sets two parameters
and gets the value of a third parameter. We explain the TileDB parameters
at the end of this tutorial.

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        Config config;

        // Set value
        config["vfs.s3.connect_timeout_ms"] = 5000;

        // Append parameter segments with successive []
        config["vfs."]["s3."]["endpoint_override"] = "localhost:8888";

        // Get value
        std::string tile_cache_size = config["sm.tile_cache_size"];
        std::cout << "Tile cache size: " << tile_cache_size << "\n\n";

   .. tab-container:: python
      :title: Python

      .. code-block:: python

        config = tiledb.Config()

        # Set value
        config["vfs.s3.connect_timeout_ms"] = 5000

        # Get value
        tile_cache_size = config["sm.tile_cache_size"]
        print("Tile cache size: %s" % str(tile_cache_size))

The above code snippet produces the following output in our program:

.. code-block:: bash

   Tile cache size: 10000000


Iterating over config parameters
--------------------------------

TileDB allows you to iterate over the configuration parameters as well.
The code below prints the default parameters of a config object, as
we iterate before setting any new parameter value.

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

       Config config;
       std::cout << "Default settings:\n";
       for (auto& p : config) {
         std::cout << "\"" << p.first << "\" : \"" << p.second << "\"\n";
       }

   .. tab-container:: python
      :title: Python

      .. code-block:: python

        config = tiledb.Config()
        print("\nDefault settings:")
        for p in config.items():
            print("\"%s\" : \"%s\"" % (p[0], p[1]))

The corresponding output is (note that we ran this on a machine with
8 cores):

.. code-block:: bash

   Default settings:
   "sm.array_schema_cache_size" : "10000000"
   "sm.check_coord_dups" : "true"
   "sm.check_coord_oob" : "true"
   "sm.dedup_coords" : "false"
   "sm.enable_signal_handlers" : "true"
   "sm.fragment_metadata_cache_size" : "10000000"
   "sm.num_async_threads" : "1"
   "sm.num_reader_threads" : "1"
   "sm.num_tbb_threads" : "-1"
   "sm.num_writer_threads" : "1"
   "sm.tile_cache_size" : "10000000"
   "vfs.file.max_parallel_ops" : "8"
   "vfs.hdfs.kerb_ticket_cache_path" : ""
   "vfs.hdfs.name_node_uri" : ""
   "vfs.hdfs.username" : ""
   "vfs.min_parallel_size" : "10485760"
   "vfs.num_threads" : "8"
   "vfs.s3.connect_max_tries" : "5"
   "vfs.s3.connect_scale_factor" : "25"
   "vfs.s3.connect_timeout_ms" : "3000"
   "vfs.s3.endpoint_override" : ""
   "vfs.s3.max_parallel_ops" : "8"
   "vfs.s3.multipart_part_size" : "5242880"
   "vfs.s3.proxy_host" : ""
   "vfs.s3.proxy_password" : ""
   "vfs.s3.proxy_port" : "0"
   "vfs.s3.proxy_scheme" : "https"
   "vfs.s3.proxy_username" : ""
   "vfs.s3.region" : "us-east-1"
   "vfs.s3.request_timeout_ms" : "3000"
   "vfs.s3.scheme" : "https"
   "vfs.s3.use_virtual_addressing" : "true"


TileDB allows you also to iterate only over the config parameters
with a certain *prefix* as follows:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        Config config;

        // Print only the S3 settings
        std::cout << "\nVFS S3 settings:\n";
        for (auto i = config.begin("vfs.s3."); i != config.end(); ++i) {
          auto& p = *i;
          std::cout << "\"" << p.first << "\" : \"" << p.second << "\"\n";
        }

   .. tab-container:: python
      :title: Python

      .. code-block:: python

        config = tiledb.Config()
        # Print only the S3 settings.
        print("\nVFS S3 settings:")
        for p in config.items("vfs.s3."):
            print("\"%s\" : \"%s\"" % (p[0], p[1]))

The above produces the following output. Observe that the prefix
is *stripped* from the retrieved parameter names.

.. code-block:: bash

   VFS S3 settings:
   "connect_max_tries" : "5"
   "connect_scale_factor" : "25"
   "connect_timeout_ms" : "3000"
   "endpoint_override" : ""
   "max_parallel_ops" : "8"
   "multipart_part_size" : "5242880"
   "proxy_host" : ""
   "proxy_password" : ""
   "proxy_port" : "0"
   "proxy_scheme" : "https"
   "proxy_username" : ""
   "region" : "us-east-1"
   "request_timeout_ms" : "3000"
   "scheme" : "https"
   "use_virtual_addressing" : "true"

Saving/Loading config to/from file
----------------------------------

You can save the configuration parameters you used in your program
into a (local) text file, and subsequently load them from the
file into a new TileDB config if needed as follows:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        // Save to file
        Config config;
        config["sm.tile_cache_size"] = 0;
        config.save_to_file("tiledb_config.txt");

        // Load from file
        Config config_load("tiledb_config.txt");
        std::string tile_cache_size = config_load["sm.tile_cache_size"];
        std::cout << "\nTile cache size after loading from file: " << tile_cache_size
                  << "\n";

   .. tab-container:: python
      :title: Python

      .. code-block:: python

        # Save to file
        config = tiledb.Config()
        config["sm.tile_cache_size"] = 0
        config.save("tiledb_config.txt")

        # Load from file
        config_load = tiledb.Config.load("tiledb_config.txt")
        print("\nTile cache size after loading from file: %s" % str(config_load["sm.tile_cache_size"]))

The above code creates a config object, changes the tile cache size to ``0``,
and saves the entire configuration into a file. Next, it creates a new
config loading the values from the created file. Running the program
produces the following output. Observe that the loaded tile cache size
value is ``0``, which is the value we altered prior to saving the config
to the file.

.. code-block:: bash

   Tile cache size after loading from file: 0

Inspecting the contents of the exported config file, we get the following:

.. code-block:: bash

  $ cat tiledb_config.txt
  sm.array_schema_cache_size 10000000
  sm.check_coord_dups true
  sm.check_coord_oob true
  sm.dedup_coords false
  sm.enable_signal_handlers true
  sm.fragment_metadata_cache_size 10000000
  sm.num_async_threads 1
  sm.num_reader_threads 1
  sm.num_tbb_threads -1
  sm.num_writer_threads 1
  sm.tile_cache_size 0
  vfs.file.max_parallel_ops 8
  vfs.min_parallel_size 10485760
  vfs.num_threads 8
  vfs.s3.connect_max_tries 5
  vfs.s3.connect_scale_factor 25
  vfs.s3.connect_timeout_ms 3000
  vfs.s3.max_parallel_ops 8
  vfs.s3.multipart_part_size 5242880
  vfs.s3.proxy_port 0
  vfs.s3.proxy_scheme https
  vfs.s3.region us-east-1
  vfs.s3.request_timeout_ms 3000
  vfs.s3.scheme https
  vfs.s3.use_virtual_addressing true

Observe that config parameters that have an empty string as a value
are not exported (e.g., ``vfs.s3.proxy_host``).
Note also that ``vfs.s3.proxy_username`` and
``vfs.s3.proxy_password`` are not exported for security purposes.

Summary of Parameters
---------------------

Below we provide a table with all the TileDB configuration parameters,
along with their description and default values.

.. table:: TileDB config parameters
    :widths: auto

    ======================================    ===================     ==================================================
    **Parameter**                             **Default Value**       **Description**
    --------------------------------------    -------------------     --------------------------------------------------
    ``"sm.array_schema_cache_size"``          ``"10000000"``          The array schema cache size in bytes.
    ``"sm.check_coord_dups"``                 ``"true"``              This is applicable only if ``sm.dedup_coords`` is
                                                                      ``false``. If ``true``, an error will be thrown if
                                                                      there are cells with duplicate coordinates during
                                                                      sparse array writes. If ``false`` and there are
                                                                      duplicates, the duplicates will be written without
                                                                      errors, but the TileDB behavior could be
                                                                      unpredictable.
    ``"sm.check_coord_oob"``                  ``"true"``              If ``true``, an error will be thrown if
                                                                      there are cells with coordinates lying outside
                                                                      the array domain during sparse array writes.
    ``"sm.dedup_coords"``                     ``"false"``             If ``true``, cells with duplicate coordinates
                                                                      will be removed during sparse array writes. Note
                                                                      that ties during deduplication are broken
                                                                      arbitrarily.
    ``"sm.enable_signal_handlers"``           ``"true"``              Determines whether or not TileDB will install
                                                                      signal handlers.
    ``"sm.fragment_metadata_cache_size"``     ``"10000000"``          The fragment metadata cache size in bytes.
    ``"sm.num_async_threads"``                ``"1"``                 The number of threads allocated for async queries.
    ``"sm.num_reader_threads"``               ``"1"``                 The number of threads allocated for filesystem
                                                                      read operations.
    ``"sm.num_writer_threads"``               ``"1"``                 The number of threads allocated for filesystem
                                                                      write operations.
    ``"sm.num_tbb_threads"``                  ``"-1"``                The number of threads allocated for the TBB thread
                                                                      pool (if TBB is enabled). **Note:** this is a
                                                                      whole-program setting. Usually this should not be
                                                                      modified from the default. See also the
                                                                      documentation for TBB's ``task_scheduler_init``
                                                                      class.
    ``"sm.tile_cache_size"``                  ``"10000000"``          The tile cache size in bytes.
    ``"vfs.num_threads"``                     # of cores              The number of threads allocated for VFS
                                                                      operations (any backend), per VFS instance.
    ``"vfs.file.max_parallel_ops"``           ``vfs.num_threads``     The maximum number of parallel operations on
                                                                      objects with ``file:///`` URIs.
    ``"vfs.min_parallel_size"``               ``"10485760"``          The minimum number of bytes in a parallel VFS
                                                                      operation (except parallel S3 writes, which are
                                                                      controlled by ``vfs.s3.multipart_part_size``).
    ``"vfs.s3.connect_max_tries"``            ``"5"``                 The maximum tries for a connection. Any ``long``
                                                                      value is acceptable.
    ``"vfs.s3.connect_scale_factor"``         ``"25"``                The scale factor for exponential backoff when
                                                                      connecting to S3. Any ``long`` value is
                                                                      acceptable.
    ``"vfs.s3.connect_timeout_ms"``           ``"3000"``              The connection timeout in ms. Any ``long`` value
                                                                      is acceptable.
    ``"vfs.s3.endpoint_override"``            ``""``                  The S3 endpoint, if S3 is enabled.
    ``"vfs.s3.max_parallel_ops"``             ``vfs.num_threads``     The maximum number of S3 backend parallel
                                                                      operations.
    ``"vfs.s3.multipart_part_size"``          ``"5242880"``           The part size (in bytes) used in S3 multipart
                                                                      writes. Any ``uint64_t`` value is acceptable.
                                                                      **Note:** ``vfs.s3.multipart_part_size *
                                                                      vfs.s3.max_parallel_ops`` bytes will be buffered
                                                                      before issuing multipart uploads in parallel.
    ``"vfs.s3.proxy_host"``                   ``""``                  The S3 proxy host.
    ``"vfs.s3.proxy_password"``               ``""``                  The S3 proxy password.
    ``"vfs.s3.proxy_port"``                   ``"0"``                 The S3 proxy port.
    ``"vfs.s3.proxy_scheme"``                 ``"https"``             The S3 proxy scheme.
    ``"vfs.s3.proxy_username"``               ``""``                  The S3 proxy username.
    ``"vfs.s3.region"``                       ``"us-east-1"``         The S3 region.
    ``"vfs.s3.request_timeout_ms"``           ``"3000"``              The request timeout in ms. Any ``long`` value is
                                                                      acceptable.
    ``"vfs.s3.scheme"``                       ``"https"``             The S3 scheme.
    ``"vfs.s3.use_virtual_addressing"``       ``"true"``              Determines whether to use virtual addressing
                                                                      or not.
    ``"vfs.hdfs.kerb_ticket_cache_path"``     ``""``                  Path to the Kerberos ticket cache when connecting
                                                                      to an HDFS cluster.
    ``"vfs.hdfs.name_node_uri"``              ``""``                  Optional namenode URI to use (TileDB will use
                                                                      ``"default"`` if not specified). URI must be
                                                                      specified in the format
                                                                      ``<protocol>://<hostname>:<port>``,
                                                                      ex: ``hdfs://localhost:9000``. If the string
                                                                      starts with a protocol type such as ``file://``
                                                                      or ``s3://`` this protocol will be used (default
                                                                      ``hdfs://``).
    ``"vfs.hdfs.username"``                   ``""``                  Username to use when connecting to the HDFS
                                                                      cluster.
    ======================================    ===================     ==================================================


