Working with HDFS
=================

This is a simple guide that demonstrates how to use TileDB on
`HDFS <http://hadoop.apache.org/>`_. HDFS is a distributed Java-based
filesystem for storing large amounts of data. It is the underlying
distributed storage layer for the Hadoop stack.

.. note::

   The HDFS backend currently only works on POSIX (Linux, OSX) platforms.
   Windows is currently not supported.

TileDB integrates with HDFS though the ``libhdfs`` library
(HDFS C-API). The HDFS backend is enabled by default and ``libhdfs`` loading
happens **at runtime** based on environment variables:

.. table:: HDFS environment variables
    :widths: auto

    =====================================   ========================================================================
    **Environment variable**                **Description**
    -------------------------------------   ------------------------------------------------------------------------
    ``HADOOP_HOME``                         The root of your installed Hadoop distribution. TileDB will search the
                                            path ``$HADOOP_HOME/lib/native`` to find the ``libhdfs`` shared library.
    ``JAVA_HOME``                           The location of the Java SDK installation. The ``JAVA_HOME`` variable may
                                            be set to the root path for the JAVA SDK, the JRE path, or to the directory
                                            containing the ``libjvm`` library.
    ``CLASSPATH``                           The Java classpath **including** the Hadoop jar files. The correct
                                            ``CLASSPATH`` variable can be set directly using the hadoop utility:
                                            ``CLASSPATH=$HADOOP_HOME/bin/hadoop classpath –glob``
    =====================================   ========================================================================

If the library cannot be found, or if the Hadoop library cannot locate the
correct library dependencies a runtime, an error will be returned.

To use HDFS with TileDB, change the URI you use to an HDFS path:

   ``hdfs://<authority>:<port>/path/to/array``

For instance, if you are running a local HDFS namenode on port 9000:

   ``hdfs://localhost:9000/path/to/array``

If you want to use the namenode specified in your HDFS configuration
files, then change the prefix to:

    ``hdfs:///path/to/array`` or ``hdfs://default/path/to/array``

Most HDFS configuration variables are defined in Hadoop specific XML files.
TileDB allows the following configuration variables to be
set at run time through configuration parameters:


.. table:: TileDB HDFS config parameters
    :widths: auto

    =====================================   ========================================================================
    **Parameter**                           **Description**
    -------------------------------------   ------------------------------------------------------------------------
    ``vfs.hdfs.username``                   Optional runtime username to use when connecting to the HDFS cluster.
    ``vfs.hdfs.name_node_uri``              Optional namenode URI to use (TileDB will use ``"default"`` if not
                                            specified). URI must be specified in the format
                                            ``<protocol>://<hostname>:<port>``, ex: ``hdfs://localhost:9000``.
                                            If the string starts with a protocol type such as ``file://`` or
                                            ``s3://``, this protocol will be used instead of the default ``hdfs://``.
    ``vfs.hdfs.kerb_ticket_cache_path``     Path to the Kerberos ticket cache when connecting to an HDFS cluster.
    =====================================   ========================================================================

