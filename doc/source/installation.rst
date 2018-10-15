.. _installation:

Installation
============

TileDB is distributed in two main components: the **core TileDB library** and
the **high-level APIs**. The core library implements all TileDB functionality,
and the APIs define the interfaces to the core library for different programming
languages.

Depending on your intended use of TileDB, you will need to install the core
TileDB library, or one of the high-level APIs such as for Python, or both. When
installing a high-level TileDB API, the corresponding installation of the core
library has been automated where possible.

Quick Installation
------------------

Apart from these quick installation steps, the rest of this page details how to
configure and build custom installations of the core TileDB library itself from
source or the pre-built packages.

C or C++ APIs
~~~~~~~~~~~~~

To use TileDB with the `C or C++ <https://github.com/TileDB-Inc/TileDB>`_ APIs:

.. content-tabs::

   .. tab-container:: macos
      :title: macOS

      .. code-block:: bash

         # Homebrew:
         $ brew update
         $ brew install tiledb-inc/stable/tiledb

         # Or Conda:
         $ conda install -c conda-forge tiledb

   .. tab-container:: linux
      :title: Linux

      .. code-block:: bash

         # Conda:
         $ conda install -c conda-forge tiledb

         # Or Docker:
         $ docker pull tiledb/tiledb
         $ docker run -it tiledb/tiledb

   .. tab-container:: windows
      :title: Windows

      .. code-block:: powershell

         # Conda
         > conda install -c conda-forge tiledb

         # Or download the pre-built release binaries from:
         # https://github.com/TileDB-Inc/TileDB/releases

Once you install TileDB, visit the :ref:`Usage <usage>` page for how to build
and link your programs against TileDB.

Python API
~~~~~~~~~~

To use TileDB with the `Python API <https://github.com/TileDB-Inc/TileDB-Py>`_:

.. code-block:: bash

    # Pip:
    $ pip install tiledb

    # Or Conda:
    $ conda install -c conda-forge tiledb-py

Both the Pip and Conda packages will automatically build and locally install
the core library in addition to the Python interface.
Full build and install instructions for the Python API can be found at the
`TileDB-Inc/TileDB-Py <https://github.com/TileDB-Inc/TileDB-Py>`_ repo.

R API
~~~~~

To use TileDB with the `R API <https://github.com/TileDB-Inc/TileDB-R>`_:

  .. code-block:: r
    
    > # From the R REPL    
    > install.packages("devtools")
    > library(devtools)
    > devtools::install_github("TileDB-Inc/TileDB-R@latest")
    ...
    > library(tiledb)
    > tiledb::libtiledb_version()
    major minor patch
    1     3     0

TileDB needs to be installed beforehand (from a package or from source)
for the TileDB-R package to build and link correctly.
Full build and install instructions for the R API can be found at the
`TileDB-Inc/TileDB-R <https://github.com/TileDB-Inc/TileDB-R>`_ repo.

Golang API
~~~~~~~~~~

To use TileDB with the `Golang API <https://github.com/TileDB-Inc/TileDB-Go>`_:

.. code-block:: bash

    # Go Get
    $ go get -v github.com/TileDB-Inc/TileDB-Go

Go get will automatically build and locally install the Golang interface.

TileDB needs to be installed beforehand (from a package or from source)
for the TileDB-Go library to build and link correctly.
Full build and install instructions for the Golang API can be found at the
`TileDB-Inc/TileDB-Go <https://github.com/TileDB-Inc/TileDB-Go>`_ repo.

Pre-built Packages
------------------

Homebrew
~~~~~~~~

The core TileDB library can be installed easily using the Homebrew package
manager for macOS. Install instructions for Homebrew are provided on the
`package manager's website <https://brew.sh/>`_.

To install the latest stable version of TileDB

.. code-block:: console

   $ brew update
   $ brew install tiledb-inc/stable/tiledb

HDFS and S3 backends are enabled by default. To disable one or more backends,
use the ``--without-`` switch to disable them

.. code-block:: console

   $ brew install tiledb-inc/stable/tiledb --without-s3
   $ brew install tiledb-inc/stable/tiledb --without-hdfs

A full list of build options can be viewed with the ``info`` command

.. code-block:: console

   $ brew info tiledb-inc/stable/tiledb

Other helpful brew commands:

* ``brew upgrade tiledb-inc/stable/tiledb``: Upgrade to the latest stable
  version of TileDB
* ``brew uninstall tiledb-inc/stable/tiledb``: Uninstall TileDB

The Homebrew Tap is located at https://github.com/TileDB-Inc/homebrew.

Docker
~~~~~~

TileDB is available as a pre-built Docker image

.. code-block:: console

   $ docker pull tiledb/tiledb
   $ docker run -it tiledb/tiledb

which uses the latest TileDB version, or

.. code-block:: console

   $ docker pull tiledb/tiledb:<version>
   $ docker run -it tiledb/tiledb:<version>

which uses a specific TileDB version (ex. ``<version>`` could be ``1.2.0``).

More info at the `TileDB Docker Hub <https://hub.docker.com/r/tiledb/tiledb/>`_
repo and the `TileDB-Docker <https://github.com/TileDB-Inc/TileDB-Docker>`_
GitHub repo.

Conda
~~~~~

A package for TileDB is available for the
`Conda package manager <https://conda.io/docs/>`_. Conda makes it easy to
install software into separate distinct environments on Windows, Linux, and
macOS

.. code-block:: console

   $ conda create -n tiledb
   $ conda activate tiledb
   $ conda install -c conda-forge tiledb

If you are compiling / linking against the TileDB conda package,
you may need to explicity add the conda path after activating the environment
with ``conda activate tiledb`` (``conda activate`` sets the ``CONDA_PREFIX``
environment variable)

.. code-block:: console

   $ export CPATH=$CONDA_PREFIX/include
   $ export LIBRARY_PATH=$CONDA_PREFIX/lib
   $ export LD_LIBRARY_PATH=$CONDA_PREFIX/lib

Or, instead of exporting those environment variables, you can pass them as
command line flags during compilation

.. code-block:: console

   $ g++ -std=c++11 example.cpp -o example -I$CONDA_PREFIX/include -L$CONDA_PREFIX/lib -ltiledb

Windows Binaries
~~~~~~~~~~~~~~~~

You can download pre-built Windows binaries in the .zip file from the
`latest TileDB release <https://github.com/TileDB-Inc/TileDB/releases>`_.
You can then simply configure your project (if using Visual Studio) according to
the :ref:`Windows usage <windows-usage>` instructions.

Building from Source
--------------------

TileDB has been tested on **Ubuntu Linux** (v.14.04+), **CentOS Linux** (v.7+),
**macOS El Capitan** (v.10.11) and **Windows** (7+), but TileDB should work
with any reasonably recent version of Ubuntu, CentOS, macOS or Windows with
an installed compiler supporting C++11.

macOS/Linux
~~~~~~~~~~~

Begin by downloading a
`release tarball <https://github.com/TileDB-Inc/TileDB/releases>`_ or by cloning
the TileDB GitHub repo and checking out a release tag (where ``<version>`` is
the version you wish to use (e.g., ``1.2.0``)

.. code-block:: console

   $ git clone https://github.com/TileDB-Inc/TileDB
   $ git checkout <version>
   $ cd TileDB

To **configure** TileDB, use the ``bootstrap`` script

.. code-block:: console

   $ mkdir build
   $ cd build
   $ ../bootstap <flags>
   $ # Or use CMake directly instead of bootstrap:
   $ # cmake <flags> ..

The flags for the bootstrap script and the CMake equivalents are as follows:

==========================   ======================================================  ==============================
**Flag**                     **Description**                                         **CMake Equivalent**
--------------------------   ------------------------------------------------------  ------------------------------
``--help``                   Prints command line flag options                        n/a
``--prefix=PREFIX``          Install files in tree rooted at ``PREFIX``              ``CMAKE_INSTALL_PREFIX=<PREFIX>``
                             (defaults to ``TileDB/dist``)
``--dependency=DIRs``        Colon separated list to binary dependencies             ``CMAKE_PREFIX_PATH=<DIRs>``
``--enable-debug``           Enable debug build                                      ``CMAKE_BUILD_TYPE=Debug``
``--enable-coverage``        Enable build with code coverage support                 ``CMAKE_BUILD_TYPE=Coverage``
``--enable-verbose``         Enable verbose status messages                          ``TILEDB_VERBOSE=ON``
``--enable-hdfs``            Enables building with HDFS storage backend support      ``TILEDB_HDFS=ON``
``--enable-s3``              Enables building with S3 storage backend support        ``TILEDB_S3=ON``
``--enable-static-tiledb``   Enables building TileDB as a static library             ``TILEDB_STATIC=ON``
``--disable-werror``         Disables building with the ``-Werror`` flag             ``TILEDB_WERROR=OFF``
``--disable-cpp-api``        Disables building the TileDB C++ API                    ``TILEDB_CPP_API=OFF``
``--disable-tbb``            Disables use of TBB for parallelization                 ``TILEDB_TBB=OFF``
``--disable-tests``          Disables building the TileDB test suite                 ``TILEDB_TESTS=OFF``
==========================   ======================================================  ==============================

To **build** after configuration, run the generated make script

.. code-block:: console

   $ make -j <nprocs>

To **install** to the configured prefix

.. code-block:: console

   $ make install-tiledb

Other helpful makefile targets:

* ``make check``: Runs the tests
* ``make examples``: Builds the examples

Windows
~~~~~~~

Building TileDB on Windows has been tested to work with Microsoft Visual Studio
2015 and later. You can install the free
`Community Edition <https://www.visualstudio.com/vs/community/>`_ if you'd like
the full IDE, or the
`Build Tools <https://www.visualstudio.com/downloads/#Other%20Tools%20and%20Frameworks>`_
if you don't need or want the IDE installed.

During the Visual Studio setup process, make sure the **Git for Windows** component
is selected if you do not already have a working Git installation. Also be sure
to select the CMake component if you do not have a working CMake installation.

In addition, you will need to install
`PowerShell <https://docs.microsoft.com/en-us/powershell/>`_ (free).

To build and install TileDB, first open PowerShell and clone the TileDB
repository

.. code-block:: console

    > git clone https://github.com/TileDB-Inc/TileDB
    > cd TileDB

Next, ensure the CMake binaries are in your path. If you installed Visual Studio, execute

.. code-block:: console

    > $env:Path += ";C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin"
    > # If you installed the build tools, instead execute:
    > # $env:Path += ";C:\Program Files (x86)\Microsoft Visual Studio\2017\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin"

Create a build directory and **configure** TileDB

.. code-block:: console

    > mkdir build
    > cd build
    > ..\bootstrap.ps1 <flags>
    > # Or use CMake directly:
    > # cmake <flags> ..

The flags for the bootstrap script and the CMake equivalents are as follows:

=======================   ================================================  ==============================
**Flag**                  **Description**                                   **CMake Equivalent**
-----------------------   ------------------------------------------------  ------------------------------
``-?``                    Display a usage message.                          n/a
``-Prefix``               Install files in tree rooted at ``PREFIX``        ``CMAKE_INSTALL_PREFIX=<PREFIX>``
                          (defaults to ``TileDB\dist``)
``-Dependency``           Semicolon separated list to binary dependencies.  ``CMAKE_PREFIX_PATH=<DIRs>``
``-CMakeGenerator``       Optionally specify the CMake generator string,    ``-G <generator>``
                          e.g. "Visual Studio 15 2017". Check
                          'cmake --help' for a list of supported
                          generators.
``-EnableDebug``          Enable debug build                                ``CMAKE_BUILD_TYPE=Debug``
``-EnableVerbose``        Enable verbose status messages.                   ``TILEDB_VERBOSE=ON``
``-EnableS3``             Enables building with the S3 storage backend.     ``TILEDB_S3=ON``
``-EnableStaticTileDB``   Enables building TileDB as a static library       ``TILEDB_STATIC=ON``
``-DisableWerror``        Disables building with the ``/WX`` flag           ``TILEDB_WERROR=OFF``
``-DisableCppApi``        Disables building the TileDB C++ API              ``TILEDB_CPP_API=OFF``
``-DisableTBB``           Disables use of TBB for parallelization           ``TILEDB_TBB=OFF``
``-DisableTests``         Disables building the TileDB test suite           ``TILEDB_TESTS=OFF``
=======================   ================================================  ==============================

To **build** after configuration

.. code-block:: console

    > cmake --build . --config Release

To **install**

.. code-block:: console

    > cmake --build . --target install-tiledb --config Release

Other helpful build targets:

* ``cmake --build . --target check --config Release``: Runs the tests
* ``cmake --build . --target examples --config Release``: Builds the examples

.. warning::

   If you build ``libtiledb`` in ``Release`` mode (resp. ``Debug``), make sure
   to build ``check`` and ``examples`` in ``Release`` mode as well (resp. ``Debug``),
   otherwise the test and example executables will not run properly.

.. warning::

   Should you experience any problem with the build, it is
   always a good idea to delete the ``build`` and ``dist`` directories in your TileDB
   repo path and restart the process, as ``cmake``'s cached state could present some
   unexpected problems.

Cygwin
~~~~~~

`Cygwin <https://https://cygwin.com/>`_ is a Unix like environment and command line interface for
Microsoft Windows that provides a large collection of GNU / OpenSource tools (including the gcc toolchain) and
supporting libraries that provide substantial POSIX API functionality.
TileDB is able to compile from source in the Cygwin environment if Intel TBB is disabled 
and some TileDB dependencies are installed as Cygwin packages.

The following Cygwin packages need to be installed:

* ``gcc / g++``
* ``git``
* ``cmake``
* ``make``
* ``lz4-devel``
* ``zlib-devel``
* ``libzstd-devel (+src)``
* ``bzip2 (+src)``
* ``openssl-devel``

You can then clone and build TileDB using git / cmake / make:

.. code-block:: console

   $ git clone https://github.com/TileDB-Inc/TileDB
   $ cd TileDB && mkdir build && cd build
   $ cmake -DTILEDB_TBB=OFF ..
   $ make
   $ make check


Build Requirements
------------------

TileDB requires a recent version (3.3 or later) of the
`CMake <https://cmake.org/>`_ build system, and a compiler supporting C++11.
For compression, TileDB relies on the following libraries:

* `zlib <https://zlib.net/>`_
* `LZ4 <http://lz4.github.io/lz4/>`_
* `bzip2 <http://www.bzip.org/>`_
* `Zstandard <http://facebook.github.io/zstd/>`_

When building from source, TileDB will locate these dependencies if already
installed on your system, and locally install (not system-wide) any of them
that are missing.

Optional Dependencies
~~~~~~~~~~~~~~~~~~~~~

**TBB**

Some TileDB internals are parallelized using the
`Intel Threaded Building Blocks <https://www.threadingbuildingblocks.org/>`__
library. The TileDB build system will install this library if it is not
already present on your system. You can disable the TBB dependency when
configuring the TileDB build, in which case TileDB will fall back on serial
implementations of several algorithms. As a part of the TileDB installation
process, the TBB dynamic library will also be installed in the same
destination as the TileDB dynamic library. The TBB headers are not installed
with TileDB.

**S3**

Backend support for S3 stores requires the
`AWS C++ SDK <https://github.com/aws/aws-sdk-cpp>`__. Similarly to the
required dependencies, the TileDB build system will install the SDK locally
if it is not already present on your system (when the S3 build option is
enabled).

TileDB also integrates well with the S3-compliant `minio <https://minio.io>`__
object store.

**HDFS**

Backend support for the Hadoop File System
`HDFS <http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html>`_
is optional. TileDB relies on the C interface to HDFS provided by
`libhdfs <http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/LibHdfs.html>`_
to interact with the distributed filesystem.

During the build process the following environmental variables must be set:

* ``JAVA_HOME``: Path to the location of the Java installation.
* ``HADOOP_HOME``: Path to the location of the HDFS installation.
* ``CLASSPATH``: The Hadoop jars must be added to the ``CLASSPATH`` before interacting with ``libhdfs``.

Consult the `HDFS user guide <https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html>`_
for installing, setting up, and using the distributed Hadoop file system.

.. note::
   HDFS is not currently supported on Windows.

Dependency Installation
~~~~~~~~~~~~~~~~~~~~~~~

If any dependencies are not found pre-installed on your system, the TileDB
build process will download and build them automatically. Preferentially, any
dependencies built by this process will be built as static libraries, which
are statically linked against the TileDB shared library during the build.
This simplifies usage of TileDB, as it results in a single binary object, e.g.
``libtiledb.so`` that contains all of the dependencies. When installing
TileDB, only the TileDB include files and the dynamic object ``libtiledb.so``
will be copied into the installation prefix.

If TileDB is itself built as a static library (using the ``TILEDB_STATIC=ON``
CMake variable or corresponding ``bootstrap`` flag), the dependency static
libraries must be installed alongside the resulting static ``libtiledb.a``
object. This is because static libraries cannot be statically linked together
into a single object (at least not in a portable way). Therefore, when
installing TileDB all static dependency libraries will be copied into the
installation prefix alongside ``libtiledb.a``.

.. note::
   The TBB dependency is also built as a static library by default (except on
   Windows). If you require a dynamically-linked TBB, use the
   ``TILEDB_TBB_SHARED=ON`` CMake variable. Note that the ``libtbb.so`` shared
   library will then be installed alongside ``libtiledb.so`` during installation.
