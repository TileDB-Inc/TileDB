.. _usage:

Usage
=====

This page contains instructions on how to build and link a program against TileDB.

macOS or Linux
--------------

To use TileDB in a program, just ``#include <tiledb/tiledb.h>`` (C API) or
``#include <tiledb/tiledb>`` (C++ API) and specify ``-ltiledb`` when
compiling, e.g. ::

    gcc example.c -o example -ltiledb

To use the C++ API, add ``#include <tiledb/tiledb>`` to your C++ project. The
TileDB C++ API requires a compiler with C++11 support, so your project must
be compiled using the C++11 standard, e.g.::

    g++ -std=c++11 example.cpp -o example -ltiledb

If TileDB was installed in a non-default location on your system, use the ``-I``
and ``-L`` options::

    gcc example.c -o example -I<path/to/TileDB>/include -L<path/to/TileDB>/lib -ltiledb

At runtime, if TileDB is installed in a non-default location, you must
make the linker aware of where the shared library resides by exporting an
environment variable:

.. content-tabs::

   .. tab-container:: macos
      :title: macOS

      .. code-block:: console

         $ export DYLD_LIBRARY_PATH="<path/to/TileDB>/lib:$DYLD_LIBRARY_PATH"
         $ ./example

   .. tab-container:: linux
      :title: Linux

      .. code-block:: console

         $ export LD_LIBRARY_PATH="<path/to/TileDB>/lib:$LD_LIBRARY_PATH"
         $ ./example

You can avoid the use of these environment variables by installing TileDB in
a global (standard) location on your system, or hard-coding the path to the
TileDB library at build time by configuring the ``rpath``, e.g.::

    g++ -std=c++11 example.cpp -o example \
        -I<path/to/TileDB>/include \
        -L<path/to/TileDB>/lib \
        -Wl,-rpath,<path/to/TileDB>/lib \
        -ltiledb

Building your program this way will result in a binary that will run without
having to configure the ``LD_LIBRARY_PATH`` or ``DYLD_LIBRARY_PATH``
environment variables.

.. _windows-usage:

Windows
-------

To use TileDB from a Visual Studio C++ project, we need to add project properties telling the
compiler and linker where to find the headers and libraries.

Open your project's Property Pages. Under the General options for C/C++, edit
the "Additional Include Directories"  property. Add a new entry pointing to
your TileDB installation (either built from source or extracted from the
binary release .zip file), e.g. ``C:\path\to\TileDB\include``.

Under the General options for the Linker, edit the "Additional Library
Directories" property. Add a new entry pointing to your TileDB installation,
e.g. ``C:\path\to\TileDB\lib``. Under the Input options for Linker, edit
"Additional Dependencies" and add ``tiledb.lib``.

You should now be able to ``#include <tiledb/tiledb.h>`` (C API) or
``#include <tiledb/tiledb>`` (C++ API) in your project.

.. note::

   When building your project in Visual Studio, ensure that the ``x64`` build
   configuration is selected. Because TileDB is currently only available as a
   64-bit library, applications that link with TileDB must also be 64-bit.

At runtime, the directory containing the DLLs must be in your PATH
environment variable, or you will see error messages at startup that the
TileDB library or its dependencies could not be located. You can do this in
Visual Studio by adding ``PATH=C:\path\to\TileDB\bin`` to the "Environment"
setting under "Debugging" in the Property Pages. You can also do this from the
Windows Control Panel, or at the command prompt like so:

.. content-tabs::

   .. tab-container:: windowsps
      :title: PS

      .. code-block:: console

         > $env:Path += ";C:\path\to\TileDB\bin"
         > my_program.exe

   .. tab-container:: windowscmd
      :title: cmd.exe

      .. code-block:: console

         > set PATH=%PATH%;C:\path\to\TileDB\bin
         > my_program.exe

CMake
-----

TileDB includes support for CMake's ``find_package()``. To use, TileDB
must be installed globally or ``CMAKE_PREFIX_PATH`` must be set to the TileDB
installation directory.

For example if TileDB was built with ``../bootstrap`` and no prefix was given
then the ``</path/to/TileDB>/dist/lib/cmake/TileDB`` directory will contain the
``TileDBConfig.cmake`` file used for ``find_package(TileDB)``. In your project,
you would set ``CMAKE_PREFIX_PATH`` like so::

    list(APPEND CMAKE_PREFIX_PATH "</path/to/TileDB>/dist")

You can also pass this like any other CMake variable on the command line when
configuring your project, e.g.::

    cmake -DCMAKE_PREFIX_PATH=</path/to/TileDB>/dist ..

To link the executable ``MyExe`` in your project with the TileDB shared library,
you would then use::

    # Find TileDB
    find_package(TileDB REQUIRED)
    # Link to shared library, this will set header include directories also.
    target_link_libraries(MyExe PRIVATE TileDB::tiledb_shared)

While disabled by default, TileDB can also be built as a static library. To do
this, use the ``--enable-static-tiledb`` (macOS/Linux) or ``-EnableStaticTileDB``
(Windows) bootstrap flag when configuring TileDB, or use the CMake equivalent flag
``-DTILEDB_STATIC=ON``. Then in your project simply link against the
``tiledb_static`` target instead::

    # Find TileDB
    find_package(TileDB REQUIRED)
    # Link to static library, this will set header include directories also
    target_link_libraries(MyExe PRIVATE TileDB::tiledb_static)

You can see the
`example CMake project <https://github.com/TileDB-Inc/TileDB/tree/dev/examples/cmake_project>`__
in the TileDB source repository to see an example project structure that links
against TileDB.