Usage
=====

If TileDB is not installed globally, or you would like to specify a custom version of
TileDB to build and link against, you will need to specify the correct paths for including
the ``tiledb/tiledb.h`` (C API) or ``tiledb/tiledb`` (C++ API) files and linking to the
``libtiledb`` shared library.

macOS or Linux
--------------

By default, if you do not specify an installation prefix with
``../bootstrap --prefix=PREFIX``, TileDB is installed to the ``dist`` folder in the root
TileDB directory: ``</path/to/TileDB>/dist``.

To build and link to the TileDB C API, add ``#include <tiledb/tiledb.h>`` to your C or C++
project and link to the TileDB shared library ``libtiledb.(so, dylib)``, e.g.::

    gcc -x c++ example.cpp \
        -I</path/to/TileDB>/dist/include \
        -L</path/to/TileDB>/dist/lib \
        -ltiledb -o example

The ``-I`` and ``-L`` options are required if TileDB is not installed globally on your
system.

To use the C++ API, add ``#include <tiledb/tiledb>`` to your C++ project. The TileDB C++
API requires a compiler with C++11 support, so your project must be compiled using the
C++11 standard, e.g.::

    g++ -std=c++11 example.cpp \
        -I</path/to/TileDB>/dist/include \
        -L</path/to/TileDB>/dist/lib \
        -ltiledb -o example

At runtime, if you linked to ``libtiledb`` in a non-standard location, you must make the
linker aware of where the shared library resides.

On Linux, you can modify the ``LD_LIBRARY_PATH`` environment variable at runtime::

    env LD_LIBRARY_PATH="</path/to/TileDB>/dist/lib:$LD_LIBRARY_PATH" ./example

For macOS the linker environment variable is ``DYLD_LIBARAY_PATH``.

You can avoid the use of these environment variables by installing TileDB in a global
(standard) location on your system, or hard-coding the path to the TileDB library at build
time by configuring the ``rpath``, e.g.::

    g++ -std=c++11 example.cpp \
        -I</path/to/TileDB>/dist/include \
        -L</path/to/TileDB>/dist/lib \
        -Wl,-rpath,</path/to/TileDB>/dist/lib \
        -ltiledb -o example

Building your program this way will result in a binary that will run without having to
configure the ``LD_LIBRARY_PATH`` or ``DYLD_LIBRARY_PATH`` environment variables.

.. _windows-usage:

Windows
-------

To use TileDB from a Visual Studio C++ project, we need to add project properties telling the
compiler and linker where to find the headers and libraries.

Open your project's Property Pages. Under the General options for C/C++, edit the
"Additional Include Directories"  property. Add a new entry pointing to your TileDB installation
(either built from source or extracted from the binary release .zip file), e.g. ``C:\path\to\TileDB\include``.

Under the General options for the Linker, edit the "Additional Library Directories" property.
Add a new entry pointing to your TileDB installation, e.g. ``C:\path\to\TileDB\lib``.
Under the Input options for Linker, edit "Additional Dependencies" and add tiledb.lib.

You should now be able to ``#include <tiledb/tiledb.h>`` (C API) or ``#include <tiledb/tiledb>`` (C++ API) in your project.

When building your project, ensure that the ``x64`` build configuration is
selected. Because TileDB is currently only available as a 64-bit library,
applications that link with TileDB must also be 64-bit.

Note that at runtime, the directory containing the DLLs must be in your PATH environment variable,
or you will see error messages at startup that the TileDB library or its dependencies could not be located.
You can do this in Visual Studio by adding ``PATH=C:\path\to\TileDB\bin`` to the "Environment" setting under
"Debugging" in the Property Pages. You can also do this from the Windows Control Panel, or at the cmd.exe prompt like so::

    > set PATH=%PATH%;C:\path\to\TileDB\bin
    > my_program.exe
