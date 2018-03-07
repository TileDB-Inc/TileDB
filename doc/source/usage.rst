Usage
=====

If TileDB is not installed globally, or you would like to specify a custom version of tiledb
to build and link against you need to specify the correct paths for including ``tiledb.h``
and linking the ``libtiledb`` shared library.

macOS or Linux
--------------

By default the make install script without specifying an install prefix, ``../bootstrap --prefix=PREFIX``,
installs to the dist folder in the TileDB repo: ``<path/to/TileDB>/dist``.

To include the ``tiledb.h`` header file and link the TileDB dynamically shared library
``libtiledb.(so, dylib)`` in your C / C++ project::

    gcc -x c++ example.cpp \
        -I</path/to/TileDB>/dist/include \
        -L</path/to/TileDB>/dist/lib \
        -ltiledb -o example

If dynamically linking ``libtiledb`` from a non-standard location,
you must make the linker aware of where the shared library resides.

On Linux, you can modify the ``LD_LIBRARY_PATH`` variable at runtime::

    env LD_LIBRARY_PATH="<path/to/TileDB>/dist/lib:$LD_LIBRARY_PATH" ./example

For macOS the linker variable is ``DYLD_LIBARAY_PATH``.

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

You should now be able to ``#include <tiledb/tiledb.h>`` in your project.

Note that at runtime, the directory containing the DLLs must be in your PATH environment variable,
or you will see error messages at startup that the TileDB library or its dependencies could not be located.
You can do this in Visual Studio by adding ``PATH=C:\path\to\TileDB\bin`` to the "Environment" setting under
"Debugging" in the Property Pages. You can also do this from the Windows Control Panel, or at the cmd.exe prompt like so::

    > set PATH=%PATH%;C:\path\to\TileDB\bin
    > my_program.exe
