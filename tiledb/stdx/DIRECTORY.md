## stdx

This directory contains local implementations of C++ standard library components that have been introduced in recent versions of the standard, but that are not included in the standard library shipping with one or more of the compilers supported by libtiledb.

### `ranges`

There are two views standardized as part of C++23 that we provide a subset for: `zip_view` and `chunk_view`.  The file `tiledb/stdx/ranges` is an include file (mimicking "`ranges`" in the standard library) that provides these two views.  The implementations of these views are included in the `__ranges` subdirectory.  (This naming convention mirrors that of LLVM.)  The include directives for the implementations are surrounded by the appropriate `version` preprocessor directives so that once `zip_view` and/or `chunk_view` are available for a given compiler, the standard implementation will be used instead of our local implementation.

### `thread` and `stop_token`

These are copies of Josuttis's model implementation.