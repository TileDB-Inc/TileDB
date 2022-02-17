# CMake

TileDB uses CMake for its configuration, build system, and package management.

## Build Options

_TODO Describe the `TILEDB_*` options and how/when to add new options._

## TileDB Object Libraries

The main TileDB library is in the process of being broken into discrete object libraries. 

_TODO About the object libraries and best practices_

## TileDB Static and Shared Libraries

_TODO About combining the object libraries into the main libraries created by CMake_

## Adding a dependency

There are several steps to adding a new dependency to TileDB. Most of the complexity is setting up a CMake external project for the new dependency. Broadly, the steps are:

1. Add a new file `cmake/Modules/Find<Dep>_EP.cmake` where `<Dep>` is the name of the dependency. It's easiest if you follow the pattern of an existing dependency, such as `FindBlosc_EP.cmake`.
1. `include()` the new CMake file in `cmake/TileDB-Superbuild.cmake` next to the other dependency includes.
1. Add a call to `find_package(<Dep>_EP)` in `tiledb/CMakeLists.txt` next to the other dependency `find_package()` calls.
1. Link to the dependency by adding `<Dep>::<Dep>` to the `target_link_libraries(TILEDB_CORE_OBJECTS_ILIB ...)` command in `tiledb/CMakeLists.txt`.
1. Add a call to `append_dep_lib(<Dep>::<Dep>)` next to the other calls to `append_dep_lib` in `tiledb/CMakeLists.txt`. This is required to support transitive linking against the dependency for static TileDB when using `target_link_libraries(tgt TileDB::tiledb_static)`.
Dependencies built by external projects should be built as static libraries whenever possible.

## Using the common modules

* _TODO  Describe (generally speaking) what is included in the cmake/common*.cmake modules_
* _TODO Guidelines for adding to the modules (including best practices for CMake macros and functions)._


