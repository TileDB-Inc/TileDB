TileDB C++ API Reference
========================

.. highlight:: c++

Context
-------
.. doxygenclass:: tiledb::Context
    :project: TileDB-C++
    :members:

Config
------
.. doxygenclass:: tiledb::Config
    :project: TileDB-C++
    :members:

Exceptions
----------
.. doxygenstruct:: tiledb::TileDBError
    :project: TileDB-C++
.. doxygenstruct:: tiledb::TypeError
    :project: TileDB-C++
.. doxygenstruct:: tiledb::SchemaMismatch
    :project: TileDB-C++
.. doxygenstruct:: tiledb::AttributeError
    :project: TileDB-C++

Array
-----
.. doxygenclass:: tiledb::Array
    :project: TileDB-C++
    :members:

Array Schema
------------
.. doxygenclass:: tiledb::ArraySchema
    :project: TileDB-C++
    :members:

Attribute
---------
.. doxygenclass:: tiledb::Attribute
    :project: TileDB-C++
    :members:

Domain
------
.. doxygenclass:: tiledb::Domain
    :project: TileDB-C++
    :members:

Dimension
---------
.. doxygenclass:: tiledb::Dimension
    :project: TileDB-C++
    :members:

Query
-----
.. doxygenclass:: tiledb::Query
    :project: TileDB-C++
    :members:

Compressor
----------
.. doxygenclass:: tiledb::Compressor
    :project: TileDB-C++
    :members:

Group
-----
.. doxygenfunction:: tiledb::create_group
    :project: TileDB-C++

Map
---
.. doxygenclass:: tiledb::Map
    :project: TileDB-C++
    :members:

Map Schema
----------
.. doxygenclass:: tiledb::MapSchema
    :project: TileDB-C++
    :members:

Map Item
--------
.. doxygenclass:: tiledb::MapItem
    :project: TileDB-C++
    :members:

Object Management
-----------------
.. doxygenclass:: tiledb::Object
    :project: TileDB-C++
    :members:

VFS
---
.. doxygenclass:: tiledb::VFS
    :project: TileDB-C++
    :members:

Utils
-----
.. doxygenfunction:: tiledb::group_by_cell(const std::pair<std::vector<uint64_t>, std::vector<T>>&, uint64_t, uint64_t)
    :project: TileDB-C++
.. doxygenfunction:: tiledb::group_by_cell(const std::vector<T>&, uint64_t)
    :project: TileDB-C++
.. doxygenfunction:: tiledb::group_by_cell(const std::vector<T>&, uint64_t, uint64_t)
    :project: TileDB-C++
.. doxygenfunction:: tiledb::group_by_cell(const std::vector<uint64_t>&, const std::vector<T>&)
    :project: TileDB-C++
.. doxygenfunction:: tiledb::group_by_cell(const std::vector<uint64_t>&, const std::vector<T>&, uint64_t, uint64_t)
    :project: TileDB-C++
.. doxygenfunction:: tiledb::group_by_cell(const std::vector<T>&)
    :project: TileDB-C++
.. doxygenfunction:: tiledb::ungroup_var_buffer
    :project: TileDB-C++
.. doxygenfunction:: tiledb::flatten
    :project: TileDB-C++

Version
-------
.. doxygenclass:: tiledb::Version
    :project: TileDB-C++
    :members:
