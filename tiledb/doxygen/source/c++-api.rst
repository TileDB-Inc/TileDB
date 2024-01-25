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

Dimension
---------
.. doxygenclass:: tiledb::Dimension
    :project: TileDB-C++
    :members:
    
Domain
------
.. doxygenclass:: tiledb::Domain
    :project: TileDB-C++
    :members:
    
Attribute
---------
.. doxygenclass:: tiledb::Attribute
    :project: TileDB-C++
    :members:
  
Array Schema
------------
.. doxygenclass:: tiledb::ArraySchema
    :project: TileDB-C++
    :members:
    
Array
-----
.. doxygenclass:: tiledb::Array
    :project: TileDB-C++
    :members:
    
Query
-----
.. doxygenclass:: tiledb::Query
    :project: TileDB-C++
    :members:

QueryCondition
--------------
.. doxygenclass:: tiledb::QueryCondition
    :project: TileDB-C++
    :members:

Subarray
--------
.. doxygenclass:: tiledb::Subarray
   :project: TileDB-C++
   :members:

Filter
------
.. doxygenclass:: tiledb::Filter
    :project: TileDB-C++
    :members:

Filter List
-----------
.. doxygenclass:: tiledb::FilterList
    :project: TileDB-C++
    :members:

Group
-----
.. doxygenfunction:: tiledb::create_group
    :project: TileDB-C++

Object Management
-----------------
.. doxygenclass:: tiledb::Object
    :project: TileDB-C++
    :members:
.. doxygenclass:: tiledb::ObjectIter
    :project: TileDB-C++
    :members:

VFS
---
.. doxygenclass:: tiledb::VFS
    :project: TileDB-C++
    :members:

Utils
-----
.. doxygenfile:: tiledb/sm/cpp_api/utils.h
    :project: TileDB-C++

Version
-------
.. doxygenfunction:: tiledb::version
    :project: TileDB-C++

Stats
-----
.. doxygenclass:: tiledb::Stats
    :project: TileDB-C++
    :members:
    
FragmentInfo
------------
.. doxygenclass:: tiledb::FragmentInfo
    :project: TileDB-C++
    :members:

Experimental
------------
.. autodoxygenfile:: tiledb_experimental
    :project: TileDB-C++

.. doxygenclass:: tiledb::ArraySchemaEvolution
    :project: TileDB-C++
    :members:

.. doxygenclass:: tiledb::Group
    :project: TileDB-C++
    :members:
