Language Bindings
=================

TileDB exposes functionality and interfaces with high-level languages
through a highly portable C API. The C API was designed to make
interfacing with high-level languages as easy as possible without
sacrificing performance. To achieve high performance, TileDB is an
**embedded storage manager**, in that it is a shared library that is
wrapped by the high-level language binding. Resources and memory are
passed directly as raw pointers. All buffer allocation/management is
delegated to the high-level language, making interaction with languages
that have garbage collection much easier. In delegating allocation to
the caller, TileDB also avoids expensive intermediate copies or
deserialization costs.

In terms of traditional databases, the philosophy and design of TileDB
is more akin to SQLite than say Postgres. Traditional access to data
management systems (e.g., via ODBC connectors) often require interaction
through a socket protocol that implies data serialization /
deserialization. This approach is untenable for massive arrays where
serialization costs can quickly dominate and multiple copies of the data
have to be held in memory at once. Additionally, since TileDB is simply
a C library, it avoids the complexities and overheads associated with
interfacing with many “big data” solutions that are built on the Java
virtual machine (JVM).

C++
---

TileDB offers a modern C++ wrapper for the C API. Both the C and C++
APIs are part of the ``libtiledb`` shared library, and both can be
wrapped by other high-level languages.

Python
------

The TileDB Python interface wraps the C API and is tightly coupled with
**NumPy**, in that reads/writes from/to TileDB arrays are performed
using numpy arrays. Copies are avoided in many cases by allocating numpy
arrays directly (based on the array attribute ``dtype``) and having
TileDB write from / read into the underlying numpy data buffer. For
sparse arrays and variable-sized attributes (where the result size is
unknown), copies are minimized but sometimes may be unavoidable (TileDB
may overestimate buffer sizes prior to read queries and then reallocate
to them to their appropriate result size).
