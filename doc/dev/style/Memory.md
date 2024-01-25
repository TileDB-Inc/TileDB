---
title: Memory Guidelines
---

## Pointers and Allocations

The use of `new` or `malloc` is forbidden unless justifcation and a team
agreement on an exception is granted. Inplace a smart pointer should be used.

That is a `unique_ptr`, `shared_ptr` or `weak_ptr` should be used in all cases.
With `weak_ptr` being rarely used.

In the core TileDB library we have our own `tdb_unique_ptr` and `tdb_shared_ptr`
that must be used inplace of the `stl` versions.

`make_unique_ptr` or `make_shared_ptr` is the preferred, exception safe, method
of creating the smart pointers.

Extreme care must be given to all allocations. Even when using STL containers
like `std::vector` thoughtful consideration must be given for the dynamic
allocations that will occur.

If we are only allocating one megabyte or less _strongly_ consider fixed
allocations instead. Fixed allocation help to avoid fragmentation of the heap.
