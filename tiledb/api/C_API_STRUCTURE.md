# Structure of the C API

## Layers

The C API consists of layers starting with the API functions themselves and ending with classes that call into the non-API parts of the core library. There are five layers, each with its own responsibilities.

### Public API functions

Public API functions themselves are declared with `extern "C"` linkage. They are implemented as wrappers around an API implementation function. There is a one-to-one relationship between public API functions and API implementation functions.

Public API functions are responsible for uniform error processing through handling exceptions. The wrapper provides uniformity of behavior for (1) return values, (2) logging format, (3) error return. Non-uniform error handling may be done outside this layer, although that's not usually necessary.

### API implementation functions

API implementation functions translate C calling sequences into C++ calling sequences. At the foundation is translating between C-style pointers and C++ objects. This is sometimes much more than simple argument translation. This translation may require conversion between the differing lifespans of core objects and API-visible objects that allow access to them. Different classes may be used to represent the same kind of core object, depending on life cycle; API implementation functions are responsible for selecting the correct one.

These functions are the first line for argument validation. They have principal responsibility for the ways that argument may be invalid as C arguments, the most notable of which is null pointers. They have secondary responsibility for validating arguments in other ways; in these cases the underlying classes have the primary responsibility.

These functions have primary responsibility for memory allocation of API-visible objects. The legacy pattern was raw `new` and `delete` with explicit error recovery. The new pattern is to use a handle class. Once the conversion to handles is complete, it won't be necessary to state that memory is a responsibility here.

### Handle classes

Handle classes manage memory allocation for API-visible objects, deriving from a generic `class api:handle` base class. Handle classes hide the details of memory allocation from API implementation functions

Handles are carriers for facade instances. There's a one-to-one relationship between a handle class and a facade base class (see below). Handle classes forward all functions used by implementation functions to the facade.

There's a one-to-one relationship between handle objects and facade objects. Handles are constructed carrying facades, and facades are not created unless through a handle.

An API implementation function may not naively assume that an argument pointer to a C API function is a valid pointer to a handle; such an assumption constitutes a security risk. Associated with each handle type is a warehouse function (parallel to a factory function) that converts an API argument pointer into a facade pointer. At root this is simply getting the pointer value, but the warehouse function may refuse a request for an unknown object.

**[Future]** Handle classes are the nexus for API object registration, a facility that keeps track of all outstanding objects obtained through the C API.

### Facade classes

Facade classes present a uniform interface to core objects regardless of the life cycle of the object. In particular, there's a difference between newly-created objects, such as those created with `*_alloc` functions, and pre-existing objects, such as those retrieved from aggregates like an array schema. Thus facade classes generally derive from an abstract base class and implement them in accordance with life cycle concerns.

The motivation for this structure is to be able to interconvert between C.41-compliant core objects and a C.41-hostile set C API functions. Depending on
lifecycle state, some operations may be prohibited. For example, array schema data is considered immutable once it's been created, so `set_*` C API functions must fail on objects that have been (C.41 fully-) constructed and must succeed on "pre-construction" objects (those that have been constructed in the C API but not yet constructed in core).

For each facade base class, there's an optional proxy class, that is, a `{0,1}` to `1` relationship. If there's an `*_alloc` function in the C API, there will be a proxy class, but otherwise there need not be one.

### Proxy classes

A proxy class represent a C API object that is newly created by collecting constructor arguments through API `set_` calls.

## Implementation restrictions

* In order to facilitate correct SWIG wrapper generation, non-TileDB headers included by the external header files *must* be wrapped as follows:
  ```
  #ifndef TILEDB_CAPI_WRAPPING
  #include <stdint.h>
  #endif
  ```
