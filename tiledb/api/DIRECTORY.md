## API

This directory is the (future) home of the C API and the C++ API. The API's are the leaves of the dependency tree insofar as code organization is concerned. API code may reference anything in the code base, but nothing here should be referenced by anything but user code.

Also included here are API support functions and API-specific tests. The scope of tests are those that exercise the API narrowly, that is, unit tests of the API code itself insofar as it's possible to write them. They should be more than unit tests of the major classes they use but less than full integration tests of the library.

Functions outside of the API itself should be defined in `namespace tiledb::api`.

### C API

The C API has started to move to this directory and is currently in transition.

This directory is for declaration and definition of C API functions, but is not for any common infrastructure they share.  

### C API Support

This directory contains support code shared generally between implementations of C API functions.

### C++ API

The C++ API has not yet moved to this directory.