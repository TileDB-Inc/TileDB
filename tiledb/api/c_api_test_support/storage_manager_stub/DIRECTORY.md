## Storage Manager Stub

The `class StorageManager` is too large and unwieldy to support unit testing, yet there's member variable of that type in each `Context`. This directory defines a stub class for `StorageManager` that allows testing with `Context` in cases that don't require anything but a stub.

### Override Headers

The name of the header may not change; they're fixed in other headers. The file `storage_manager_override` declares the class `StorageManagerStub` and puts that declaration into `storage_manager.h`. The file `storage_manager_declaration_override.h` specializes a template that has the effect of assigning the alias `StorageManager` to StorageManagerStub`.

### CMake support

In order to activate the override, the directory path must be modified to reach this directory so that the override header will be found on the include path. The file `storage_manager_override.cmake` defines an object library that sets up the include path.

### Stub Definition

The file `storage_manager_stub.cc` is empty, but is needed to define an object library for the overridden storage manager.
