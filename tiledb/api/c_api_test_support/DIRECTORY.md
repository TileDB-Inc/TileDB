## C API Test Support

This directory provides support classes for testing the C API at the unit level

### Storage Manager Stub

The `class StorageManager` is too large and unwieldy to support unit testing, yet there's member variable of that type in each `Context`. This directory defines a stub class for `StorageManager` that allows testing with `Context` in cases that don't require anything but a stub.