# Directory for C API hook testing

A user activates the C API hook by writing an override header and building the library with its directory as part of the include path. This is a compile-time mechanism, so in order to test it well, the same code should execute both with and without a test hook compiled in.

This directory proivdes a place to put an override header that's not ordinarily included in the build.   