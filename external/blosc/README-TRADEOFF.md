## CAUTION: Trade-off ahead

`blosc` is a C library, not C++.
It requires a single, dynamic initializer for the `host_implementation`
  structure in `shuffle.c`.
It uses the `pthread` library function `pthread_once` to accomplish this.
C++ has built-in static initialization that accomplished the same thing.
Windows, however, does not have standard `pthread` support;
  it is, however, available as an external package.

The trade-off requires either (1) or (2), neither of which is good.
Both alternatives require build alterations.

1. Modify the blosc files to make it possible to avoid pthread entirely.
   The costs are (a) a requirement to keep modifying upstream sources at each
     upgrade, (b) an ancillary source file to perform the initialization, and (c)
     an ancillary source file that defines stubs for otherwise-unused functions.
2. Add an external dependency, a Windows `pthread` implementation.
   The costs are (a) selecting a project to rely upon, (b) getting a build
     working with it and (c) keeping it updated.

The trade-off is decided here in favor of (1).
The source modifications were able to be kept small.
The ancillary sources are simple.
The build modifications, though not tiny, are certainly much smaller than
  adding another external project would be.

### Differences from 1.21.0 release

The major difference is that only some files are used.
It's the minimal set need to get `shuffle.c` to compile.

#### Modifications to `shuffle.c`

Modification of blosc sources are isolated to a single file, `shuffle.c`.
All the changes were made with conditional compilation.
The variable `INITIALIZE_WITHOUT_PTHREAD` triggers the changes;
if left undefined, `shuffle.c` is token-identical to its original.
Furthermore, with one exception, the `#ifdef` blocks around code
  do not change the text on a line, allowing easier future patching.
The exception is the removal of a `static` declaration.

For reference in regard to future upgrades, here are the points of modification:
* Disable the inclusion of pthread headers.
* Remove the `pthread_once_t` variable. 
* Remove `static` from the definition of `set_host_implementation()`.
  This function performs the initialization and must be referenced externally.
* Replace the function `init_shuffle_implementation()` with a null function.
  This function is used dynamically to ensure initialization before use.

#### Initialization in `initialize-shuffle.cc`

The new initialization file is short.
It consists of a single declaration of `set_host_implementation()`
  and an initializer that calls it.

#### Stub functions in `bitshuffle.c`

The initialized structure requires pointers to bit-shuffle functions.
We are using a different implementation, so these are redefined as stubs.