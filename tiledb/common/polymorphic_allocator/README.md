Polymorphic Allocator Fallback Implementation
===

This implementation of polymorphic_allocator was pulled from Pablo Halpern's
C++11 implementation available here:

https://github.com/phalpern/CppCon2017Code/tree/d26e7f4f6c593fe135c6b454aee93486790726b7

I have a personal forked copy here in case that repository ever dissappears:

https://github.com/davisp/phalpern-CppCon2017Code/tree/d26e7f4f6c593fe135c6b454aee93486790726b7

The only changes from the original files in that repository are to reformat
using TileDB coding style and adding a handful of `std::` namespace qualifiers
to avoid symbol name clashes with internal TileDB symbols.
