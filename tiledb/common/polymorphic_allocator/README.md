Pablo Halpern's Polymorphic Allocator
===

* Original repository: https://github.com/phalpern/CppCon2017Code
* Fork for just in case: https://github.com/davisp/phalpern-CppCon2017Code

This is an import of the polymorphic allocator written by Pablo Halpern who
is the main person behind the std::pmr C++17 feature. This code is written
using only C++11 features and can be used by implementations that don't yet
support std::pmr natively. Of note, std::pmr is only supported by XCode 15 on
macOS 14 (Sonoma).

For now, I'm going to default to using this implementation. We'll need to
figure out if the best way forward is to rely solely on this implementation
or make sure that we're using a native standard library version on platforms
that support it. One sticking point here is that XCode 15 on macOS 13 will
compile code with std::pmr resources but then fail at run time with dynamic
linker errors.

The notable missing parts of this implementation are that this only contains
the basic `new_delete_resource` memory resource class. The others like
`monotonic_buffer_resource` and the pool resource classes are unimplemented.

Notes on the Import
---

This import is a slightly modified version of the original code in the
repositories linked above. The three main differences:

1. The code has been reformatted with our clang-format rules.
2. There was a need to add `std::` namespace to avoid conflicting with symbols
   defined under the `tiledb::` namespace.
3. Original includes used `<>` for system lookup. These now use `""` for
   relative paths.
