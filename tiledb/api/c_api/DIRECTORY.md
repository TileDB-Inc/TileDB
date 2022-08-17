## C API

The C API code is in sections, roughly one for each type exposed through the API.
Each section has its own directory, containing two headers, at least one API source file, and unit tests.

* **External header**. This header is included in the top-level `tiledb.h` header. These headers will be included in both C and C++ programs and must compile appropriately.
  * Declares C API functions.
  * Only included in `tiledb.h`. Should not appear elsewhere, including in other external headers.
  * Has `extern "C"` linkage.
  * Uses macros such as `TILEDB_EXPORT` and `NOEXCEPT` to allow for differences between languages and platforms.
  * Uses name convention `<section>_api_external.h`
* **Internal header**. This header is not user-visible and is for inclusion in API source files.
  * Only included in API sources and API white-box unit tests. Since these headers are not user-visible, they do not appear in integration tests; such tests should only use external headers.
  * Included source files in other sections as the arguments of C API function dictate.
  * Uses name convention `<section>_api_internal.h`
* **Enumeration header**. This header is user-visible by inclusion through the external header, but should never be included directly. This header contains code fragments that define values for enumeration symbols; the fragments require a particular pattern of use for the preprocessor.
  * Included in the external header, where it defines a plain `enum` for C.
  * Included in the internal header, where it defines an `enum class` for C++
  * Uses name convention `<section>_api_enum.h`
* **API source**
  * Defines C API functions with an exception wrapper around a matching implementation function.
  * Defines implementations functions, one for each C API function. These implementation functions do not have separate header declarations.
  * Uses name convention `<section>_api.cc`
