## C API

The C API code is in sections, roughly one for each type exposed through the API.
Each section has its own directory, containing two headers, at least one API source file, and unit tests.
 
* **External header**. This header is included in the top-level `tiledb.h` header. These headers will be included in both C and C++ programs and must compile appropriately.
  * Declares C API functions.
  * Only included in `tiledb.h`. Should not appear elsewhere, including in other external headers.
  * Has `extern "C"` linkage.
  * Uses macros such as `TILEDB_EXPORT` and `NOEXCEPT` to allow for differences between languages and platforms.
* **Internal header**. This header is not user-visible and is for inclusion in API source files.
  * Only included in API sources and API white-box unit tests. Since these headers are not user-visible, they do not appear in integration tests; such test should only use external headers.
  * Included source files in other sections as the arguments of C API function dictate. 
* **API source**
    * Defines C API functions with an exception wrapper around a matching implementation function.
    * Defines implementations functions, one for each C API function. These implementation functions do not have separate header declarations.
