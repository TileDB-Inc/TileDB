/*********************************************************************
  Blosc - Blocked Shuffling and Compression Library

  Author: Francesc Alted <francesc@blosc.org>

  See LICENSES/BLOSC.txt for details about copyright and rights to use.
**********************************************************************/
#ifndef BLOSC_EXPORT_H
#define BLOSC_EXPORT_H

/* Macros for specifying exported symbols.
   BLOSC_EXPORT is used to decorate symbols that should be
   exported by the blosc shared library.
   BLOSC_NO_EXPORT is used to decorate symbols that should NOT
   be exported by the blosc shared library.
*/
#if defined(BLOSC_SHARED_LIBRARY)
  #if defined(_MSC_VER)
    #define BLOSC_EXPORT __declspec(dllexport)
  #elif (defined(__GNUC__) && __GNUC__ >= 4) || defined(__clang__)
    #if defined(_WIN32) || defined(__CYGWIN__) || defined(__MINGW32__)
      #define BLOSC_EXPORT __attribute__((dllexport))
    #else
      #define BLOSC_EXPORT __attribute__((visibility("default")))
    #endif  /* defined(_WIN32) || defined(__CYGWIN__) */
  #else
    #error Cannot determine how to define BLOSC_EXPORT for this compiler.
  #endif
#else
  #define BLOSC_EXPORT
#endif  /* defined(BLOSC_SHARED_LIBRARY) */

#if defined(__GNUC__) || defined(__clang__)
  #define BLOSC_NO_EXPORT __attribute__((visibility("hidden")))
#else
  #define BLOSC_NO_EXPORT
#endif  /* defined(__GNUC__) || defined(__clang__) */

/* When testing, export everything to make it easier to implement tests. */
#if defined(BLOSC_TESTING)
  #undef BLOSC_NO_EXPORT
  #define BLOSC_NO_EXPORT BLOSC_EXPORT
#endif  /* defined(BLOSC_TESTING) */

#endif  /* BLOSC_EXPORT_H */
