#ifndef TILEDB_EXAMPLES_H
#define TILEDB_EXAMPLES_H

#include <stdio.h>
#include <tiledb/tiledb.h>

/**
 * Attempt to retrieve an error from the tiledb context
 * and print to stderr if present.
 *
 * @param line the line number of the last API call
 * @param ctx the context pointer
 *
 * @return 0 if no error was found, 1 if one was.
 */
static int try_print_error(int line, tiledb_ctx_t* ctx) {
  // Retrieve the last error that occurred
  tiledb_error_t* err = NULL;
  tiledb_ctx_get_last_error(ctx, &err);

  if (err == NULL) {
    return 0;
  }

  const char* msg;
  tiledb_error_message(err, &msg);
  fprintf(stderr, "%d: %s\n", line, msg);

  // Clean up
  tiledb_error_free(&err);

  return 1;
}

/**
 * Attempt to retrieve an error from the tiledb context.
 * If present, print to stderr and exit.
 */
#define IF_ERROR_EXIT(ctx)                               \
  do {                                                   \
    if (try_print_error(__LINE__, (ctx)) != TILEDB_OK) { \
      abort();                                           \
    }                                                    \
  } while (0)

/**
 * Run a tiledb API and then check for errors, exiting if one is found.
 */
#define TRY(ctx, capi_call)                  \
  do {                                       \
    const capi_return_t __ret = (capi_call); \
    if (__ret != TILEDB_OK) {                \
      IF_ERROR_EXIT(ctx);                    \
    }                                        \
  } while (0)

#endif
