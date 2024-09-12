#ifndef TILEDB_EXAMPLES_H
#define TILEDB_EXAMPLES_H

#include <stdio.h>
#include <tiledb/tiledb.h>

static int try_print_error(int line, tiledb_ctx_t* ctx) {
  // Retrieve the last error that occurred
  tiledb_error_t* err = NULL;
  tiledb_ctx_get_last_error(ctx, &err);

  if (err == NULL) {
    return TILEDB_OK;
  }

  const char* msg;
  tiledb_error_message(err, &msg);
  fprintf(stderr, "%d: %s\n", line, msg);

  // Clean up
  tiledb_error_free(&err);

  return TILEDB_ERR;
}

#define IF_ERROR_EXIT(ctx)                               \
  do {                                                   \
    if (try_print_error(__LINE__, (ctx)) != TILEDB_OK) { \
      exit(TILEDB_ERR);                                  \
    }                                                    \
  } while (0)

#define TRY(ctx, capi_call)                  \
  do {                                       \
    const capi_return_t __ret = (capi_call); \
    if (__ret != TILEDB_OK) {                \
      IF_ERROR_EXIT(ctx);                    \
    }                                        \
  } while (0)

#endif

