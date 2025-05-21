#include <cassert>
#include <iostream>

#include "tiledb/common/assert.h"

int main(int, char**) {
#if defined(_MSC_VER)
  // We disable the following events on abort:
  // _WRITE_ABORT_MSG: Display message box with Abort, Retry, Ignore
  // _CALL_REPORTFAULT: Send an error report to Microsoft
  // The second parameter specifies which flags to change, and the first
  // the value of these flags.
  _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
#endif
  passert(false);

  std::cout << "Assert did not exit!" << std::endl;
  return 0;
}
