#include <cassert>
#include <iostream>

int main(int, char**) {
#if defined(_MSC_VER)
  // Do not display message box on abort.
  _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
#endif
  assert(false);

  std::cout << "Assert did not exit!" << std::endl;
  return 0;
}