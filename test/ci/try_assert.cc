#include <cassert>
#include <iostream>

int main(int, char**) {
  assert(false);

  std::cout << "Assert did not exit!" << std::endl;
  return 0;
}