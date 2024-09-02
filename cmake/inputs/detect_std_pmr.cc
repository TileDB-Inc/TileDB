
#include <memory_resource>

int
main() {
  auto resource = std::pmr::get_default_resource();
}
