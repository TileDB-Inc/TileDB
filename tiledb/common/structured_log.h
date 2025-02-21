#include <fstream>
#include <rfl.hpp>
#include <rfl/json.hpp>
#include <string>

namespace tiledb::common {

template <typename T>
class StructuredLog {
 public:
  StructuredLog(std::string_view path)
      : out_(std::string(path)) {
  }

  ~StructuredLog() {
    flush();
  }

  void record(const T& value) {
    recorded_.push_back(value);

    if (recorded_.size() >= 1024 * 1024) {
      flush();
    }
  }

  void flush() {
    for (const auto& record : recorded_) {
      out_ << rfl::json::write(record) << std::endl;
    }
    recorded_.clear();
  }

 private:
  std::vector<T> recorded_;
  std::ofstream out_;
};

}  // namespace tiledb::common
