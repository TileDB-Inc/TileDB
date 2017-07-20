//
// Created by Jake Bolewski on 7/20/17.
//

#ifndef TILEDB_URI_H
#define TILEDB_URI_H

#include <string>
#include "status.h"

namespace tiledb {

namespace uri {

class URI {
 public:
  URI() = default;

  URI(const std::string& path)
      : uri_(path) {
  }

  uri::URI join_path(const std::string& path) const {
    return uri::URI(uri_ + "/" + path);
  };

  std::string to_string() const {
    return uri_;
  }

  size_t size() const {
    return uri_.size();
  }

  std::string to_posix_path() const {
    return uri_;
  }

  const char* c_str() const {
    return uri_.c_str();
  }

 private:
  std::string uri_;
};

}  // namespace uri
}  // namespace tiledb

#endif  // TILEDB_URI_H
