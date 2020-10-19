/**
 * @file   mem_filesystem.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file implements the in-memory filesystem class
 */

#include <sstream>
#include <unordered_set>

#include "tiledb/common/logger.h"
#include "tiledb/sm/filesystem/mem_filesystem.h"
#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class MemFilesystem::FSNode {
 public:
  FSNode(const std::string& name)
      : name_(name){};

  virtual ~FSNode() = default;

  std::string get_name() const {
    return name_;
  }

  virtual bool is_dir() const = 0;
  virtual bool is_file() const = 0;
  virtual std::vector<std::string> ls(const std::string& dir) const = 0;
  virtual bool has_child(const std::string& child) const = 0;

  // check if map instead
  std::unordered_map<std::string, FSNode*> children_;

 protected:
  std::string name_;
};

class MemFilesystem::File : public MemFilesystem::FSNode {
 public:
  File(const std::string& name)
      : FSNode(name){};

  ~File() {
    free(data_);
  };

  bool is_dir() const override {
    return false;
  }

  bool is_file() const override {
    return true;
  }

  std::vector<std::string> ls(const std::string& dir) const override {
    return {dir + name_};
  }

  bool has_child(const std::string&) const override {
    return false;
  }

  uint64_t get_size() const {
    return size_;
  }

  // TODO: size is in number of bytes
  Status append(const void* data, const uint64_t nbytes) {
    if ((data == nullptr) || nbytes == 0) {
      return LOG_STATUS(Status::MemFSError(std::string(
          "Wrong input buffer or size when writing to file: " + name_)));
    }

    /* if the file was empty */
    if (data_ == nullptr) {
      data_ = malloc(nbytes);
      if (data_ == nullptr) {
        return LOG_STATUS(Status::MemFSError(
            std::string("Out of memory, cannot write to file: " + name_)));
      }

      size_ = nbytes;
    } else {
      /* if the file already had some data */
      void* new_data;
      new_data = realloc(data_, size_ + nbytes);

      if (new_data == nullptr) {
        return LOG_STATUS(Status::MemFSError(std::string(
            "Out of memory, cannot append new data to file: " + name_ +
            "with size " + "size_")));
      }

      std::memcpy((char*)new_data + size_, data, nbytes);
      data_ = new_data;
      size_ += nbytes;
    }

    return Status::Ok();
  }

  Status read(
      const uint64_t offset, void* buffer, const uint64_t nbytes) const {
    assert(buffer);
    if (offset + nbytes > size_)
      return LOG_STATUS(
          Status::MemFSError("Cannot read from file; Read exceeds file size"));

    std::memcpy(buffer, (char*)data_ + offset, nbytes);
    return Status::Ok();
  }

 private:
  void* data_;
  uint64_t size_;
};

class MemFilesystem::Directory : public MemFilesystem::FSNode {
 public:
  Directory(const std::string& name)
      : FSNode(name){};

  // TODO: add body
  ~Directory(){};

  bool is_dir() const override {
    return true;
  }

  bool is_file() const override {
    return false;
  }

  std::vector<std::string> ls(const std::string& dir) const override {
    std::vector<std::string> names;
    for (const auto& child : children_) {
      names.emplace_back(dir + child.first);
    }
    /* TODO: check if needed
    sort(result.begin(), result.end());
    */
    return names;
  }

  bool has_child(const std::string& child) const override {
    return (children_.count(child) != 0);
  }
};

/** Constructor. */
MemFilesystem::MemFilesystem()
    : root_(new Directory("")){};

/** Destructor. */
MemFilesystem::~MemFilesystem() {
  delete_rec(root_);
};

Status MemFilesystem::ls(
    const std::string& path, std::vector<std::string>* paths) const {
  assert(paths);
  if (root_ == nullptr) {
    return LOG_STATUS(Status::MemFSError(std::string(
        "ls not possible, in-memory filesystem is not initialized")));
  }

  std::vector<std::string> tokens = tokenize(path);
  FSNode* cur = root_;
  std::string dir;
  for (auto& token : tokens) {
    dir = dir + "/" + token;
    cur = cur->children_[token];
  }

  *paths = cur->ls(dir);
  return Status::Ok();
}

Status MemFilesystem::create_dir(const std::string& path) {
  if (root_ == nullptr) {
    return LOG_STATUS(Status::MemFSError(
        std::string("Directory creation not possible, in-memory filesystem is "
                    "not initialized")));
  }

  std::vector<std::string> tokens = tokenize(path);
  FSNode* cur = root_;
  for (auto& token : tokens) {
    if (!cur->has_child(token)) {
      cur->children_[token] = new Directory(token);
    }
    cur = cur->children_[token];
  }

  return Status::Ok();
}

Status MemFilesystem::touch(const std::string& path) {
  FSNode* node;
  return touch(path, &node);
}

Status MemFilesystem::touch(const std::string& path, FSNode** new_node) {
  // TODO: check if path is well formatted
  assert(new_node);
  if (root_ == nullptr) {
    root_ = new Directory("");
  }

  std::vector<std::string> tokens = tokenize(path);
  FSNode* cur = root_;
  for (unsigned long i = 0; i < tokens.size() - 1; i++) {
    auto token = tokens[i];
    if (!cur->has_child(token)) {
      cur->children_[token] = new Directory(token);
    }
    cur = cur->children_[token];
  }

  std::string filename = tokens[tokens.size() - 1];
  cur->children_[filename] = new File(filename);
  *new_node = cur->children_[filename];

  return Status::Ok();
}

Status MemFilesystem::write(
    const std::string& path, const void* data, const uint64_t nbytes) {
  FSNode* node;
  RETURN_NOT_OK(get_node(path, &node));
  /* If the file doesn't exist, create it */
  if (node == nullptr) {
    touch(path, &node);
    /* If it's a directory, ignore */
  } else if (node->is_dir()) {
    return LOG_STATUS(Status::MemFSError(std::string(
        "Failed to write on directory, write is only possible on files.")));
  }

  return ((File*)node)->append(data, nbytes);
}

Status MemFilesystem::read(
    const std::string& path,
    const uint64_t offset,
    void* buffer,
    const uint64_t nbytes) const {
  FSNode* node;
  RETURN_NOT_OK(get_node(path, &node));
  if ((node == nullptr) || node->is_dir()) {
    return LOG_STATUS(Status::MemFSError(
        std::string("File not found, read failed for : " + path)));
  }

  return ((File*)node)->read(offset, buffer, nbytes);
}

std::vector<std::string> MemFilesystem::tokenize(
    const std::string& path, const char delim) {
  std::vector<std::string> tokens;
  std::stringstream ss(path);
  std::string token;
  while (std::getline(ss, token, delim)) {
    if (!token.empty()) {
      tokens.emplace_back(token);
    }
  }

  return tokens;
}

Status MemFilesystem::get_node(const std::string& path, FSNode** node) const {
  assert(node);
  if (root_ == nullptr) {
    return LOG_STATUS(Status::MemFSError(
        std::string("Directory creation not possible, in-memory filesystem not "
                    "initialized")));
  }

  std::vector<std::string> tokens = tokenize(path);
  auto cur = root_;
  for (const auto& token : tokens) {
    if (cur->has_child(token)) {
      cur = cur->children_[token];
    } else {
      *node = nullptr;
      return Status::Ok();
    }
  }

  *node = cur;
  return Status::Ok();
}

void MemFilesystem::delete_rec(FSNode* node) {
  for (auto child : node->children_) {
    delete_rec(child.second);
  }
  delete node;
}

Status MemFilesystem::remove(const std::string& path) {
  FSNode* node;
  RETURN_NOT_OK(get_node(path, &node));
  if (node == nullptr) {
    return LOG_STATUS(Status::MemFSError(
        std::string("File not found, remove failed for : " + path)));
  }

  std::vector<std::string> tokens = tokenize(path);
  auto cur = root_;
  FSNode* parent = nullptr;
  for (const auto& token : tokens) {
    parent = cur;
    cur = cur->children_[token];
  }

  delete_rec(cur);
  if (parent) {
    parent->children_.erase(tokens.back());
  }
  return Status::Ok();
};

}  // namespace sm
}  // namespace tiledb