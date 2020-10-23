/**
 * @file   mem_filesystem.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 TileDB, Inc.
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
using namespace std;

namespace tiledb {
namespace sm {

class MemFilesystem::FSNode {
 public:
  FSNode(const string& name)
      : name_(name){};

  virtual ~FSNode() = default;

  string get_name() const {
    return name_;
  }

  virtual bool is_dir() const = 0;
  virtual bool is_file() const = 0;
  virtual vector<string> ls(const string& full_path) const = 0;
  virtual bool has_child(const string& child) const = 0;

  // check if map instead
  unordered_map<string, unique_ptr<FSNode>> children_;

 protected:
  string name_;
};

class MemFilesystem::File : public MemFilesystem::FSNode {
 public:
  File(const string& name)
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

  vector<string> ls(const string& full_path) const override {
    return {full_path + name_};
  }

  bool has_child(const string&) const override {
    return false;
  }

  uint64_t get_size() const {
    return size_;
  }

  Status append(const void* data, const uint64_t nbytes) {
    if ((data == nullptr) || nbytes == 0) {
      return LOG_STATUS(Status::MemFSError(
          string("Wrong input buffer or size when writing to file: " + name_)));
    }

    /* if the file was empty */
    if (data_ == nullptr) {
      data_ = malloc(nbytes);
      if (data_ == nullptr) {
        return LOG_STATUS(Status::MemFSError(
            string("Out of memory, cannot write to file: " + name_)));
      }

      size_ = nbytes;
    } else {
      /* if the file already had some data */
      void* new_data;
      new_data = realloc(data_, size_ + nbytes);

      if (new_data == nullptr) {
        return LOG_STATUS(Status::MemFSError(string(
            "Out of memory, cannot append new data to file: " + name_ +
            "with size " + "size_")));
      }

      memcpy((char*)new_data + size_, data, nbytes);
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

    memcpy(buffer, (char*)data_ + offset, nbytes);
    return Status::Ok();
  }

 private:
  void* data_;
  uint64_t size_;
};

class MemFilesystem::Directory : public MemFilesystem::FSNode {
 public:
  Directory(const string& name)
      : FSNode(name){};

  ~Directory(){};

  bool is_dir() const override {
    return true;
  }

  bool is_file() const override {
    return false;
  }

  vector<string> ls(const string& full_path) const override {
    vector<string> names;
    for (const auto& child : children_) {
      names.emplace_back(full_path + child.first);
    }

    sort(names.begin(), names.end());
    return names;
  }

  bool has_child(const string& child) const override {
    return (children_.count(child) != 0);
  }
};

/** Constructor. */
MemFilesystem::MemFilesystem()
    : root_(unique_ptr<Directory>(new Directory(""))){};

/** Destructor. */
MemFilesystem::~MemFilesystem(){};

Status MemFilesystem::ls(const string& path, vector<string>* paths) const {
  assert(paths);
  if (root_ == nullptr) {
    return LOG_STATUS(Status::MemFSError(
        string("ls not possible, in-memory filesystem is not initialized")));
  }

  vector<string> tokens = tokenize(path);
  FSNode* cur = root_.get();
  string dir;
  for (auto& token : tokens) {
    dir = dir + "/" + token;
    cur = cur->children_[token].get();
  }

  *paths = cur->ls(dir);
  return Status::Ok();
}

Status MemFilesystem::create_dir(const string& path) {
  if (root_ == nullptr) {
    return LOG_STATUS(Status::MemFSError(
        string("Directory creation not possible, in-memory filesystem is "
               "not initialized")));
  }

  vector<string> tokens = tokenize(path);
  FSNode* cur = root_.get();
  for (auto& token : tokens) {
    if (!cur->has_child(token)) {
      cur->children_[token] = unique_ptr<Directory>(new Directory(token));
    }
    cur = cur->children_[token].get();
  }

  return Status::Ok();
}

Status MemFilesystem::touch(const string& path) {
  FSNode* node = create_file(path);
  if (node == nullptr) {
    return LOG_STATUS(
        Status::MemFSError(string("Failed to create file: " + path)));
  }

  return Status::Ok();
}

MemFilesystem::FSNode* MemFilesystem::create_file(const string& path) {
  // TODO: check if path is well formatted
  assert(root_);

  vector<string> tokens = tokenize(path);
  FSNode* cur = root_.get();
  for (unsigned long i = 0; i < tokens.size() - 1; i++) {
    auto token = tokens[i];
    if (!cur->has_child(token)) {
      cur->children_[token] = unique_ptr<Directory>(new Directory(token));
    }
    cur = cur->children_[token].get();
  }

  string filename = tokens[tokens.size() - 1];
  cur->children_[filename] = unique_ptr<File>(new File(filename));
  return cur->children_[filename].get();
}

Status MemFilesystem::write(
    const string& path, const void* data, const uint64_t nbytes) {
  FSNode* node = lookup_node(path);
  /* If the file doesn't exist, create it */
  if (node == nullptr) {
    node = create_file(path);
    if (node == nullptr) {
      return LOG_STATUS(
          Status::MemFSError(string("Failed to create file: " + path)));
    }
    /* If it's a directory, ignore */
  } else if (node->is_dir()) {
    return LOG_STATUS(Status::MemFSError(string(
        "Failed to write on directory, write is only possible on files.")));
  }

  return ((File*)node)->append(data, nbytes);
}

Status MemFilesystem::read(
    const string& path,
    const uint64_t offset,
    void* buffer,
    const uint64_t nbytes) const {
  FSNode* node = lookup_node(path);
  if ((node == nullptr) || node->is_dir()) {
    return LOG_STATUS(Status::MemFSError(
        string("File not found, read failed for : " + path)));
  }

  return ((File*)node)->read(offset, buffer, nbytes);
}

vector<string> MemFilesystem::tokenize(const string& path, const char delim) {
  vector<string> tokens;
  stringstream ss(path);
  string token;
  while (getline(ss, token, delim)) {
    if (!token.empty()) {
      tokens.emplace_back(token);
    }
  }

  return tokens;
}

MemFilesystem::FSNode* MemFilesystem::lookup_node(const string& path) const {
  assert(root_);

  vector<string> tokens = tokenize(path);
  auto cur = root_.get();
  for (const auto& token : tokens) {
    if (cur->has_child(token)) {
      cur = cur->children_[token].get();
    } else {
      return nullptr;
    }
  }

  return cur;
}

Status MemFilesystem::remove(const string& path) {
  vector<string> tokens = tokenize(path);
  auto cur = root_.get();
  FSNode* parent = nullptr;
  for (const auto& token : tokens) {
    if (cur->has_child(token)) {
      parent = cur;
      cur = cur->children_[token].get();
    } else {
      return LOG_STATUS(Status::MemFSError(
          string("File not found, remove failed for : " + path)));
    }
  }

  if (parent) {
    parent->children_.erase(tokens.back());
  }
  return Status::Ok();
};

}  // namespace sm
}  // namespace tiledb