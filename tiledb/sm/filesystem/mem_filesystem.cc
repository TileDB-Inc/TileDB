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

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class MemFilesystem::FSNode {
 public:
  FSNode(const std::string& name, FSNode* parent)
      : name_(name)
      , parent_(parent){};

  virtual ~FSNode() = default;

  /* Indicates if it's a file or a directory */
  virtual bool is_dir() const = 0;

  /* Lists the contents of a node */
  virtual std::vector<std::string> ls(const std::string& full_path) const = 0;

  /* Indicates if a given node is a child of this node */
  virtual bool has_child(const std::string& child) const = 0;

  /* Gets the size in bytes of this node */
  virtual Status get_size(uint64_t* size) const = 0;

  /* Outputs the contents of the node to a buffer */
  virtual Status read(
      const uint64_t offset, void* buffer, const uint64_t nbytes) const = 0;

  /* Outputs the contents of a buffer to this node */
  virtual Status append(const void* data, const uint64_t nbytes) = 0;

  /* A hashtable of all the next-level subnodes of this node*/
  std::unordered_map<std::string, std::unique_ptr<FSNode>> children_;

  /* The name of this node */
  std::string name_;

  /* A pointer to the parent node of this node */
  FSNode* parent_;
};

class MemFilesystem::File : public MemFilesystem::FSNode {
 public:
  /** Constructor. */
  File(const std::string& name, FSNode* parent)
      : FSNode(name, parent)
      , data_(nullptr)
      , size_(0){};

  /** Destructor. */
  ~File() {
    if (data_) {
      free(data_);
    }
  };

  bool is_dir() const override {
    return false;
  }

  /* Returns the full path to this file */
  std::vector<std::string> ls(const std::string& full_path) const override {
    return {full_path + name_};
  }

  bool has_child(const std::string&) const override {
    return false;
  }

  Status get_size(uint64_t* size) const override {
    *size = size_;
    return Status::Ok();
  }

  /* Writes nbytes of data in the end of this file */
  Status append(const void* data, const uint64_t nbytes) override {
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
      memcpy(data_, data, nbytes);
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

      memcpy((char*)new_data + size_, data, nbytes);
      data_ = new_data;
      size_ += nbytes;
    }

    return Status::Ok();
  }

  /* Outputs in a buffer nbytes of this file starting at offset */
  Status read(const uint64_t offset, void* buffer, const uint64_t nbytes)
      const override {
    assert(buffer);

    if (offset + nbytes > size_)
      return LOG_STATUS(
          Status::MemFSError("Cannot read from file; Read exceeds file size"));

    memcpy(buffer, (char*)data_ + offset, nbytes);
    return Status::Ok();
  }

 private:
  /* the data stored in this file */
  void* data_;
  /* the size in bytes of the data in this file */
  uint64_t size_;
};

class MemFilesystem::Directory : public MemFilesystem::FSNode {
 public:
  Directory(const std::string& name, FSNode* parent)
      : FSNode(name, parent){};

  ~Directory(){};

  bool is_dir() const override {
    return true;
  }

  std::vector<std::string> ls(const std::string& full_path) const override {
    std::vector<std::string> names;
    for (const auto& child : children_) {
      names.emplace_back(full_path + child.first);
    }

    sort(names.begin(), names.end());
    return names;
  }

  bool has_child(const std::string& child) const override {
    return children_.count(child) != 0;
  }

  Status get_size(uint64_t*) const override {
    /* we don't store the size of directories */
    return Status::MemFSError(
        std::string("Cannot get size, the path is a directory"));
  }

  Status read(const uint64_t, void*, const uint64_t) const override {
    /* we can't read the contents of directories */
    return Status::MemFSError(
        std::string("Cannot read contents, the path is a directory"));
  }

  Status append(const void*, const uint64_t) override {
    /* we can't write in a directory */
    return Status::MemFSError(
        std::string("Cannot append contents, the path is a directory"));
  }
};

MemFilesystem::MemFilesystem()
    : root_(std::unique_ptr<Directory>(new Directory("", nullptr))) {
  assert(root_);
};

MemFilesystem::~MemFilesystem(){};

Status MemFilesystem::create_dir(const std::string& path) const {
  std::unique_lock<std::mutex> fs_lock(memfilesystem_mutex_);
  return create_dir_unsafe(path);
}

Status MemFilesystem::create_dir_unsafe(const std::string& path) const {
  std::vector<std::string> tokens = tokenize(path);
  FSNode* cur = root_.get();
  for (auto& token : tokens) {
    if (!cur->has_child(token)) {
      cur->children_[token] =
          std::unique_ptr<Directory>(new Directory(token, cur));
    } else if (!cur->is_dir()) {
      return LOG_STATUS(Status::MemFSError(std::string(
          "Cannot create directory, a file with that name exists already: " +
          path)));
    }
    cur = cur->children_[token].get();
  }

  return Status::Ok();
}

Status MemFilesystem::file_size(const std::string& path, uint64_t* size) const {
  std::unique_lock<std::mutex> fs_lock(memfilesystem_mutex_);

  FSNode* cur = lookup_node(path);
  if (cur == nullptr) {
    return LOG_STATUS(
        Status::MemFSError(std::string("Cannot get file size of :" + path)));
  }

  return cur->get_size(size);
}

bool MemFilesystem::is_dir(const std::string& path) const {
  std::unique_lock<std::mutex> fs_lock(memfilesystem_mutex_);

  FSNode* cur = lookup_node(path);
  if (cur == nullptr) {
    return false;
  }

  return cur->is_dir();
}

bool MemFilesystem::is_file(const std::string& path) const {
  std::unique_lock<std::mutex> fs_lock(memfilesystem_mutex_);

  FSNode* cur = lookup_node(path);
  if (cur == nullptr) {
    return false;
  }

  return !cur->is_dir();
}

Status MemFilesystem::ls(
    const std::string& path, std::vector<std::string>* paths) const {
  assert(paths);
  if (root_ == nullptr) {
    return LOG_STATUS(Status::MemFSError(std::string(
        "ls not possible, in-memory filesystem is not initialized")));
  }

  std::vector<std::string> tokens = tokenize(path);

  std::unique_lock<std::mutex> fs_lock(memfilesystem_mutex_);

  FSNode* cur = root_.get();
  std::string dir;
  for (auto& token : tokens) {
    dir = dir + token + "/";
    cur = cur->children_[token].get();
  }

  *paths = cur->ls(dir);
  return Status::Ok();
}

Status MemFilesystem::move(
    const std::string& old_path, const std::string& new_path) const {
  std::unique_lock<std::mutex> fs_lock(memfilesystem_mutex_);

  FSNode* old_node = lookup_node(old_path);
  if (old_node == nullptr) {
    return LOG_STATUS(Status::MemFSError(
        std::string("Move failed, file not found: " + old_path)));
  }

  FSNode* old_parent_node = old_node->parent_;
  if (old_parent_node == nullptr) {
    return LOG_STATUS(
        Status::MemFSError(std::string("Cannot move the root directory")));
  }

  /* get the path to the directory where the new file is to be created */
  std::vector<std::string> new_tokens = tokenize(new_path);
  std::string new_dir;
  for (unsigned long i = 0; i < new_tokens.size() - 1; i++) {
    new_dir = new_dir + new_tokens[i] + "/";
  }

  FSNode* new_parent_node = lookup_node(new_dir);
  if (new_parent_node == nullptr) {
    create_dir_unsafe(new_dir);
    new_parent_node = lookup_node(new_dir);
  }

  std::vector<std::string> old_tokens = tokenize(old_path);
  new_parent_node->children_[new_tokens.back()] =
      std::move(old_parent_node->children_[old_tokens.back()]);
  new_parent_node->children_[new_tokens.back()]->name_ = new_tokens.back();
  new_parent_node->children_[new_tokens.back()]->parent_ = new_parent_node;
  old_parent_node->children_.erase(old_tokens.back());

  return Status::Ok();
}

Status MemFilesystem::read(
    const std::string& path,
    const uint64_t offset,
    void* buffer,
    const uint64_t nbytes) const {
  std::unique_lock<std::mutex> fs_lock(memfilesystem_mutex_);

  FSNode* node = lookup_node(path);
  if (node == nullptr) {
    return LOG_STATUS(Status::MemFSError(
        std::string("File not found, read failed for : " + path)));
  }

  return node->read(offset, buffer, nbytes);
}

Status MemFilesystem::remove(const std::string& path, bool is_dir) const {
  std::vector<std::string> tokens = tokenize(path);

  std::unique_lock<std::mutex> fs_lock(memfilesystem_mutex_);

  auto cur = root_.get();
  FSNode* parent = nullptr;
  for (const auto& token : tokens) {
    if (cur->has_child(token)) {
      parent = cur;
      cur = cur->children_[token].get();
    } else {
      return LOG_STATUS(Status::MemFSError(
          std::string("File not found, remove failed for : " + path)));
    }
  }

  if (cur == root_.get()) {
    return LOG_STATUS(
        Status::MemFSError(std::string("Cannot remove the root directory")));
  }

  if (cur->is_dir() != is_dir) {
    return LOG_STATUS(
        Status::MemFSError(std::string("Remove failed, wrong file type")));
  }

  if (parent) {
    parent->children_.erase(tokens.back());
  }
  return Status::Ok();
};

Status MemFilesystem::touch(const std::string& path) const {
  std::unique_lock<std::mutex> fs_lock(memfilesystem_mutex_);
  return touch_unsafe(path);
}

Status MemFilesystem::touch_unsafe(const std::string& path) const {
  std::vector<std::string> tokens = tokenize(path);
  FSNode* cur = root_.get();
  for (unsigned long i = 0; i < tokens.size() - 1; i++) {
    auto token = tokens[i];
    if (!cur->has_child(token)) {
      return LOG_STATUS(Status::MemFSError(std::string(
          "Failed to create file, the parent directory doesn't exist.")));
    }
    cur = cur->children_[token].get();
  }

  if (!cur->is_dir()) {
    return LOG_STATUS(Status::MemFSError(std::string(
        "Failed to create file, the parent directory doesn't exist.")));
  }

  std::string filename = tokens[tokens.size() - 1];
  cur->children_[filename] = std::unique_ptr<File>(new File(filename, cur));
  return Status::Ok();
}

Status MemFilesystem::write(
    const std::string& path, const void* data, const uint64_t nbytes) {
  std::unique_lock<std::mutex> fs_lock(memfilesystem_mutex_);

  FSNode* node = lookup_node(path);
  /* If the file doesn't exist, create it */
  if (node == nullptr) {
    touch_unsafe(path);
    node = lookup_node(path);
    /* If it's a directory, ignore */
  } else if (node->is_dir()) {
    return LOG_STATUS(Status::MemFSError(std::string(
        "Failed to write on directory, write is only possible on files.")));
  }

  return ((File*)node)->append(data, nbytes);
}

std::vector<std::string> MemFilesystem::tokenize(
    const std::string& path, const char delim) {
  std::vector<std::string> tokens;
  std::stringstream ss(path);
  std::string token;
  while (getline(ss, token, delim)) {
    if (!token.empty()) {
      tokens.emplace_back(token);
    }
  }

  return tokens;
}

MemFilesystem::FSNode* MemFilesystem::lookup_node(
    const std::string& path) const {
  std::vector<std::string> tokens = tokenize(path);
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

}  // namespace sm
}  // namespace tiledb