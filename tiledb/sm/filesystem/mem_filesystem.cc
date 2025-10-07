/**
 * @file   mem_filesystem.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2025 TileDB, Inc.
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

#include <algorithm>
#include <mutex>
#include <sstream>
#include <unordered_set>

#include "tiledb/common/assert.h"
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/filesystem/mem_filesystem.h"
#include "uri.h"

using namespace tiledb::common;
using tiledb::common::filesystem::directory_entry;

namespace tiledb::sm {

class MemFilesystem::FSNode {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  FSNode() = default;

  /** Copy constructor. */
  DISABLE_COPY(FSNode);

  /** Move constructor. */
  DISABLE_MOVE(FSNode);

  /** Destructor. */
  virtual ~FSNode() = default;

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  /** Copy-assignment. */
  DISABLE_COPY_ASSIGN(FSNode);

  /** Move-assignment. */
  DISABLE_MOVE_ASSIGN(FSNode);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Indicates if it's a file or a directory */
  virtual bool is_dir() const = 0;

  /** Lists the contents of a node */
  virtual tuple<Status, optional<std::vector<directory_entry>>> ls(
      const std::string& full_path) const = 0;

  /** Indicates if a given node is a child of this node */
  virtual bool has_child(const std::string& child) const = 0;

  /** Gets the size in bytes of this node */
  virtual Status get_size(uint64_t* size) const = 0;

  /** Outputs the contents of the node to a buffer */
  virtual Status read(
      const uint64_t offset, void* buffer, const uint64_t nbytes) const = 0;

  /** Outputs the contents of a buffer to this node */
  virtual Status append(const void* data, const uint64_t nbytes) = 0;

  /* ********************************* */
  /*         PUBLIC ATTRIBUTES         */
  /* ********************************* */

  /** Protects `children_`. */
  mutable std::mutex mutex_;

  /** A hashtable of all the next-level subnodes of this node*/
  std::unordered_map<std::string, tdb_unique_ptr<FSNode>> children_;
};

class MemFilesystem::File : public MemFilesystem::FSNode {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  File()
      : FSNode()
      , data_(nullptr)
      , size_(0) {
  }

  /** Copy constructor. */
  DISABLE_COPY(File);

  /** Move constructor. */
  DISABLE_MOVE(File);

  /** Destructor. */
  ~File() {
    if (data_) {
      tdb_free(data_);
    }
  }

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  /** Copy-assignment. */
  DISABLE_COPY_ASSIGN(File);

  /** Move-assignment. */
  DISABLE_MOVE_ASSIGN(File);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  bool is_dir() const override {
    passert(!mutex_.try_lock());
    return false;
  }

  /** Returns the full path to this file */
  tuple<Status, optional<std::vector<directory_entry>>> ls(
      const std::string&) const override {
    passert(!mutex_.try_lock());

    auto st = LOG_STATUS(Status_MemFSError(
        std::string("Cannot get children, the path is a file")));
    return {st, nullopt};
  }

  bool has_child(const std::string&) const override {
    return false;
  }

  Status get_size(uint64_t* const size) const override {
    passert(!mutex_.try_lock());
    iassert(size);

    *size = size_;
    return Status::Ok();
  }

  /** Outputs in a buffer nbytes of this file starting at offset */
  Status read(const uint64_t offset, void* buffer, const uint64_t nbytes)
      const override {
    passert(!mutex_.try_lock());
    iassert(buffer);

    if (offset + nbytes > size_)
      return LOG_STATUS(Status_MemFSError(fmt::format(
          "Cannot read from file; Read exceeds file size: offset {} nbytes {} "
          "size_ {}",
          offset,
          nbytes,
          size_)));

    memcpy(buffer, (char*)data_ + offset, nbytes);
    return Status::Ok();
  }

  /** Writes nbytes of data in the end of this file */
  Status append(const void* const data, const uint64_t nbytes) override {
    passert(!mutex_.try_lock());
    iassert(data);

    if ((data == nullptr) || nbytes == 0) {
      return LOG_STATUS(Status_MemFSError(
          std::string("Wrong input buffer or size when writing to file")));
    }

    if (data_ == nullptr) {
      data_ = tdb_malloc(nbytes);
      if (data_ == nullptr) {
        return LOG_STATUS(Status_MemFSError(
            std::string("Out of memory, cannot write to file")));
      }
      memcpy(data_, data, nbytes);
      size_ = nbytes;
    } else {
      void* new_data;
      new_data = tdb_realloc(data_, size_ + nbytes);

      if (new_data == nullptr) {
        return LOG_STATUS(
            Status_MemFSError("Out of memory, cannot append new data to file"));
      }

      memcpy((char*)new_data + size_, data, nbytes);
      data_ = new_data;
      size_ += nbytes;
    }

    return Status::Ok();
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** the data stored in this file */
  void* data_;

  /** the size in bytes of the data in this file */
  uint64_t size_;
};

class MemFilesystem::Directory : public MemFilesystem::FSNode {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  Directory()
      : FSNode() {
  }

  /** Copy constructor. */
  DISABLE_COPY(Directory);

  /** Move constructor. */
  DISABLE_MOVE(Directory);

  /** Destructor. */
  ~Directory() = default;

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  /** Copy-assignment. */
  DISABLE_COPY_ASSIGN(Directory);

  /** Move-assignment. */
  DISABLE_MOVE_ASSIGN(Directory);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  bool is_dir() const override {
    passert(!mutex_.try_lock());
    return true;
  }

  tuple<Status, optional<std::vector<directory_entry>>> ls(
      const std::string& full_path) const override {
    passert(!mutex_.try_lock());

    std::vector<directory_entry> names;
    names.reserve(children_.size());
    for (const auto& child : children_) {
      std::unique_lock<std::mutex> lock(child.second->mutex_);
      if (child.second->is_dir()) {
        names.emplace_back("mem://" + full_path + child.first, 0, true);
      } else {
        uint64_t size;
        RETURN_NOT_OK_TUPLE(child.second->get_size(&size), nullopt);
        names.emplace_back("mem://" + full_path + child.first, size, false);
      }
    }

    return {Status::Ok(), names};
  }

  bool has_child(const std::string& child) const override {
    passert(!mutex_.try_lock());

    return children_.count(child) != 0;
  }

  Status get_size(uint64_t* const) const override {
    passert(!mutex_.try_lock());

    return LOG_STATUS(Status_MemFSError(
        std::string("Cannot get size, the path is a directory")));
  }

  Status read(const uint64_t, void*, const uint64_t) const override {
    return LOG_STATUS(Status_MemFSError(
        std::string("Cannot read contents, the path is a directory")));
  }

  Status append(const void* const, const uint64_t) override {
    passert(!mutex_.try_lock());

    return LOG_STATUS(Status_MemFSError(
        std::string("Cannot append contents, the path is a directory")));
  }
};

MemFilesystem::MemFilesystem()
    : root_(tdb_unique_ptr<FSNode>(tdb_new(Directory))) {
  passert(root_);
}

MemFilesystem::~MemFilesystem() {
}

bool MemFilesystem::supports_uri(const URI& uri) const {
  return uri.is_memfs();
}

void MemFilesystem::create_dir(const URI& uri) const {
  create_dir_internal(uri.to_path());
}

uint64_t MemFilesystem::file_size(const URI& uri) const {
  FSNode* cur;
  std::unique_lock<std::mutex> cur_lock;
  auto path = uri.to_path();
  throw_if_not_ok(lookup_node(path, &cur, &cur_lock));

  if (cur == nullptr) {
    throw MemFSException(std::string("Cannot get file size of :" + path));
  }

  uint64_t size;
  throw_if_not_ok(cur->get_size(&size));
  return size;
}

bool MemFilesystem::is_dir(const URI& uri) const {
  FSNode* cur;
  std::unique_lock<std::mutex> cur_lock;
  if (!lookup_node(uri.to_path(), &cur, &cur_lock).ok() || cur == nullptr) {
    return false;
  }

  return cur->is_dir();
}

bool MemFilesystem::is_file(const URI& uri) const {
  FSNode* cur;
  std::unique_lock<std::mutex> cur_lock;
  if (!lookup_node(uri.to_path(), &cur, &cur_lock).ok() || cur == nullptr) {
    return false;
  }

  return !cur->is_dir();
}

Status MemFilesystem::ls(
    const std::string& path, std::vector<std::string>* const paths) const {
  iassert(paths);

  for (auto& fs : ls_with_sizes(URI(path))) {
    paths->emplace_back(fs.path().native());
  }

  return Status::Ok();
}

std::vector<directory_entry> MemFilesystem::ls_with_sizes(
    const URI& path) const {
  auto abspath = path.to_path();
  std::vector<std::string> tokens = tokenize(abspath);

  FSNode* cur = root_.get();
  std::unique_lock<std::mutex> cur_lock(cur->mutex_);
  std::string dir;
  for (auto& token : tokens) {
    passert(cur_lock.owns_lock());
    passert(cur_lock.mutex() == &cur->mutex_);
    dir = dir + token + "/";

    if (cur->children_.count(token) != 1) {
      throw MemFSException("Unable to list on non-existent path " + abspath);
    }

    cur = cur->children_[token].get();
    cur_lock = std::unique_lock<std::mutex>(cur->mutex_);
  }

  passert(cur_lock.owns_lock());
  passert(cur_lock.mutex() == &cur->mutex_);
  auto&& [st, entries] = cur->ls(dir);
  throw_if_not_ok(st);

  return *entries;
}

void MemFilesystem::move(
    const std::string& old_path, const std::string& new_path) const {
  std::vector<std::string> old_path_tokens = tokenize(old_path);
  if (old_path_tokens.size() <= 1) {
    throw MemFSException("Cannot move the root directory");
  }

  // Remove the last token so that `old_path_tokens` contains the path
  // tokens to the parent of `old_path`.
  const std::string old_path_last_token = old_path_tokens.back();
  old_path_tokens.pop_back();

  // Lookup the `old_path` parent.
  FSNode* old_node_parent;
  std::unique_lock<std::mutex> old_node_parent_lock;
  throw_if_not_ok(
      lookup_node(old_path_tokens, &old_node_parent, &old_node_parent_lock));

  // Detach `old_path` from the directory tree.
  if (old_node_parent->children_.count(old_path_last_token) == 0) {
    throw MemFSException("Move failed, file not found: " + old_path);
  }
  tdb_unique_ptr<FSNode> old_node_ptr =
      std::move(old_node_parent->children_[old_path_last_token]);
  old_node_parent->children_.erase(old_path_last_token);
  old_node_parent_lock.unlock();

  // We now have a handle to the old node. It is detached from its
  // parent and we hold no locks. Next, we add the old node to the
  // parent of the new node.
  std::vector<std::string> new_path_tokens = tokenize(new_path);
  if (new_path_tokens.size() <= 1) {
    throw MemFSException("Cannot move to the root directory");
  }

  // Remove the last token so that `new_path_tokens` contains the path
  // tokens to the parent of `new_path`.
  const std::string new_path_last_token = new_path_tokens.back();
  new_path_tokens.pop_back();

  // Lookup the `new_path` parent.
  FSNode* new_node_parent;
  std::unique_lock<std::mutex> new_node_parent_lock;
  throw_if_not_ok(
      lookup_node(new_path_tokens, &new_node_parent, &new_node_parent_lock));

  // Add `old_path` to the directory tree.
  new_node_parent->children_[new_path_last_token] = std::move(old_node_ptr);
}

void MemFilesystem::move_dir(const URI& old_uri, const URI& new_uri) const {
  move(old_uri.to_path(), new_uri.to_path());
}

void MemFilesystem::move_file(const URI& old_uri, const URI& new_uri) const {
  move(old_uri.to_path(), new_uri.to_path());
}

uint64_t MemFilesystem::read(
    const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) {
  FSNode* node;
  std::unique_lock<std::mutex> node_lock;
  auto path = uri.to_path();
  throw_if_not_ok(lookup_node(path, &node, &node_lock));

  if (node == nullptr) {
    throw MemFSException("File not found, read failed for : " + path);
  }

  throw_if_not_ok(node->read(offset, buffer, nbytes));
  return nbytes;
}

void MemFilesystem::remove(const std::string& path, const bool is_dir) const {
  std::vector<std::string> tokens = tokenize(path);

  FSNode* cur = root_.get();
  std::unique_lock<std::mutex> cur_lock(cur->mutex_);
  FSNode* parent = nullptr;
  std::unique_lock<std::mutex> parent_lock;
  for (const auto& token : tokens) {
    passert(cur_lock.owns_lock());
    passert(cur_lock.mutex() == &cur->mutex_);

    passert(!parent || parent_lock.owns_lock());
    passert(!parent || parent_lock.mutex() == &parent->mutex_);

    if (!cur->has_child(token)) {
      throw MemFSException("File not found, remove failed for : " + path);
    }

    parent = cur;
    std::swap(parent_lock, cur_lock);

    cur = cur->children_[token].get();
    cur_lock = std::unique_lock<std::mutex>(cur->mutex_);
  }

  if (cur == root_.get()) {
    throw MemFSException("Cannot remove the root directory");
  }

  if (cur->is_dir() != is_dir) {
    throw MemFSException("Remove failed, wrong file type");
  }

  cur_lock.unlock();

  if (parent) {
    parent->children_.erase(tokens.back());
  }
}

void MemFilesystem::remove_dir(const URI& uri) const {
  remove(uri.to_path(), true);
}

void MemFilesystem::remove_file(const URI& uri) const {
  remove(uri.to_path(), false);
}

void MemFilesystem::touch(const URI& uri) const {
  touch_internal(uri.to_path());
}

MemFilesystem::FSNode* MemFilesystem::create_dir_internal(
    const std::string& path) const {
  std::vector<std::string> tokens = tokenize(path);

  FSNode* cur = root_.get();
  std::unique_lock<std::mutex> cur_lock(cur->mutex_);

  for (auto iter = tokens.begin(); iter != tokens.end(); ++iter) {
    const std::string& token = *iter;

    passert(cur_lock.owns_lock());
    passert(cur_lock.mutex() == &cur->mutex_);

    if (!cur->has_child(token)) {
      cur->children_[token] = tdb_unique_ptr<FSNode>(tdb_new(Directory));
    } else if (!cur->is_dir()) {
      throw MemFSException(std::string(
          "Cannot create directory, a file with that name exists already: " +
          path));
    }

    cur = cur->children_[token].get();

    // Only take the lock for `cur` if it is not the newly-created
    // directory.
    if (std::next(iter) != tokens.end()) {
      cur_lock = std::unique_lock<std::mutex>(cur->mutex_);
    }
  }

  passert(cur_lock.owns_lock());
  cur_lock.unlock();

  return cur;
}

MemFilesystem::FSNode* MemFilesystem::touch_internal(
    const std::string& path) const {
  std::vector<std::string> tokens = tokenize(path);

  FSNode* cur = root_.get();
  std::unique_lock<std::mutex> cur_lock(cur->mutex_);
  for (unsigned long i = 0; i < tokens.size() - 1; ++i) {
    const std::string& token = tokens[i];

    passert(cur_lock.owns_lock());
    passert(cur_lock.mutex() == &cur->mutex_);

    if (!cur->has_child(token)) {
      throw MemFSException(
          "Failed to create file, the parent directory doesn't exist.");
    }

    cur = cur->children_[token].get();
    cur_lock = std::unique_lock<std::mutex>(cur->mutex_);
  }

  if (!cur->is_dir()) {
    throw MemFSException(
        "Failed to create file, the parent directory doesn't exist.");
  }

  const std::string& filename = tokens[tokens.size() - 1];
  cur->children_[filename] = tdb_unique_ptr<FSNode>(tdb_new(File));

  return cur->children_[filename].get();
}

void MemFilesystem::write(
    const URI& uri, const void* buffer, uint64_t buffer_size, bool) {
  iassert(buffer);
  auto path = uri.to_path();

  FSNode* node;
  std::unique_lock<std::mutex> node_lock;
  throw_if_not_ok(lookup_node(path, &node, &node_lock));

  // If the file doesn't exist, create it.
  if (node == nullptr) {
    node = touch_internal(path);
    node_lock = std::unique_lock<std::mutex>(node->mutex_);
  }

  throw_if_not_ok(node->append(buffer, buffer_size));
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

Status MemFilesystem::lookup_node(
    const std::string& path,
    MemFilesystem::FSNode** node,
    std::unique_lock<std::mutex>* node_lock) const {
  iassert(node);
  iassert(node_lock);
  iassert(!node_lock->owns_lock());

  const std::vector<std::string> tokens = tokenize(path);
  return lookup_node(tokens, node, node_lock);
}

Status MemFilesystem::lookup_node(
    const std::vector<std::string>& tokens,
    MemFilesystem::FSNode** node,
    std::unique_lock<std::mutex>* node_lock) const {
  iassert(node);
  iassert(node_lock);
  iassert(!node_lock->owns_lock());

  FSNode* cur = root_.get();
  std::unique_lock<std::mutex> cur_lock(cur->mutex_);
  for (const auto& token : tokens) {
    passert(cur_lock.owns_lock());
    passert(cur_lock.mutex() == &cur->mutex_);

    if (cur->has_child(token)) {
      cur = cur->children_[token].get();
    } else {
      *node = nullptr;
      return Status::Ok();
    }

    cur_lock = std::unique_lock<std::mutex>(cur->mutex_);
  }

  passert(cur_lock.owns_lock());
  passert(cur_lock.mutex() == &cur->mutex_);

  *node = cur;
  std::swap(*node_lock, cur_lock);

  return Status::Ok();
}

}  // namespace tiledb::sm
