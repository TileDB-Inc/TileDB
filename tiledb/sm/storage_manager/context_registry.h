/**
 * @file tiledb/sm/storage_manager/context_registry.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file declares `class ContextRegistry`.
 *
 * @section Life Cycle
 *
 * `class ContextRegistry` is used as a singleton. It's defined within the
 * accessor function for the singleton, and is thus dynamically initialized.
 */

#ifndef TILEDB_CONTEXT_REGISTRY_H
#define TILEDB_CONTEXT_REGISTRY_H

#include "tiledb/common/registry/registry.h"

namespace tiledb::sm {

// Forward
class Context;

class ContextRegistry {
  friend class Context;
  using registry_type = common::Registry<Context>;

  registry_type registry_;

  using handle_type = registry_type::registry_handle_type;

  /**
   * Register a context in this registry.
   *
   * This function is private so that it's accessible only to `class Context`.
   *
   * @param context
   * @return
   */
  handle_type register_context(Context& context) {
    return registry_.register_item(context);
  }

 public:
  /**
   * Ordinary constructor is the default one.
   *
   * @section Design
   *
   * Having a default constructor is an intentional design choice. Because this
   * class is used only as a singleton, it's desirable that it be nonparametric,
   * exactly so that it can be initialized without interacting with the life
   * cycle of the library itself, including such issues as dynamic loading.
   *
   * This decision is a consequence of the fact that the library itself does not
   * require a designated point of initialization. There's no class that
   * represents the library, for example, nor is there a C API call to
   * initialize the library. As such, we have no good place to designate
   * parameters, and thus this class does not use any.
   */
  ContextRegistry() = default;

  /**
   * Destructor is the default one.
   */
  ~ContextRegistry() = default;

  /**
   * Accessor function to the singleton.
   *
   * The registry is not parametric. It uses no configuration variables nor
   * arguments. As a result, the singleton instance can be default-constructed
   * in the body of this function.
   *
   * @return a reference to the singleton instance of `ContextRegistry`
   */
  static ContextRegistry& get() {
    static ContextRegistry context_registry{};
    return context_registry;
  }
};

}  // namespace tiledb::sm

#endif  // TILEDB_CONTEXT_REGISTRY_H
