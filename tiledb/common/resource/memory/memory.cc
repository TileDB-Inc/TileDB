/**
 * @file   tiledb/common/resource/memory/memory.cc
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
 */

#include "./memory.h"

namespace tiledb::common {
namespace detail {
/**
 * A class that sets up the default memory resource that the default constructor
 * of `std::pmr::allocator` uses.
 *
 * The default resource that's used if not overridden is `new_delete_resource`.
 * Using this resource would allocate memory outside of any budget. In order to
 * avoid inadvertent leaks, we disallow the default-constructed resource from
 * allocating anything. We can't do this by setting the default resource to
 * `nullptr`, but we can by setting it to `std::pmr::null_memory_resource()`,
 * whose `allocate()` function always throws `bad_alloc`.
 *
 * There is a single, static instance of this class defined below.
 *
 * Note that this initialization is quite simple and is not intended to support
 * initialization of other static variables that use the default constructor of
 * `std::pmr::allocator`.
 */
struct DefaultMemoryResourceSetup {
  /**
   * Default constructor.
   *
   * The behavior of this constructor is idempotent. It's not an error to call
   * it more that once. It's simply not necessary to do so.
   */
  DefaultMemoryResourceSetup() {
    std::pmr::set_default_resource(std::pmr::null_memory_resource());
  }
};

/**
 * The single instance of `DefaultMemoryResourceSetup`.
 */
static DefaultMemoryResourceSetup DMR_initializer{};
}  // namespace detail

}  // namespace tiledb::common