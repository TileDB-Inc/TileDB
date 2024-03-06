/* polymorphic_allocator.cpp                  -*-C++-*-
 *
 *            Copyright 2012 Pablo Halpern.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */

#include "polymorphic_allocator.h"

namespace cpp17 {

atomic<pmr::memory_resource*> pmr::memory_resource::s_default_resource(nullptr);

pmr::new_delete_resource* pmr::new_delete_resource_singleton() {
  // TBD: I think the standard makes this exception-safe, otherwise, we need
  // to use 'call_once()' in '<mutex>'.
  static new_delete_resource singleton;
  return &singleton;
}

}  // namespace cpp17

// end polymorphic_allocator.cpp
