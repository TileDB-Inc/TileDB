/**
 * @file cancellation_source.h
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
 * This file declares `class CancellationSource`.
 */

#ifndef TILEDB_CANCELLATION_SOURCE_H
#define TILEDB_CANCELLATION_SOURCE_H

#include "tiledb/stdx/stop_token"  // substitutes for <stop_token>
//-------- clang-format separator --------
#include "storage_manager_declaration.h"

namespace tiledb::sm {

/**
 * The legacy cancellation source is a wrapper around `StorageManager` but with
 * a restricted interface.
 */
class LegacyCancellationSource {
  StorageManager& sm_;

 public:
  explicit LegacyCancellationSource(StorageManager& sm);

  [[nodiscard]] bool cancellation_in_progress() const;
};
using CancellationSource = LegacyCancellationSource;

}  // namespace tiledb::sm

namespace tiledb::common {

/**
 * Marker class for cancellation source constructor
 */
struct CancellationOriginT {
  CancellationOriginT() = default;
};
/**
 * Marker element for cancellation source constructor
 */
constexpr static CancellationOriginT cancellation_origin{};

/**
 * This is the new cancellation source. It has not yet replaced the legacy one,
 * but it's here in stub form so that the constructors of the job system may be
 * defined as they will be later.
 *
 * @section Usage
 *
 * Each job is a cancellation origin. Use the origin-marked constructor to
 * create the cancellation source member variable of a job.
 *
 * Activities within a job are subordinate cancellation sources. Create such
 * objects with the copy constructor. They should be passed by value, not by
 * reference.

 * @section Design
 *
 * Cancellation is propagated explicitly downward away from the root, not
 * implicitly by copying the cancellation source. This choice is required so
 * that individual branches of a job tree may be cancelled separately, that is,
 * to cancel a branch only without cancelling the whole tree.
 *
 * Using a simple copy for subordinate object is possible because
 * `std::stop_source` contains all the referential apparatus to ensure that
 * copies of a stop source refer to the same `std::stop_state`.
 */
class NewCancellationSource {
  /**
   * Cancellation state
   */
  std::stop_token stop_token_{};

 public:
  /**
   * Default constructor is deleted.
   *
   * `std::stop_token` does have a default constructor, creating an object with
   * no associated `stop_state`. In the present case it would be inimical to the
   * goals of reliable cancellation to admit the possibility that a cancellation
   * source couldn't cancel anything.
   */
  NewCancellationSource() = delete;

  /**
   * Constructor for an origin cancellation source
   *
   * This constructor uses a marker class, instead of being the default
   * constructor, in order to clearly indicate that it's an origin.
   */
  explicit NewCancellationSource(CancellationOriginT){};

  /**
   * Copy constructor
   */
  NewCancellationSource(const NewCancellationSource&) = default;

  [[nodiscard]] bool cancellation_requested() const;
};

}  // namespace tiledb::common
#endif  // TILEDB_CANCELLATION_SOURCE_H
