/**
 * @file   current_domain.h
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
 * This file declares serialization functions for array schema CurrentDomain.
 */

#ifndef TILEDB_SERIALIZATION_CURRENT_DOMAIN_H
#define TILEDB_SERIALIZATION_CURRENT_DOMAIN_H

#ifdef TILEDB_SERIALIZATION
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/enums/current_domain_type.h"
#include "tiledb/sm/serialization/capnp_utils.h"

#endif

namespace tiledb::sm {

class CurrentDomain;
class NDRectangle;
class Domain;

namespace serialization {

#ifdef TILEDB_SERIALIZATION

/**
 * Convert current domain objects to Cap'n Proto message
 *
 * @param crd The array schema current doamin
 * @param current_domain_builder cap'n proto class
 */
void current_domain_to_capnp(
    shared_ptr<CurrentDomain> crd,
    capnp::CurrentDomain::Builder& current_domain_builder);

/**
 * Deserialize a CurrentDomain from Cap'n Proto message
 *
 * @param current_domain_reader capnp reader
 * @param domain TileDB ArraySchema domain
 * @param memory_tracker TileDB memory tracker
 * @return crd deserialized array schema current domain
 */
shared_ptr<CurrentDomain> current_domain_from_capnp(
    const capnp::CurrentDomain::Reader& current_domain_reader,
    shared_ptr<Domain> domain,
    shared_ptr<MemoryTracker> memory_tracker);

/**
 * Convert NDRectangle objects to Cap'n Proto message
 *
 * @param ndr The n-dimensional rectangle
 * @param ndrectangle_builder cap'n proto class
 */
void ndrectangle_to_capnp(
    shared_ptr<NDRectangle> ndr,
    capnp::NDRectangle::Builder& ndrectangle_builder);

/**
 * Deserialize a NDRectangle from Cap'n Proto message
 *
 * @param ndrectangle_reader capnp reader
 * @param domain TileDB ArraySchema domain
 * @param memory_tracker TileDB memory tracker
 * @return ndr deserialized n-dimensional rectangle
 */
shared_ptr<NDRectangle> ndrectangle_from_capnp(
    const capnp::NDRectangle::Reader& ndrectangle_reader,
    shared_ptr<Domain> domain,
    shared_ptr<MemoryTracker> memory_tracker);

#endif

}  // namespace serialization
}  // namespace tiledb::sm

#endif  // TILEDB_SERIALIZATION_CURRENT_DOMAIN_H
