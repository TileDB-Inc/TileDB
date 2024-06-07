/**
 * @file   rest_client.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2024 TileDB, Inc.
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
 * This file defines a pair of REST client classes: one disabled from
 * interacting with a REST server and one enabled to. Construction of
 * `RestClient` instances is available only through a factory.  Only one of
 * these classes is available to be constructed, depending on the build
 * configuration specified.
 *
 * @section Cyclic build dependency
 *
 * `class RestClient` is the nexus of a serious class of cyclic dependencies in
 * the code. These cycles are a hard blocker to complete the agenda of
 * converting the library to a hierarchy (i.e. DAG) of object libraries.
 *
 * The origin of the cycles is the commingling of REST operations with direct
 * operations in the code of fundamental classes such as `Array`, `Query`, and
 * `Group`. The skeleton of the dependency looks as follows; `Array::open` is
 * the example.
 *
 *   - `Array::Array` is created with a URI. That URI can be either of two
 *     possibilities. The member variable `remote_` is initialized accordingly.
 *       - Direct URI to an object store or file system. (`remote_ == false`)
 *       - REST URI that begins with `tiledb:`. (`remote_ == true`)
 *   - Calling `Array::open` implements two different ways of opening the file,
 *     depending on whether it's remote or not.
 *       - Direct (non-remote) opening does not use `RestClient` in any way.
 *         This operation does not create any cycles.
 *       - Remote opening always uses `RestClient`, calling
 *         `get_array_schema_from_rest` and `post_array_from_rest`.
 *   - As a particular example, `post_array_from_rest` calls
 *     `array_deserialize`. (All `RestClient` operations use capnp
 *     serialization functions analogously.)
 *   - `array_deserialize` calls the `Array` constructor.
 *
 * Compacting and summarizing the call sequence:
 *   - `Array` depends on `RestClient` because of `Array::open` uses
 *     `RestClient::post_array_from_rest`.
 *   - `RestClient` depends on `Array` because of
 *     `RestClient::post_array_from_rest` uses `Array::Array`.
 *
 * The root cause of the cycle is that the `Array` constructor commingles both
 * direct and remote URIs into the same argument. As long as this persists,
 * there's a necessary cycle. This example is a specific example of a widespread
 * and generic pattern in the use of `RestClient`.
 *
 * The minimum requirement is that there be two separate constructors and that
 * they be defined in different translation units (so that they don't end up in
 * the same object file). These two constructors might both be on an existing
 * class itself, possibly, or they might be on different classes. Regardless of
 * the approach, the elimination of these cycles is a long term project.
 *
 * @section Dependency on TILEDB_SERIALIZATION
 *
 * The REST client has two different behaviors depending on the build
 * configuration for serialization.
 *   - If serialization is off, the REST client is a stub, throwing for all
 *     remote operations.
 *   - If serialization is on, the REST client performs remote operations
 *     with a server.
 *
 * The earliest way this polymorphism was accomplished was with two different
 * versions of the class, both named `class RestClient`, selected with the
 * preprocessor. This was a distinctly C-style way of programming with a number
 * of disadvantages. (All these are corollaries of the general maxim that The
 * Preprocessor is Evil).
 *   - Only a single configuration could be compiled in a build tree at a time.
 *   - It was impossible to test both kinds of REST client with any single
 *     configuration.
 *   - All object libraries with any dependence on `class RestClient` came in
 *     two different versions, but with the same name.
 *
 * In order to improve the situation, we have the following design goals:
 *   - No use of the preprocessor in C++ code involving the REST client.
 *   - For all code that is not involved in implementing the REST client, there
 *     must be a single object library that applies to both configurations.
 *   - When possible, all code involving implementing the server-enabled REST
 *     client should be segregated into separate compilation units. This cannot
 *     be a hard requirement until after all the dependency cycles are broken.
 *   - When it's necessary that there be different object libraries that depend
 *     upon the configuration, they must be have distinct names. The build
 *     configuration variable may only be used to define how these object
 *     libraries are used, and not how they are defined.
 *   - Only C++ language mechanisms may be used to select configurations within
 *     C++ code. These include the following:
 *     - Class inheritance and virtual functions.
 *     - Template instantiations. In particular, certain classes might usefully
 *       be compiled with a boolean template argument for serialization.
 *     - Link specifications.
 *
 * @section Alleviating trouble with `RestClient`
 *
 * Summarizing, we have two major issues with `RestClient`: dependency cycles
 * and multiple configurations. There's an overlap between approach to address
 * these issues, but there's one dominant principle that applies: Minimize the
 * dependency of other code on `class RestClient`. This translates into a tiered
 * approach:
 *   - Almost all code references the REST client through a base class and its
 *     virtual functions. This class does not depend upon the build
 *     configuration in any way. As a consequence, only a small amount of code
 *     has any involvement with this configuration variable.
 *   - Construction of REST client objects occurs only through a factory
 *     function. The factory function itself is necessarily involved with the
 *     the build configuration, but also isolates outside code from how the
 *     factory works.
 *     - The only class that constructs REST client objects is `class Context`.
 *       The factory eliminates a potential build dependency for this class.
 *   - The factory perforce must behave in two modes: server-disabled and
 *     server-enabled. Using two different classes for these two modes would
 *     incur duplication in object libraries. Instead, the factory initializes
 *     itself according the link specification.
 *     - The constructor for server-disabled clients is always present. If
 *       it's the only one present, that's what the factory returns.
 *     - The constructor for server-enabled clients is optional; it may be
 *       linked in or not. If it's linked it, the factory returns server-enabled
 *       clients.
 *
 * This technical design makes a tradeoff. It incurs overhead in a few ways, all
 * of which are neglible in practice.
 * - There are virtual function calls for all REST client operations. Given that
 *   each such operation performs I/O, the call overhead in minuscule. Put
 *   differently, tight inner loops do not call these functions.
 * - The code for the server-disabled REST client is present in the binary even
 *   when the factory is producing server-enabled clients. The excess code is a
 *   couple dozen stubs, all of which throw the same exception. The object code
 *   overhead is less than a kilobyte.
 * - The factory and its initialization have a layer of internal indirection.
 *   Given that the only time REST clients are constructed is one per `class
 *   Context`, this overhead is even less than that for its virtual functions.
 *
 * @section Upshot for users
 *
 * The resulting system is very easy to use:
 * - If you want an executable with server-disabled objects, just link the code
 *   you ordinarily would. Ensure your link specification does not inadvertently
 *   include `rest_client_remote`.
 * - If you want an executable with server-enabled objects, link it with the
 *   object library `rest_client_remote`. The presence of this object library
 *   changes the initialization of the factory from the default.
 *
 */

#ifndef TILEDB_REST_CLIENT_H
#define TILEDB_REST_CLIENT_H

#include <string>
#include <unordered_map>

#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/group/group.h"
#include "tiledb/sm/serialization/query.h"
#include "tiledb/sm/stats/stats.h"

using namespace tiledb::common;

namespace tiledb::sm {

/**
 * Exception class with `RestClient` origin
 */
class RestClientException : public StatusException {
 public:
  explicit RestClientException(const std::string& message)
      : StatusException("RestClient", message) {
  }
};

/**
 * Exception class for operations disabled in the base class
 */
class RestClientDisabledException : public RestClientException {
 public:
  explicit RestClientDisabledException()
      : RestClientException(
            "Cannot use rest client; serialization not enabled.") {
  }
};

/*
 * Forward declarations required for `RestClient` member function declarations
 */
class ArraySchema;
class ArraySchemaEvolution;
class Config;
class FragmentInfo;
class MemoryTracker;
class Query;
class QueryPlan;

// Forward for friend declaration within `class RestClient`
class RestClientFactory;

/**
 * Base class for REST client is also the server-disabled class.
 *
 * There are two REST client classes:
 * 1. This one fails all operations. This is the "server-disabled" or "local"
 *    version.
 * 2. The other one interacts with a server to perform operations. That is the
 *    "server-enabled" or "remote" version.
 * The "local" vs. "remote" distinction is about where the processing happens,
 * not where the storage is located. Their full names might be "locally-
 * processed" vs. "remotely-processed", but that's not what they're called.
 *
 * This class (the local one) is the base class of the other (remote) one. As a
 * result, this class does not have pure virtual methods. All virtual member
 * functions defined in the base are implemented. They are, however, implemented
 * as stubs that do nothing but throw.
 *
 * One facility is present in the base class: the extra headers. This implements
 * the C API function `tiledb_ctx_set_tag`. In production code, this data is
 * only used to additional HTTP headers. There are additional uses, however,
 * including test code and context tracking. This facility is in the base class
 * so that it's available regardless of the build configuration.
 */
class RestClient {
  /*
   * The factory is the only way to get to the constructor of this class.
   */
  friend class RestClientFactory;

 protected:
  /**
   * The set of extra headers that are attached to REST requests. This is the
   * data structure where the arguments to `tiledb_ctx_set_tag` end up.
   */
  std::unordered_map<std::string, std::string> extra_headers_;

  /**
   * Name of the rest server used for remote operations.
   */
  std::string rest_server_;

 public:
  RestClient(const Config& config);

  virtual ~RestClient() = default;

  /**
   * Add a header that will be attached to all requests.
   *
   * This member function supports `tiledb_ctx_set_tag`. Although the designated
   * use of this function is for remote REST operations, it needs to be
   * available in both local and remote configurations for testing.
   */
  void add_header(const std::string& name, const std::string& value);

  /**
   * Constant accessor to `extra_headers_` member variable.
   *
   * This function supports testing the C API function `tiledb_ctx_set_tag`,
   * which is write-only with respect to the C API.
   */
  const std::unordered_map<std::string, std::string>& extra_headers() const {
    return extra_headers_;
  }

  /**
   * Accessor to configured rest server
   *
   * @section Maturity
   *
   * This function only make sense for remote operation. It's in the base class
   * at present to avoid rewriting tests.
   */
  inline std::string rest_server() const {
    return rest_server_;
  }

  //-------------------------------------------------------
  // Rest client operations
  //-------------------------------------------------------

  /// Operation disabled in base class.
  inline virtual tuple<Status, std::optional<bool>>
  check_array_exists_from_rest(const URI&) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual tuple<Status, std::optional<bool>>
  check_group_exists_from_rest(const URI&) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual tuple<Status, optional<shared_ptr<ArraySchema>>>
  get_array_schema_from_rest(const URI&) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual std::tuple<
      shared_ptr<ArraySchema>,
      std::unordered_map<std::string, shared_ptr<ArraySchema>>>
  post_array_schema_from_rest(
      const Config&, const URI&, uint64_t, uint64_t, bool) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual void post_array_from_rest(
      const URI&, ContextResources&, Array*) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status post_array_schema_to_rest(
      const URI&, const ArraySchema&) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual void delete_array_from_rest(const URI&) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual void post_delete_fragments_to_rest(
      const URI&, Array*, uint64_t, uint64_t) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual void post_delete_fragments_list_to_rest(
      const URI&, Array*, const std::vector<URI>&) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status deregister_array_from_rest(const URI&) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status get_array_non_empty_domain(Array*, uint64_t, uint64_t) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status get_array_max_buffer_sizes(
      const URI&,
      const ArraySchema&,
      const void*,
      std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status get_array_metadata_from_rest(
      const URI&, uint64_t, uint64_t, Array*) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status post_array_metadata_to_rest(
      const URI&, uint64_t, uint64_t, Array*) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual std::vector<shared_ptr<const Enumeration>>
  post_enumerations_from_rest(
      const URI&,
      uint64_t,
      uint64_t,
      Array*,
      const std::vector<std::string>&,
      shared_ptr<MemoryTracker>) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual void post_query_plan_from_rest(
      const URI&, Query&, QueryPlan&) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status submit_query_to_rest(const URI&, Query*) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status finalize_query_to_rest(const URI&, Query*) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status submit_and_finalize_query_to_rest(const URI&, Query*) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status get_query_est_result_sizes(const URI&, Query*) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status post_array_schema_evolution_to_rest(
      const URI&, ArraySchemaEvolution*) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status post_fragment_info_from_rest(
      const URI&, FragmentInfo*) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status post_group_metadata_from_rest(const URI&, Group*) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status put_group_metadata_to_rest(const URI&, Group*) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status post_group_from_rest(const URI&, Group*) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status patch_group_to_rest(const URI&, Group*) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual void delete_group_from_rest(const URI&, bool) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status post_group_create_to_rest(const URI&, Group*) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status post_consolidation_to_rest(const URI&, const Config&, const std::vector<std::string>*) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual Status post_vacuum_to_rest(const URI&, const Config&) {
    throw RestClientDisabledException();
  }

  /// Operation disabled in base class.
  inline virtual std::vector<std::vector<std::string>>
  post_consolidation_plan_from_rest(const URI&, const Config&, uint64_t) {
    throw RestClientDisabledException();
  }
};

// Forward declaration.
class RestClientFactoryAssistant;

/**
 * Factory class for `RestClient`.
 */
class RestClientFactory {
  /*
   * Friend declaration allows override of the default factory behavior. The
   * assistant has disciplined interaction with the internals of this class,
   * unlike arbitrary friends might be.
   */
  friend class RestClientFactoryAssistant;

  /**
   * Alias for the function type of the factory.
   */
  using factory_type = std::shared_ptr<RestClient>(
      stats::Stats& parent_stats,
      const Config& config,
      ThreadPool& compute_tp,
      Logger& logger,
      shared_ptr<MemoryTracker>&& tracker);

  /**
   *
   */
  static factory_type* factory_override_;

 public:
  /**
   * Factory for `RestClient` objects
   *
   * @section Maturity
   *
   * The construction arguments are all the elements of `ContextResources`
   * except one: the `RestClient` instance. The `RestClient` is current part
   * of the context resources rather than part of the context itself. Once
   * the member variable moves from `ContextResources` to `Context`, the
   * factory can simply take a single `ContextResources` object.
   *
   * @param parent_stats The stats object from `ContextResources`
   * @param config  The config from `ContextResources`
   * @param compute_tp The compute thread pool from `ContextResources`
   * @param logger The logger from `ContextResources`
   * @param tracker The memory tracker from `ContextResources`.
   * @return a newly constructed instance
   */
  static std::shared_ptr<RestClient> make(
      stats::Stats& parent_stats,
      const Config& config,
      ThreadPool& compute_tp,
      Logger& logger,
      shared_ptr<MemoryTracker>&& tracker);
};

/*
 * Forward declaration of the remote class.
 *
 * This declaration is necessary for a friend declaration in the factory. For
 * the most part we want all references to the REST client to be through the
 * base class and not refer to the remote class specifically.
 */
class RestClientRemoteFactory;

/**
 * Assistant to the factory provides a limited interface to the internals of
 * the factory.
 *
 * This class has no public members. Its functions are only available to
 * friends.
 */
class RestClientFactoryAssistant {
  friend class RestClientRemoteFactory;
  using factory_type = RestClientFactory::factory_type;

  static factory_type* override_factory(factory_type* f);
};

}  // namespace tiledb::sm

#endif  // TILEDB_REST_CLIENT_H
