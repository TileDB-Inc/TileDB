/**
 * @file tiledb/api/c_api_support/exception_wrapper/exception_wrapper.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2023 TileDB, Inc.
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
 * This file defines templates that wrap an API function with exception
 * handlers.
 */

#ifndef TILEDB_C_API_SUPPORT_EXCEPTION_WRAPPER_H
#define TILEDB_C_API_SUPPORT_EXCEPTION_WRAPPER_H

#include <utility>                   // std::forward
#include "../argument_validation.h"  // CAPIException
#include "tiledb/api/c_api/api_external_common.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/error/error_api_internal.h"
#include "tiledb/common/exception/exception.h"

namespace tiledb::api {
namespace detail {

//-------------------------------------------------------
// Error message generation from exceptions
//-------------------------------------------------------
/**
 * A visitor for an `ErrorTree`.
 *
 * As a grammar, the visitation generates the following productions:
 *   trees : list
 *   list : full-item
 *        | list "," full-item
 *   full-item : Item [ "(" list ")" ]
 *
 * @tparam T the type representing items within the tree
 */
template <class T>
concept ErrorTreeVisitor = requires(T t, const typename T::item_type& e) {
  /*
   * The action taken when starting a new level. This action is only ever
   * immediately after an item.
   */
  t.start_level();

  /*
   * The action taken when ending a level, including the root. This action
   * always immediately follows some item.
   */
  t.end_level();

  /*
   * The action taken after one item and before another. This action always
   * immediately follows an item or an end-level.
   */
  t.separator();

  /*
   * The action taken for an item. This action always follows either a level
   * start or a separator.
   */
  t.item(e);
};

/**
 * A `std::exception`, possibly nested, treated as an error tree.
 *
 * An error tree is an interface for error messages. It takes as a template
 * argument an error type taken as a primitive. It support two kinds of compound
 * operations: sequence and nesting. Sequences support individual parallel
 * operations that might generate more than one error message. Nesting supports
 * rethrown exceptions and stack traces.
 */
class ErrorTreeStdException {
  /**
   * An exception to be visited.
   */
  const std::exception& e_;

  /**
   * Perform a complete visit of this exception as an error tree, calling the
   * visitor at each event.
   *
   * The visit is a depth-first, left-to-right traversal of the error tree. The
   * events are documented in more depth in `concept ErrorTreeVisitor`.
   *
   * @tparam V The type of the visitor
   * @param v The visitor
   * @param e The exception to be visited
   */
  template <ErrorTreeVisitor V>
  void visit_nested_exception(V& v, const std::exception& e) const noexcept {
    v.item(e);
    try {
      std::rethrow_if_nested(e);
    } catch (const std::exception& e2) {
      v.start_level();
      visit_nested_exception(v, e2);
      v.end_level();
    } catch (...) {
      v.item(std::logic_error("exception substituted for unknown type"));
    }
  }

 public:
  /**
   * Default constructor is deleted.
   */
  ErrorTreeStdException() = delete;

  /**
   * Ordinary constructor
   */
  explicit ErrorTreeStdException(const std::exception& e)
      : e_{e} {};

  /**
   * Perform a visitation with the specified visitor.
   *
   * This function is `const`. All results from the visitation are held within
   * the visitor.
   */
  template <ErrorTreeVisitor V>
  void visit(V& v) const {
    visit_nested_exception(v, e_);
  }
};

/**
 * Visitor for `ErrorTreeStdException` generates text from a possibly-nested
 * exception.
 *
 * All the visitation function have `try` blocks that suppress all exceptions.
 * The only exception anticipated is `bad_alloc`, which would not allow a string
 * lengthening to succeed regardless.
 */
class ETVisitorStdException {
  std::string s_{};

 public:
  ETVisitorStdException() = default;
  void start_level() noexcept;
  void end_level() noexcept;
  void separator() noexcept;
  using item_type = std::exception;
  void item(const item_type& e) noexcept;
  [[nodiscard]] inline std::string value() const {
    return s_;
  }
};

/**
 * Create a log message from an exception.
 */
inline std::string log_message(const std::exception& e) {
  ETVisitorStdException v{};
  ErrorTreeStdException x{e};
  x.visit(v);
  return v.value();
}

//-------------------------------------------------------
// Handlers and actions
//-------------------------------------------------------
/*
 * Responsibilities of the exception wrappers:
 * - Ensure that no exception propagates out of the C API
 * - Provide uniform treatment of errors caught at the top level
 *
 * Actions taken for each (ordinary) exception:
 * - Generate a log message from an exception object. The message generator
 *   handles nested exceptions.
 * - Log the generated message.
 * - (optional) Save the error to a context
 * - (optional) Pass the error back through an error argument
 *
 * @section Maturity Notes
 *
 * Out of memory exceptions, notably `bad_alloc`, are not at present handled
 * with an audited zero-allocation method.
 */

/**
 * Generic class for exception actions; only implemented as specializations.
 *
 * A fully instantiated class has a member function `action` that runs all the
 * `action` member functions of the actions in each class argument in sequence.
 *
 * @tparam n The number of arguments in the parameter pack A
 * @tparam A A list of action classes
 */
template <class... A>
class ExceptionActionImpl;

/**
 * The recursive case inherits from both the first element and this class whose
 * template argument has the first element removed.
 *
 * The effect of this on a fully-instantiated class is that it inherits
 * simultaneously from all its template arguments. `action...()` and
 * `validate()` are combined appropriately. Other name conflicts will lead to
 * ambiguity.
 *
 * @tparam Head The first action class
 * @tparam Tail A list of any remaining action classes
 */
template <class Head, class... Tail>
class ExceptionActionImpl<Head, Tail...> : public Head,
                                           public ExceptionActionImpl<Tail...> {
 public:
  explicit ExceptionActionImpl(Head&& head, Tail&&... tail) noexcept
      : Head(head)
      , ExceptionActionImpl<Tail...>{std::forward<Tail>(tail)...} {
  }
  /**
   * Action to take upon catching `std::exception`
   *
   * @param e An exception
   */
  inline void action(const std::exception& e) {
    Head::action(e);
    ExceptionActionImpl<Tail...>::action(e);
  }
  /**
   * Action to take when no exception was caught.
   */
  inline void action_on_success() {
    Head::action_on_success();
    ExceptionActionImpl<Tail...>::action_on_success();
  }
  /**
   * Validation action.
   */
  inline void validate() {
    Head::validate();
    ExceptionActionImpl<Tail...>::validate();
  }
};

/**
 * The trivial case with no action class arguments has an empty action.
 */
template <>
class ExceptionActionImpl<> {
 public:
  ExceptionActionImpl() = default;
  inline void action(const std::exception&) {
  }
  inline void action_on_success() {
  }
  inline void validate() {
  }
};

/*
 * In C++20, we'd define a concept for the action classes. Informally, they
 * define three functions:
 * - validate(). Checks the validity of constructor arguments.
 * - action(). Action to take on exception.
 * - action_on_success(). Action to take when there's no exception.
 *
 * Note that `validate()` looks a lot like a second-state initialization. That's
 * because it is. The action classes cannot be C.41-compliant and throw errors
 * on construction, because they're implementing the top-level exception
 * handler. If they threw errors on construction, it would be outside the
 * wrapper function and they'd propagate errors to the calling "C" application.
 * On the other hand, `validate()` is called within the wrapper.
 *
 * Even though `validate()` is called after construction, each component action
 * should know its validity at the time of construction. `validate()` should be
 * considered an opportunity to report invalidaty by throwing, since these
 * constructors cannot throw. Furthermore, if `validate()` fails, the first
 * component to fail validation will throw, so later components will not receive
 * a `validate()` call. `action()` on the composite will process the error,
 * which means `action()` on a component may be called before `validate()`.
 */

/**
 * Default action write an error to the log.
 */
class LogAction {
 public:
  LogAction() = default;
  inline void action(const std::exception& e) {
    (void)LOG_ERROR(log_message(e));
  }
  inline void action_on_success() {
  }
  inline void validate() {
  }
};

/**
 * Exception class to report that a context is invalid.
 */
class InvalidContextException : public std::runtime_error {
 public:
  explicit InvalidContextException(std::string&& message)
      : std::runtime_error(message) {
  }
};

/**
 * Actions when `Context` is present in the API function.
 *
 * @invariant valid_ if and only if ctx_ is a pointer to a valid context handle
 */
class ContextAction {
  /**
   * Context argument as passed to the API function.
   */
  tiledb_ctx_handle_t* ctx_;

  /**
   * Validity of the context
   */
  bool valid_;

 public:
  /**
   * Constructor
   *
   * @param ctx An _unvalidated_ context pointer.
   */
  explicit ContextAction(tiledb_ctx_handle_t* ctx) noexcept
      : ctx_(ctx)
      , valid_(is_handle_valid(ctx)) {
  }

  /**
   * Report a validity failure by throwing.
   */
  inline void validate() {
    if (valid_) {
      return;
    }
    /*
     * This function with throw with an explanation in the exception.
     */
    ensure_context_is_valid<InvalidContextException>(ctx_);
  }

  /**
   * Action on exception
   */
  inline void action(const std::exception& e) {
    if (!valid_) {
      // Don't even try to report our own invalidity
      return;
    }
    ctx_->context().save_error(log_message(e));
  }

  /**
   * Action on success.
   *
   * Success does not cause a context to clear its last error.
   */
  inline void action_on_success() {
  }
};

/**
 * Exception to report that an error action is invalid.
 */
class InvalidErrorException : public std::runtime_error {
 public:
  explicit InvalidErrorException(std::string&& message)
      : std::runtime_error(message) {
  }
};

/**
 * Actions when `Error` is present in the API function.
 *
 * @invariant valid_ if and only if err_ is a non-null pointer
 */
class ErrorAction {
  /**
   * Pointer to which an error handle might be written.
   */
  tiledb_error_handle_t** err_;

  /**
   * This action is valid if the error pointer is not null.
   */
  bool valid_;

 public:
  /**
   * Constructor
   *
   * @param err An _unvalidated_ error pointer.
   */
  explicit ErrorAction(tiledb_error_handle_t** err) noexcept
      : err_(err)
      , valid_(err != nullptr) {
  }

  /**
   * Validation reports that this object was constructed with a null pointer
   * argument.
   */
  inline void validate() const {
    if (!valid_) {
      throw InvalidErrorException("Error argument may not be a null pointer");
    }
  }

  /**
   * Action to report an error
   */
  inline void action(const std::exception& e) {
    if (!valid_) {
      return;
    }
    tiledb::api::create_error(err_, log_message(e));
  }

  /**
   * Action if there is no error
   */
  inline void action_on_success() {
    /*
     * No need to check validity here. `validate()` must have returned in order
     * for this function to run.
     */
    *err_ = nullptr;
  }
};

/**
 * Composite action that only logs.
 */
class ExceptionActionDetail : public ExceptionActionImpl<LogAction> {
 public:
  ExceptionActionDetail()
      : ExceptionActionImpl<LogAction>{LogAction{}} {
  }
};

/**
 * Composite action that logs and reports an error to a context
 */
class ExceptionActionDetailCtx
    : public ExceptionActionImpl<LogAction, ContextAction> {
 public:
  explicit ExceptionActionDetailCtx(tiledb_ctx_handle_t* ctx)
      : ExceptionActionImpl<LogAction, ContextAction>{
            LogAction{}, ContextAction{ctx}} {
  }
};

/**
 * Composite action that logs and reports an error through a new error handle.
 */
class ExceptionActionDetailErr
    : public ExceptionActionImpl<LogAction, ErrorAction> {
 public:
  explicit ExceptionActionDetailErr(tiledb_error_handle_t** err)
      : ExceptionActionImpl<LogAction, ErrorAction>{
            LogAction{}, ErrorAction{err}} {
  }
};

/**
 * Composite action that logs and reports an error to both a context and through
 * a new error handle.
 *
 * We don't have any API functions that require this at the present time, but
 * this class is used in testing to validate that chained action objects work
 * correctly in all circumstances.
 */
class ExceptionActionDetailCtxErr
    : public ExceptionActionImpl<LogAction, ContextAction, ErrorAction> {
 public:
  ExceptionActionDetailCtxErr(
      tiledb_ctx_handle_t* ctx, tiledb_error_handle_t** err)
      : ExceptionActionImpl<LogAction, ContextAction, ErrorAction>{
            LogAction{}, ContextAction{ctx}, ErrorAction{err}} {
  }
};

}  // namespace detail

using ExceptionAction = detail::ExceptionActionDetail;
using ExceptionActionCtx = detail::ExceptionActionDetailCtx;
using ExceptionActionErr = detail::ExceptionActionDetailErr;
using ExceptionActionCtxErr = detail::ExceptionActionDetailCtxErr;

//-------------------------------------------------------
// Exception wrapper
//-------------------------------------------------------
/**
 * Null aspect for `class CAPIFunction` has null operations for all aspects.
 * @tparam f
 */
template <auto f>
class CAPIFunctionNullAspect {
 public:
  template <typename... Args>
  static void call(Args...) {
  }
};

/**
 * Selection struct defines the default aspect type for CAPIFunction. This class
 * is always used with second template argument as `void`. This definition is
 * for the general case; a specialization can override it.
 */
template <auto f, typename>
struct CAPIFunctionSelector {
  using aspect_type = CAPIFunctionNullAspect<f>;
};

/**
 * Non-specialized wrapper for implementations functions for the C API. May
 * only be used as a specialization.
 */
template <
    auto f,
    class H,
    class A = typename CAPIFunctionSelector<f, void>::aspect_type>
class CAPIFunction;

/**
 * Wrapper for implementations functions for the C API
 */
template <class R, class... Args, R (*f)(Args...), class H, class A>
class CAPIFunction<f, H, A> {
 public:
  /**
   * Forwarded alias to template parameter H.
   */
  using handler_type = H;

  /**
   * The wrapper function
   *
   * @param h An error handler
   * @param args Arguments to an API implementation function
   * @return
   */
  static R function(H& h, Args... args) {
    /*
     * The order of the catch blocks is not arbitrary:
     * - `std::bad_alloc` comes first because it overrides other problems
     * - `InvalidContextException` and `InvalidErrorException` come next,
     *    because they have return codes that override the generic `TILEDB_ERR`
     * - `StatusException` is derived from `std::exception`, so it must precede
     *   it in order to be caught separately
     * - `std::exception` is for all other expected exceptions
     * - `...` is only for ultimate exception safety. This catch block should
     *   never execute.
     */
    try {
      /*
       * If error-handling arguments are invalid, `validate` will throw and the
       * underlying function will not execute.
       */
      h.validate();
      /*
       * Note that we don't need std::forward here because all the arguments
       * must have "C" linkage.
       */
      A::call(args...);
      if constexpr (std::same_as<R, void>) {
        f(args...);
        h.action_on_success();
      } else {
        auto x{f(args...)};
        h.action_on_success();
        return x;
      }
    } catch (const std::bad_alloc& e) {
      h.action(e);
      if constexpr (!std::same_as<R, void>) {
        return TILEDB_OOM;
      }
    } catch (const detail::InvalidContextException& e) {
      h.action(e);
      if constexpr (!std::same_as<R, void>) {
        return TILEDB_INVALID_CONTEXT;
      }
    } catch (const BudgetUnavailable& e) {
      h.action(e);
      if constexpr (!std::same_as<R, void>) {
        return TILEDB_BUDGET_UNAVAILABLE;
      }
    } catch (const detail::InvalidErrorException& e) {
      h.action(e);
      if constexpr (!std::same_as<R, void>) {
        return TILEDB_INVALID_ERROR;
      }
    } catch (const StatusException& e) {
      h.action(e);
      if constexpr (!std::same_as<R, void>) {
        return TILEDB_ERR;
      }
    } catch (const std::exception& e) {
      h.action(e);
      if constexpr (!std::same_as<R, void>) {
        return TILEDB_ERR;
      }
    } catch (...) {
      h.action(CAPIException("unknown exception type; no further information"));
      if constexpr (!std::same_as<R, void>) {
        return TILEDB_ERR;
      }
    }
  };

  /**
   * The plain wrapper function.
   *
   * @param args Arguments to an API implementation function
   * @return The return value of the call to the implementation function.
   */
  inline static capi_return_t function_plain(Args... args) {
    ExceptionAction action{};
    return function(action, args...);
  }

  /**
   * The wrapper function returning void. It's the same as the full wrapper
   * function but suppresses the return value.
   *
   * @param args Arguments to an API implementation function
   */
  inline static void void_function(Args... args) {
    ExceptionAction action{};
    (void)function(action, args...);
  };

  /**
   * Wrapper function for interface functions with an error argument but whose
   * corresponding implementation functions do not need one.
   *
   * @param ctx Object to receive any error message
   * @param args Arguments forwarded from API function
   * @return The return value of the call to the implementation function.
   */
  inline static capi_return_t function_context(
      tiledb_ctx_handle_t* ctx, Args... args) {
    tiledb::api::ExceptionActionCtx action{ctx};
    return CAPIFunction<f, ExceptionActionCtx>::function(action, args...);
  }

  /**
   * Wrapper function for interface functions with an error argument but whose
   * corresponding implementation functions do not need one.
   *
   * @param error Object to receive any error message
   * @param args Arguments forwarded from API function
   * @return The return value of the call to the implementation function.
   */
  inline static capi_return_t function_error(
      tiledb_error_handle_t** error, Args... args) {
    ExceptionActionErr action{error};
    return function(action, args...);
  }
};

//-------------------------------------------------------
// Exception-wrapping function transformers
//-------------------------------------------------------
/*
 * `class CAPIFunction` is the foundation for a set of function transformers
 * that convert API implementation functions into API interface functions. We
 * have five such function transformers:
 * - api_entry_plain: Just does the transformation and nothing else.
 * - api_entry_void: Similar to _plain, it removes the return value.
 * - api_entry_with_context: Uses an initial context argument to return error
 *     messages.
 * - api_entry_context: Adds a context argument to those of the implementation
 *     function and returns error messages through it.
 * - api_entry_error: Adds an error argument to those of the implementation
 *     function and returns error messages through it.
 *
 * `api_entry_with_context` is primarily used to wrap old code that still does
 * manual error returns through its context argument. It's usually the case that
 * the only use of a context argument is for error returns. In such cases the
 * implementation function can be rewritten without it and the wrapper changed
 * to `api_entry_context`.
 */

/**
 * Plain API function transformer keeps the signature intact.
 *
 * @tparam f An API implementation function
 */
template <auto f>
constexpr auto api_entry_plain =
    CAPIFunction<f, ExceptionAction>::function_plain;

/**
 * Function transformer changes an API implementation function with `void`
 * return to an API interface function, also with `void` return.
 *
 * @tparam f An implementation function.
 */
template <auto f>
constexpr auto api_entry_void =
    CAPIFunction<f, tiledb::api::ExceptionAction>::void_function;

/**
 * Declaration only defined through a specialization.
 *
 * @tparam f An API implementation function
 */
template <auto f>
struct CAPIFunctionContext;

/**
 * Wrapper class for API implementation functions with a leading context
 * argument.
 *
 * @tparam Args Arguments of the implementation following the initial context
 * @tparam f An API implementation function
 */
template <class... Args, capi_return_t (*f)(tiledb_ctx_handle_t*, Args...)>
struct CAPIFunctionContext<f> : CAPIFunction<f, ExceptionActionCtx> {
  inline static capi_return_t function_with_context(
      tiledb_ctx_handle_t* ctx, Args... args) {
    tiledb::api::ExceptionActionCtx action{ctx};
    return CAPIFunction<f, ExceptionActionCtx>::function(action, ctx, args...);
  }
};

/**
 * API function transformer changes an implementation function with a context as
 * its first argument into an API function with the same signature.
 *
 * @tparam f An API implementation function
 */
template <auto f>
constexpr auto api_entry_with_context =
    CAPIFunctionContext<f>::function_with_context;

/**
 * API function transformer changes an implementation function without a context
 * argument to an API interface function with such an argument prepended.
 *
 * @tparam f An API implementation function
 */
template <auto f>
constexpr auto api_entry_context =
    CAPIFunction<f, ExceptionActionErr>::function_context;

/**
 * API function transformer changes an implementation function without an error
 * argument to an API interface function with such an argument prepended.
 *
 * @tparam f An API implementation function
 */
template <auto f>
constexpr auto api_entry_error =
    CAPIFunction<f, ExceptionActionErr>::function_error;

}  // namespace tiledb::api

#endif  // TILEDB_C_API_SUPPORT_EXCEPTION_WRAPPER_H
