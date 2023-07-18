/**
 * @file flow_graph/graph_type.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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

#ifndef TILEDB_FLOW_GRAPH_GRAPH_TYPE_H
#define TILEDB_FLOW_GRAPH_GRAPH_TYPE_H

#include <tuple>

namespace tiledb::flow_graph {

//-------------------------------------------------------
/*
 * Product type support
 *
 * C++ has something of a product type in `std::tuple` and the beginnings of
 * product type support with `std::apply`. These mechanisms are not part of a
 * unified approach to dealing with product types. The code here moves in that
 * direction, however modest and incomplete the effort.
 *
 * We take `std::tuple` as a canonical product type, but not the only one. We
 * want every product type to be tuple-like, however, in the sense that we
 * require it be a valid argument to `std::apply`.
 *
 * We also require that product types have a fixed number of factors. An
 * alternative would allow a variable number of factors, such as `vector<any>`.
 * In other words, the product types here are definite products rather than
 * indefinite products. This choice rests on the C++ type system, which can only
 * deal with variable-length lists of types at compile type, not at run-time.
 * There is no run-time representation of a type, unlike, say, languages based
 * on a virtual machine.
 */
//-------------------------------------------------------

/**
 * A type is applicable if it may usefully appear as the second argument in
 * `std::apply`. The documentation for `std::apply` calls this "tuple-like".
 *
 * A class is tuple-like if it supports `std::get` and `std::tuple_size`. The
 * phrase "usefully appear" is key, as every class may appear as the second
 * argument of `apply`, but if the class is not tuple-like, `apply` will invoke
 * its function with exactly one argument.
 *
 * @tparam T a candidate tuple-like type
 */
template <class T>
concept applicable = requires(T) {
  []<size_t n>(T& x) { std::get<n>(x); };
  { std::tuple_size_v<T> } -> std::convertible_to<size_t>;
};

/**
 * Concept that `T` is instantiated from `std::tuple`.
 *
 * @tparam T A possible `std::tuple` class
 */
template <class T>
concept is_tuple = requires(T x) { []<class... U>(std::tuple<U...>) {}(x); };

/**
 * Concept that a type declares a tuple type.
 *
 * @tparam T A possible product type
 */
template <class T>
concept declares_tuple_traits = requires { typename T::type_tuple; };

/**
 * Traits class for product types.
 *
 * Note that the traits class is defined only for classes meeting the conceptual
 * criteria. Independent specializations are not supported at this time.
 *
 * @tparam T
 */
template <class T>
  requires is_tuple<T> || declares_tuple_traits<T>
struct product_traits;

/**
 * Traits class for product types that are not `std::tuple`
 *
 * @tparam T A possible product type
 */
template <declares_tuple_traits T>
struct product_traits<T> {
  using type_tuple = T::type_tuple;
};

/**
 * Traits class for `std::tuple` as a product type.
 *
 * @tparam X parameter pack for matching `std::tuple` template arguments
 */
template <class... X>
struct product_traits<std::tuple<X...>> {
  using type_tuple = std::tuple<X...>;
  static_assert(is_tuple<std::tuple<X...>>);
};

/**
 * Concept that a type has a `type_tuple` trait.
 *
 * @tparam T A possible product type
 */
template <class T>
concept has_product_traits = requires {
  // At this time `type_tuple` is the only trait.
  typename product_traits<std::remove_cv_t<T>>::type_tuple;
};

/**
 * A type is a product type if it may usefully appear as the second argument in
 * `std::apply` and if we can obtain a tuple of its factor types.
 *
 * @tparam T a candidate product type
 */
template <class T>
concept product_type = applicable<T> && has_product_traits<T>;

//----------------------------------
// `sizeof` for product types
//----------------------------------

/**
 * The number of factors in a product type.
 *
 * @tparam T a product type
 * @return number of factors of 'T'
 */
template <product_type T>
constexpr size_t factor_sizeof() {
  return std::tuple_size_v<T>;
}

/**
 * The number of factors in a product type.
 *
 * This function takes an argument, but only to infer the type from it. The
 * number of factors is defined by the type itself. All objects of this type
 * have the same number of factors.
 *
 * @tparam T a product type
 * @return number of factors of 'T'
 */
template <product_type T>
constexpr size_t factor_sizeof(const T&) {
  return std::tuple_size_v<T>;
}

/**
 * Concept that a product type has no factors.
 *
 * @tparam T a product type
 */
template <class T>
concept empty_product_type = product_type<T> && (factor_sizeof<T>() == 0);

//----------------------------------
//----------------------------------

/**
 * Predicate that a type is product type whose members are all lvalue
 * references.
 */
template <class T>
concept contains_lvalue_references = product_type<T> && requires(T y) {
  std::apply(
      []<class... U>(U...) { return (std::is_lvalue_reference_v<U> && ...); },
      y);
};

/**
 * Predicate that the type of an object passed as an argument is an applicable
 * type whose members are all lvalue references.
 */
template <product_type TT>
constexpr bool contains_lvalue_references_v(const TT& x) {
  struct wrapper {
    static constexpr bool value(const TT&) {
      return contains_lvalue_references<TT>;
    }
  };
  return wrapper::value(std::forward<const TT&>(x));
}

//-------------------------------------------------------
// invocation over a product
//-------------------------------------------------------
/*
 * Invoking a function over a product means invoking a function on each factor
 * of the product and then doing something with the results. Both these aspects,
 * the multiple invocations and the results, have their own kinds of variation.
 *
 * As notation, suppose a product type `TT`, a value `xx` of that type, and a
 * function `F`. From this, suppose that `TT` has factor types `T_1, ..., T_n,`,
 * that `xx` is equivalent to `{x_1, ..., x_n}`, that the calls are captured as
 * (named) temporary variables `y_1, ..., y_n`, that functions `F_1, ..., F_n`
 * are called, and the types of the return values are `R_1, ..., R_n`.
 * ```
 * // for i from 1 to n
 * R_i y_i = F_i(x_i);
 * ```
 *
 * The first variation is with the multiple functions `F_i` invoked and their
 * relation to `F`. The essence of the variation is that each invocation has
 * a different argument type `T_i`. That type ultimately appears somewhere as
 * a template argument, both the kind of template may vary.
 *
 * As a first hypothetical, suppose that `T_i` is an argument of a function
 * template.
 * ```
 * template <class T_i>
 * R_i F_i<T_i&&>;
 * ```
 * As of C++20, function class templates may not be passed as template
 * arguments, so we can't support this kind of function at this time. Thus we
 * may assume that `F` is always a class. Then `T_i` is a template argument
 * either of the class or one of its member functions. For definiteness, we'll
 * assume that the function is always `operator()`. (It would be possible to
 * have class with a factory returning pointers to functions or pointers to
 * function objects; we do not consider them here. Such a thing would be best
 * considered as a pointer to a callable thing rather than a callable thing
 * itself.) Henceforth we may assume that `T_i` is always an argument within
 * the context of some class.
 *
 * As a second hypothetical, suppose that `T_i` is  an argument on the class `F`
 * itself.
 * ```
 *   template<class T>
 *   struct F {
 *     R operator()(T&&);
 *   };
 * ```
 * The problem with this is that the standard library operates entirely on
 * function objects, not on function classes. Although it's possible to invoke
 * these, the design decision for the standard library is not to use them.
 * Therefore we won't either.
 *
 * As a side note, the standard library always uses `class F` to declare
 * function classes. If the template argument were on the class, it would have
 * to be declared `template<class> class F`. The syntax of C++ does not allow a
 * template argument to be either `class` or `template<class> class`. To do
 * otherwise would require duplication of all the apparatus for a second kind
 * of function class or changes to the language.
 *
 * The third case is not hypothetical. Here `T_i` is deduced from an object
 * parameter.
 * ```
 *   struct F {
 *     template<class T>
 *     R operator()(T&&);             // required in C++20
 *     // template<class T>
 *     // static R operator()(T&&);   // allowed in C++23
 *   };
 * ```
 * Invocation with this definition requires an object, so there are no
 * particular construction requirements (although of course it must be
 * constructible somehow).
 *
 * There is far too much variation for a single apply-to-product function to
 * make sense. In all cases, the types `R_i` of return values may depend on the
 * type argument `T_i`. `F` might return a product of `R_i` or it might combine
 * them in some way. If all the `R_i` are the same, or at least compatible, a
 * fold expression might be computed. And there's also the question about how
 * many to compute. Boolean folds with `||` and `&&` have short-circuit
 * behavior.
 */

/**
 * `TT` is a product type and `F` is object-invocable on each element of `TT`,
 * returning `R`.
 *
 * Object-invocable means that `operator()` is parametric on the member
 * function, meaning that `operator()` is a function template. The type of the
 * argument of `operator()` comes from the function template argument.
 *
 * @tparam R
 * @tparam F
 * @tparam TT
 */
template <class F, class TT>
concept invocable_over_product = product_type<TT> && requires(TT xx) {
  std::apply([]<class... T>(T...) { (std::is_invocable_v<F, T> && ...); }, xx);
};

/**
 * `TT` is a product type and `F` is object-invocable on each element of `TT`,
 * returning `R`.
 *
 * Object-invocable means that `operator()` is parametric on the member
 * function, meaning that `operator()` is a function template. The type of the
 * argument of `operator()` comes from the function template argument.
 *
 * @tparam R
 * @tparam F
 * @tparam TT
 */
template <class R, class F, class TT>
concept invocable_over_product_r = product_type<TT> && requires(TT xx) {
  std::apply(
      []<class... T>(T...) { (std::is_invocable_r_v<R, F, T> && ...); }, xx);
};

/**
 * `TT` is a product type and `F` is invocable on each element of `TT`,
 * returning `bool`.
 *
 * @tparam F
 * @tparam TT
 */
template <class F, class TT>
concept predicate_over_product = invocable_over_product_r<bool, F, TT>;

/*
 * Note that we do not have a single "invoke" function in parallel with the
 * "invocable" concept `invocable_over_product_r`. Defining a single function
 * would require a way to specify the fold expression that combines the results
 * of individual invocations with each other.
 * ```
 * template <class R, template <class> typename F, class TT, ??? Fold>
 * constexpr R invoke_over_product_r
 * ```
 * Instead of a single invocation function, we define the most common ones.
 */

template <class F, product_type TT>
  requires predicate_over_product<F, TT>
constexpr bool invoke_over_product_and(F&& f, const TT& xx) {
  return std::apply(
      [&f]<class... T>(T&&... x) -> bool {
        return (
            (std::invoke(std::forward<F&&>(f), std::forward<T&&>(x))) && ...);
      },
      std::forward<const TT&>(xx));
};

template <class F, product_type TT>
  requires predicate_over_product<F, TT>
constexpr bool invoke_over_product_or(F&& f, const TT& xx) {
  return std::apply(
      [&f]<class... T>(T&&... x) -> bool {
        return (
            (std::invoke(std::forward<F&&>(f), std::forward<T&&>(x))) || ...);
      },
      std::forward<const TT&>(xx));
};

//-------------------------------------------------------
// reference equality
//-------------------------------------------------------
/**
 * Heterogeneous equality operation
 *
 * Two elements are always unequal if they are of different types. If they're
 * of the same type, we defer to the equality operator on that type. If
 * there's no equality operator for that type, we return false.
 *
 * @tparam T type of left-hand operand
 * @tparam U type of right-hand operand
 * @param x value of left-hand operand
 * @param y value of right-hand operand
 */
template <class T, class U>
constexpr bool is_equal_reference(const T& x, const U& y) {
  if constexpr (!(std::is_same_v<std::remove_cv_t<T>, std::remove_cv_t<U>>)) {
    return false;
  } else {
    return std::addressof(x) == std::addressof(y);
  }
}

/**
 * Bound version of `is_equal_reference`; one argument is bound at construction.
 *
 * @tparam T type of left-hand operand
 */
template <class T>
class is_equal_as_reference_to {
  /**
   * Left-hand operand
   */
  const T& x_;

 public:
  /**
   * Ordinary constructor takes one operand of the equality operator.
   *
   * @param x value of left-hand operand
   */
  constexpr explicit is_equal_as_reference_to(const T& x)
      : x_{x} {};

  /**
   * Call operator takes the other operand of the equality operator.
   *
   * @tparam U type of right-hand operand
   * @param y value of right-hand operand
   * @return whether constructor argument `x` and this argument `y` are equal
   *   as references
   */
  template <class U>
  constexpr bool operator()(const U& y) {
    return is_equal_reference(x_, std::forward<const U&>(y));
  }
};

/**
 * List membership for a reference within a list of references
 *
 * @tparam T Base type of the reference argument `x`
 * @tparam UU A tuple-like type holding references
 * @param x Reference to an object
 * @param yy A list of references
 * @return `true` if and only if the reference `x` is equal to some element in
 *   the list `yy`
 */
template <class T, product_type UU>
constexpr bool is_reference_element_of(const T& x, const UU& yy) {
  return invoke_over_product_or(
      is_equal_as_reference_to{std::forward<const T&>(x)},
      std::forward<const UU&>(yy));
}

//-------------------------------------------------------
// `count_until_satisfied`
//-------------------------------------------------------

/**
 * Evaluates a predicate on each argument until the predicate is satisfied.
 * Returns the number of times the predicate was evaluated and not satisfied.
 *
 * The two extremal cases are worth noting. If first argument in the parameter
 * pack satisfies the predicate, this function returns zero. If no argument in
 * the pack satisfies the predicate, this function returns the number of
 * arguments in the pack.
 *
 * @tparam F The underlying type of a predicate object
 * @tparam T A pack of types for the arguments
 * @param f A predicate object
 * @param x A pack of arguments
 * @return an integer in the range [0, factor_sizeof(xx)]
 */
template <class F, class... T>
constexpr size_t count_until_satisfied(F&& f, T&&... x) {
  if constexpr (sizeof...(T) == 0) {
    /*
     * End recursion when parameter pack is empty.
     */
    return 0;
  } else {
    return [&f]<class U, class... V>(U&& y, V&&... z) -> size_t {
      if (std::invoke(std::forward<F&&>(f), std::forward<U&&>(y))) {
        /*
         * Short-circuit when the invocation is true.
         */
        return 0;
      } else {
        /*
         * Invoke recursively when the invocation is false.
         */
        return 1 + count_until_satisfied(
                       std::forward<F&&>(f), std::forward<V&&>(z)...);
      }
    }(std::forward<T&&>(x)...);
  }
};

/**
 * `count_until_satisfied` applied to a product type.
 *
 * @tparam F A predicate that may be evaluated over a product type.
 * @tparam TT A product type
 * @param f A predicate object
 * @param xx A product object
 * @return an integer in the range [0, factor_sizeof(xx)]
 */
template <class F, product_type TT>
constexpr size_t count_until_satisfied_p(F&& f, const TT& xx)
  requires predicate_over_product<F, TT>
{
  return std::apply(
      [&f]<class... T>(T... x) {
        return count_until_satisfied(std::forward<F&&>(f), x...);
      },
      std::forward<const TT&>(xx));
};

//-------------------------------------------------------
// reference_tuple
//-------------------------------------------------------
/*
 * Directly declaring a node list of reference types is verbose and redundant:
 * ```
 * constexpr static std::tuple<
 *     const DummyOutputNodeSpecification<T>&,
 *     const DummyInputNodeSpecification<T>&>
 *     nodes{a, b};
 * ```
 * The reason for this verbosity is that if nodes are declared `constexpr`, the
 * expression `tuple{a,b}` is taken as a tuple of values, not of references. We
 * would prefer to specify node lists with simple declaration lists where the
 * reference type is inferred rather than declared.
 */

/**
 * Product type containing a tuple of references
 *
 * @tparam T Pack parameter for the type of each tuple element.
 */
template <typename... T>
class reference_tuple : public std::tuple<T...> {
 public:
  /*
   * The constructor forward is arguments to the base-class tuple.
   */
  template <typename... U>
  explicit constexpr reference_tuple(const U&... u)
      : std::tuple<T...>{std::forward<const U&>(u)...} {
  }
  using type_tuple = std::tuple<T...>;
};
/**
 * Deduction guide ensures that reference_tuple is instantiated as a tuple of
 * references, that is, that each element of pack `T` is a `const U&`.
 */
template <typename... U>
reference_tuple(const U&...) -> reference_tuple<const U&...>;

/*
template <class... X>
struct type_tuple<reference_tuple<X...>> {
  using types = std::tuple<X...>;
};
*/

}  // namespace tiledb::flow_graph
namespace std {

/**
 * Specialization of `std::tuple_size` for `reference_tuple`.
 *
 * This is required for `std::apply` to work with `reference_tuple`. Without it,
 * `apply` does not treat it as a "tuple-like" type and does not unpack it as
 * a parameter pack but instead leaves it unchanged as a single argument.
 */
template <class... T>
struct tuple_size<  // NOLINT(cert-dcl58-cpp)
    tiledb::flow_graph::reference_tuple<T...>>
    : integral_constant<std::size_t, sizeof...(T)> {};
/*
 * "cert-dcl58-cpp" says not to modify anything in the namespace `std`. The
 * existing way this check is implemented is overly strict. It does not exempt
 * definitions such as `tuple_size` that are explicitly permitted to be
 * specialized with user-defined types.
 *
 * See: https://clang.llvm.org/extra/clang-tidy/checks/cert/dcl58-cpp.html
 */

}  // namespace std

#endif  // TILEDB_FLOW_GRAPH_GRAPH_TYPE_H
