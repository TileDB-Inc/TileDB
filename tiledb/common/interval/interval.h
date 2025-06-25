/**
 * @file   interval.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * This file defines classes for set-theoretic intervals. The operations are
 * standard ones on sets: intersection, union (where defined), partial ordering,
 * etc. Intervals are defined for any totally ordered set. All integral and
 * floating-point types are totally ordered, but so also are strings of totally
 * ordered symbols. In particular, no arithmetic operations such as addition are
 * assumed. For this sense, see
 *      https://en.wikipedia.org/wiki/Interval_(mathematics)
 *
 * To avoid confusion, there is another, different use for intervals that is
 * prevalent, which is to use interval arithmetic to represent error bounds.
 * This not the purpose here. For this other sense, see
 *      https://en.wikipedia.org/wiki/Interval_arithmetic
 *
 * Three foundational kinds of intervals are from integral types, floating point
 * types, and strings.
 *
 * @subsection Requirements on the set
 *
 * Total ordering means there is a trichotomous order predicate `<` such that
 * for any `x` and `y`, exactly one of `x < y`, `y < x` or `x == y` is true.
 * In C++20, this is formalized as `std::totally_ordered` and the comparison
 * predicate as `operator<=>`.
 *
 * @subsection Definition of an interval
 *
 * An interval is a subset given by a lower bound and an upper bound, either of
 * which may be absent ("infinite bound"), and either of which may be open
 * (`<`), denoted with parentheses `()`, or closed (`<=`), denoted with square
 * brackets `[]`. Here's a list of the possible defining inequalities
 *      a < x < b           open, open
 *      a < x <= b          open, closed
 *      a <= x < b          closed, open
 *      a <= x <= b         closed, closed
 *      x < b               missing, open
 *      x <= b              missing, closed
 *      a < x               open, missing
 *      a <= x              closed, missing
 *      <no constraint>     missing, missing (i.e. the whole set)
 * These inequalities are the fundamental defining predicates of an interval,
 * from which all else follows.
 *
 * The empty set is a valid interval, since there is no
 * requirement that the upper bound be greater than the lower bound. A set
 * consisting of a single point `a` is also a valid interval, with upper and
 * lower bounds equal and both closed. The entire set is also valid interval,
 * the case where both bounds are absent.
 *
 * All told, there are 11 distinct kinds of intervals under this definition. The
 * bounds are non-degenerate: `a < b`. It will be clear that a simplistic
 * representation of an interval as two values is inadequate.
 *
 * Group 1
 *      ∅        empty set
 * Group 2
 *      [a,a]    single point, closed
 * Group 3
 *      (a,b)    finite, open
 *      (a,b]    finite, half-open half-closed
 *      [a,b)    finite, half-closed half-open
 *      [a,b]    finite, closed
 * Group 4
 *      (a,+∞)   half-infinite, lower half-open
 *      [a,+∞)   half-infinite, lower half-closed
 * Group 5
 *      (-∞,b)   half-infinite, upper half-open
 *      (-∞,b]   half-infinite, upper half-closed
 * Group 6
 *      (-∞,+∞)  bi-infinite
 *
 * In the implementation, each of these intervals has its own marked
 * constructor.
 *
 * @subsection Motivation
 *
 * For practical applications, it suffices to use the union a set of intervals
 * to represent arbitrary subsets of an ordered set. Every subset representable
 * as a set of intervals has a canonical representation as a set of ordered,
 * strictly separable intervals. This representation provides a solid foundation
 * for parallel and distributed processing.
 *
 * @section Operations
 *
 * @subsection Set membership
 *
 * An interval is a kind of set. Not much more to be said.
 *
 * @subsection Intersection
 *
 * The intersection of two intervals is always an interval. This is one of the
 * reasons that the empty set should be considered an interval. (Mathematically
 * speaking, this closure property means that intervals are a meet-lattice.)
 *
 * @subsection Comparison, strict separation, and adjacency
 *
 * Intervals are partially ordered under a quantified comparison predicate. I_1
 * is less than I_2 if every t_1 in I_1 is less than every t_2 in I_2. Intervals
 * that intersect are never ordered, since for a common element x, x (as a
 * member of one set) is never less than x (as a member of the other).
 *
 * The empty set acts atypically here. Because universal quantifier over an
 * empty set is true, the empty set is less than every set and every set is less
 * than the empty set: ∅ < I < ∅. As a result, we must exclude the empty set
 * from the domain of `<` when considered as a partial order. If intervals that
 * intersect are not ordered, intervals that are disjoint (i.e. don't intersect)
 * are ordered only if neither is empty.
 *
 * Two points are separable if there's a third distinct point between them. Two
 * intervals I_1 and I_2 are separable if every point in I_1 is separable from
 * every point in I_2. Separable intervals are always disjoint and ordered.
 *
 * Disjoint intervals, though, are not always separable; they may not have any
 * point between them. For example consider the intervals [a,b) and [b,c).
 * Disjoint intervals that are not strictly separable are called adjacent.
 *
 * The empty set is disjoint with every other set, but it's also inseparable
 * from every set. The definition of separability is a universal quantifer over
 * points in the set, so it's true for the empty set. Hence the empty set is
 * also adjacent to every set.
 *
 * @subsection Union
 *
 * The union of two intervals is not always an interval, but it can be. The
 * union of separable intervals is never an interval. If two non-empty intervals
 * are not separable, they either intersect or they're adjacent, and in both of
 * these cases their union is an interval. Union with the empty set is also
 * defined; it's the identity operator.
 *
 * @section Complications
 *
 * Of course it's not as simple as the above description might indicate. Some
 * complications are theoretical, some are practical.
 *
 * @subsection Finite types vs. infinite ideas
 *
 * We consider the defining inequalities as upon the mathematical sets on which
 * the intervals are defined, with specific types being finite subsets of them.
 * This matter with boundaries. Consider the integer interval [0,+infinity)
 * represented with k-bit integers. As a representation, it's the same as the
 * interval [0,2^k-1], but we don't use 2^k-1 as a placeholder for "unbounded".
 * If we consider the same interval in 2k-bit integers, we still have
 * [0,+infinity), not [0,2^(2k)-1].
 *
 * In the case of floating point numbers, larger floating point representations
 * admit larger exponents, so we consider those types as dense sets.
 * Accordingly, we abjure the use of `std::nextafter` here, since it's a
 * representation-dependent increment.
 *
 * If type `T` is a subset of type `U` (e.g. `uint8_t` and `uint16_t`, `float`
 * and `double`), then we have a faithful conversion of values in `T` to values
 * in `U`. We also have a conversion from `Interval<T>` to `Interval<U>`. These
 * conversions must keep the defining inequalities intact. Less obviously, we
 * want the same behavior converting from integral types to floating point
 * types.
 *
 * @subsection Discrete vs. dense types
 *
 * A dense set is one that, given any two points, there is another point between
 * them. A discrete set is one where adjacent points are separated from each
 * other by gaps. Dense types are canonically defined by their inequalities, but
 * discrete ones are not. Consider the interval [1,3] over the integers. It's a
 * set with three elements {1,2,3} and has three additional representations as
 * intervals: [1,4) (0,3] (0,4). For the integers, the inequalities a<x and
 * a+1<=x are equivalent to each other, likewise x<b and x<=b-1. We are
 * providing set-theoretic operations for `Interval` and thus we remain agnostic
 * about how a particular set is represented is such operations.
 *
 * Discrete and dense are concepts that are rightly associated with upper and
 * lower neighborhoods of points. Consider a set with lexicographic ordering,
 * for example strings of the symbols {A,B,C,D}. On the upper side, there's
 * always a gap. The first string after `B` is `BA' and there are no strings
 * between them. There is, however, no last string before `B`, because some
 * element of the sequence `AD`, `ADD`, `ADDD`, ... is always after every
 * element that comes before `B`. This is true for every string (except the
 * empty string and the first string `A`), so such sets are generically
 * left-dense and right-discrete.
 *
 * In order to evaluate set-theoretic function and to encompass all these cases,
 * we therefore require an adjacency predicate for the base type. Adjacency is
 * always false for floating point numbers and can be evaluated as a+1==b for
 * integral types. For lexicographic ordering, the adjacency requirement is more
 * involved, but basically says that they can differ in at most their last
 * symbol and that the last symbols have to be adjacent.
 *
 * An open interval on a discrete set may only have a single element. In that
 * case the bounds are twice-adjacent--the lower bound is adjacent to the
 * element and the element is adjacent to the upper bound.
 *
 * @subsection Extended elements. Elements that aren't ordered.
 *
 * Floating point types have two infinity values. In the parlance of IEEE 754,
 * these are "extended numbers". Infinite values are meaningful when used as
 * constructor arguments, but not as ordinary elements. Infinite values, for
 * consistency, are not stored as bounds, but converted to unbounded intervals.
 *
 * Floating point types have two NaN values, quiet and signaling, though the
 * difference is not relevant here. NaN values are not meaningful as numbers.
 * Accordingly, we can't accept NaN as a bound, and so constructors that accept
 * a raw type cannot be labelled `noexcept`. Type members that are not numbers
 * cannot be elements of a set of numbers (obviously?).
 */

#pragma once
#ifndef TILEDB_INTERVAL_H
#define TILEDB_INTERVAL_H

#include <cmath>
#include <optional>
#include <stdexcept>
#include <tuple>

#include "tiledb/common/common-std.h"

using std::isnan, std::isinf, std::isfinite;

namespace tiledb::common {
namespace detail {

/**
 * Interval<T> cannot be instantiated without defining TypeTraits<T>. The
 * necessary traits involve adjacency, infinite elements, and unordered
 * elements.
 *
 * This default traits class should not be instantiated. Its members are only
 * declared and not defined. They're all declared [[maybe_unused]] to silence
 * warnings, since specializations of this class are used. The documentation for
 * the declarations in this class are about the requirements of a traits
 * specialization.
 *
 * @tparam T Each interval is a subset of the set of all T.
 * @tparam Enable Allows specializations with enable_if
 */
template <class T, typename Enable = T>
struct TypeTraits {
  /**
   * Returns a pair of predicates "adjacent" and "twice-adjacent". `a` is
   * adjacent to `b` if `a < b` and there is no `c` such that `a < c < b'. `a`
   * is twice-adjacent to `b` if `a < b` and there exists a `c` such that `a` is
   * adjacent to `c` and `c` is adjacent to `b`.
   */
  [[maybe_unused]] static tuple<bool, bool> adjacency(T, T);

  /**
   * Returns the predicate "adjacent".
   */
  [[maybe_unused]] static bool adjacent(T, T);

  /**
   * Predicate constant that T contains unordered elements. Floating point types
   * contain NaN values, which are not ordered and thus unsuitable as interval
   * bounds.
   *
   * Used as a `if constexpr` guard for calling `is_ordered`.
   */
  [[maybe_unused]] static const bool has_unordered_elements;

  /**
   * Predicate constant that T contains infinite elements, that is, an element
   * that represents an infinity. An infinite element presented as an interval
   * bound needs to be mapped to a canonical form.
   */
  [[maybe_unused]] static const bool has_infinite_elements;

  /**
   * Predicate function that an element of T is a number. This function allows
   * excluding floating-point NaN as an interval bound.
   *
   * This function need not be defined for types that do not have unordered
   * elements. All occurences appear within an `if constexpr` guard with
   * `has_unordered_elements`.
   */
  [[maybe_unused]] static bool is_ordered(T);

  /**
   * Predicate function that an element of T is a finite number. This function
   * allows detecting floating point infinities as interval bounds in
   * constructors.
   */
  [[maybe_unused]] static bool is_finite(T);

  /**
   * Predicate function that an infinite element is positive infinity. Returns
   * false otherwise.
   *
   * @precondition argument is an infinite element
   */
  [[maybe_unused]] static bool is_infinity_positive(T);
};

/**
 * Specialization of TypeTraits for integral types. Integral types have
 * non-trivial adjacency, no unordered elements, and no infinite elements.
 */
template <class T>
struct TypeTraits<
    T,
    typename std::enable_if<std::is_integral<T>::value, T>::type> {
  /**
   * Adjacency for integral types means that the lower is one less than the
   * upper.
   */
  static tuple<bool, bool> adjacency(T a, T b) {
    if (a >= b) {
      return {false, false};
    }
    // Assert: a < b
    // Assert: a+1 cannot overflow. b-1 cannot overflow
    return {a + 1 == b, a + 1 == b - 1};
  }

  /**
   * Adjacency for integral types means that the lower is one less than the
   * upper.
   */
  static bool adjacent(T a, T b) {
    if (a >= b) {
      return false;
    }
    // Assert: a < b
    // Assert: a+1 cannot overflow.
    return a + 1 == b;
  }

  static constexpr bool has_unordered_elements = false;
  static constexpr bool has_infinite_elements = false;
};

/**
 * Specialization of TypeTraits for floating-point types. Floating point numbers
 * have trivial adjacency and presence of both unordered and infinite elements.
 */
template <class T>
struct TypeTraits<
    T,
    typename std::enable_if<std::is_floating_point<T>::value, T>::type> {
  /**
   * Floating point numbers are never adjacent.
   */
  static tuple<bool, bool> adjacency(T, T) {
    return {false, false};
  };

  static bool adjacent(T, T) {
    return false;
  };

  static constexpr bool has_unordered_elements = true;
  static constexpr bool has_infinite_elements = true;

  /**
   * An extended number is either finite or infinite, but must not be NaN.
   */
  static bool is_ordered(T x) {
    return !isnan(x);
  }

  /**
   * Floating point types have infinite elements.
   */
  static bool is_finite(T x) {
    return isfinite(x);
  }

  /**
   * Floating point infinities compare with finite values.
   */
  static bool is_infinity_positive(T x) {
    return x > 0;
  }
};

/**
 * Non-template base class for `Interval`.
 */
struct IntervalBase {
  /** Marks a constructor for the empty set */
  static constexpr class empty_set_t {
  } empty_set = empty_set_t();
  /** Marks a constructor for a single-point set */
  static constexpr class single_point_t {
  } single_point = single_point_t();
  /** Marks a boundary as open in a constructor */
  static constexpr class open_t {
  } open = open_t();
  /** Marks a boundary as closed in a constructor */
  static constexpr class closed_t {
  } closed = closed_t();
  /** Mark a lower boundary as infinite (i.e. not present) */
  static constexpr class minus_infinity_t {
  } minus_infinity = minus_infinity_t();
  /** Mark an upper boundary as infinite (i.e. not present) */
  static constexpr class plus_infinity_t {
  } plus_infinity = plus_infinity_t();
};

}  // namespace detail

// Forward declaration
template <class T>
class WhiteboxInterval;

/**
 * @class Interval
 *
 * Intervals are immutable upon construction. That is to say that intervals are
 * a value class. The value of an Interval variable changes by assignment, not
 * by manipulating its innards in any other way.
 *
 * The implementation uses as direct a representation as is possible, with as
 * few encoding tricks as possible. Lower and upper bounds use `std::optional`
 * for storage, with no meaning assigned to a null. The value of a bound is
 * absent when logically consistent, as is the case with the empty set and with
 * infinite bounds. Other aspects of the interval are encoded directly with
 * boolean values. There are eight of them, which fortuitously fit into a single
 * byte as bit fields.
 *
 * The public constructors do not throw exceptions when the argument elements
 * are ordered, either ordinary or extended (infinite). The only time a
 * constructor will throw is when an argument is unordered, notably NaN. For
 * types with unordered elements, the constructors are not marked `noexcept`,
 * although this is out of expediency rather than necessity. Also not present is
 * an auxiliary class for "the ordered elements of T", which would also allow
 * `noexcept` constructors.
 *
 * @tparam T A type whose values include a totally ordered set
 */
template <class T>
class Interval : public detail::IntervalBase {
  typedef detail::TypeTraits<T> Traits;
  /**
   * WhiteboxInterval makes available internals of Interval for testing.
   */
  friend class WhiteboxInterval<T>;

  /**
   * @var Lower bound of the interval, if one exists.
   *
   * @invariant lower_bound.has_value() \iff is_lower_bound_open_ \or
   * is_lower_bound_closed_
   */
  optional<T> lower_bound_;
  /**
   * @var Upper bound of the interval, if one exists.
   *
   * @invariant upper_bound.has_value() \iff is_upper_bound_open_ \or
   * is_upper_bound_closed_
   */
  optional<T> upper_bound_;
  /*
   * The various types of interval as stored as precomputed predicate values
   * about the various aspects of the class. The predicates values are stored as
   * bit fields.
   */
  /**
   * @var is_empty_ True if and only if the interval is the empty set.
   *
   * @invariant is_empty_ \implies !has_single_point_
   * @invariant is_empty_ \implies !is_lower_open_
   * @invariant is_empty_ \implies !is_lower_closed_
   * @invariant is_empty_ \implies !is_lower_infinite_
   * @invariant is_empty_ \implies !is_upper_open_
   * @invariant is_empty_ \implies !is_upper_closed_
   * @invariant is_empty_ \implies !is_upper_infinite_
   * @invariant is_empty_ \implies !lower_bound.has_value()
   * @invariant is_empty_ \implies !upper_bound.has_value()
   */
  bool is_empty_ : 1;
  /**
   * @var is_single_point_ True iff and only iff the interval consists of a set
   * containing a single element.
   *
   * @invariant has_single_point_ \implies !is_empty_
   * @invariant has_single_point_ \implies !is_lower_infinite_
   * @invariant has_single_point_ \implies !is_upper_infinite_
   *
   * @invariant has_single_point_ \implies ( lower_bound == upper_bound \or
   * adjacent(lower_bound, upper_bound) \or
   * twice_adjacent(lower_bound,upper_bound) )
   *
   * @invariant ( has_single_point_ \and lower_bound == upper_bound ) \implies
   * is_lower_closed_ \and is_upper_closed_
   *
   * @invariant ( has_single_point_ \and adjacent(lower_bound, upper_bound) )
   * \implies (( is_lower_closed_ \and is_upper_open_ ) \or ( is_lower_open_
   * \and is_upper_closed_ ))
   *
   * @invariant ( has_single_point_ \and twice_adjacent(lower_bound,
   * upper_bound) ) \implies ( is_lower_open_ \and is_upper_open_ )
   */
  bool has_single_point_ : 1;
  /**
   * @var is_lower_open_
   *
   * @invariant is_lower_open_ \implies !is_lower_closed
   * @invariant is_lower_open_ \implies !is_lower_infinite_
   * @invariant is_lower_open_ \implies lower_bound.has_value()
   */
  bool is_lower_open_ : 1;
  /**
   * @var is_lower_closed_
   *
   * @invariant is_lower_closed \implies !is_lower_open_
   * @invariant is_lower_closed \implies !is_lower_infinite_
   * @invariant is_lower_closed_ \implies lower_bound.has_value()
   */
  bool is_lower_closed_ : 1;
  /**
   * @var is_lower_infinite_
   *
   * @invariant is_lower_infinite_ \implies !is_lower_open
   * @invariant is_lower_infinite_ \implies !is_lower_closed
   * @invariant is_lower_infinite_ \implies !lower_bound.has_value()
   */
  bool is_lower_infinite_ : 1;
  /**
   * @var is_upper_open_
   *
   * @invariant is_upper_open_ \implies !is_upper_closed
   * @invariant is_upper_open_ \implies !is_upper_infinite_
   * @invariant is_upper_open_ \implies upper_bound.has_value()
   */
  bool is_upper_open_ : 1;
  /**
   * @var is_upper_closed_
   *
   * @invariant is_upper_closed \implies !is_upper_open_
   * @invariant is_upper_closed \implies !is_upper_infinite_
   * @invariant is_upper_closed_ \implies upper_bound.has_value()
   */
  bool is_upper_closed_ : 1;
  /**
   * @var is_upper_infinite_
   *
   * @invariant is_upper_infinite_ \implies !is_upper_open
   * @invariant is_upper_infinite_ \implies !is_upper_closed
   * @invariant is_upper_infinite_ \implies !upper_bound.has_value()
   */
  bool is_upper_infinite_ : 1;

  /**
   * A Bound is auxiliary class to support the construction of Interval object.
   * Each contains data for either an upper or lower bound. Objects have life
   * span confined to the duration of object construction.
   *
   * It's possible for a bound (an inequality) to not be satisfiable in four
   * cases, and only when the template argument T contains infinite elements.
   * The non-satisfiable bounds are +infinity < x, x < -infinity, and likewise
   * for <=. A non-satisfiable bound must instantiate an empty set.
   *
   * @invariant satisfiable_ \implies is_open_ || is_closed_ \implies
   * bound.has_value()
   *
   * @invariant satisfiable_ \implies exactly one of is_open_, is_closed_, or
   * is_finite_ is true
   */
  struct Bound {
    optional<T> bound_;
    bool is_open_;
    bool is_closed_;
    bool is_infinite_;
    bool is_satisfiable_;
    /**
     * Finite constructor uses T for the bound instead of optional<T>.
     */
    Bound(T bound, bool is_closed) noexcept
        : bound_(bound)
        , is_open_(!is_closed)
        , is_closed_(is_closed)
        , is_infinite_(false)
        , is_satisfiable_(true) {
    }

    /**
     * Special constructor for cases where the bound is nullopt.
     */
    Bound(bool is_infinite, bool is_satisfiable) noexcept
        : bound_(nullopt)
        , is_open_(false)
        , is_closed_(false)
        , is_infinite_(is_infinite)
        , is_satisfiable_(is_satisfiable) {
    }

    /**
     * Extraction constructor for copying individual lower or upper bounds out
     * of an Interval.
     */
    Bound(
        optional<T> bound,
        bool is_open,
        bool is_closed,
        bool is_infinite) noexcept
        : bound_(bound)
        , is_open_(is_open)
        , is_closed_(is_closed)
        , is_infinite_(is_infinite)
        , is_satisfiable_(true) {
    }

    /**
     * When comparing bound `left` and `right` as lower bounds, we are comparing
     * intervals `Left = left,+infinity)` and `Right = right,+infinity)`. We
     * want the inequality convention for bounds to follow that for elements of
     * T, so that, say `Left < Right` when `x < y` (disregarding adjacency).
     * This makes the less-than relationship on Bounds the superset relationship
     * on their Intervals extended to infinity.
     *
     * In other words, `Left < Right` when there exist elements in `Left`
     * that are less than every element in `Right`.
     *
     * @param right. The right hand side of a comparison left < right. This
     * interval is the left hand side.
     * @return 0 if Left = Right as sets, -1 if Left is a proper superset of
     * Right, +1 otherwise
     * @precondition is_satisfiable && right.is_satisfiable. In other words,
     * this function is only for bounds of already-constructed Interval objects.
     */
    int compare_as_lower(Bound right) const {
      // Check the precondition anyway.
      // Could be converted to an NDEBUG-dependent assertion later.
      if (!is_satisfiable_ || !right.is_satisfiable_) {
        throw std::invalid_argument(
            "Interval::Bound::compare_as_lower - "
            "Non-satisfiable bounds are not comparable.");
      }
      // Assert: Both bounds are satisfiable
      if (is_infinite_ || right.is_infinite_) {
        return is_infinite_ ? (right.is_infinite_ ? 0 : -1) : +1;
      }
      // Assert: Bound bounds are finite.
      T left_bound = bound_.value();
      T right_bound = right.bound_.value();
      if ((is_open_ && right.is_open_) || (is_closed_ && right.is_closed_)) {
        /*
         * When both bounds are the same type, comparison is the same as the
         * underlying set.
         */
        return left_bound < right_bound  ? -1 :
               left_bound == right_bound ? 0 :
                                           +1;
      }
      if (is_open_ && right.is_closed_) {
        if (right_bound <= left_bound) {
          return +1;
        }
        if (Traits::adjacent(left_bound, right_bound)) {
          return 0;
        }
        return -1;
      }
      // else: Left is closed and right is open
      if (left_bound <= right_bound) {
        return -1;
      }
      if (Traits::adjacent(right_bound, left_bound)) {
        return 0;
      }
      return +1;
    }

    /**
     * When comparing bound `left` and `right` as upper bounds, we are comparing
     * intervals `Left = (-infinity,left` and `Right = (-infinity,right`. We
     * want the inequality convention for bounds to follow that for elements of
     * T, so that, say `Left < Right` when `x < y` (disregarding adjacency).
     * This makes the less-than relationship on Bounds the subset relationship
     * on their Intervals extended to infinity.
     *
     * @param right. The right hand side of a comparison left < right. This
     * interval is the left hand side.
     * @return 0 if Left = Right as sets, -1 if Left is a proper subset of
     * Right, +1 otherwise
     * @precondition is_satisfiable && right.is_satisfiable. In other words,
     * this function is only for bounds of already-constructed Interval objects.
     */
    int compare_as_upper(Bound right) const {
      // Check the precondition anyway.
      // Could be converted to an NDEBUG-dependent assertion later.
      if (!is_satisfiable_ || !right.is_satisfiable_) {
        throw std::invalid_argument(
            "Interval::Bound::compare_as_lower - "
            "Non-satisfiable bounds are not comparable.");
      }
      // Assert: Both bounds are satisfiable
      if (is_infinite_ || right.is_infinite_) {
        return is_infinite_ ? (right.is_infinite_ ? 0 : +1) : -1;
      }
      // Assert: Bound bounds are finite.
      T left_bound = bound_.value();
      T right_bound = right.bound_.value();
      if ((is_open_ && right.is_open_) || (is_closed_ && right.is_closed_)) {
        /*
         * When both bounds are the same type, comparison is the same as the
         * underlying set.
         */
        return left_bound < right_bound  ? -1 :
               left_bound == right_bound ? 0 :
                                           +1;
      }
      if (is_open_ && right.is_closed_) {
        /*
         * ...,left) vs. ...,right]
         *
         * If `left == right`, then the open side (Left) does not contain the
         * point `right` and the closed side (Right) does. In this case Left is
         * a proper subset of Right and we return -1.
         *
         * For adjacency types, the sets are equal when `right` is adjacent to
         * `left`. This requires `right < left`.
         */
        if (left_bound <= right_bound) {
          return -1;
        }
        if (Traits::adjacent(right_bound, left_bound)) {
          return 0;
        }
        return +1;
      }
      // else: Left is closed and right is open
      if (left_bound >= right_bound) {
        return +1;
      }
      if (Traits::adjacent(left_bound, right_bound)) {
        return 0;
      }
      return -1;
    }

    /**
     * When comparing bound `left` and `right` as upper bounds, we are comparing
     * intervals `Left = (-infinity,left` and `Right = right,+infinity)`. We
     * say that `Left < Right` if the sets are disjoint. If they are disjoint,
     * they can be adjacent. The return value of this function captures these
     * three possibilities.
     *
     * Note: This comparison is for bounds from different intervals. A different
     * kind of mixed comparison would swap left and right. This isn't needed
     * outside the constructor. For the analogue, see `adjust_bounds`.
     *
     * @return -1 if Left < Right and Left is not adjacent to Right. 0 if Left <
     * Right and Left is adjacent to Right. +1 if Left and Right have a
     * non-trivial intersection.
     */
    int compare_as_mixed(Bound right) const {
      // Check the precondition anyway.
      // Could be converted to an NDEBUG-dependent assertion later.
      if (!is_satisfiable_ || !right.is_satisfiable_) {
        throw std::invalid_argument(
            "Interval::Bound::compare_as_lower - "
            "Non-satisfiable bounds are not comparable.");
      }
      // Assert: Both bounds are satisfiable
      if (is_infinite_ || right.is_infinite_) {
        return +1;
      }
      // Assert: Bound bounds are finite.
      T left_bound = bound_.value();
      T right_bound = right.bound_.value();
      if (is_open_ && right.is_open_) {
        return left_bound <= right_bound                 ? -1 :
               Traits::adjacent(right_bound, left_bound) ? 0 :
                                                           +1;
      }
      if (is_closed_ && right.is_closed_) {
        return right_bound <= left_bound                 ? +1 :
               Traits::adjacent(left_bound, right_bound) ? 0 :
                                                           -1;
      }
      /*
       * Bounds are mixed
       */
      return left_bound < right_bound ? -1 : left_bound == right_bound ? 0 : +1;
    }
  };

  /**
   * A null bound from an argument.
   */
  static Bound BoundNull() {
    return Bound(false, false);
  }

  /**
   * An infinite bound from an argument.
   */
  static Bound BoundInfinity() {
    return Bound(true, true);
  }

  /**
   * Extract the lower bound from this interval.
   */
  Bound BoundLower() const {
    return Bound(
        lower_bound_, is_lower_open_, is_lower_closed_, is_lower_infinite_);
  }

  /**
   * Extract the upper bound from this interval.
   */
  Bound BoundUpper() const {
    return Bound(
        upper_bound_, is_upper_open_, is_upper_closed_, is_upper_infinite_);
  }

  /**
   * Standard constructor. All member variables of the class are constant; this
   * constructor simply forwards all its arguments as initializers. Arguments
   * must be checked in advance to ensure all the class invariants are
   * satisfied, so it's not public.
   *
   * Note that the satisfiability member of a Bound is ignored here. It's used
   * only when adjusting bounds, to create a empty set if a bound is not
   * satisfiable.
   *
   * @precondition lower, upper are both ordered, non-exceptional elements
   */
  explicit Interval(Bound lower, Bound upper, bool empty, bool single) noexcept
      : lower_bound_(lower.bound_)
      , upper_bound_(upper.bound_)
      , is_empty_(empty)
      , has_single_point_(single)
      , is_lower_open_(lower.is_open_)
      , is_lower_closed_(lower.is_closed_)
      , is_lower_infinite_(lower.is_infinite_)
      , is_upper_open_(upper.is_open_)
      , is_upper_closed_(upper.is_closed_)
      , is_upper_infinite_(upper.is_infinite_) {
  }

  /**
   * Interval constructor from the return value of an argument validator.
   *
   * @param x a tuple of lower and upper bounds and empty/single flags.
   */
  explicit Interval(tuple<Bound, Bound, bool, bool> x) noexcept
      : Interval(
            std::get<0>(x), std::get<1>(x), std::get<2>(x), std::get<3>(x)) {
  }

  /**
   * Adjusts a pair of bounds into correct interval constructor arguments. Maps
   * non-satisfiable bounds to empty intervals. Evaluates the single-point
   * predicate.
   *
   * @param lower, upper. Ordinary elements of T. May not include extended or
   * unordered elements.
   * @return constructor arguments
   *
   * @precondition lower and upper bounds have been normalized
   */
  tuple<Bound, Bound, bool, bool> adjust_bounds(
      Bound lower, Bound upper) noexcept {
    if (!lower.is_satisfiable_ || !upper.is_satisfiable_) {
      // If either of the bounds are not satisfiable, we have an empty set.
      return {BoundNull(), BoundNull(), true, false};
    }
    if (lower.is_infinite_ || upper.is_infinite_) {
      // If either bound is infinite, it's neither empty nor single-point.
      return {lower, upper, false, false};
    }

    // Both bounds are neither empty nor infinite, so each has finite bounds.
    T lower_value = lower.bound_.value();
    T upper_value = upper.bound_.value();

    if (lower_value > upper_value) {
      //  If the lower bound is larger than the upper, it's always an empty set.
      return {BoundNull(), BoundNull(), true, false};
    }

    if (lower_value == upper_value) {
      if (lower.is_closed_ && upper.is_closed_) {
        // Exactly one element can satisfy the inequalities.
        return {lower, upper, false, true};
      } else {
        // The inequalities can't be simultaneously satisfied.
        return {BoundNull(), BoundNull(), true, false};
      }
    }

    /*
     * We have the ordinary case where lower_value < upper_value.
     *
     * In only one case we still have the empty set, where we have an
     * open interval and adjacent bounds. In all other cases the set is not
     * empty.
     *
     * We have single-point sets in a handful of cases. If the interval is open,
     * it has a single point when the bounds are twice-adjacent. If there
     * interval is half-open on side and half-closed on the other, it has a
     * single point when the bounds are adjacent.
     */
    if (lower.is_open_ && upper.is_open_) {
      /*
       * This is the only case where we need to calculate twice-adjacency, and
       * we generically need to calculate adjacency at the same time.
       */
      auto [adjacent, twice_adjacent] =
          Traits::adjacency(lower_value, upper_value);
      if (adjacent) {
        return {BoundNull(), BoundNull(), true, false};
      } else {
        return {lower, upper, false, twice_adjacent};
      }
    } else if (lower.is_closed_ && upper.is_closed_) {
      // We have checked for the single-point case already.
      return {lower, upper, false, false};
    } else {
      // Assert: This interval is half-open on side, half-closed on the other.
      return {lower, upper, false, Traits::adjacent(lower_value, upper_value)};
    }
  }

  /**
   * Normalize a boundary value specified as either an open or closed bound of
   * an interval. (In other words, this function is not called for
   * infinite-marked bounds.) Unordered elements are rejected as invalid.
   * Infinite elements are converted into an infinite boundary specification.
   *
   * This is the only part of the constructor machinery that originates an
   * exception. It only happens when T has unordered elements (such as NaN) and
   * one is passed as an argument.
   *
   * @param bound An arbitrary value of T
   * @param is_closed True
   * @param is_for_upper_bound
   * @return bound, is_open, is_closed, is_infinite, not_satisfiable
   *
   * @precondition [in] bound is ordered. Throws if not.
   * @postcondition [out] bound is an ordinary element and not infinite
   */
  Bound normalize_bound(T bound, bool is_closed, bool is_for_upper_bound) {
    if constexpr (Traits::has_unordered_elements) {
      if (!Traits::is_ordered(bound)) {
        throw std::invalid_argument(
            "Interval::constructor - "
            "Unordered member is invalid as an interval bound");
      }
    }
    if constexpr (!Traits::has_infinite_elements) {
      return Bound(bound, is_closed);
    } else {
      if (Traits::is_finite(bound)) {
        return Bound(bound, is_closed);
      } else {
        bool positive = Traits::is_infinity_positive(T(bound));
        if ((is_for_upper_bound && !positive) ||
            (!is_for_upper_bound && positive)) {
          // We have either x < -infinity or +infinity < x, both of which
          // represent the empty set.
          return BoundNull();
        }
        // Otherwise we convert to an infinite bound
        return BoundInfinity();
      }
    }
  }

 public:
  /**
   * Empty constructor is disabled.
   */
  Interval() = delete;

  /**
   * Default copy constructor
   */
  Interval(const Interval&) = default;

  /**
   * Default move constructor
   */
  Interval(Interval&&) noexcept = default;

  /**
   * Default copy assignment
   */
  Interval& operator=(const Interval&) = default;

  /**
   * Default move assignment
   */
  Interval& operator=(Interval&&) noexcept = default;

  /**
   * Empty set constructor.
   *
   * Empty sets may also be constructed with boundaries that define an empty
   * set, such as the lower bound being greater than the upper bound. This
   * constructor is for those situations where the user specifically intends
   * the empty set as a value.
   */
  explicit Interval(const empty_set_t&) noexcept
      : lower_bound_(nullopt)
      , upper_bound_(nullopt)
      , is_empty_(true)
      , has_single_point_(false)
      , is_lower_open_(false)
      , is_lower_closed_(false)
      , is_lower_infinite_(false)
      , is_upper_open_(false)
      , is_upper_closed_(false)
      , is_upper_infinite_(false) {};

  /**
   * Single-point set constructor.
   *
   * Single-point sets may also be constructed in another situation,
   * specifically as a closed interval with equal upper and lower bounds.
   */
  Interval(const single_point_t&, T x)
      : Interval(adjust_bounds(
            normalize_bound(x, true, false), normalize_bound(x, true, true))) {
      };

  /**
   * Finite set constructor: open
   */
  Interval(const open_t&, T lower, T upper, const open_t&)
      : Interval(adjust_bounds(
            normalize_bound(lower, false, false),
            normalize_bound(upper, false, true))) {};

  /**
   * Finite set constructor: half-open, half-closed
   */
  Interval(const open_t&, T lower, T upper, const closed_t&)
      : Interval(adjust_bounds(
            normalize_bound(lower, false, false),
            normalize_bound(upper, true, true))) {};

  /**
   * Finite set constructor: half-closed, half-open
   */
  Interval(const closed_t&, T lower, T upper, const open_t&)
      : Interval(adjust_bounds(
            normalize_bound(lower, true, false),
            normalize_bound(upper, false, true))) {};

  /**
   * Finite set constructor: closed
   */
  Interval(const closed_t&, T lower, T upper, const closed_t&)
      : Interval(adjust_bounds(
            normalize_bound(lower, true, false),
            normalize_bound(upper, true, true))) {};

  /**
   * Lower-infinite set constructor: upper half-open
   */
  Interval(const minus_infinity_t&, T upper, const open_t&)
      : Interval(adjust_bounds(
            BoundInfinity(), normalize_bound(upper, false, true))) {};

  /**
   * Lower-infinite set constructor: upper half-closed
   */
  Interval(const minus_infinity_t&, T upper, const closed_t&)
      : Interval(adjust_bounds(
            BoundInfinity(), normalize_bound(upper, true, true))) {};

  /**
   * Upper-infinite set constructor: lower half-open
   */
  Interval(const open_t&, T lower, const plus_infinity_t&)
      : Interval(adjust_bounds(
            normalize_bound(lower, false, false), BoundInfinity())) {};

  /**
   * Upper-infinite set constructor: lower half-closed
   */
  Interval(const closed_t&, T lower, const plus_infinity_t&)
      : Interval(adjust_bounds(
            normalize_bound(lower, true, false), BoundInfinity())) {};

  /**
   * Bi-infinite set constructor
   */
  Interval(const minus_infinity_t&, const plus_infinity_t&) noexcept
      : lower_bound_(nullopt)
      , upper_bound_(nullopt)
      , is_empty_(false)
      , has_single_point_(false)
      , is_lower_open_(false)
      , is_lower_closed_(false)
      , is_lower_infinite_(true)
      , is_upper_open_(false)
      , is_upper_closed_(false)
      , is_upper_infinite_(true) {};

  [[nodiscard]] bool lower_bound_has_value() const noexcept {
    return lower_bound_.has_value();
  }
  [[nodiscard]] bool upper_bound_has_value() const noexcept {
    return upper_bound_.has_value();
  }
  /**
   * Accessor for the lower bound. Throws std::bad_optional_access if the
   * lower bound is nullopt.
   *
   * @precondition lower_bound_has_value()
   */
  T lower_bound() const {
    return lower_bound_.value();
  }
  /**
   * Accessor for the upper bound. Throws std::bad_optional_access if the
   * upper bound is nullopt.
   *
   * @precondition upper_bound_has_value()
   */
  T upper_bound() const {
    return upper_bound_.value();
  }
  [[nodiscard]] bool is_empty() const noexcept {
    return is_empty_;
  }
  [[nodiscard]] bool has_single_point() const noexcept {
    return has_single_point_;
  }
  [[nodiscard]] bool is_lower_bound_open() const noexcept {
    return is_lower_open_;
  }
  [[nodiscard]] bool is_lower_bound_closed() const noexcept {
    return is_lower_closed_;
  }
  [[nodiscard]] bool is_lower_bound_infinite() const noexcept {
    return is_lower_infinite_;
  }
  [[nodiscard]] bool is_upper_bound_open() const noexcept {
    return is_upper_open_;
  }
  [[nodiscard]] bool is_upper_bound_closed() const noexcept {
    return is_upper_closed_;
  }
  [[nodiscard]] bool is_upper_bound_infinite() const noexcept {
    return is_upper_infinite_;
  }
  /**
   * Predicate that an interval can be cut into two adjacent intervals, each
   * of which is not empty. This predicate is equivalent to an interval having
   * more than a single element.
   */
  [[nodiscard]] bool can_split_nontrivially() const noexcept {
    return !is_empty_ && !has_single_point_;
  }

  /**
   * Two intervals are equal if they are equal as sets.
   *
   * @param y The right-hand-side operand. This interval is the left-hand-side.
   */
  bool operator==(const Interval<T>& y) const {
    if (is_empty_ && y.is_empty_) {
      return true;
    } else if (is_empty_ || y.is_empty_) {
      return false;
    }
    return (BoundLower().compare_as_lower(y.BoundLower()) == 0) &&
           (BoundUpper().compare_as_upper(y.BoundUpper()) == 0);
  }

  /**
   * Compare to another interval. Convention is that this interval is the
   * operand on the left side of and the argument is that on the right side.
   *
   * The first return value is a three-way comparison value. Return is -1 if
   * this interval is disjoint and less than the argument. Return is +1 if this
   * interval is disjoint and greater than the argument. Return is 0 if this
   * interval and the argument have a non-empty intersection. The empty interval
   * is excluded from comparison.
   *
   * The second return value is a predicate that the intervals are adjacent. If
   * the first return is 0, then the second is always false, since intersecting
   * sets cannot be adjacent.
   *
   * Has much of the same logic as `interval_union`, but neither is reducible
   * to the other.
   */
  tuple<int, bool> compare(const Interval<T>& y) const {
    if (is_empty_) {
      throw std::domain_error(
          "Interval::compare - "
          "The empty set cannot be compared");
    } else if (y.is_empty_) {
      throw std::invalid_argument(
          "Interval::compare - "
          "The empty set cannot be compared");
    }

    // Calculate which of the two lower bounds is smaller than the other.
    Bound left_lower = BoundLower();
    Bound right_lower = y.BoundLower();
    int c_lower = left_lower.compare_as_lower(right_lower);
    // Calculate which of the two upper bounds is larger than the other.
    Bound left_upper = BoundUpper();
    Bound right_upper = y.BoundUpper();
    int c_upper = left_upper.compare_as_upper(right_upper);
    /*
     * Compare the greatest lower bound with the least upper bound. If the
     * result is positive, the intervals intersect.
     */
    Bound least_upper_bound = c_upper < 0 ? left_upper : right_upper;
    Bound greatest_lower_bound = c_lower < 0 ? right_lower : left_lower;
    int c_middle = least_upper_bound.compare_as_mixed(greatest_lower_bound);
    if (c_middle > 0) {
      return {0, false};
    }
    /*
     * At this point the intervals are disjoint, so we can use the comparison
     * for either upper or lower bounds; they're the same in this situation.
     *
     * If c_middle is zero, the intervals are adjacent.
     */
    return {c_lower < 0 ? -1 : +1, c_middle == 0};
  }

  /**
   * Membership predicate that the argument is an element of the interval.
   */
  bool is_member(T x) noexcept {
    if constexpr (Traits::has_unordered_elements) {
      if (!Traits::is_ordered(x)) {
        // An unordered element is not a member of any interval.
        return false;
      }
    }
    if (is_empty_) {
      // No element is a member of the empty set.
      return false;
    }
    // Check that the lower bound is satisfied.
    if (!is_lower_infinite_ && x < lower_bound_) {
      return false;
    }
    if (is_lower_open_ && x == lower_bound_) {
      return false;
    }
    // Check that the upper bound is satisfied.
    if (!is_upper_infinite_ && upper_bound_ < x) {
      return false;
    }
    if (is_upper_open_ && upper_bound_ == x) {
      return false;
    }
    return true;
  }

  /**
   * A three-way analog to `is_member`. Unlike that function, this one can have
   * invalid arguments. There's also a restriction on the domain; this interval
   * must not be empty, because we have no way of returning a meaningful result
   * in that case.
   *
   * @return 0 if is_member(x). negative if x is less than every member of this
   * interval. positive if x is greater than every member of this interval.
   *
   * @precondition This interval must not be empty.
   * @precondition Argument must be an ordered element.
   */
  int compare(T x) {
    if constexpr (Traits::has_unordered_elements) {
      if (!Traits::is_ordered(x)) {
        // An unordered element is not a member of any interval.
        throw std::invalid_argument(
            "Interval::compare - "
            "Unordered element cannot be compared.");
      }
    }
    if (is_empty_) {
      /*
       * The definition of < is universally quantified over set members. An
       * empty universal quantifier is always true, so for any interval I, both
       * ∅ < I and I < ∅. Since we can't return both +1 and -1 simultaneously,
       * we've excluded the empty set from the domain of this function.
       */
      throw std::domain_error(
          "Interval::compare - "
          "Empty set cannot be compared.");
    }
    // Check that the lower bound is satisfied. Return early if one is not.
    if (!is_lower_infinite_ && x < lower_bound_) {
      return -1;
    }
    if (is_lower_open_ && x == lower_bound_) {
      return -1;
    }
    // Check that the upper bound is satisfied.
    if (!is_upper_infinite_ && upper_bound_ < x) {
      return +1;
    }
    if (is_upper_open_ && upper_bound_ == x) {
      return +1;
    }
    return 0;
  }

  /**
   * Calculate the intersection of another interval with this one.
   */
  Interval<T> intersection(Interval<T> y) {
    /*
     * The empty set always has an empty intersection.
     */
    if (is_empty_ || y.is_empty_) {
      return Interval(empty_set);
    }
    /*
     * Because of adjacency, the bounds may be equal as sets but have different
     * representations. If this is the case, prefer a closed bound. A closed
     * bound has a greater value as a lower bound, what we want on the lower
     * side. An closed bound has a lesser value as an upper bound, what we want
     * on the upper side.
     */
    // Calculate which of the two lower bounds is larger than the other.
    Bound a = BoundLower();
    Bound b = y.BoundLower();
    int c = a.compare_as_lower(b);
    Bound greatest_lower_bound = (c < 0 || (c == 0 && b.is_closed_)) ? b : a;
    // Calculate which of the two upper bounds is smaller than the other.
    a = BoundUpper();
    b = y.BoundUpper();
    c = a.compare_as_upper(b);
    Bound least_upper_bound = (c < 0 || (c == 0 && a.is_closed_)) ? a : b;
    // Adjust the bounds as normal. Empty intersection is calculated here.
    return Interval(adjust_bounds(greatest_lower_bound, least_upper_bound));
  }

  /**
   * Calculate the union of another interval with this one.
   *
   * The set union of two intervals is always a set, but it's not always an
   * interval. This function calculates a union when the result is an interval
   * and returns `{false,nullopt}` when it's not.
   *
   * We can't call this function `union` because it's a reserved word.
   */
  tuple<bool, optional<Interval<T>>> interval_union(Interval<T> y) {
    /*
     * An empty set gives the identity function on the other operand.
     */
    if (is_empty_) {
      return {true, y};
    }
    if (y.is_empty_) {
      return {true, *this};
    }
    // Calculate which of the two lower bounds is smaller than the other.
    Bound left_lower = BoundLower();
    Bound right_lower = y.BoundLower();
    int c_lower = left_lower.compare_as_lower(right_lower);
    // Calculate which of the two upper bounds is larger than the other.
    Bound left_upper = BoundUpper();
    Bound right_upper = y.BoundUpper();
    int c_upper = left_upper.compare_as_upper(right_upper);
    /*
     * Compare the greatest lower bound with the least upper bound. If they're
     * separable, the union isn't defined. We don't need to favor closed bounds,
     * since they don't appear in the result.
     */
    Bound greatest_lower_bound = c_lower < 0 ? right_lower : left_lower;
    Bound least_upper_bound = c_upper < 0 ? left_upper : right_upper;
    if (least_upper_bound.compare_as_mixed(greatest_lower_bound) < 0) {
      return {false, nullopt};
    }
    Bound least_lower_bound =
        (c_lower < 0 || (c_lower == 0 && left_lower.is_closed_)) ? left_lower :
                                                                   right_lower;
    Bound greatest_upper_bound =
        (c_upper < 0 || (c_upper == 0 && right_upper.is_closed_)) ?
            right_upper :
            left_upper;
    return {
        true, Interval(adjust_bounds(least_lower_bound, greatest_upper_bound))};
  }

  /**
   * Cut this interval into two pieces at a given point.
   *
   * The default result is a pair of intersections of this interval with
   * `(-infinity,cut_point)` and `[cut_point,+infinity)`. If this interval is
   * empty, then both results are empty. If this interval is not empty, then
   * all the points of this interval are in one of the two results, and at most
   * one of the resulting intervals may be empty.
   *
   * The non-default result intersects with `(-infinity,cut_point]` and
   * `(cut_point,+infinity)`. It's the same but for the cut point, which moves
   * to the other side if it's an element of this interval.
   *
   * @param cut_point. An arbitrary point of T; it does not need to be a member
   * of this interval.
   *
   * @param lower_open_upper_closed. If true, cut with `(-infinity,cut_point)`
   * and `[cut_point,+infinity)`. If false, cut with `(-infinity,cut_point]`
   * and `(cut_point,+infinity)`.
   */
  tuple<Interval<T>, Interval<T>> cut(
      T cut_point, bool lower_open_upper_closed = true) {
    if (is_empty_) {
      /**
       * The empty set always splits into two empty sets.
       */
      return {Interval<T>(empty_set), Interval<T>(empty_set)};
    }
    // Assert: This interval is not empty.
    // This is a precondition for calling `compare_three_way` below.

    if constexpr (Traits::has_unordered_elements) {
      if (!Traits::is_ordered(cut_point)) {
        throw std::invalid_argument(
            "Interval::cut - "
            "Unordered element invalid as a cut point.");
      }
    }
    // Assert: cut_point is an ordered element
    // This is a precondition for calling `compare_three_way` below.

    int c;
    if constexpr (Traits::has_infinite_elements) {
      if (Traits::is_finite(cut_point)) {
        c = compare(cut_point);
      } else {
        /*
         * The two intervals for a positive infinite cut point are the whole set
         * (below) and the empty set (above); the order is reversed for negative
         * infinite. The result case is like the cut point being outside the
         * interval, but the outcome does not depend on set membership.
         */
        c = Traits::is_infinity_positive(cut_point) ? +1 : -1;
      }
      // Assert: c == 0 \implies cut_point is finite
    } else {
      c = compare(cut_point);
      // Assert: cut_point is finite
    }
    // Assert: c == 0 \implies cut_point is finite

    /*
     * A cut at a non-member element returns this interval and an empty
     * interval, the order depending on whether the cut point is above or
     * below the interval.
     */
    if (c > 0) {
      return {*this, Interval<T>(empty_set)};
    } else if (c < 0) {
      return {Interval<T>(empty_set), *this};
    }
    // Assert: c == 0 and thus cut_point is finite
    // Assert: c == 0 and thus cut_point is a member of this interval

    return {
        Interval(adjust_bounds(
            BoundLower(), Bound(cut_point, !lower_open_upper_closed))),
        Interval(adjust_bounds(
            Bound(cut_point, lower_open_upper_closed), BoundUpper()))};
  }

};  // namespace tiledb::common

}  // namespace tiledb::common

#endif  // TILEDB_INTERVAL_H
