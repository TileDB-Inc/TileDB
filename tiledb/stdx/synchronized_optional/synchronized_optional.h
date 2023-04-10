/**
 * @file tiledb/stdx/synchronized_optional/synchronized_optional.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file implements `synchronized_optional`, which is itself simply a
 * version of `optional` that's multiprocessing-safe. The main difference is
 * that `synchronized_optional` does not allow aliasing so that it can provide
 * atomicity without exposing a mutex.
 *
 * @par Comparison with `boost::synchronized_value`
 *
 * This class is similar in spirit to `boost::synchronized_value`, but with
 * notable differences. The main similarity is the use of an auxiliary reference
 * class that ensure locking coterminous with the duration of the lock. The
 * differences are in how these classes are implemented. The Boost version has
 * named classes for those references, and they expose the mutex type as a
 * template argument `Lockable`. The present version uses an opaque reference
 * class that not only does not expose the mutex but doesn't even require one.
 */

#ifndef TILEDB_SYNCHRONIZED_OPTIONAL_H
#define TILEDB_SYNCHRONIZED_OPTIONAL_H

#include <shared_mutex>
#include "controlled_reference.h"
#include "tiledb/common/common.h"

namespace stdx {

// Forward
template <class, template <class> class, class>
class controlled_reference;
template <class>
class synchronized_optional;

namespace detail {
/**
 * Predicate that two `synchronized_optional` objects are the same object. Used
 * in binary operations.
 *
 * This isn't just an optimization. We must not lock the same object twice.
 *
 * @tparam T The underlying type of the left hand side
 * @tparam U The underlying type of the right hand side
 * @param x Reference to the left hand side
 * @param y Reference to the right hand side
 * @return whether the two objects are the same
 */
template <class T, class U>
constexpr bool is_same_as(
    const synchronized_optional<T>& x, const synchronized_optional<U>& y) {
  return std::addressof(x) == std::addressof(y);
}

/*
 * Whitebox testing class forward declaration
 */
template <class>
class whitebox_synchronized_optional;

}  // namespace detail

/**
 * A synchronized version of `std::optional`
 *
 * The central principle of this class is atomicity of an optional value.
 * Atomicity is only available by means of a mutex internal to the class, so all
 * accesses to the object must happen through the class. In particular, in order
 * to preserve atomicty it must be impossible to obtain a direct reference (`T&`
 * or `T*`) to the underlying value without first holding a lock. Requiring a
 * user to explicitly lock during each access would be error-prone at best, so
 * `sychronized_optional` does not expose any way of directly obtaining such
 * a reference.
 *
 * Access is therefore possible only indirectly, as an object of an external
 * reference class. The reference object holds the required lock for the
 * lifespan of the reference object. It _is_ possible to obtain a direct
 * reference through the indirect reference object, but it is an error (meaning
 * undefined behavior) if the direct reference has a lifespan shorter than that
 * of the indirect reference object from which it was obtained.
 *
 * This class is meant to mimic the interface of `std::optional` insofar as that
 * is possible. Some of its operators are not needed at present; these are
 * either absent or in some cases deleted. Some of its operators are present but
 * with different return values, particularly all those around value access.
 *
 * \par Locking Policy
 *
 * The implementation of this class uses `std::shared_mutex` rather than
 * `std::mutex`. This choice allows both simultaneous access for multiple reads
 * and exclusive access for a single write. Reads use a shared lock and writes
 * use an exclusive lock.
 *
 * Even though simultaneous access is possible, it is usually a mistake to hold
 * a long-lived lock on this class by storing a long-lived external reference
 * object. For example, such a practice would interfere with partially shutting
 * down the program that requires resetting the value, a write operation. The
 * call to `reset` would block until the reference object was out of scope, a
 * situation that could deadlock.
 *
 * The external reference types, one for reading and one for writing, are scope-
 * oriented for this reason. They allow a reference to be easily maintained only
 * as long as is required. Reference objects should be declared separately as
 * variables that will be destroyed at the end of the block. Reference objects
 * declared as temporaries may be destroyed prematurely and should thus be
 * avoided.
 *
 * For example, suppose we have `class X { void f(): };` and a variable
 * `synchronized_optional<X> x;` known not to be null. The reference variable
 * would be declared as `auto ref{*x}` for writes and `const auto ref{*x}` for
 * reads. In a subsequent statement within the block `ref->f()` may appear.
 *
 * @par Comparison to `std::optional`
 *
 * `stdx::synchronized_optional` is intended to be as much like `std::optional`
 * as possible. The central necessary change is in how value references work,
 * as described above. This section outlines the differences.
 *
 * There are two blanket distinctions that apply universally. The class is
 * synchronized, so all operations must obtain a lock of some kind; both
 * distinctions arise as a consequence from this. The first is that nothing in
 * this class is `noexcept`, not even when the underlying class might otherwise
 * allow it. The second is that nothing is `constexpr`, which is as C++20 does.
 * Synchronized objects are only relevant if you need to change them.
 *
 * **Constructor**. The constructors are mostly the same, with a few
 * exceptions.
 * - The converting constructs are not implemented for lack of need, but could
 *   be. Same for the in-place constructor with initializer list.
 *
 * **Destructor**. Destroying a `synchronized_optional` while there is an
 * outstanding reference object to it is undefined behavior.
 *
 * **Assignment**. The differences here are analogous to those in the
 * constructors. See above.
 *
 * **Dereference and `value`**. The `operator*`, and `value` do not return
 * direct pointers or references but rather pointer/reference objects.
 * `value_or` is not implemented for lack of need.
 *
 * The rvalue-ref-qualified versions of these are explicitly deleted, however.
 * Reference objects to temporaries will almost certainly lead to undefined
 * behavior, since the destructor of the `synchronized_optional` runs when the
 * expression goes out of scope. Similarly, `operator->` is deleted; see below.
 *
 * **Value presence**. `has_value` and `operator bool` are identical but for
 * not having `noexcept`.
 *
 * **Modifiers**. `swap` and  `reset` are identical but for `noexcept` and
 * `constexpr`. The ordinary `emplace` returns an external reference object
 * instead of a C++ reference. The type-converting `emplace` is not implemented
 * for lack of need.
 *
 * **Comparisons**. Comparisons are just like those in `std::optional`. They're
 * defined as non-member functions.
 *
 * @par On the absence of `operator->`
 *
 * `synchronized_optionat::operator->` is deleted because its actual behavior
 * does not match its idiomatic meaning. C++ scoping rules for expressions do
 * not extend the lifespan of a temporary except in certain circumstances. These
 * circumstances do not include the evaluation of expressions of the form
 * `a->b()`. At present the consequence of this is the following sequence of
 * operations:
 * - Call `a.operator->`, which constructs an object of type
 *   `a::reference_type` and acquires a lock.
 * - Call `a::reference_type::operator*`, which returns an object reference.
 * - The reference object goes out of scope, so its destructor executes and
 *   releases the lock.
 * - Call `ref.b()`, which is called not under lock, that is, without
 *   synchronization. That's the wrong behavior for something called
 *   `synchronized_optional`.
 *
 * The expectation that `b` execute under lock in the expression `a->b()` is the
 * reason to delete `operator->`. It's contrary to expectations of how such an
 * expression should function.
 *
 * It would take a change in the language definition for `operator->` to work
 * reliably across compiler platforms. The issue is that there's no explicit
 * lifespan extension defined for the evaluation of `operator->` that mandates
 * that its lifespan extend past the duration of the function. The compiler is
 * free to resolve to a direct object reference `x` before executing `x.b()`.
 *
 * @tparam T The underlying object is of type `T`
 */
template <class T>
class synchronized_optional {
  /**
   * Friend declaration allows whitebox testing with private class members.
   */
  friend class detail::whitebox_synchronized_optional<T>;

  /**
   * The lock used to synchronize read access to `base_`.
   */
  using read_lock_type = std::shared_lock<std::shared_mutex>;
  /**
   * The lock used to synchronize write access to `base_`.
   */
  using write_lock_type = std::lock_guard<std::shared_mutex>;
  /**
   * The lock used where a write lock must be acquired before anything else
   * happens but then subsequently tranferred to a write handle.
   */
  using transferrable_write_lock_type = std::unique_lock<std::shared_mutex>;

  // Forward
  template <class, class>
  class handle;

  using read_handle_type = handle<T, read_lock_type>;
  using write_handle_type = handle<T, write_lock_type>;

  /**
   * Read traits for the controlled reference. `attach()` returns a read handle.
   */
  class CR_read_traits {
   public:
    using handle_type = read_handle_type;
    static handle_type attach(const synchronized_optional& source) {
      return source.read_attach();
    }
  };

  /**
   * Write traits for the controlled reference. `attach()` returns a write
   * handle.
   */
  class CR_write_traits {
   public:
    using handle_type = write_handle_type;
    static handle_type attach(synchronized_optional& source) {
      return source.write_attach();
    }
    static handle_type attach(
        synchronized_optional& source, std::adopt_lock_t) {
      return source.write_attach(std::adopt_lock);
    }
  };

 public:
  /**
   * The non-`const` reference class
   */
  using reference_type =
      controlled_reference<T, synchronized_optional, CR_write_traits>;

  /**
   * The `const` reference class
   */
  using const_reference_type =
      controlled_const_reference<T, synchronized_optional, CR_read_traits>;

 private:
  /**
   * Internal handle class for use with `controlled_reference`.
   *
   * This class holds a lock on the mutex within `synchronized_optional`. The
   * lock type `L` specifies either a read lock or a write lock. The lock is
   * created before the handle is created and then ownership is transferred to
   * the lock. As a result, either no write lock exists and multiple read locks
   * may exist simultaneously, or exactly one write lock exists.
   *
   * This class is used for both `const` and non-`const` handles, using
   * `const_cast` to ignore type differences. `const` correctness is provided
   * through `controlled_const_reference` and there's no reason to enforce it in
   * the handle as well.
   */
  template <class U, class L>
  class handle {
    /**
     * The underlying object to which this handle refers.
     *
     * Its type is a reference rather than a pointer. Should this object outlive
     * its referent (not a correct use, but syntactically possible), this
     * reference becomes invalid and the object will have undefined behavior.
     */
    synchronized_optional& referent_;

    /**
     * A lock on the referent held for the duration of this object's life span.
     *
     * It's marked `maybe_unused` because, as a sentry object, it's initialized
     * for its side effects and never referenced explicitly thereafter. The
     * marking is commented out for now because gcc emits a warning that it's
     * ignoring the attribute, a warning that escalates to an error in CI.
     */
    /*[[maybe_unused]]*/ L lock_;

   public:
    /**
     * Non-`const` handle constructor.
     *
     * @pre The parent mutex is locked and not governed by any lock object.
     *
     * @param x Parent object that will become the referent of this handle
     */
    explicit handle(synchronized_optional& x)
        : referent_(x)
        , lock_(x.m_, std::adopt_lock) {
    }

    /**
     * Handle constructor must execute only under lock.
     *
     * @pre The parent mutex is locked and not governed by any lock object.
     *
     * @param x Parent object that will become the referent of this handle
     */
    explicit handle(const synchronized_optional& x)
        : referent_(const_cast<synchronized_optional&>(x))
        , lock_(x.m_, std::adopt_lock) {
    }

    /**
     * Default constructor is deleted. This handle must refer to an object; it
     * may not be empty.
     */
    handle() = delete;

    /**
     * Copy constructor is deleted.
     *
     * It would be possible to implement a copy constructor for read handles,
     * but we don't need it. It's not possible to implement a copy constructor
     * for write handles, since there can be only one at a time.
     */
    handle(const handle&) = delete;

    /**
     * Move constructor is deleted.
     *
     * It would be tricky and possibly problematic to implement a move
     * constructor with transfer semantics. We avoid doing that because we don't
     * need it. The handle is supposed to be coterminous with some lock, but
     * with move-transfers there's a small interval between the point of the
     * move and the point of destruction that would violate this invariant.
     */
    handle(handle&&) = delete;

    /**
     * Copy assignment is deleted. See also the copy constructor for more.
     */
    handle& operator=(const handle&) = delete;

    /**
     * Move assignment is deleted. See also the move constructor for more.
     */
    handle& operator=(handle&&) = delete;

    U& operator*() const {
      return referent_.base_.operator*();
    }

    U* operator->() = delete;
  };

  /**
   * The type of the `base_` object that is the subject of synchronization.
   */
  using base_type = optional<T>;

  /**
   * The underlying `optional` object.
   *
   * It's a member variable rather than deriving by inheritance because
   * everything in `optional` needs either to be (1) reimplemented under lock
   * or (2) moved to the reference class.
   */
  base_type base_;

  /**
   * Mutex protects the atomicity of initialization and assignment to the
   * base `optional`.
   */
  mutable std::shared_mutex m_{};

  /**
   * Attach an access handle to this object.
   *
   * Note that the handle constructor must be in the last statement for two
   * reasons. (1) The lock is adopted in the constructor, so further statements
   * would not happen under lock. (2) We require RVO to avoid copying a lock
   * in the handle.
   *
   * Note that we don't keep track of existing handles in this class. We rely
   * on the lock held within the handle to unlock at the end of its lifespan.
   */
  write_handle_type write_attach() {
    transferrable_write_lock_type lock(m_);
    lock.release();  // dissociate the mutex without unlocking it
    return write_handle_type{*this};
  }

  write_handle_type write_attach(std::adopt_lock_t) {
    return write_handle_type{*this};
  }

  read_handle_type read_attach() const {
    read_lock_type lock(m_);
    lock.release();  // dissociate the mutex without unlocking it
    return read_handle_type{*this};
  }

  /**
   * Copy the underlying `optional` object.
   *
   * Note that this does not violate the "no alias" principle of this class
   * because it's constructing a copy of the object; it does not provide a
   * reference to it.
   *
   * @return a copy of the underlying optional<T>
   */
  inline const base_type& self_copy() const {
    read_lock_type lock(m_);
    /*
     * Explicit invoke the copy constructor on the base object. The copy will
     * be completed under lock. There's mandatory RVO on the temporary copy.
     */
    return {base_};
  }

  /**
   * Copy the underlying `optional` object.
   *
   * Note that this does not violate the "no alias" principle of this class
   * because it's constructing a copy of the object; it does not provide a
   * reference to it.
   *
   * @return a copy of the underlying optional<T>
   */
  inline base_type&& self_move() {
    read_lock_type lock(m_);
    /*
     * Explicitly invoke the move constructor on the base object. The move will
     * be completed under lock. There's mandatory RVO on the temporary copy.
     */
    return move(base_);
  }

 public:
  /**
   * Default constructor creates a null object
   */
  synchronized_optional() = default;

  /**
   * Explicit null object constructor
   */
  inline explicit synchronized_optional(std::nullopt_t)
      : synchronized_optional{} {
  }

  /**
   * In-place constructor
   *
   * @tparam Args
   * @param args
   */
  template <class... Args>
  inline explicit synchronized_optional(std::in_place_t, Args&&... args)
      : base_(std::in_place, std::forward<Args>(args)...) {
  }

  /**
   * Copy constructor
   */
  inline synchronized_optional(const synchronized_optional& x)
      : base_{x.self_copy()} {
    /*
     * The synchronization invariants here are subtle, since there are two of
     * them, one on this object and one on the argument. `self` operates
     * atomically on the argument `x`, so its return value is coherent. This
     * object is being initialized, though, so nothing else can yet have a
     * reference that might cause a synchronization issue.
     *
     * The behavior of synchronization for other constructors and assignments is
     * the analogous to here.
     */
  }

  /**
   * Move constructor
   *
   * Note that this move constructor is *not* `noexcept` because of possible
   * locking exceptions.
   */
  // NOLINTNEXTLINE(performance-noexcept-move-constructor)
  inline synchronized_optional(synchronized_optional&& x)
      : base_{move(x.self_move())} {
  }

  /**
   * Null assignment
   *
   * Note that, unlike the corresponding member function in `optional`, we can't
   * make this `noexcept` because locking is involved.
   */
  inline synchronized_optional& operator=(std::nullopt_t) {
    write_lock_type lock(m_);
    base_.reset();
    return *this;
  }

  /**
   * Copy assignment
   */
  inline synchronized_optional& operator=(const synchronized_optional& x) {
    write_lock_type lock(m_);
    base_ = x.self_copy();
    return *this;
  }

  /**
   * Move assignment
   *
   * Note that this move assignment is *not* `noexcept` because of possible
   * locking exceptions.
   */
  // NOLINTNEXTLINE(performance-noexcept-move-constructor)
  inline synchronized_optional& operator=(synchronized_optional&& x) {
    write_lock_type lock(m_);
    base_ = move(x.self_move());
    return *this;
  }

  /**
   * Construct a new value in-place, replacing any existing one.
   *
   * Note that emplace, although it has side effects like assignment, has a
   * different return value convention. It returns a reference to the newly-
   * created object rather than a reference to the object as a whole. In our
   * case that means it returns a reference object, which extends the duration
   * of the exclusive write-lock until the end of the expression.
   *
   * @tparam Args Argument types of a `T` constructor
   * @param args Arguments to a `T` constructor
   * @return reference object to the underlying value
   */
  template <class... Args>
  reference_type emplace(Args&&... args) {
    transferrable_write_lock_type lock(m_);
    static_cast<void>(base_.emplace(std::forward<Args>(args)...));
    lock.release();
    return reference_type{*this, std::adopt_lock};
  }

  /*
   * Dereference and value functions
   */

  /**
   * Return a constant reference to the present object.
   */
  const_reference_type operator*() const& {
    return const_reference_type{*this};
  }

  /**
   * Return a reference to the present object.
   */
  reference_type operator*() & {
    return reference_type{*this};
  }

  /**
   * `operator->` is deleted because its actual behavior cannot match its
   * idiomatic usage.
   */
  const_reference_type operator->() const = delete;

  /**
   * `operator->` is deleted because its actual behavior cannot match its
   * idiomatic usage.
   */
  reference_type operator->() = delete;

  /**
   * References to temporaries are not allowed.
   */
  const_reference_type&& operator*() const&& = delete;

  /**
   * References to temporaries are not allowed.
   */
  reference_type&& operator*() && = delete;

  /**
   * Return a writable reference to the value
   */
  reference_type value() & {
    return reference_type{*this};
  }

  const_reference_type value() const& {
    return const_reference_type{*this};
  }

  /**
   * Return a read-only reference to a non-const value.
   *
   * This function does not have an analogous call in `optional`. In that class
   * there's no need for it, since there's no lock to maintain.
   */
  const_reference_type const_value() & {
    return const_reference_type{*this};
  }

  /**
   * Writable references to temporaries are not allowed.
   *
   * An ordinary reference is writable. A temporary will usually be out of scope
   * before a reference to it would have a chance to be used. This reflects a
   * design decision in the present class not to have `synchronized_optional`
   * reach out it its references on destruction and invalidate them. This would
   * be possible in a class that had bidirectional navigation between objects
   * and reference; this class only has one direction.
   */
  reference_type value() && = delete;

  /**
   * Return the underlying value as an rvalue.
   *
   * Note that this function, even though it's defined for rvalues, still needs
   * to acquire a read-lock on the present object. Unlike for a writable
   * reference, though, the lock is released before this function returns.
   */
  const T&& value() const&& {
    read_lock_type lock(m_);
    return move(base_).value();
  }

  /**
   * Return the underlying value as an rvalue.
   *
   * See the documentation for `const T&& value() const&&`.
   */
  const T&& const_value() && {
    read_lock_type lock(m_);
    return move(base_).value();
  }

  /*
   * Predicates for value presence
   */
  /**
   * Predicate that this object contains a value.
   */
  // NOLINTNEXTLINE(google-explicit-constructor)
  inline operator bool() const {
    read_lock_type lock(m_);
    return base_.operator bool();
  }

  /**
   * Predicate that this object contains a value.
   */
  inline bool has_value() const {
    read_lock_type lock(m_);
    return base_.has_value();
  }

  /*
   * Modifiers
   */
 private:
  /**
   * Swapping values requires acquiring write locks on both objects.
   *
   * Currently disabled because of deadlocks between `scoped_lock` and other
   * locks.
   */
  void swap(synchronized_optional& x) {
    /*
     * Swapping with oneself does nothing.
     */
    if (detail::is_same_as(*this, x)) {
      return;
    }
    /*
     * It's necessary to use `scoped_lock` here for its deadlock avoidance.
     * Consider objects `A` and `B`, which might be referred to by other names.
     * If `swap(A,B)` executes in one thread while `swap(B,A)` executes in
     * another, a naive locking strategy can deadlock.
     *
     * Note that `scoped_lock` calls the `lock` method of the mutex, which
     * for `shared_mutex` is the exclusive lock.
     */
    std::scoped_lock lock{m_, x.m_};
    // Assert: we have write locks on both objects
    base_.swap(x.base_);
  };

 public:
  /**
   * Reset is equivalent to assigning `nullopt`.
   */
  inline void reset() {
    write_lock_type lock(m_);
    base_.reset();
  }

  /*
   * Friend declarations for comparison operators. We need these so that they
   * can lock their operands.
   */
  // Equality
  template <class U, class V>
  friend bool operator==(
      const synchronized_optional<U>& x, const synchronized_optional<V>& y);
  template <class U>
  friend bool operator==(const synchronized_optional<U>& x, std::nullopt_t);
  template <class V>
  friend bool operator==(std::nullopt_t, const synchronized_optional<V>& y);
  template <class U, class V>
  friend bool operator==(const synchronized_optional<U>& x, const V& y);
  template <class U, class V>
  friend bool operator==(const U& x, const synchronized_optional<V>& y);
  // Inequality
  template <class U, class V>
  friend bool operator!=(
      const synchronized_optional<U>& x, const synchronized_optional<V>& y);
  template <class U>
  friend bool operator!=(const synchronized_optional<U>& x, std::nullopt_t);
  template <class V>
  friend bool operator!=(std::nullopt_t, const synchronized_optional<V>& y);
  template <class U, class V>
  friend bool operator!=(const synchronized_optional<U>& x, const V& y);
  template <class U, class V>
  friend bool operator!=(const U& x, const synchronized_optional<V>& y);
  // Less than
  template <class U, class V>
  friend bool operator<(
      const synchronized_optional<U>& x, const synchronized_optional<V>& y);
  template <class U>
  friend bool operator<(const synchronized_optional<U>& x, std::nullopt_t);
  template <class V>
  friend bool operator<(std::nullopt_t, const synchronized_optional<V>& y);
  template <class U, class V>
  friend bool operator<(const synchronized_optional<U>& x, const V& y);
  template <class U, class V>
  friend bool operator<(const U& x, const synchronized_optional<V>& y);
  // Less than or equal to
  template <class U, class V>
  friend bool operator<=(
      const synchronized_optional<U>& x, const synchronized_optional<V>& y);
  template <class U>
  friend bool operator<=(const synchronized_optional<U>& x, std::nullopt_t);
  template <class V>
  friend bool operator<=(std::nullopt_t, const synchronized_optional<V>& y);
  template <class U, class V>
  friend bool operator<=(const synchronized_optional<U>& x, const V& y);
  template <class U, class V>
  friend bool operator<=(const U& x, const synchronized_optional<V>& y);
  // Greater than
  template <class U, class V>
  friend bool operator>(
      const synchronized_optional<U>& x, const synchronized_optional<V>& y);
  template <class U>
  friend bool operator>(const synchronized_optional<U>& x, std::nullopt_t);
  template <class V>
  friend bool operator>(std::nullopt_t, const synchronized_optional<V>& y);
  template <class U, class V>
  friend bool operator>(const synchronized_optional<U>& x, const V& y);
  template <class U, class V>
  friend bool operator>(const U& x, const synchronized_optional<V>& y);
  // Greater than or equal to
  template <class U, class V>
  friend bool operator>=(
      const synchronized_optional<U>& x, const synchronized_optional<V>& y);
  template <class U>
  friend bool operator>=(const synchronized_optional<U>& x, std::nullopt_t);
  template <class V>
  friend bool operator>=(std::nullopt_t, const synchronized_optional<V>& y);
  template <class U, class V>
  friend bool operator>=(const synchronized_optional<U>& x, const V& y);
  template <class U, class V>
  friend bool operator>=(const U& x, const synchronized_optional<V>& y);
};

/*
 * Comparison operators
 *
 * There are thirty comparison operators, a combinatorial product of the
 * following:
 * - (3) comparisons: less than, equal, greater than
 * - (2) negations
 * - (5) argument signatures, the sum of
 *   - (1) `synchronized_optional` vs. itself (symmetric)
 *   - (2) `synchronized_optional` vs. `nullopt`
 *   - (2) `synchronized_optional` vs. underlying type
 */

/*
 * Equality
 */
template <class T, class U>
bool operator==(
    const synchronized_optional<T>& x, const synchronized_optional<U>& y) {
  if (detail::is_same_as(x, y)) {
    return true;
  }
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return x.base_ == y.base_;
}

template <class T>
bool operator==(const synchronized_optional<T>& x, std::nullopt_t) {
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  return x.base_ == std::nullopt;
}

template <class U>
bool operator==(std::nullopt_t, const synchronized_optional<U>& y) {
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return std::nullopt == y.base_;
}

template <class T, class U>
bool operator==(const synchronized_optional<T>& x, const U& y) {
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  return x.base_ == y;
}

template <class T, class U>
bool operator==(const T& x, const synchronized_optional<U>& y) {
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return x == y.base_;
}

/*
 * Inequality
 */
template <class T, class U>
bool operator!=(
    const synchronized_optional<T>& x, const synchronized_optional<U>& y) {
  if (detail::is_same_as(x, y)) {
    return false;
  }
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return x.base_ != y.base_;
}

template <class T>
bool operator!=(const synchronized_optional<T>& x, std::nullopt_t) {
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  return x.base_ != std::nullopt;
}

template <class U>
bool operator!=(std::nullopt_t, const synchronized_optional<U>& y) {
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return std::nullopt != y.base_;
}

template <class T, class U>
bool operator!=(const synchronized_optional<T>& x, const U& y) {
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  return x.base_ != y;
}

template <class T, class U>
bool operator!=(const T& x, const synchronized_optional<U>& y) {
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return x != y.base_;
}

/*
 * Less than
 */
template <class T, class U>
bool operator<(
    const synchronized_optional<T>& x, const synchronized_optional<U>& y) {
  if (detail::is_same_as(x, y)) {
    return false;
  }
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return x.base_ < y.base_;
}

template <class T>
bool operator<(const synchronized_optional<T>& x, std::nullopt_t) {
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  return x.base_ < std::nullopt;
}

template <class U>
bool operator<(std::nullopt_t, const synchronized_optional<U>& y) {
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return std::nullopt < y.base_;
}

template <class T, class U>
bool operator<(const synchronized_optional<T>& x, const U& y) {
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  return x.base_ < y;
}

template <class T, class U>
bool operator<(const T& x, const synchronized_optional<U>& y) {
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return x < y.base_;
}

/*
 * Less than or equal to
 */
template <class T, class U>
bool operator<=(
    const synchronized_optional<T>& x, const synchronized_optional<U>& y) {
  if (detail::is_same_as(x, y)) {
    return true;
  }
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return x.base_ <= y.base_;
}

template <class T>
bool operator<=(const synchronized_optional<T>& x, std::nullopt_t) {
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  return x.base_ <= std::nullopt;
}

template <class U>
bool operator<=(std::nullopt_t, const synchronized_optional<U>& y) {
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return std::nullopt <= y.base_;
}

template <class T, class U>
bool operator<=(const synchronized_optional<T>& x, const U& y) {
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  return x.base_ <= y;
}

template <class T, class U>
bool operator<=(const T& x, const synchronized_optional<U>& y) {
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return x <= y.base_;
}

/*
 * Greater than
 */
template <class T, class U>
bool operator>(
    const synchronized_optional<T>& x, const synchronized_optional<U>& y) {
  if (detail::is_same_as(x, y)) {
    return false;
  }
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return x.base_ > y.base_;
}

template <class T>
bool operator>(const synchronized_optional<T>& x, std::nullopt_t) {
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  return x.base_ > std::nullopt;
}

template <class U>
bool operator>(std::nullopt_t, const synchronized_optional<U>& y) {
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return std::nullopt > y.base_;
}

template <class T, class U>
bool operator>(const synchronized_optional<T>& x, const U& y) {
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  return x.base_ > y;
}

template <class T, class U>
bool operator>(const T& x, const synchronized_optional<U>& y) {
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return x > y.base_;
}

/*
 * Greater than or equal to
 */
template <class T, class U>
bool operator>=(
    const synchronized_optional<T>& x, const synchronized_optional<U>& y) {
  if (detail::is_same_as(x, y)) {
    return true;
  }
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return x.base_ >= y.base_;
}

template <class T>
bool operator>=(const synchronized_optional<T>& x, std::nullopt_t) {
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  return x.base_ >= std::nullopt;
}

template <class U>
bool operator>=(std::nullopt_t, const synchronized_optional<U>& y) {
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return std::nullopt >= y.base_;
}

template <class T, class U>
bool operator>=(const synchronized_optional<T>& x, const U& y) {
  typename synchronized_optional<T>::read_lock_type lock_x(x.m_);
  return x.base_ >= y;
}

template <class T, class U>
bool operator>=(const T& x, const synchronized_optional<U>& y) {
  typename synchronized_optional<U>::read_lock_type lock_y(y.m_);
  return x >= y.base_;
}

}  // namespace stdx

#endif  // TILEDB_SYNCHRONIZED_OPTIONAL_H
