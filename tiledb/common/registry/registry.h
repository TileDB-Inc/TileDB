/**
 * @file tiledb/common/registry/registry.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * This file defines the registry classes. As they are all template classes, the
 * full definitions are herein.
 */

#ifndef TILEDB_REGISTRY_H
#define TILEDB_REGISTRY_H

#include <algorithm>
#include <list>
#include <memory>
#include <mutex>
#include <stdexcept>

namespace tiledb::common {

// Forward declaration for RegistryEntry
template <class T>
class Registry;

/**
 * An entry contained with an instance of `class Registry<T>`.
 *
 * A registry entry allows an item to refer to itself directly without
 * requiring a container look-up.
 *
 * This class is declared as `struct` because it's an internal type only
 * visible to `class Registry` and `class RegistryHandle`. Since those two
 * classes are friends, there's no particular benefit to private members here.
 */
template <class T>
struct RegistryEntry {
  using registry_type = Registry<T>;

  /**
   * The item that this entry refers to
   *
   * Note that this reference alone does not guarantee validity of the referent
   * object, which might not have returned from its constructor yet.
   */
  T& item_;

  /**
   * A non-owning pointer to the item
   *
   * This member variable is default-initialized to owning nothing. It's
   * assigned to directly in `RegistryHandle<T>::register_shared_ptr`.
   */
  std::weak_ptr<T> item_ptr_{};

  /**
   * The registry that this entry occurs within
   *
   * @section Design
   *
   * It might seem wasteful to keep a reference to a registry as a member
   * variable here, since instances of this type are held only in a container of
   * that same registry. If it weren't present here, it would have to be present
   * somehow within items of `class T`, either explicitly as a member variable
   * or implicitly as a member of a handle member variable (of a different type
   * than used here).
   *
   * This decision puts all the complexity of implementation on one side of the
   * interface. Registered item instances only need to do two things:
   * - Define a handle member variable
   * - Initialize that handle in the constructor by registering itself
   * Removal of an item from the registry will then happen automatically during
   * the destruction of its corresponding handle.
   */
  registry_type& registry_;

  /**
   * The type to which this handle refers
   */
  using value_type = T;

  /**
   * There's no item handle without an item.
   */
  RegistryEntry() = delete;

  /**
   * Ordinary constructor
   *
   * @param i The item to register
   * @param r The registry that will contain the item
   */
  RegistryEntry(T& i, registry_type& r)
      : item_(i)
      , registry_(r) {
  }

  /**
   * The destructor is default
   */
  ~RegistryEntry() = default;
};

// Forward for `class RegistryHandle`
template <class T>
class RegistryHandle;

/**
 * A synchronized registry
 *
 * The registry holds references to its registered items, not the items
 * themselves.
 */
template <class T>
class Registry {
  /**
   * `class RegistryHandle<T>` is a friend because it needs to know the iterator
   * type of the container used to implement the registry
   */
  friend class RegistryHandle<T>;

  /**
   * Mutex that synchronizes `registry_list_`
   */
  mutable std::mutex m_;

  /**
   * Condition variable used to implement alarms on registry size. We notify
   * every time we change the size of the registry.
   */
  mutable std::condition_variable cv_;

  /**
   * The type of the scope-based lock used throughout the implementation. This
   * class only rarely does anything complicated enough to need something more.
   */
  using lock_guard_type = std::lock_guard<std::mutex>;

  /**
   * The type of the container used to hold registry entries.
   */
  using registry_list_type = std::list<RegistryEntry<T>>;

  /**
   * The iterator type is only visible to friends.
   */
  using registry_iterator_type = registry_list_type::iterator;

  /**
   * List of entries with this registry
   *
   * Note that presence of an entry within this list does _not_ guarantee
   * validity of the item within the entry. The reason for this is that the
   * handle, as a member variable of the item, is initialized within its
   * constructor. Since creation of a handle requires adding an entry to this
   * container, entries appear before their constructors return.
   *
   * This property is necessarily true of any registration system that operates
   * at construction time. While it's an inherent limitation, it's better than
   * performing all registration outside the class and relying on user code to
   * be fully reliable. All that's necessary to complete registration is a
   * single call on the handle after the constructor returns. In the present
   * case that call is `RegistryHandle<T>::register_shared_ptr`.
   *
   * It's also worth noting that entries within this container never refer to
   * stale objects, ones whose destructors have already run. That's because the
   * handle, as a member variable, is destroyed before the destructor returns.
   */
  registry_list_type registry_list_;

 public:
  /**
   * The type of the item within a registry entry.
   */
  using value_type = T;

  /**
   * The return type of `size()` is forwarded from its underlying container.
   */
  using size_type = registry_list_type::size_type;

  /**
   * The handle type. A registered class should have a member variable of this
   * type for auto-deregistration at destruction.
   */
  using registry_handle_type = RegistryHandle<T>;

  /**
   * Create an entry within this registry
   *
   * @param item A reference to the item to be entered
   * @return A handle to the new entry within this registry
   * @postcondition The item within the return value refers to the argument
   */
  const registry_handle_type register_item(value_type& item) {
    lock_guard_type lg(m_);
    registry_list_.emplace_back(item, *this);
    cv_.notify_all();   // `emplace_back` adds one to the registry size
    /*
     * `emplace_back` returns a reference to the list member, not its iterator.
     * We have to construct an iterator instead.
     */
    return registry_handle_type(--registry_list_.end());  // mandatory RVO
  }

  /**
   * The current number of entries within the registry
   *
   * @section Caution
   *
   * This isn't a high-performance function, because it acquires a lock for
   * every call. It's not advised to poll this function.
   */
  size_type size() const noexcept {
    /*
     * We're locking because the standard library does not offer a guarantee
     * that `size()` is atomic
     */
    lock_guard_type lg(m_);
    return registry_list_.size();
  }

  /**
   * Block until the registry is empty.
   */
  void wait_for_empty() const {
    std::unique_lock<std::mutex> lck(m_);
    cv_.wait(lck, [this]() { return registry_list_.size() == 0; });
  }

  /**
   * Iterate over each element and apply a function to each.
   *
   * Caution: This function holds a lock during the operation. For long-running
   * operations, use this function only to dispatch to other threads.
   *
   * @tparam F A unary function `void f(value_type &)`
   * @param f A callable object of type F
   */
  template <class F>
  void for_each(F f) const
  {
    lock_guard_type lg(m_);
    /*
     * We iterate over entries, but apply the function to valid items. Thus we
     * need to provide `for_each` with an adapter.
     */
    auto g{
        [&f](const RegistryEntry<T>& entry) -> void {
          /*
           * We can only iterate over items that have registered `shared_ptr`.
           * Otherwise we might iterate over a dangling reference.
           */
          auto item_ptr{entry.item_ptr_.lock()};
          if (item_ptr) {
            f(*item_ptr);
          }
        }
    };
    std::for_each(
        registry_list_.begin(), registry_list_.end(), g);
  }

 private:
  /**
   * Remove an entry from this registry.
   *
   * @section Design
   *
   * This function is private so that it's called only by `RegistryHandle<T>`,
   * which does so in its destructor. Calling this function in any other
   * circumstance would invalidate the iterator held inside of the handle. This
   * would cause a violation of the class invariant of `RegistryHandle` that
   * the handle is always valid.
   *
   * @param iter Iterator to the entry to remove from this registry.
   */
  void deregister(registry_iterator_type& iter) {
    lock_guard_type lg(m_);
    registry_list_.erase(iter);
    cv_.notify_all();   // `erase` subtracts one from the registry size
  }
};

/**
 * Handle for an entry within a registry
 *
 * Handles are values and should be passed by value, not by reference and not
 * through a pointer. The handle encapsulates a referential object whose details
 * are opaque outside the class.
 *
 * @section Design
 *
 * The class invariant that the handle always refers to something might look
 * bland, but it's crucial. The invariant says that we may have neither empty
 * handles (referring to no object) nor dangling handles (referring to a
 * destroyed object).
 *
 * @invariant The handle refers to an item inside the registry.
 * @tparam T class whose instances have entries within a registry
 */
template <class T>
class RegistryHandle {
  /*
   * `class Registry<T>` is a friend so that `class T` sees only the public
   * interface of this handle class, and in particular not its constructor.
   * The registration function of the registry acts as a factory for this class
   * and is the only way to construct objects of this type.
   */
  friend class Registry<T>;

  /**
   * This class is a wrapper around an instance of this referential type.
   *
   * The type is an iterator into the internal container of the registry. At the
   * end of the duration where an item is registered, an iterator allows
   * immediate erasure of the entry; there's no separate index or other
   * secondary structure required.
   */
  using referential_type = Registry<T>::registry_iterator_type;

  /**
   * The iterator that comprises the underlying data of this handle.
   */
  referential_type iter_;

 public:
  /**
   * Default constructor is deleted.
   *
   * There's no such this as a registry handle without a corresponding entry in
   * the registry.
   */
  RegistryHandle() = delete;

 private:
  /**
   * Ordinary constructor is ordinary.
   *
   * @param iter An iterator to the internal container of a registry
   */
  explicit RegistryHandle(const referential_type& iter)
      : iter_(iter) {
  }

 public:
  /**
   * Destroying a handle removes its corresponding entry in its origin registry.
   */
  ~RegistryHandle() {
    iter_->registry_.deregister(iter_);
  }

  /**
   * Register a shared_ptr to an item already in the registry.
   *
   * Registration is optional for construction of a handle, but required to
   * access the original item. This is a consequence of the life cycle
   * assumptions, since the handle can exist before the construction of its item
   * has completed.
   *
   * Note that we can't write a postcondition that the item has a shared_ptr
   * already registered to it. That's to account for the possible, albeit very
   * unlikely, situation that (1) the destructor for the shared_ptr that the one
   * that the argument was copied from runs, and (2) that it was the last
   * shared_ptr to its referent, and thus (3) that the use count on the control
   * block goes to zero before the call returns.
   *
   * @pre This function has not been called before on this handle
   * @pre `ptr` is not null
   * @pre The `ptr` argument must point to the same object as the item that this
   *   handle refers to
   * @param ptr A shared_ptr to (putatively) the same item that's in the handle
   * @throws std::logic_error if there's already a shared_ptr registered
   * @throws std::invalid_argument if the precondition is not met
   */
  void register_shared_ptr(std::shared_ptr<T> ptr) {
    /*
     * Note that this code is implemented here, in the handle class, rather than
     * in the entry class itself, where it would be more natural. This is an
     * optimization to avoid a possible copy of the `shared_ptr` argument.
     */
    if (iter_->item_ptr_.use_count() != 0) {
      /*
       * This checks for a precondition failure, but it's not a full validation
       * of the precondition that the function has not been called. That's
       * because this handle does not have a mutex; if the function were to be
       * called in two different threads this function can fail.
       *
       * The implementation decision here is that it's not worth the expense of
       * a mutex in order to guard against an active attempt to violate the
       * precondition.
       */
      throw std::logic_error(
          "May not register a shared_ptr twice on the same handle");
    }
    if (!ptr) {
      throw std::invalid_argument("May not register a null shared_ptr");
    }
    if (std::addressof(*ptr) != std::addressof(iter_->item_)) {
      throw std::invalid_argument(
          "Argument does not refer to the same object that this handle does");
    }
    iter_->item_ptr_ = ptr;
  }

  /**
   * Release any reference held within the registry.
   *
   * This function is an inverse to `register_shared_ptr`; it takes the entry
   * back to its default state of not referring to any `shared_ptr`.
   *
   * @section Caution
   *
   * This function is not needed in the typical use case of the registry where
   * objects are registered immediately after construction and automatically
   * removed from the registry when destroyed. In this case existence within the
   * registry and accessibility to the object are basically, though with
   * caveats, coterminous. This function would be required, for example, if the
   * same object is going to have a `shared_ptr` registered, reset, and then
   * registered again.
   */
  void reset() noexcept {
    iter_->iter_ptr_.reset();
  }

  /**
   * Access the underlying item, if any.
   *
   * This function always returns a `shared_ptr`, but it does not always return
   * a non-empty pointer. If the returned pointer is empty, it's ambiguous
   * between two possibilities:
   *   - Beginning of life span. The item never had a `shared_ptr` registered in
   *     the first place.
   *   - End of life span. The original `shared_ptr` and any copies have all
   *     been destroyed. Note that the destructor for the underlying object
   *     might not have completed yet; if not, it will be pending in some other
   *     thread.
   *
   * If the returned `shared_ptr` is non-empty, then the underlying object is
   * guaranteed to exist. That doesn't always mean, however, that there's any
   * other `shared_ptr` keeping the referent alive. The return value cam be the
   * last `shared_ptr` to the referent.
   *
   * @section Design
   *
   * This function returns a `shared_ptr<T>` rather than `T&` so that it can
   * never return a dangling reference. The existence of a handle does not
   * guarantee existence of any underlying object.
   *
   * @return A shared_ptr to the underlying item of the handle.
   */
  std::shared_ptr<T> get() noexcept {
    return iter_->item_ptr_.lock();
  }
};

}  // namespace tiledb::sm

#endif  // TILEDB_REGISTRY_H
