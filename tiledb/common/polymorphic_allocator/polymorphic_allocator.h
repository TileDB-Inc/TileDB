/* polymorphic_allocator.h                  -*-C++-*-
 *
 *            Copyright 2017 Pablo Halpern.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */

#ifndef INCLUDED_POLYMORPHIC_ALLOCATOR_DOT_H
#define INCLUDED_POLYMORPHIC_ALLOCATOR_DOT_H

#include <atomic>
#include <cstddef>  // For max_align_t
#include <memory>
#include <new>
#include <scoped_allocator>

namespace cpp17 {

using namespace std;

// The `byte` type is defined exactly this way in C++17's `<cstddef>` (section
// [cstddef.syn]).  It is defined here to allow use of
// `polymorphic_allocator<byte>` as a vocabulary type.
enum class byte : unsigned char {};

namespace pmr {

// Abstract base class for allocator resources.
// Conforms to the C++17 standard, section [mem.res.class].
class memory_resource {
  static constexpr size_t max_align = alignof(max_align_t);

  static atomic<memory_resource*> s_default_resource;

  friend memory_resource* set_default_resource(memory_resource*);
  friend memory_resource* get_default_resource();

 public:
  virtual ~memory_resource();

  void* allocate(size_t bytes, size_t alignment = max_align) {
    return do_allocate(bytes, alignment);
  }
  void deallocate(void* p, size_t bytes, size_t alignment = max_align) {
    return do_deallocate(p, bytes, alignment);
  }

  // `is_equal` is needed because polymorphic allocators are sometimes
  // produced as a result of type erasure.  In that case, two different
  // instances of a polymorphic_memory_resource may actually represent
  // the same underlying allocator and should compare equal, even though
  // their addresses are different.
  bool is_equal(const memory_resource& other) const noexcept {
    return do_is_equal(other);
  }

 protected:
  virtual void* do_allocate(size_t bytes, size_t alignment) = 0;
  virtual void do_deallocate(void* p, size_t bytes, size_t alignment) = 0;
  virtual bool do_is_equal(const memory_resource& other) const noexcept = 0;
};

inline bool operator==(const memory_resource& a, const memory_resource& b) {
  // Call `is_equal` rather than using address comparisons because some
  // polymorphic allocators are produced as a result of type erasure.  In
  // that case, `a` and `b` may contain `memory_resource`s with different
  // addresses which, nevertheless, should compare equal.
  return &a == &b || a.is_equal(b);
}

inline bool operator!=(const memory_resource& a, const memory_resource& b) {
  return !(a == b);
}

namespace __details {

// STL allocator that holds a pointer to a polymorphic allocator resource.
// Used to implement `polymorphic_allocator`, which is a scoped allocator.
template <class Tp>
class polymorphic_allocator_imp {
  memory_resource* m_resource;

 public:
  using value_type = Tp;

  // These types are old-fashioned, pre-C++11 requirements, still needed by
  // g++'s `basic_string` implementation.
  using size_type = size_t;
  using difference_type = ptrdiff_t;
  using reference = Tp&;
  using const_reference = Tp const&;
  using pointer = Tp*;
  using const_pointer = Tp const*;

  polymorphic_allocator_imp();
  polymorphic_allocator_imp(memory_resource* r);

  template <class U>
  polymorphic_allocator_imp(const polymorphic_allocator_imp<U>& other);

  Tp* allocate(size_t n);
  void deallocate(Tp* p, size_t n);

  // Return a default-constructed allocator
  polymorphic_allocator_imp select_on_container_copy_construction() const;

  memory_resource* resource() const;
};

template <class T1, class T2>
bool operator==(
    const polymorphic_allocator_imp<T1>& a,
    const polymorphic_allocator_imp<T2>& b);

template <class T1, class T2>
bool operator!=(
    const polymorphic_allocator_imp<T1>& a,
    const polymorphic_allocator_imp<T2>& b);

template <size_t Align>
struct aligned_chunk;

template <>
struct aligned_chunk<1> {
  char x;
};
template <>
struct aligned_chunk<2> {
  short x;
};
template <>
struct aligned_chunk<4> {
  int x;
};
template <>
struct aligned_chunk<8> {
  long long x;
};
template <>
struct aligned_chunk<16> {
  __attribute__((aligned(16))) char x;
};
template <>
struct aligned_chunk<32> {
  __attribute__((aligned(32))) char x;
};
template <>
struct aligned_chunk<64> {
  __attribute__((aligned(64))) char x;
};

// Adaptor to make a polymorphic allocator resource type from an STL allocator
// type.  This is really a C++20 feature, but it's useful for implementing
// this component.
template <class Allocator>
class resource_adaptor_imp : public memory_resource {
  typename allocator_traits<Allocator>::template rebind_alloc<max_align_t>
      m_alloc;

  template <size_t Align>
  void* allocate_imp(size_t bytes);

  template <size_t Align>
  void deallocate_imp(void* p, size_t bytes);

 public:
  typedef Allocator allocator_type;

  resource_adaptor_imp() = default;

  resource_adaptor_imp(const resource_adaptor_imp&) = default;

  template <class Allocator2>
  resource_adaptor_imp(
      Allocator2&& a2,
      typename enable_if<is_convertible<Allocator2, Allocator>::value, int>::
          type = 0);

 protected:
  void* do_allocate(size_t bytes, size_t alignment = 0) override;
  void do_deallocate(void* p, size_t bytes, size_t alignment = 0) override;

  bool do_is_equal(const memory_resource& other) const noexcept override;

  allocator_type get_allocator() const {
    return m_alloc;
  }
};

}  // end namespace __details

// A resource_adaptor converts a traditional STL allocator to a polymorphic
// memory resource.  Somehow, this didn't make it into C++17, but it should
// have, so here it is.
// This alias ensures that `resource_adaptor<T>` and
// `resource_adaptor<U>` are always the same type, whether or not
// `T` and `U` are the same type.
template <class Allocator>
using resource_adaptor = __details::resource_adaptor_imp<
    typename allocator_traits<Allocator>::template rebind_alloc<byte>>;

// Memory resource that uses new and delete.
class new_delete_resource : public resource_adaptor<std::allocator<byte>> {};

// Return a pointer to a global instance of `new_delete_resource`.
new_delete_resource* new_delete_resource_singleton();

// Get the current default resource
memory_resource* get_default_resource();

// Set the default resource
memory_resource* set_default_resource(memory_resource* r);

template <class Tp>
class polymorphic_allocator : public scoped_allocator_adaptor<
                                  __details::polymorphic_allocator_imp<Tp>> {
  typedef __details::polymorphic_allocator_imp<Tp> Imp;
  typedef scoped_allocator_adaptor<Imp> Base;

 public:
  // g++-4.6.3 does not use allocator_traits in shared_ptr, so we have to
  // provide an explicit rebind.
  template <typename U>
  struct rebind {
    typedef polymorphic_allocator<U> other;
  };

  polymorphic_allocator() = default;
  polymorphic_allocator(memory_resource* r)
      : Base(Imp(r)) {
  }

  template <class U>
  polymorphic_allocator(const polymorphic_allocator<U>& other)
      : Base(Imp((other.resource()))) {
  }

  template <class U>
  polymorphic_allocator(const __details::polymorphic_allocator_imp<U>& other)
      : Base(other) {
  }

  // Return a default-constructed allocator
  polymorphic_allocator select_on_container_copy_construction() const {
    return polymorphic_allocator();
  }

  memory_resource* resource() const {
    return this->outer_allocator().resource();
  }
};

template <class T1, class T2>
inline bool operator==(
    const polymorphic_allocator<T1>& a, const polymorphic_allocator<T2>& b) {
  return a.outer_allocator() == b.outer_allocator();
}

template <class T1, class T2>
inline bool operator!=(
    const polymorphic_allocator<T1>& a, const polymorphic_allocator<T2>& b) {
  return !(a == b);
}

}  // end namespace pmr

///////////////////////////////////////////////////////////////////////////////
// INLINE AND TEMPLATE FUNCTION IMPLEMENTATIONS
///////////////////////////////////////////////////////////////////////////////

inline pmr::memory_resource::~memory_resource() {
}

inline pmr::memory_resource* pmr::get_default_resource() {
  memory_resource* ret = pmr::memory_resource::s_default_resource.load();
  if (nullptr == ret)
    ret = new_delete_resource_singleton();
  return ret;
}

inline pmr::memory_resource* pmr::set_default_resource(
    pmr::memory_resource* r) {
  if (nullptr == r)
    r = new_delete_resource_singleton();

  // TBD, should use an atomic swap
  pmr::memory_resource* prev = get_default_resource();
  pmr::memory_resource::s_default_resource.store(r);
  return prev;
}

template <class Allocator>
template <class Allocator2>
inline pmr::__details::resource_adaptor_imp<Allocator>::resource_adaptor_imp(
    Allocator2&& a2,
    typename enable_if<is_convertible<Allocator2, Allocator>::value, int>::type)
    : m_alloc(forward<Allocator2>(a2)) {
}

template <class Allocator>
template <size_t Align>
void* pmr::__details::resource_adaptor_imp<Allocator>::allocate_imp(
    size_t bytes) {
  typedef __details::aligned_chunk<Align> chunk;
  size_t chunks = (bytes + Align - 1) / Align;

  typedef typename allocator_traits<Allocator>::template rebind_traits<chunk>
      chunk_traits;
  typename chunk_traits::allocator_type rebound(m_alloc);
  return chunk_traits::allocate(rebound, chunks);
}

template <class Allocator>
template <size_t Align>
void pmr::__details::resource_adaptor_imp<Allocator>::deallocate_imp(
    void* p, size_t bytes) {
  typedef __details::aligned_chunk<Align> chunk;
  size_t chunks = (bytes + Align - 1) / Align;

  typedef typename allocator_traits<Allocator>::template rebind_traits<chunk>
      chunk_traits;
  typename chunk_traits::allocator_type rebound(m_alloc);
  return chunk_traits::deallocate(rebound, static_cast<chunk*>(p), chunks);
}

template <class Allocator>
void* pmr::__details::resource_adaptor_imp<Allocator>::do_allocate(
    size_t bytes, size_t alignment) {
  static const size_t max_natural_alignment = sizeof(max_align_t);

  if (0 == alignment) {
    // Choose natural alignment for `bytes`
    alignment = ((bytes ^ (bytes - 1)) >> 1) + 1;
    if (alignment > max_natural_alignment)
      alignment = max_natural_alignment;
  }

  switch (alignment) {
    case 1:
      return allocate_imp<1>(bytes);
    case 2:
      return allocate_imp<2>(bytes);
    case 4:
      return allocate_imp<4>(bytes);
    case 8:
      return allocate_imp<8>(bytes);
    case 16:
      return allocate_imp<16>(bytes);
    case 32:
      return allocate_imp<32>(bytes);
    case 64:
      return allocate_imp<64>(bytes);
    default: {
      size_t chunks = (bytes + sizeof(void*) + alignment - 1) / 64;
      size_t chunkbytes = chunks * 64;
      void* original = allocate_imp<64>(chunkbytes);

      // Make room for original pointer storage
      char* p = static_cast<char*>(original) + sizeof(void*);

      // Round up to nearest alignment boundary
      p += alignment - 1;
      p -= (size_t(p)) & (alignment - 1);

      // Store original pointer in word before allocated pointer
      reinterpret_cast<void**>(p)[-1] = original;

      return p;
    }
  }
}

template <class Allocator>
void pmr::__details::resource_adaptor_imp<Allocator>::do_deallocate(
    void* p, size_t bytes, size_t alignment) {
  static const size_t max_natural_alignment = sizeof(max_align_t);

  if (0 == alignment) {
    // Choose natural alignment for `bytes`
    alignment = ((bytes ^ (bytes - 1)) >> 1) + 1;
    if (alignment > max_natural_alignment)
      alignment = max_natural_alignment;
  }

  switch (alignment) {
    case 1:
      deallocate_imp<1>(p, bytes);
      break;
    case 2:
      deallocate_imp<2>(p, bytes);
      break;
    case 4:
      deallocate_imp<4>(p, bytes);
      break;
    case 8:
      deallocate_imp<8>(p, bytes);
      break;
    case 16:
      deallocate_imp<16>(p, bytes);
      break;
    case 32:
      deallocate_imp<32>(p, bytes);
      break;
    case 64:
      deallocate_imp<64>(p, bytes);
      break;
    default: {
      size_t chunks = (bytes + sizeof(void*) + alignment - 1) / 64;
      size_t chunkbytes = chunks * 64;
      void* original = reinterpret_cast<void**>(p)[-1];

      deallocate_imp<64>(original, chunkbytes);
    }
  }
}

template <class Allocator>
bool pmr::__details::resource_adaptor_imp<Allocator>::do_is_equal(
    const memory_resource& other) const noexcept {
  const resource_adaptor_imp* other_p =
      dynamic_cast<const resource_adaptor_imp*>(&other);

  if (other_p)
    return this->m_alloc == other_p->m_alloc;
  else
    return false;
}

namespace __pmrd = pmr::__details;

template <class Tp>
inline __pmrd::polymorphic_allocator_imp<Tp>::polymorphic_allocator_imp()
    : m_resource(get_default_resource()) {
}

template <class Tp>
inline __pmrd::polymorphic_allocator_imp<Tp>::polymorphic_allocator_imp(
    pmr::memory_resource* r)
    : m_resource(r ? r : get_default_resource()) {
}

template <class Tp>
template <class U>
inline __pmrd::polymorphic_allocator_imp<Tp>::polymorphic_allocator_imp(
    const __pmrd::polymorphic_allocator_imp<U>& other)
    : m_resource(other.resource()) {
}

template <class Tp>
inline Tp* __pmrd::polymorphic_allocator_imp<Tp>::allocate(size_t n) {
  return static_cast<Tp*>(m_resource->allocate(n * sizeof(Tp), alignof(Tp)));
}

template <class Tp>
inline void __pmrd::polymorphic_allocator_imp<Tp>::deallocate(Tp* p, size_t n) {
  m_resource->deallocate(p, n * sizeof(Tp), alignof(Tp));
}

template <class Tp>
inline __pmrd::polymorphic_allocator_imp<Tp> __pmrd::polymorphic_allocator_imp<
    Tp>::select_on_container_copy_construction() const {
  return __pmrd::polymorphic_allocator_imp<Tp>();
}

template <class Tp>
inline pmr::memory_resource* __pmrd::polymorphic_allocator_imp<Tp>::resource()
    const {
  return m_resource;
}

template <class T1, class T2>
inline bool __pmrd::operator==(
    const __pmrd::polymorphic_allocator_imp<T1>& a,
    const __pmrd::polymorphic_allocator_imp<T2>& b) {
  // `operator==` for `memory_resource` first checks for equality of
  // addresses and calls `is_equal` only if the addresses differ.  The call
  // `is_equal` because some polymorphic allocators are produced as a result
  // of type erasure.  In that case, `a` and `b` may contain
  // `memory_resource`s with different addresses which, nevertheless,
  // should compare equal.
  return *a.resource() == *b.resource();
}

template <class T1, class T2>
inline bool __pmrd::operator!=(
    const __pmrd::polymorphic_allocator_imp<T1>& a,
    const __pmrd::polymorphic_allocator_imp<T2>& b) {
  return *a.resource() != *b.resource();
}

}  // namespace cpp17

#endif  // ! defined(INCLUDED_POLYMORPHIC_ALLOCATOR_DOT_H)
