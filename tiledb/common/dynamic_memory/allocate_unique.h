/*
 * @file allocate_unique.h
 *
 * This code is based upon code by Miro Knejp in Document P0316R0, addressed to
 * the Library Evolution Working Group.
 *
 * http://open-std.org/JTC1/SC22/WG21/docs/papers/2017/p0316r0.html
 */

#ifndef TILEDB_COMMON_ALLOCATE_UNIQUE_H
#define TILEDB_COMMON_ALLOCATE_UNIQUE_H

namespace std {

template <class T, class Alloc>
class allocator_delete {
 public:
  using allocator_type = remove_cv_t<Alloc>;
  using pointer = typename allocator_traits<allocator_type>::pointer;

  template <class OtherAlloc>
  allocator_delete(OtherAlloc&& other);

  void operator()(pointer p);

  allocator_type& get_allocator();
  const allocator_type& get_allocator() const;

 private:
  allocator_type alloc;  // for exposition only
};

template <class T, class Alloc>
class allocator_delete<T, Alloc&> {
 public:
  using allocator_type = remove_cv_t<Alloc>;
  using pointer = typename allocator_traits<allocator_type>::pointer;

  allocator_delete(reference_wrapper<Alloc> alloc);

  void operator()(pointer p);

  Alloc& get_allocator() const;

 private:
  reference_wrapper<Alloc> alloc;  // for exposition only
};

// template<class T, class OtherAlloc>
// allocator_delete(OtherAlloc&& alloc)
//  -> allocator_delete<T, typename allocator_traits<OtherAlloc>::template
//  rebind_alloc<T>>;
//
// template<class T, class Alloc>
// allocator_delete(reference_wrapper<Alloc> alloc)
//  -> allocator_delete<T, Alloc&>;

template <class T, class Alloc, class... Args>
unique_ptr<
    T,
    allocator_delete<
        T,
        typename allocator_traits<Alloc>::template rebind_alloc<T>>>
allocate_unique(Alloc&& alloc, Args&&... args);

template <class T, class Alloc, class... Args>
unique_ptr<T, allocator_delete<T, Alloc&>> allocate_unique(
    reference_wrapper<Alloc> alloc, Args&&... args);

}  // namespace std

#endif  // TILEDB_COMMON_ALLOCATE_UNIQUE_H
