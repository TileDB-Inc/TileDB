#ifndef TILEDB_TRAITS_H
#define TILEDB_TRAITS_H

#include <iterator>
#include <type_traits>
#include "external/include/span/span.hpp"

// An attempt to make a const span iterator.  Since span does not have a const
// iterator, a joined span probably should not either.

namespace tiledb::common {

template <class T, class = decltype(typename T::iterator{})>
inline constexpr bool check_has_iterator(int) {
  return true;
}

template <class>
inline constexpr bool check_has_iterator(unsigned) {
  return false;
}

template <class T, bool U = check_has_iterator<T>(42)>
struct has_iterator {
  constexpr static bool value = U;
};

template <class T>
bool has_iterator_v = has_iterator<T>::value;

template <class T, class = decltype(typename T::const_iterator{})>
inline constexpr bool check_has_const_iterator(int) {
  return true;
}

template <class>
inline constexpr bool check_has_const_iterator(unsigned) {
  return false;
}

template <class T, bool U = check_has_const_iterator<T>(42)>
struct has_const_iterator {
  constexpr static bool value = U;
};

template <class T>
bool has_const_iterator_v = has_const_iterator<T>::value;

template <class IT, class T = decltype(*std::declval<IT>())>
inline constexpr bool is_const_iterator_v =
    !std::is_assignable<decltype(*std::declval<IT>()), T>::value;

template <typename T, typename = void>
struct has_begin_end : std::false_type {};

template <typename T>
struct has_begin_end<
    T,
    std::void_t<
        decltype(std::begin(std::declval<T>())),
        decltype(std::end(std::declval<T>()))>> : std::true_type {};

template <typename T>
constexpr bool has_begin_end_v = has_begin_end<T>::value;

template <typename T, typename = void>
struct has_cbegin_cend : std::false_type {};

template <typename T>
struct has_cbegin_cend<
    T,
    std::void_t<
        decltype(std::cbegin(std::declval<T>())),
        decltype(std::cend(std::declval<T>()))>> : std::true_type {};

template <typename T>
constexpr bool has_cbegin_cend_v = has_cbegin_cend<T>::value;

template <typename T, typename = void>
struct has_to_string : std::false_type {};

template <typename T>
struct has_to_string<
    T,
    std::void_t<decltype(std::to_string(std::declval<T>()))>> : std::true_type {
};

template <typename T>
constexpr bool has_to_string_v = has_to_string<T>::value;

template <class T>
struct is_span {
  constexpr static bool value = false;
};

template <class T>
struct is_span<tcb::span<T>> {
  constexpr static bool value = true;
};

template <class T>
constexpr bool is_span_v = is_span<T>::value;
}  // namespace tiledb::common
#endif  //  TILEDB_TRAITS_H
