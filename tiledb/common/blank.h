#ifndef TILEDB_BLANK_H
#define TILEDB_BLANK_H

namespace tiledb::common {

/**
 * `blank` is a marker class to highlight calls to constructors that are slated
 * for removal. Without specialization, `blank<T>` is not constructable. Each
 * specialization provides access to a specific constructor is not visible from
 * the class itself.
 *
 * `blank` can be used as a transition mechanism. For example, an initial use of
 * blank can is to hide an existing constructor as private and only make it
 * available through a blank. This does not prevent the use of the old
 * constructor in new code, but it does make the use blindingly obvious.
 */
template <class T>
struct blank {
  template <typename... Args>
  explicit blank(Args&...) = delete;
};

}  // namespace tiledb::common

#endif  // TILEDB_BLANK_H
