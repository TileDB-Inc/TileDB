/**
 * @file  tiledb_cpp_api_map.h
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file declares the C++ API for the TileDB Map object.
 */
#ifndef TILEDB_CPP_API_MAP_H
#define TILEDB_CPP_API_MAP_H

#include "context.h"
#include "deleter.h"
#include "map_schema.h"
#include "utils.h"

#include <map>
#include <memory>
#include <string>

namespace tiledb {

class Map;

namespace impl {
class MultiMapItemProxy;
class MapItemProxy;
}  // namespace impl

/** Object representing a Map key and its values. **/
class MapItem {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Load a MapItem given a pointer. **/
  MapItem(const Context& ctx, tiledb_kv_item_t** item, Map* map = nullptr)
      : ctx_(ctx)
      , map_(map) {
    item_ = std::shared_ptr<tiledb_kv_item_t>(*item, deleter_);
    *item = nullptr;
  }

  MapItem(const MapItem&) = default;
  MapItem(MapItem&& o) = default;
  MapItem& operator=(const MapItem&) = default;
  MapItem& operator=(MapItem&& o) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Checks the goodness of the key-value item. Useful when
   * checking if a retrieved key-value item exists in a map.
   */
  bool good() const {
    return item_ != nullptr;
  }

  /** Set an attribute to the given value **/
  template <typename T>
  void set(const std::string& attr, const T& val) {
    using DataT = typename impl::TypeHandler<T>;

    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_kv_item_set_value(
        ctx,
        item_.get(),
        attr.c_str(),
        DataT::data(val),
        DataT::tiledb_type,
        DataT::size(val) * sizeof(typename DataT::value_type)));
  }

  /** Get the key for the item. */
  template <typename T>
  T key() const {
    using DataT = typename impl::TypeHandler<T>;
    auto& ctx = ctx_.get();

    typename DataT::value_type* buf;
    tiledb_datatype_t type;
    uint64_t size;

    ctx.handle_error(tiledb_kv_item_get_key(
        ctx, item_.get(), (const void**)&buf, &type, &size));

    impl::type_check<T>(type, unsigned(size / sizeof(T)));
    T key;
    DataT::set(key, buf, size);
    return key;
  }

  /** Get the key datatype and size **/
  std::pair<tiledb_datatype_t, uint64_t> key_info() const {
    auto& ctx = ctx_.get();
    const void* key;
    tiledb_datatype_t type;
    uint64_t size;
    ctx.handle_error(
        tiledb_kv_item_get_key(ctx, item_.get(), &key, &type, &size));
    return {type, size};
  }

  /**
   * Get the value for a given attribute.
   *
   * @note
   * This does not check for the number of elements, but rather returns
   * the size (in elements) retrieved.
   * */
  template <typename T>
  std::pair<const T*, uint64_t> get_ptr(const std::string& attr) const {
    using DataT = typename impl::TypeHandler<T>;
    const Context& ctx = ctx_.get();

    typename DataT::value_type* data;
    tiledb_datatype_t type;
    uint64_t size;

    ctx.handle_error(tiledb_kv_item_get_value(
        ctx, item_.get(), attr.c_str(), (const void**)&data, &type, &size));

    auto num = static_cast<unsigned>(size / sizeof(typename DataT::value_type));
    impl::type_check<T>(type);  // Just check type
    return std::pair<const T*, uint64_t>(data, num);
  }

  /** Get a attribute with a given return type. **/
  template <typename T>
  T get(const std::string& attr) const {
    using DataT = typename impl::TypeHandler<T>;

    const typename DataT::value_type* data;
    unsigned num;
    std::tie(data, num) = get_ptr<typename DataT::value_type>(attr);

    T ret;
    DataT::set(ret, data, num * sizeof(typename DataT::value_type));
    return ret;
  }

  /** Get a proxy to set/get with operator[] **/
  impl::MapItemProxy operator[](const std::string& attr);

  impl::MapItemProxy operator[](const char* cstr);

  impl::MultiMapItemProxy operator[](const std::vector<std::string>& attrs);

  /** Ptr to underlying object. **/
  std::shared_ptr<tiledb_kv_item_t> ptr() const {
    return item_;
  }

  template <typename T>
  void set(const T&);

  template <typename T>
  T get() const;

  template <typename T>
  void operator=(const T& v) {
    set<T>(v);
  }

  template <typename T>
  operator T() {
    return get<T>();
  }

 private:
  friend class Map;
  friend class impl::MapItemProxy;
  friend class impl::MultiMapItemProxy;

  void add_to_map();

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Make an item with the given key. **/
  MapItem(
      const Context& ctx,
      const void* key,
      tiledb_datatype_t type,
      size_t size,
      Map* map = nullptr)
      : ctx_(ctx)
      , map_(map) {
    tiledb_kv_item_t* p;
    ctx.handle_error(tiledb_kv_item_alloc(ctx, &p));
    ctx.handle_error(tiledb_kv_item_set_key(ctx, p, key, type, size));
    item_ = std::shared_ptr<tiledb_kv_item_t>(p, deleter_);
  }

  /** A TileDB context reference wrapper. */
  std::reference_wrapper<const Context> ctx_;

  /** Ptr to TileDB object. **/
  std::shared_ptr<tiledb_kv_item_t> item_;

  /** Deleter for tiledb object **/
  impl::Deleter deleter_;

  /** Underlying Map **/
  Map* map_ = nullptr;
};

namespace impl {

/**
 * Proxy class for multi-attribute set and get.
 * The class facilitates tuple unrolling, and is
 * equivalent to setting one attribute at a time.
 * After assignment, the item is added to the
 * underlying map.
 *
 * @details
 * This class should never be constructed explicitly.
 * Instead, it should be used to retrieve the value
 * with the correct type.
 *
 * **Example:**
 *
 * @code{.cpp}
 *   using my_cell_t = std::tuple<int, std::string, std::vector<float>>;
 *
 *   // Implicit conversion
 *   my_cell_t vals_implicit = map[100][{"a1", "a2", "a3"}];
 *
 *   // Explicit conversion
 *   auto vals_explicit = map[100][{"a1", "a2", "a3"}].get<my_cell_t>();
 *
 *   // Defer conversion
 *   auto vals_deferred = map[100][{"a1", "a2", "a3"}];
 *
 *   // vals_deferred is of type MultMapItemProxy, no values
 *   // are fetched yet.
 *
 *   // Writing & flushing map[100] here would change
 *   // the result of below.
 *
 *   auto my_fn = [](my_cell_t cell){ std::cout << std::get<0>(cell); };
 *   my_fn(vals_deferred); // Retrieve values
 *
 *   // Set new values, but does not explicity flush to storage
 *   vals_deferred = std::make_tuple(10, "str", {1.2, 3.2});
 *
 * @endcode
 **/
class MultiMapItemProxy {
 public:
  MultiMapItemProxy(const std::vector<std::string>& attrs, MapItem& item)
      : attrs(attrs)
      , item(item) {
  }

  /** Get multiple attributes into an existing tuple **/
  template <typename... T>
  void get(std::tuple<T...>& tp) const {
    if (attrs.size() != sizeof...(T)) {
      throw TileDBError("Attribute list size does not match tuple length.");
    }
    get_tuple<0, T...>(tp);
  }

  /** Get multiple attributes **/
  template <typename... T>
  std::tuple<T...> get() const {
    std::tuple<T...> ret;
    get<T...>(ret);
    return ret;
  }

  /** Set the attributes **/
  template <typename... T>
  void set(const std::tuple<T...>& vals) {
    if (attrs.size() != sizeof...(T)) {
      throw TileDBError("Attribute list size does not match tuple length.");
    }
    set_tuple<0, T...>(vals);
    add_to_map();
  }

  /** Implicit cast to a tuple. **/
  template <typename... T>
  operator std::tuple<T...>() const {
    return get<T...>();
  }

  /** Set the attributes with a tuple **/
  template <typename... T>
  MultiMapItemProxy& operator=(const std::tuple<T...>& vals) {
    set<T...>(vals);
    return *this;
  }

 private:
  /** Iterate over a tuple. Set version (non const) **/
  /** Base case. Do nothing and terminate recursion. **/
  template <std::size_t I = 0, typename... T>
  inline typename std::enable_if<I == sizeof...(T), void>::type set_tuple(
      const std::tuple<T...>&) {
  }

  template <std::size_t I = 0, typename... T>
      inline typename std::enable_if <
      I<sizeof...(T), void>::type set_tuple(const std::tuple<T...>& t) {
    item.set(attrs[I], std::get<I>(t));
    set_tuple<I + 1, T...>(t);
  }

  /** Iterate over a tuple. Get version (const) **/
  /** Base case. Do nothing and terminate recursion. **/
  template <std::size_t I = 0, typename... T>
  inline typename std::enable_if<I == sizeof...(T), void>::type get_tuple(
      std::tuple<T...>&) const {
  }

  template <std::size_t I = 0, typename... T>
      inline typename std::enable_if <
      I<sizeof...(T), void>::type get_tuple(std::tuple<T...>& t) const {
    std::get<I>(t) =
        item.get<typename std::tuple_element<I, std::tuple<T...>>::type>(
            attrs[I]);
    get_tuple<I + 1, T...>(t);
  }

  /** Keyed attributes **/
  const std::vector<std::string>& attrs;

  /** Item that created proxy. **/
  MapItem& item;

  /** Add to underlying map, if associated with one. Otherwise pass. **/
  bool add_to_map() const;
};

/**
 * Proxy struct to set a single value with operator[].
 * If bound to a map, the item will be added after assignment.
 *
 * @details
 * This class should never be constructed explicitly.
 * Instead, it should be used to retrieve the value
 * with the correct type.
 *
 * **Example:**
 *
 * @code{.cpp}
 *   // Implicit conversion
 *   my_cell_t a2_implicit = map[100]["a2"];
 *
 *   // Explicit conversion
 *   auto a2_explicit = map[100]["a2"].get<std::string>();
 *
 *   // Defer conversion
 *   auto a2_deferred = map[100]["a2"];
 *   // a2_deferred is of type MapItemProxy and is symbolic.
 *
 *   // Writing & flushing map[100]["a2"] here would change
 *   // the result of below.
 *
 *   auto my_fn = [](std::string a2){ std::cout << a2; };
 *   my_fn(a2_deferred); // Retrieve value
 *
 *   // Assigning adds to the map, but does not flush
 *   a2_deferred = "new_value";
 *
 * @endcode
 **/
class MapItemProxy {
 public:
  /** Create a proxy for the given attribute and underlying MapItem **/
  MapItemProxy(std::string attr, MapItem& item)
      : attr(std::move(attr))
      , item(item) {
  }

  /** Set the value **/
  template <typename T>
  void set(T val) {
    item.set<T>(attr, val);
    add_to_map();
  }

  /** Get the value, fundamental type. **/
  template <typename T>
  T get() const {
    return item.get<T>(attr);
  }

  /** Set value with operator= **/
  template <typename T>
  MapItemProxy& operator=(const T& val) {
    set(val);
    add_to_map();
    return *this;
  }

  /** Implicit cast **/
  template <typename T>
  operator T() {
    return get<T>();
  }

  /** Bound attribute name **/
  const std::string attr;

  /** Underlying Item **/
  MapItem& item;

 private:
  /** Add to underlying map, if associated with one. Otherwise pass. **/
  bool add_to_map() const;
};

}  // namespace impl

/** Iterate over items in a map. **/
class MapIter : public std::iterator<std::forward_iterator_tag, MapItem> {
 public:
  /** Construct a iterator for a given map. */
  explicit MapIter(Map& map, bool end = false);
  MapIter(const MapIter&) = delete;
  MapIter(MapIter&&) = default;
  MapIter& operator=(const MapIter&) = delete;
  MapIter& operator=(MapIter&&) = default;

  /** Flush on iterator destruction. **/
  ~MapIter() = default;

  /**
   * Only iterate over keys with some type. Only keys with the
   * underlying tiledb datatype and num will be returned.
   */
  template <typename T>
  void limit_key_type() {
    using DataT = impl::TypeHandler<T>;
    limit_type_ = true;
    type_ = DataT::tiledb_type;
    num_ = DataT::tiledb_num;
  }

  /** Disable any key filters. */
  void all_keys() {
    limit_type_ = false;
  }

  /** Iterators are only equal when both are end. **/
  bool operator==(const MapIter& o) const {
    return done_ == o.done_;
  }

  bool operator!=(const MapIter& o) const {
    return done_ != o.done_;
  }

  MapItem& operator*() const {
    return *item_;
  }

  MapItem* operator->() const {
    return item_.get();
  }

  MapIter& operator++();

 private:
  /** Base map. **/
  Map* map_;
  impl::Deleter deleter_;

  /** Current item **/
  std::unique_ptr<MapItem> item_;

  /** TileDB iterator object **/
  std::shared_ptr<tiledb_kv_iter_t> iter_;

  /** Whether the iterator has reached the end **/
  int done_;

  /** Settings determining filters for the iterator. **/
  bool limit_type_ = false;
  tiledb_datatype_t type_;
  unsigned num_;
};

namespace impl {

/**
 * A copy constructable reference to a MapIter.
 * Used for (auto &i : map) style iteration.
 */
class MapIterReference {
 public:
  MapIterReference(MapIter& iter)
      : iter_(iter) {
  }

  bool operator==(const MapIterReference& o) const {
    return iter_ == o.iter_;
  }

  bool operator!=(const MapIterReference& o) const {
    return iter_ != o.iter_;
  }

  MapItem& operator*() const {
    return iter_.operator*();
  }

  MapItem* operator->() const {
    return iter_.operator->();
  }

  MapIterReference& operator++() {
    iter_.operator++();
    return *this;
  }

 private:
  MapIter& iter_;
};

}  // namespace impl

/**
 * A Key value store backed by a TileDB Sparse array. A Map
 * supports multiple key types and the value is defined by the
 * set of attributes in a MapSchema.
 *
 * **Example:**
 *
 * @code{.cpp}
 * // Make the map
 * tiledb::MapSchema schema(ctx);
 * schema.add_attributes(...);
 * Map::create("my_map", schema);
 *
 * // Write to the map
 * std::vector<double> key = {2345.1, 345.2};
 *
 * // Attribute values
 * int t1 = 3;
 * std::string t2{"ccc"};
 * std::array<float, 2> t3({{3.1f, 3.2f}});
 *
 * map[key][{"a1", "a2", "a3"}] = std::make_tuple(t1, t2, t3);
 * map.flush(); // Flush to disk
 *
 * // Read value from map
 * std::tuple<int, std::string, std::array<float, 2>> vals = map[key];
 * @endcode
 */
class Map {
 public:
  using iterator = MapIter;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Load a map.
   *
   * @param ctx Context
   * @param uri URI of map.
   */
  Map(const Context& ctx, const std::string& uri)
      : schema_(MapSchema(ctx, (tiledb_kv_schema_t*)nullptr))
      , uri_(uri)
      , iter_(*this)
      , iter_end_(*this, true) {
    tiledb_kv_t* kv;
    ctx.handle_error(tiledb_kv_alloc(ctx, uri.c_str(), &kv));
    kv_ = std::shared_ptr<tiledb_kv_t>(kv, deleter_);
    ctx.handle_error(tiledb_kv_open(ctx, kv, nullptr, 0));

    tiledb_kv_schema_t* kv_schema;
    ctx.handle_error(tiledb_kv_get_schema(ctx, kv, &kv_schema));
    schema_ = MapSchema(ctx, kv_schema);

    is_closed_ = false;
    iter_ = MapIter(*this);
  }

  Map(const Map&) = default;
  Map(Map&& o) = default;
  Map& operator=(const Map&) = default;
  Map& operator=(Map&& o) = default;

  ~Map() {
    close();
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Create a map item with the given key. Once populated with attributes,
   * it can be added to a Map with map.add_item()
   */
  template <typename T>
  static MapItem create_item(const Context& ctx, const T& key) {
    using DataT = typename impl::TypeHandler<T>;
    return MapItem(
        ctx,
        DataT::data(key),
        DataT::tiledb_type,
        DataT::size(key) * sizeof(typename DataT::value_type),
        nullptr);
  }

  /** Check if a key is in the map. */
  template <typename T>
  bool has_key(const T& key) {
    using DataT = typename impl::TypeHandler<T>;
    int has;
    auto& ctx = context();

    ctx.handle_error(tiledb_kv_has_key(
        ctx,
        kv_.get(),
        DataT::data(key),
        DataT::tiledb_type,
        DataT::size(key) * sizeof(typename DataT::value_type),
        &has));

    return has != 0;
  }

  /**
   * Get an item from the map given a key. Throws `TileDBError` on missing
   * key.
   */
  template <typename T>
  MapItem get_item(const T& key) {
    using DataT = typename impl::TypeHandler<T>;

    auto& ctx = context();
    tiledb_kv_item_t* item;

    ctx.handle_error(tiledb_kv_get_item(
        ctx,
        kv_.get(),
        DataT::data(key),
        DataT::tiledb_type,
        DataT::size(key) * sizeof(typename DataT::value_type),
        &item));

    return MapItem(schema_.context(), &item, this);
  }

  /** Get an item with a given key. If the item doesn't exist; create it. */
  template <typename T>
  MapItem operator[](const T& key) {
    using DataT = typename impl::TypeHandler<T>;

    auto& ctx = context();
    tiledb_kv_item_t* item;

    ctx.handle_error(tiledb_kv_get_item(
        ctx,
        kv_.get(),
        DataT::data(key),
        DataT::tiledb_type,
        DataT::size(key) * sizeof(typename DataT::value_type),
        &item));

    if (item == nullptr) {
      MapItem mapitem = create_item(schema_.context(), key);
      mapitem.map_ = this;
      return mapitem;
    }

    return MapItem(schema_.context(), &item, this);
  }

  /**
   * Add an item to the map. This populates the map with the key and attribute
   * values.
   */
  Map& add_item(const MapItem& item) {
    auto& ctx = schema_.context();
    ctx.handle_error(tiledb_kv_add_item(ctx, kv_.get(), item.ptr().get()));
    return *this;
  }

  /** Max number of items to buffer in memory before flushing to storage. **/
  void set_max_buffered_items(uint64_t num) {
    auto& ctx = context();
    ctx.handle_error(tiledb_kv_set_max_buffered_items(ctx, kv_.get(), num));
  }

  /** Flush to storage. **/
  void flush() {
    auto& ctx = context();
    ctx.handle_error(tiledb_kv_flush(ctx, kv_.get()));
  }

  /** Get the schema of the map. **/
  const MapSchema& schema() const {
    return schema_;
  }

  /** Get the underlying context. **/
  const Context& context() const {
    return schema_.context();
  }

  /** URI **/
  const std::string& uri() const {
    return uri_;
  }

  void open() {
    auto& ctx = context();
    ctx.handle_error(tiledb_kv_open(ctx, kv_.get(), nullptr, 0));
    tiledb_kv_schema_t* kv_schema;
    ctx.handle_error(tiledb_kv_get_schema(ctx, kv_.get(), &kv_schema));
    schema_ = MapSchema(ctx, kv_schema);
    iter_ = MapIter(*this);
    is_closed_ = false;
  }

  /** Close the map. */
  void close() {
    auto& ctx = context();
    ctx.handle_error(tiledb_kv_close(ctx, kv_.get()));
    is_closed_ = true;
  }

  /** Returns a shared pointer to the C TileDB kv object. */
  std::shared_ptr<tiledb_kv_t> ptr() const {
    return kv_;
  }

  /**
   * Iterator to beginning of map.
   *
   * Note the iterator is global to the Map object.
   * If multiple iterators are needed for concurrent use,
   * then multiple Map objects or manually created MapIter objects are needed.
   **/
  impl::MapIterReference begin() {
    return !is_closed_ ? iter_ : iter_end_;
  }

  /**
   * Iterate over keys of type T.
   *
   * Note the iterator is global to the Map object.
   * If multiple iterators are needed for concurrent use,
   * then multiple Map objects or manually created MapIter objects are needed.
   * */
  template <typename T>
  impl::MapIterReference begin() {
    iter_.limit_key_type<T>();
    return iter_;
  }

  /** Iterator to end of map. **/
  impl::MapIterReference end() {
    // Note this doesn't init, so we can return a new object.
    return iter_end_;
  }

  /* ********************************* */
  /*               STATIC              */
  /* ********************************* */

  /** Create a new map. */
  static void create(const std::string& uri, const MapSchema& schema) {
    auto& ctx = schema.context();
    schema.check();
    ctx.handle_error(tiledb_kv_create(ctx, uri.c_str(), schema.ptr().get()));
  }

  /**
   * Create a TileDB map from a std::map.
   * The resulting tiledb map will be accessible as
   * map[key][attr_name].
   **/
  template <
      typename MapT,
      typename Key = typename MapT::key_type,
      typename Value = typename MapT::mapped_type>
  static void create(
      const Context& ctx,
      const std::string& uri,
      const MapT& map,
      const std::string& attr_name) {
    MapSchema schema(ctx);
    auto a = Attribute::create<Value>(ctx, attr_name);
    schema.add_attribute(a);
    create(uri, schema);
    Map m(ctx, uri);
    for (const auto& p : map) {
      m[p.first][attr_name] = p.second;
    }
    m.close();
  }

  /** Consolidate map fragments. **/
  static void consolidate(const Context& ctx, const std::string& map) {
    ctx.handle_error(tiledb_kv_consolidate(ctx, map.c_str()));
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  friend class MapItem;

  /** Schema of the map. **/
  MapSchema schema_;

  /** True if the map is closed. */
  bool is_closed_;

  /** ptr to underlying TileDB object. **/
  std::shared_ptr<tiledb_kv_t> kv_;

  /** Closes the map on destruction. **/
  impl::Deleter deleter_;

  /** URI **/
  std::string uri_;

  /** Object-global iterator */
  iterator iter_, iter_end_;
};

/* ********************************* */
/*            DEFINITIONS            */
/* ********************************* */

template <typename T>
inline void MapItem::set(const T& v) {
  if (map_->schema_.attribute_num() != 1)
    throw TileDBError(
        "Attribute name must be defined for maps with >1 attribute.");
  operator[](map_->schema_.attribute(0).name()) = v;
}

template <typename T>
inline T MapItem::get() const {
  if (map_->schema_.attribute_num() != 1)
    throw TileDBError(
        "Attribute name must be defined for maps with >1 attribute.");
  return get<T>(map_->schema_.attribute(0).name());
}

inline void MapItem::add_to_map() {
  if (map_)
    map_->add_item(*this);
}

inline impl::MapItemProxy MapItem::operator[](const std::string& attr) {
  return impl::MapItemProxy(attr, *this);
}

inline impl::MapItemProxy MapItem::operator[](const char* cstr) {
  return operator[](std::string(cstr));
}

inline impl::MultiMapItemProxy MapItem::operator[](
    const std::vector<std::string>& attrs) {
  return impl::MultiMapItemProxy(attrs, *this);
}

inline bool impl::MapItemProxy::add_to_map() const {
  if (item.map_ != nullptr) {
    item.map_->add_item(item);
    return true;
  }
  return false;
}

inline bool impl::MultiMapItemProxy::add_to_map() const {
  if (item.map_ != nullptr) {
    item.map_->add_item(item);
    return true;
  }
  return false;
}

inline MapIter::MapIter(Map& map, bool end)
    : map_(&map)
    , done_((int)end) {
  if (!end && map.ptr() != nullptr) {
    auto& ctx = map_->context();
    tiledb_kv_iter_t* kv_iter;
    ctx.handle_error(tiledb_kv_iter_alloc(ctx, map_->ptr().get(), &kv_iter));
    iter_ = std::shared_ptr<tiledb_kv_iter_t>(kv_iter, deleter_);
    this->operator++();
  }
}

inline MapIter& MapIter::operator++() {
  auto& ctx = map_->context();
  if (done_)
    return *this;
  ctx.handle_error(tiledb_kv_iter_done(ctx, iter_.get(), &done_));
  if (done_)
    return *this;
  tiledb_kv_item_t* p;
  ctx.handle_error(tiledb_kv_iter_here(ctx, iter_.get(), &p));
  item_ = std::unique_ptr<MapItem>(new MapItem(ctx, &p, map_));
  ctx.handle_error(tiledb_kv_iter_next(ctx, iter_.get()));
  if (limit_type_) {
    auto t = item_->key_info();
    if (t.first != type_ ||
        (num_ != TILEDB_VAR_NUM && t.second / impl::type_size(t.first) != num_))
      operator++();
  }
  return *this;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_MAP_H
