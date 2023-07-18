/**
 * @file flow_graph/generic_factory.h
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
 * @section DESCRIPTION Generic factory classes
 */

#ifndef TILEDB_GENERIC_FACTORY
#define TILEDB_GENERIC_FACTORY

#include <memory>  // for `shared_ptr`

namespace tiledb {

//-------------------------------------------------------
// GeneralFactory
//-------------------------------------------------------
/*
 * The `GeneralFactory` is a fully generic factory for both classes and class
 * templates. It provides type erasure to the classes it can produce, and so
 * can produce multiple object types.
 *
 * On the front end, the class presents a production function with an
 * argument signature common to all classes the factory can produce. A generic
 * factory cannot return its produced objects by any kind of value or reference,
 * so it has `void` return and takes a placement argument.
 *
 * On the back end, the class forwards production calls to type-specific
 * factories and wraps them with a consistent placement interface.
 *
 * Life Cycle: Objects constructed in place require explicit calls to their
 * destructors. `GeneralFactory` does not have any requirements on the classes
 * it produces. It is thus incumbent on the user to ensure proper life cycle
 * behavior. There are two ordinary situations where this is straightforward:
 *   1. The class has a trivial destructor. In this case the destructor does not
 *     need to be called at all.
 *   2. The class is derived from a common base class with a virtual destructor.
 *     In this case it's a simple matter to make a type-safe wrapper around the
 *     factory.
 * There's another possibility available with C++23:
 *   3. Each produced class has a destructor with an explicit object parameter.
 *     In such cases a pointer to the destructor is an ordinary pointer to
 *     function, which can be recorded with the produced object and called to
 *     destroy the object. This is something akin to manually creating virtual
 *     destructor.
 */

/**
 * Marker class for constructing `GenericFactory` objects.
 * @tparam T
 */
template <class T>
struct ForClassT {
  ForClassT() = default;
};

template <class T>
constexpr ForClassT<T> ForClass{ForClassT<T>{}};

/**
 * Marker class for constructing `GenericFactory` objects.
 * @tparam T
 */
template <template <class> class T>
struct ForClassTemplateT {
  ForClassTemplateT() = default;
};

template <template <class> class T>
constexpr ForClassTemplateT<T> ForClassTemplate{ForClassTemplateT<T>{}};

/**
 * Generic class declared but not defined; for specialization only.
 */
template <auto f>
class GeneralFactoryImplBase;

/**
 * Abstract base class for factory implementations.
 *
 * The abstract factory only has an argument `f` for the factory signature; it
 * does not have the type actually constructed.
 *
 * @tparam f the function from which the signature of `make` will be drawn
 */
template <class... Args, void (*f)(Args...)>
class GeneralFactoryImplBase<f> {
  /**
   * The size of the class is common to all factories.
   */
  const size_t size_of_class_;

 public:
  explicit constexpr GeneralFactoryImplBase(size_t size)
      : size_of_class_(size){};
  virtual void make(void*, Args&&...) const = 0;
  /**
   * The size of the class that a factory creates.
   */
  [[nodiscard]] size_t size_of_class() const {
    return size_of_class_;
  }
};

/**
 * Generic class declared but not defined; for specialization only.
 */
template <auto f, class P>
class GeneralFactoryImpl;

/**
 * Factory implementation.
 *
 * The implementation function has both a signature argument `f` and a type
 * `T` for the constructor. The class `T` must have a constructor whose
 * signature matches the arguments of `f`.
 *
 * @tparam f the function from which the signature of `make` will be drawn
 * @tparam P the policy class that contains a typed factory
 */
template <class... Args, void (*f)(Args...), class P>
class GeneralFactoryImpl<f, P> : public GeneralFactoryImplBase<f> {
  using Base = GeneralFactoryImplBase<f>;
  using produced_type = P::produced_type;

 public:
  GeneralFactoryImpl()
      : Base(sizeof(produced_type)) {
  }
  void make(void* p, Args&&... args) const override {
    new (p) produced_type{P::make(std::forward<Args>(args)...)};
  }
};

/**
 * Generic class declared but not defined; for specialization only.
 */
template <auto f>
class GeneralFactory;

/**
 * A type-erased factory class.
 *
 * The type of the class to be constructed is erased, but its constructor
 * signature is not. Template arguments for `GeneralFactory` define the
 * signature of its factory function. If a class has a constructor whose
 * function arguments match that of the factory, then the factory can produce
 * it.
 *
 * @tparam f the function that defines the signature of the factory function
 */
template <class... Args, void (*f)(Args...)>
class GeneralFactory<f> {
  /**
   * Pointer to implementation of the factory.
   *
   * `shared_ptr` is used here to enable copy construction.
   *
   * This member would be well-suited for `derived_variant`. None of the
   * derived classes add any state over the base class.
   */
  std::shared_ptr<GeneralFactoryImplBase<f>> factory_;

 public:
  template <class P>
  explicit GeneralFactory(ForClassT<P>)
      : factory_(std::make_shared<GeneralFactoryImpl<f, P>>()) {
  }

  /**
   * The factory function uses a placement signature.
   *
   * @param p The address at which to construct the object
   * @param args Constructor arguments
   */
  void make(void* p, Args&&... args) const {
    factory_->make(p, std::forward<Args>(args)...);
  }

  [[nodiscard]] size_t size_of_class() const {
    return factory_->size_of_class();
  }
  /**
   * Copy constructor
   */
  GeneralFactory(const GeneralFactory&) = default;
  /**
   * Copy constructor
   */
  GeneralFactory(GeneralFactory&&) noexcept = default;
  /**
   * No assignment
   */
  GeneralFactory& operator=(const GeneralFactory&) = delete;
  /**
   * No assignment
   */
  GeneralFactory& operator=(GeneralFactory&&) = delete;
};

//-------------------------------------------------------
// ClassFactory and ClassTemplateFactory
//-------------------------------------------------------

template <class T>
struct SingleClassFactoryPolicy {
  using produced_type = T;

  template <class... Args>
  static T make(Args&&... args) {
    return {std::forward<Args>(args)...};
  }
};

/**
 * Generic class declared but not defined; for specialization only.
 */
template <auto f>
class ClassFactory;

/**
 * A type-erased factory class.
 *
 * The type of the class to be constructed is erased, but its constructor
 * signature is not. The template argument for `ClassFactory` defines the
 * signature of its factory function. If a class has a constructor whose
 * function arguments match that of the factory, then the factory can produce
 * it.
 *
 * @tparam f the function that defines the signature of the factory function
 */
template <class... Args, void (*f)(Args...)>
class ClassFactory<f> : public GeneralFactory<f> {
  using Base = GeneralFactory<f>;

 public:
  template <class T>
  explicit ClassFactory(ForClassT<T>)
      : Base{ForClass<SingleClassFactoryPolicy<T>>} {
  }
};

/**
 * Generic class declared but not defined; for specialization only.
 */
template <auto f, class T>
class ClassTemplateFactory;

template <class... Args, void (*f)(Args...), class T>
class ClassTemplateFactory<f, T> : public GeneralFactory<f> {
  using Base = GeneralFactory<f>;

 public:
  template <template <class> class TT>
  explicit ClassTemplateFactory(ForClassTemplateT<TT>)
      : Base{ForClass<SingleClassFactoryPolicy<TT<T>>>} {
  }
};

}  // namespace tiledb
#endif