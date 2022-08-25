---
title: Functions
---

## Inputs on the right. Outputs on the left.

The legacy of C is that functions have only a single return and only return by
value. When a function should naturally return multiple return values, it was
expedient to pass "out-arguments" to the function. In C this was a pointer; in
C++ it could also be a reference. This convention commingled in- and
out-arguments in a call list. C++17 can do better.

* **Use`std::tuple` to return multiple values.**

    Strictly speaking, returning multiple values was possible before. The overhead
    required was burdensome. A function could return a `struct` value, packaging
    multiple values as a single one. `std::tuple` allows something like an in-place
    `struct` declaration; the elements are typed but anonymous. Such a function is
    still returning a single value, but of a type that's designed to hold multiples.

* **Use `std::optional` to return a value that might not be initialized.**

    A near-universal pattern is to return a value when a function succeeds but not
    to return anything when it fails. This is the pattern when a function throws on
    failure. If it should not throw on failure, though, it has to return both a
    success/failure status and sometimes a value. `std::optional` is the standard
    library's way of representing "sometimes a value". The "not this time" value is
    `std::nullopt`.

    There's an alternative return value that this style guide considers an
    antipattern: a default-constructed instance. Of all the problems it has, the
    most pernicious is that is discourages adherence to "C.41: A constructor should
    create a fully initialized object". It's less efficient, too, since it requires
    an extraneous construction and assignment. The present style guide enables
    direct-initialization with no run-time overhead.

* **Use `std::optional` even for `bool`-convertible classes like `shared_ptr`.**

    Yes, it's certainly possible to use an a empty `shared_ptr` to signify a
    "pointer that's not present". A consistent return convention makes it quicker to
    read code. Please don't put your colleagues in the position of needing to figure
    out that you're doing something different to accomplish something that's not
    different.

* **Remember the `std::in_place_t` form of the `std::optional` constructor.**

  As a rule, it's more efficient to construct an object immediately before you
  need it. When returning an optional, that's the return statement. The `in_place`
  argument to `optional<T>` allows its constructor to also construct the `T`
  instance, removing any need for a temporary or extraneous variable.

* **Prefer a structured binding declaration to initialize variables.**

    Structured binding, introduced in C++17, allows multiple assignment or multiple
    initialization from tuples (or from other types that can act like tuples). While
    it's the preferred way of retrieving return values, it's not the only way.
    
    The easiest alternative is `auto x{f(...)};`. This statement initializes `x` as
    a tuple. The disadvantage is that elements have to be retrieved by position. A
    structured binding declaration allows elements to be retrieved by name. A tuple
    variable is perfectly good, though, when the value is being forwarded on.
    
    Another alternative is to use `std::tie` to assign to existing variables. This
    can be better in circumstances where it's beneficial to reuse an existing
    variable.

* **Prefer structured direct-initialization over structured assignment.**

    One of the forms of structured binding looks like an assignment; it performs
    copy-initialization. The other forms perform direct-initialization. The direct
    form is more general and more efficient that the copy form. (The copy form 
    might be needed in unusual circumstances.) In particular, direct-initialization
    works with non-copyable classes.
    
    Also, use the curly-brace syntax for direct-initialization. There's a
    parenthesis syntax available, but its easily confused at a glance with the
    lambda syntax.

* **By default, use the `&&` ref-qualifier in a structured binding.**

    Roughly speaking, the `&&` ref-qualifier yields perfect forwarding, as if the
    structured variables had been directly initialized in the `return` statement of
    the function.

    If the structured variables should all be constant, then `const auto&` may be
    appropriate, and the rules for temporary materialization will apply. The
    structured binding syntax does not allow mixed declarations with both `const`
    and non-`const` variables, as the cv-declaration is about the temporary value
    (the right side, roughly) rather than to the variables.

* **Check that a returned class has a move constructor.**

    This point is about efficiency. If a move constructor is not present, the
    compiler will use the copy constructor. The default move constructor will
    typically suffice.

### Alternative for tl;dr

* Declare as `tuple<boolean_like, optional<T>> f(...)`
* Initialize as `auto && [b, t]{ f(...) };`
* Move constructor `T(T&&) = default;`

### Example code

```c++
#include <optional>

class NC {
  int x;
  int y;
 public:
  NC() = delete;
  explicit NC(int x, int y) : x(x), y(y) {};
  NC(const NC&) = delete; // No copy constructor
  NC(NC&&) = default;  // ... but there is a move constructor
  int operator()() const { return x; }
};

std::tuple<bool, std::optional<NC>> foo() {
  return {true,std::optional<NC>( std::in_place, 1, 3 )};
}

std::tuple<bool, std::optional<NC>> bar() {
  return {false, std::nullopt};
}

TEST_CASE("foo")
{
  auto && [b, nc]{ foo() };
  CHECK(b);
  REQUIRE(nc.has_value());
  CHECK((*nc)() == 1);
}

TEST_CASE("bar")
{
  auto && [b, nc]{ bar() };
  CHECK_FALSE(b);
  CHECK_FALSE(nc.has_value());
}
```

## References

* [https://en.cppreference.com/w/cpp/utility/tuple](https://en.cppreference.com/w/cpp/utility/tuple)
* [https://en.cppreference.com/w/cpp/utility/optional](https://en.cppreference.com/w/cpp/utility/optional)
* [https://en.cppreference.com/w/cpp/language/structured_binding](https://en.cppreference.com/w/cpp/language/structured_binding)
