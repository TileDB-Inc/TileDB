---
title: Initialization
---

## Prefer direct initialization with a braced initialization list 

The title says it all. Here are some examples.

```c++
// No. This is initialization with an assignment expression.
int x = 1;

// No. This uses a parenthesized expression list.
int x(1);

// No. This has a brace initialization list, but it uses an assignment expression,
// not direct initialization
std::vector<int> y = {1,2,3};

// Yes
int x{1};
std::vector<int> y{1,2,3};
```

## References

* The FAQ for C++11 calls this [uniform initialization](https://isocpp.org/wiki/faq/cpp11-language#uniform-init). The term "uniform initialization" isn't the official name for the syntax, however. Rather, it describes the goal to allow a single syntax to be used for all initializations.
* Section "11.6 Initializers \[dcl.init\]" in the C++17 standard, ISO/IEC 14882:2017.
