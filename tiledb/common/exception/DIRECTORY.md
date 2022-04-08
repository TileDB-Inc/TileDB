# Directory `/tiledb/common/exception`

This directory defines `tiledb::common::StatusException` and `tiledb::common::Status`. `class Status` is a legacy class, originally used as a poor substitute for exceptions. `class StatusException` is a proper exception class derived from `std::exception`. `StatusException` is interconvertible with error `Status` objects.

## Goal: Replace `Status` objects

The goal is to incrementally replace `Status` objects with an appropriate replacement. In many cases this will be to throw `StatusException` instead of returning an error `Status`, but not in all cases. In some cases a `bool` may be an appropriate return. In others there might be (or might be created) a semantically appropriate return type that distinguishes between different kinds of failure condititions.

There is no particular urgency to this goal. The code has used `Status` for years. This change is not being made to solve any particular technical defect.

### Benefits of `StatusException`

* **Elimination of code-junk macros `RETURN_NOT_OK` and their ilk.** Error handling is fundamental. It can't be avoided; it must be faced. In C++ there's a basic choice to use exceptions or to use return values. There's syntactic overhead with both. The choice is between `try` statements with exceptions and `return`-`if`-`return` statements otherwise.

  Using `try` statement syntactically marks off error handling code to readily identifiable blocks. Using `if` statements (or a macro encapsulation) intermixes code for each possible failure in with the code that might fail. Since most failures are rare-to-nonexistent, particularly possible logic errors, `try` statements end up with code that is far easier to read.
* **Promotion of RAII and C.41.** When there's a policy of avoiding exceptions, constructors can't throw to enforce class invariants. This means that having class invariants requires private constructors and dedicated factory functions in all cases. Of course it's easier not to do this, which leads to the practice of not defining class invariants. This bypasses the benefits of RAII and obviates the utility of C.41 compliance. 

## Progress toward the goal

* **`class StatusException` is available for use.** Functions that currently return `Status` but only do so to signal logic errors are immediate candidates for conversion.
* **`Status` factory classes are still defined in `status.h`**. Prior to beginning this goal, `class Status` contained a number of member functions to create `Status` objects with designated origins. These functions required global declaration where local declaration would have been more appropriate. These functions are still declared in `status.h` but only because the task of moving all the declarations to more appropriate source files has not yet started.
* **`Status` still has its legacy implementation pattern.** `Status` is due for one more rewrite. Its current internals can be replaced with a single member variable of type `optional<StatusException>`.
