---
title: Error handling
---

The code began its life with a don't-throw-exceptions practice. It's beginning a transition into a do-throw-exceptions practice, although that hasn't started quite yet.

## All functions

* **Every function should signal an error only when it gives up what it's trying to do.**

This point may sound obvious, but it's a bit more subtle than that. The magic word is **only**. A function that is unable to do something but knows that it can't could return a `bool` value of `false`, for example. A function that can't tell whether it can do something or not needs to signal an error.

Another way of stating this point is that signalling an error isn't the same thing as failing an operation. If a function only ever succeeds at its operation, then it must signal an error. If a function can both succeed and fail at its operation, then it need not signal an error. Yet it's also possible for a function to both succeed and fail and also stop working and throw an error.

It's up to the function to determine when it should fail (ordinary, without error) and when it should signal an error (not ordinary). The decision about whether or not to do this is not a matter of style, but of substance.

* **A function should signal an error only when circumstances are outside its expectations.**

Some code might be described as whimsical, unexpectedly not performing the duties they seem to have taken on. (It might be appropriate to refer to these as Bartelby functions, because they would prefer not to perform certain tasks.) This point is that a function should be consistent in its error behavior.

* **Each function should document its expectations.**

A user of a function should have some idea about when it may rely upon the behavior of a function. If the function does not contain documentation about what its expectations are, a user cannot know with precision when it may be relied upon.

An expection is somewhat broader than a precondition. All preconditions are expectations, to be sure. Some expectations, though, are not. For example, an expectation about the state of a filesystem might be present. If so, however, it should be documented. There should be no implicit expectations.

* **Signal an error by returning an error status.**

As of now, this code signals error by returning an object of `class Status`.

This advice is will evolve. The first change will be "Signal an either by returning an error status or throwing". The second will be "Signal an error by throwing".

## Signalling an error by returning an error status

This section applies to functions that might not use a `throw` statement to cede control to their caller.

* **Each function that can signal an error without throwing should return a `Status` object.**

The definition of `class Status` is in `tiledb/common/status.h`. Include that header in every compilation unit that defines such functions. If only a declaration is needed, a forward declaration of `class Status` is acceptable.

* **Use a `tuple` return value to return both `Status` and other values.**

A function may return both `Status` and other return values by returning a `tuple`. See style guide entry _Inputs on the right. Outputs on the left._ for more information about how to accomplish this.

* **Return `Status::Ok()` to not signal an error.**

The factory function `Status::Ok()` returns a value that does not signal anything.
 
* **Use a factory function in the vicinity of the error to make `Status` objects.**

A number of factory functions exist to create status messages local to a specific area of the code. A function should use one of these functions to construct `Status` objects that it returns.

### Example

For example, code that is checking for an error related to a `Context` might look like the following:

```c++
#include "tiledb/common/status.h"
using namespace = tiledb::common;
bool failure_condition();

Status foo() {
  if (failure_condition()) {
    return Status_ContextError("Cannot do requested thing");
  }  
  return Status::Ok(); 
}
```

## Signalling an error by throwing

Although throwing exceptions is not yet permitted in library code, some of its precepts can already be stated.

* **Signal an error by throwing an object of `class tdb::exception`.**

The library has `tdb::exception` to allow greater expressiveness and precision than what's afforded in `std::exception`.

* **Convert from returning `Status st` by throwing `tdb::exception(st)`**

`tdb::exception` has a constructor with a single `Status` argument. This constructor is present to allow rapid conversion from returning status to throwing.

## Style guide for error messages

* **Make error messages either informative or empty.**

When someone looks at an exception in practice, the source location and call stack will be available. If what you're saying doesn't add to this, don't say anything else. 
